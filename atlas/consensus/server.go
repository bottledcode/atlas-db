/*
 * This file is part of Atlas-DB.
 *
 * Atlas-DB is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of
 * the License, or (at your option) any later version.
 *
 * Atlas-DB is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with Atlas-DB. If not, see <https://www.gnu.org/licenses/>.
 *
 */

package consensus

import (
	"bytes"
	"context"
	"io"
	"sync"
	"time"

	"github.com/bottledcode/atlas-db/atlas/cache"
	"github.com/bottledcode/atlas-db/atlas/faster"
	"github.com/bottledcode/atlas-db/atlas/kv"
	"github.com/bottledcode/atlas-db/atlas/options"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type Server struct {
	UnimplementedConsensusServer
}

var logs *faster.LogManager
var stateMachine *cache.CloxCache[*Record] // Cache of Records
var ownership sync.Map                     // map[string]OwnershipState

func IsOwned(key []byte) bool {
	own := getCurrentOwnershipState(key)
	own.mu.RLock()
	defer own.mu.RUnlock()
	return own.owned
}

type OwnershipState struct {
	mu             sync.RWMutex
	promised       *Ballot
	owned          bool
	maxAppliedSlot uint64
	followers      []chan *RecordMutation
}

func init() {
	logs = faster.NewLogManager()
	// Initialize CloxCache with dynamic configuration based on hardware
	var cfg cache.Config
	if options.CurrentOptions.MaxCacheSize > 0 {
		// Use configured cache size
		cfg = cache.ConfigFromMemorySize(options.CurrentOptions.MaxCacheSize)
		if options.Logger != nil {
			options.Logger.Info("📊 CloxCache configured from max_cache_size",
				zap.String("size", cache.FormatMemory(options.CurrentOptions.MaxCacheSize)),
				zap.Int("shards", cfg.NumShards),
				zap.Int("slots_per_shard", cfg.SlotsPerShard))
		}
	} else {
		// Auto-detect hardware and configure cache
		cfg = cache.ConfigFromHardware()
		hw := cache.DetectHardware()
		if options.Logger != nil {
			options.Logger.Info("📊 CloxCache auto-configured from hardware",
				zap.Int("cpu_count", hw.NumCPU),
				zap.String("total_memory", cache.FormatMemory(hw.TotalMemory)),
				zap.String("cache_size", cache.FormatMemory(hw.CacheSize)),
				zap.Int("shards", cfg.NumShards),
				zap.Int("slots_per_shard", cfg.SlotsPerShard))
		}
	}

	stateMachine = cache.NewCloxCache[*Record](cfg)
	ownership = sync.Map{}
}

func NewServer() *Server {
	return &Server{}
}

func (s *Server) RequestSlots(req *SlotRequest, stream grpc.ServerStreamingServer[RecordMutation]) error {
	if len(req.RequestedSlots) > 0 {
		key := req.GetKey()
		log, release, err := logs.GetLog(key)
		if err != nil {
			return status.Errorf(codes.Internal, "failed to recover key: %v", err)
		}
		defer release()
		// this is a bittorrent-style request for specific slots
		for _, slot := range req.RequestedSlots {
			entry, err := log.Read(slot)
			if err != nil {
				return status.Errorf(codes.Internal, "failed to read slot %d: %v", slot, err)
			}
			if entry == nil || !entry.Committed {
				return status.Errorf(codes.NotFound, "slot %d not found or not committed", slot)
			}
			mutation := &RecordMutation{}
			err = proto.Unmarshal(entry.Value, mutation)
			if err != nil {
				return status.Errorf(codes.Internal, "failed to unmarshal record mutation for slot %d: %v", slot, err)
			}
			err = stream.Send(mutation)
			if err != nil {
				return status.Errorf(codes.Internal, "failed to send record mutation for slot %d: %v", slot, err)
			}
		}
		return nil
	}

	// this is a request for a contiguous section of slots
	key := req.GetKey()
	log, release, err := logs.GetLog(key)
	if err != nil {
		return status.Errorf(codes.Internal, "failed to recover key: %v", err)
	}
	defer release()

	err = log.ReplayRange(req.StartSlot, req.EndSlot, func(entry *faster.LogEntry) error {
		mutation := &RecordMutation{}
		err := proto.Unmarshal(entry.Value, mutation)
		if err != nil {
			return err
		}
		err = stream.Send(mutation)
		return err
	})
	if err != nil {
		return status.Errorf(codes.Internal, "failed to scan key %v[%d-%d]: %v", key, req.StartSlot, req.EndSlot, err)
	}

	return nil
}

func (s *Server) Follow(req *SlotRequest, stream grpc.ServerStreamingServer[RecordMutation]) error {
	// catch up the node
	own := getCurrentOwnershipState(req.Key)
	own.mu.Lock()
	if !own.owned {
		own.mu.Unlock()
		return status.Errorf(codes.FailedPrecondition, "not currently the owner of %v", req.Key)
	}

	following := make(chan *RecordMutation, 100)
	own.followers = append(own.followers, following)
	own.mu.Unlock()

	defer func() {
		own.mu.Lock()
		// remove follower
		for i, follower := range own.followers {
			if follower == following {
				own.followers = append(own.followers[:i], own.followers[i+1:]...)
				break
			}
		}
		own.mu.Unlock()
		close(following)
	}()

	err := s.RequestSlots(req, stream)
	if err != nil {
		return err
	}

	for {
		select {
		case <-stream.Context().Done():
			return nil // clean disconnect
		case next, ok := <-following:
			if !ok {
				// closed chan, so can no longer follow
				return status.Errorf(codes.Unavailable, "No longer the owner")
			}

			err := stream.Send(next)
			if err != nil {
				return err
			}
		}
	}
}

func (s *Server) Replicate(client grpc.ClientStreamingServer[ReplicationRequest, ReplicationResponse]) error {
	applied := false
	db := NewDataRepository(client.Context())
	for {
		req, err := client.Recv()
		if err == io.EOF {
			applied = true
			break
		}
		if err != nil {
			return status.Errorf(codes.Internal, "failed to receive replication request: %v", err)
		}
		err = db.Put(req.GetData())
		if err != nil {
			return status.Errorf(codes.Internal, "failed to replicate data: %v", err)
		}
		applied = true
	}
	return client.SendAndClose(&ReplicationResponse{
		Committed: applied,
	})
}

func (s *Server) DeReference(req *DereferenceRequest, client grpc.ServerStreamingServer[DereferenceResponse]) error {
	db := NewDataRepository(client.Context())
	prefix := db.GetPrefix(req.GetReference())
	err := db.PrefixScan(nil, false, prefix, func(key DataKey, data *Data, transaction *kv.Transaction) error {
		err := client.Send(&DereferenceResponse{
			Data: data,
		})
		if err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		return status.Errorf(codes.Internal, "failed to scan data: %v", err)
	}
	return nil
}

func (b *Ballot) Less(other *Ballot) bool {
	if b.GetId() != other.GetId() {
		return b.GetId() < other.GetId()
	}
	return b.GetNode() < other.GetNode()
}

func (s *Server) applyMutation(obj *Record, mutation *RecordMutation) *Record {
	updated := proto.Clone(obj).(*Record)
	switch m := mutation.GetMessage().(type) {
	case *RecordMutation_Compaction:
		updated = m.Compaction
	case *RecordMutation_Noop:
		// do nothing
	case *RecordMutation_Tombstone:
		updated.Data = nil
	case *RecordMutation_ValueAddress:
		updated.Data = m.ValueAddress
	case *RecordMutation_AddPrincipal:
		// Atomically add principal to ACL
		if updated.Acl == nil {
			updated.Acl = &Acl{}
		}
		principal := m.AddPrincipal.Principal
		switch m.AddPrincipal.Role {
		case AclRole_OWNER:
			updated.Acl.Owners = append(updated.Acl.Owners, principal)
		case AclRole_READER:
			updated.Acl.Readers = append(updated.Acl.Readers, principal)
		case AclRole_WRITER:
			updated.Acl.Writers = append(updated.Acl.Writers, principal)
		}
	case *RecordMutation_RemovePrincipal:
		// Atomically remove principal from ACL
		if updated.Acl != nil {
			principal := m.RemovePrincipal.Principal
			switch m.RemovePrincipal.Role {
			case AclRole_OWNER:
				updated.Acl.Owners = removePrincipalFromList(updated.Acl.Owners, principal)
			case AclRole_READER:
				updated.Acl.Readers = removePrincipalFromList(updated.Acl.Readers, principal)
			case AclRole_WRITER:
				updated.Acl.Writers = removePrincipalFromList(updated.Acl.Writers, principal)
			}
		}
	}
	return updated
}

// removePrincipalFromList removes a principal from a list
func removePrincipalFromList(list []*Principal, toRemove *Principal) []*Principal {
	result := make([]*Principal, 0, len(list))
	for _, p := range list {
		if p.Name != toRemove.Name || p.Value != toRemove.Value {
			result = append(result, p)
		}
	}
	return result
}

// call if this is the first time we've seen this key since startup
func (s *Server) recoverKey(key []byte) (*faster.FasterLog, func(), error) {
	obj := &Record{}

	// Check if key exists in cache
	if _, ok := stateMachine.Get(key); ok {
		return logs.GetLog(key)
	}

	log, release, err := logs.InitKey(key, func(snapshot *faster.Snapshot) error {
		return proto.Unmarshal(snapshot.Data, obj)
	}, func(entry *faster.LogEntry) error {
		record := &RecordMutation{}
		err := proto.Unmarshal(entry.Value, record)
		if err != nil {
			return err
		}
		obj = s.applyMutation(obj, record)
		obj.MaxSlot = entry.Slot
		return nil
	})
	if err != nil {
		return nil, nil, err
	}
	obj.BaseRecord = proto.Clone(obj).(*Record)

	// Insert into cache
	if !stateMachine.Put(key, obj) {
		// Cache admission rejected - still continue, but log might be evicted
		options.Logger.Warn("cache admission rejected for key", zap.String("key", string(key)))
	}

	return log, release, nil
}

func (s *Server) StealTableOwnership(ctx context.Context, req *StealTableOwnershipRequest) (*StealTableOwnershipResponse, error) {
	key := req.GetBallot().GetKey()
	newBallot := req.GetBallot()

	ownership := getCurrentOwnershipState(key)

	ownership.mu.Lock()
	defer ownership.mu.Unlock()

	if newBallot.Less(ownership.promised) {
		return &StealTableOwnershipResponse{
			Promised:      false,
			HighestBallot: ownership.promised,
		}, nil
	}

	log, release, err := s.recoverKey(key)
	if err != nil {
		return nil, err
	}
	defer release()

	ownership.promised = newBallot
	wasOwned := ownership.owned
	ownership.owned = false

	if wasOwned {
		options.Logger.Info("StealTableOwnership: releasing ownership", zap.ByteString("key", key))
		for _, f := range ownership.followers {
			close(f)
		}
		ownership.followers = make([]chan *RecordMutation, 0)
	}

	uncommitted, err := log.ScanUncommitted()
	if err != nil {
		return nil, err
	}
	missingRecords := make([]*RecordMutation, len(uncommitted))
	var (
		haveHighest     bool
		highestSlotID   uint64
		highestSlotNode uint64
	)

	for i, entry := range uncommitted {
		mutation := &RecordMutation{}
		err = proto.Unmarshal(entry.Value, mutation)
		if err != nil {
			options.Logger.Warn("StealTableOwnership: failed to unmarshal uncommitted entry", zap.Error(err))
			continue
		}
		missingRecords[i] = mutation

		if !haveHighest || entry.Slot > highestSlotID {
			haveHighest = true
			highestSlotID = entry.Slot
			highestSlotNode = entry.Ballot.NodeID
		}
	}

	_, committedMax, committedCount := log.GetCommittedRange()
	if committedCount > 0 && (!haveHighest || committedMax > highestSlotID) {
		if committedEntry, readErr := log.ReadCommittedOnly(committedMax); readErr == nil {
			highestSlotNode = committedEntry.Ballot.NodeID
		}
		haveHighest = true
		highestSlotID = committedMax
	}

	var highestSlot *Slot
	if haveHighest {
		highestSlot = &Slot{
			Key:  key,
			Id:   highestSlotID,
			Node: highestSlotNode,
		}
	}

	return &StealTableOwnershipResponse{
		Promised:       true,
		MissingRecords: missingRecords,
		HighestSlot:    highestSlot,
	}, nil
}

func (s *Server) WriteMigration(ctx context.Context, req *WriteMigrationRequest) (*WriteMigrationResponse, error) {
	record := req.GetRecord()
	key := record.GetSlot().GetKey()

	log, release, err := s.recoverKey(key)
	if err != nil {
		return nil, err
	}
	defer release()

	own := getCurrentOwnershipState(key)

	own.mu.Lock()
	defer own.mu.Unlock()

	if record.GetBallot().Less(own.promised) {
		return &WriteMigrationResponse{Accepted: false}, nil
	}

	migration, err := proto.Marshal(record)
	if err != nil {
		return nil, err
	}

	err = log.Accept(record.GetSlot().GetId(), faster.Ballot{
		ID:     record.GetBallot().GetId(),
		NodeID: record.GetBallot().GetNode(),
	}, migration)
	if err != nil {
		return nil, err
	}

	if own.promised.Less(record.GetBallot()) {
		own.promised = record.GetBallot()
	}

	return &WriteMigrationResponse{Accepted: true}, nil
}

func (s *Server) AcceptMigration(ctx context.Context, req *WriteMigrationRequest) (*emptypb.Empty, error) {
	mutation := req.GetRecord()
	key := mutation.GetSlot().GetKey()
	slotId := mutation.GetSlot().GetId()

	ownership := getCurrentOwnershipState(key)
	ownership.mu.RLock()
	defer ownership.mu.RUnlock()

	log, release, err := s.recoverKey(key)
	if err != nil {
		return nil, err
	}
	defer release()

	err = log.Commit(slotId)
	if err != nil {
		return nil, err
	}

	// Get record from cache or reconstruct from log
	record, ok := stateMachine.Get(key)
	if !ok {
		// Cache miss - reconstruct from log
		record = &Record{}
		err = log.IterateCommitted(func(entry *faster.LogEntry) error {
			mu := &RecordMutation{}
			err := proto.Unmarshal(entry.Value, mu)
			if err != nil {
				return err
			}
			record = s.applyMutation(record, mu)
			record.MaxSlot = entry.Slot
			return nil
		}, faster.IterateOptions{
			MinSlot:            0,
			MaxSlot:            slotId,
			IncludeUncommitted: false,
			SkipErrors:         false,
		})
		if err != nil {
			return nil, err
		}
	} else {
		// Cache hit - apply only new mutations
		record = record.BaseRecord
		record.BaseRecord = proto.Clone(record).(*Record)

		err = log.IterateCommitted(func(entry *faster.LogEntry) error {
			mu := &RecordMutation{}
			err := proto.Unmarshal(entry.Value, mu)
			if err != nil {
				return err
			}
			record = s.applyMutation(record, mu)
			record.MaxSlot = entry.Slot
			return nil
		}, faster.IterateOptions{
			MinSlot:            record.BaseRecord.MaxSlot,
			MaxSlot:            0,
			IncludeUncommitted: false,
			SkipErrors:         false,
		})
		if err != nil {
			return nil, err
		}
	}

	// Update cache
	stateMachine.Put(key, record)

	for _, c := range ownership.followers {
		select {
		case c <- mutation:
		// sent successfully
		default:
			options.Logger.Warn("Dropping migration due to slow follower", zap.String("key", string(key)))
		}
	}

	return &emptypb.Empty{}, nil
}

// Ping implements a simple health check endpoint
func (s *Server) ReadRecord(ctx context.Context, req *ReadRecordRequest) (*ReadRecordResponse, error) {
	// Read the record from the local state machine
	// This RPC should only be called on the leader/owner of the key
	record, err := readLocal(req.GetKey())
	if err != nil {
		return &ReadRecordResponse{Success: false}, err
	}

	return &ReadRecordResponse{
		Success: true,
		Record:  record,
	}, nil
}

func (s *Server) Ping(ctx context.Context, req *PingRequest) (*PingResponse, error) {
	// Add mutual node discovery - when we receive a ping, add the sender to our node list
	connectionManager := GetNodeConnectionManager(ctx)
	if connectionManager != nil {
		// Try to find existing node first
		connectionManager.mu.RLock()
		existingNode, exists := connectionManager.nodes[req.SenderNodeId]
		connectionManager.mu.RUnlock()

		if !exists {
			options.Logger.Debug("Received ping from unknown node",
				zap.Uint64("sender_node_id", req.SenderNodeId))
		} else {
			// Node exists, update its last seen time to prevent health checks
			existingNode.mu.Lock()
			existingNode.lastSeen = time.Now()
			existingNode.mu.Unlock()

			// Also ensure it's marked as active if it was previously failed
			if existingNode.GetStatus() != NodeStatusActive {
				existingNode.UpdateStatus(NodeStatusActive)
				connectionManager.addToActiveNodes(existingNode)

				options.Logger.Info("Node recovered through ping",
					zap.Uint64("sender_node_id", req.SenderNodeId),
					zap.String("address", existingNode.GetAddress()))
			}
		}
	}

	return &PingResponse{
		Success:         true,
		ResponderNodeId: options.CurrentOptions.ServerId,
		Timestamp:       timestamppb.Now(),
	}, nil
}

func (s *Server) PrefixScan(ctx context.Context, req *PrefixScanRequest) (*PrefixScanResponse, error) {
	ownedKeys := make([][]byte, 0)
	ownership.Range(func(key, value any) bool {
		if bytes.HasPrefix(key.([]byte), req.Prefix) {
			ownedKeys = append(ownedKeys, key.([]byte))
		}
		return true
	})

	return &PrefixScanResponse{
		Success: true,
		Keys:    ownedKeys,
	}, nil
}
