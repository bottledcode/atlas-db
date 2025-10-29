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
	"fmt"
	"io"
	"runtime"
	"sync"
	"time"

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

const NodeTable = "atlas.nodes"

type Server struct {
	UnimplementedConsensusServer
}

var logs *faster.LogManager
var stateMachine sync.Map // map[string]Record
var ownership sync.Map    // map[string]OwnershipState

type OwnershipState struct {
	mu             sync.RWMutex
	promised       *Ballot
	owned          bool
	maxAppliedSlot uint64
}

func init() {
	logs = faster.NewLogManager()
	stateMachine = sync.Map{}
	ownership = sync.Map{}
}

func NewServer() *Server {
	return &Server{}
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
	case *RecordMutation_AclUpdated:
		updated.Acl = m.AclUpdated
	case *RecordMutation_ValueAddress:
		updated.Data = m.ValueAddress
	}
	return updated
}

// call if this is the first time we've seen this key since startup
func (s *Server) recoverKey(key []byte) (*faster.FasterLog, func(), error) {
	obj := &Record{}

	if _, ok := stateMachine.Load(key); ok {
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

	if stateMachine.CompareAndSwap(key, nil, obj) {
		return log, release, nil
	}

	release()

	return nil, nil, fmt.Errorf("concurrent key access: %v", string(key))
}

func (s *Server) StealTableOwnership(ctx context.Context, req *StealTableOwnershipRequest) (*StealTableOwnershipResponse, error) {
	key := req.GetBallot().GetKey()
	newBallot := req.GetBallot()

	// initialize with <0, self>
	ownerShipVal, _ := ownership.LoadOrStore(key, &OwnershipState{
		promised: &Ballot{Id: 0, Node: uint64(options.CurrentOptions.ServerId)},
		owned:    true,
	})
	ownership := ownerShipVal.(*OwnershipState)

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
	}

	uncommitted, err := log.ScanUncommitted()
	if err != nil {
		return nil, err
	}
	missingRecords := make([]*RecordMutation, len(uncommitted))
	var highestSlot *Slot

	for i, entry := range uncommitted {
		mutation := &RecordMutation{}
		err = proto.Unmarshal(entry.Value, mutation)
		if err != nil {
			options.Logger.Warn("StealTableOwnership: failed to unmarshal uncommitted entry", zap.Error(err))
			continue
		}
		missingRecords[i] = mutation

		if highestSlot == nil || highestSlot.Id < entry.Slot {
			highestSlot = &Slot{
				Key:  key,
				Id:   entry.Slot,
				Node: entry.Ballot.NodeID,
			}
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

	ownershipVal, _ := ownership.LoadOrStore(key, &OwnershipState{
		promised: &Ballot{Id: 0, Node: uint64(options.CurrentOptions.ServerId)},
		owned:    true,
	})
	ownership := ownershipVal.(*OwnershipState)

	ownership.mu.Lock()
	defer ownership.mu.Unlock()

	if record.GetBallot().Less(ownership.promised) {
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

	if ownership.promised.Less(record.GetBallot()) {
		ownership.promised = record.GetBallot()
	}

	return &WriteMigrationResponse{Accepted: true}, nil
}

func (s *Server) AcceptMigration(ctx context.Context, req *WriteMigrationRequest) (*emptypb.Empty, error) {
	mutation := req.GetRecord()
	key := mutation.GetSlot().GetKey()
	slotId := mutation.GetSlot().GetId()

	ownershipVal, _ := ownership.LoadOrStore(key, &OwnershipState{
		promised: &Ballot{Id: 0, Node: uint64(options.CurrentOptions.ServerId)},
		owned:    true,
	})
	ownership := ownershipVal.(*OwnershipState)
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

	val, ok := stateMachine.Load(key)
	if !ok {
		return nil, fmt.Errorf("state machine missing key during commit: %v", string(key))
	}
	record := val.(*Record)
	record = record.BaseRecord
	record.BaseRecord = proto.Clone(record).(*Record)

	err = log.IterateCommitted(func(entry *faster.LogEntry) error {
		mu := &RecordMutation{}
		err := proto.Unmarshal(entry.Value, mu)
		if err != nil {
			return err
		}
		record = s.applyMutation(record, mu)
		return nil
	}, faster.IterateOptions{
		MinSlot:            record.BaseRecord.MaxSlot,
		MaxSlot:            0, // always apply all commits so we don't have to worry about out-of-order commits
		IncludeUncommitted: false,
		SkipErrors:         false,
	})
	if err != nil {
		return nil, err
	}

	stateMachine.Store(key, record)
	return &emptypb.Empty{}, nil
}

// Ping implements a simple health check endpoint
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

func (s *Server) ReadKey(ctx context.Context, req *ReadKeyRequest) (*ReadKeyResponse, error) {
spin:
	ownershipVar, ok := ownership.Load(req.Key)
	if !ok {
		return &ReadKeyResponse{
			Success: false,
		}, fmt.Errorf("this node is no longer the owner for this key")
	}
	ownership := ownershipVar.(*OwnershipState)
	ownership.mu.RLock()
	defer ownership.mu.RUnlock()

	recordVar, ok := stateMachine.Load(req.GetKey())
	if !ok {
		return nil, fmt.Errorf("state machine missing key: %v", req.GetKey())
	}
	record := recordVar.(*Record)

	if record.MaxSlot < req.Watermark {
		// spin wait until we reach the watermark
		runtime.Gosched()
		goto spin
	}

	if !canRead(ctx, record) {
		return &ReadKeyResponse{
			Success: false,
		}, fmt.Errorf("principal isn't allowed to read this key")
	}

	return &ReadKeyResponse{
		Success: true,
		Value:   record.Data,
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

func (s *Server) DeleteKey(context.Context, *WriteKeyRequest) (*WriteKeyResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method DeleteKey not implemented, call WriteKey instead")
}

func (s *Server) WriteKey(ctx context.Context, req *WriteKeyRequest) (*WriteKeyResponse, error) {
	return nil, fmt.Errorf("Cannot call WriteKey on consensus server; use WriteMigration instead")
}
