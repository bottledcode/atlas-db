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
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/bottledcode/atlas-db/atlas/faster"
	"github.com/bottledcode/atlas-db/atlas/kv"
	"github.com/bottledcode/atlas-db/atlas/options"
	"github.com/dgraph-io/badger/v4"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const NodeTable = "atlas.nodes"

type Server struct {
	UnimplementedConsensusServer
	logs       *faster.LogManager
	cache      map[string]*OwnershipTracking
	namedLocks lock
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

func getTrackingKey(key []byte) []byte {
	return []byte(fmt.Sprintf("b:%s", key))
}

func getLogKey(key []byte) []byte {
	return []byte(fmt.Sprintf("log:%s", key))
}

func (s *Server) StealTableOwnership(ctx context.Context, req *StealTableOwnershipRequest) (*StealTableOwnershipResponse, error) {
	log, err := s.logs.GetLog(req.GetBallot().GetKey())
	if err != nil {
		return nil, err
	}

	key := req.GetBallot().GetKey()
	db := kv.GetPool().MetaStore()

	trackingKey := getTrackingKey(key)

	tx, err := db.Begin(true, 0)
	if err != nil {
		return nil, err
	}
	defer tx.Discard()
	ballots, err := tx.Get(ctx, trackingKey)
	if errors.Is(err, badger.ErrKeyNotFound) {
		// this is a totally new object for us
		tracking := &OwnershipTracking{
			Promised:         req.Ballot,
			Owned:            false,
			NextSlot:         0,
			MaxAcceptedSlot:  0,
			MaxCommittedSlot: 0,
		}
		write, err := proto.Marshal(tracking)
		if err != nil {
			return nil, err
		}
		err = tx.Put(ctx, trackingKey, write)
		if err != nil {
			return nil, err
		}
		err = tx.Commit()
		if err != nil {
			return &StealTableOwnershipResponse{
				Promised: false,
			}, err
		}
		return &StealTableOwnershipResponse{
			Promised:       true,
			MissingRecords: nil,
		}, nil
	}
	if err != nil {
		return nil, err
	}

	var tracking OwnershipTracking
	err = proto.Unmarshal(ballots, &tracking)
	if err != nil {
		return nil, err
	}

	if req.GetBallot().Less(tracking.Promised) {
		return &StealTableOwnershipResponse{
			Promised:       false,
			MissingRecords: nil,
		}, nil
	}

	// we give up ownership
	tracking.Promised = req.GetBallot()
	tracking.Owned = false

	write, err := proto.Marshal(&tracking)
	if err != nil {
		return nil, err
	}
	err = tx.Put(ctx, trackingKey, write)
	if err != nil {
		return nil, err
	}

	logKey := getLogKey(key)
	it := tx.IterateHistory(ctx, logKey)
	missingBallots := make([]*RecordMutation, 0)
	success := &StealTableOwnershipResponse{
		Promised:       true,
		MissingRecords: make([]*RecordMutation, 0),
		HighestBallot:  tracking.GetPromised(),
		HighestSlot:    nil,
	}

	current := &RecordMutation{}
	for it.Rewind(); it.Valid(); it.Next() {
		err = it.Item().Value(func(val []byte) error {
			err := proto.Unmarshal(val, current)
			return err
		})
		if err != nil {
			return nil, err
		}
		if !current.GetCommitted() {
			missingBallots = append(missingBallots, proto.Clone(current).(*RecordMutation))
		}
	}

	success.HighestSlot = current

	err = tx.Commit()
	if err != nil {
		return &StealTableOwnershipResponse{
			Promised: false,
		}, err
	}

	return success, nil
}

func (s *Server) WriteMigration(ctx context.Context, req *WriteMigrationRequest) (*WriteMigrationResponse, error) {
	record := req.GetRecord()
	if record == nil {
		return nil, status.Errorf(codes.InvalidArgument, "missing record")
	}

	db := kv.GetPool().MetaStore()
	trackingKey := []byte(fmt.Sprintf("b:%s", record.GetSlot().GetKey()))

	// Phase-2b ballot check: Read tracking to verify we can accept this ballot
	// This is a read-only check before we commit to any writes
	otxCheck, err := db.Begin(false, 0)
	if err != nil {
		return nil, err
	}
	defer otxCheck.Discard()

	bytes, err := otxCheck.Get(ctx, trackingKey)
	var tracking *OwnershipTracking

	if errors.Is(err, badger.ErrKeyNotFound) {
		// New object - no prior promise, so we can accept any ballot
		tracking = &OwnershipTracking{
			Promised:         record.GetBallot(),
			Owned:            false,
			NextSlot:         0,
			MaxAcceptedSlot:  0,
			MaxCommittedSlot: 0,
		}
		options.Logger.Debug("WriteMigration: new object, accepting ballot",
			zap.String("key", string(record.GetSlot().GetKey())),
			zap.Uint64("ballot_id", record.GetSlot().GetId()),
			zap.Uint64("ballot_node", record.GetSlot().GetNode()))
	} else if err != nil {
		return nil, err
	} else {
		// Existing object - check ballot against promise
		tracking = &OwnershipTracking{}
		err = proto.Unmarshal(bytes, tracking)
		if err != nil {
			return nil, err
		}

		// WPaxos Phase-2b requirement: await m.b >= ballots[m.o]
		// Reject if the incoming ballot is less than the promised ballot
		if record.GetBallot().Less(tracking.Promised) {
			options.Logger.Info("WriteMigration: rejecting lower ballot",
				zap.String("key", string(record.GetSlot().GetKey())),
				zap.Uint64("incoming_ballot_id", record.GetSlot().GetId()),
				zap.Uint64("incoming_ballot_node", record.GetSlot().GetNode()),
				zap.Uint64("promised_ballot_id", tracking.Promised.GetId()),
				zap.Uint64("promised_ballot_node", tracking.Promised.GetNode()))
			return &WriteMigrationResponse{
				Accepted: false,
			}, nil
		}

		options.Logger.Debug("WriteMigration: accepting ballot",
			zap.String("key", string(record.GetSlot().GetKey())),
			zap.Uint64("incoming_ballot_id", record.GetSlot().GetId()),
			zap.Uint64("promised_ballot_id", tracking.Promised.GetId()))
	}

	// Ballot check passed - now write to log (source of truth)
	// Transaction 1: Write to versioned log at the specific slot ID
	tx, err := db.Begin(true, record.GetSlot().GetId())
	if err != nil {
		return nil, err
	}
	defer tx.Discard()

	logKey := getLogKey(req.GetRecord().GetSlot().GetKey())
	logBytes, err := proto.Marshal(record)
	if err != nil {
		return nil, err
	}

	err = tx.Put(ctx, logKey, logBytes)
	if err != nil {
		return nil, err
	}

	// DO NOT commit log yet - we need tracking update to be atomic with it
	// Instead, prepare tracking update first

	// Retry loop for tracking update with conflict resolution
	// We've already decided to accept based on the initial ballot check
	// Now we must ensure tracking reflects our acceptance
	for {
		// Update tracking with new ballot and max accepted slot
		// Use max() to handle concurrent updates correctly
		if tracking.Promised == nil || record.GetBallot().Less(tracking.Promised) {
			// Keep existing promise if it's higher
		} else {
			tracking.Promised = record.GetBallot()
		}

		if tracking.MaxAcceptedSlot < record.GetSlot().GetId() {
			tracking.MaxAcceptedSlot = record.GetSlot().GetId()
		}

		trackingBytes, err := proto.Marshal(tracking)
		if err != nil {
			return nil, err
		}

		// Transaction 2: Update tracking metadata (BEFORE committing log)
		otx, err := db.Begin(true, 0)
		if err != nil {
			return nil, err
		}
		defer otx.Discard()

		err = otx.Put(ctx, trackingKey, trackingBytes)
		if err != nil {
			return nil, err
		}

		// CRITICAL: Commit tracking FIRST, then log
		// This ensures ballot promise is persisted before the log entry
		err = otx.Commit()
		if err == nil {
			// Success! Break out of retry loop
			break
		}

		if !errors.Is(err, badger.ErrConflict) {
			// Real error (not a conflict) - fail
			options.Logger.Error("WriteMigration: failed to commit tracking update",
				zap.String("key", string(record.GetSlot().GetKey())),
				zap.Error(err))
			return nil, err
		}

		// Conflict detected - someone else updated tracking concurrently
		// Re-read tracking to get the latest values and retry
		options.Logger.Debug("WriteMigration: tracking conflict, re-reading and retrying",
			zap.String("key", string(record.GetSlot().GetKey())))

		otxRetry, err := db.Begin(false, 0)
		if err != nil {
			return nil, err
		}
		defer otxRetry.Discard()

		bytes, err = otxRetry.Get(ctx, trackingKey)
		if err != nil {
			options.Logger.Error("WriteMigration: failed to re-read tracking after conflict",
				zap.String("key", string(record.GetSlot().GetKey())),
				zap.Error(err))
			return nil, err
		}

		tracking = &OwnershipTracking{}
		err = proto.Unmarshal(bytes, tracking)
		if err != nil {
			return nil, err
		}

		// Loop will retry with updated tracking values
		// We'll take max(existing, ours) for both Promised and MaxAcceptedSlot
	}

	// Now commit the log - if this fails, we have promise but no log entry
	// This is safe: we'll reject lower ballots, and leader can retry
	err = tx.Commit()
	if err != nil {
		options.Logger.Error("WriteMigration: failed to commit log entry (tracking already committed)",
			zap.String("key", string(record.GetSlot().GetKey())),
			zap.Error(err))
		// This is problematic but safer than the reverse
		// We promised a ballot but didn't accept the value
		// The leader will retry and we'll reject lower ballots
		return nil, err
	}

	return &WriteMigrationResponse{
		Accepted: true,
	}, nil
}

func (s *Server) reconstructRecord(ctx context.Context, tx kv.Transaction, key []byte) (*Record, error) {
	it := tx.IterateHistoryReverse(ctx, key)

	// walk history backwards until we arrive at a completed record
	completed := &Record{
		DerivedFrom: nil,
		Acl:         nil,
		Data:        nil,
	}

	gotData := false
	gotAcl := false
	gotDerivedFrom := false

	slot := &RecordMutation{}

	for it.Rewind(); it.Valid(); it.Next() {
		err := it.Item().Value(func(v []byte) error {
			return proto.Unmarshal(v, slot)
		})

		if err != nil {
			return nil, err
		}

		switch m := slot.GetMessage().(type) {
		case *RecordMutation_Compaction:
			completed = m.Compaction
			gotData = true
			gotAcl = true
		case *RecordMutation_Tombstone:
			if !gotData {
				gotData = true
			}
		case *RecordMutation_AclUpdated:
			if !gotAcl {
				gotAcl = true
				completed.Acl = m.AclUpdated
			}
		case *RecordMutation_ValueAddress:
			if !gotData {
				gotData = true
				completed.Data = m.ValueAddress
			}
		case *RecordMutation_Noop:
		default:
			return nil, fmt.Errorf("unknown message type: %T", m)
		}

		if !gotDerivedFrom {
			gotDerivedFrom = true
			completed.DerivedFrom = slot.GetSlot()
		}

		if gotData && gotAcl && gotDerivedFrom {
			break
		}
	}

	return completed, nil
}

func (s *Server) AcceptMigration(ctx context.Context, req *WriteMigrationRequest) (*emptypb.Empty, error) {
	record := req.GetRecord()
	if record == nil {
		return nil, status.Errorf(codes.InvalidArgument, "missing record")
	}

	mdb := kv.GetPool().MetaStore()
	vdb := kv.GetPool().DataStore()

	tx, err := mdb.Begin(false, record.GetSlot().GetId())
	if err != nil {
		return nil, err
	}
	defer tx.Discard()

	logKey := []byte(fmt.Sprintf("log:%s", record.GetSlot().GetKey()))

	newValue, err := s.reconstructRecord(ctx, tx, logKey)
	if err != nil {
		return nil, err
	}

	trackingKey := getTrackingKey(record.GetSlot().GetKey())
	tracking := &OwnershipTracking{}
	for {
		otx, err := mdb.Begin(true, 0)
		if err != nil {
			return nil, err
		}

		bytes, err := otx.Get(ctx, trackingKey)
		if err != nil {
			otx.Discard()
			return nil, err
		}
		err = proto.Unmarshal(bytes, tracking)
		if err != nil {
			otx.Discard()
			return nil, err
		}
		if tracking.MaxCommittedSlot < req.GetRecord().GetSlot().GetId() {
			tracking.MaxCommittedSlot = req.GetRecord().GetSlot().GetId()
			bytes, err = proto.Marshal(tracking)
			if err != nil {
				otx.Discard()
				return nil, err
			}
			err = otx.Put(ctx, trackingKey, bytes)
			if err != nil {
				otx.Discard()
				return nil, err
			}

			err = otx.Commit()
			if err != nil && errors.Is(err, badger.ErrConflict) {
				otx.Discard()
				return nil, err
			} else if err == nil {
				otx.Discard()
				break
			}
		} else {
			otx.Discard()
			break // a later slot has been committed
		}
	}

	currentValue := &Record{}
	for {
		vtx, err := vdb.Begin(true, 0)
		if err != nil {
			return nil, err
		}
		bytes, err := vtx.Get(ctx, newValue.GetDerivedFrom().GetKey())
		if err != nil {
			vtx.Discard()
			return nil, err
		}
		err = proto.Unmarshal(bytes, currentValue)
		if err != nil {
			vtx.Discard()
			return nil, err
		}
		if currentValue.GetDerivedFrom().GetId() > newValue.GetDerivedFrom().GetId() {
			// the current value is already beyond this commit. Nothing to do here
			vtx.Discard()
			break
		} else if newValue.GetDerivedFrom().GetId()-currentValue.GetDerivedFrom().GetId() > 1 {
			// we must keep serialization order, so keep trying.
			vtx.Discard()
			continue
		}
		err = vtx.Put(ctx, newValue.GetDerivedFrom().GetKey(), bytes)
		if err != nil {
			vtx.Discard()
			return nil, err
		}
		err = vtx.Commit()
		if err != nil && !errors.Is(err, badger.ErrConflict) {
			vtx.Discard()
			return nil, err
		} else if err == nil {
			vtx.Discard()
			break
		}
		vtx.Discard()
	}

	return nil, nil
}

// applyAddNodeOperation handles the ADD_NODE operation by properly adding the node using NodeRepositoryKV
func (s *Server) applyAddNodeOperation(ctx context.Context, sessionData []byte, kvStore kv.Store) error {
	// Parse the node data from the ADD_NODE operation
	var nodeData map[string]any
	err := json.Unmarshal(sessionData, &nodeData)
	if err != nil {
		return fmt.Errorf("failed to unmarshal ADD_NODE data: %w", err)
	}

	// Extract node information from the operation data
	id, ok := nodeData["id"].(float64) // JSON unmarshal numbers as float64
	if !ok {
		return fmt.Errorf("missing or invalid node ID")
	}

	address, ok := nodeData["address"].(string)
	if !ok {
		return fmt.Errorf("missing or invalid node address")
	}

	port, ok := nodeData["port"].(float64)
	if !ok {
		return fmt.Errorf("missing or invalid node port")
	}

	region, ok := nodeData["region"].(string)
	if !ok {
		return fmt.Errorf("missing or invalid node region")
	}

	active, ok := nodeData["active"].(bool)
	if !ok {
		return fmt.Errorf("missing or invalid node active status")
	}

	// Construct the node object
	node := &Node{
		Id:      int64(id),
		Address: address,
		Port:    int64(port),
		Region:  &Region{Name: region},
		Active:  active,
		Rtt:     durationpb.New(0), // Default RTT for new nodes
	}

	nodeRepo := NewNodeRepository(ctx, kvStore)
	err = nodeRepo.AddNode(node)
	if err != nil {
		return fmt.Errorf("failed to add node via repository: %w", err)
	}
	qm := GetDefaultQuorumManager(ctx)
	err = qm.AddNode(ctx, node)
	if err != nil {
		return fmt.Errorf("failed to add node to quorum: %w", err)
	}

	options.Logger.Info("Applied ADD_NODE migration",
		zap.Int64("node_id", node.Id),
		zap.String("address", node.Address),
		zap.String("region", node.Region.Name))

	return nil
}

func constructCurrentNode() *Node {
	return &Node{
		Id:      options.CurrentOptions.ServerId,
		Address: options.CurrentOptions.AdvertiseAddress,
		Port:    int64(options.CurrentOptions.AdvertisePort),
		Region: &Region{
			Name: options.CurrentOptions.Region,
		},
		Active: true,
		Rtt:    durationpb.New(0),
	}
}

// JoinCluster adds a node to the cluster on behalf of the node.
func (s *Server) JoinCluster(ctx context.Context, req *Node) (*JoinClusterResponse, error) {
	// Get KV store for metadata operations
	kvPool := kv.GetPool()
	if kvPool == nil {
		return nil, fmt.Errorf("KV pool not initialized")
	}

	metaStore := kvPool.MetaStore()
	if metaStore == nil {
		return nil, fmt.Errorf("metaStore is closed")
	}

	// check if we currently are the owner for node configuration
	tableRepo := NewTableRepositoryKV(ctx, metaStore)
	table, err := tableRepo.GetTable(NodeTable)
	if err != nil {
		return nil, err
	}

	if table == nil {
		return nil, status.Errorf(codes.Internal, "node table not found")
	}

	if table.GetOwner().GetId() != options.CurrentOptions.ServerId {
		return &JoinClusterResponse{
			Success: false,
			Table:   table,
		}, nil
	}

	qm := GetDefaultQuorumManager(ctx)
	q, err := qm.GetQuorum(ctx, NodeTable)
	if err != nil {
		return nil, err
	}

	// Create KV change for adding the node
	nodeChange := map[string]any{
		"operation": "ADD_NODE",
		"id":        req.GetId(),
		"address":   req.GetAddress(),
		"port":      req.GetPort(),
		"region":    req.GetRegion().GetName(),
		"active":    true,
	}

	migrationData, err := json.Marshal(nodeChange)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal node change: %w", err)
	}

	nodeTable := table
	dr := NewDataRepository(ctx, kv.GetPool().DataStore())
	mr := NewMigrationRepositoryKV(ctx, metaStore, dr)
	nextVersion, err := mr.GetNextVersion(NodeTable)
	if err != nil {
		return nil, fmt.Errorf("failed to get next migration version: %w", err)
	}

	// create a new migration
	migration := &Migration{
		Version: &MigrationVersion{
			TableVersion:     nodeTable.GetVersion(),
			MigrationVersion: nextVersion,
			NodeId:           options.CurrentOptions.ServerId,
			TableName:        NodeTable,
		},
		Migration: &Migration_Data{
			Data: &DataMigration{
				Session: &DataMigration_Change{
					Change: &KVChange{
						Operation: &KVChange_Data{
							Data: &RawData{
								Data: migrationData,
							},
						},
					},
				},
			},
		},
	}

	mreq := &WriteMigrationRequest{
		Sender:    constructCurrentNode(),
		Migration: migration,
	}

	resp, err := q.WriteMigration(ctx, mreq)
	if err != nil {
		return nil, err
	}

	if !resp.Success {
		return &JoinClusterResponse{
			Success: false,
			Table:   nodeTable,
		}, nil
	}

	_, err = q.AcceptMigration(ctx, mreq)
	if err != nil {
		return nil, err
	}

	return &JoinClusterResponse{
		Success: true,
		NodeId:  req.GetId(),
	}, nil
}

var gossipQueue sync.Map

type gossipKey struct {
	table        string
	tableVersion int64
	version      int64
	by           int64
}

func createGossipKey(version *MigrationVersion) gossipKey {
	return gossipKey{
		table:        version.GetTableName(),
		tableVersion: version.GetTableVersion(),
		version:      version.GetMigrationVersion(),
		by:           version.GetNodeId(),
	}
}

// applyGossipMigration applies a gossip migration to the database.
func (s *Server) applyGossipMigration(ctx context.Context, req *GossipMigration) error {
	// Get KV store for metadata operations
	kvPool := kv.GetPool()
	if kvPool == nil {
		return fmt.Errorf("KV pool not initialized")
	}

	metaStore := kvPool.MetaStore()
	if metaStore == nil {
		return fmt.Errorf("metaStore is closed")
	}

	dr := NewDataRepository(ctx, kv.GetPool().DataStore())
	mr := NewMigrationRepositoryKV(ctx, metaStore, dr)

	// check to see if we have the previous migration already
	prev, err := mr.GetMigrationVersion(req.GetPreviousMigration())
	if err != nil {
		return err
	}
	if len(prev) == 0 {
		// no previous version found, so we need to store this migration in the gossip queue
		gk := createGossipKey(req.GetPreviousMigration())
		// see if we have it in the gossip queue
		if prev, ok := gossipQueue.LoadAndDelete(gk); ok {
			// we already have it, so now apply it
			err = s.applyGossipMigration(ctx, prev.(*GossipMigration))
			if err != nil {
				// put the failed migration back in the queue
				gossipQueue.Store(gk, prev)
				return err
			}
		} else {
			// we don't have it, so store the current migration in the queue
			gossipQueue.Store(gk, req)
			return nil
		}
	}

	// todo: do we need to check the previous migration was committed?

	// write the migration to the log
	err = mr.AddMigration(req.GetMigrationRequest())
	if err != nil {
		return err
	}

	// Get the appropriate KV store for the migration
	var kvStore kv.Store
	if kvPool == nil {
		return fmt.Errorf("KV pool not initialized")
	}

	if strings.HasPrefix(req.GetMigrationRequest().GetVersion().GetTableName(), "atlas.") {
		// Use metadata store for atlas tables
		kvStore = kvPool.MetaStore()
	} else {
		// Use data store for user tables
		kvStore = kvPool.DataStore()
	}

	if kvStore == nil {
		return fmt.Errorf("KV store not available")
	}

	// we have a previous migration, so apply this one
	err = s.applyMigration([]*Migration{req.GetMigrationRequest()}, kvStore)
	if err != nil {
		return err
	}

	// and now mark it as committed
	err = mr.CommitMigrationExact(req.GetMigrationRequest().GetVersion())
	if err != nil {
		return err
	}

	return nil
}

func SendGossip(ctx context.Context, req *GossipMigration, kvStore kv.Store) error {
	// now we see if there is still a ttl remaining
	if req.GetTtl() <= 0 {
		return nil
	}

	// pick 5 random nodes to gossip to, excluding the sender, owner, and myself
	nr := NewNodeRepository(ctx, kvStore)
	nodes, err := nr.GetRandomNodes(5,
		req.GetMigrationRequest().GetVersion().GetNodeId(),
		options.CurrentOptions.ServerId,
		req.GetSender().GetId(),
	)
	if err != nil {
		return err
	}

	errs := make([]error, len(nodes))

	nextReq := &GossipMigration{
		MigrationRequest:  req.GetMigrationRequest(),
		Table:             req.GetTable(),
		PreviousMigration: req.GetPreviousMigration(),
		Ttl:               req.GetTtl() - 1,
		Sender:            constructCurrentNode(),
	}

	wg := sync.WaitGroup{}
	wg.Add(len(nodes))

	// gossip to the nodes
	for i, node := range nodes {
		go func(i int) {
			defer wg.Done()

			client, closer, err := getNewClient(fmt.Sprintf("%s:%d", node.GetAddress(), node.GetPort()))
			if err != nil {
				errs[i] = err
			}
			defer closer()

			_, err = client.Gossip(ctx, nextReq)
			if err != nil {
				errs[i] = err
			}
		}(i)
	}

	// wait for gossip to complete
	wg.Wait()

	options.Logger.Info("gossip complete", zap.String("table", req.GetTable().GetName()), zap.Int64("version", req.GetTable().GetVersion()))

	return errors.Join(errs...)
}

// Gossip is a method that randomly disseminates information to other nodes in the cluster.
// A leader disseminates this to every node in the cluster after a commit.
func (s *Server) Gossip(ctx context.Context, req *GossipMigration) (*emptypb.Empty, error) {
	// Get KV store for metadata operations
	kvPool := kv.GetPool()
	if kvPool == nil {
		return nil, fmt.Errorf("KV pool not initialized")
	}

	metaStore := kvPool.MetaStore()
	if metaStore == nil {
		return nil, fmt.Errorf("metaStore is closed")
	}

	tr := NewTableRepositoryKV(ctx, metaStore)

	// update the local table cache if the version is newer than the current version
	tb, err := tr.GetTable(req.GetTable().GetName())
	if err != nil {
		return nil, err
	}
	if tb == nil {
		err = tr.InsertTable(req.GetTable())
		if err != nil {
			return nil, err
		}
	} else if tb.GetVersion() < req.GetTable().GetVersion() {
		err = tr.UpdateTable(req.GetTable())
		if err != nil {
			return nil, err
		}
	}

	// apply the current migration if we can do so
	err = s.applyGossipMigration(ctx, req)
	if err != nil {
		return nil, err
	}

	// gossip to other nodes
	err = SendGossip(ctx, req, metaStore)
	if err != nil {
		return nil, err
	}

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
			// Node doesn't exist, we need to discover it from the node repository
			nodeRepo := connectionManager.storage
			if nodeRepo != nil {
				var discoveredNode *Node
				_ = nodeRepo.Iterate(false, func(node *Node, txn *kv.Transaction) error {
					if node.Id == req.SenderNodeId {
						discoveredNode = node
						return nil // found it, stop iterating
					}
					return nil // continue searching
				})

				if discoveredNode != nil {
					options.Logger.Info("Discovered node through ping, adding to connection manager",
						zap.Int64("sender_node_id", req.SenderNodeId),
						zap.String("address", discoveredNode.GetAddress()))

					// Add the discovered node to our connection manager
					err := connectionManager.AddNode(ctx, discoveredNode)
					if err != nil {
						options.Logger.Warn("Failed to add discovered node to connection manager",
							zap.Int64("sender_node_id", req.SenderNodeId),
							zap.Error(err))
					}
				} else {
					options.Logger.Debug("Received ping from unknown node not in repository",
						zap.Int64("sender_node_id", req.SenderNodeId))
				}
			}
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
					zap.Int64("sender_node_id", req.SenderNodeId),
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
	// Verify we're the leader for this table
	kvPool := kv.GetPool()
	if kvPool == nil {
		return &ReadKeyResponse{
			Success: false,
			Error:   "KV pool not initialized",
		}, nil
	}

	metaStore := kvPool.MetaStore()
	if metaStore == nil {
		return &ReadKeyResponse{
			Success: false,
			Error:   "metaStore is closed",
		}, nil
	}

	// Check if we're actually the leader for this table
	tr := NewTableRepositoryKV(ctx, metaStore)
	table, err := tr.GetTable(req.GetTable())
	if err != nil {
		return &ReadKeyResponse{
			Success: false,
			Error:   fmt.Sprintf("failed to get table info: %v", err),
		}, nil
	}

	if table == nil {
		return &ReadKeyResponse{
			Success: false,
			Error:   "table not found",
		}, nil
	}

	// Verify we're the owner of this table
	if table.Owner.Id != options.CurrentOptions.ServerId {
		return &ReadKeyResponse{
			Success: false,
			Error:   "not the leader for this table",
		}, nil
	}

	// Enforce ACL if present in meta store (owner-only model for now)
	dataStore := kvPool.DataStore()
	if dataStore == nil {
		return &ReadKeyResponse{
			Success: false,
			Error:   "data store not available",
		}, nil
	}

	keyBytes := []byte(req.GetKey())

	// Read the key from local store
	value, err := dataStore.Get(ctx, keyBytes)
	if err != nil {
		if errors.Is(err, kv.ErrKeyNotFound) {
			// Key not found, but this is a successful read
			return &ReadKeyResponse{
				Success: true,
				Value:   nil, // nil value indicates key not found
			}, nil
		}
		return &ReadKeyResponse{
			Success: false,
			Error:   fmt.Sprintf("failed to read key: %v", err),
		}, nil
	}

	var record Record
	err = proto.Unmarshal(value, &record)
	if err != nil {
		return &ReadKeyResponse{
			Success: false,
			Error:   fmt.Sprintf("failed to unmarshal record: %v", err),
		}, nil
	}

	if !canRead(ctx, &record) {
		return &ReadKeyResponse{
			Success: false,
			Error:   "principal isn't allowed to read this key",
		}, nil
	}

	// Check if the record contains a DataReference instead of direct data
	var valueData []byte
	if record.GetValue() != nil {
		// Direct value present
		valueData = record.GetValue().GetData()
	} else if record.GetRef() != nil {
		// DataReference present - need to dereference
		dr := NewDataRepository(ctx, dataStore)
		dereferencedRecord, err := dr.Dereference(&record)
		if err != nil {
			return &ReadKeyResponse{
				Success: false,
				Error:   fmt.Sprintf("failed to dereference record: %v", err),
			}, nil
		}

		if dereferencedRecord == nil {
			return &ReadKeyResponse{
				Success: false,
				Error:   "dereferenced record is nil - referenced data not found",
			}, nil
		}

		if dereferencedRecord.GetValue() == nil {
			return &ReadKeyResponse{
				Success: false,
				Error:   "dereferenced record contains no value data",
			}, nil
		}

		valueData = dereferencedRecord.GetValue().GetData()
	}
	// If both GetValue() and GetRef() are nil, valueData remains nil (valid for key-not-found case)

	return &ReadKeyResponse{
		Success: true,
		Value:   valueData,
	}, nil
}

func (s *Server) PrefixScan(ctx context.Context, req *PrefixScanRequest) (*PrefixScanResponse, error) {
	if req.GetTablePrefix() == "" && req.GetRowPrefix() != "" {
		return &PrefixScanResponse{
			Success: false,
			Error:   "row prefix must be specified with the table prefix",
		}, nil
	}

	kvPool := kv.GetPool()
	if kvPool == nil {
		return &PrefixScanResponse{
			Success: false,
			Error:   "KV pool not initialized",
		}, nil
	}

	metaStore := kvPool.MetaStore()
	if metaStore == nil {
		return &PrefixScanResponse{
			Success: false,
			Error:   "metadata store is not available",
		}, nil
	}

	dataStore := kvPool.DataStore()
	if dataStore == nil {
		return &PrefixScanResponse{
			Success: false,
			Error:   "data store is not available",
		}, nil
	}

	nr := NewNodeRepository(ctx, metaStore)
	currentNode, err := nr.GetNodeById(options.CurrentOptions.ServerId)
	if err != nil {
		return &PrefixScanResponse{
			Success: false,
			Error:   fmt.Sprintf("failed to get the current node: %v", err),
		}, nil
	}
	if currentNode == nil {
		return &PrefixScanResponse{
			Success: false,
			Error:   "node not yet part of a cluster",
		}, nil
	}

	options.Logger.Info("PrefixScan request",
		zap.String("table prefix", req.GetTablePrefix()),
		zap.String("row prefix", req.GetRowPrefix()),
		zap.Int64("node_id", currentNode.Id))

	keyPrefix := kv.NewKeyBuilder().Table(req.GetTablePrefix())
	if req.GetRowPrefix() != "" {
		keyPrefix = keyPrefix.Row(req.GetRowPrefix())
	}

	matchingKeys, err := dataStore.PrefixScan(ctx, keyPrefix.Build())
	if err != nil {
		return &PrefixScanResponse{
			Success: false,
			Error:   fmt.Sprintf("failed to scan prefix: %v", err),
		}, nil
	}

	// check ACL across matching keys
	txn, err := dataStore.Begin(false)
	if err != nil {
		return nil, err
	}
	defer txn.Discard()

	var ownedKeys []string
	var record Record
	for key, val := range matchingKeys {
		err = proto.Unmarshal(val, &record)
		if err != nil {
			return nil, err
		}
		if canRead(ctx, &record) {
			kb := kv.NewKeyBuilderFromBytes([]byte(key))
			ownedKeys = append(ownedKeys, kb.DottedKey())
		}
	}

	options.Logger.Info("PrefixScan completed",
		zap.String("table prefix", req.GetTablePrefix()),
		zap.String("row prefix", req.GetRowPrefix()),
		zap.Int("matched", len(matchingKeys)),
		zap.Int("owned", len(ownedKeys)))

	return &PrefixScanResponse{
		Success: true,
		Keys:    ownedKeys,
	}, nil
}

func (s *Server) DeleteKey(context.Context, *WriteKeyRequest) (*WriteKeyResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method DeleteKey not implemented, call WriteKey instead")
}

func (s *Server) WriteKey(ctx context.Context, req *WriteKeyRequest) (*WriteKeyResponse, error) {
	// Verify we're the leader for this table
	kvPool := kv.GetPool()
	if kvPool == nil {
		return &WriteKeyResponse{
			Success: false,
			Error:   "KV pool not initialized",
		}, nil
	}

	metaStore := kvPool.MetaStore()
	if metaStore == nil {
		return &WriteKeyResponse{
			Success: false,
			Error:   "metaStore is closed",
		}, nil
	}

	// Check if we're actually the leader for this table
	tr := NewTableRepositoryKV(ctx, metaStore)
	table, err := tr.GetTable(req.GetTable())
	if err != nil {
		return &WriteKeyResponse{
			Success: false,
			Error:   fmt.Sprintf("failed to get table info: %v", err),
		}, nil
	}

	if table == nil {
		return &WriteKeyResponse{
			Success: false,
			Error:   "table not found",
		}, nil
	}

	// Verify we're the owner of this table
	if table.Owner.Id != options.CurrentOptions.ServerId {
		return &WriteKeyResponse{
			Success: false,
			Error:   "not the leader for this table",
		}, nil
	}

	// We are the leader - execute the full consensus protocol
	qm := GetDefaultQuorumManager(ctx)
	nr := NewNodeRepository(ctx, metaStore)

	currentNode, err := nr.GetNodeById(options.CurrentOptions.ServerId)
	if err != nil {
		return &WriteKeyResponse{
			Success: false,
			Error:   fmt.Sprintf("failed to get the current node: %v", err),
		}, nil
	}
	if currentNode == nil {
		return &WriteKeyResponse{
			Success: false,
			Error:   "node not yet part of a cluster",
		}, nil
	}

	quorum, err := qm.GetQuorum(ctx, req.GetTable())
	if err != nil {
		return &WriteKeyResponse{
			Success: false,
			Error:   fmt.Sprintf("failed to get quorum: %v", err),
		}, nil
	}

	// Attempt to steal table ownership to confirm leadership
	tableOwnership, err := quorum.StealTableOwnership(ctx, &StealTableOwnershipRequest{
		Sender: currentNode,
		Reason: StealReason_writeReason,
		Table:  table,
	})
	if err != nil {
		return &WriteKeyResponse{
			Success: false,
			Error:   fmt.Sprintf("failed to steal ownership: %v", err),
		}, nil
	}

	if !tableOwnership.Promised {
		return &WriteKeyResponse{
			Success: false,
			Error:   "leadership could not be confirmed",
		}, nil
	}

	// Execute the migration
	dr := NewDataRepository(ctx, kv.GetPool().DataStore())
	mr := NewMigrationRepositoryKV(ctx, metaStore, dr)
	version, err := mr.GetNextVersion(req.GetTable())
	if err != nil {
		return &WriteKeyResponse{
			Success: false,
			Error:   fmt.Sprintf("failed to get the next version: %v", err),
		}, nil
	}

	principal := getPrincipalFromContext(ctx)
	now := timestamppb.Now()

	store := kvPool.DataStore()
	txn, err := store.Begin(false)
	defer txn.Discard()
	if err != nil {
		return &WriteKeyResponse{
			Success: false,
			Error:   fmt.Sprintf("failed to begin transaction: %v", err),
		}, nil
	}
	switch op := req.GetValue().Operation.(type) {
	case *KVChange_Set:
		key := op.Set.GetKey()
		var record Record
		val, err := txn.Get(ctx, key)
		if err != nil && errors.Is(err, kv.ErrKeyNotFound) {
			options.Logger.Info("WriteKey creating new key",
				zap.String("key", string(key)),
				zap.String("principal", principal),
			)
			if op.Set.Data.AccessControl == nil && principal != "" {
				op.Set.Data.AccessControl = &ACL{
					Owners: &ACLData{
						Principals: []string{principal},
						CreatedAt:  now,
						UpdatedAt:  now,
					},
				}
			}
			break
		}
		if err != nil {
			return &WriteKeyResponse{
				Success: false,
				Error:   fmt.Sprintf("failed to get key: %v", err),
			}, nil
		}
		err = proto.Unmarshal(val, &record)
		if err != nil {
			return nil, fmt.Errorf("failed to unmarshal record: %v", err)
		}
		options.Logger.Info("WriteKey ACL check",
			zap.String("key", string(key)),
			zap.String("principal", principal),
			zap.Bool("has_acl", record.AccessControl != nil),
			zap.Bool("can_write", canWrite(ctx, &record)),
		)
		if canWrite(ctx, &record) {
			// copy the previous acl to the current operation
			op.Set.Data.AccessControl = record.GetAccessControl()
			break
		}
		return &WriteKeyResponse{
			Success: false,
			Error:   "permission denied: principal isn't allowed to write to this key",
		}, nil
	case *KVChange_Del:
		key := op.Del.GetKey()
		var record Record
		val, err := txn.Get(ctx, key)
		if err != nil && errors.Is(err, kv.ErrKeyNotFound) {
			break
		}
		if err != nil {
			return &WriteKeyResponse{
				Success: false,
				Error:   fmt.Sprintf("failed to get key: %v", err),
			}, nil
		}
		err = proto.Unmarshal(val, &record)
		if err != nil {
			return nil, fmt.Errorf("failed to unmarshal record: %v", err)
		}
		if canWrite(ctx, &record) {
			break
		}
		return &WriteKeyResponse{
			Success: false,
			Error:   "principal isn't allowed to delete this key",
		}, nil
	case *KVChange_Acl:
		key := op.Acl.GetKey()
		var record Record
		val, err := txn.Get(ctx, key)
		if err != nil && errors.Is(err, kv.ErrKeyNotFound) {
			break
		}
		if err != nil {
			return &WriteKeyResponse{
				Success: false,
				Error:   fmt.Sprintf("failed to get key: %v", err),
			}, nil
		}
		err = proto.Unmarshal(val, &record)
		if err != nil {
			return nil, fmt.Errorf("failed to unmarshal record: %v", err)
		}
		if isOwner(ctx, &record) {
			break
		}
		return &WriteKeyResponse{
			Success: false,
			Error:   "principal isn't allowed to modify ACLs for this key",
		}, nil
	}

	migration := &WriteMigrationRequest{
		Sender: currentNode,
		Migration: &Migration{
			Version: &MigrationVersion{
				TableVersion:     table.GetVersion(),
				MigrationVersion: version,
				NodeId:           currentNode.GetId(),
				TableName:        req.GetTable(),
			},
			Migration: &Migration_Data{
				Data: &DataMigration{
					Session: &DataMigration_Change{
						Change: req.GetValue(),
					},
				},
			},
		},
	}

	// Write migration phase
	migrationResult, err := quorum.WriteMigration(ctx, migration)
	if err != nil {
		return &WriteKeyResponse{
			Success: false,
			Error:   fmt.Sprintf("failed to write migration: %v", err),
		}, nil
	}

	if !migrationResult.GetSuccess() {
		return &WriteKeyResponse{
			Success: false,
			Error:   "migration failed due to outdated table",
		}, nil
	}

	// Accept migration phase
	_, err = quorum.AcceptMigration(ctx, migration)
	if err != nil {
		return &WriteKeyResponse{
			Success: false,
			Error:   fmt.Sprintf("failed to accept migration: %v", err),
		}, nil
	}

	return &WriteKeyResponse{
		Success: true,
	}, nil
}
