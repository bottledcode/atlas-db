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
	"strings"
	"sync"

	"github.com/bottledcode/atlas-db/atlas"
	"github.com/bottledcode/atlas-db/atlas/kv"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/emptypb"
)

const NodeTable = "atlas.nodes"

type Server struct {
	UnimplementedConsensusServer
}

func (s *Server) StealTableOwnership(ctx context.Context, req *StealTableOwnershipRequest) (*StealTableOwnershipResponse, error) {
	// Get KV store for metadata operations
	kvPool := kv.GetPool()
	if kvPool == nil {
		return nil, fmt.Errorf("KV pool not initialized")
	}

	metaStore := kvPool.MetaStore()
	if metaStore == nil {
		return nil, fmt.Errorf("metadata store not available")
	}

	tr := NewTableRepositoryKV(ctx, metaStore)
	existingTable, err := tr.GetTable(req.GetTable().GetName())
	if err != nil {
		return nil, err
	}

	if existingTable == nil {
		// this is a new table...
		err = tr.InsertTable(req.GetTable())
		if err != nil {
			return nil, err
		}

		if req.GetTable().GetType() == TableType_group {
			// ensure the group is empty
			var group *TableGroup
			group, err = tr.GetGroup(req.GetTable().GetName())
			if err != nil {
				return nil, err
			}

			// a new group must be empty
			if len(group.GetTables()) > 0 {
				err = errors.New("new group must be empty")
				return nil, err
			}
		}

		return &StealTableOwnershipResponse{
			Promised: true,
			Response: &StealTableOwnershipResponse_Success{
				Success: &StealTableOwnershipSuccess{
					Table:             req.Table,
					MissingMigrations: make([]*Migration, 0),
				},
			},
		}, nil
	}

	if existingTable.GetVersion() > req.GetTable().GetVersion() {
		atlas.Logger.Info(
			"the existing table version is higher than the requested version",
			zap.String("table", existingTable.GetName()),
			zap.Int64("existing_version", existingTable.GetVersion()),
			zap.Int64("requested_version", req.GetTable().GetVersion()),
		)

		// the ballot number is lower, so reject the steal
		return &StealTableOwnershipResponse{
			Promised: false,
			Response: &StealTableOwnershipResponse_Failure{
				Failure: &StealTableOwnershipFailure{
					Table: existingTable,
				},
			},
		}, nil
	}

	if existingTable.GetVersion() == req.GetTable().GetVersion() {
		// the ballot number is the same, so compare the node ids of the current stealers and the existing owner
		if existingTable.GetOwner() != nil && req.GetTable().GetOwner() != nil {
			if existingTable.GetOwner().GetId() > req.GetTable().GetOwner().GetId() {
				return &StealTableOwnershipResponse{
					Promised: false,
					Response: &StealTableOwnershipResponse_Failure{
						Failure: &StealTableOwnershipFailure{
							Table: existingTable,
						},
					},
				}, nil
			}
		}

		// the ballot number is the same, and the new owner wins by node id
	}

	// the ballot number is higher

	// if this table is a group, all tables in the group must be stolen
	var missing []*Migration
	mr := NewMigrationRepositoryKV(ctx, metaStore)
	if existingTable.GetType() == TableType_group {

		// first we update the table to the new owner
		err = tr.UpdateTable(req.GetTable())
		if err != nil {
			return nil, err
		}

		// then retrieve the group membership
		var group *TableGroup
		group, err = tr.GetGroup(existingTable.GetName())
		if err != nil {
			return nil, err
		}

		// and steal each table in the group
		var m []*Migration
		for _, t := range group.GetTables() {
			m, err = s.stealTableOperation(tr, mr, t)
			if err != nil {
				return nil, err
			}
			missing = append(missing, m...)
		}
	} else {
		missing, err = s.stealTableOperation(tr, mr, req.GetTable())
		if err != nil {
			return nil, err
		}
	}

	return &StealTableOwnershipResponse{
		Promised: true,
		Response: &StealTableOwnershipResponse_Success{
			Success: &StealTableOwnershipSuccess{
				Table:             req.Table,
				MissingMigrations: missing,
			},
		},
	}, nil
}

func (s *Server) stealTableOperation(tr TableRepository, mr MigrationRepository, table *Table) ([]*Migration, error) {
	atlas.Ownership.Remove(table.GetName())
	err := tr.UpdateTable(table)
	if err != nil {
		return nil, err
	}

	missing, err := mr.GetUncommittedMigrations(table)
	if err != nil {
		return nil, err
	}

	return missing, nil
}

func (s *Server) WriteMigration(ctx context.Context, req *WriteMigrationRequest) (*WriteMigrationResponse, error) {
	// Get KV store for metadata operations
	kvPool := kv.GetPool()
	if kvPool == nil {
		return nil, fmt.Errorf("KV pool not initialized")
	}

	metaStore := kvPool.MetaStore()
	if metaStore == nil {
		return nil, fmt.Errorf("metadata store not available")
	}

	tableRepo := NewTableRepositoryKV(ctx, metaStore)
	existingTable, err := tableRepo.GetTable(req.GetMigration().GetVersion().GetTableName())
	if err != nil {
		return nil, err
	}

	if existingTable == nil {
		atlas.Logger.Warn("table not found, but expected", zap.String("table", req.GetMigration().GetVersion().GetTableName()))

		return &WriteMigrationResponse{
			Success: false,
			Table:   nil,
		}, fmt.Errorf("table %s not found", req.GetMigration().GetVersion().GetTableName())
	}

	if existingTable.GetVersion() > req.GetMigration().GetVersion().GetTableVersion() {
		// the table version is higher than the requested version, so reject the migration since it isn't the owner
		return &WriteMigrationResponse{
			Success: false,
			Table:   existingTable,
		}, nil
	} else if existingTable.GetOwner() != nil && existingTable.GetVersion() == req.GetMigration().GetVersion().GetTableVersion() && existingTable.GetOwner().GetId() != req.GetSender().GetId() {
		// the table version is the same, but the owner is different, so reject the migration
		return &WriteMigrationResponse{
			Success: false,
			Table:   existingTable,
		}, nil
	}

	// insert the migration
	migrationRepo := NewMigrationRepositoryKV(ctx, metaStore)
	err = migrationRepo.AddMigration(req.GetMigration())
	if err != nil {
		return nil, err
	}

	return &WriteMigrationResponse{
		Success: true,
		Table:   nil,
	}, nil
}

func (s *Server) AcceptMigration(ctx context.Context, req *WriteMigrationRequest) (*emptypb.Empty, error) {
	// Get the appropriate KV store for the migration
	var kvStore kv.Store
	kvPool := kv.GetPool()
	if kvPool == nil {
		return nil, fmt.Errorf("KV pool not initialized")
	}

	// Use metadata store for consensus operations
	metaStore := kvPool.MetaStore()
	if metaStore == nil {
		return nil, fmt.Errorf("metadata store not available")
	}

	if strings.HasPrefix(req.GetMigration().GetVersion().GetTableName(), "atlas.") {
		// Use metadata store for atlas tables
		kvStore = metaStore
	} else {
		// Use data store for user tables
		kvStore = kvPool.DataStore()
		if kvStore == nil {
			return nil, fmt.Errorf("data store not available")
		}
	}

	mr := NewMigrationRepositoryKV(ctx, metaStore)
	migrations, err := mr.GetMigrationVersion(req.GetMigration().GetVersion())
	if err != nil {
		return nil, err
	}

	err = s.applyMigration(migrations, kvStore)
	if err != nil {
		return nil, err
	}

	err = mr.CommitMigrationExact(req.GetMigration().GetVersion())
	if err != nil {
		return nil, err
	}

	atlas.Ownership.Commit(req.GetMigration().GetVersion().GetTableName(), req.GetMigration().GetVersion().GetTableVersion())

	return &emptypb.Empty{}, nil
}

func (s *Server) applyMigration(migrations []*Migration, kvStore kv.Store) error {
	for _, migration := range migrations {
		switch migration.GetMigration().(type) {
		case *Migration_Schema:
			// Schema migrations are not supported in KV mode
			// Skip silently for backward compatibility during transition
			atlas.Logger.Warn("Schema migration ignored in KV mode", 
				zap.String("table", migration.GetVersion().GetTableName()))
			continue
		case *Migration_Data:
			err := s.applyKVDataMigration(migration.GetData(), kvStore)
			if err != nil {
				return fmt.Errorf("failed to apply KV data migration: %w", err)
			}
		}
	}
	return nil
}

func (s *Server) applyKVDataMigration(dataMigration *DataMigration, kvStore kv.Store) error {
	for _, sessionData := range dataMigration.GetSession() {
		// Parse the KV change from the session data
		var kvChange KVChange
		err := json.Unmarshal(sessionData, &kvChange)
		if err != nil {
			return fmt.Errorf("failed to unmarshal KV change: %w", err)
		}

		// Apply the KV operation to the store
		ctx := context.Background()
		keyBytes := []byte(kvChange.Key)

		switch kvChange.Operation {
		case "SET":
			err = kvStore.Put(ctx, keyBytes, kvChange.Value)
			if err != nil {
				return fmt.Errorf("failed to SET key %s: %w", kvChange.Key, err)
			}
			atlas.Logger.Debug("Applied KV SET migration", 
				zap.String("key", kvChange.Key),
				zap.Int("value_size", len(kvChange.Value)))

		case "DEL":
			err = kvStore.Delete(ctx, keyBytes)
			if err != nil {
				return fmt.Errorf("failed to DELETE key %s: %w", kvChange.Key, err)
			}
			atlas.Logger.Debug("Applied KV DELETE migration", zap.String("key", kvChange.Key))

		default:
			return fmt.Errorf("unknown KV operation: %s", kvChange.Operation)
		}
	}
	return nil
}

func constructCurrentNode() *Node {
	return &Node{
		Id:      atlas.CurrentOptions.ServerId,
		Address: atlas.CurrentOptions.AdvertiseAddress,
		Port:    int64(atlas.CurrentOptions.AdvertisePort),
		Region: &Region{
			Name: atlas.CurrentOptions.Region,
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

	if table.GetOwner().GetId() != atlas.CurrentOptions.ServerId {
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

	// Create KV change for adding the node (replaces SQLite session)
	nodeChange := map[string]interface{}{
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
	mr := NewMigrationRepositoryKV(ctx, metaStore)
	nextVersion, err := mr.GetNextVersion(NodeTable)
	if err != nil {
		return nil, fmt.Errorf("failed to get next migration version: %w", err)
	}

	// create a new migration
	migration := &Migration{
		Version: &MigrationVersion{
			TableVersion:     nodeTable.GetVersion(),
			MigrationVersion: nextVersion,
			NodeId:           atlas.CurrentOptions.ServerId,
			TableName:        NodeTable,
		},
		Migration: &Migration_Data{
			Data: &DataMigration{
				Session: [][]byte{
					migrationData,
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

	mr := NewMigrationRepositoryKV(ctx, metaStore)

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
	nr := NewNodeRepositoryKV(ctx, kvStore)
	nodes, err := nr.GetRandomNodes(5,
		req.GetMigrationRequest().GetVersion().GetNodeId(),
		atlas.CurrentOptions.ServerId,
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

			client, err, closer := getNewClient(fmt.Sprintf("%s:%d", node.GetAddress(), node.GetPort()))
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

	atlas.Logger.Info("gossip complete", zap.String("table", req.GetTable().GetName()), zap.Int64("version", req.GetTable().GetVersion()))

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
