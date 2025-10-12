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
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"sync"
	"time"

	"github.com/bottledcode/atlas-db/atlas/kv"
	"github.com/bottledcode/atlas-db/atlas/options"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

var NodeTable = KeyName("atlas.nodes")

type Server struct {
	UnimplementedConsensusServer
}

func NewServer() *Server {
	return &Server{}
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

	if existingTable == nil && req.GetReason() == StealReason_queryReason {
		return &StealTableOwnershipResponse{
			Promised: false,
			Response: &StealTableOwnershipResponse_Failure{
				Failure: &StealTableOwnershipFailure{
					Table: req.Table,
				},
			},
		}, nil
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
				err = errors.New("the new group must be empty")
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

	if req.GetReason() == StealReason_queryReason || req.GetReason() == StealReason_discoveryReason {
		return &StealTableOwnershipResponse{
			Promised: false,
			Response: &StealTableOwnershipResponse_Failure{
				Failure: &StealTableOwnershipFailure{
					Table: existingTable,
				},
			},
		}, nil
	}

	if existingTable.GetVersion() > req.GetTable().GetVersion() {
		options.Logger.Info(
			"the existing table version is higher than the requested version",
			zap.ByteString("table", existingTable.GetName()),
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
	dr := NewDataRepository(ctx, kvPool.DataStore())
	mr := NewMigrationRepositoryKV(ctx, metaStore, dr)
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
		options.Logger.Warn("the table isn't found, but expected", zap.ByteString("table", req.GetMigration().GetVersion().GetTableName()))

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
	dr := NewDataRepository(ctx, kvPool.DataStore())
	migrationRepo := NewMigrationRepositoryKV(ctx, metaStore, dr)
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

	if bytes.HasPrefix(req.GetMigration().GetVersion().GetTableName(), []byte("atlas.")) {
		// Use metadata store for atlas tables
		kvStore = metaStore
	} else {
		// Use data store for user tables
		kvStore = kvPool.DataStore()
		if kvStore == nil {
			return nil, fmt.Errorf("data store not available")
		}
	}

	dr := NewDataRepository(ctx, kvStore)
	mr := NewMigrationRepositoryKV(ctx, metaStore, dr)
	migrations, err := mr.GetMigrationVersion(req.GetMigration().GetVersion())
	if err != nil {
		return nil, err
	}

	err = s.applyMigration(ctx, migrations, kvStore)
	if err != nil {
		return nil, err
	}

	err = mr.CommitMigrationExact(req.GetMigration().GetVersion())
	if err != nil {
		return nil, err
	}

	return &emptypb.Empty{}, nil
}

func (s *Server) applyMigration(ctx context.Context, migrations []*Migration, kvStore kv.Store) error {
	for _, migration := range migrations {
		switch migration.GetMigration().(type) {
		case *Migration_Schema:
			// Schema migrations are not supported in KV mode
			// Skip silently for backward compatibility during transition
			options.Logger.Warn("Schema migration ignored in KV mode",
				zap.ByteString("table", migration.GetVersion().GetTableName()))
			continue
		case *Migration_Data:
			err := s.applyKVDataMigration(ctx, migration, kvStore)
			if err != nil {
				return fmt.Errorf("failed to apply KV data migration: %w", err)
			}
		}
	}
	return nil
}

func (s *Server) applyKVDataMigration(ctx context.Context, migration *Migration, kvStore kv.Store) error {
	dataMigration := migration.GetData()
	mv := migration.GetVersion()

	if halt, err := sender.maybeHandleMagicKey(ctx, migration); err != nil {
		return err
	} else if halt {
		return nil
	}

	switch migrationType := dataMigration.GetSession().(type) {
	case *DataMigration_Change:
		switch op := migrationType.Change.GetOperation().(type) {
		case *KVChange_Sub:
			err := DefaultNotificationSender().Notify(migration)
			if err != nil {
				return err
			}
		case *KVChange_Notify:
			sender.notification <- &notification{
				sub: op.Notify.Origin,
				pub: op.Notify,
			}
			sender.HandleNotifications()
		case *KVChange_Set:
			record := op.Set.Data
			value, err := proto.Marshal(record)
			if err != nil {
				return fmt.Errorf("failed to marshal KV record: %w", err)
			}
			err = kvStore.Put(ctx, op.Set.Key, value)
			if err != nil {
				return fmt.Errorf("failed to SET key %s: %w", op.Set.Key, err)
			}
			options.Logger.Info("Applied KV SET migration",
				zap.String("key", string(op.Set.Key)),
				zap.Int("value_size", len(value)),
				zap.Int64("table_version", mv.GetTableVersion()),
				zap.Int64("migration_version", mv.GetMigrationVersion()),
				zap.Int64("node_id", mv.GetNodeId()),
				zap.ByteString("table", mv.GetTableName()),
			)
			go func() {
				err := DefaultNotificationSender().Notify(DefaultNotificationSender().GenerateNotification(migration))
				if err != nil {
					options.Logger.Error("failed to notify migration", zap.Error(err))
				}
			}()
		case *KVChange_Del:
			err := kvStore.Delete(ctx, op.Del.Key)
			if err != nil {
				return fmt.Errorf("failed to DELETE key %s: %w", op.Del.Key, err)
			}
			options.Logger.Info("Applied KV DELETE migration",
				zap.String("key", string(op.Del.Key)),
				zap.Int64("table_version", mv.GetTableVersion()),
				zap.Int64("migration_version", mv.GetMigrationVersion()),
				zap.Int64("node_id", mv.GetNodeId()),
				zap.ByteString("table", mv.GetTableName()),
			)
			go func() {
				err := DefaultNotificationSender().Notify(DefaultNotificationSender().GenerateNotification(migration))
				if err != nil {
					options.Logger.Error("failed to notify migration", zap.Error(err))
				}
			}()
		case *KVChange_Data:
			sessionData := op.Data.GetData()
			var operationCheck map[string]any
			err := json.Unmarshal(sessionData, &operationCheck)
			if err != nil {
				return fmt.Errorf("failed to unmarshal operation data: %w", err)
			}
			operation, ok := operationCheck["operation"].(string)
			if !ok {
				return fmt.Errorf("missing or invalid operation field")
			}
			switch operation {
			case "ADD_NODE":
				err := s.applyAddNodeOperation(ctx, sessionData, kvStore)
				if err != nil {
					return fmt.Errorf("failed to apply the ADD_NODE operation: %w", err)
				}
			default:
				return fmt.Errorf("unknown KV operation: %s", operation)
			}
		case *KVChange_Acl:
			switch change := op.Acl.GetChange().(type) {
			case *AclChange_Addition:
				var record Record
				val, err := kvStore.Get(ctx, op.Acl.GetKey())
				if err != nil && !errors.Is(err, kv.ErrKeyNotFound) {
					return fmt.Errorf("failed to GET key %s: %w", op.Acl.GetKey(), err)
				}
				if err == nil {
					// Key exists - unmarshal existing record
					err = proto.Unmarshal(val, &record)
					if err != nil {
						return fmt.Errorf("failed to unmarshal ACL record: %w", err)
					}
				}
				// If key doesn't exist, record remains zero-value (empty record with ACL to be added)

				// Initialize AccessControl if it doesn't exist
				if record.AccessControl == nil {
					record.AccessControl = &ACL{}
				}
				now := timestamppb.Now()

				// Handle Owners
				if change.Addition.Owners != nil {
					if record.AccessControl.Owners == nil {
						record.AccessControl.Owners = &ACLData{
							Principals: []string{},
							CreatedAt:  now,
							UpdatedAt:  now,
						}
					}
					for _, r := range change.Addition.Owners.GetPrincipals() {
						if !slices.Contains(record.AccessControl.Owners.Principals, r) {
							record.AccessControl.Owners.Principals = append(record.AccessControl.Owners.Principals, r)
							record.AccessControl.Owners.UpdatedAt = now
						}
					}
				}

				// Handle Readers
				if change.Addition.Readers != nil {
					if record.AccessControl.Readers == nil {
						record.AccessControl.Readers = &ACLData{
							Principals: []string{},
							CreatedAt:  now,
							UpdatedAt:  now,
						}
					}
					for _, r := range change.Addition.Readers.GetPrincipals() {
						if !slices.Contains(record.AccessControl.Readers.Principals, r) {
							record.AccessControl.Readers.Principals = append(record.AccessControl.Readers.Principals, r)
							record.AccessControl.Readers.UpdatedAt = now
						}
					}
				}

				// Handle Writers
				if change.Addition.Writers != nil {
					if record.AccessControl.Writers == nil {
						record.AccessControl.Writers = &ACLData{
							Principals: []string{},
							CreatedAt:  now,
							UpdatedAt:  now,
						}
					}
					for _, r := range change.Addition.Writers.GetPrincipals() {
						if !slices.Contains(record.AccessControl.Writers.Principals, r) {
							record.AccessControl.Writers.Principals = append(record.AccessControl.Writers.Principals, r)
							record.AccessControl.Writers.UpdatedAt = now
						}
					}
				}
				value, err := proto.Marshal(&record)
				if err != nil {
					return fmt.Errorf("failed to marshal ACL record: %w", err)
				}
				err = kvStore.Put(ctx, op.Acl.GetKey(), value)
				if err != nil {
					return fmt.Errorf("failed to SET key %s: %w", op.Acl.GetKey(), err)
				}
				go func() {
					err := DefaultNotificationSender().Notify(DefaultNotificationSender().GenerateNotification(migration))
					if err != nil {
						options.Logger.Error("failed to notify migration", zap.Error(err))
					}
				}()
			case *AclChange_Deletion:
				var record Record
				val, err := kvStore.Get(ctx, op.Acl.GetKey())
				if err != nil {
					return fmt.Errorf("failed to GET key %s: %w", op.Acl.GetKey(), err)
				}
				err = proto.Unmarshal(val, &record)
				if err != nil {
					return fmt.Errorf("failed to unmarshal ACL record: %w", err)
				}

				// Only process deletion if AccessControl exists
				if record.AccessControl != nil {
					// Handle Owners deletion
					if change.Deletion.Owners != nil && record.AccessControl.Owners != nil {
						var next []string
						for _, r := range record.AccessControl.Owners.Principals {
							if !slices.Contains(change.Deletion.Owners.GetPrincipals(), r) {
								next = append(next, r)
							}
						}
						record.AccessControl.Owners.Principals = next
					}

					// Handle Readers deletion
					if change.Deletion.Readers != nil && record.AccessControl.Readers != nil {
						var nextRead []string
						for _, r := range record.AccessControl.Readers.Principals {
							if !slices.Contains(change.Deletion.Readers.GetPrincipals(), r) {
								nextRead = append(nextRead, r)
							}
						}
						record.AccessControl.Readers.Principals = nextRead
					}

					// Handle Writers deletion
					if change.Deletion.Writers != nil && record.AccessControl.Writers != nil {
						var nextWrite []string
						for _, r := range record.AccessControl.Writers.Principals {
							if !slices.Contains(change.Deletion.Writers.GetPrincipals(), r) {
								nextWrite = append(nextWrite, r)
							}
						}
						record.AccessControl.Writers.Principals = nextWrite
					}

					// If Owners is empty, check if we should remove entire ACL
					ownersEmpty := record.AccessControl.Owners == nil || len(record.AccessControl.Owners.Principals) == 0
					if ownersEmpty {
						// No owners means the record becomes public (remove entire ACL)
						record.AccessControl = nil
					}
				}
				value, err := proto.Marshal(&record)
				if err != nil {
					return fmt.Errorf("failed to marshal ACL record: %w", err)
				}
				err = kvStore.Put(ctx, op.Acl.GetKey(), value)
				if err != nil {
					return fmt.Errorf("failed to SET key %s: %w", op.Acl.GetKey(), err)
				}
				go func() {
					err := DefaultNotificationSender().Notify(DefaultNotificationSender().GenerateNotification(migration))
					if err != nil {
						options.Logger.Error("failed to notify migration", zap.Error(err))
					}
				}()
			}
		default:
			return fmt.Errorf("unknown KV operation: %s", op)
		}
	}

	DefaultNotificationSender().HandleNotifications()

	return nil
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

	// Immediately broadcast the new node to all known nodes to ensure they can forward requests
	// This avoids reliance on gossip propagation delays
	go func() {
		broadcastCtx := context.Background()
		nodeRepo := NewNodeRepository(broadcastCtx, metaStore)
		// Get all nodes to broadcast to
		err := nodeRepo.Iterate(false, func(node *Node, txn *kv.Transaction) error {
			// Skip the newly joined node and ourselves
			if node.GetId() == req.GetId() || node.GetId() == options.CurrentOptions.ServerId {
				return nil
			}

			// Send gossip to this node to immediately update its node list
			client, closer, err := getNewClient(fmt.Sprintf("%s:%d", node.GetAddress(), node.GetPort()))
			if err != nil {
				options.Logger.Warn("Failed to connect to node for immediate broadcast",
					zap.Int64("node_id", node.GetId()),
					zap.Error(err))
				return nil // Continue with other nodes
			}
			defer closer()

			gossipMig := &GossipMigration{
				MigrationRequest:  migration,
				Table:             nodeTable,
				PreviousMigration: nil, // This will be handled by gossip ordering
				Ttl:               0,    // Don't cascade further
				Sender:            constructCurrentNode(),
			}

			_, err = client.Gossip(broadcastCtx, gossipMig)
			if err != nil {
				options.Logger.Warn("Failed to broadcast new node to peer",
					zap.Int64("target_node_id", node.GetId()),
					zap.Int64("new_node_id", req.GetId()),
					zap.Error(err))
			} else {
				options.Logger.Info("Successfully broadcast new node to peer",
					zap.Int64("target_node_id", node.GetId()),
					zap.Int64("new_node_id", req.GetId()))
			}
			return nil
		})
		if err != nil {
			options.Logger.Error("Failed to iterate nodes for broadcast", zap.Error(err))
		}
	}()

	return &JoinClusterResponse{
		Success: true,
		NodeId:  req.GetId(),
	}, nil
}

var gossipQueue sync.Map

type gossipKey struct {
	table        KeyName
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

	if bytes.HasPrefix(req.GetMigrationRequest().GetVersion().GetTableName(), KeyName("atlas.")) {
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
	err = s.applyMigration(ctx, []*Migration{req.GetMigrationRequest()}, kvStore)
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

	options.Logger.Info("gossip complete", zap.ByteString("table", req.GetTable().GetName()), zap.Int64("version", req.GetTable().GetVersion()))

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
	table, err := tr.GetTable(req.GetKey())
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
	if req.GetPrefix() == nil {
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
		zap.ByteString("table prefix", req.GetPrefix()),
		zap.Int64("node_id", currentNode.Id))

	keyPrefix := req.GetPrefix()

	matchingKeys, err := dataStore.PrefixScan(ctx, keyPrefix)
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

	var ownedKeys [][]byte
	var record Record
	for key, val := range matchingKeys {
		err = proto.Unmarshal(val, &record)
		if err != nil {
			return nil, err
		}
		if canRead(ctx, &record) {
			ownedKeys = append(ownedKeys, []byte(key))
		}
	}

	options.Logger.Info("PrefixScan completed",
		zap.ByteString("table prefix", req.GetPrefix()),
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
