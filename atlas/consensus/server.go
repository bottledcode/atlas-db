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
	"time"

	"github.com/bottledcode/atlas-db/atlas/kv"
	"github.com/bottledcode/atlas-db/atlas/options"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/timestamppb"
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

	if req.GetReason() == StealReason_queryReason {
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
		options.Logger.Warn("table not found, but expected", zap.String("table", req.GetMigration().GetVersion().GetTableName()))

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

	// Enforce ACL for write/delete operations using session principal
	principal := getPrincipalFromContext(ctx)
	for _, mig := range migrations {
		if d := mig.GetData(); d != nil {
			if ch := d.GetChange(); ch != nil {
				switch op := ch.GetOperation().(type) {
				case *KVChange_Set:
					// Check per-key ACL; missing ACL allows write (public)
					if metaStore != nil {
						haveAny := false
						// First, check OWNER ACL (implies full access)
						if aclVal, err := metaStore.Get(ctx, []byte(CreateACLKey(string(op.Set.Key)))); err == nil {
							haveAny = true
							if aclData, decErr := DecodeACLData(aclVal); decErr != nil {
								return nil, status.Errorf(codes.Internal, "ACL decode failed")
							} else if checkACLAccess(aclData, principal) {
								// Allowed by OWNER ACL
								break
							}
						} else if !errors.Is(err, kv.ErrKeyNotFound) {
							return nil, status.Errorf(codes.Internal, "ACL check failed: %v", err)
						}

						// Then, check WRITE-specific ACL
						if aclVal, err := metaStore.Get(ctx, []byte(CreateWriteACLKey(string(op.Set.Key)))); err == nil {
							haveAny = true
							if aclData, decErr := DecodeACLData(aclVal); decErr != nil {
								return nil, status.Errorf(codes.Internal, "ACL decode failed")
							} else if checkACLAccess(aclData, principal) {
								// Allowed by WRITE ACL
								break
							}
						} else if !errors.Is(err, kv.ErrKeyNotFound) {
							return nil, status.Errorf(codes.Internal, "ACL check failed: %v", err)
						}

						// If any ACLs existed but none allowed this principal, deny
						if haveAny {
							return nil, status.Errorf(codes.PermissionDenied, "write access denied")
						}
						// If no ACLs exist, allow (public)
					}
				case *KVChange_Del:
					// Check per-key ACL; missing ACL allows delete (public)
					if metaStore != nil {
						haveAny := false
						// First, check OWNER ACL
						if aclVal, err := metaStore.Get(ctx, []byte(CreateACLKey(string(op.Del.Key)))); err == nil {
							haveAny = true
							if aclData, decErr := DecodeACLData(aclVal); decErr != nil {
								return nil, status.Errorf(codes.Internal, "ACL decode failed")
							} else if checkACLAccess(aclData, principal) {
								break
							}
						} else if !errors.Is(err, kv.ErrKeyNotFound) {
							return nil, status.Errorf(codes.Internal, "ACL check failed: %v", err)
						}

						// Then, check WRITE-specific ACL (delete is a write)
						if aclVal, err := metaStore.Get(ctx, []byte(CreateWriteACLKey(string(op.Del.Key)))); err == nil {
							haveAny = true
							if aclData, decErr := DecodeACLData(aclVal); decErr != nil {
								return nil, status.Errorf(codes.Internal, "ACL decode failed")
							} else if checkACLAccess(aclData, principal) {
								break
							}
						} else if !errors.Is(err, kv.ErrKeyNotFound) {
							return nil, status.Errorf(codes.Internal, "ACL check failed: %v", err)
						}

						if haveAny {
							return nil, status.Errorf(codes.PermissionDenied, "delete access denied")
						}
					}
				}
			}
		}
	}

	err = s.applyMigration(migrations, kvStore)
	if err != nil {
		return nil, err
	}

	// After applying, set or clear ACL entries as needed
	for _, mig := range migrations {
		if d := mig.GetData(); d != nil {
			if ch := d.GetChange(); ch != nil {
				switch op := ch.GetOperation().(type) {
				case *KVChange_Set:
					if metaStore != nil && principal != "" {
						// Only set ACL if not present (creation) - use new multi-principal format
						aclKey := CreateACLKey(string(op.Set.Key))
						_, e := metaStore.Get(ctx, []byte(aclKey))
						if errors.Is(e, kv.ErrKeyNotFound) {
							// ACL not found - create initial ACL for this principal
							aclData, err := encodeACLData([]string{principal})
							if err != nil {
								options.Logger.Warn("Failed to encode ACL data after successful write",
									zap.String("key", string(op.Set.Key)),
									zap.String("principal", principal),
									zap.Error(err))
							} else if err := metaStore.Put(ctx, []byte(aclKey), aclData); err != nil {
								// Log error but don't fail the migration since data was already applied
								options.Logger.Warn("Failed to set ACL after successful write",
									zap.String("key", string(op.Set.Key)),
									zap.String("principal", principal),
									zap.Error(err))
							} else if options.CurrentOptions.DevelopmentMode {
								mv := mig.GetVersion()
								options.Logger.Info("ACL SET alongside migration",
									zap.String("data_key", string(op.Set.Key)),
									zap.String("acl_key", aclKey),
									zap.Strings("principals", []string{principal}),
									zap.Int64("table_version", mv.GetTableVersion()),
									zap.Int64("migration_version", mv.GetMigrationVersion()),
									zap.Int64("node_id", mv.GetNodeId()),
									zap.String("table", mv.GetTableName()),
								)
							}
						} else if e != nil {
							// Unexpected error checking ACL existence - log but don't fail since data was already applied
							options.Logger.Warn("Failed to check ACL existence after successful write",
								zap.String("key", string(op.Set.Key)),
								zap.Error(e))
						}
					}
				case *KVChange_Del:
					if metaStore != nil {
						// Delete all ACL entries (owner, read, write) for this key
						keys := []string{
							CreateACLKey(string(op.Del.Key)),
							CreateReadACLKey(string(op.Del.Key)),
							CreateWriteACLKey(string(op.Del.Key)),
						}
						for _, aclKey := range keys {
							if err := metaStore.Delete(ctx, []byte(aclKey)); err != nil {
								if errors.Is(err, kv.ErrKeyNotFound) {
									// Not found is fine; continue deleting remaining ACL keys
									continue
								}
								// Propagate other errors so callers can handle appropriately
								return nil, status.Errorf(codes.Internal, "failed to delete ACL key %s: %v", aclKey, err)
							}
							if options.CurrentOptions.DevelopmentMode {
								mv := mig.GetVersion()
								options.Logger.Info("ACL DEL alongside migration",
									zap.String("data_key", string(op.Del.Key)),
									zap.String("acl_key", aclKey),
									zap.Int64("table_version", mv.GetTableVersion()),
									zap.Int64("migration_version", mv.GetMigrationVersion()),
									zap.Int64("node_id", mv.GetNodeId()),
									zap.String("table", mv.GetTableName()),
								)
							}
						}
					}
				}
			}
		}
	}

	err = mr.CommitMigrationExact(req.GetMigration().GetVersion())
	if err != nil {
		return nil, err
	}

	return &emptypb.Empty{}, nil
}

func (s *Server) applyMigration(migrations []*Migration, kvStore kv.Store) error {
	for _, migration := range migrations {
		switch migration.GetMigration().(type) {
		case *Migration_Schema:
			// Schema migrations are not supported in KV mode
			// Skip silently for backward compatibility during transition
			options.Logger.Warn("Schema migration ignored in KV mode",
				zap.String("table", migration.GetVersion().GetTableName()))
			continue
		case *Migration_Data:
			err := s.applyKVDataMigration(migration, kvStore)
			if err != nil {
				return fmt.Errorf("failed to apply KV data migration: %w", err)
			}
		}
	}
	return nil
}

func (s *Server) applyKVDataMigration(migration *Migration, kvStore kv.Store) error {
	ctx := context.Background()
	dataMigration := migration.GetData()
	mv := migration.GetVersion()

	switch migrationType := dataMigration.GetSession().(type) {
	case *DataMigration_Change:
		switch op := migrationType.Change.GetOperation().(type) {
		case *KVChange_Set:
			err := kvStore.Put(ctx, op.Set.Key, op.Set.Value)
			if err != nil {
				return fmt.Errorf("failed to SET key %s: %w", op.Set.Key, err)
			}
			options.Logger.Info("Applied KV SET migration",
				zap.String("key", string(op.Set.Key)),
				zap.Int("value_size", len(op.Set.Value)),
				zap.Int64("table_version", mv.GetTableVersion()),
				zap.Int64("migration_version", mv.GetMigrationVersion()),
				zap.Int64("node_id", mv.GetNodeId()),
				zap.String("table", mv.GetTableName()),
			)
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
				zap.String("table", mv.GetTableName()),
			)
		default:
			return fmt.Errorf("unknown KV operation: %s", op)
		}
	case *DataMigration_RawData:
		sessionData := migrationType.RawData.GetData()
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
			// Parse as node data structure
			err := s.applyAddNodeOperation(ctx, sessionData, kvStore)
			if err != nil {
				return fmt.Errorf("failed to apply ADD_NODE operation: %w", err)
			}

		default:
			return fmt.Errorf("unknown KV raw operation: %s", operation)
		}
	}

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
	id, ok := nodeData["id"].(float64) // JSON unmarshals numbers as float64
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

	// Use NodeRepositoryKV to properly add the node with indexing
	nodeRepo := NewNodeRepositoryKV(ctx, kvStore)
	if kvRepo, ok := nodeRepo.(*NodeRepositoryKV); ok {
		err = kvRepo.AddNode(node)
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
	} else {
		return fmt.Errorf("failed to cast to NodeRepositoryKV")
	}

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
			NodeId:           options.CurrentOptions.ServerId,
			TableName:        NodeTable,
		},
		Migration: &Migration_Data{
			Data: &DataMigration{
				Session: &DataMigration_RawData{
					RawData: &RawData{
						Data: migrationData,
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
				_ = nodeRepo.Iterate(func(node *Node) error {
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
	// Check per-key ACL; missing ACL means public read allowed
	if metaStore != nil {
		principal := getPrincipalFromContext(ctx)
		haveAny := false
		allowed := false
		// Check OWNER ACL first (implies full access)
		if aclVal, err := metaStore.Get(ctx, []byte(CreateACLKey(req.GetKey()))); err == nil {
			haveAny = true
			if aclData, decErr := DecodeACLData(aclVal); decErr != nil {
				return &ReadKeyResponse{Success: false, Error: "ACL decode failed"}, nil
			} else if checkACLAccess(aclData, principal) {
				allowed = true
				if options.CurrentOptions.DevelopmentMode {
					options.Logger.Info("ACL allow (owner)", zap.String("key", req.GetKey()), zap.String("principal", principal))
				}
			} else if options.CurrentOptions.DevelopmentMode {
				options.Logger.Info("ACL miss (owner)", zap.String("key", req.GetKey()), zap.String("principal", principal))
			}
		} else if !errors.Is(err, kv.ErrKeyNotFound) {
			return &ReadKeyResponse{Success: false, Error: "ACL check failed"}, nil
		}

		// Then check READ-specific ACL only if not already allowed by owner
		if !allowed {
			if aclVal, err := metaStore.Get(ctx, []byte(CreateReadACLKey(req.GetKey()))); err == nil {
				haveAny = true
				if aclData, decErr := DecodeACLData(aclVal); decErr != nil {
					return &ReadKeyResponse{Success: false, Error: "ACL decode failed"}, nil
				} else if checkACLAccess(aclData, principal) {
					allowed = true
					if options.CurrentOptions.DevelopmentMode {
						options.Logger.Info("ACL allow (read)", zap.String("key", req.GetKey()), zap.String("principal", principal))
					}
				} else if options.CurrentOptions.DevelopmentMode {
					options.Logger.Info("ACL miss (read)", zap.String("key", req.GetKey()), zap.String("principal", principal))
				}
			} else if !errors.Is(err, kv.ErrKeyNotFound) {
				return &ReadKeyResponse{Success: false, Error: "ACL check failed"}, nil
			}
		}

		if haveAny && !allowed {
			if options.CurrentOptions.DevelopmentMode {
				options.Logger.Info("ACL deny (read)", zap.String("key", req.GetKey()), zap.String("principal", principal))
			}
			return &ReadKeyResponse{Success: false, Error: "access denied"}, nil
		}
		// If no ACLs found at all, allow public access
	}

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

	return &ReadKeyResponse{
		Success: true,
		Value:   value,
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
	nr := NewNodeRepositoryKV(ctx, metaStore)

	currentNode, err := nr.GetNodeById(options.CurrentOptions.ServerId)
	if err != nil {
		return &WriteKeyResponse{
			Success: false,
			Error:   fmt.Sprintf("failed to get current node: %v", err),
		}, nil
	}
	if currentNode == nil {
		return &WriteKeyResponse{
			Success: false,
			Error:   "node not yet part of cluster",
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
	mr := NewMigrationRepositoryKV(ctx, metaStore)
	version, err := mr.GetNextVersion(req.GetTable())
	if err != nil {
		return &WriteKeyResponse{
			Success: false,
			Error:   fmt.Sprintf("failed to get next version: %v", err),
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
						Change: &KVChange{
							Operation: &KVChange_Set{
								Set: &SetChange{
									Key:   []byte(req.GetKey()),
									Value: req.GetValue(),
								},
							},
						},
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
