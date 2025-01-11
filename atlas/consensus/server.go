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
	"errors"
	"fmt"
	"github.com/bottledcode/atlas-db/atlas"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/emptypb"
	"strings"
	"sync"
	"zombiezen.com/go/sqlite"
)

const NodeTable = "atlas.nodes"

type Server struct {
	UnimplementedConsensusServer
}

func (s *Server) StealTableOwnership(ctx context.Context, req *StealTableOwnershipRequest) (*StealTableOwnershipResponse, error) {
	conn, err := atlas.MigrationsPool.Take(ctx)
	if err != nil {
		return nil, err
	}
	defer func() {
		if err != nil {
			_, _ = atlas.ExecuteSQL(ctx, "ROLLBACK", conn, false)
		}
		atlas.MigrationsPool.Put(conn)
	}()

	_, err = atlas.ExecuteSQL(ctx, "BEGIN IMMEDIATE", conn, false)

	tableRepo := GetDefaultTableRepository(ctx, conn)
	existingTable, err := tableRepo.GetTable(req.GetTable().GetName())
	if err != nil {
		return nil, err
	}

	if existingTable == nil {
		// this is a new table...
		err = tableRepo.InsertTable(req.GetTable())
		if err != nil {
			return nil, err
		}

		_, err = atlas.ExecuteSQL(ctx, "COMMIT", conn, false)
		if err != nil {
			return nil, err
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

		_, err = atlas.ExecuteSQL(ctx, "ROLLBACK", conn, false)
		if err != nil {
			return nil, err
		}

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
				_, err = atlas.ExecuteSQL(ctx, "ROLLBACK", conn, false)
				if err != nil {
					return nil, err
				}

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

	atlas.Ownership.Remove(req.GetTable().GetName())
	err = tableRepo.UpdateTable(req.GetTable())
	if err != nil {
		return nil, err
	}

	migrationRepo := GetDefaultMigrationRepository(ctx, conn)

	missing, err := migrationRepo.GetUncommittedMigrations(req.GetTable())
	if err != nil {
		return nil, err
	}

	_, err = atlas.ExecuteSQL(ctx, "COMMIT", conn, false)
	if err != nil {
		return nil, err
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
func (s *Server) WriteMigration(ctx context.Context, req *WriteMigrationRequest) (*WriteMigrationResponse, error) {
	conn, err := atlas.MigrationsPool.Take(ctx)
	if err != nil {
		return nil, err
	}
	defer func() {
		if err != nil {
			_, _ = atlas.ExecuteSQL(ctx, "ROLLBACK", conn, false)
		}
		atlas.MigrationsPool.Put(conn)
	}()

	_, err = atlas.ExecuteSQL(ctx, "BEGIN IMMEDIATE", conn, false)
	if err != nil {
		return nil, err
	}

	tableRepo := GetDefaultTableRepository(ctx, conn)
	existingTable, err := tableRepo.GetTable(req.GetMigration().GetVersion().GetTableName())
	if err != nil {
		return nil, err
	}

	if existingTable == nil {
		atlas.Logger.Warn("table not found, but expected", zap.String("table", req.GetMigration().GetVersion().GetTableName()))
		_, err = atlas.ExecuteSQL(ctx, "ROLLBACK", conn, false)
		if err != nil {
			return nil, err
		}

		return &WriteMigrationResponse{
			Success: false,
			Table:   nil,
		}, fmt.Errorf("table %s not found", req.GetMigration().GetVersion().GetTableName())
	}

	if existingTable.GetVersion() > req.GetMigration().GetVersion().GetTableVersion() {
		// the table version is higher than the requested version, so reject the migration since it isn't the owner
		_, err = atlas.ExecuteSQL(ctx, "ROLLBACK", conn, false)
		if err != nil {
			return nil, err
		}

		return &WriteMigrationResponse{
			Success: false,
			Table:   existingTable,
		}, nil
	} else if existingTable.GetOwner() != nil && existingTable.GetVersion() == req.GetMigration().GetVersion().GetTableVersion() && existingTable.GetOwner().GetId() != req.GetSender().GetId() {
		// the table version is the same, but the owner is different, so reject the migration
		_, err = atlas.ExecuteSQL(ctx, "ROLLBACK", conn, false)
		if err != nil {
			return nil, err
		}

		return &WriteMigrationResponse{
			Success: false,
			Table:   existingTable,
		}, nil
	}

	// insert the migration
	migrationRepo := GetDefaultMigrationRepository(ctx, conn)
	err = migrationRepo.AddMigration(req.GetMigration())
	if err != nil {
		return nil, err
	}

	_, err = atlas.ExecuteSQL(ctx, "COMMIT", conn, false)
	if err != nil {
		return nil, err
	}

	return &WriteMigrationResponse{
		Success: true,
		Table:   nil,
	}, nil
}

func (s *Server) AcceptMigration(ctx context.Context, req *WriteMigrationRequest) (*emptypb.Empty, error) {
	conn, err := atlas.MigrationsPool.Take(ctx)
	if err != nil {
		return nil, err
	}
	defer func() {
		if err != nil {
			_, _ = atlas.ExecuteSQL(ctx, "ROLLBACK", conn, false)
		}
		atlas.MigrationsPool.Put(conn)
	}()

	commitConn := conn
	if !strings.HasPrefix(req.GetMigration().GetVersion().GetTableName(), "atlas.") {
		commitConn, err = atlas.Pool.Take(ctx)
		if err != nil {
			return nil, err
		}
		defer func() {
			if err != nil {
				_, _ = atlas.ExecuteSQL(ctx, "ROLLBACK", commitConn, false)
			}
			atlas.Pool.Put(commitConn)
		}()
	}

	_, err = atlas.ExecuteSQL(ctx, "BEGIN IMMEDIATE", conn, false)
	if err != nil {
		return nil, err
	}

	mr := GetDefaultMigrationRepository(ctx, conn)
	migrations, err := mr.GetMigrationVersion(req.GetMigration().GetVersion())
	if err != nil {
		return nil, err
	}

	err = s.applyMigration(migrations, commitConn)
	if err != nil {
		return nil, err
	}

	err = mr.CommitMigrationExact(req.GetMigration().GetVersion())
	if err != nil {
		return nil, err
	}

	if commitConn != conn {
		_, err = atlas.ExecuteSQL(ctx, "COMMIT", commitConn, false)
		if err != nil {
			return nil, err
		}
	}

	_, err = atlas.ExecuteSQL(ctx, "COMMIT", conn, false)
	if err != nil {
		return nil, err
	}

	atlas.Ownership.Commit(req.GetMigration().GetVersion().GetTableName(), req.GetMigration().GetVersion().GetTableVersion())

	return &emptypb.Empty{}, nil
}

func (s *Server) applyMigration(migrations []*Migration, commitConn *sqlite.Conn) error {
	for _, migration := range migrations {
		switch migration.GetMigration().(type) {
		case *Migration_Schema:
			for _, command := range migration.GetSchema().GetCommands() {
				stmt, _, err := commitConn.PrepareTransient(command)
				if err != nil {
					return err
				}
				_, err = stmt.Step()
				if err != nil {
					return err
				}
				err = stmt.Finalize()
				if err != nil {
					return err
				}
			}
		case *Migration_Data:
			for _, data := range migration.GetData().GetSession() {
				reader := bytes.NewReader(data)
				err := commitConn.ApplyChangeset(reader, nil, func(conflictType sqlite.ConflictType, iterator *sqlite.ChangesetIterator) sqlite.ConflictAction {
					// todo: handle conflicts
					return sqlite.ChangesetReplace
				})
				if err != nil {
					return err
				}
			}
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
	conn, err := atlas.MigrationsPool.Take(ctx)
	if err != nil {
		return nil, err
	}
	defer func() {
		if err != nil {
			_, _ = atlas.ExecuteSQL(ctx, "ROLLBACK", conn, false)
		}
		atlas.MigrationsPool.Put(conn)
	}()

	_, err = atlas.ExecuteSQL(ctx, "BEGIN IMMEDIATE", conn, false)
	if err != nil {
		return nil, err
	}

	// check if we currently are the owner for node configuration
	tableRepo := GetDefaultTableRepository(ctx, conn)
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

	// add the node to the cluster as a migration to be replicated
	sess, err := conn.CreateSession("")
	if err != nil {
		return nil, err
	}
	defer func() {
		if sess != nil {
			sess.Delete()
		}
	}()
	err = sess.Attach("nodes")
	if err != nil {
		return nil, err
	}

	_, err = atlas.ExecuteSQL(ctx, `
insert into nodes (id, address, port, region, active, created_at, rtt)
VALUES (:id, :address, :port, :region, 1, current_timestamp, 0)`, conn, false, atlas.Param{
		Name:  "id",
		Value: req.GetId(),
	}, atlas.Param{
		Name:  "address",
		Value: req.GetAddress(),
	}, atlas.Param{
		Name:  "port",
		Value: req.GetPort(),
	}, atlas.Param{
		Name:  "region",
		Value: req.GetRegion().GetName(),
	})
	if err != nil {
		return nil, err
	}

	migrationData := bytes.Buffer{}
	err = sess.WritePatchset(&migrationData)
	if err != nil {
		return nil, err
	}

	_, err = atlas.ExecuteSQL(ctx, "ROLLBACK", conn, false)
	if err != nil {
		return nil, err
	}

	tr := GetDefaultTableRepository(ctx, conn)
	nodeTable, err := tr.GetTable(NodeTable)
	if err != nil {
		return nil, err
	}

	mr := GetDefaultMigrationRepository(ctx, conn)
	nextVersion, err := mr.GetNextVersion(NodeTable)

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
					migrationData.Bytes(),
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
func (s *Server) applyGossipMigration(ctx context.Context, req *GossipMigration, conn *sqlite.Conn, commitConn *sqlite.Conn) error {
	mr := GetDefaultMigrationRepository(ctx, conn)

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
			err = s.applyGossipMigration(ctx, prev.(*GossipMigration), conn, commitConn)
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

	// we have a previous migration, so apply this one
	err = s.applyMigration([]*Migration{req.GetMigrationRequest()}, commitConn)
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

func SendGossip(ctx context.Context, req *GossipMigration, conn *sqlite.Conn) error {
	// now we see if there is still a ttl remaining
	if req.GetTtl() <= 0 {
		return nil
	}

	// pick 5 random nodes to gossip to, excluding the sender, owner, and myself
	nr := GetDefaultNodeRepository(ctx, conn)
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
	// determine if we have already applied this data or not
	conn, err := atlas.MigrationsPool.Take(ctx)
	if err != nil {
		return nil, err
	}
	defer atlas.MigrationsPool.Put(conn)

	// rollback on err
	defer func() {
		if err != nil {
			_, _ = atlas.ExecuteSQL(ctx, "ROLLBACK", conn, false)
		}
	}()

	commitConn := conn
	if strings.HasPrefix(req.GetTable().GetName(), "atlas.") {
		commitConn, err = atlas.Pool.Take(ctx)
		if err != nil {
			return nil, err
		}
		defer atlas.Pool.Put(commitConn)

		_, err = atlas.ExecuteSQL(ctx, "BEGIN IMMEDIATE", commitConn, false)
		if err != nil {
			return nil, err
		}

		defer func() {
			if err != nil {
				_, _ = atlas.ExecuteSQL(ctx, "ROLLBACK", commitConn, false)
			}
		}()
	}

	_, err = atlas.ExecuteSQL(ctx, "BEGIN IMMEDIATE", conn, false)
	if err != nil {
		return nil, err
	}

	tr := GetDefaultTableRepository(ctx, conn)

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
	err = s.applyGossipMigration(ctx, req, conn, commitConn)
	if err != nil {
		return nil, err
	}

	if commitConn != conn {
		_, err = atlas.ExecuteSQL(ctx, "COMMIT", commitConn, false)
		if err != nil {
			return nil, err
		}
	}

	_, err = atlas.ExecuteSQL(ctx, "COMMIT", conn, false)
	if err != nil {
		return nil, err
	}

	// gossip to other nodes
	err = SendGossip(ctx, req, conn)
	if err != nil {
		return nil, err
	}

	return &emptypb.Empty{}, nil
}
