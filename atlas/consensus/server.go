package consensus

import (
	"bytes"
	"context"
	"github.com/bottledcode/atlas-db/atlas"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
	"strings"
	"zombiezen.com/go/sqlite"
)

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

	err = tableRepo.UpdateTable(req.GetTable())
	if err != nil {
		return nil, err
	}

	// remove the table from ownership
	err = tableRepo.RemoveOwnership(existingTable)

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
	existingTable, err := tableRepo.GetTable(req.GetTableId())

	if existingTable.GetVersion() > req.GetTableVersion() {

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
	err = migrationRepo.AddMigration(req.GetMigration(), req.GetSender())
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
	if !strings.HasPrefix(req.GetTableId(), "atlas.") {
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
	migrations, err := mr.GetMigrationVersion(req.GetTableId(), req.GetMigration().GetVersion())
	if err != nil {
		return nil, err
	}

	var stmt *sqlite.Stmt
	for _, migration := range migrations {
		switch migration.GetMigration().(type) {
		case *Migration_Schema:
			for _, command := range migration.GetSchema().GetCommands() {
				stmt, _, err = commitConn.PrepareTransient(command)
				if err != nil {
					return nil, err
				}
				_, err = stmt.Step()
				if err != nil {
					return nil, err
				}
				err = stmt.Finalize()
				if err != nil {
					return nil, err
				}
			}
		case *Migration_Data:
			for _, data := range migration.GetData().GetSession() {
				reader := bytes.NewReader(data)
				err = commitConn.ApplyChangeset(reader, nil, func(conflictType sqlite.ConflictType, iterator *sqlite.ChangesetIterator) sqlite.ConflictAction {
					// todo: handle conflicts
					return sqlite.ChangesetReplace
				})
				if err != nil {
					return nil, err
				}
			}
		}
	}

	err = mr.CommitMigration(req.GetTableId(), req.GetMigration().GetVersion())
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

	return &emptypb.Empty{}, nil
}
func (s *Server) LearnMigration(*LearnMigrationRequest, Consensus_LearnMigrationServer) error {
	return status.Errorf(codes.Unimplemented, "method LearnMigration not implemented")
}
