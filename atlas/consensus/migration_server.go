package consensus

import (
	"context"
	"errors"
	"fmt"
	"github.com/bottledcode/atlas-db/atlas"
	"google.golang.org/protobuf/types/known/timestamppb"
	"slices"
	"strings"
)

func (t *Table) FromSqlRow(row *atlas.Row) error {
	switch row.GetColumn("replication_level").GetString() {
	case "global":
		t.ReplicationLevel = ReplicationLevel_global
	case "regional":
		t.ReplicationLevel = ReplicationLevel_regional
	case "local":
		t.ReplicationLevel = ReplicationLevel_local
	}

	t.Id = row.GetColumn("id").GetInt()
	t.Name = row.GetColumn("table_name").GetString()
	t.Version = row.GetColumn("version").GetInt()
	t.CreatedAt = timestamppb.New(row.GetColumn("created_at").GetTime())
	t.GlobalOwner = &Node{
		NodeId: row.GetColumn("owner_node_id").GetInt(),
	}
	t.AllowedRegions = strings.Split(row.GetColumn("allowed_regions").GetString(), ",")

	return nil
}

func (n *Node) FromSqlRow(row *atlas.Row) error {
	n.NodeId = row.GetColumn("id").GetInt()
	n.NodeAddress = row.GetColumn("address").GetString()
	n.NodePort = row.GetColumn("port").GetInt()
	n.NodeRegion = row.GetColumn("region").GetString()
	n.IsActive = row.GetColumn("active").GetBool()

	return nil
}

var ErrCannotChangeReplicationLevel = errors.New("cannot change replication level of a table")
var ErrTablePolicyViolation = errors.New("table policy violation")

// StealTableOwnership is an attempt to steal ownership of a table. New tables are automatically granted ownership
// immediately, while old tables must go through a paxos process to achieve consensus.
func (s *Server) StealTableOwnership(ctx context.Context, req *StealTableOwnershipRequest) (*StealTableOwnershipResponse, error) {
	// Determine if this is for an existing table or a new table,
	// if it is for a new table, this will be straightforward.
	conn, err := atlas.MigrationsPool.Take(ctx)
	if err != nil {
		return nil, err
	}
	defer atlas.MigrationsPool.Put(conn)

	// Start our transaction and lock the database, as well as set up a rollback on error.
	_, err = atlas.ExecuteSQL(ctx, "begin immediate", conn, false)
	if err != nil {
		return nil, err
	}
	defer func() {
		if err != nil {
			_, _ = atlas.ExecuteSQL(ctx, "rollback", conn, false)
		}
	}()

	// Request the existing table entry from the database
	results, err := atlas.ExecuteSQL(ctx, `
select (id, replication_level, table_name, created_at, owner_node_id, version, allowed_regions)
from tables
where id = :id
`, conn, false, atlas.Param{
		Name:  "id",
		Value: req.GetTable().GetId(),
	})
	if err != nil {
		return nil, err
	}

	var ownerNodeId *int64

	if req.GetTable().GetGlobalOwner() != nil {
		id := req.GetTable().GetGlobalOwner().GetNodeId()
		ownerNodeId = &id
	}

	// If the table entry does not exist, we can automatically grant ownership to the requesting node.
	if results.Empty() {
		_, err = atlas.ExecuteSQL(
			ctx,
			`
insert into tables (id, table_name, replication_level, owner_node_id, created_at)
values (:id, :table_name, :replication_level, :owner_node_id, :created_at)
`,
			conn,
			false,
			atlas.Param{
				Name:  "table_name",
				Value: req.GetTable().GetName(),
			}, atlas.Param{
				Name:  "replication_level",
				Value: req.GetTable().GetReplicationLevel().String(),
			}, atlas.Param{
				Name:  "owner_node_id",
				Value: ownerNodeId,
			}, atlas.Param{
				Name:  "created_at",
				Value: req.GetTable().GetCreatedAt().AsTime(),
			}, atlas.Param{
				Name:  "id",
				Value: req.GetTable().GetId(),
			})
		if err != nil {
			return nil, err
		}

		return &StealTableOwnershipResponse{
			Success: true,
			Table:   req.GetTable(),
		}, nil
	}

	// This is an existing table, so we need to go through the paxos process -- this is a Prepare step.
	existingTable := Table{}
	err = existingTable.FromSqlRow(results.GetIndex(0))
	if err != nil {
		return nil, err
	}

	// It is an error to change the replication level of a table.
	if existingTable.ReplicationLevel != req.GetTable().GetReplicationLevel() {
		return nil, ErrCannotChangeReplicationLevel
	}

	if existingTable.GetReplicationLevel() == ReplicationLevel_local && req.Reason != StealReason_SCHEMA_MIGRATION {
		// Local tables can only be stolen for schema migration purposes.
		return nil, fmt.Errorf("cannot steal local table for reason %s: %w", req.Reason.String(), ErrTablePolicyViolation)
	}

	// All global tables belong to a single owner.
	//This owner can be the only writer.
	// Further, if a node wants to perform a schema migration, it must become the owner of all data.
	// This is a w-paxos process.
	if existingTable.GetReplicationLevel() == ReplicationLevel_global || req.Reason == StealReason_SCHEMA_MIGRATION {
		// retrieve the current owner
		results, err = atlas.ExecuteSQL(
			ctx,
			`
select nodes.id as id, active, r.name as region, port, address
from nodes
         inner join main.regions r on nodes.region_id = r.id
where nodes.id = :id
`,
			conn,
			false,
			atlas.Param{
				Name:  "id",
				Value: existingTable.GetGlobalOwner().GetNodeId(),
			},
		)
		err = existingTable.GlobalOwner.FromSqlRow(results.GetIndex(0))
		if err != nil {
			return nil, err
		}

		// we need to check if the owner node is the same as the one requesting ownership
		if existingTable.GetGlobalOwner().GetNodeId() == req.GetTable().GetGlobalOwner().GetNodeId() {
			return &StealTableOwnershipResponse{
				Success: true,
				Table:   req.GetTable(),
			}, nil
		}

		// now we check the table version, which functions as a ballot number for this table
		if existingTable.GetVersion() > req.GetTable().GetVersion() {
			return &StealTableOwnershipResponse{
				Success: false,
				Table:   &existingTable,
			}, nil
		}

		// now we inspect the reason for stealing the table
		if req.GetReason() == StealReason_OWNER_DOWN {
			// if the owner is down, we probably have a bunch of competing requests to steal the table,
			// but this one was the first!

			// let's first check if we agree the node is down
			if existingTable.GetGlobalOwner().IsActive {
				// we do not recognize the node as down!
				// todo: schedule a heartbeat check with the node
				return &StealTableOwnershipResponse{
					Success: false,
					Table:   &existingTable,
				}, nil
			}

			// the node is down, so we can steal the table
		}

		if req.GetReason() == StealReason_LOCAL_JOIN {
			// The node wants to perform a join operation and needs ownership to perform the join.
			// The node requesting this should already have most of the tables to do this, so we give it the
			// table.
			// This if-statement is intentionally left empty.
		}

		if req.GetReason() == StealReason_POLICY {
			// The node has determined that it should own the table due to policies on the node (e.g., it is
			// receiving excessive writes or reads to the table). This is a valid reason to steal the table.
			// This if-statement is intentionally left empty.
		}

		if req.GetReason() == StealReason_SCHEMA_MIGRATION {
			// The node is performing a schema migration and needs to exclusively own the table to perform the migration.
			// This if-statement is intentionally left empty.
		}

		if len(existingTable.GetAllowedRegions()) > 0 && !slices.Contains(existingTable.GetAllowedRegions(), req.GetTable().GetGlobalOwner().GetNodeRegion()) {
			// The node is not allowed to own this table due to the table policy.
			// If an operator wishes to change the policy, it must be submitted from an allowed region.
			return nil, fmt.Errorf("node is not in an allowed region: %w", ErrTablePolicyViolation)
		}

		// We have passed all the checks, and we may promise this table to the node.
		// Note, this prevents any further writes to the table until ownership is resolved.
		// The owning node will be responsible for updating any table metadata.
		_, err = atlas.ExecuteSQL(ctx, `
update tables
set owner_node_id = :owner_node_id,
    version       = :version,
    promised      = 1
where id = :id
`, conn, false, atlas.Param{
			Name:  "owner_node_id",
			Value: req.GetTable().GetGlobalOwner().GetNodeId(),
		}, atlas.Param{
			Name:  "version",
			Value: req.GetTable().GetVersion(),
		}, atlas.Param{
			Name:  "id",
			Value: req.GetTable().GetId(),
		})

		// For non-global requests, we remove any owner entries.
		// This table is now owned by a single node until it is released by deleting the global owner.
		_, err = atlas.ExecuteSQL(ctx, `
update leadership
set owner = 0
where table_id = :table_id
`, conn, false, atlas.Param{
			Name:  "table_id",
			Value: req.GetTable().GetId(),
		})

		_, err = atlas.ExecuteSQL(ctx, "commit", conn, false)
		if err != nil {
			return nil, err
		}

		return &StealTableOwnershipResponse{
			Success: true,
			Table:   req.GetTable(),
		}, nil
	}

	// We are dealing with regional tables from here on. Regional tables use the leadership table to determine ownership.
	// The leadership table has a 'promised' column that matches the table version to keep track of paxos consensus.

	// lets get the current region id
	regionId, err := atlas.GetRegionIdFromName(ctx, conn, req.GetSender().GetNodeRegion())
	if err != nil {
		return nil, err
	}

	// First, we check the ballot number of the table entry.
	if existingTable.GetVersion() >= req.GetTable().GetVersion() {
		return &StealTableOwnershipResponse{
			Success: false,
			Table:   &existingTable,
		}, nil
	}

	// Next, we check if the table is already promised to another node.
	results, err = atlas.ExecuteSQL(ctx, `
select * from leadership where table_id = :table_id and promised = :version
`, conn, false, atlas.Param{
		Name:  "table_id",
		Value: req.GetTable().GetId(),
	}, atlas.Param{
		Name:  "version",
		Value: req.GetTable().GetVersion(),
	})
	if err != nil {
		return nil, err
	}

	if !results.Empty() {
		return &StealTableOwnershipResponse{
			Success: false,
			Table:   &existingTable,
		}, nil
	}

	// Check if the table is exclusively owned by a node.
	if existingTable.GetGlobalOwner() != nil {
		return &StealTableOwnershipResponse{
			Success: false,
			Table:   &existingTable,
		}, nil
	}

	// We have passed all the checks, and we may promise this table to the node.
	// First, we will update the table entry.
	_, err = atlas.ExecuteSQL(ctx, `update tables set version = :version where id = :table_id`, conn, false, atlas.Param{
		Name:  "version",
		Value: req.GetTable().GetVersion(),
	}, atlas.Param{
		Name:  "table_id",
		Value: req.GetTable().GetId(),
	})
	if err != nil {
		return nil, err
	}

	// Now, we remove the current region owner.
	_, err = atlas.ExecuteSQL(ctx, `
update leadership
set owner = 0
where table_id = :table_id
  and region_id = :region_id`, conn, false, atlas.Param{
		Name:  "table_id",
		Value: req.GetTable().GetId(),
	}, atlas.Param{
		Name:  "region_id",
		Value: regionId,
	})

	// Finally, we promise the table to the node.
	_, err = atlas.ExecuteSQL(ctx, `
insert into leadership (table_id, node_id, region_id, promised, owner)
values (:table_id, :node_id, :region_id, :version, 0)`, conn, false, atlas.Param{
		Name:  "table_id",
		Value: req.GetTable().GetId(),
	}, atlas.Param{
		Name:  "node_id",
		Value: req.GetSender().GetNodeId(),
	}, atlas.Param{
		Name:  "region_id",
		Value: regionId,
	}, atlas.Param{
		Name:  "version",
		Value: req.GetTable().GetVersion(),
	})
	if err != nil {
		return nil, err
	}

	_, err = atlas.ExecuteSQL(ctx, "commit", conn, false)
	if err != nil {
		return nil, err
	}

	return &StealTableOwnershipResponse{
		Success: true,
		Table:   req.GetTable(),
	}, nil
}

func StoleTableOwnership(ctx context.Context, table *Table) (*Table, error) {
	return nil, nil
}

func (s *Server) WriteMigration(context.Context, *WriteMigrationRequest) (*WriteMigrationResponse, error) {
	return nil, nil
}
