package consensus_test

import (
	"context"
	"github.com/bottledcode/atlas-db/atlas"
	"github.com/bottledcode/atlas-db/atlas/consensus"
	"github.com/bottledcode/atlas-db/atlas/test"
	"github.com/stretchr/testify/assert"
	"google.golang.org/protobuf/types/known/timestamppb"
	"testing"
)

func TestStealTableOwnership(t *testing.T) {
	ctx := context.Background()
	server := &consensus.Server{}

	t.Run("Steal ownership of a new table", func(t *testing.T) {
		f, cleanup := test.GetTempDb(t)
		defer cleanup()
		m, cleanup2 := test.GetTempDb(t)
		defer cleanup2()
		atlas.CreatePool(&atlas.Options{
			DbFilename:   f,
			MetaFilename: m,
		})
		defer atlas.DrainPool()

		conn, err := atlas.MigrationsPool.Take(ctx)
		assert.NoError(t, err)

		n := &consensus.Node{
			NodeId: 2,
		}
		consensus.TestCreateNode(t, n, conn)

		atlas.MigrationsPool.Put(conn)

		req := &consensus.StealTableOwnershipRequest{
			Table: &consensus.Table{
				Id:               2,
				Name:             "new_table",
				ReplicationLevel: consensus.ReplicationLevel_global,
				CreatedAt:        timestamppb.Now(),
				GlobalOwner:      n,
			},
			Reason: consensus.StealReason_OWNER_DOWN,
			Sender: n,
		}

		resp, err := server.StealTableOwnership(ctx, req)
		assert.NoError(t, err)
		assert.True(t, resp.Success)
		assert.Equal(t, req.Table, resp.Table)
	})

	t.Run("Steal ownership of an existing table", func(t *testing.T) {
		f, cleanup := test.GetTempDb(t)
		defer cleanup()
		m, cleanup2 := test.GetTempDb(t)
		defer cleanup2()
		atlas.CreatePool(&atlas.Options{
			DbFilename:   f,
			MetaFilename: m,
		})
		defer atlas.DrainPool()

		conn, err := atlas.MigrationsPool.Take(ctx)
		assert.NoError(t, err)

		r := &consensus.Region{}
		consensus.TestCreateRegion(t, r, conn)
		n := &consensus.Node{
			NodeId:     1,
			NodeRegion: r.GetRegionName(),
		}
		consensus.TestCreateNode(t, n, conn)

		newNode := &consensus.Node{
			NodeId:     2,
			NodeRegion: r.GetRegionName(),
		}
		consensus.TestCreateNode(t, newNode, conn)

		req := &consensus.StealTableOwnershipRequest{
			Table: &consensus.Table{
				Id:               1,
				Name:             "test_table",
				ReplicationLevel: consensus.ReplicationLevel_global,
				CreatedAt:        timestamppb.Now(),
				GlobalOwner:      n,
				Version:          1,
			},
			Reason: consensus.StealReason_LOCAL_JOIN,
			Sender: newNode,
		}

		consensus.TestCreateTableEntry(t, req.Table, conn)
		req.Table.Version = 2
		req.Table.GlobalOwner = newNode

		atlas.MigrationsPool.Put(conn)

		resp, err := server.StealTableOwnership(ctx, req)
		assert.NoError(t, err)
		assert.True(t, resp.Success)
		assert.Equal(t, req.Table, resp.Table)
	})
}
