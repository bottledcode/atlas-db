package consensus

import (
	"context"
	"github.com/bottledcode/atlas-db/atlas"
	"github.com/bottledcode/atlas-db/atlas/test"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNodeAddProposal_Success(t *testing.T) {
	server := Server{}
	ctx := context.Background()
	node := &Node{
		NodeId:     1,
		NodeRegion: "region1",
	}

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

	_, err = atlas.ExecuteSQL(ctx, "INSERT INTO regions (name) VALUES ('region1')", conn, false)
	assert.NoError(t, err)

	atlas.MigrationsPool.Put(conn)

	promise, err := server.nodeAddProposal(ctx, node)
	assert.NoError(t, err)
	assert.NotNil(t, promise)
	assert.True(t, promise.Promise)
	assert.Equal(t, node, promise.GetNode())
}

func TestNodeAddProposal_RegionNotFound(t *testing.T) {
	server := &Server{}
	ctx := context.Background()
	node := &Node{
		NodeId:     1,
		NodeRegion: "nonexistent_region",
	}

	f, cleanup := test.GetTempDb(t)
	defer cleanup()
	m, cleanup2 := test.GetTempDb(t)
	defer cleanup2()
	atlas.CreatePool(&atlas.Options{
		DbFilename:   f,
		MetaFilename: m,
	})
	defer atlas.DrainPool()

	promise, err := server.nodeAddProposal(ctx, node)
	assert.Error(t, err)
	assert.Nil(t, promise)
	assert.Contains(t, err.Error(), "region nonexistent_region not found")
}

func TestNodeAddProposal_AtlasError(t *testing.T) {
	server := &Server{}
	ctx := context.Background()
	node := &Node{
		NodeId:     1,
		NodeRegion: "region1",
	}

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

	_, err = atlas.ExecuteSQL(ctx, "INSERT INTO regions (name) VALUES ('region1')", conn, false)
	assert.NoError(t, err)
	_, err = atlas.ExecuteSQL(ctx, "INSERT INTO nodes (id, address, port, region_id) VALUES (1, 'localhost', 1234, 1)", conn, false)
	assert.NoError(t, err)

	atlas.MigrationsPool.Put(conn)

	promise, err := server.nodeAddProposal(ctx, node)
	assert.NoError(t, err)
	assert.Equal(t, &Node{
		NodeId:      1,
		NodeAddress: "localhost",
		NodeRegion:  "region1",
		NodePort:    1234,
	}, promise.GetNode())
}

func TestNodeRemoveProposal_Success(t *testing.T) {
	server := Server{}
	ctx := context.Background()
	node := &Node{
		NodeId:      1,
		NodeAddress: "localhost",
		NodeRegion:  "region1",
		NodePort:    1234,
	}

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

	_, err = atlas.ExecuteSQL(ctx, "INSERT INTO regions (name) VALUES ('region1')", conn, false)
	assert.NoError(t, err)
	_, err = atlas.ExecuteSQL(ctx, "INSERT INTO nodes (id, address, port, region_id) VALUES (1, 'localhost', 1234, 1)", conn, false)
	assert.NoError(t, err)

	atlas.MigrationsPool.Put(conn)

	promise, err := server.nodeRemoveProposal(ctx, node)
	assert.NoError(t, err)
	assert.NotNil(t, promise)
	assert.True(t, promise.Promise)
	assert.Equal(t, node, promise.GetNode())
}

func TestNodeRemoveProposal_RegionNotFound(t *testing.T) {
	server := &Server{}
	ctx := context.Background()
	node := &Node{
		NodeId:     1,
		NodeRegion: "nonexistent_region",
	}

	f, cleanup := test.GetTempDb(t)
	defer cleanup()
	m, cleanup2 := test.GetTempDb(t)
	defer cleanup2()
	atlas.CreatePool(&atlas.Options{
		DbFilename:   f,
		MetaFilename: m,
	})
	defer atlas.DrainPool()

	promise, err := server.nodeRemoveProposal(ctx, node)
	assert.Error(t, err)
	assert.Nil(t, promise)
	assert.Contains(t, err.Error(), "region nonexistent_region not found")
}

func TestNodeRemoveProposal_NodeNotFound(t *testing.T) {
	server := &Server{}
	ctx := context.Background()
	node := &Node{
		NodeId:     1,
		NodeRegion: "region1",
	}

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

	_, err = atlas.ExecuteSQL(ctx, "INSERT INTO regions (name) VALUES ('region1')", conn, false)
	assert.NoError(t, err)

	atlas.MigrationsPool.Put(conn)

	promise, err := server.nodeRemoveProposal(ctx, node)
	assert.NoError(t, err)
	assert.NotNil(t, promise)
	assert.False(t, promise.Promise)
	assert.Nil(t, promise.GetNode())
}
