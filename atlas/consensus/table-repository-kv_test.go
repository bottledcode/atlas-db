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
 */

package consensus

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/bottledcode/atlas-db/atlas/kv"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func setupTestStore(t *testing.T) (kv.Store, func()) {
	tempDir, err := os.MkdirTemp("", "atlas-db-test-*")
	require.NoError(t, err)

	store, err := kv.NewBadgerStore(filepath.Join(tempDir, "test.db"))
	require.NoError(t, err)

	cleanup := func() {
		_ = store.Close()
		_ = os.RemoveAll(tempDir)
	}

	return store, cleanup
}

func TestTableRepositoryKV_InsertAndGetTable(t *testing.T) {
	store, cleanup := setupTestStore(t)
	defer cleanup()

	ctx := context.Background()
	repo := NewTableRepositoryKV(ctx, store).(*TableRepositoryKV)

	// Create test table
	table := &Table{
		Name:              "test_table",
		Version:           1,
		ReplicationLevel:  ReplicationLevel_regional,
		AllowedRegions:    []string{"us-east-1", "us-west-2"},
		RestrictedRegions: []string{"eu-west-1"},
		CreatedAt:         timestamppb.New(time.Now()),
		Type:              TableType_table,
		Group:             "",
		ShardPrincipals:   []string{"user_id"},
		Owner: &Node{
			Id:      123,
			Address: "192.168.1.100",
			Port:    8080,
			Region:  &Region{Name: "us-east-1"},
			Active:  true,
			Rtt:     durationpb.New(50 * time.Millisecond),
		},
	}

	// Test Insert
	err := repo.InsertTable(table)
	assert.NoError(t, err)

	// Test Get
	retrieved, err := repo.GetTable("test_table")
	assert.NoError(t, err)
	assert.NotNil(t, retrieved)

	// Verify all fields
	assert.Equal(t, table.Name, retrieved.Name)
	assert.Equal(t, table.Version, retrieved.Version)
	assert.Equal(t, table.ReplicationLevel, retrieved.ReplicationLevel)
	assert.Equal(t, table.AllowedRegions, retrieved.AllowedRegions)
	assert.Equal(t, table.RestrictedRegions, retrieved.RestrictedRegions)
	assert.Equal(t, table.Type, retrieved.Type)
	assert.Equal(t, table.ShardPrincipals, retrieved.ShardPrincipals)

	// Verify owner
	assert.NotNil(t, retrieved.Owner)
	assert.Equal(t, table.Owner.Id, retrieved.Owner.Id)
	assert.Equal(t, table.Owner.Address, retrieved.Owner.Address)
	assert.Equal(t, table.Owner.Port, retrieved.Owner.Port)
	assert.Equal(t, table.Owner.Region.Name, retrieved.Owner.Region.Name)
	assert.Equal(t, table.Owner.Active, retrieved.Owner.Active)
}

func TestTableRepositoryKV_UpdateTable(t *testing.T) {
	store, cleanup := setupTestStore(t)
	defer cleanup()

	ctx := context.Background()
	repo := NewTableRepositoryKV(ctx, store).(*TableRepositoryKV)

	// Create and insert initial table
	table := &Table{
		Name:             "test_table",
		Version:          1,
		ReplicationLevel: ReplicationLevel_regional,
		AllowedRegions:   []string{"us-east-1"},
		Type:             TableType_table,
		CreatedAt:        timestamppb.New(time.Now()),
	}

	err := repo.InsertTable(table)
	require.NoError(t, err)

	// Update table
	table.Version = 2
	table.ReplicationLevel = ReplicationLevel_global
	table.AllowedRegions = []string{"us-east-1", "us-west-2", "eu-west-1"}

	err = repo.UpdateTable(table)
	assert.NoError(t, err)

	// Verify update
	retrieved, err := repo.GetTable("test_table")
	assert.NoError(t, err)
	assert.Equal(t, int64(2), retrieved.Version)
	assert.Equal(t, ReplicationLevel_global, retrieved.ReplicationLevel)
	assert.Equal(t, []string{"us-east-1", "us-west-2", "eu-west-1"}, retrieved.AllowedRegions)
}

func TestTableRepositoryKV_UpdateTable_StaleIndexes(t *testing.T) {
	store, cleanup := setupTestStore(t)
	defer cleanup()

	ctx := context.Background()
	repo := NewTableRepositoryKV(ctx, store).(*TableRepositoryKV)

	// Create initial table with regional replication
	table := &Table{
		Name:             "test_table",
		Version:          1,
		ReplicationLevel: ReplicationLevel_regional,
		Group:            "old_group",
		Owner: &Node{
			Id:      100,
			Address: "node1.example.com",
			Port:    8080,
			Region:  &Region{Name: "us-east-1"},
			Active:  true,
			Rtt:     durationpb.New(10 * time.Millisecond),
		},
		Type:      TableType_table,
		CreatedAt: timestamppb.New(time.Now()),
	}

	err := repo.InsertTable(table)
	require.NoError(t, err)

	// Verify initial state - table should appear in regional queries
	regionalTables, err := repo.GetTablesByReplicationLevel(ReplicationLevel_regional)
	assert.NoError(t, err)
	assert.Len(t, regionalTables, 1)
	assert.Equal(t, "test_table", regionalTables[0].Name)

	// Global queries should be empty
	globalTables, err := repo.GetTablesByReplicationLevel(ReplicationLevel_global)
	assert.NoError(t, err)
	assert.Len(t, globalTables, 0)

	// Update table to change replication level, group, and owner
	table.Version = 2
	table.ReplicationLevel = ReplicationLevel_global
	table.Group = "new_group"
	table.Owner = &Node{
		Id:      200,
		Address: "node2.example.com",
		Port:    8080,
		Region:  &Region{Name: "us-west-2"},
		Active:  true,
		Rtt:     durationpb.New(15 * time.Millisecond),
	}

	err = repo.UpdateTable(table)
	require.NoError(t, err)

	// BUG DEMONSTRATION: After update, table should NOT appear in regional queries
	// but it will due to stale index entries
	regionalTablesAfterUpdate, err := repo.GetTablesByReplicationLevel(ReplicationLevel_regional)
	assert.NoError(t, err)
	
	// This assertion will FAIL because of stale indexes - the table will still appear
	// in regional queries even though it's now global
	assert.Len(t, regionalTablesAfterUpdate, 0, "Table should not appear in regional queries after being updated to global")

	// Table should now appear in global queries
	globalTablesAfterUpdate, err := repo.GetTablesByReplicationLevel(ReplicationLevel_global)
	assert.NoError(t, err)
	assert.Len(t, globalTablesAfterUpdate, 1)
	assert.Equal(t, "test_table", globalTablesAfterUpdate[0].Name)

	// Additional verification: Check for ghost entries in old group and owner indexes
	// This requires access to the underlying KV store to check for stale keys
	txn, err := store.Begin(false)
	require.NoError(t, err)
	defer txn.Discard()

	// Check if old replication index key still exists (it shouldn't)
	oldReplicationKey := kv.NewKeyBuilder().Meta().Append("index").Append("replication").
		Append(ReplicationLevel_regional.String()).Append("test_table").Build()
	_, err = txn.Get(ctx, oldReplicationKey)
	assert.Error(t, err, "Old replication index key should not exist after update")
	assert.ErrorIs(t, err, kv.ErrKeyNotFound, "Expected key not found error for old replication index")

	// Check if old group index key still exists (it shouldn't)
	oldGroupKey := kv.NewKeyBuilder().Meta().Append("index").Append("group").
		Append("old_group").Append("test_table").Build()
	_, err = txn.Get(ctx, oldGroupKey)
	assert.Error(t, err, "Old group index key should not exist after update")
	assert.ErrorIs(t, err, kv.ErrKeyNotFound, "Expected key not found error for old group index")

	// Check if old owner index key still exists (it shouldn't)
	oldOwnerKey := kv.NewKeyBuilder().Meta().Append("index").Append("owner").
		Append("100").Append("test_table").Build()
	_, err = txn.Get(ctx, oldOwnerKey)
	assert.Error(t, err, "Old owner index key should not exist after update")
	assert.ErrorIs(t, err, kv.ErrKeyNotFound, "Expected key not found error for old owner index")
}

func TestTableRepositoryKV_GetTablesByReplicationLevel(t *testing.T) {
	store, cleanup := setupTestStore(t)
	defer cleanup()

	ctx := context.Background()
	repo := NewTableRepositoryKV(ctx, store).(*TableRepositoryKV)

	// Create tables with different replication levels
	tables := []*Table{
		{
			Name:             "regional_table_1",
			Version:          1,
			ReplicationLevel: ReplicationLevel_regional,
			Type:             TableType_table,
			CreatedAt:        timestamppb.New(time.Now()),
		},
		{
			Name:             "regional_table_2",
			Version:          1,
			ReplicationLevel: ReplicationLevel_regional,
			Type:             TableType_table,
			CreatedAt:        timestamppb.New(time.Now()),
		},
		{
			Name:             "global_table",
			Version:          1,
			ReplicationLevel: ReplicationLevel_global,
			Type:             TableType_table,
			CreatedAt:        timestamppb.New(time.Now()),
		},
	}

	for _, table := range tables {
		err := repo.InsertTable(table)
		require.NoError(t, err)
	}

	// Test regional tables query
	regionalTables, err := repo.GetTablesByReplicationLevel(ReplicationLevel_regional)
	assert.NoError(t, err)
	assert.Len(t, regionalTables, 2)

	// Test global tables query
	globalTables, err := repo.GetTablesByReplicationLevel(ReplicationLevel_global)
	assert.NoError(t, err)
	assert.Len(t, globalTables, 1)
	assert.Equal(t, "global_table", globalTables[0].Name)
}

func TestTableRepositoryKV_GroupOperations(t *testing.T) {
	store, cleanup := setupTestStore(t)
	defer cleanup()

	ctx := context.Background()
	repo := NewTableRepositoryKV(ctx, store).(*TableRepositoryKV)

	// Create group
	groupTable := &Table{
		Name:             "test_group",
		Version:          1,
		ReplicationLevel: ReplicationLevel_regional,
		Type:             TableType_group,
		CreatedAt:        timestamppb.New(time.Now()),
	}

	group := &TableGroup{
		Details: groupTable,
		Tables:  make([]*Table, 0),
	}

	err := repo.InsertGroup(group)
	require.NoError(t, err)

	// Create tables in the group
	table1 := &Table{
		Name:             "table_in_group_1",
		Version:          1,
		ReplicationLevel: ReplicationLevel_regional,
		Type:             TableType_table,
		Group:            "test_group",
		CreatedAt:        timestamppb.New(time.Now()),
	}

	table2 := &Table{
		Name:             "table_in_group_2",
		Version:          1,
		ReplicationLevel: ReplicationLevel_regional,
		Type:             TableType_table,
		Group:            "test_group",
		CreatedAt:        timestamppb.New(time.Now()),
	}

	err = repo.InsertTable(table1)
	require.NoError(t, err)
	err = repo.InsertTable(table2)
	require.NoError(t, err)

	// Test getting group
	retrievedGroup, err := repo.GetGroup("test_group")
	assert.NoError(t, err)
	assert.NotNil(t, retrievedGroup)
	assert.Equal(t, "test_group", retrievedGroup.Details.Name)
	assert.Len(t, retrievedGroup.Tables, 2)

	// Verify table names in group
	tableNames := []string{retrievedGroup.Tables[0].Name, retrievedGroup.Tables[1].Name}
	assert.Contains(t, tableNames, "table_in_group_1")
	assert.Contains(t, tableNames, "table_in_group_2")
}

func TestTableRepositoryKV_ShardOperations(t *testing.T) {
	store, cleanup := setupTestStore(t)
	defer cleanup()

	ctx := context.Background()
	repo := NewTableRepositoryKV(ctx, store).(*TableRepositoryKV)

	// Create parent table for sharding
	parentTable := &Table{
		Name:             "sharded_table",
		Version:          1,
		ReplicationLevel: ReplicationLevel_regional,
		Type:             TableType_table,
		ShardPrincipals:  []string{"user_id", "tenant_id"},
		CreatedAt:        timestamppb.New(time.Now()),
	}

	err := repo.InsertTable(parentTable)
	require.NoError(t, err)

	// Create shard
	shardTable := &Table{
		Name:             "", // Will be auto-generated
		Version:          1,
		ReplicationLevel: ReplicationLevel_regional,
		Type:             TableType_table,
		CreatedAt:        timestamppb.New(time.Now()),
	}

	principals := []*Principal{
		{Name: "user_id", Value: "user123"},
		{Name: "tenant_id", Value: "tenant456"},
	}

	shard := &Shard{
		Table:      parentTable,
		Shard:      shardTable,
		Principals: principals,
	}

	// Test inserting shard
	err = repo.InsertShard(shard)
	assert.NoError(t, err)
	assert.NotEmpty(t, shardTable.Name)
	assert.Contains(t, shardTable.Name, "sharded_table_")

	// Test getting shard
	retrievedShard, err := repo.GetShard(parentTable, principals)
	assert.NoError(t, err)
	assert.NotNil(t, retrievedShard)
	assert.Equal(t, shardTable.Name, retrievedShard.Shard.Name)
}

func TestTableRepositoryKV_ErrorCases(t *testing.T) {
	store, cleanup := setupTestStore(t)
	defer cleanup()

	ctx := context.Background()
	repo := NewTableRepositoryKV(ctx, store).(*TableRepositoryKV)

	// Test getting non-existent table
	table, err := repo.GetTable("non_existent")
	assert.NoError(t, err)
	assert.Nil(t, table)

	// Test inserting duplicate table
	testTable := &Table{
		Name:             "duplicate_test",
		Version:          1,
		ReplicationLevel: ReplicationLevel_regional,
		Type:             TableType_table,
		CreatedAt:        timestamppb.New(time.Now()),
	}

	err = repo.InsertTable(testTable)
	require.NoError(t, err)

	// Try to insert again
	err = repo.InsertTable(testTable)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "already exists")

	// Test updating non-existent table
	nonExistentTable := &Table{
		Name:             "does_not_exist",
		Version:          1,
		ReplicationLevel: ReplicationLevel_regional,
		Type:             TableType_table,
		CreatedAt:        timestamppb.New(time.Now()),
	}

	err = repo.UpdateTable(nonExistentTable)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "does not exist")

	// Test getting group that's not a group type
	regularTable := &Table{
		Name:             "not_a_group",
		Version:          1,
		ReplicationLevel: ReplicationLevel_regional,
		Type:             TableType_table,
		CreatedAt:        timestamppb.New(time.Now()),
	}

	err = repo.InsertTable(regularTable)
	require.NoError(t, err)

	group, err := repo.GetGroup("not_a_group")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "not a group")
	assert.Nil(t, group)
}
