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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMigrationRepositoryKV_GetNextVersion(t *testing.T) {
	store, cleanup := setupTestStore(t)
	defer cleanup()

	ctx := context.Background()
	dr := NewDataRepository(ctx, store)
	repo := NewMigrationRepositoryKV(ctx, store, dr).(*MigrationR)

	// Test next version for empty table
	version, err := repo.GetNextVersion(KeyName("test_table"))
	assert.NoError(t, err)
	assert.Equal(t, int64(1), version)

	// Add some migrations
	migration1 := &Migration{
		Version: &MigrationVersion{
			TableName:        KeyName("test_table"),
			TableVersion:     1,
			MigrationVersion: 1,
			NodeId:           123,
		},
		Migration: &Migration_Schema{
			Schema: &SchemaMigration{
				Commands: []string{"CREATE TABLE test (id INTEGER)"},
			},
		},
	}

	migration2 := &Migration{
		Version: &MigrationVersion{
			TableName:        KeyName("test_table"),
			TableVersion:     1,
			MigrationVersion: 3,
			NodeId:           123,
		},
		Migration: &Migration_Schema{
			Schema: &SchemaMigration{
				Commands: []string{"ALTER TABLE test ADD COLUMN name TEXT"},
			},
		},
	}

	err = repo.AddMigration(migration1)
	require.NoError(t, err)
	err = repo.AddMigration(migration2)
	require.NoError(t, err)

	// Next version should be 4 (max version + 1)
	nextVersion, err := repo.GetNextVersion(KeyName("test_table"))
	assert.NoError(t, err)
	assert.Equal(t, int64(4), nextVersion)
}

func TestMigrationRepositoryKV_AddAndGetMigration(t *testing.T) {
	store, cleanup := setupTestStore(t)
	defer cleanup()

	ctx := context.Background()
	dr := NewDataRepository(ctx, store)
	repo := NewMigrationRepositoryKV(ctx, store, dr).(*MigrationR)

	// Test schema migration
	schemaMigration := &Migration{
		Version: &MigrationVersion{
			TableName:        KeyName("test_table"),
			TableVersion:     1,
			MigrationVersion: 1,
			NodeId:           123,
		},
		Migration: &Migration_Schema{
			Schema: &SchemaMigration{
				Commands: []string{
					"CREATE TABLE test (id INTEGER PRIMARY KEY)",
					"CREATE INDEX idx_test_id ON test(id)",
				},
			},
		},
	}

	err := repo.AddMigration(schemaMigration)
	assert.NoError(t, err)

	// Retrieve migration
	retrieved, err := repo.GetMigrationVersion(schemaMigration.Version)
	assert.NoError(t, err)
	assert.Len(t, retrieved, 1)

	retrievedMigration := retrieved[0]
	assert.Equal(t, schemaMigration.Version.TableName, retrievedMigration.Version.TableName)
	assert.Equal(t, schemaMigration.Version.MigrationVersion, retrievedMigration.Version.MigrationVersion)
	assert.Equal(t, schemaMigration.Version.NodeId, retrievedMigration.Version.NodeId)

	// Check schema commands
	schemaCommands := retrievedMigration.GetSchema().GetCommands()
	assert.Len(t, schemaCommands, 2)
	assert.Equal(t, "CREATE TABLE test (id INTEGER PRIMARY KEY)", schemaCommands[0])
	assert.Equal(t, "CREATE INDEX idx_test_id ON test(id)", schemaCommands[1])
}

func TestMigrationRepositoryKV_DataMigration(t *testing.T) {
	store, cleanup := setupTestStore(t)
	defer cleanup()

	ctx := context.Background()
	dr := NewDataRepository(ctx, store)
	repo := NewMigrationRepositoryKV(ctx, store, dr).(*MigrationR)

	// Test data migration
	sessionData1 := []byte("INSERT INTO test (id) VALUES (1)")

	dataMigration := &Migration{
		Version: &MigrationVersion{
			TableName:        KeyName("test_table"),
			TableVersion:     1,
			MigrationVersion: 2,
			NodeId:           123,
		},
		Migration: &Migration_Data{
			Data: &DataMigration{
				Session: &DataMigration_Change{
					Change: &KVChange{
						Operation: &KVChange_Data{
							Data: &RawData{
								Data: sessionData1,
							},
						},
					},
				},
			},
		},
	}

	err := repo.AddMigration(dataMigration)
	assert.NoError(t, err)

	// Retrieve migration
	retrieved, err := repo.GetMigrationVersion(dataMigration.Version)
	assert.NoError(t, err)
	assert.Len(t, retrieved, 1)

	retrievedMigration := retrieved[0]
	assert.Equal(t, dataMigration.Version.TableName, retrievedMigration.Version.TableName)

	// Check data session
	rawData := retrievedMigration.GetData().GetChange().GetData()
	assert.NotNil(t, rawData)
	assert.Equal(t, sessionData1, rawData.GetData())
}

func TestMigrationRepositoryKV_CommitOperations(t *testing.T) {
	store, cleanup := setupTestStore(t)
	defer cleanup()

	ctx := context.Background()
	dr := NewDataRepository(ctx, store)
	repo := NewMigrationRepositoryKV(ctx, store, dr).(*MigrationR)

	// Add migration
	migration := &Migration{
		Version: &MigrationVersion{
			TableName:        KeyName("test_table"),
			TableVersion:     1,
			MigrationVersion: 1,
			NodeId:           123,
		},
		Migration: &Migration_Schema{
			Schema: &SchemaMigration{
				Commands: []string{"CREATE TABLE test (id INTEGER)"},
			},
		},
	}

	err := repo.AddMigration(migration)
	require.NoError(t, err)

	// Test CommitMigrationExact
	err = repo.CommitMigrationExact(migration.Version)
	assert.NoError(t, err)

	// Verify committed status by checking uncommitted migrations
	table := &Table{Name: KeyName("test_table")}
	uncommitted, err := repo.GetUncommittedMigrations(table)
	assert.NoError(t, err)
	assert.Len(t, uncommitted, 0) // Should be empty since we committed it
}

func TestMigrationRepositoryKV_CommitAllMigrations(t *testing.T) {
	store, cleanup := setupTestStore(t)
	defer cleanup()

	ctx := context.Background()
	dr := NewDataRepository(ctx, store)
	repo := NewMigrationRepositoryKV(ctx, store, dr).(*MigrationR)

	// Add multiple migrations for same table
	migration1 := &Migration{
		Version: &MigrationVersion{
			TableName:        KeyName("test_table"),
			TableVersion:     1,
			MigrationVersion: 1,
			NodeId:           123,
		},
		Migration: &Migration_Schema{
			Schema: &SchemaMigration{
				Commands: []string{"CREATE TABLE test (id INTEGER)"},
			},
		},
	}

	migration2 := &Migration{
		Version: &MigrationVersion{
			TableName:        KeyName("test_table"),
			TableVersion:     1,
			MigrationVersion: 2,
			NodeId:           124,
		},
		Migration: &Migration_Schema{
			Schema: &SchemaMigration{
				Commands: []string{"ALTER TABLE test ADD COLUMN name TEXT"},
			},
		},
	}

	err := repo.AddMigration(migration1)
	require.NoError(t, err)
	err = repo.AddMigration(migration2)
	require.NoError(t, err)

	// Test CommitAllMigrations
	err = repo.CommitAllMigrations(KeyName("test_table"))
	assert.NoError(t, err)

	// Verify all migrations are committed
	table := &Table{Name: KeyName("test_table")}
	uncommitted, err := repo.GetUncommittedMigrations(table)
	assert.NoError(t, err)
	assert.Len(t, uncommitted, 0)
}

func TestMigrationRepositoryKV_GetUncommittedMigrations(t *testing.T) {
	store, cleanup := setupTestStore(t)
	defer cleanup()

	ctx := context.Background()
	dr := NewDataRepository(ctx, store)
	repo := NewMigrationRepositoryKV(ctx, store, dr).(*MigrationR)

	// Add regular migration
	regularMigration := &Migration{
		Version: &MigrationVersion{
			TableName:        KeyName("test_table"),
			TableVersion:     1,
			MigrationVersion: 1,
			NodeId:           123,
		},
		Migration: &Migration_Schema{
			Schema: &SchemaMigration{
				Commands: []string{"CREATE TABLE test (id INTEGER)"},
			},
		},
	}

	// Add gossip migration
	gossipMigration := &Migration{
		Version: &MigrationVersion{
			TableName:        KeyName("test_table"),
			TableVersion:     1,
			MigrationVersion: 2,
			NodeId:           124,
		},
		Migration: &Migration_Schema{
			Schema: &SchemaMigration{
				Commands: []string{"ALTER TABLE test ADD COLUMN name TEXT"},
			},
		},
	}

	err := repo.AddMigration(regularMigration)
	require.NoError(t, err)
	err = repo.AddGossipMigration(gossipMigration)
	require.NoError(t, err)

	// Get uncommitted migrations (should exclude gossip)
	table := &Table{Name: KeyName("test_table")}
	uncommitted, err := repo.GetUncommittedMigrations(table)
	assert.NoError(t, err)
	assert.Len(t, uncommitted, 1) // Only regular migration, not gossip

	assert.Equal(t, regularMigration.Version.MigrationVersion, uncommitted[0].Version.MigrationVersion)
}

func TestMigrationRepositoryKV_GossipMigration(t *testing.T) {
	store, cleanup := setupTestStore(t)
	defer cleanup()

	ctx := context.Background()
	dr := NewDataRepository(ctx, store)
	repo := NewMigrationRepositoryKV(ctx, store, dr).(*MigrationR)

	gossipMigration := &Migration{
		Version: &MigrationVersion{
			TableName:        KeyName("test_table"),
			TableVersion:     1,
			MigrationVersion: 1,
			NodeId:           123,
		},
		Migration: &Migration_Schema{
			Schema: &SchemaMigration{
				Commands: []string{"CREATE TABLE gossip_test (id INTEGER)"},
			},
		},
	}

	err := repo.AddGossipMigration(gossipMigration)
	assert.NoError(t, err)

	// Retrieve migration
	retrieved, err := repo.GetMigrationVersion(gossipMigration.Version)
	assert.NoError(t, err)
	assert.Len(t, retrieved, 1)

	// Verify it's marked as gossip (indirectly by checking uncommitted list)
	table := &Table{Name: KeyName("test_table")}
	uncommitted, err := repo.GetUncommittedMigrations(table)
	assert.NoError(t, err)
	assert.Len(t, uncommitted, 0) // Gossip migrations don't appear in uncommitted list
}

func TestMigrationRepositoryKV_DuplicateInsert(t *testing.T) {
	store, cleanup := setupTestStore(t)
	defer cleanup()

	ctx := context.Background()
	dr := NewDataRepository(ctx, store)
	repo := NewMigrationRepositoryKV(ctx, store, dr).(*MigrationR)

	migration := &Migration{
		Version: &MigrationVersion{
			TableName:        KeyName("test_table"),
			TableVersion:     1,
			MigrationVersion: 1,
			NodeId:           123,
		},
		Migration: &Migration_Schema{
			Schema: &SchemaMigration{
				Commands: []string{"CREATE TABLE test (id INTEGER)"},
			},
		},
	}

	// First insert should succeed
	err := repo.AddMigration(migration)
	assert.NoError(t, err)

	// Second insert should be ignored (ON CONFLICT DO NOTHING behavior)
	err = repo.AddMigration(migration)
	assert.NoError(t, err) // No error, but no change

	// Verify only one migration exists
	retrieved, err := repo.GetMigrationVersion(migration.Version)
	assert.NoError(t, err)
	assert.Len(t, retrieved, 1)
}

func TestMigrationRepositoryKV_ErrorCases(t *testing.T) {
	store, cleanup := setupTestStore(t)
	defer cleanup()

	ctx := context.Background()
	dr := NewDataRepository(ctx, store)
	repo := NewMigrationRepositoryKV(ctx, store, dr).(*MigrationR)

	// Test GetMigrationVersion with nil version
	migrations, err := repo.GetMigrationVersion(nil)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "version is nil")
	assert.Nil(t, migrations)

	// Test CommitMigrationExact with non-existent migration
	nonExistentVersion := &MigrationVersion{
		TableName:        KeyName("non_existent_table"),
		TableVersion:     1,
		MigrationVersion: 999,
		NodeId:           999,
	}

	err = repo.CommitMigrationExact(nonExistentVersion)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "not found")

	// Test GetMigrationVersion with non-existent version
	migrations, err = repo.GetMigrationVersion(nonExistentVersion)
	assert.NoError(t, err) // Should return empty slice, not error
	assert.Len(t, migrations, 0)
}
