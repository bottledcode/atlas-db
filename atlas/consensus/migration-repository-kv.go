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
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/bottledcode/atlas-db/atlas/kv"
	"google.golang.org/protobuf/proto"
)

// MigrationRepositoryKV implements MigrationRepository using key-value operations
type MigrationRepositoryKV struct {
	store kv.Store
	ctx   context.Context
}

// NewMigrationRepositoryKV creates a new KV-based migration repository
func NewMigrationRepositoryKV(ctx context.Context, store kv.Store) MigrationRepository {
	repo := &MigrationR{
		BaseRepository: BaseRepository[*StoredMigrationBatch, MigrationKey]{
			store: store,
			ctx:   ctx,
		},
		data: &DataR{
			BaseRepository[*Record, DataKey]{
				store: store,
				ctx:   ctx,
			},
		},
	}
	repo.repo = repo
	return repo
}

func getMigrationPrefixKey(table string) []byte {
	return kv.NewKeyBuilder().Migration(table, -1).Build()
}

func getMigrationTablePrefixKey(table string) []byte {
	return kv.NewKeyBuilder().Meta().Migration(table, 0).Build()
}

func getMigrationPrefixKeyWithVersion(version *MigrationVersion) []byte {
	return kv.NewKeyBuilder().
		Migration(version.GetTableName(), version.GetMigrationVersion()).
		Node(version.GetNodeId()).
		TableVersion(version.GetTableVersion()).
		Build()
}

func getUncommittedIndexKey(version *MigrationVersion) []byte {
	return kv.NewKeyBuilder().
		Migration(version.GetTableName(), version.GetMigrationVersion()).
		Index().
		Uncommitted().
		Node(version.GetNodeId()).
		Build()
}

func getMigrationIndexKey(version *MigrationVersion) []byte {
	return kv.NewKeyBuilder().
		Index().
		Migration(version.GetTableName(), version.GetMigrationVersion()).
		Node(version.GetNodeId()).
		Build()
}

func (m *MigrationRepositoryKV) GetNextVersion(table string) (int64, error) {
	// Scan for the highest version number for this table
	// Key pattern: meta:migration:{table}:version:{version}:node:{nodeId}
	prefix := getMigrationPrefixKey(table)

	txn, err := m.store.Begin(false)
	if err != nil {
		return 0, fmt.Errorf("failed to begin read transaction: %w", err)
	}
	defer txn.Discard()

	iterator := txn.NewIterator(kv.IteratorOptions{
		PrefetchValues: false,
		Prefix:         prefix,
	})
	defer func() { _ = iterator.Close() }()

	maxVersion := int64(0)

	for iterator.Seek(prefix); iterator.Valid(); iterator.Next() {
		item := iterator.Item()
		key := item.Key()

		keyStr := string(key)

		// Parse version from key: meta:migration:{table}:version:{version}:...
		parts := strings.Split(keyStr, ":")
		if len(parts) >= 5 && parts[4] != "" {
			if version, err := strconv.ParseInt(parts[4], 10, 64); err == nil {
				if version > maxVersion {
					maxVersion = version
				}
			}
		}
	}

	return maxVersion + 1, nil
}

func (m *MigrationRepositoryKV) GetMigrationVersion(version *MigrationVersion) ([]*Migration, error) {
	if version == nil {
		return nil, errors.New("version is nil")
	}

	// Key: meta:migration:{table}:version:{version}:node:{nodeId}:table_version:{table_version}
	key := getMigrationPrefixKeyWithVersion(version)

	txn, err := m.store.Begin(false)
	if err != nil {
		return nil, fmt.Errorf("failed to read transaction: %w", err)
	}
	defer txn.Discard()

	data, err := txn.Get(m.ctx, key)
	if err != nil {
		if errors.Is(err, kv.ErrKeyNotFound) {
			return []*Migration{}, nil
		}
		return nil, fmt.Errorf("failed to get migration: %w", err)
	}

	var batch StoredMigrationBatch
	if err := proto.Unmarshal(data, &batch); err != nil {
		return nil, fmt.Errorf("failed to unmarshal migration batch: %w", err)
	}

	return []*Migration{batch.Migration}, nil
}

func (m *MigrationRepositoryKV) CommitAllMigrations(table string) error {
	// Update all migrations for this table to committed=true
	prefix := getMigrationTablePrefixKey(table)

	txn, err := m.store.Begin(true)
	if err != nil {
		return fmt.Errorf("failed to begin write transaction: %w", err)
	}
	defer txn.Discard()

	iterator := txn.NewIterator(kv.IteratorOptions{
		Prefix:         prefix,
		PrefetchValues: true,
	})

	batch := txn.NewBatch()

	for iterator.Rewind(); iterator.Valid(); iterator.Next() {
		item := iterator.Item()
		data, err := item.Value()
		if err != nil {
			continue
		}

		var storedBatch StoredMigrationBatch
		if err := proto.Unmarshal(data, &storedBatch); err != nil {
			continue
		}

		// Only process uncommitted, non-gossip migrations
		if storedBatch.Committed || storedBatch.Gossip {
			continue
		}

		// Mark as committed
		storedBatch.Committed = true

		updatedData, err := proto.Marshal(&storedBatch)
		if err != nil {
			continue
		}

		if err := batch.Set(item.KeyCopy(), updatedData); err != nil {
			_ = iterator.Close()
			return fmt.Errorf("failed to add to batch: %w", err)
		}

		// Remove from uncommitted index
		version := storedBatch.Migration.GetVersion()
		uncommittedIndexKey := getUncommittedIndexKey(version)

		if err := batch.Delete(uncommittedIndexKey); err != nil {
			_ = iterator.Close()
			return fmt.Errorf("failed to delete uncommitted index entry: %w", err)
		}
	}

	// Close iterator before flushing/committing
	_ = iterator.Close()

	if err := batch.Flush(); err != nil {
		return fmt.Errorf("failed to flush batch: %w", err)
	}

	return txn.Commit()
}

func (m *MigrationRepositoryKV) CommitMigrationExact(version *MigrationVersion) error {
	// Key: meta:migration:{table}:version:{version}:node:{nodeId}:table_version:{table_version}
	key := getMigrationPrefixKeyWithVersion(version)

	txn, err := m.store.Begin(true)
	if err != nil {
		return fmt.Errorf("failed to begin write transaction: %w", err)
	}
	defer txn.Discard()

	data, err := txn.Get(m.ctx, key)
	if err != nil {
		if errors.Is(err, kv.ErrKeyNotFound) {
			return fmt.Errorf("migration not found")
		}
		return fmt.Errorf("failed to get migration: %w", err)
	}

	var storedBatch StoredMigrationBatch
	if err := proto.Unmarshal(data, &storedBatch); err != nil {
		return fmt.Errorf("failed to unmarshal migration batch: %w", err)
	}

	// Only proceed if migration is not already committed and not gossip
	if storedBatch.Committed {
		return nil
	}

	storedBatch.Committed = true

	updatedData, err := proto.Marshal(&storedBatch)
	if err != nil {
		return fmt.Errorf("failed to marshal updated batch: %w", err)
	}

	if err := txn.Put(m.ctx, key, updatedData); err != nil {
		return fmt.Errorf("failed to update migration: %w", err)
	}

	// Remove from uncommitted index if it was previously uncommitted and not gossip
	if !storedBatch.Gossip {
		uncommittedIndexKey := getUncommittedIndexKey(version)

		if err := txn.Delete(m.ctx, uncommittedIndexKey); err != nil {
			return fmt.Errorf("failed to delete uncommitted index entry: %w", err)
		}
	}

	return txn.Commit()
}

func (m *MigrationRepositoryKV) GetUncommittedMigrations(table *Table) ([]*Migration, error) {
	// Get all uncommitted, non-gossip migrations for this table
	prefix := getMigrationTablePrefixKey(table.Name)

	txn, err := m.store.Begin(false)
	if err != nil {
		return nil, fmt.Errorf("failed to begin a read transaction: %w", err)
	}
	defer txn.Discard()

	iterator := txn.NewIterator(kv.IteratorOptions{
		Prefix:         prefix,
		PrefetchValues: true,
	})
	defer func() { _ = iterator.Close() }()

	var migrations []*Migration

	for iterator.Rewind(); iterator.Valid(); iterator.Next() {
		item := iterator.Item()
		data, err := item.Value()
		if err != nil {
			continue
		}

		var storedBatch StoredMigrationBatch
		if err := proto.Unmarshal(data, &storedBatch); err != nil {
			continue
		}

		// Skip committed or gossip migrations
		if storedBatch.Committed || storedBatch.Gossip {
			continue
		}

		migrations = append(migrations, storedBatch.Migration)
	}

	return migrations, nil
}

func (m *MigrationRepositoryKV) AddMigration(migration *Migration) error {
	return m.addMigrationInternal(migration, false)
}

func (m *MigrationRepositoryKV) AddGossipMigration(migration *Migration) error {
	return m.addMigrationInternal(migration, true)
}

func (m *MigrationRepositoryKV) addMigrationInternal(migration *Migration, gossip bool) error {
	version := migration.GetVersion()

	// Key: meta:migration:{table}:version:{version}:node:{nodeId}:table_version:{table_version}
	key := getMigrationPrefixKeyWithVersion(version)

	txn, err := m.store.Begin(true)
	if err != nil {
		return fmt.Errorf("failed to begin write transaction: %w", err)
	}
	defer txn.Discard()

	// Check if migration already exists (ON CONFLICT DO NOTHING equivalent)
	if _, err := txn.Get(m.ctx, key); err == nil {
		// Migration already exists, do nothing
		return nil
	} else if !errors.Is(err, kv.ErrKeyNotFound) {
		return fmt.Errorf("failed to check migration existence: %w", err)
	}

	// Create a stored migration batch with protobuf
	storedBatch := &StoredMigrationBatch{
		Migration: migration,
		Committed: false,
		Gossip:    gossip,
	}

	data, err := proto.Marshal(storedBatch)
	if err != nil {
		return fmt.Errorf("failed to marshal migration batch: %w", err)
	}

	if err := txn.Put(m.ctx, key, data); err != nil {
		return fmt.Errorf("failed to store migration: %w", err)
	}

	// Also create index entries for efficient querying
	if err := m.updateMigrationIndexes(txn, storedBatch); err != nil {
		return fmt.Errorf("failed to update migration indexes: %w", err)
	}

	return txn.Commit()
}

// updateMigrationIndexes maintains indexes for efficient migration queries
func (m *MigrationRepositoryKV) updateMigrationIndexes(txn kv.Transaction, storedBatch *StoredMigrationBatch) error {
	version := storedBatch.Migration.GetVersion()

	// Index by table: meta:index:migration:table:{table}:{version}:{nodeId} -> migration_key
	tableIndexKey := getMigrationIndexKey(version)

	migrationKey := getMigrationPrefixKeyWithVersion(version)

	if err := txn.Put(m.ctx, tableIndexKey, migrationKey); err != nil {
		return err
	}

	// Index by committed status: meta:index:migration:uncommitted:{table}:{version}:{nodeId} -> migration_key
	if !storedBatch.Committed && !storedBatch.Gossip {
		uncommittedIndexKey := getUncommittedIndexKey(version)

		if err := txn.Put(m.ctx, uncommittedIndexKey, migrationKey); err != nil {
			return err
		}
	}

	return nil
}

// GetMigrationsByTable provides efficient querying of migrations by table
func (m *MigrationRepositoryKV) GetMigrationsByTable(tableName string) ([]*Migration, error) {
	prefix := getMigrationTablePrefixKey(tableName)

	txn, err := m.store.Begin(false)
	if err != nil {
		return nil, fmt.Errorf("failed to begin a read transaction: %w", err)
	}
	defer txn.Discard()

	iterator := txn.NewIterator(kv.IteratorOptions{
		Prefix:         prefix,
		PrefetchValues: true,
	})
	defer func() { _ = iterator.Close() }()

	var migrations []*Migration

	for iterator.Rewind(); iterator.Valid(); iterator.Next() {
		item := iterator.Item()
		data, err := item.Value()
		if err != nil {
			continue
		}

		var storedBatch StoredMigrationBatch
		if err := proto.Unmarshal(data, &storedBatch); err != nil {
			continue
		}

		migrations = append(migrations, storedBatch.Migration)
	}

	return migrations, nil
}

// DeleteMigration removes a migration (useful for cleanup)
func (m *MigrationRepositoryKV) DeleteMigration(version *MigrationVersion) error {
	// Key: meta:migration:{table}:version:{version}:node:{nodeId}:table_version:{table_version}
	key := getMigrationPrefixKeyWithVersion(version)

	txn, err := m.store.Begin(true)
	if err != nil {
		return fmt.Errorf("failed to begin write transaction: %w", err)
	}
	defer txn.Discard()

	// Delete the main migration record
	if err := txn.Delete(m.ctx, key); err != nil {
		return fmt.Errorf("failed to delete migration: %w", err)
	}

	// Delete index entries
	tableIndexKey := getMigrationIndexKey(version)
	_ = txn.Delete(m.ctx, tableIndexKey)

	uncommittedIndexKey := getUncommittedIndexKey(version)
	_ = txn.Delete(m.ctx, uncommittedIndexKey)

	return txn.Commit()
}
