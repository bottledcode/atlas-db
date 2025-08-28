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
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/bottledcode/atlas-db/atlas/kv"
)

// MigrationRepositoryKV implements MigrationRepository using key-value operations
type MigrationRepositoryKV struct {
	store kv.Store
	ctx   context.Context
}

// NewMigrationRepositoryKV creates a new KV-based migration repository
func NewMigrationRepositoryKV(ctx context.Context, store kv.Store) MigrationRepository {
	return &MigrationRepositoryKV{
		store: store,
		ctx:   ctx,
	}
}

// MigrationStorageModel represents how migration data is stored in KV format
type MigrationStorageModel struct {
	TableID          string  `json:"table_id"`
	TableVersion     int64   `json:"table_version"`
	MigrationVersion int64   `json:"migration_version"`
	BatchPart        int     `json:"batch_part"`
	ByNodeID         int64   `json:"by_node_id"`
	Command          *string `json:"command,omitempty"` // For schema migrations
	Data             *[]byte `json:"data,omitempty"`    // For data migrations
	Committed        bool    `json:"committed"`
	Gossip           bool    `json:"gossip"`
}

// MigrationBatchModel represents a complete migration batch
type MigrationBatchModel struct {
	Version   *MigrationVersion        `json:"version"`
	Parts     []*MigrationStorageModel `json:"parts"`
	Committed bool                     `json:"committed"`
	Gossip    bool                     `json:"gossip"`
}

func (m *MigrationRepositoryKV) GetNextVersion(table string) (int64, error) {
	// Scan for the highest version number for this table
	// Key pattern: meta:migration:{table}:version:{version}:node:{nodeId}
	prefix := kv.NewKeyBuilder().Meta().Append("migration").Append(table).Append("version").Build()

	txn, err := m.store.Begin(false)
	if err != nil {
		return 0, fmt.Errorf("failed to begin read transaction: %w", err)
	}
	defer txn.Discard()

	iterator := txn.NewIterator(kv.IteratorOptions{
		PrefetchValues: false,
	})
	defer iterator.Close()

	maxVersion := int64(0)
	
	// Use proper BadgerDB prefix iteration pattern
	for iterator.Seek(prefix); iterator.Valid(); iterator.Next() {
		item := iterator.Item()
		key := item.Key()
		
		// Check if key still has our prefix
		if !bytes.HasPrefix(key, prefix) {
			break
		}
		
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
	key := kv.NewKeyBuilder().Meta().Append("migration").
		Append(version.GetTableName()).
		Append("version").Append(fmt.Sprintf("%d", version.GetMigrationVersion())).
		Append("node").Append(fmt.Sprintf("%d", version.GetNodeId())).
		Append("table_version").Append(fmt.Sprintf("%d", version.GetTableVersion())).
		Build()

	txn, err := m.store.Begin(false)
	if err != nil {
		return nil, fmt.Errorf("failed to begin read transaction: %w", err)
	}
	defer txn.Discard()

	data, err := txn.Get(m.ctx, key)
	if err != nil {
		if errors.Is(err, kv.ErrKeyNotFound) {
			return []*Migration{}, nil
		}
		return nil, fmt.Errorf("failed to get migration: %w", err)
	}

	var batch MigrationBatchModel
	if err := json.Unmarshal(data, &batch); err != nil {
		return nil, fmt.Errorf("failed to unmarshal migration batch: %w", err)
	}

	return m.convertBatchToMigrations(&batch), nil
}

func (m *MigrationRepositoryKV) convertBatchToMigrations(batch *MigrationBatchModel) []*Migration {
	if len(batch.Parts) == 0 {
		return []*Migration{}
	}

	// Group parts by migration type (schema vs data)
	migrations := make([]*Migration, 0)
	currentMigration := &Migration{
		Version: batch.Version,
	}

	// Determine migration type from first part
	firstPart := batch.Parts[0]
	if firstPart.Command != nil {
		// Schema migration
		commands := make([]string, 0, len(batch.Parts))
		for _, part := range batch.Parts {
			if part.Command != nil {
				commands = append(commands, *part.Command)
			}
		}
		currentMigration.Migration = &Migration_Schema{
			Schema: &SchemaMigration{
				Commands: commands,
			},
		}
	} else {
		// Data migration
		sessions := make([][]byte, 0, len(batch.Parts))
		for _, part := range batch.Parts {
			if part.Data != nil {
				sessions = append(sessions, *part.Data)
			}
		}
		currentMigration.Migration = &Migration_Data{
			Data: &DataMigration{
				Session: sessions,
			},
		}
	}

	migrations = append(migrations, currentMigration)
	return migrations
}

func (m *MigrationRepositoryKV) CommitAllMigrations(table string) error {
	// Update all migrations for this table to committed=true
	prefix := kv.NewKeyBuilder().Meta().Append("migration").Append(table).Build()

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

		var migrationBatch MigrationBatchModel
		if err := json.Unmarshal(data, &migrationBatch); err != nil {
			continue
		}

		// Mark as committed
		migrationBatch.Committed = true

		updatedData, err := json.Marshal(&migrationBatch)
		if err != nil {
			continue
		}

		if err := batch.Set(item.KeyCopy(), updatedData); err != nil {
			iterator.Close()
			return fmt.Errorf("failed to add to batch: %w", err)
		}
	}

	// Close iterator before flushing/committing
	iterator.Close()

	if err := batch.Flush(); err != nil {
		return fmt.Errorf("failed to flush batch: %w", err)
	}

	return txn.Commit()
}

func (m *MigrationRepositoryKV) CommitMigrationExact(version *MigrationVersion) error {
	// Key: meta:migration:{table}:version:{version}:node:{nodeId}:table_version:{table_version}
	key := kv.NewKeyBuilder().Meta().Append("migration").
		Append(version.GetTableName()).
		Append("version").Append(fmt.Sprintf("%d", version.GetMigrationVersion())).
		Append("node").Append(fmt.Sprintf("%d", version.GetNodeId())).
		Append("table_version").Append(fmt.Sprintf("%d", version.GetTableVersion())).
		Build()

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

	var batch MigrationBatchModel
	if err := json.Unmarshal(data, &batch); err != nil {
		return fmt.Errorf("failed to unmarshal migration batch: %w", err)
	}

	batch.Committed = true

	updatedData, err := json.Marshal(&batch)
	if err != nil {
		return fmt.Errorf("failed to marshal updated batch: %w", err)
	}

	if err := txn.Put(m.ctx, key, updatedData); err != nil {
		return fmt.Errorf("failed to update migration: %w", err)
	}

	return txn.Commit()
}

func (m *MigrationRepositoryKV) GetUncommittedMigrations(table *Table) ([]*Migration, error) {
	// Get all uncommitted, non-gossip migrations for this table
	prefix := kv.NewKeyBuilder().Meta().Append("migration").Append(table.Name).Build()

	txn, err := m.store.Begin(false)
	if err != nil {
		return nil, fmt.Errorf("failed to begin read transaction: %w", err)
	}
	defer txn.Discard()

	iterator := txn.NewIterator(kv.IteratorOptions{
		Prefix:         prefix,
		PrefetchValues: true,
	})
	defer iterator.Close()

	var migrations []*Migration

	for iterator.Rewind(); iterator.Valid(); iterator.Next() {
		item := iterator.Item()
		data, err := item.Value()
		if err != nil {
			continue
		}

		var batch MigrationBatchModel
		if err := json.Unmarshal(data, &batch); err != nil {
			continue
		}

		// Skip committed or gossip migrations
		if batch.Committed || batch.Gossip {
			continue
		}

		batchMigrations := m.convertBatchToMigrations(&batch)
		migrations = append(migrations, batchMigrations...)
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
	key := kv.NewKeyBuilder().Meta().Append("migration").
		Append(version.GetTableName()).
		Append("version").Append(fmt.Sprintf("%d", version.GetMigrationVersion())).
		Append("node").Append(fmt.Sprintf("%d", version.GetNodeId())).
		Append("table_version").Append(fmt.Sprintf("%d", version.GetTableVersion())).
		Build()

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

	// Convert migration to storage model
	batch := &MigrationBatchModel{
		Version:   version,
		Parts:     make([]*MigrationStorageModel, 0),
		Committed: false,
		Gossip:    gossip,
	}

	batchPart := 0
	switch migration.GetMigration().(type) {
	case *Migration_Schema:
		for _, command := range migration.GetSchema().GetCommands() {
			part := &MigrationStorageModel{
				TableID:          version.GetTableName(),
				TableVersion:     version.GetTableVersion(),
				MigrationVersion: version.GetMigrationVersion(),
				BatchPart:        batchPart,
				ByNodeID:         version.GetNodeId(),
				Command:          &command,
				Data:             nil,
				Committed:        false,
				Gossip:           gossip,
			}
			batch.Parts = append(batch.Parts, part)
			batchPart++
		}
	case *Migration_Data:
		for _, data := range migration.GetData().GetSession() {
			dataCopy := make([]byte, len(data))
			copy(dataCopy, data)

			part := &MigrationStorageModel{
				TableID:          version.GetTableName(),
				TableVersion:     version.GetTableVersion(),
				MigrationVersion: version.GetMigrationVersion(),
				BatchPart:        batchPart,
				ByNodeID:         version.GetNodeId(),
				Command:          nil,
				Data:             &dataCopy,
				Committed:        false,
				Gossip:           gossip,
			}
			batch.Parts = append(batch.Parts, part)
			batchPart++
		}
	}

	data, err := json.Marshal(batch)
	if err != nil {
		return fmt.Errorf("failed to marshal migration batch: %w", err)
	}

	if err := txn.Put(m.ctx, key, data); err != nil {
		return fmt.Errorf("failed to store migration: %w", err)
	}

	// Also create index entries for efficient querying
	if err := m.updateMigrationIndexes(txn, batch); err != nil {
		return fmt.Errorf("failed to update migration indexes: %w", err)
	}

	return txn.Commit()
}

// updateMigrationIndexes maintains indexes for efficient migration queries
func (m *MigrationRepositoryKV) updateMigrationIndexes(txn kv.Transaction, batch *MigrationBatchModel) error {
	version := batch.Version

	// Index by table: meta:index:migration:table:{table}:{version}:{nodeId} -> migration_key
	tableIndexKey := kv.NewKeyBuilder().Meta().Append("index").Append("migration").
		Append("table").Append(version.GetTableName()).
		Append(fmt.Sprintf("%d", version.GetMigrationVersion())).
		Append(fmt.Sprintf("%d", version.GetNodeId())).
		Build()

	migrationKey := kv.NewKeyBuilder().Meta().Append("migration").
		Append(version.GetTableName()).
		Append("version").Append(fmt.Sprintf("%d", version.GetMigrationVersion())).
		Append("node").Append(fmt.Sprintf("%d", version.GetNodeId())).
		Append("table_version").Append(fmt.Sprintf("%d", version.GetTableVersion())).
		Build()

	if err := txn.Put(m.ctx, tableIndexKey, migrationKey); err != nil {
		return err
	}

	// Index by committed status: meta:index:migration:uncommitted:{table}:{version}:{nodeId} -> migration_key
	if !batch.Committed && !batch.Gossip {
		uncommittedIndexKey := kv.NewKeyBuilder().Meta().Append("index").Append("migration").
			Append("uncommitted").Append(version.GetTableName()).
			Append(fmt.Sprintf("%d", version.GetMigrationVersion())).
			Append(fmt.Sprintf("%d", version.GetNodeId())).
			Build()

		if err := txn.Put(m.ctx, uncommittedIndexKey, migrationKey); err != nil {
			return err
		}
	}

	return nil
}

// GetMigrationsByTable provides efficient querying of migrations by table
func (m *MigrationRepositoryKV) GetMigrationsByTable(tableName string) ([]*Migration, error) {
	prefix := kv.NewKeyBuilder().Meta().Append("migration").Append(tableName).Build()

	txn, err := m.store.Begin(false)
	if err != nil {
		return nil, fmt.Errorf("failed to begin read transaction: %w", err)
	}
	defer txn.Discard()

	iterator := txn.NewIterator(kv.IteratorOptions{
		Prefix:         prefix,
		PrefetchValues: true,
	})
	defer iterator.Close()

	var migrations []*Migration

	for iterator.Rewind(); iterator.Valid(); iterator.Next() {
		item := iterator.Item()
		data, err := item.Value()
		if err != nil {
			continue
		}

		var batch MigrationBatchModel
		if err := json.Unmarshal(data, &batch); err != nil {
			continue
		}

		batchMigrations := m.convertBatchToMigrations(&batch)
		migrations = append(migrations, batchMigrations...)
	}

	return migrations, nil
}

// DeleteMigration removes a migration (useful for cleanup)
func (m *MigrationRepositoryKV) DeleteMigration(version *MigrationVersion) error {
	// Key: meta:migration:{table}:version:{version}:node:{nodeId}:table_version:{table_version}
	key := kv.NewKeyBuilder().Meta().Append("migration").
		Append(version.GetTableName()).
		Append("version").Append(fmt.Sprintf("%d", version.GetMigrationVersion())).
		Append("node").Append(fmt.Sprintf("%d", version.GetNodeId())).
		Append("table_version").Append(fmt.Sprintf("%d", version.GetTableVersion())).
		Build()

	txn, err := m.store.Begin(true)
	if err != nil {
		return fmt.Errorf("failed to begin write transaction: %w", err)
	}
	defer txn.Discard()

	// Delete main migration record
	if err := txn.Delete(m.ctx, key); err != nil {
		return fmt.Errorf("failed to delete migration: %w", err)
	}

	// Delete index entries
	tableIndexKey := kv.NewKeyBuilder().Meta().Append("index").Append("migration").
		Append("table").Append(version.GetTableName()).
		Append(fmt.Sprintf("%d", version.GetMigrationVersion())).
		Append(fmt.Sprintf("%d", version.GetNodeId())).
		Build()
	txn.Delete(m.ctx, tableIndexKey)

	uncommittedIndexKey := kv.NewKeyBuilder().Meta().Append("index").Append("migration").
		Append("uncommitted").Append(version.GetTableName()).
		Append(fmt.Sprintf("%d", version.GetMigrationVersion())).
		Append(fmt.Sprintf("%d", version.GetNodeId())).
		Build()
	txn.Delete(m.ctx, uncommittedIndexKey)

	return txn.Commit()
}
