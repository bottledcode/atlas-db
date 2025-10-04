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
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/bottledcode/atlas-db/atlas/kv"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// TableRepositoryKV implements TableRepository using key-value operations
type TableRepositoryKV struct {
	store kv.Store
	ctx   context.Context
}

// NewTableRepositoryKV creates a new KV-based table repository
func NewTableRepositoryKV(ctx context.Context, store kv.Store) TableRepository {
	return &TableRepositoryKV{
		store: store,
		ctx:   ctx,
	}
}

// TableStorageModel represents how table data is stored in KV format
type TableStorageModel struct {
	Name              string            `json:"name"`
	Version           int64             `json:"version"`
	ReplicationLevel  string            `json:"replication_level"`
	AllowedRegions    []string          `json:"allowed_regions"`
	RestrictedRegions []string          `json:"restricted_regions"`
	OwnerNodeID       *int64            `json:"owner_node_id"`
	CreatedAt         time.Time         `json:"created_at"`
	GroupID           string            `json:"group_id,omitempty"`
	TableType         string            `json:"table_type"`
	ShardPrincipals   []string          `json:"shard_principals"`
	Owner             *NodeStorageModel `json:"owner,omitempty"`
}

// NodeStorageModel represents node data in KV storage
type NodeStorageModel struct {
	ID        int64     `json:"id"`
	Address   string    `json:"address"`
	Port      int64     `json:"port"`
	Region    string    `json:"region"`
	Active    bool      `json:"active"`
	RTT       int64     `json:"rtt_nanoseconds"`
	CreatedAt time.Time `json:"created_at"`
}

func (r *TableRepositoryKV) GetTable(name string) (*Table, error) {
	// Key: meta:table:{table_name}
	key := kv.NewKeyBuilder().Meta().Append("table").Append(name).Build()

	txn, err := r.store.Begin(false)
	if err != nil {
		return nil, fmt.Errorf("failed to begin read transaction: %w", err)
	}
	defer txn.Discard()

	data, err := txn.Get(r.ctx, key)
	if err != nil {
		if errors.Is(err, kv.ErrKeyNotFound) {
			return nil, nil
		}
		return nil, fmt.Errorf("failed to get table %s: %w", name, err)
	}

	var storageModel TableStorageModel
	if err := json.Unmarshal(data, &storageModel); err != nil {
		return nil, fmt.Errorf("failed to unmarshal table data: %w", err)
	}

	// If table has an owner, fetch node details
	if storageModel.OwnerNodeID != nil {
		nodeData, err := r.getNodeByID(*storageModel.OwnerNodeID)
		if err == nil && nodeData != nil {
			storageModel.Owner = nodeData
		}
	}

	return r.convertStorageModelToTable(&storageModel), nil
}

func (r *TableRepositoryKV) GetTablesBatch(names []string) ([]*Table, error) {
	if len(names) == 0 {
		return []*Table{}, nil
	}

	txn, err := r.store.Begin(false)
	if err != nil {
		return nil, fmt.Errorf("failed to begin read transaction: %w", err)
	}
	defer txn.Discard()

	// Build result slice with same length as input, pre-filled with nil
	results := make([]*Table, len(names))

	// Track unique node IDs we need to fetch
	nodeIDs := make(map[int64]bool)

	// First pass: fetch all table data
	storageModels := make([]*TableStorageModel, len(names))
	for i, name := range names {
		key := kv.NewKeyBuilder().Meta().Append("table").Append(name).Build()

		data, err := txn.Get(r.ctx, key)
		if err != nil {
			if errors.Is(err, kv.ErrKeyNotFound) {
				// Keep nil in results[i]
				continue
			}
			return nil, fmt.Errorf("failed to get table %s: %w", name, err)
		}

		var storageModel TableStorageModel
		if err := json.Unmarshal(data, &storageModel); err != nil {
			return nil, fmt.Errorf("failed to unmarshal table data for %s: %w", name, err)
		}

		storageModels[i] = &storageModel

		// Track node ID if present
		if storageModel.OwnerNodeID != nil {
			nodeIDs[*storageModel.OwnerNodeID] = true
		}
	}

	// Batch fetch all unique owner nodes
	nodeCache := make(map[int64]*NodeStorageModel)
	for nodeID := range nodeIDs {
		nodeData, err := r.getNodeByID(nodeID)
		if err == nil && nodeData != nil {
			nodeCache[nodeID] = nodeData
		}
	}

	// Second pass: attach owner data and convert to Table objects
	for i, storageModel := range storageModels {
		if storageModel == nil {
			continue
		}

		if storageModel.OwnerNodeID != nil {
			if nodeData, found := nodeCache[*storageModel.OwnerNodeID]; found {
				storageModel.Owner = nodeData
			}
		}

		results[i] = r.convertStorageModelToTable(storageModel)
	}

	return results, nil
}

func (r *TableRepositoryKV) getNodeByID(nodeID int64) (*NodeStorageModel, error) {
	// Key: meta:node:{node_id}
	key := kv.NewKeyBuilder().Meta().Append("node").Append(fmt.Sprintf("%d", nodeID)).Build()

	data, err := r.store.Get(r.ctx, key)
	if err != nil {
		if errors.Is(err, kv.ErrKeyNotFound) {
			return nil, nil
		}
		return nil, err
	}

	var node NodeStorageModel
	if err := json.Unmarshal(data, &node); err != nil {
		return nil, err
	}

	return &node, nil
}

func (r *TableRepositoryKV) convertStorageModelToTable(model *TableStorageModel) *Table {
	table := &Table{
		Name:              model.Name,
		Version:           model.Version,
		ReplicationLevel:  ReplicationLevel(ReplicationLevel_value[model.ReplicationLevel]),
		AllowedRegions:    model.AllowedRegions,
		RestrictedRegions: model.RestrictedRegions,
		CreatedAt:         timestamppb.New(model.CreatedAt),
		Type:              TableType(TableType_value[model.TableType]),
		Group:             model.GroupID,
		ShardPrincipals:   model.ShardPrincipals,
	}

	if model.Owner != nil {
		table.Owner = &Node{
			Id:      model.Owner.ID,
			Address: model.Owner.Address,
			Port:    model.Owner.Port,
			Region:  &Region{Name: model.Owner.Region},
			Active:  model.Owner.Active,
			Rtt:     durationpb.New(time.Duration(model.Owner.RTT)),
		}
	}

	return table
}

func (r *TableRepositoryKV) convertTableToStorageModel(table *Table) *TableStorageModel {
	model := &TableStorageModel{
		Name:              table.Name,
		Version:           table.Version,
		ReplicationLevel:  table.ReplicationLevel.String(),
		AllowedRegions:    table.AllowedRegions,
		RestrictedRegions: table.RestrictedRegions,
		CreatedAt:         table.CreatedAt.AsTime(),
		GroupID:           table.Group,
		TableType:         table.Type.String(),
		ShardPrincipals:   table.ShardPrincipals,
	}

	if table.Owner != nil {
		model.OwnerNodeID = &table.Owner.Id
		model.Owner = &NodeStorageModel{
			ID:      table.Owner.Id,
			Address: table.Owner.Address,
			Port:    table.Owner.Port,
			Region:  table.Owner.Region.Name,
			Active:  table.Owner.Active,
			RTT:     table.Owner.Rtt.AsDuration().Nanoseconds(),
		}
	}

	return model
}

func (r *TableRepositoryKV) UpdateTable(table *Table) error {
	// Key: meta:table:{table_name}
	key := kv.NewKeyBuilder().Meta().Append("table").Append(table.Name).Build()

	txn, err := r.store.Begin(true)
	if err != nil {
		return fmt.Errorf("failed to begin write transaction: %w", err)
	}
	defer txn.Discard()

	// Get the existing table data to identify old index keys to clean up
	existingData, err := txn.Get(r.ctx, key)
	if err != nil {
		if errors.Is(err, kv.ErrKeyNotFound) {
			return fmt.Errorf("table %s does not exist", table.Name)
		}
		return fmt.Errorf("failed to check table existence: %w", err)
	}

	// Parse existing table to get old index values
	var existingStorageModel TableStorageModel
	if err := json.Unmarshal(existingData, &existingStorageModel); err != nil {
		return fmt.Errorf("failed to unmarshal existing table data: %w", err)
	}
	existingTable := r.convertStorageModelToTable(&existingStorageModel)

	// Remove old index entries before updating
	if err := r.removeTableIndex(txn, existingTable); err != nil {
		return fmt.Errorf("failed to remove old table index: %w", err)
	}

	// Update the table data
	storageModel := r.convertTableToStorageModel(table)
	data, err := json.Marshal(storageModel)
	if err != nil {
		return fmt.Errorf("failed to marshal table data: %w", err)
	}

	if err := txn.Put(r.ctx, key, data); err != nil {
		return fmt.Errorf("failed to update table: %w", err)
	}

	// Add new index entries
	if err := r.updateTableIndex(txn, table); err != nil {
		return fmt.Errorf("failed to update table index: %w", err)
	}

	return txn.Commit()
}

func (r *TableRepositoryKV) InsertTable(table *Table) error {
	// Key: meta:table:{table_name}
	key := kv.NewKeyBuilder().Meta().Append("table").Append(table.Name).Build()

	txn, err := r.store.Begin(true)
	if err != nil {
		return fmt.Errorf("failed to begin write transaction: %w", err)
	}
	defer txn.Discard()

	// Check if table already exists
	_, err = txn.Get(r.ctx, key)
	if err == nil {
		return fmt.Errorf("table %s already exists", table.Name)
	}
	if !errors.Is(err, kv.ErrKeyNotFound) {
		return fmt.Errorf("failed to check table existence: %w", err)
	}

	storageModel := r.convertTableToStorageModel(table)
	data, err := json.Marshal(storageModel)
	if err != nil {
		return fmt.Errorf("failed to marshal table data: %w", err)
	}

	if err := txn.Put(r.ctx, key, data); err != nil {
		return fmt.Errorf("failed to insert table: %w", err)
	}

	// Update table index for efficient queries
	if err := r.updateTableIndex(txn, table); err != nil {
		return fmt.Errorf("failed to update table index: %w", err)
	}

	return txn.Commit()
}

// updateTableIndex maintains indexes for efficient querying
func (r *TableRepositoryKV) updateTableIndex(txn kv.Transaction, table *Table) error {
	// Index by replication level: meta:index:replication:{level}:{table_name} -> table_name
	indexKey := kv.NewKeyBuilder().Meta().Append("index").Append("replication").
		Append(table.ReplicationLevel.String()).Append(table.Name).Build()

	if err := txn.Put(r.ctx, indexKey, []byte(table.Name)); err != nil {
		return err
	}

	// Index by group if applicable
	if table.Group != "" {
		groupIndexKey := kv.NewKeyBuilder().Meta().Append("index").Append("group").
			Append(table.Group).Append(table.Name).Build()
		if err := txn.Put(r.ctx, groupIndexKey, []byte(table.Name)); err != nil {
			return err
		}
	}

	// Index by owner node if applicable
	if table.Owner != nil {
		ownerIndexKey := kv.NewKeyBuilder().Meta().Append("index").Append("owner").
			Append(fmt.Sprintf("%d", table.Owner.Id)).Append(table.Name).Build()
		if err := txn.Put(r.ctx, ownerIndexKey, []byte(table.Name)); err != nil {
			return err
		}
	}

	return nil
}

// removeTableIndex removes secondary indexes for efficient querying cleanup
func (r *TableRepositoryKV) removeTableIndex(txn kv.Transaction, table *Table) error {
	// Remove replication level index: meta:index:replication:{level}:{table_name}
	indexKey := kv.NewKeyBuilder().Meta().Append("index").Append("replication").
		Append(table.ReplicationLevel.String()).Append(table.Name).Build()

	if err := txn.Delete(r.ctx, indexKey); err != nil {
		return err
	}

	// Remove group index if applicable
	if table.Group != "" {
		groupIndexKey := kv.NewKeyBuilder().Meta().Append("index").Append("group").
			Append(table.Group).Append(table.Name).Build()
		if err := txn.Delete(r.ctx, groupIndexKey); err != nil {
			return err
		}
	}

	// Remove owner node index if applicable
	if table.Owner != nil {
		ownerIndexKey := kv.NewKeyBuilder().Meta().Append("index").Append("owner").
			Append(fmt.Sprintf("%d", table.Owner.Id)).Append(table.Name).Build()
		if err := txn.Delete(r.ctx, ownerIndexKey); err != nil {
			return err
		}
	}

	return nil
}

func (r *TableRepositoryKV) GetGroup(name string) (*TableGroup, error) {
	// First get the group details (which is just a table with type=group)
	groupTable, err := r.GetTable(name)
	if err != nil {
		return nil, err
	}
	if groupTable == nil {
		return nil, nil
	}

	if groupTable.Type != TableType_group {
		return nil, errors.New("not a group")
	}

	group := &TableGroup{
		Details: groupTable,
		Tables:  make([]*Table, 0),
	}

	// Find all tables in this group using the group index
	// Key pattern: meta:index:group:{group_name}:{table_name} -> table_name
	prefix := kv.NewKeyBuilder().Meta().Append("index").Append("group").Append(name).Build()

	txn, err := r.store.Begin(false)
	if err != nil {
		return nil, fmt.Errorf("failed to begin read transaction: %w", err)
	}
	defer txn.Discard()

	iterator := txn.NewIterator(kv.IteratorOptions{
		Prefix:         prefix,
		PrefetchValues: true,
	})
	defer func() { _ = iterator.Close() }()

	for iterator.Rewind(); iterator.Valid(); iterator.Next() {
		item := iterator.Item()
		tableName, err := item.Value()
		if err != nil {
			continue
		}

		// Skip the group table itself
		if string(tableName) == name {
			continue
		}

		// Get the full table details
		table, err := r.GetTable(string(tableName))
		if err != nil {
			continue // Log this error but continue processing
		}
		if table != nil {
			group.Tables = append(group.Tables, table)
		}
	}

	return group, nil
}

func (r *TableRepositoryKV) UpdateGroup(group *TableGroup) error {
	return r.UpdateTable(group.Details)
}

func (r *TableRepositoryKV) InsertGroup(group *TableGroup) error {
	return r.InsertTable(group.Details)
}

// hashPrincipals uses the same hashing logic as the original SQL implementation
func (r *TableRepositoryKV) hashPrincipals(principals []*Principal, order []string) (string, error) {
	p := make(map[string]string, len(principals))
	for _, principal := range principals {
		p[principal.GetName()] = principal.GetValue()
	}

	var hashStr strings.Builder

	for _, o := range order {
		if v, ok := p[o]; ok {
			hashStr.WriteString(v)
		} else {
			return "", errors.New("missing principal")
		}
	}

	// hash the string using Larson's hash (same as original)
	var hash uint32
	str := hashStr.String()
	for _, b := range str {
		hash = hash*101 + uint32(b)
	}

	// return the hash number as a string (same character set as original)
	chars := []rune{'Z', 'A', 'C', '2', 'B', '3', 'E', 'F', '4', 'G', 'H', '5', 'T', 'K', '6', '7', 'P', '8', 'R', 'S', '9', 'W', 'X', 'Y'}
	hashStr.Reset()
	for {
		hashStr.WriteRune(chars[hash%24])
		hash /= 24
		if hash == 0 {
			break
		}
	}
	return hashStr.String(), nil
}

func (r *TableRepositoryKV) GetShard(shard *Table, principals []*Principal) (*Shard, error) {
	hash, err := r.hashPrincipals(principals, shard.GetShardPrincipals())
	if err != nil {
		return nil, err
	}

	// retrieve the shard table
	st, err := r.GetTable(shard.GetName() + "_" + hash)
	if err != nil {
		return nil, err
	}
	if st == nil {
		return nil, nil
	}

	return &Shard{
		Table:      shard, // Parent table
		Shard:      st,    // Retrieved shard table
		Principals: principals,
	}, nil
}

func (r *TableRepositoryKV) UpdateShard(shard *Shard) error {
	return r.UpdateTable(shard.GetShard())
}

func (r *TableRepositoryKV) InsertShard(shard *Shard) error {
	hash, err := r.hashPrincipals(shard.GetPrincipals(), shard.GetTable().GetShardPrincipals())
	if err != nil {
		return err
	}
	shard.GetShard().Name = shard.GetTable().GetName() + "_" + hash

	return r.InsertTable(shard.GetShard())
}

// GetTablesByReplicationLevel provides efficient querying by replication level
func (r *TableRepositoryKV) GetTablesByReplicationLevel(level ReplicationLevel) ([]*Table, error) {
	// Key pattern: meta:index:replication:{level}:{table_name} -> table_name
	prefix := kv.NewKeyBuilder().Meta().Append("index").Append("replication").
		Append(level.String()).Build()

	txn, err := r.store.Begin(false)
	if err != nil {
		return nil, fmt.Errorf("failed to begin read transaction: %w", err)
	}
	defer txn.Discard()

	iterator := txn.NewIterator(kv.IteratorOptions{
		Prefix:         prefix,
		PrefetchValues: true,
	})
	defer func() { _ = iterator.Close() }()

	var tables []*Table
	for iterator.Rewind(); iterator.Valid(); iterator.Next() {
		item := iterator.Item()
		tableName, err := item.Value()
		if err != nil {
			continue
		}

		table, err := r.GetTable(string(tableName))
		if err != nil {
			continue // Log this error but continue
		}
		if table != nil {
			tables = append(tables, table)
		}
	}

	return tables, nil
}

// GetTablesOwnedByNode provides efficient querying by owner node
func (r *TableRepositoryKV) GetTablesOwnedByNode(nodeID int64) ([]*Table, error) {
	// Key pattern: meta:index:owner:{node_id}:{table_name} -> table_name
	prefix := kv.NewKeyBuilder().Meta().Append("index").Append("owner").
		Append(fmt.Sprintf("%d", nodeID)).Build()

	txn, err := r.store.Begin(false)
	if err != nil {
		return nil, fmt.Errorf("failed to begin read transaction: %w", err)
	}
	defer txn.Discard()

	iterator := txn.NewIterator(kv.IteratorOptions{
		Prefix:         prefix,
		PrefetchValues: true,
	})
	defer func() { _ = iterator.Close() }()

	var tables []*Table
	for iterator.Rewind(); iterator.Valid(); iterator.Next() {
		item := iterator.Item()
		tableName, err := item.Value()
		if err != nil {
			continue
		}

		table, err := r.GetTable(string(tableName))
		if err != nil {
			continue // Log this error but continue
		}
		if table != nil {
			tables = append(tables, table)
		}
	}

	return tables, nil
}
