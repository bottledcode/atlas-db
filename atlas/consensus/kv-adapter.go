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
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/bottledcode/atlas-db/atlas"
	"github.com/bottledcode/atlas-db/atlas/kv"
	"go.uber.org/zap"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// KVConsensusAdapter provides a clean interface for KV operations using the existing w-paxos consensus
type KVConsensusAdapter struct {
	server    *Server
	kvStore   kv.Store
	nodeID    int64
	region    string
	address   string
	port      int64
}

// NewKVConsensusAdapter creates a new KV consensus adapter
func NewKVConsensusAdapter(server *Server, kvStore kv.Store) *KVConsensusAdapter {
	return &KVConsensusAdapter{
		server:  server,
		kvStore: kvStore,
		nodeID:  atlas.CurrentOptions.ServerId,
		region:  atlas.CurrentOptions.Region,
		address: atlas.CurrentOptions.AdvertiseAddress,
		port:    int64(atlas.CurrentOptions.AdvertisePort),
	}
}

// KVChange represents a change to a key-value pair for replication
type KVChange struct {
	Operation string `json:"operation"` // "SET" or "DEL"
	Key       string `json:"key"`
	Value     []byte `json:"value,omitempty"` // nil for DEL
	Timestamp int64  `json:"timestamp"`
}

// SetKey implements distributed SET operation using w-paxos consensus
func (a *KVConsensusAdapter) SetKey(ctx context.Context, key string, value []byte) error {
	atlas.Logger.Debug("KV consensus SET operation", zap.String("key", key), zap.Int("value_size", len(value)))

	// Create table representation for this key
	table := a.keyToTable(key)
	
	// Attempt to steal ownership (PREPARE phase of w-paxos)
	ownership, err := a.stealKeyOwnership(ctx, table)
	if err != nil {
		return fmt.Errorf("failed to steal key ownership: %w", err)
	}

	if !ownership.Promised {
		return fmt.Errorf("ownership steal rejected for key %s", key)
	}

	// Create migration representing this SET operation
	kvChange := &KVChange{
		Operation: "SET",
		Key:       key,
		Value:     value,
		Timestamp: time.Now().UnixNano(),
	}

	changeData, err := json.Marshal(kvChange)
	if err != nil {
		return fmt.Errorf("failed to marshal KV change: %w", err)
	}

	migration := &Migration{
		Version: &MigrationVersion{
			TableVersion:     table.Version,
			MigrationVersion: 1, // Always 1 for KV operations (no migration history per key)
			NodeId:           a.nodeID,
			TableName:        key, // Use key as table name
		},
		Migration: &Migration_Data{
			Data: &DataMigration{
				Session: [][]byte{changeData},
			},
		},
	}

	// Write migration (ACCEPT phase of w-paxos)
	writeReq := &WriteMigrationRequest{
		Sender:    a.constructCurrentNode(),
		Migration: migration,
	}

	resp, err := a.server.WriteMigration(ctx, writeReq)
	if err != nil {
		return fmt.Errorf("failed to write migration: %w", err)
	}

	if !resp.Success {
		return fmt.Errorf("migration write rejected for key %s", key)
	}

	// Apply the migration locally
	_, err = a.server.AcceptMigration(ctx, writeReq)
	if err != nil {
		return fmt.Errorf("failed to accept migration: %w", err)
	}

	atlas.Logger.Debug("KV consensus SET completed", zap.String("key", key))
	return nil
}

// GetKey implements distributed GET operation with freshness checking
func (a *KVConsensusAdapter) GetKey(ctx context.Context, key string) ([]byte, error) {
	atlas.Logger.Debug("KV consensus GET operation", zap.String("key", key))

	// For GET operations, we first try to read from local store
	// The consensus layer will handle freshness and forwarding as needed
	
	// Check if we have the key locally
	keyBytes := []byte(key)
	value, err := a.kvStore.Get(ctx, keyBytes)
	if err != nil {
		if errors.Is(err, kv.ErrKeyNotFound) {
			return nil, nil // Key not found
		}
		return nil, fmt.Errorf("failed to get key from local store: %w", err)
	}

	// TODO: Add freshness check through consensus layer
	// For now, return local value (this will be enhanced when we integrate
	// the gossip freshness tracking)

	atlas.Logger.Debug("KV consensus GET completed", zap.String("key", key), zap.Int("value_size", len(value)))
	return value, nil
}

// DeleteKey implements distributed DELETE operation using w-paxos consensus
func (a *KVConsensusAdapter) DeleteKey(ctx context.Context, key string) error {
	atlas.Logger.Debug("KV consensus DELETE operation", zap.String("key", key))

	// Similar to SetKey but with DELETE operation
	table := a.keyToTable(key)
	
	// Steal ownership (PREPARE phase)
	ownership, err := a.stealKeyOwnership(ctx, table)
	if err != nil {
		return fmt.Errorf("failed to steal key ownership: %w", err)
	}

	if !ownership.Promised {
		return fmt.Errorf("ownership steal rejected for key %s", key)
	}

	// Create DELETE migration
	kvChange := &KVChange{
		Operation: "DEL",
		Key:       key,
		Value:     nil,
		Timestamp: time.Now().UnixNano(),
	}

	changeData, err := json.Marshal(kvChange)
	if err != nil {
		return fmt.Errorf("failed to marshal KV change: %w", err)
	}

	migration := &Migration{
		Version: &MigrationVersion{
			TableVersion:     table.Version,
			MigrationVersion: 1,
			NodeId:           a.nodeID,
			TableName:        key,
		},
		Migration: &Migration_Data{
			Data: &DataMigration{
				Session: [][]byte{changeData},
			},
		},
	}

	// Write and accept migration
	writeReq := &WriteMigrationRequest{
		Sender:    a.constructCurrentNode(),
		Migration: migration,
	}

	resp, err := a.server.WriteMigration(ctx, writeReq)
	if err != nil {
		return fmt.Errorf("failed to write migration: %w", err)
	}

	if !resp.Success {
		return fmt.Errorf("migration write rejected for key %s", key)
	}

	_, err = a.server.AcceptMigration(ctx, writeReq)
	if err != nil {
		return fmt.Errorf("failed to accept migration: %w", err)
	}

	atlas.Logger.Debug("KV consensus DELETE completed", zap.String("key", key))
	return nil
}

// keyToTable converts a KV key to a Table representation for consensus
func (a *KVConsensusAdapter) keyToTable(key string) *Table {
	// Determine replication level based on key prefix
	replicationLevel := a.determineReplicationLevel(key)
	
	return &Table{
		Name:             key,
		ReplicationLevel: replicationLevel,
		Owner: &Node{
			Id:      a.nodeID,
			Address: a.address,
			Port:    a.port,
			Region: &Region{
				Name: a.region,
			},
			Active: true,
		},
		CreatedAt: timestamppb.New(time.Now()),
		Version:   time.Now().UnixNano(), // Use timestamp as version for uniqueness
		Type:      TableType_table,
	}
}

// determineReplicationLevel infers replication level from key patterns
func (a *KVConsensusAdapter) determineReplicationLevel(key string) ReplicationLevel {
	// Simple heuristic based on key patterns
	if strings.HasPrefix(key, "global:") {
		return ReplicationLevel_global
	}
	if strings.HasPrefix(key, "regional:") || strings.HasPrefix(key, "region:") {
		return ReplicationLevel_regional
	}
	// Default to regional for better consistency
	return ReplicationLevel_regional
}

// stealKeyOwnership attempts to steal ownership of a key (PREPARE phase of w-paxos)
func (a *KVConsensusAdapter) stealKeyOwnership(ctx context.Context, table *Table) (*StealTableOwnershipResponse, error) {
	req := &StealTableOwnershipRequest{
		Sender: a.constructCurrentNode(),
		Table:  table,
		Reason: StealReason_queryReason, // Using queryReason for KV operations
	}

	return a.server.StealTableOwnership(ctx, req)
}

// constructCurrentNode creates a Node representation of the current node
func (a *KVConsensusAdapter) constructCurrentNode() *Node {
	return &Node{
		Id:      a.nodeID,
		Address: a.address,
		Port:    a.port,
		Region: &Region{
			Name: a.region,
		},
		Active: true,
	}
}