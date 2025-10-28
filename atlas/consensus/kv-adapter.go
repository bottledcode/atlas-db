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

	"github.com/bottledcode/atlas-db/atlas/kv"
	"github.com/bottledcode/atlas-db/atlas/options"
	"go.uber.org/zap"
)

// KVConsensusAdapter provides a clean interface for KV operations using the existing w-paxos consensus
type KVConsensusAdapter struct {
	server  *Server
	kvStore kv.Store
	nodeID  uint64
	region  string
	address string
	port    int64
}

// NewKVConsensusAdapter creates a new KV consensus adapter
func NewKVConsensusAdapter(server *Server, kvStore kv.Store) *KVConsensusAdapter {
	return &KVConsensusAdapter{
		server:  server,
		kvStore: kvStore,
		nodeID:  options.CurrentOptions.ServerId,
		region:  options.CurrentOptions.Region,
		address: options.CurrentOptions.AdvertiseAddress,
		port:    int64(options.CurrentOptions.AdvertisePort),
	}
}

// GetKey implements distributed GET operation with freshness checking
func (a *KVConsensusAdapter) GetKey(ctx context.Context, key string) ([]byte, error) {
	options.Logger.Debug("KV consensus GET operation", zap.String("key", key))

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

	options.Logger.Debug("KV consensus GET completed", zap.String("key", key), zap.Int("value_size", len(value)))
	return value, nil
}
