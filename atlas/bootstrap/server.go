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

package bootstrap

import (
	"fmt"

	"google.golang.org/protobuf/proto"

	"github.com/bottledcode/atlas-db/atlas/kv"
)

type Server struct {
	UnimplementedBootstrapServer
}

func (b *Server) GetBootstrapData(request *BootstrapRequest, stream Bootstrap_GetBootstrapDataServer) (err error) {
	if request.GetVersion() != 1 {
		return stream.Send(&BootstrapResponse{
			Response: &BootstrapResponse_IncompatibleVersion{
				IncompatibleVersion: &IncompatibleVersion{
					NeedsVersion: 1,
				},
			},
		})
	}

	// Get KV store pools
	kvPool := kv.GetPool()
	if kvPool == nil {
		return fmt.Errorf("KV pool not initialized")
	}

	// Create database snapshot containing both metadata and data
	snapshot := &DatabaseSnapshot{
		MetaEntries: make([]*KVEntry, 0),
		DataEntries: make([]*KVEntry, 0),
	}

	// Capture metadata store state (consensus tables, nodes, migrations, etc.)
	metaStore := kvPool.MetaStore()
	if metaStore != nil {
		err = b.captureStoreSnapshot(metaStore, &snapshot.MetaEntries)
		if err != nil {
			return fmt.Errorf("failed to capture metadata snapshot: %w", err)
		}
	}

	// Capture data store state (user data)
	dataStore := kvPool.DataStore()
	if dataStore != nil {
		err = b.captureStoreSnapshot(dataStore, &snapshot.DataEntries)
		if err != nil {
			return fmt.Errorf("failed to capture data snapshot: %w", err)
		}
	}

	// Stream the snapshot in chunks to the joining node
	snapshotData, err := proto.Marshal(snapshot)
	if err != nil {
		return fmt.Errorf("failed to marshal database snapshot: %w", err)
	}

	// Stream data in chunks to avoid overwhelming the network
	// The given size should avoid packet fragmentation in most networks.
	const chunkSize = 1400
	totalSize := len(snapshotData)

	for offset := 0; offset < totalSize; offset += chunkSize {
		end := min(offset+chunkSize, totalSize)

		chunk := snapshotData[offset:end]
		response := &BootstrapResponse{
			Response: &BootstrapResponse_BootstrapData{
				BootstrapData: &BootstrapData{
					Version: 1,
					Data:    chunk,
				},
			},
		}

		err = stream.Send(response)
		if err != nil {
			return fmt.Errorf("failed to send bootstrap chunk: %w", err)
		}
	}

	// Send empty chunk to signal end of stream
	return stream.Send(&BootstrapResponse{
		Response: &BootstrapResponse_BootstrapData{
			BootstrapData: &BootstrapData{
				Version: 1,
				Data:    []byte{},
			},
		},
	})
}

// captureStoreSnapshot captures all key-value pairs from a store
func (b *Server) captureStoreSnapshot(store kv.Store, entries *[]*KVEntry) error {
	// Create iterator to scan all keys
	iter := store.NewIterator(kv.IteratorOptions{
		PrefetchValues: true,
		PrefetchSize:   100,
	})
	defer func() { _ = iter.Close() }()

	// Iterate through all key-value pairs
	for iter.Rewind(); iter.Valid(); iter.Next() {
		item := iter.Item()

		// Get key and value
		key := item.KeyCopy()
		value, err := item.ValueCopy()
		if err != nil {
			return fmt.Errorf("failed to read value for key %s: %w", string(key), err)
		}

		// Add to entries
		*entries = append(*entries, &KVEntry{
			Key:   key,
			Value: value,
		})
	}

	return nil
}
