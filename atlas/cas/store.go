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

package cas

import (
	"context"
	"crypto/sha256"
	"fmt"
	"io"

	"github.com/bottledcode/atlas-db/atlas/consensus"
)

const (
	// ChunkSize is the size of each chunk for splitting large values
	ChunkSize = 1024 * 1024 // 1MB
)

// Store provides content-addressable storage for Atlas-DB.
// This is a thin client wrapper that chunks data and makes RPC calls.
// Actual storage happens on the server side via the consensus layer.
type Store struct {
	quorumMgr consensus.QuorumManager
}

// NewStore creates a new CAS store
func NewStore(quorumMgr consensus.QuorumManager) *Store {
	return &Store{
		quorumMgr: quorumMgr,
	}
}

// Put stores data in the CAS and returns its hash.
// The data is chunked and replicated to all nodes in the cluster via RPC.
// This operation is idempotent - storing the same data multiple times is safe.
// No data is stored locally; everything goes directly to the server.
func (s *Store) Put(ctx context.Context, key []byte, data []byte) (hash []byte, err error) {
	// Compute hash
	h := sha256.Sum256(data)
	hash = h[:]

	// Replicate to all nodes (broadcast) - no local storage
	bq, err := s.quorumMgr.GetBroadcastQuorum(ctx, key)
	if err != nil {
		return hash, fmt.Errorf("failed to get broadcast quorum: %w", err)
	}

	stream, err := bq.Replicate(ctx)
	if err != nil {
		return hash, fmt.Errorf("failed to create a replication stream: %w", err)
	}

	// Split into chunks and send sequentially (gRPC streams are not thread-safe)
	chunks := s.splitIntoChunks(data)
	for i, chunk := range chunks {
		err = stream.Send(&consensus.ReplicationRequest{
			Data: &consensus.Data{
				Key:   hash,
				Value: chunk,
				Chunk: uint64(i),
			}})
		if err != nil {
			return hash, fmt.Errorf("replication failed: %w", err)
		}
	}

	// Close the stream
	_, err = stream.CloseAndRecv()
	if err != nil {
		return hash, fmt.Errorf("replication failed: %w", err)
	}

	return hash, nil
}

// Get retrieves data by its hash from the cluster.
// It fetches chunks from peers using DeReference RPC and reassembles them.
func (s *Store) Get(ctx context.Context, hash []byte) ([]byte, error) {
	// Get a quorum to query from
	bq, err := s.quorumMgr.GetBroadcastQuorum(ctx, hash)
	if err != nil {
		return nil, fmt.Errorf("failed to get broadcast quorum: %w", err)
	}

	// Call DeReference RPC to fetch chunks
	stream, err := bq.DeReference(ctx, &consensus.DereferenceRequest{
		Reference: &consensus.DataReference{
			Address: hash,
		},
	})
	if err != nil {
		return nil, fmt.Errorf("failed to dereference: %w", err)
	}

	// Receive and reassemble chunks
	chunks := make(map[uint64][]byte)
	var maxChunk uint64

	for {
		resp, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("failed to receive chunk: %w", err)
		}

		data := resp.GetData()
		chunkID := data.GetChunk()
		chunks[chunkID] = data.GetValue()

		if chunkID > maxChunk {
			maxChunk = chunkID
		}
	}

	// Reassemble chunks in order
	if len(chunks) == 0 {
		return nil, fmt.Errorf("hash not found: %x", hash)
	}

	var result []byte
	for i := uint64(0); i <= maxChunk; i++ {
		chunk, ok := chunks[i]
		if !ok {
			return nil, fmt.Errorf("missing chunk %d when reassembling %x", i, hash)
		}
		result = append(result, chunk...)
	}

	return result, nil
}

// splitIntoChunks splits data into fixed-size chunks for transmission
func (s *Store) splitIntoChunks(data []byte) [][]byte {
	var chunks [][]byte
	for i := 0; i < len(data); i += ChunkSize {
		end := min(i+ChunkSize, len(data))
		chunks = append(chunks, data[i:end])
	}
	return chunks
}

// ComputeHash computes the SHA-256 hash of data
func ComputeHash(data []byte) []byte {
	h := sha256.Sum256(data)
	return h[:]
}
