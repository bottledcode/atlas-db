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
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
	"strconv"
	"time"

	"github.com/bottledcode/atlas-db/atlas/kv"
	"google.golang.org/protobuf/types/known/durationpb"
)

// NodeRepositoryKV implements NodeRepository using key-value operations
type NodeRepositoryKV struct {
	store kv.Store
	ctx   context.Context
}

// NewNodeRepositoryKV creates a new KV-based node repository
func NewNodeRepositoryKV(ctx context.Context, store kv.Store) NodeRepository {
	return &NodeRepositoryKV{
		store: store,
		ctx:   ctx,
	}
}

// NodeStorageModelKV represents how node data is stored in KV format
type NodeStorageModelKV struct {
	ID        int64     `json:"id"`
	Address   string    `json:"address"`
	Port      int64     `json:"port"`
	Region    string    `json:"region"`
	Active    bool      `json:"active"`
	RTT       int64     `json:"rtt_nanoseconds"`
	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`
}

func (n *NodeRepositoryKV) convertStorageModelToNode(model *NodeStorageModelKV) *Node {
	return &Node{
		Id:      model.ID,
		Address: model.Address,
		Port:    model.Port,
		Region: &Region{
			Name: model.Region,
		},
		Active: model.Active,
		Rtt:    durationpb.New(time.Duration(model.RTT)),
	}
}

func (n *NodeRepositoryKV) convertNodeToStorageModel(node *Node) *NodeStorageModelKV {
	now := time.Now()
	return &NodeStorageModelKV{
		ID:        node.Id,
		Address:   node.Address,
		Port:      node.Port,
		Region:    node.Region.Name,
		Active:    node.Active,
		RTT:       node.Rtt.AsDuration().Nanoseconds(),
		CreatedAt: now,
		UpdatedAt: now,
	}
}

func (n *NodeRepositoryKV) GetNodeById(id int64) (*Node, error) {
	// Key: meta:node:{node_id}
	key := kv.NewKeyBuilder().Meta().Append("node").Append(fmt.Sprintf("%d", id)).Build()

	txn, err := n.store.Begin(false)
	if err != nil {
		return nil, fmt.Errorf("failed to begin read transaction: %w", err)
	}
	defer txn.Discard()

	data, err := txn.Get(n.ctx, key)
	if err != nil {
		if errors.Is(err, kv.ErrKeyNotFound) {
			return nil, nil
		}
		return nil, fmt.Errorf("failed to get node %d: %w", id, err)
	}

	var storageModel NodeStorageModelKV
	if err := json.Unmarshal(data, &storageModel); err != nil {
		return nil, fmt.Errorf("failed to unmarshal node data: %w", err)
	}

	return n.convertStorageModelToNode(&storageModel), nil
}

func (n *NodeRepositoryKV) GetNodeByAddress(address string, port uint) (*Node, error) {
	// Use address index: meta:index:node:address:{address}:{port} -> node_id
	indexKey := kv.NewKeyBuilder().Meta().Append("index").Append("node").
		Append("address").Append(address).Append(fmt.Sprintf("%d", port)).Build()

	txn, err := n.store.Begin(false)
	if err != nil {
		return nil, fmt.Errorf("failed to begin read transaction: %w", err)
	}
	defer txn.Discard()

	nodeIDData, err := txn.Get(n.ctx, indexKey)
	if err != nil {
		if errors.Is(err, kv.ErrKeyNotFound) {
			return nil, nil
		}
		return nil, fmt.Errorf("failed to get node address index: %w", err)
	}

	nodeID, err := strconv.ParseInt(string(nodeIDData), 10, 64)
	if err != nil {
		return nil, fmt.Errorf("invalid node ID in index: %w", err)
	}

	return n.GetNodeById(nodeID)
}

func (n *NodeRepositoryKV) GetNodesByRegion(region string) ([]*Node, error) {
	// Use region index: meta:index:node:region:{region}:{node_id} -> node_id
	prefix := kv.NewKeyBuilder().Meta().Append("index").Append("node").
		Append("region").Append(region).Build()

	txn, err := n.store.Begin(false)
	if err != nil {
		return nil, fmt.Errorf("failed to begin read transaction: %w", err)
	}
	defer txn.Discard()

	iterator := txn.NewIterator(kv.IteratorOptions{
		Prefix:         prefix,
		PrefetchValues: true,
	})
	defer iterator.Close()

	var nodes []*Node

	for iterator.Rewind(); iterator.Valid(); iterator.Next() {
		item := iterator.Item()
		nodeIDData, err := item.Value()
		if err != nil {
			continue
		}

		nodeID, err := strconv.ParseInt(string(nodeIDData), 10, 64)
		if err != nil {
			continue
		}

		node, err := n.GetNodeById(nodeID)
		if err != nil {
			continue
		}

		// Only return active nodes
		if node != nil && node.Active {
			nodes = append(nodes, node)
		}
	}

	return nodes, nil
}

func (n *NodeRepositoryKV) GetRegions() ([]*Region, error) {
	// Scan region index for unique regions: meta:index:node:region:{region}:{node_id}
	prefix := kv.NewKeyBuilder().Meta().Append("index").Append("node").
		Append("region").Build()

	txn, err := n.store.Begin(false)
	if err != nil {
		return nil, fmt.Errorf("failed to begin read transaction: %w", err)
	}
	defer txn.Discard()

	iterator := txn.NewIterator(kv.IteratorOptions{
		Prefix:         prefix,
		PrefetchValues: false,
	})
	defer iterator.Close()

	regionMap := make(map[string]bool)

	for iterator.Rewind(); iterator.Valid(); iterator.Next() {
		item := iterator.Item()
		key := string(item.Key())

		// Parse region from key: meta:index:node:region:{region}:{node_id}
		parts := splitKey(key)
		if len(parts) >= 6 && parts[5] != "" {
			region := parts[5]
			regionMap[region] = true
		}
	}

	regions := make([]*Region, 0, len(regionMap))
	for regionName := range regionMap {
		regions = append(regions, &Region{Name: regionName})
	}

	return regions, nil
}

func (n *NodeRepositoryKV) Iterate(fn func(*Node) error) error {
	// Scan active nodes: meta:index:node:active:true:{node_id} -> node_id
	prefix := kv.NewKeyBuilder().Meta().Append("index").Append("node").
		Append("active").Append("true").Build()

	txn, err := n.store.Begin(false)
	if err != nil {
		return fmt.Errorf("failed to begin read transaction: %w", err)
	}
	defer txn.Discard()

	iterator := txn.NewIterator(kv.IteratorOptions{
		Prefix:         prefix,
		PrefetchValues: true,
	})
	defer iterator.Close()

	for iterator.Rewind(); iterator.Valid(); iterator.Next() {
		item := iterator.Item()
		nodeIDData, err := item.Value()
		if err != nil {
			continue
		}

		nodeID, err := strconv.ParseInt(string(nodeIDData), 10, 64)
		if err != nil {
			continue
		}

		node, err := n.GetNodeById(nodeID)
		if err != nil {
			continue
		}

		if node != nil {
			if err := fn(node); err != nil {
				return err
			}
		}
	}

	return nil
}

func (n *NodeRepositoryKV) TotalCount() (int64, error) {
	// Count active nodes by scanning the active index
	prefix := kv.NewKeyBuilder().Meta().Append("index").Append("node").
		Append("active").Append("true").Build()

	txn, err := n.store.Begin(false)
	if err != nil {
		return 0, fmt.Errorf("failed to begin read transaction: %w", err)
	}
	defer txn.Discard()

	iterator := txn.NewIterator(kv.IteratorOptions{
		Prefix:         prefix,
		PrefetchValues: false,
	})
	defer iterator.Close()

	count := int64(0)
	for iterator.Rewind(); iterator.Valid(); iterator.Next() {
		count++
	}

	return count, nil
}

func (n *NodeRepositoryKV) GetRandomNodes(num int64, excluding ...int64) ([]*Node, error) {
	// Get all active node IDs first
	prefix := kv.NewKeyBuilder().Meta().Append("index").Append("node").
		Append("active").Append("true").Build()

	txn, err := n.store.Begin(false)
	if err != nil {
		return nil, fmt.Errorf("failed to begin read transaction: %w", err)
	}
	defer txn.Discard()

	iterator := txn.NewIterator(kv.IteratorOptions{
		Prefix:         prefix,
		PrefetchValues: true,
	})
	defer iterator.Close()

	var candidateIDs []int64
	excludeMap := make(map[int64]bool)
	for _, id := range excluding {
		excludeMap[id] = true
	}

	for iterator.Rewind(); iterator.Valid(); iterator.Next() {
		item := iterator.Item()
		nodeIDData, err := item.Value()
		if err != nil {
			continue
		}

		nodeID, err := strconv.ParseInt(string(nodeIDData), 10, 64)
		if err != nil {
			continue
		}

		// Skip excluded nodes
		if !excludeMap[nodeID] {
			candidateIDs = append(candidateIDs, nodeID)
		}
	}

	// Randomly select nodes
	if int64(len(candidateIDs)) <= num {
		// Return all candidates if we don't have enough
		nodes := make([]*Node, 0, len(candidateIDs))
		for _, nodeID := range candidateIDs {
			node, err := n.GetNodeById(nodeID)
			if err != nil {
				continue
			}
			if node != nil {
				nodes = append(nodes, node)
			}
		}
		return nodes, nil
	}

	// Fisher-Yates shuffle to get random selection
	selectedIDs := make([]int64, num)
	candidatesCopy := make([]int64, len(candidateIDs))
	copy(candidatesCopy, candidateIDs)

	for i := int64(0); i < num; i++ {
		remaining := int64(len(candidatesCopy)) - i
		randIndex, err := rand.Int(rand.Reader, big.NewInt(remaining))
		if err != nil {
			return nil, fmt.Errorf("failed to generate random number: %w", err)
		}

		idx := randIndex.Int64()
		selectedIDs[i] = candidatesCopy[i+idx]

		// Swap selected element to the end
		candidatesCopy[i+idx], candidatesCopy[i] = candidatesCopy[i], candidatesCopy[i+idx]
	}

	// Get the actual nodes
	nodes := make([]*Node, 0, num)
	for _, nodeID := range selectedIDs {
		node, err := n.GetNodeById(nodeID)
		if err != nil {
			continue
		}
		if node != nil {
			nodes = append(nodes, node)
		}
	}

	return nodes, nil
}

// AddNode adds a new node to the repository
func (n *NodeRepositoryKV) AddNode(node *Node) error {
	// Key: meta:node:{node_id}
	key := kv.NewKeyBuilder().Meta().Append("node").Append(fmt.Sprintf("%d", node.Id)).Build()

	txn, err := n.store.Begin(true)
	if err != nil {
		return fmt.Errorf("failed to begin write transaction: %w", err)
	}
	defer txn.Discard()

	// Check if node already exists
	if _, err := txn.Get(n.ctx, key); err == nil {
		return fmt.Errorf("node with ID %d already exists", node.Id)
	} else if !errors.Is(err, kv.ErrKeyNotFound) {
		return fmt.Errorf("failed to check node existence: %w", err)
	}

	storageModel := n.convertNodeToStorageModel(node)
	data, err := json.Marshal(storageModel)
	if err != nil {
		return fmt.Errorf("failed to marshal node data: %w", err)
	}

	if err := txn.Put(n.ctx, key, data); err != nil {
		return fmt.Errorf("failed to store node: %w", err)
	}

	// Update indexes
	if err := n.updateNodeIndexes(txn, node); err != nil {
		return fmt.Errorf("failed to update node indexes: %w", err)
	}

	return txn.Commit()
}

// UpdateNode updates an existing node
func (n *NodeRepositoryKV) UpdateNode(node *Node) error {
	// Key: meta:node:{node_id}
	key := kv.NewKeyBuilder().Meta().Append("node").Append(fmt.Sprintf("%d", node.Id)).Build()

	txn, err := n.store.Begin(true)
	if err != nil {
		return fmt.Errorf("failed to begin write transaction: %w", err)
	}
	defer txn.Discard()

	// Check if node exists
	oldData, err := txn.Get(n.ctx, key)
	if err != nil {
		if errors.Is(err, kv.ErrKeyNotFound) {
			return fmt.Errorf("node with ID %d does not exist", node.Id)
		}
		return fmt.Errorf("failed to get existing node: %w", err)
	}

	// Get old node for index cleanup
	var oldStorageModel NodeStorageModelKV
	if err := json.Unmarshal(oldData, &oldStorageModel); err != nil {
		return fmt.Errorf("failed to unmarshal old node data: %w", err)
	}
	oldNode := n.convertStorageModelToNode(&oldStorageModel)

	// Remove old indexes
	if err := n.removeNodeIndexes(txn, oldNode); err != nil {
		return fmt.Errorf("failed to remove old node indexes: %w", err)
	}

	storageModel := n.convertNodeToStorageModel(node)
	storageModel.CreatedAt = oldStorageModel.CreatedAt // Preserve creation time
	storageModel.UpdatedAt = time.Now()                // Update modification time

	data, err := json.Marshal(storageModel)
	if err != nil {
		return fmt.Errorf("failed to marshal node data: %w", err)
	}

	if err := txn.Put(n.ctx, key, data); err != nil {
		return fmt.Errorf("failed to update node: %w", err)
	}

	// Update indexes with new values
	if err := n.updateNodeIndexes(txn, node); err != nil {
		return fmt.Errorf("failed to update node indexes: %w", err)
	}

	return txn.Commit()
}

// updateNodeIndexes maintains indexes for efficient node queries
func (n *NodeRepositoryKV) updateNodeIndexes(txn kv.Transaction, node *Node) error {
	nodeIDStr := fmt.Sprintf("%d", node.Id)

	// Index by address: meta:index:node:address:{address}:{port} -> node_id
	addressKey := kv.NewKeyBuilder().Meta().Append("index").Append("node").
		Append("address").Append(node.Address).Append(fmt.Sprintf("%d", node.Port)).Build()
	if err := txn.Put(n.ctx, addressKey, []byte(nodeIDStr)); err != nil {
		return err
	}

	// Index by region: meta:index:node:region:{region}:{node_id} -> node_id
	regionKey := kv.NewKeyBuilder().Meta().Append("index").Append("node").
		Append("region").Append(node.Region.Name).Append(nodeIDStr).Build()
	if err := txn.Put(n.ctx, regionKey, []byte(nodeIDStr)); err != nil {
		return err
	}

	// Index by active status: meta:index:node:active:{active}:{node_id} -> node_id
	activeStr := "false"
	if node.Active {
		activeStr = "true"
	}
	activeKey := kv.NewKeyBuilder().Meta().Append("index").Append("node").
		Append("active").Append(activeStr).Append(nodeIDStr).Build()
	if err := txn.Put(n.ctx, activeKey, []byte(nodeIDStr)); err != nil {
		return err
	}

	return nil
}

// removeNodeIndexes removes old index entries
func (n *NodeRepositoryKV) removeNodeIndexes(txn kv.Transaction, node *Node) error {
	nodeIDStr := fmt.Sprintf("%d", node.Id)

	// Remove address index
	addressKey := kv.NewKeyBuilder().Meta().Append("index").Append("node").
		Append("address").Append(node.Address).Append(fmt.Sprintf("%d", node.Port)).Build()
	txn.Delete(n.ctx, addressKey)

	// Remove region index
	regionKey := kv.NewKeyBuilder().Meta().Append("index").Append("node").
		Append("region").Append(node.Region.Name).Append(nodeIDStr).Build()
	txn.Delete(n.ctx, regionKey)

	// Remove active status indexes (both true and false)
	activeKeyTrue := kv.NewKeyBuilder().Meta().Append("index").Append("node").
		Append("active").Append("true").Append(nodeIDStr).Build()
	txn.Delete(n.ctx, activeKeyTrue)

	activeKeyFalse := kv.NewKeyBuilder().Meta().Append("index").Append("node").
		Append("active").Append("false").Append(nodeIDStr).Build()
	txn.Delete(n.ctx, activeKeyFalse)

	return nil
}

// DeleteNode removes a node from the repository
func (n *NodeRepositoryKV) DeleteNode(nodeID int64) error {
	// Get node first for index cleanup
	node, err := n.GetNodeById(nodeID)
	if err != nil {
		return fmt.Errorf("failed to get node for deletion: %w", err)
	}
	if node == nil {
		return fmt.Errorf("node with ID %d does not exist", nodeID)
	}

	key := kv.NewKeyBuilder().Meta().Append("node").Append(fmt.Sprintf("%d", nodeID)).Build()

	txn, err := n.store.Begin(true)
	if err != nil {
		return fmt.Errorf("failed to begin write transaction: %w", err)
	}
	defer txn.Discard()

	// Remove indexes
	if err := n.removeNodeIndexes(txn, node); err != nil {
		return fmt.Errorf("failed to remove node indexes: %w", err)
	}

	// Delete main node record
	if err := txn.Delete(n.ctx, key); err != nil {
		return fmt.Errorf("failed to delete node: %w", err)
	}

	return txn.Commit()
}

// Helper function to split keys consistently
func splitKey(key string) []string {
	parts := make([]string, 0)
	current := ""

	for _, char := range key {
		if char == ':' {
			parts = append(parts, current)
			current = ""
		} else {
			current += string(char)
		}
	}

	if current != "" {
		parts = append(parts, current)
	}

	return parts
}
