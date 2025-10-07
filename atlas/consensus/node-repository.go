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
	"crypto/rand"
	"math/big"
	"strconv"

	"github.com/bottledcode/atlas-db/atlas/kv"
)

type NodeRepository interface {
	GetNodeById(id int64) (*Node, error)
	GetNodeByAddress(address string, port uint) (*Node, error)
	GetNodesByRegion(region string) ([]*Node, error)
	GetRegions() ([]*Region, error)
	Iterate(fn func(*Node) error) error
	TotalCount() (int64, error)
	GetRandomNodes(num int64, excluding ...int64) ([]*Node, error)
}


type NodeKey struct {
	GenericKey
}

type NodeR struct {
	BaseRepository[*Node, NodeKey]
}

// NewNodeRepository creates a new NodeRepository with proper initialization
func NewNodeRepository(store kv.Store, ctx context.Context) *NodeR {
	repo := &NodeR{
		BaseRepository: BaseRepository[*Node, NodeKey]{
			store: store,
			ctx:   ctx,
		},
	}
	// Set the repo field to enable polymorphic method dispatch
	repo.BaseRepository.repo = repo
	return repo
}

func (n *NodeR) CreateKey(k []byte) NodeKey {
	return NodeKey{
		GenericKey{raw: k},
	}
}

func (n *NodeR) getNodeKey(id int64) NodeKey {
	key := kv.NewKeyBuilder().Meta().Table("atls").Node(id).Build()
	return n.CreateKey(key)
}

func (n *NodeR) getActivePrefixKey(region string, node int64) Prefix {
	key := kv.NewKeyBuilder().Meta().Table("atls").Index().
		Append("node").
		Append("active")

	if region != "" {
		key = key.Append("region").Append(region)
		if node > 0 {
			key = key.Append("n").Append(strconv.Itoa(int(node)))
		}
	}
	return Prefix{
		raw: key.Build(),
	}
}

func (n *NodeR) getRegionPrefixKey(region string, node int64) Prefix {
	key := kv.NewKeyBuilder().Meta().Table("atls").Index().Append("node")
	key = key.Append("region")
	if region != "" {
		key = key.Append(region)
		if node > 0 {
			key = key.Append("n").Append(strconv.Itoa(int(node)))
		}
	}
	return Prefix{
		raw: key.Build(),
	}
}

func (n *NodeR) getAddressPrefixKey(address string, port int64, node int64) Prefix {
	key := kv.NewKeyBuilder().Meta().Table("atls").Index().
		Append("node").
		Append("address").Append(address)
	if port > 0 {
		key = key.Append("port").Append(strconv.Itoa(int(port)))
		if node > 0 {
			key = key.Append("n").Append(strconv.Itoa(int(node)))
		}
	}
	return Prefix{
		raw: key.Build(),
	}
}

func (n *NodeR) GetKeys(node *Node) *StructuredKey {
	activeKey := n.getActivePrefixKey(node.Region.GetName(), node.GetId())

	regionKey := n.getRegionPrefixKey(node.Region.GetName(), node.GetId())

	addressKey := n.getAddressPrefixKey(node.GetAddress(), node.GetPort(), node.GetId())

	key := &StructuredKey{
		PrimaryKey:      n.getNodeKey(node.GetId()).raw,
		IndexKeys:       [][]byte{regionKey.raw, addressKey.raw},
		RemoveIndexKeys: make([][]byte, 0),
	}

	if node.GetActive() {
		key.IndexKeys = append(key.IndexKeys, activeKey.raw)
	} else {
		key.RemoveIndexKeys = append(key.RemoveIndexKeys, activeKey.raw)
	}

	return key
}

func (n *NodeR) GetNodeById(id int64) (*Node, error) {
	key := n.getNodeKey(id)
	return n.GetByKey(key)
}

func (n *NodeR) GetNodeByAddress(address string, port uint) (*Node, error) {
	prefix := n.getAddressPrefixKey(address, int64(port), 0)
	var node *Node
	// Scan index keys - values are primary keys, not full messages
	err := n.ScanIndex(prefix, func(primaryKey []byte) error {
		var err error
		node, err = n.GetByKey(n.CreateKey(primaryKey))
		return err
	})
	return node, err
}

func (n *NodeR) GetNodesByRegion(region string) ([]*Node, error) {
	prefix := n.getActivePrefixKey(region, 0)
	var nodes []*Node
	err := n.ScanIndex(prefix, func(primaryKey []byte) error {
		node, err := n.GetByKey(n.CreateKey(primaryKey))
		if err != nil {
			return err
		}
		nodes = append(nodes, node)
		return nil
	})
	return nodes, err
}

func (n *NodeR) GetRegions() ([]*Region, error) {
	prefix := n.getRegionPrefixKey("", 0)
	regionMap := make(map[string]*Region)
	err := n.ScanIndex(prefix, func(primaryKey []byte) error {
		node, err := n.GetByKey(n.CreateKey(primaryKey))
		if err != nil {
			return err
		}
		regionMap[node.Region.GetName()] = node.Region
		return nil
	})

	var regions []*Region
	for _, region := range regionMap {
		regions = append(regions, region)
	}
	return regions, err
}

func (n *NodeR) Iterate(fn func(*Node) error) error {
	prefix := n.getActivePrefixKey("", 0)
	return n.ScanIndex(prefix, func(primaryKey []byte) error {
		node, err := n.GetByKey(n.CreateKey(primaryKey))
		if err != nil {
			return err
		}
		return fn(node)
	})
}

func (n *NodeR) TotalCount() (int64, error) {
	prefix := n.getActivePrefixKey("", 0)
	return n.CountPrefix(prefix)
}

func (n *NodeR) GetRandomNodes(num int64, excluding ...int64) ([]*Node, error) {
	prefix := n.getActivePrefixKey("", 0)
	excludeMap := make(map[int64]bool)
	for _, id := range excluding {
		excludeMap[id] = true
	}

	var candidates []*Node
	err := n.ScanIndex(prefix, func(primaryKey []byte) error {
		node, err := n.GetByKey(n.CreateKey(primaryKey))
		if err != nil {
			return err
		}
		if !excludeMap[node.GetId()] {
			candidates = append(candidates, node)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}

	if int64(len(candidates)) <= num {
		return candidates, nil
	}

	selectedNodes := make([]*Node, num)
	candidatesCopy := make([]*Node, len(candidates))
	copy(candidatesCopy, candidates)

	for i := range num {
		remaining := int64(len(candidatesCopy)) - i
		randIndex, err := rand.Int(rand.Reader, big.NewInt(remaining))
		if err != nil {
			return nil, err
		}

		idx := randIndex.Int64()
		selectedNodes[i] = candidatesCopy[i+idx]
		candidatesCopy[i+idx], candidatesCopy[i] = candidatesCopy[i], candidatesCopy[i+idx]
	}

	return selectedNodes, nil
}
