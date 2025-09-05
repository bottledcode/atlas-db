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
	"testing"

	"github.com/bottledcode/atlas-db/atlas/options"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap/zaptest"
)

func TestQuorumManagerFilterHealthyNodes(t *testing.T) {
	options.Logger = zaptest.NewLogger(t)

	// Create mock connection manager
	mockRepo := NewMockNodeRepository()
	connectionManager := &NodeConnectionManager{
		nodes:       make(map[int64]*ManagedNode),
		activeNodes: make(map[string][]*ManagedNode),
		storage:     mockRepo,
	}

	// Create quorum manager with connection manager
	qm := &defaultQuorumManager{
		nodes:             make(map[RegionName][]*QuorumNode),
		connectionManager: connectionManager,
	}

	// Create test nodes
	node1 := createTestNode(1, "us-east-1", "host1", 8080)
	node2 := createTestNode(2, "us-east-1", "host2", 8080)
	node3 := createTestNode(3, "us-west-2", "host3", 8080)

	// Create QuorumNodes
	qn1 := &QuorumNode{Node: node1}
	qn2 := &QuorumNode{Node: node2}
	qn3 := &QuorumNode{Node: node3}

	// Add to quorum manager
	qm.nodes[RegionName("us-east-1")] = []*QuorumNode{qn1, qn2}
	qm.nodes[RegionName("us-west-2")] = []*QuorumNode{qn3}

	// Add healthy nodes to connection manager (only node1 and node3 are healthy)
	managedNode1 := &ManagedNode{Node: node1, status: NodeStatusActive}
	managedNode3 := &ManagedNode{Node: node3, status: NodeStatusActive}

	connectionManager.addToActiveNodes(managedNode1)
	connectionManager.addToActiveNodes(managedNode3)

	// Test filtering
	filteredNodes := qm.filterHealthyNodes(qm.nodes)

	// Should only contain healthy nodes
	assert.Len(t, filteredNodes[RegionName("us-east-1")], 1, "Only node1 should be healthy in us-east-1")
	assert.Equal(t, int64(1), filteredNodes[RegionName("us-east-1")][0].Id)

	assert.Len(t, filteredNodes[RegionName("us-west-2")], 1, "Node3 should be healthy in us-west-2")
	assert.Equal(t, int64(3), filteredNodes[RegionName("us-west-2")][0].Id)
}
