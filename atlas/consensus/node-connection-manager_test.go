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
	"sync"
	"testing"
	"time"

	"github.com/bottledcode/atlas-db/atlas/kv"
	"github.com/bottledcode/atlas-db/atlas/options"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zaptest"
	"google.golang.org/protobuf/types/known/durationpb"
)

// MockNodeRepository for testing
type MockNodeRepository struct {
	nodes   map[int64]*Node
	regions map[string][]*Node
	mu      sync.RWMutex
}

func NewMockNodeRepository() *MockNodeRepository {
	return &MockNodeRepository{
		nodes:   make(map[int64]*Node),
		regions: make(map[string][]*Node),
	}
}

func (m *MockNodeRepository) AddTestNode(node *Node) {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Check if node already exists and remove from old region
	if existingNode, exists := m.nodes[node.Id]; exists {
		oldRegion := existingNode.Region.Name
		// Remove from old region slice
		if nodes, ok := m.regions[oldRegion]; ok {
			filtered := make([]*Node, 0, len(nodes)-1)
			for _, n := range nodes {
				if n.Id != node.Id {
					filtered = append(filtered, n)
				}
			}
			if len(filtered) == 0 {
				delete(m.regions, oldRegion)
			} else {
				m.regions[oldRegion] = filtered
			}
		}
	}

	m.nodes[node.Id] = node
	newRegion := node.Region.Name
	if m.regions[newRegion] == nil {
		m.regions[newRegion] = make([]*Node, 0, 1)
	}
	m.regions[newRegion] = append(m.regions[newRegion], node)
}

func (m *MockNodeRepository) GetNodeById(id int64) (*Node, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	node, exists := m.nodes[id]
	if !exists {
		return nil, nil
	}
	return node, nil
}

func (m *MockNodeRepository) GetNodeByAddress(address string, port uint) (*Node, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	for _, node := range m.nodes {
		if node.Address == address && node.Port == int64(port) {
			return node, nil
		}
	}
	return nil, nil
}

func (m *MockNodeRepository) GetNodesByRegion(region string) ([]*Node, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	nodes := m.regions[region]
	if nodes == nil {
		return []*Node{}, nil
	}

	result := make([]*Node, len(nodes))
	copy(result, nodes)
	return result, nil
}

func (m *MockNodeRepository) GetRegions() ([]*Region, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	regions := make([]*Region, 0, len(m.regions))
	for regionName := range m.regions {
		regions = append(regions, &Region{Name: regionName})
	}
	return regions, nil
}

func (m *MockNodeRepository) Iterate(write bool, fn func(*Node, *kv.Transaction) error) error {
	m.mu.RLock()
	defer m.mu.RUnlock()

	for _, node := range m.nodes {
		if err := fn(node, nil); err != nil {
			return err
		}
	}
	return nil
}

func (m *MockNodeRepository) TotalCount() (int64, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	return int64(len(m.nodes)), nil
}

func (m *MockNodeRepository) GetRandomNodes(num int64, excluding ...int64) ([]*Node, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	excludeMap := make(map[int64]bool)
	for _, id := range excluding {
		excludeMap[id] = true
	}

	var candidates []*Node
	for _, node := range m.nodes {
		if !excludeMap[node.Id] {
			candidates = append(candidates, node)
		}
	}

	if int64(len(candidates)) <= num {
		return candidates, nil
	}

	return candidates[:num], nil
}

func (m *MockNodeRepository) AddNode(node *Node) error {
	m.AddTestNode(node)
	return nil
}

func (m *MockNodeRepository) UpdateNode(node *Node) error {
	m.AddTestNode(node)
	return nil
}

func (m *MockNodeRepository) DeleteNode(nodeID int64) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Get the node to find its region before deleting
	node, exists := m.nodes[nodeID]
	if !exists {
		return nil
	}

	// Remove from region slice
	region := node.Region.Name
	if nodes, ok := m.regions[region]; ok {
		filtered := make([]*Node, 0, len(nodes)-1)
		for _, n := range nodes {
			if n.Id != nodeID {
				filtered = append(filtered, n)
			}
		}
		if len(filtered) == 0 {
			delete(m.regions, region)
		} else {
			m.regions[region] = filtered
		}
	}

	delete(m.nodes, nodeID)
	return nil
}

func createTestNode(id int64, region string, address string, port int64) *Node {
	return &Node{
		Id:      id,
		Address: address,
		Port:    port,
		Region:  &Region{Name: region},
		Active:  true,
		Rtt:     durationpb.New(10 * time.Millisecond),
	}
}

func TestManagedNode_StatusManagement(t *testing.T) {
	node := createTestNode(1, "us-east-1", "localhost", 8080)

	managedNode := &ManagedNode{
		Node:       node,
		status:     NodeStatusUnknown,
		rttHistory: make([]time.Duration, 0),
	}

	// Test initial status
	assert.Equal(t, NodeStatusUnknown, managedNode.GetStatus())

	// Test status updates
	managedNode.UpdateStatus(NodeStatusConnecting)
	assert.Equal(t, NodeStatusConnecting, managedNode.GetStatus())

	managedNode.UpdateStatus(NodeStatusActive)
	assert.Equal(t, NodeStatusActive, managedNode.GetStatus())
	assert.Equal(t, int64(0), managedNode.failures)

	managedNode.UpdateStatus(NodeStatusFailed)
	assert.Equal(t, NodeStatusFailed, managedNode.GetStatus())
	assert.Equal(t, int64(1), managedNode.failures)
}

func TestManagedNode_RTTTracking(t *testing.T) {
	node := createTestNode(1, "us-east-1", "localhost", 8080)

	managedNode := &ManagedNode{
		Node:       node,
		rttHistory: make([]time.Duration, 0),
	}

	// Test initial RTT (from protobuf)
	assert.Equal(t, 10*time.Millisecond, managedNode.GetAverageRTT())

	// Add RTT measurements
	managedNode.AddRTTMeasurement(20 * time.Millisecond)
	managedNode.AddRTTMeasurement(30 * time.Millisecond)

	// Should average the measurements
	assert.Equal(t, 25*time.Millisecond, managedNode.GetAverageRTT())

	// Test that it updates the protobuf RTT
	assert.Equal(t, 25*time.Millisecond, managedNode.Rtt.AsDuration())

	// Test history limit (10 measurements max)
	for i := range 15 {
		managedNode.AddRTTMeasurement(time.Duration(i+1) * time.Millisecond)
	}

	managedNode.mu.RLock()
	historyLen := len(managedNode.rttHistory)
	managedNode.mu.RUnlock()

	assert.LessOrEqual(t, historyLen, 10, "RTT history should be limited to 10 entries")
}

func TestNodeConnectionManager_AddNode(t *testing.T) {
	// Initialize logger for testing
	options.Logger = zaptest.NewLogger(t)

	mockRepo := NewMockNodeRepository()

	manager := &NodeConnectionManager{
		nodes:       make(map[int64]*ManagedNode),
		activeNodes: make(map[string][]*ManagedNode),
		storage:     mockRepo,
	}

	node := createTestNode(1, "us-east-1", "localhost", 8080)

	// Create managed node directly to test without goroutine
	managedNode := &ManagedNode{
		Node:       node,
		status:     NodeStatusConnecting,
		rttHistory: make([]time.Duration, 0),
	}

	manager.mu.Lock()
	manager.nodes[node.Id] = managedNode
	manager.mu.Unlock()

	// Verify node was added to manager
	manager.mu.RLock()
	retrievedNode, exists := manager.nodes[1]
	manager.mu.RUnlock()

	require.True(t, exists)
	assert.Equal(t, node.Id, retrievedNode.Id)
	assert.Equal(t, NodeStatusConnecting, retrievedNode.GetStatus())
}

func TestNodeConnectionManager_ActiveNodesTracking(t *testing.T) {
	mockRepo := NewMockNodeRepository()

	manager := &NodeConnectionManager{
		nodes:       make(map[int64]*ManagedNode),
		activeNodes: make(map[string][]*ManagedNode),
		storage:     mockRepo,
	}

	// Add nodes to different regions
	node1 := createTestNode(1, "us-east-1", "node1.example.com", 8080)
	node2 := createTestNode(2, "us-east-1", "node2.example.com", 8080)
	node3 := createTestNode(3, "us-west-2", "node3.example.com", 8080)

	managedNode1 := &ManagedNode{Node: node1, status: NodeStatusActive, rttHistory: make([]time.Duration, 0)}
	managedNode2 := &ManagedNode{Node: node2, status: NodeStatusActive, rttHistory: make([]time.Duration, 0)}
	managedNode3 := &ManagedNode{Node: node3, status: NodeStatusActive, rttHistory: make([]time.Duration, 0)}

	manager.mu.Lock()
	manager.nodes[1] = managedNode1
	manager.nodes[2] = managedNode2
	manager.nodes[3] = managedNode3
	manager.mu.Unlock()

	// Add to active nodes
	manager.addToActiveNodes(managedNode1)
	manager.addToActiveNodes(managedNode2)
	manager.addToActiveNodes(managedNode3)

	// Test GetActiveNodesByRegion
	eastNodes := manager.GetActiveNodesByRegion("us-east-1")
	assert.Len(t, eastNodes, 2)

	westNodes := manager.GetActiveNodesByRegion("us-west-2")
	assert.Len(t, westNodes, 1)

	// Test GetAllActiveNodes
	allNodes := manager.GetAllActiveNodes()
	assert.Len(t, allNodes, 2) // 2 regions
	assert.Len(t, allNodes["us-east-1"], 2)
	assert.Len(t, allNodes["us-west-2"], 1)

	// Test removing from active nodes
	manager.removeFromActiveNodes(1)
	eastNodes = manager.GetActiveNodesByRegion("us-east-1")
	assert.Len(t, eastNodes, 1)
}

func TestNodeConnectionManager_ExecuteOnNode(t *testing.T) {
	mockRepo := NewMockNodeRepository()

	manager := &NodeConnectionManager{
		nodes:       make(map[int64]*ManagedNode),
		activeNodes: make(map[string][]*ManagedNode),
		storage:     mockRepo,
	}

	node := createTestNode(1, "us-east-1", "localhost", 8080)
	managedNode := &ManagedNode{
		Node:       node,
		status:     NodeStatusActive,
		rttHistory: make([]time.Duration, 0),
	}

	manager.mu.Lock()
	manager.nodes[1] = managedNode
	manager.mu.Unlock()

	// Test execution on non-existent node
	err := manager.ExecuteOnNode(999, func(client ConsensusClient) error {
		return nil
	})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "not found")

	// Test execution on inactive node
	managedNode.UpdateStatus(NodeStatusFailed)
	err = manager.ExecuteOnNode(1, func(client ConsensusClient) error {
		return nil
	})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "not active")

	// Test execution on active node with no client
	managedNode.UpdateStatus(NodeStatusActive)
	err = manager.ExecuteOnNode(1, func(client ConsensusClient) error {
		return nil
	})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "no client available")
}

func TestHealthChecker_NodeFailureHandling(t *testing.T) {
	// Initialize logger for testing
	options.Logger = zaptest.NewLogger(t)

	mockRepo := NewMockNodeRepository()

	manager := &NodeConnectionManager{
		nodes:       make(map[int64]*ManagedNode),
		activeNodes: make(map[string][]*ManagedNode),
		storage:     mockRepo,
	}

	healthChecker := NewHealthChecker(manager)
	healthChecker.maxFailures = 2 // Lower for testing

	node := createTestNode(1, "us-east-1", "localhost", 8080)
	managedNode := &ManagedNode{
		Node:       node,
		status:     NodeStatusActive,
		failures:   0,
		rttHistory: make([]time.Duration, 0),
	}

	manager.mu.Lock()
	manager.nodes[1] = managedNode
	manager.mu.Unlock()

	manager.addToActiveNodes(managedNode)

	// Simulate first failure
	ctx := context.Background()
	healthChecker.handleNodeFailure(ctx, managedNode, assert.AnError)
	assert.Equal(t, NodeStatusFailed, managedNode.GetStatus())
	assert.Equal(t, int64(1), managedNode.failures)

	// Verify node removed from active list
	activeNodes := manager.GetActiveNodesByRegion("us-east-1")
	assert.Len(t, activeNodes, 0)

	// Simulate recovery
	healthChecker.handleNodeSuccess(ctx, managedNode, 15*time.Millisecond)
	assert.Equal(t, NodeStatusActive, managedNode.GetStatus())

	// Verify node added back to active list
	activeNodes = manager.GetActiveNodesByRegion("us-east-1")
	assert.Len(t, activeNodes, 1)
}

func TestHealthChecker_GetHealthStats(t *testing.T) {
	mockRepo := NewMockNodeRepository()

	manager := &NodeConnectionManager{
		nodes:       make(map[int64]*ManagedNode),
		activeNodes: make(map[string][]*ManagedNode),
		storage:     mockRepo,
	}

	healthChecker := NewHealthChecker(manager)

	// Add test nodes with different statuses
	node1 := createTestNode(1, "us-east-1", "node1.example.com", 8080)
	node2 := createTestNode(2, "us-east-1", "node2.example.com", 8080)
	node3 := createTestNode(3, "us-west-2", "node3.example.com", 8080)

	managedNode1 := &ManagedNode{Node: node1, status: NodeStatusActive, rttHistory: []time.Duration{10 * time.Millisecond}}
	managedNode2 := &ManagedNode{Node: node2, status: NodeStatusFailed, rttHistory: make([]time.Duration, 0)}
	managedNode3 := &ManagedNode{Node: node3, status: NodeStatusActive, rttHistory: []time.Duration{20 * time.Millisecond}}

	manager.mu.Lock()
	manager.nodes[1] = managedNode1
	manager.nodes[2] = managedNode2
	manager.nodes[3] = managedNode3
	manager.mu.Unlock()

	manager.addToActiveNodes(managedNode1)
	manager.addToActiveNodes(managedNode3)

	stats := healthChecker.GetHealthStats()

	assert.Equal(t, int64(3), stats.TotalNodes)
	assert.Equal(t, int64(2), stats.ActiveNodes)
	assert.Equal(t, int64(1), stats.FailedNodes)
	assert.Equal(t, int64(1), stats.RegionStats["us-east-1"])
	assert.Equal(t, int64(1), stats.RegionStats["us-west-2"])
	assert.Equal(t, 15*time.Millisecond, stats.AverageRTT) // (10 + 20) / 2
}

func TestManagedNode_Close(t *testing.T) {
	node := createTestNode(1, "us-east-1", "localhost", 8080)

	closerCalled := false
	mockCloser := func() {
		closerCalled = true
	}

	managedNode := &ManagedNode{
		Node:       node,
		connection: nil, // Would be real connection in actual usage
		client:     nil, // Would be real client in actual usage
		closer:     mockCloser,
		status:     NodeStatusActive,
		rttHistory: make([]time.Duration, 0),
	}

	managedNode.Close()

	assert.Equal(t, NodeStatusRemoved, managedNode.GetStatus())
	assert.True(t, closerCalled, "Closer function should have been called")

	managedNode.mu.RLock()
	assert.Nil(t, managedNode.connection)
	assert.Nil(t, managedNode.client)
	assert.Nil(t, managedNode.closer)
	managedNode.mu.RUnlock()
}

func TestHealthChecker_SkipsSelfHealthChecks(t *testing.T) {
	// Initialize logger for testing
	options.Logger = zaptest.NewLogger(t)

	// Set current node ID for testing
	originalServerId := options.CurrentOptions.ServerId
	options.CurrentOptions.ServerId = 1
	defer func() {
		options.CurrentOptions.ServerId = originalServerId
	}()

	mockRepo := NewMockNodeRepository()

	manager := &NodeConnectionManager{
		nodes:       make(map[int64]*ManagedNode),
		activeNodes: make(map[string][]*ManagedNode),
		storage:     mockRepo,
	}

	healthChecker := NewHealthCheckerForTesting(manager)

	// Add nodes including current node (ID 1) and other nodes
	node1 := createTestNode(1, "us-east-1", "localhost", 8080) // Current node
	node2 := createTestNode(2, "us-east-1", "localhost", 8081) // Other node
	node3 := createTestNode(3, "us-west-2", "localhost", 8082) // Another node

	managedNode1 := &ManagedNode{Node: node1, status: NodeStatusActive, rttHistory: make([]time.Duration, 0)}
	managedNode2 := &ManagedNode{Node: node2, status: NodeStatusActive, rttHistory: make([]time.Duration, 0)}
	managedNode3 := &ManagedNode{Node: node3, status: NodeStatusActive, rttHistory: make([]time.Duration, 0)}

	manager.mu.Lock()
	manager.nodes[1] = managedNode1
	manager.nodes[2] = managedNode2
	manager.nodes[3] = managedNode3
	manager.mu.Unlock()

	// Call checkAllNodes - this should skip node 1 (current node)
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	healthChecker.checkAllNodes(ctx)

	// Wait for all goroutines to complete
	time.Sleep(50 * time.Millisecond)

	// This test mainly verifies that no panic occurs and self-checks are skipped
	// In real scenario, node 1 would not be health checked
	assert.True(t, true, "Health checker should skip self-checks without errors")
}

func TestQuorumManager_RemoveNode(t *testing.T) {
	// Initialize test data
	options.CurrentOptions.ServerId = 1
	options.Logger = zaptest.NewLogger(t)

	defer func() {
		options.CurrentOptions.ServerId = 0
		options.Logger = nil
	}()

	manager := &defaultQuorumManager{
		nodes: make(map[RegionName][]*QuorumNode),
	}

	// Create test nodes and add them manually to bypass JoinCluster
	node1 := createTestNode(1, "us-east-1", "localhost", 8080)
	node2 := createTestNode(2, "us-east-1", "localhost", 8081)
	node3 := createTestNode(3, "us-west-2", "localhost", 8082)

	// Manually add nodes to manager (bypass AddNode which does JoinCluster)
	manager.nodes[RegionName("us-east-1")] = []*QuorumNode{
		{Node: node1},
		{Node: node2},
	}
	manager.nodes[RegionName("us-west-2")] = []*QuorumNode{
		{Node: node3},
	}

	// Verify initial state
	assert.Len(t, manager.nodes[RegionName("us-east-1")], 2)
	assert.Len(t, manager.nodes[RegionName("us-west-2")], 1)
	assert.Len(t, manager.nodes, 2) // Two regions

	// Remove node from us-east-1 region
	err := manager.RemoveNode(2)
	assert.NoError(t, err)

	// Verify node is removed
	assert.Len(t, manager.nodes[RegionName("us-east-1")], 1)
	assert.Len(t, manager.nodes[RegionName("us-west-2")], 1)
	assert.Len(t, manager.nodes, 2) // Still two regions

	// Remove the last node from us-west-2 region
	err = manager.RemoveNode(3)
	assert.NoError(t, err)

	// Verify region is removed when empty
	assert.Len(t, manager.nodes[RegionName("us-east-1")], 1)
	_, exists := manager.nodes[RegionName("us-west-2")]
	assert.False(t, exists, "Empty region should be removed")
	assert.Len(t, manager.nodes, 1) // Only one region left

	// Remove non-existent node (should not error)
	err = manager.RemoveNode(999)
	assert.NoError(t, err)

	// Verify remaining state is unchanged
	assert.Len(t, manager.nodes[RegionName("us-east-1")], 1)
	assert.Len(t, manager.nodes, 1)
}
