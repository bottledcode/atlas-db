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
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestQuorumManagerIntegration(t *testing.T) {
	// Setup test environment
	options.Logger = zaptest.NewLogger(t)

	// Create a test KV store
	dataPath := t.TempDir() + "/data.db"
	metaPath := t.TempDir() + "/meta.db"

	err := kv.CreatePool(dataPath, metaPath)
	require.NoError(t, err)
	defer func() {
		if pool := kv.GetPool(); pool != nil {
			pool.Close()
		}
		kv.DrainPool()
	}()

	ctx := context.Background()

	// Create a test table for quorum operations
	kvPool := kv.GetPool()
	require.NotNil(t, kvPool)
	metaStore := kvPool.MetaStore()
	require.NotNil(t, metaStore)

	tableRepo := NewTableRepositoryKV(ctx, metaStore)
	testTable := &Table{
		Name:              "test_integration_table",
		ReplicationLevel:  ReplicationLevel_global,
		Owner:             nil,
		CreatedAt:         timestamppb.Now(),
		Version:           1,
		AllowedRegions:    []string{"us-east-1", "us-west-2"},
		RestrictedRegions: nil,
		Group:             "",
		Type:              TableType_table,
		ShardPrincipals:   nil,
	}

	err = tableRepo.InsertTable(testTable)
	require.NoError(t, err)

	// Reset the singleton managers to ensure clean state
	manager = nil
	managerOnce = sync.Once{}
	connectionManager = nil
	connectionManagerOnce = sync.Once{}

	// Get the quorum manager (which should now integrate with connection manager)
	qm := GetDefaultQuorumManager(ctx)

	// Create test nodes
	node1 := &Node{
		Id:      1,
		Address: "node1.example.com",
		Port:    8080,
		Region:  &Region{Name: "us-east-1"},
		Active:  true,
		Rtt:     durationpb.New(10 * time.Millisecond),
	}

	node2 := &Node{
		Id:      2,
		Address: "node2.example.com",
		Port:    8080,
		Region:  &Region{Name: "us-east-1"},
		Active:  true,
		Rtt:     durationpb.New(15 * time.Millisecond),
	}

	node3 := &Node{
		Id:      3,
		Address: "node3.example.com",
		Port:    8080,
		Region:  &Region{Name: "us-west-2"},
		Active:  true,
		Rtt:     durationpb.New(20 * time.Millisecond),
	}

	// Add nodes to the quorum manager (which should also add them to connection manager)
	err = qm.AddNode(ctx, node1)
	assert.NoError(t, err)

	err = qm.AddNode(ctx, node2)
	assert.NoError(t, err)

	err = qm.AddNode(ctx, node3)
	assert.NoError(t, err)

	// Access the internal connection manager to verify nodes were added
	defaultQM, ok := qm.(*defaultQuorumManager)
	require.True(t, ok, "QuorumManager should be *defaultQuorumManager")
	require.NotNil(t, defaultQM.connectionManager, "Connection manager should be initialized")

	// Verify nodes are tracked in connection manager
	defaultQM.connectionManager.mu.RLock()
	managedNodesCount := len(defaultQM.connectionManager.nodes)
	defaultQM.connectionManager.mu.RUnlock()

	assert.Equal(t, 3, managedNodesCount, "All 3 nodes should be in connection manager")

	// Manually mark some nodes as active in the connection manager for testing
	// (In real usage, the health checker would do this)
	managedNode1 := &ManagedNode{
		Node:       node1,
		status:     NodeStatusActive,
		client:     nil, // In real usage this would have actual client
		rttHistory: make([]time.Duration, 0),
	}

	managedNode2 := &ManagedNode{
		Node:       node2,
		status:     NodeStatusFailed, // Mark node2 as failed
		client:     nil,
		rttHistory: make([]time.Duration, 0),
	}

	managedNode3 := &ManagedNode{
		Node:       node3,
		status:     NodeStatusActive,
		client:     nil,
		rttHistory: make([]time.Duration, 0),
	}

	// Replace the managed nodes with our test versions
	defaultQM.connectionManager.mu.Lock()
	defaultQM.connectionManager.nodes[1] = managedNode1
	defaultQM.connectionManager.nodes[2] = managedNode2
	defaultQM.connectionManager.nodes[3] = managedNode3
	defaultQM.connectionManager.mu.Unlock()

	// Add active nodes to the active nodes map
	defaultQM.connectionManager.addToActiveNodes(managedNode1)
	defaultQM.connectionManager.addToActiveNodes(managedNode3)
	// Note: managedNode2 is deliberately NOT added to active nodes since it's failed

	// Test quorum formation - should only use healthy nodes
	quorum, err := qm.GetQuorum(ctx, "test_integration_table")

	if err != nil {
		// This might fail with "insufficient active nodes" which is expected
		// if the health filtering is working correctly and we don't have enough healthy nodes
		t.Logf("Quorum formation failed as expected with health filtering: %v", err)

		// Verify that the error is due to insufficient nodes (health filtering working)
		assert.Contains(t, err.Error(), "unable to form a quorum")

		// Check that only healthy nodes are considered
		activeNodes := defaultQM.connectionManager.GetAllActiveNodes()
		totalActiveNodes := 0
		for _, regionNodes := range activeNodes {
			totalActiveNodes += len(regionNodes)
		}
		assert.Equal(t, 2, totalActiveNodes, "Should only have 2 active nodes (node1 and node3)")

		// Verify that node2 (failed) is not in active nodes
		eastNodes := defaultQM.connectionManager.GetActiveNodesByRegion("us-east-1")
		assert.Len(t, eastNodes, 1, "Only node1 should be active in us-east-1")
		assert.Equal(t, int64(1), eastNodes[0].Id, "The active node in us-east-1 should be node1")

	} else {
		// If quorum formation succeeded, verify it only uses healthy nodes
		assert.NotNil(t, quorum, "Quorum should be formed successfully")

		majorityQuorum, ok := quorum.(*majorityQuorum)
		require.True(t, ok, "Quorum should be *majorityQuorum")

		// Verify that the quorum only contains healthy nodes (node1 and node3)
		allQuorumNodeIDs := make(map[int64]bool)
		for _, qn := range majorityQuorum.q1 {
			allQuorumNodeIDs[qn.Id] = true
		}
		for _, qn := range majorityQuorum.q2 {
			allQuorumNodeIDs[qn.Id] = true
		}

		// Should not contain node2 (failed node)
		assert.False(t, allQuorumNodeIDs[2], "Failed node2 should not be in quorum")
		t.Logf("Quorum successfully formed with %d nodes in Q1 and %d nodes in Q2",
			len(majorityQuorum.q1), len(majorityQuorum.q2))
	}
}

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
