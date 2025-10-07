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
	"os"
	"testing"

	"github.com/bottledcode/atlas-db/atlas/kv"
)

// setupTestRepository creates an in-memory Badger store for testing
func setupTestRepository(t *testing.T) (NodeRepository, func()) {
	tmpDir, err := os.MkdirTemp("", "node_repo_test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}

	store, err := kv.NewBadgerStore(tmpDir)
	if err != nil {
		t.Fatalf("Failed to create BadgerStore: %v", err)
	}

	repo := NewNodeRepository(context.Background(), store)

	cleanup := func() {
		if err := store.Close(); err != nil {
			t.Logf("Failed to close store: %v", err)
		}
		if err := os.RemoveAll(tmpDir); err != nil {
			t.Logf("Failed to remove temp dir: %v", err)
		}
	}

	return repo, cleanup
}

// createTestNodeWithActive creates a test node with the given parameters including active status
func createTestNodeWithActive(id int64, address string, port int64, regionName string, active bool) *Node {
	return &Node{
		Id:      id,
		Address: address,
		Port:    port,
		Region: &Region{
			Name: regionName,
		},
		Active: active,
	}
}

func TestNodeRepository_PutAndGetById(t *testing.T) {
	repo, cleanup := setupTestRepository(t)
	defer cleanup()

	// Create a test node
	node := createTestNodeWithActive(1, "192.168.1.100", 8080, "us-west-1", true)

	// Put the node
	err := repo.AddNode(node)
	if err != nil {
		t.Fatalf("Failed to put node: %v", err)
	}

	// Get the node by ID
	retrieved, err := repo.GetNodeById(1)
	if err != nil {
		t.Fatalf("Failed to get node by ID: %v", err)
	}

	// Verify the retrieved node matches
	if retrieved.GetId() != node.GetId() {
		t.Errorf("Expected ID %d, got %d", node.GetId(), retrieved.GetId())
	}
	if retrieved.GetAddress() != node.GetAddress() {
		t.Errorf("Expected address %s, got %s", node.GetAddress(), retrieved.GetAddress())
	}
	if retrieved.GetPort() != node.GetPort() {
		t.Errorf("Expected port %d, got %d", node.GetPort(), retrieved.GetPort())
	}
	if retrieved.Region.GetName() != node.Region.GetName() {
		t.Errorf("Expected region %s, got %s", node.Region.GetName(), retrieved.Region.GetName())
	}
	if retrieved.GetActive() != node.GetActive() {
		t.Errorf("Expected active %v, got %v", node.GetActive(), retrieved.GetActive())
	}
}

func TestNodeRepository_GetByIdNonExistent(t *testing.T) {
	repo, cleanup := setupTestRepository(t)
	defer cleanup()

	// Try to get a non-existent node
	_, err := repo.GetNodeById(999)
	if err == nil {
		t.Error("Expected error when getting non-existent node, got nil")
	}
}

func TestNodeRepository_GetByAddress(t *testing.T) {
	repo, cleanup := setupTestRepository(t)
	defer cleanup()

	// Create and insert test nodes
	node1 := createTestNodeWithActive(1, "192.168.1.100", 8080, "us-west-1", true)
	node2 := createTestNodeWithActive(2, "192.168.1.101", 8080, "us-east-1", true)

	err := repo.AddNode(node1)
	if err != nil {
		t.Fatalf("Failed to put node1: %v", err)
	}
	err = repo.AddNode(node2)
	if err != nil {
		t.Fatalf("Failed to put node2: %v", err)
	}

	// Get node by address
	retrieved, err := repo.GetNodeByAddress("192.168.1.100", 8080)
	if err != nil {
		t.Fatalf("Failed to get node by address: %v", err)
	}

	if retrieved.GetId() != 1 {
		t.Errorf("Expected node ID 1, got %d", retrieved.GetId())
	}
	if retrieved.GetAddress() != "192.168.1.100" {
		t.Errorf("Expected address 192.168.1.100, got %s", retrieved.GetAddress())
	}
}

func TestNodeRepository_GetNodesByRegion(t *testing.T) {
	repo, cleanup := setupTestRepository(t)
	defer cleanup()

	// Create nodes in different regions
	node1 := createTestNodeWithActive(1, "192.168.1.100", 8080, "us-west-1", true)
	node2 := createTestNodeWithActive(2, "192.168.1.101", 8081, "us-west-1", true)
	node3 := createTestNodeWithActive(3, "192.168.1.102", 8082, "us-east-1", true)

	for _, node := range []*Node{node1, node2, node3} {
		if err := repo.AddNode(node); err != nil {
			t.Fatalf("Failed to put node: %v", err)
		}
	}

	// Get nodes by region
	westNodes, err := repo.GetNodesByRegion("us-west-1")
	if err != nil {
		t.Fatalf("Failed to get nodes by region: %v", err)
	}

	if len(westNodes) != 2 {
		t.Errorf("Expected 2 nodes in us-west-1, got %d", len(westNodes))
	}

	// Verify the nodes are the correct ones
	nodeIDs := make(map[int64]bool)
	for _, node := range westNodes {
		nodeIDs[node.GetId()] = true
	}

	if !nodeIDs[1] || !nodeIDs[2] {
		t.Errorf("Expected nodes 1 and 2 in us-west-1, got node IDs: %v", nodeIDs)
	}
}

func TestNodeRepository_Delete(t *testing.T) {
	repo, cleanup := setupTestRepository(t)
	defer cleanup()

	// Create and insert a node
	node := createTestNodeWithActive(1, "192.168.1.100", 8080, "us-west-1", true)
	err := repo.AddNode(node)
	if err != nil {
		t.Fatalf("Failed to put node: %v", err)
	}

	// Verify the node exists
	_, err = repo.GetNodeById(1)
	if err != nil {
		t.Fatalf("Node should exist before deletion: %v", err)
	}

	// Delete the node
	err = repo.DeleteNode(node.GetId())
	if err != nil {
		t.Fatalf("Failed to delete node: %v", err)
	}

	// Verify the node no longer exists
	_, err = repo.GetNodeById(1)
	if err == nil {
		t.Error("Expected error when getting deleted node, got nil")
	}
}

func TestNodeRepository_UpdateActiveStatus(t *testing.T) {
	repo, cleanup := setupTestRepository(t)
	defer cleanup()

	// Create an active node
	node := createTestNodeWithActive(1, "192.168.1.100", 8080, "us-west-1", true)
	err := repo.AddNode(node)
	if err != nil {
		t.Fatalf("Failed to put active node: %v", err)
	}

	// Update to inactive
	node.Active = false
	err = repo.AddNode(node)
	if err != nil {
		t.Fatalf("Failed to update node to inactive: %v", err)
	}

	// Retrieve and verify
	retrieved, err := repo.GetNodeById(1)
	if err != nil {
		t.Fatalf("Failed to get updated node: %v", err)
	}

	if retrieved.GetActive() {
		t.Error("Expected node to be inactive after update")
	}
}

func TestNodeRepository_Iterate(t *testing.T) {
	repo, cleanup := setupTestRepository(t)
	defer cleanup()

	// Create active and inactive nodes
	activeNode1 := createTestNodeWithActive(1, "192.168.1.100", 8080, "us-west-1", true)
	activeNode2 := createTestNodeWithActive(2, "192.168.1.101", 8081, "us-east-1", true)
	inactiveNode := createTestNodeWithActive(3, "192.168.1.102", 8082, "us-west-1", false)

	for _, node := range []*Node{activeNode1, activeNode2, inactiveNode} {
		if err := repo.AddNode(node); err != nil {
			t.Fatalf("Failed to put node: %v", err)
		}
	}

	// Iterate over active nodes
	var iteratedIDs []int64
	err := repo.Iterate(func(n *Node) error {
		iteratedIDs = append(iteratedIDs, n.GetId())
		return nil
	})
	if err != nil {
		t.Fatalf("Failed to iterate: %v", err)
	}

	// Should only get active nodes
	if len(iteratedIDs) != 2 {
		t.Errorf("Expected 2 active nodes, got %d", len(iteratedIDs))
	}

	// Verify we got the right nodes
	hasNode1 := false
	hasNode2 := false
	for _, id := range iteratedIDs {
		if id == 1 {
			hasNode1 = true
		}
		if id == 2 {
			hasNode2 = true
		}
	}

	if !hasNode1 || !hasNode2 {
		t.Errorf("Expected nodes 1 and 2, got IDs: %v", iteratedIDs)
	}
}

func TestNodeRepository_TotalCount(t *testing.T) {
	repo, cleanup := setupTestRepository(t)
	defer cleanup()

	// Initially should be 0
	count, err := repo.TotalCount()
	if err != nil {
		t.Fatalf("Failed to get count: %v", err)
	}
	if count != 0 {
		t.Errorf("Expected 0 nodes initially, got %d", count)
	}

	// Add active nodes
	activeNode1 := createTestNodeWithActive(1, "192.168.1.100", 8080, "us-west-1", true)
	activeNode2 := createTestNodeWithActive(2, "192.168.1.101", 8081, "us-east-1", true)
	inactiveNode := createTestNodeWithActive(3, "192.168.1.102", 8082, "us-west-1", false)

	for _, node := range []*Node{activeNode1, activeNode2, inactiveNode} {
		if err := repo.AddNode(node); err != nil {
			t.Fatalf("Failed to put node: %v", err)
		}
	}

	// Count should only include active nodes
	count, err = repo.TotalCount()
	if err != nil {
		t.Fatalf("Failed to get count after insert: %v", err)
	}
	if count != 2 {
		t.Errorf("Expected 2 active nodes, got %d", count)
	}
}

func TestNodeRepository_GetRandomNodes(t *testing.T) {
	repo, cleanup := setupTestRepository(t)
	defer cleanup()

	// Create active nodes
	for i := int64(1); i <= 5; i++ {
		node := createTestNodeWithActive(i, "192.168.1."+string(rune(100+i)), 8080, "us-west-1", true)
		if err := repo.AddNode(node); err != nil {
			t.Fatalf("Failed to put node %d: %v", i, err)
		}
	}

	// Get random 3 nodes
	nodes, err := repo.GetRandomNodes(3)
	if err != nil {
		t.Fatalf("Failed to get random nodes: %v", err)
	}

	if len(nodes) != 3 {
		t.Errorf("Expected 3 nodes, got %d", len(nodes))
	}

	// Get random nodes excluding some
	nodesExcluding, err := repo.GetRandomNodes(2, 1, 2)
	if err != nil {
		t.Fatalf("Failed to get random nodes with exclusions: %v", err)
	}

	if len(nodesExcluding) != 2 {
		t.Errorf("Expected 2 nodes after exclusions, got %d", len(nodesExcluding))
	}

	// Verify excluded nodes are not in the result
	for _, node := range nodesExcluding {
		if node.GetId() == 1 || node.GetId() == 2 {
			t.Errorf("Node %d should have been excluded", node.GetId())
		}
	}
}

func TestNodeRepository_GetRegions(t *testing.T) {
	repo, cleanup := setupTestRepository(t)
	defer cleanup()

	// Create nodes in different regions
	node1 := createTestNodeWithActive(1, "192.168.1.100", 8080, "us-west-1", true)
	node2 := createTestNodeWithActive(2, "192.168.1.101", 8081, "us-west-1", true)
	node3 := createTestNodeWithActive(3, "192.168.1.102", 8082, "us-east-1", true)
	node4 := createTestNodeWithActive(4, "192.168.1.103", 8083, "eu-west-1", true)

	for _, node := range []*Node{node1, node2, node3, node4} {
		if err := repo.AddNode(node); err != nil {
			t.Fatalf("Failed to put node: %v", err)
		}
	}

	// Get all regions
	regions, err := repo.GetRegions()
	if err != nil {
		t.Fatalf("Failed to get regions: %v", err)
	}

	if len(regions) != 3 {
		t.Errorf("Expected 3 regions, got %d", len(regions))
	}

	// Verify region names
	regionNames := make(map[string]bool)
	for _, region := range regions {
		regionNames[region.GetName()] = true
	}

	expectedRegions := []string{"us-west-1", "us-east-1", "eu-west-1"}
	for _, expected := range expectedRegions {
		if !regionNames[expected] {
			t.Errorf("Expected region %s not found", expected)
		}
	}
}

func TestNodeRepository_MultipleOperations(t *testing.T) {
	repo, cleanup := setupTestRepository(t)
	defer cleanup()

	// Test a complex scenario with multiple operations
	nodes := []*Node{
		createTestNodeWithActive(1, "192.168.1.100", 8080, "us-west-1", true),
		createTestNodeWithActive(2, "192.168.1.101", 8081, "us-west-1", true),
		createTestNodeWithActive(3, "192.168.1.102", 8082, "us-east-1", true),
	}

	// Insert all nodes
	for _, node := range nodes {
		if err := repo.AddNode(node); err != nil {
			t.Fatalf("Failed to put node: %v", err)
		}
	}

	// Verify count
	count, err := repo.TotalCount()
	if err != nil {
		t.Fatalf("Failed to get count: %v", err)
	}
	if count != 3 {
		t.Errorf("Expected 3 nodes, got %d", count)
	}

	// Deactivate one node
	nodes[0].Active = false
	if err := repo.AddNode(nodes[0]); err != nil {
		t.Fatalf("Failed to update node: %v", err)
	}

	// Count should decrease
	count, err = repo.TotalCount()
	if err != nil {
		t.Fatalf("Failed to get count after deactivation: %v", err)
	}
	if count != 2 {
		t.Errorf("Expected 2 active nodes after deactivation, got %d", count)
	}

	// Delete one node
	if err := repo.DeleteNode(nodes[1].GetId()); err != nil {
		t.Fatalf("Failed to delete node: %v", err)
	}

	// Count should decrease again
	count, err = repo.TotalCount()
	if err != nil {
		t.Fatalf("Failed to get count after deletion: %v", err)
	}
	if count != 1 {
		t.Errorf("Expected 1 active node after deletion, got %d", count)
	}
}

func TestNodeRepository_UpdateAddressAndRegion(t *testing.T) {
	repo, cleanup := setupTestRepository(t)
	defer cleanup()

	node := createTestNodeWithActive(1, "192.168.1.100", 8080, "us-west-1", true)
	err := repo.AddNode(node)
	if err != nil {
		t.Fatalf("Failed to add node: %v", err)
	}

	retrieved, err := repo.GetNodeByAddress("192.168.1.100", 8080)
	if err != nil {
		t.Fatalf("Failed to get node by original address: %v", err)
	}
	if retrieved.GetId() != 1 {
		t.Errorf("Expected node ID 1, got %d", retrieved.GetId())
	}

	westNodes, err := repo.GetNodesByRegion("us-west-1")
	if err != nil {
		t.Fatalf("Failed to get nodes by original region: %v", err)
	}
	if len(westNodes) != 1 {
		t.Fatalf("Expected 1 node in us-west-1, got %d", len(westNodes))
	}

	node.Address = "192.168.2.200"
	node.Port = 9090
	node.Region.Name = "us-east-1"
	err = repo.UpdateNode(node)
	if err != nil {
		t.Fatalf("Failed to update node: %v", err)
	}

	oldAddrNode, err := repo.GetNodeByAddress("192.168.1.100", 8080)
	if err == nil && oldAddrNode != nil {
		t.Errorf("Expected error when getting node by old address, but got node: %+v", oldAddrNode)
	}

	retrieved, err = repo.GetNodeByAddress("192.168.2.200", 9090)
	if err != nil {
		t.Fatalf("Failed to get node by new address: %v", err)
	}
	if retrieved.GetId() != 1 {
		t.Errorf("Expected node ID 1, got %d", retrieved.GetId())
	}

	westNodes, err = repo.GetNodesByRegion("us-west-1")
	if err != nil {
		t.Fatalf("Failed to get nodes by old region: %v", err)
	}
	if len(westNodes) != 0 {
		t.Errorf("Expected 0 nodes in us-west-1 after update, got %d", len(westNodes))
	}

	eastNodes, err := repo.GetNodesByRegion("us-east-1")
	if err != nil {
		t.Fatalf("Failed to get nodes by new region: %v", err)
	}
	if len(eastNodes) != 1 {
		t.Errorf("Expected 1 node in us-east-1, got %d", len(eastNodes))
	}
	if eastNodes[0].GetId() != 1 {
		t.Errorf("Expected node ID 1 in us-east-1, got %d", eastNodes[0].GetId())
	}
}
