//go:build integration
// +build integration

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

package scenarios

import (
	"os"
	"testing"
	"time"

	"github.com/bottledcode/atlas-db/integration-tests/harness"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPrefixScanSingleNode(t *testing.T) {
	cluster, err := harness.NewCluster(t, harness.ClusterConfig{
		NumNodes: 1,
		Regions:  []string{"us-east-1"},
		BasePort: 10400,
	})
	require.NoError(t, err, "Failed to create cluster")

	err = cluster.Start()
	require.NoError(t, err, "Failed to start cluster")

	// Print log file contents on failure
	defer func() {
		if t.Failed() {
			node, _ := cluster.GetNode(0)
			if node != nil {
				if logData, err := os.ReadFile(node.GetLogPath()); err == nil {
					t.Logf("Node 0 log:\n%s", string(logData))
				}
			}
		}
	}()

	node, err := cluster.GetNode(0)
	require.NoError(t, err, "Failed to get node 0")

	client := node.Client()

	// Put some keys with different prefixes
	err = client.KeyPut("users.alice", "alice_data")
	require.NoError(t, err, "Failed to put users.alice")

	err = client.KeyPut("users.bob", "bob_data")
	require.NoError(t, err, "Failed to put users.bob")

	err = client.KeyPut("products.laptop", "laptop_data")
	require.NoError(t, err, "Failed to put products.laptop")

	err = client.KeyPut("products.phone", "phone_data")
	require.NoError(t, err, "Failed to put products.phone")

	// Allow time for consensus
	time.Sleep(1 * time.Second)

	// Scan for "USERS" prefix (keys are uppercase and returned in dotted format)
	keys, err := client.Scan("USERS")
	require.NoError(t, err, "Failed to scan users prefix")
	assert.Len(t, keys, 2, "Should find 2 user keys")
	assert.Contains(t, keys, "USERS.ALICE", "Should contain users.alice")
	assert.Contains(t, keys, "USERS.BOB", "Should contain users.bob")

	// Scan for "PRODUCTS" prefix
	keys, err = client.Scan("PRODUCTS")
	require.NoError(t, err, "Failed to scan products prefix")
	assert.Len(t, keys, 2, "Should find 2 product keys")
	assert.Contains(t, keys, "PRODUCTS.LAPTOP", "Should contain products.laptop")
	assert.Contains(t, keys, "PRODUCTS.PHONE", "Should contain products.phone")

	// Scan for non-existent prefix
	keys, err = client.Scan("NONEXISTENT")
	require.NoError(t, err, "Failed to scan nonexistent prefix")
	assert.Len(t, keys, 0, "Should find 0 keys for non-existent prefix")

	// Test protocol synchronization: After EMPTY response, next command should work correctly
	// This ensures we're properly consuming the OK terminator after EMPTY
	err = client.KeyPut("test.sync", "sync_data")
	require.NoError(t, err, "Failed to put key after empty scan - protocol desync detected")

	val, err := client.KeyGet("test.sync")
	require.NoError(t, err, "Failed to get key after empty scan - protocol desync detected")
	assert.Equal(t, "sync_data", val, "Value should match after empty scan")

	t.Logf("Prefix scan single node test completed successfully")
}

func TestPrefixScanMultiNode(t *testing.T) {
	cluster, err := harness.NewCluster(t, harness.ClusterConfig{
		NumNodes: 2,
		Regions:  []string{"us-east-1"},
		BasePort: 10500,
	})
	require.NoError(t, err, "Failed to create cluster")

	err = cluster.Start()
	require.NoError(t, err, "Failed to start cluster")

	// Allow time for cluster formation (matching TestLateJoiningNode pattern)
	time.Sleep(2 * time.Second)

	// Write keys from different nodes to distribute ownership
	node0, err := cluster.GetNode(0)
	require.NoError(t, err, "Failed to get node 0")

	node1, err := cluster.GetNode(1)
	require.NoError(t, err, "Failed to get node 1")

	// Write first key from node 0 to establish table
	err = node0.Client().KeyPut("orders.order1", "order1_data")
	require.NoError(t, err, "Failed to put orders.order1")

	// Allow time for table creation to propagate
	time.Sleep(500 * time.Millisecond)

	// Write remaining keys from both nodes
	err = node0.Client().KeyPut("orders.order2", "order2_data")
	require.NoError(t, err, "Failed to put orders.order2")

	err = node1.Client().KeyPut("orders.order3", "order3_data")
	require.NoError(t, err, "Failed to put orders.order3")

	err = node1.Client().KeyPut("orders.order4", "order4_data")
	require.NoError(t, err, "Failed to put orders.order4")

	// Allow time for consensus
	time.Sleep(2 * time.Second)

	// Scan from node 0 should return all keys regardless of ownership
	keys, err := node0.Client().Scan("ORDERS")
	require.NoError(t, err, "Failed to scan orders prefix from node 0")
	assert.GreaterOrEqual(t, len(keys), 4, "Should find at least 4 order keys from distributed cluster")

	// Verify all keys are present (keys are uppercase and in dotted format)
	assert.Contains(t, keys, "ORDERS.ORDER1", "Should contain orders.order1")
	assert.Contains(t, keys, "ORDERS.ORDER2", "Should contain orders.order2")
	assert.Contains(t, keys, "ORDERS.ORDER3", "Should contain orders.order3")
	assert.Contains(t, keys, "ORDERS.ORDER4", "Should contain orders.order4")

	// Scan from node 1 should also return all keys
	keys, err = node1.Client().Scan("ORDERS")
	require.NoError(t, err, "Failed to scan orders prefix from node 1")
	assert.GreaterOrEqual(t, len(keys), 4, "Should find at least 4 order keys from node 1")

	t.Logf("Prefix scan multi-node test completed successfully")
}

func TestPrefixScanAfterRestart(t *testing.T) {
	cluster, err := harness.NewCluster(t, harness.ClusterConfig{
		NumNodes: 1,
		Regions:  []string{"us-east-1"},
		BasePort: 10600,
	})
	require.NoError(t, err, "Failed to create cluster")

	err = cluster.Start()
	require.NoError(t, err, "Failed to start cluster")

	// Print log file contents on failure
	defer func() {
		if t.Failed() {
			node, _ := cluster.GetNode(0)
			if node != nil {
				if logData, err := os.ReadFile(node.GetLogPath()); err == nil {
					t.Logf("Node 0 log:\n%s", string(logData))
				}
			}
		}
	}()

	node, err := cluster.GetNode(0)
	require.NoError(t, err, "Failed to get node 0")

	client := node.Client()

	// Create some keys
	err = client.KeyPut("restart.key1", "value1")
	require.NoError(t, err, "Failed to put restart.key1")

	err = client.KeyPut("restart.key2", "value2")
	require.NoError(t, err, "Failed to put restart.key2")

	err = client.KeyPut("restart.key3", "value3")
	require.NoError(t, err, "Failed to put restart.key3")

	// Allow time for consensus and persistence
	time.Sleep(1 * time.Second)

	// Verify keys exist before restart
	keys, err := client.Scan("RESTART")
	require.NoError(t, err, "Failed to scan before restart")
	assert.Len(t, keys, 3, "Should find 3 keys before restart")

	// Also verify we can read the values
	val, err := client.KeyGet("restart.key1")
	require.NoError(t, err, "Failed to get restart.key1 before restart")
	assert.Equal(t, "value1", val, "Value should match before restart")

	t.Logf("Keys before restart: %v", keys)

	// Stop the node
	err = cluster.StopNode(0)
	require.NoError(t, err, "Failed to stop node")

	// Wait a moment for clean shutdown
	time.Sleep(500 * time.Millisecond)

	// Restart the node - this will clear the in-memory ownership map
	err = cluster.StartNode(0)
	require.NoError(t, err, "Failed to restart node")

	// Wait for the node to be ready
	err = node.WaitForStartup(30 * time.Second)
	require.NoError(t, err, "Node did not restart in time")

	// Allow time for node to fully initialize
	time.Sleep(1 * time.Second)

	// Try to scan after restart - ownership map is now empty
	// The keys should still be found if we scan persistent storage
	keys, err = client.Scan("RESTART")
	require.NoError(t, err, "Failed to scan after restart")

	t.Logf("Keys after restart: %v", keys)

	// This is the key assertion - after restart, should we still find keys?
	// Currently this will likely fail because ownership is lost
	assert.Len(t, keys, 3, "Should find 3 keys after restart (keys persist but ownership is lost)")

	// Also verify we can still read the values (this tests the read path recovery)
	val, err = client.KeyGet("restart.key1")
	require.NoError(t, err, "Failed to get restart.key1 after restart")
	assert.Equal(t, "value1", val, "Value should match after restart")

	t.Logf("Prefix scan after restart test completed")
}
