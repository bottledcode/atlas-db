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
	err = client.Connect()
	require.NoError(t, err, "Failed to connect to socket")

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

	// Scan for "table:USERS:" prefix (keys are uppercase)
	keys, err := client.Scan("table:USERS:")
	require.NoError(t, err, "Failed to scan users prefix")
	assert.Len(t, keys, 2, "Should find 2 user keys")
	assert.Contains(t, keys, "table:USERS:row:ALICE", "Should contain users.alice")
	assert.Contains(t, keys, "table:USERS:row:BOB", "Should contain users.bob")

	// Scan for "table:PRODUCTS:" prefix
	keys, err = client.Scan("table:PRODUCTS:")
	require.NoError(t, err, "Failed to scan products prefix")
	assert.Len(t, keys, 2, "Should find 2 product keys")
	assert.Contains(t, keys, "table:PRODUCTS:row:LAPTOP", "Should contain products.laptop")
	assert.Contains(t, keys, "table:PRODUCTS:row:PHONE", "Should contain products.phone")

	// Scan for non-existent prefix
	keys, err = client.Scan("table:NONEXISTENT:")
	require.NoError(t, err, "Failed to scan nonexistent prefix")
	assert.Len(t, keys, 0, "Should find 0 keys for non-existent prefix")

	t.Logf("Prefix scan single node test completed successfully")
}

func TestPrefixScanMultiNode(t *testing.T) {
	cluster, err := harness.NewCluster(t, harness.ClusterConfig{
		NumNodes: 3,
		Regions:  []string{"us-east-1", "us-west-2"},
		BasePort: 10500,
	})
	require.NoError(t, err, "Failed to create cluster")

	err = cluster.Start()
	require.NoError(t, err, "Failed to start cluster")

	err = cluster.WaitForBootstrap(10 * time.Second)
	require.NoError(t, err, "Cluster failed to bootstrap")

	// Write keys from different nodes to distribute ownership
	node0, err := cluster.GetNode(0)
	require.NoError(t, err, "Failed to get node 0")

	node1, err := cluster.GetNode(1)
	require.NoError(t, err, "Failed to get node 1")

	// Write keys from node 0
	err = node0.Client().KeyPut("orders.order1", "order1_data")
	require.NoError(t, err, "Failed to put orders.order1")

	err = node0.Client().KeyPut("orders.order2", "order2_data")
	require.NoError(t, err, "Failed to put orders.order2")

	// Write keys from node 1
	err = node1.Client().KeyPut("orders.order3", "order3_data")
	require.NoError(t, err, "Failed to put orders.order3")

	err = node1.Client().KeyPut("orders.order4", "order4_data")
	require.NoError(t, err, "Failed to put orders.order4")

	// Allow time for consensus
	time.Sleep(2 * time.Second)

	// Scan from node 0 should return all keys regardless of ownership
	keys, err := node0.Client().Scan("table:ORDERS:")
	require.NoError(t, err, "Failed to scan orders prefix from node 0")
	assert.GreaterOrEqual(t, len(keys), 4, "Should find at least 4 order keys from distributed cluster")

	// Verify all keys are present (keys are uppercase)
	assert.Contains(t, keys, "table:ORDERS:row:ORDER1", "Should contain orders.order1")
	assert.Contains(t, keys, "table:ORDERS:row:ORDER2", "Should contain orders.order2")
	assert.Contains(t, keys, "table:ORDERS:row:ORDER3", "Should contain orders.order3")
	assert.Contains(t, keys, "table:ORDERS:row:ORDER4", "Should contain orders.order4")

	// Scan from node 1 should also return all keys
	keys, err = node1.Client().Scan("table:ORDERS:")
	require.NoError(t, err, "Failed to scan orders prefix from node 1")
	assert.GreaterOrEqual(t, len(keys), 4, "Should find at least 4 order keys from node 1")

	t.Logf("Prefix scan multi-node test completed successfully")
}
