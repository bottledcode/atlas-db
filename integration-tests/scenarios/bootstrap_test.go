//go:build integration

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
	"testing"
	"time"

	"github.com/bottledcode/atlas-db/integration-tests/harness"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSingleNodeStartup(t *testing.T) {
	cluster, err := harness.NewCluster(t, harness.ClusterConfig{
		NumNodes: 1,
		Regions:  []string{"us-east-1"},
		BasePort: 10000,
	})
	require.NoError(t, err, "Failed to create cluster")

	err = cluster.Start()
	require.NoError(t, err, "Failed to start cluster")

	node, err := cluster.GetNode(0)
	require.NoError(t, err, "Failed to get node 0")

	assert.True(t, node.IsRunning(), "Node should be running")

	client := node.Client()
	err = client.Connect()
	require.NoError(t, err, "Failed to connect to socket")

	err = client.KeyPut("test.key", "hello_world")
	require.NoError(t, err, "Failed to put key")

	value, err := client.KeyGet("test.key")
	require.NoError(t, err, "Failed to get key")
	assert.Equal(t, "hello_world", value, "Value should match")

	t.Logf("Single node test completed successfully")
}

func TestThreeNodeClusterFormation(t *testing.T) {
	cluster, err := harness.NewCluster(t, harness.ClusterConfig{
		NumNodes: 3,
		Regions:  []string{"us-east-1", "us-west-2"},
		BasePort: 10100,
	})
	require.NoError(t, err, "Failed to create cluster")

	err = cluster.Start()
	require.NoError(t, err, "Failed to start cluster")

	err = cluster.WaitForBootstrap(10 * time.Second)
	require.NoError(t, err, "Cluster failed to bootstrap")

	for i := 0; i < cluster.NumNodes(); i++ {
		node, err := cluster.GetNode(i)
		require.NoError(t, err, "Failed to get node %d", i)
		assert.True(t, node.IsRunning(), "Node %d should be running", i)

		err = node.Client().Connect()
		require.NoError(t, err, "Failed to connect to node %d socket", i)
	}

	t.Logf("Three node cluster formation completed successfully")
}

func TestDataPersistenceAfterRestart(t *testing.T) {
	cluster, err := harness.NewCluster(t, harness.ClusterConfig{
		NumNodes: 1,
		Regions:  []string{"us-east-1"},
		BasePort: 10200,
	})
	require.NoError(t, err, "Failed to create cluster")

	err = cluster.Start()
	require.NoError(t, err, "Failed to start cluster")

	node, err := cluster.GetNode(0)
	require.NoError(t, err, "Failed to get node 0")

	client := node.Client()
	err = client.KeyPut("persistent.key", "persistent_value")
	require.NoError(t, err, "Failed to put key")

	err = cluster.StopNode(0)
	require.NoError(t, err, "Failed to stop node 0")

	time.Sleep(1 * time.Second)

	err = cluster.StartNode(0)
	require.NoError(t, err, "Failed to restart node 0")

	value, err := client.KeyGet("persistent.key")
	require.NoError(t, err, "Failed to get key after restart")
	assert.Equal(t, "persistent_value", value, "Value should persist after restart")

	t.Logf("Data persistence test completed successfully")
}

func TestLateJoiningNode(t *testing.T) {
	cluster, err := harness.NewCluster(t, harness.ClusterConfig{
		NumNodes: 2,
		Regions:  []string{"us-east-1"},
		BasePort: 10300,
	})
	require.NoError(t, err, "Failed to create cluster")

	err = cluster.Start()
	require.NoError(t, err, "Failed to start initial cluster")

	time.Sleep(2 * time.Second)

	node0, err := cluster.GetNode(0)
	require.NoError(t, err, "Failed to get node 0")

	err = node0.Client().KeyPut("existing.key", "existing_value")
	require.NoError(t, err, "Failed to put key on node 0")

	newNode, err := cluster.AddNode("us-west-2")
	require.NoError(t, err, "Failed to add new node")

	err = newNode.Start()
	require.NoError(t, err, "Failed to start new node")

	err = newNode.WaitForStartup(5 * time.Second)
	require.NoError(t, err, "New node failed to start")

	err = newNode.Client().WaitForValue("existing.key", "existing_value", 10*time.Second)
	assert.NoError(t, err, "Late joining node should receive existing data via bootstrap")

	t.Logf("Late joining node test completed successfully")
}
