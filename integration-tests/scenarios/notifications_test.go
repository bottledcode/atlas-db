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

//go:build integration

package scenarios

import (
	"encoding/json"
	"io"
	"iter"
	"maps"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bottledcode/atlas-db/integration-tests/harness"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestThreeNodeNotifications(t *testing.T) {
	// Track received notifications
	var receivedNotifications atomic.Int32
	var notificationMutex sync.Mutex

	notifications := make(map[string]map[string]interface{})

	// Create a test HTTP server to receive notifications
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Read the notification payload
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Logf("Failed to read notification body: %v", err)
			w.WriteHeader(http.StatusBadRequest)
			return
		}

		// Parse the JSON array of notifications
		var notifs []map[string]interface{}
		if err := json.Unmarshal(body, &notifs); err != nil {
			t.Logf("Failed to parse notification JSON: %v", err)
			w.WriteHeader(http.StatusBadRequest)
			return
		}

		notificationMutex.Lock()
		for _, notif := range notifs {
			notifications[notif["event_id"].(string)] = notif
		}
		notificationMutex.Unlock()

		receivedNotifications.Add(int32(len(notifications)))
		t.Logf("Received %d notifications (total: %d)", len(notifications), receivedNotifications.Load())

		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	// Create a 3-node cluster in one region
	cluster, err := harness.NewCluster(t, harness.ClusterConfig{
		NumNodes: 3,
		Regions:  []string{"us-east-1"},
		BasePort: 10400,
	})
	require.NoError(t, err, "Failed to create cluster")

	err = cluster.Start()
	require.NoError(t, err, "Failed to start cluster")

	err = cluster.WaitForBootstrap(10 * time.Second)
	require.NoError(t, err, "Cluster failed to bootstrap")

	// Verify all nodes are running and connect
	for i := 0; i < cluster.NumNodes(); i++ {
		node, err := cluster.GetNode(i)
		require.NoError(t, err, "Failed to get node %d", i)
		assert.True(t, node.IsRunning(), "Node %d should be running", i)

		err = node.Client().Connect()
		require.NoError(t, err, "Failed to connect to node %d socket", i)
	}

	// Subscribe to notifications on node 0 for keys with prefix "user."
	node0, err := cluster.GetNode(0)
	require.NoError(t, err, "Failed to get node 0")

	for range 10 {
		_ = node0.Client().Subscribe("user.", server.URL)
		if err == nil {
			break
		}
		time.Sleep(1 * time.Second)
	}

	t.Logf("Subscribed to notifications for prefix 'user.' at %s", server.URL)

	// Give the subscription time to propagate through the cluster
	time.Sleep(2 * time.Second)

	// Write keys to different nodes to verify cross-node notifications
	testCases := []struct {
		nodeID int
		key    string
		value  string
	}{
		{0, "user.alice", "alice_data"},
		{1, "user.bob", "bob_data"},
		{2, "user.charlie", "charlie_data"},
		{1, "other.key", "should_not_notify"}, // Different prefix, shouldn't trigger notification
		{0, "user.diana", "diana_data"},
	}

	for _, tc := range testCases {
		node, err := cluster.GetNode(tc.nodeID)
		require.NoError(t, err, "Failed to get node %d", tc.nodeID)

		err = node.Client().KeyPut(tc.key, tc.value)
		require.NoError(t, err, "Failed to put key %s on node %d", tc.key, tc.nodeID)
		t.Logf("Put key %s=%s on node %d", tc.key, tc.value, tc.nodeID)
	}

	// Wait for notifications to be delivered
	// We expect 4 notifications (alice, bob, charlie, diana) - NOT the "other.key" one
	maxWait := 10 * time.Second
	deadline := time.Now().Add(maxWait)

	for time.Now().Before(deadline) {
		if receivedNotifications.Load() >= 4 {
			break
		}
		time.Sleep(200 * time.Millisecond)
	}

	assert.GreaterOrEqual(t, receivedNotifications.Load(), int32(4),
		"Should have received at least four notifications for user.* keys")

	// Verify that we didn't receive notification for "other.key"
	foundOtherKey := false
	for _, notif := range notifications {
		if key, ok := notif["key"].(string); ok {
			if key == "other.key" {
				foundOtherKey = true
				break
			}
		}
	}
	assert.False(t, foundOtherKey, "Should not have received notification for 'other.key'")

	// Verify notification structure contains expected fields
	if len(notifications) > 0 {
		next, _ := iter.Pull(maps.Values(notifications))
		firstNotif, _ := next()
		assert.Contains(t, firstNotif, "key", "Notification should contain 'key' field")
		assert.Contains(t, firstNotif, "version", "Notification should contain 'version' field")
		assert.Contains(t, firstNotif, "op", "Notification should contain 'op' field")
		assert.Contains(t, firstNotif, "origin", "Notification should contain 'origin' field")
	}

	t.Logf("Three-node notification test completed successfully")
	t.Logf("Total notifications received: %d", receivedNotifications.Load())
	t.Logf("Notification details: %+v", notifications)
}
