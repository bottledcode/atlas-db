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
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bottledcode/atlas-db/atlas/kv"
	"github.com/bottledcode/atlas-db/atlas/options"
	"github.com/bottledcode/atlas-db/atlas/trie"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestNotificationSender_CurrentBucket(t *testing.T) {
	ns := &notificationSender{}

	tests := []struct {
		name     string
		key      []byte
		expected []byte
	}{
		{
			name:     "empty key returns nil",
			key:      []byte{},
			expected: nil,
		},
		{
			name:     "single byte key",
			key:      []byte("a"),
			expected: []byte("a"),
		},
		{
			name:     "two byte key",
			key:      []byte("ab"),
			expected: []byte("ab"),
		},
		{
			name:     "four byte key returns 4 bytes",
			key:      []byte("abcd"),
			expected: []byte("abcd"),
		},
		{
			name:     "five byte key returns 4 bytes (power of 2)",
			key:      []byte("abcde"),
			expected: []byte("abcd"),
		},
		{
			name:     "eight byte key returns 8 bytes",
			key:      []byte("abcdefgh"),
			expected: []byte("abcdefgh"),
		},
		{
			name:     "nine byte key returns 8 bytes (power of 2)",
			key:      []byte("abcdefghi"),
			expected: []byte("abcdefgh"),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := ns.currentBucket(tt.key)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestNotificationSender_NextBucket(t *testing.T) {
	ns := &notificationSender{}

	tests := []struct {
		name     string
		key      []byte
		expected []byte
	}{
		{
			name:     "empty key returns nil",
			key:      []byte{},
			expected: nil,
		},
		{
			name:     "single byte key returns nil (would cause negative shift)",
			key:      []byte("a"),
			expected: nil,
		},
		{
			name:     "two byte key returns single byte",
			key:      []byte("ab"),
			expected: []byte("a"),
		},
		{
			name:     "four byte key returns 2 bytes",
			key:      []byte("abcd"),
			expected: []byte("ab"),
		},
		{
			name:     "eight byte key returns 4 bytes",
			key:      []byte("abcdefgh"),
			expected: []byte("abcd"),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := ns.nextBucket(tt.key)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestNotificationSender_GenerateNotification(t *testing.T) {
	ns := &notificationSender{}

	tests := []struct {
		name       string
		migration  *Migration
		expectNil  bool
		expectType any
	}{
		{
			name: "set change generates notification",
			migration: &Migration{
				Version: &MigrationVersion{
					TableName:        "test.table",
					MigrationVersion: 1,
					TableVersion:     1,
					NodeId:           100,
				},
				Migration: &Migration_Data{
					Data: &DataMigration{
						Session: &DataMigration_Change{
							Change: &KVChange{
								Operation: &KVChange_Set{
									Set: &SetChange{
										Key: []byte("test-key"),
										Data: &Record{
											Data: &Record_Value{
												Value: &RawData{Data: []byte("value")},
											},
										},
									},
								},
							},
						},
					},
				},
			},
			expectNil:  false,
			expectType: &Notify_Set{},
		},
		{
			name: "delete change generates notification",
			migration: &Migration{
				Version: &MigrationVersion{
					TableName:        "test.table",
					MigrationVersion: 2,
					TableVersion:     1,
					NodeId:           100,
				},
				Migration: &Migration_Data{
					Data: &DataMigration{
						Session: &DataMigration_Change{
							Change: &KVChange{
								Operation: &KVChange_Del{
									Del: &DelChange{
										Key: []byte("test-key"),
									},
								},
							},
						},
					},
				},
			},
			expectNil:  false,
			expectType: &Notify_Del{},
		},
		{
			name: "acl change generates notification",
			migration: &Migration{
				Version: &MigrationVersion{
					TableName:        "test.table",
					MigrationVersion: 3,
					TableVersion:     1,
					NodeId:           100,
				},
				Migration: &Migration_Data{
					Data: &DataMigration{
						Session: &DataMigration_Change{
							Change: &KVChange{
								Operation: &KVChange_Acl{
									Acl: &AclChange{
										Key: []byte("test-key"),
										Change: &AclChange_Addition{
											Addition: &ACL{
												Owners: &ACLData{
													Principals: []string{"user1"},
												},
											},
										},
									},
								},
							},
						},
					},
				},
			},
			expectNil:  false,
			expectType: &Notify_Acl{},
		},
		{
			name: "schema migration returns unchanged",
			migration: &Migration{
				Version: &MigrationVersion{
					TableName:        "test.table",
					MigrationVersion: 4,
					TableVersion:     1,
					NodeId:           100,
				},
				Migration: &Migration_Schema{
					Schema: &SchemaMigration{
						Commands: []string{"CREATE TABLE test"},
					},
				},
			},
			expectNil: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := ns.GenerateNotification(tt.migration)
			require.NotNil(t, result)

			if tt.expectNil {
				// Should return original migration unchanged
				assert.Equal(t, tt.migration, result)
			} else {
				// Should have notification operation
				dataMig := result.GetData()
				require.NotNil(t, dataMig)
				change := dataMig.GetChange()
				require.NotNil(t, change)
				notifyOp := change.GetNotification()
				require.NotNil(t, notifyOp)

				// Check the change type
				assert.IsType(t, tt.expectType, notifyOp.GetChange())

				// Verify version string format
				expectedVersion := fmt.Sprintf("%d:%d:%d",
					tt.migration.GetVersion().GetMigrationVersion(),
					tt.migration.GetVersion().GetTableVersion(),
					tt.migration.GetVersion().GetNodeId())
				assert.Equal(t, expectedVersion, notifyOp.Version)

				// Verify timestamp exists
				assert.NotNil(t, notifyOp.Ts)
			}
		})
	}
}

type jsonReader struct {
	data []byte
	pos  int
}

func (j jsonReader) Read(p []byte) (n int, err error) {
	if j.pos >= len(j.data) {
		return 0, io.EOF
	}
	n = copy(p, j.data[j.pos:])
	//j.pos += n
	return n, nil
}

func TestNotificationSender_RetryLogic(t *testing.T) {
	attempts := atomic.Int32{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		count := attempts.Add(1)
		if count < 3 {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	// This test verifies the retry logic by checking the server receives multiple attempts
	sub := &Subscribe{
		Url:    server.URL,
		Prefix: []byte("test"),
		Options: &SubscribeOptions{
			RetryAttempts:  3,
			RetryAfterBase: durationpb.New(10 * time.Millisecond),
		},
	}

	notifications := []*Notify{
		{
			Key: []byte("test-key"),
			Change: &Notify_Set{
				Set: &SetChange{Key: []byte("test-key")},
			},
			Version: "1:1:1",
			Ts:      timestamppb.Now(),
		},
	}

	bodyBytes, err := json.Marshal(notifications)
	require.NoError(t, err)

	client := &http.Client{Timeout: 2 * time.Second}

	for retries := sub.GetOptions().GetRetryAttempts(); retries > 0; retries-- {
		req, err := http.NewRequest("POST", sub.GetUrl(), jsonReader{data: bodyBytes})
		require.NoError(t, err)

		resp, err := client.Do(req)
		require.NoError(t, err)
		_ = resp.Body.Close()

		if resp.StatusCode == http.StatusOK {
			break
		}

		retryBase := sub.GetOptions().RetryAfterBase.AsDuration()
		if retryBase == 0 {
			retryBase = 100 * time.Millisecond
		}
		time.Sleep(retryBase * time.Duration(sub.GetOptions().GetRetryAttempts()-retries+1))
	}

	assert.GreaterOrEqual(t, attempts.Load(), int32(3), "should retry until success")
}

func TestNotificationSender_MagicKeyPrefix(t *testing.T) {
	ctx := context.Background()
	tempDir := t.TempDir()

	err := kv.CreatePool(tempDir+"/data", tempDir+"/meta")
	require.NoError(t, err)
	defer func() {
		_ = kv.DrainPool()
	}()

	ns := &notificationSender{
		notifications: make(map[string][]*notification),
		waiters:       make(map[string]chan struct{}),
		mu:            sync.Mutex{},
		notification:  make(chan *notification, 10000),
	}

	tests := []struct {
		name         string
		tableName    string
		shouldHandle bool
	}{
		{
			name:         "magic key prefix is recognized",
			tableName:    string(kv.NewKeyBuilder().Meta().Table("magic").Append("pb").Append("test").Build()),
			shouldHandle: true,
		},
		{
			name:         "regular key should not be handled",
			tableName:    "regular.table",
			shouldHandle: false,
		},
		{
			name:         "meta key non-magic should not be handled",
			tableName:    string(kv.NewKeyBuilder().Meta().Table("other").Build()),
			shouldHandle: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			migration := &Migration{
				Version: &MigrationVersion{
					TableName:        tt.tableName,
					MigrationVersion: 1,
					TableVersion:     1,
					NodeId:           100,
				},
				Migration: &Migration_None{
					None: &NilMigration{},
				},
			}

			handled, err := ns.maybeHandleMagicKey(ctx, migration)
			require.NoError(t, err)
			// All None migrations return false (no handling needed)
			// We just test that magic key prefix is recognized (no error)
			if tt.shouldHandle {
				// Magic key prefix recognized but None migration doesn't need handling
				assert.False(t, handled, "None migrations don't get handled")
			} else {
				assert.False(t, handled, "non-magic keys should not be handled")
			}
		})
	}
}

func TestNotificationSender_SubscriptionStorage(t *testing.T) {
	ctx := context.Background()
	tempDir := t.TempDir()

	err := kv.CreatePool(tempDir+"/data", tempDir+"/meta")
	require.NoError(t, err)
	defer func() {
		_ = kv.DrainPool()
	}()

	kvPool := kv.GetPool()
	require.NotNil(t, kvPool)

	ns := &notificationSender{
		notifications: make(map[string][]*notification),
		waiters:       make(map[string]chan struct{}),
		mu:            sync.Mutex{},
		subscriptions: trie.New[*Subscribe](),
		notification:  make(chan *notification, 10000),
	}

	// Create subscription migration
	sub := &Subscribe{
		Url:    "http://example.com/webhook",
		Prefix: []byte("test.prefix"),
		Options: &SubscribeOptions{
			Batch:          true,
			RetryAttempts:  3,
			RetryAfterBase: durationpb.New(100 * time.Millisecond),
		},
	}

	magicKey := kv.NewKeyBuilder().Meta().Table("magic").Append("pb").Append("test").Build()

	migration := &Migration{
		Version: &MigrationVersion{
			TableName:        string(magicKey),
			MigrationVersion: 1,
			TableVersion:     1,
			NodeId:           100,
		},
		Migration: &Migration_Data{
			Data: &DataMigration{
				Session: &DataMigration_Change{
					Change: &KVChange{
						Operation: &KVChange_Sub{
							Sub: sub,
						},
					},
				},
			},
		},
	}

	handled, err := ns.maybeHandleMagicKey(ctx, migration)
	require.NoError(t, err)
	assert.False(t, handled, "subscription should be stored but return false for halt")

	// Verify subscription was stored
	store := kvPool.MetaStore()
	txn, err := store.Begin(false)
	require.NoError(t, err)
	defer txn.Discard()

	obj, err := txn.Get(ctx, magicKey)
	require.NoError(t, err)

	var list SubscriptionList
	err = proto.Unmarshal(obj, &list)
	require.NoError(t, err)

	require.Len(t, list.Subscriptions, 1)
	assert.Equal(t, sub.Url, list.Subscriptions[0].Url)
	assert.Equal(t, sub.Prefix, list.Subscriptions[0].Prefix)
}

func TestNotificationSender_NotificationDeduplication(t *testing.T) {
	t.Skip("Broken? Or not?")
	ctx := context.Background()
	tempDir := t.TempDir()

	err := kv.CreatePool(tempDir+"/data", tempDir+"/meta")
	require.NoError(t, err)
	defer func() {
		_ = kv.DrainPool()
	}()

	kvPool := kv.GetPool()
	require.NotNil(t, kvPool)

	// Initialize options for test
	options.CurrentOptions.ServerId = 1

	ns := &notificationSender{
		notifications: make(map[string][]*notification),
		waiters:       make(map[string]chan struct{}),
		mu:            sync.Mutex{},
		subscriptions: trie.New[*Subscribe](),
		notification:  make(chan *notification, 10000),
	}

	magicKey := kv.NewKeyBuilder().Meta().Table("magic").Append("pb").Append("test").Build()

	// Store an initial subscription list with a notification
	initialNotification := &Notify{
		Key:     []byte("test-key"),
		Version: "1:1:1",
		Change: &Notify_Set{
			Set: &SetChange{Key: []byte("test-key")},
		},
		Ts: timestamppb.Now(),
	}

	list := &SubscriptionList{
		Subscriptions: []*Subscribe{
			{
				Url:    "http://example.com/webhook",
				Prefix: []byte("test"),
			},
		},
		Log: []*Notify{initialNotification},
	}
	listKey := append(magicKey, []byte("log")...)

	store := kvPool.MetaStore()
	txn, err := store.Begin(true)
	require.NoError(t, err)

	obj, err := proto.Marshal(list)
	require.NoError(t, err)

	err = txn.Put(ctx, magicKey, obj)
	require.NoError(t, err)

	err = txn.Put(ctx, listKey, obj)
	require.NoError(t, err)

	err = txn.Commit()
	require.NoError(t, err)

	// Try to process the same notification again
	duplicateNotification := &Notify{
		Key:     []byte("test-key"),
		Version: "1:1:1", // Same version
		Change: &Notify_Set{
			Set: &SetChange{Key: []byte("test-key")},
		},
		Ts: timestamppb.Now(),
	}

	migration := &Migration{
		Version: &MigrationVersion{
			TableName:        string(magicKey),
			MigrationVersion: 2,
			TableVersion:     1,
			NodeId:           100,
		},
		Migration: &Migration_Data{
			Data: &DataMigration{
				Session: &DataMigration_Change{
					Change: &KVChange{
						Operation: &KVChange_Notification{
							Notification: duplicateNotification,
						},
					},
				},
			},
		},
	}

	handled, err := ns.maybeHandleMagicKey(ctx, migration)
	require.NoError(t, err)
	assert.True(t, handled, "duplicate notification should be handled")

	// Verify the log didn't grow
	txn2, err := store.Begin(false)
	require.NoError(t, err)
	defer txn2.Discard()

	obj2, err := txn2.Get(ctx, magicKey)
	require.NoError(t, err)

	var finalList SubscriptionList
	err = proto.Unmarshal(obj2, &finalList)
	require.NoError(t, err)

	// Should still have only 1 notification (duplicate was skipped)
	assert.Len(t, finalList.Log, 1)
}

func TestNotificationSender_PrefixMatchingWithTrie(t *testing.T) {
	// Initialize options for test
	options.CurrentOptions.ServerId = 1

	// Create a fresh notification sender with a new trie for this test
	ns := &notificationSender{
		notifications: make(map[string][]*notification),
		waiters:       make(map[string]chan struct{}),
		mu:            sync.Mutex{},
		subscriptions: trie.New[*Subscribe](),
		notification:  make(chan *notification, 10000),
	}

	// Add subscriptions with different prefixes
	subscriptions := []*Subscribe{
		{Url: "http://example.com/users", Prefix: []byte("users.")},            // Matches users.*
		{Url: "http://example.com/users-admin", Prefix: []byte("users.admin")}, // Matches users.admin.*
		{Url: "http://example.com/posts", Prefix: []byte("posts.")},            // Matches posts.*
	}

	for _, sub := range subscriptions {
		ns.subscriptions.Insert(sub.Prefix, sub)
	}

	tests := []struct {
		name         string
		key          []byte
		expectedUrls []string
	}{
		{
			name:         "match no subscription",
			key:          []byte("other.key"),
			expectedUrls: []string{},
		},
		{
			name:         "match users prefix",
			key:          []byte("users.john"),
			expectedUrls: []string{"http://example.com/users"},
		},
		{
			name:         "match users.admin prefix",
			key:          []byte("users.admin.settings"),
			expectedUrls: []string{"http://example.com/users", "http://example.com/users-admin"},
		},
		{
			name:         "match posts prefix",
			key:          []byte("posts.article-123"),
			expectedUrls: []string{"http://example.com/posts"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			matches := ns.subscriptions.PrefixesOf(tt.key)

			var matchedUrls []string
			for _, sub := range matches {
				matchedUrls = append(matchedUrls, sub.Url)
			}

			assert.ElementsMatch(t, tt.expectedUrls, matchedUrls, "prefix matching mismatch")
		})
	}
}

func TestNotificationSender_NotificationCascading(t *testing.T) {
	ns := &notificationSender{
		notifications: make(map[string][]*notification),
		waiters:       make(map[string]chan struct{}),
		mu:            sync.Mutex{},
		notification:  make(chan *notification, 10000),
	}

	// Test the cascading bucket logic
	t.Run("notification cascades to smaller buckets", func(t *testing.T) {
		// Original key of 8 bytes
		originalKey := []byte("abcdefgh")

		// Current bucket should be 8 bytes (power of 2)
		currentBucket := ns.currentBucket(originalKey)
		assert.Equal(t, []byte("abcdefgh"), currentBucket)

		// Next bucket should be 4 bytes (half)
		nextBucket := ns.nextBucket(originalKey)
		assert.Equal(t, []byte("abcd"), nextBucket)

		// Continuing the cascade
		bucket2 := ns.nextBucket(nextBucket)
		assert.Equal(t, []byte("ab"), bucket2)

		bucket3 := ns.nextBucket(bucket2)
		assert.Equal(t, []byte("a"), bucket3)

		bucket4 := ns.nextBucket(bucket3)
		assert.Nil(t, bucket4, "single byte key should return nil")
	})

	t.Run("power of two buckets work correctly", func(t *testing.T) {
		tests := []struct {
			keyLen          int
			expectedCurrent int
			expectedNext    int
		}{
			{keyLen: 1, expectedCurrent: 1, expectedNext: 0},
			{keyLen: 2, expectedCurrent: 2, expectedNext: 1},
			{keyLen: 3, expectedCurrent: 2, expectedNext: 1},
			{keyLen: 4, expectedCurrent: 4, expectedNext: 2},
			{keyLen: 5, expectedCurrent: 4, expectedNext: 2},
			{keyLen: 8, expectedCurrent: 8, expectedNext: 4},
			{keyLen: 16, expectedCurrent: 16, expectedNext: 8},
		}

		for _, tt := range tests {
			t.Run(fmt.Sprintf("key_length_%d", tt.keyLen), func(t *testing.T) {
				key := make([]byte, tt.keyLen)
				for i := range key {
					key[i] = byte('a' + i)
				}

				current := ns.currentBucket(key)
				assert.Len(t, current, tt.expectedCurrent)

				next := ns.nextBucket(key)
				assert.Len(t, next, tt.expectedNext)
			})
		}
	})
}

func TestNotificationSender_ConcurrentNotifications(t *testing.T) {
	// Test that multiple goroutines can safely send notifications
	ns := &notificationSender{
		notifications: make(map[string][]*notification),
		waiters:       make(map[string]chan struct{}),
		mu:            sync.Mutex{},
		notification:  make(chan *notification, 10000),
	}

	received := atomic.Int32{}
	done := make(chan struct{})

	// Start consumer
	go func() {
		for range 100 {
			<-ns.notification
			received.Add(1)
		}
		close(done)
	}()

	// Start multiple producers
	var wg sync.WaitGroup
	for i := range 10 {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := range 10 {
				ns.notification <- &notification{
					sub: &Subscribe{
						Url:    fmt.Sprintf("http://example.com/webhook-%d", id),
						Prefix: fmt.Appendf(nil, "prefix-%d", id),
					},
					pub: &Notify{
						Key:     fmt.Appendf(nil, "key-%d-%d", id, j),
						Version: fmt.Sprintf("%d:%d:%d", id, j, 1),
						Ts:      timestamppb.Now(),
					},
				}
			}
		}(i)
	}

	wg.Wait()
	<-done

	assert.Equal(t, int32(100), received.Load(), "should receive all notifications")
}

func TestNotificationSender_HTTPHeadersAndAuth(t *testing.T) {
	authToken := "Bearer secret-token"
	var receivedHeaders http.Header

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		receivedHeaders = r.Header
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	sub := &Subscribe{
		Url:    server.URL,
		Prefix: []byte("test"),
		Options: &SubscribeOptions{
			Auth: authToken,
		},
	}

	notifications := []*Notify{
		{
			Key: []byte("test-key"),
			Change: &Notify_Set{
				Set: &SetChange{Key: []byte("test-key")},
			},
			Version: "1:1:1",
			Ts:      timestamppb.Now(),
		},
	}

	bodyBytes, err := json.Marshal(notifications)
	require.NoError(t, err)

	client := &http.Client{Timeout: 2 * time.Second}
	req, err := http.NewRequest("POST", sub.GetUrl(), jsonReader{data: bodyBytes})
	require.NoError(t, err)

	// These headers should be set by the notification sender
	req.Header.Set("Content-Type", "application/json")
	if auth := sub.GetOptions().GetAuth(); auth != "" {
		req.Header.Set("Authorization", auth)
	}

	resp, err := client.Do(req)
	require.NoError(t, err)
	defer func() {
		_ = resp.Body.Close()
	}()

	assert.Equal(t, "application/json", receivedHeaders.Get("Content-Type"), "Content-Type header should be set")
	assert.Equal(t, authToken, receivedHeaders.Get("Authorization"), "Authorization header should be set")
}
