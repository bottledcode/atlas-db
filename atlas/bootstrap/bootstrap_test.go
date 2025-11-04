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

package bootstrap

import (
	"context"
	"net"
	"path/filepath"
	"testing"

	"github.com/bottledcode/atlas-db/atlas/kv"
	"github.com/bottledcode/atlas-db/atlas/options"
	"go.uber.org/zap/zaptest"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/proto"
)

func setupTestEnvironment(t *testing.T) (string, string, func()) {
	// Create temporary directories for test data
	tempDir := t.TempDir()
	dataPath := filepath.Join(tempDir, "data")
	metaPath := filepath.Join(tempDir, "meta")

	// Initialize test logger
	options.Logger = zaptest.NewLogger(t)

	// Set test options
	options.CurrentOptions = &options.Options{
		DbFilename:       dataPath,
		MetaFilename:     metaPath,
		Region:           "test-region",
		AdvertiseAddress: "localhost",
		AdvertisePort:    8080,
		ServerId:         1,
		ApiKey:           "test-api-key",
	}

	cleanup := func() {
		if pool := kv.GetPool(); pool != nil {
			_ = pool.Close()
		}
		_ = kv.DrainPool()
	}

	return dataPath, metaPath, cleanup
}

func TestBootstrapServer_GetBootstrapData(t *testing.T) {
	dataPath, metaPath, cleanup := setupTestEnvironment(t)
	defer cleanup()

	// Initialize KV stores with test data
	err := kv.CreatePool(dataPath, metaPath)
	if err != nil {
		t.Fatalf("Failed to create KV pool: %v", err)
	}

	pool := kv.GetPool()
	if pool == nil {
		t.Fatal("KV pool is nil")
	}

	// Add some test data to metadata store
	metaStore := pool.MetaStore()
	if metaStore == nil {
		t.Fatal("Metadata store is nil")
	}

	// Insert test metadata
	ctx := context.Background()
	testMetaData := map[string][]byte{
		"test-node-1":      []byte("node-data-1"),
		"test-table-1":     []byte("table-config-1"),
		"test-migration-1": []byte("migration-data-1"),
	}

	for key, value := range testMetaData {
		err = metaStore.Put(ctx, []byte(key), value)
		if err != nil {
			t.Fatalf("Failed to put test data: %v", err)
		}
	}

	// Add some test data to data store
	dataStore := pool.DataStore()
	if dataStore == nil {
		t.Fatal("Data store is nil")
	}

	testUserData := map[string][]byte{
		"user-key-1": []byte("user-value-1"),
		"user-key-2": []byte("user-value-2"),
	}

	for key, value := range testUserData {
		err = dataStore.Put(ctx, []byte(key), value)
		if err != nil {
			t.Fatalf("Failed to put user data: %v", err)
		}
	}

	// Set up gRPC server with buffer connection
	lis := bufconn.Listen(1024 * 1024)
	server := grpc.NewServer()
	RegisterBootstrapServer(server, &Server{})

	go func() {
		if err := server.Serve(lis); err != nil {
			t.Logf("Server error: %v", err)
		}
	}()
	defer server.Stop()

	// Create client connection
	//nolint:staticcheck // grpc.DialContext is deprecated but still needed for tests
	conn, err := grpc.DialContext(ctx, "bufnet",
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
			return lis.Dial()
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("Failed to dial server: %v", err)
	}
	defer func() { _ = conn.Close() }()

	client := NewBootstrapClient(conn)

	// Test GetBootstrapData
	stream, err := client.GetBootstrapData(ctx, &BootstrapRequest{Version: 1})
	if err != nil {
		t.Fatalf("GetBootstrapData failed: %v", err)
	}

	// Collect all chunks
	var completeData []byte
	chunkCount := 0

	for {
		response, err := stream.Recv()
		if err != nil {
			t.Fatalf("Failed to receive chunk: %v", err)
		}

		if response.GetIncompatibleVersion() != nil {
			t.Fatalf("Incompatible version: %d", response.GetIncompatibleVersion().GetNeedsVersion())
		}

		data := response.GetBootstrapData().GetData()
		if len(data) == 0 {
			// End of stream
			break
		}

		completeData = append(completeData, data...)
		chunkCount++
	}

	t.Logf("Received %d chunks, total size: %d bytes", chunkCount, len(completeData))

	// Parse the received data
	var snapshot DatabaseSnapshot
	err = proto.Unmarshal(completeData, &snapshot)
	if err != nil {
		t.Fatalf("Failed to unmarshal snapshot: %v", err)
	}

	// Verify metadata entries
	metaEntryMap := make(map[string][]byte)
	for _, entry := range snapshot.MetaEntries {
		metaEntryMap[string(entry.Key)] = entry.Value
	}

	for key, expectedValue := range testMetaData {
		if actualValue, exists := metaEntryMap[key]; !exists {
			t.Errorf("Missing metadata key: %s", key)
		} else if string(actualValue) != string(expectedValue) {
			t.Errorf("Metadata mismatch for key %s: expected %s, got %s",
				key, string(expectedValue), string(actualValue))
		}
	}

	// Verify data entries
	dataEntryMap := make(map[string][]byte)
	for _, entry := range snapshot.DataEntries {
		dataEntryMap[string(entry.Key)] = entry.Value
	}

	for key, expectedValue := range testUserData {
		if actualValue, exists := dataEntryMap[key]; !exists {
			t.Errorf("Missing data key: %s", key)
		} else if string(actualValue) != string(expectedValue) {
			t.Errorf("Data mismatch for key %s: expected %s, got %s",
				key, string(expectedValue), string(actualValue))
		}
	}

	t.Logf("Successfully verified %d metadata entries and %d data entries",
		len(snapshot.MetaEntries), len(snapshot.DataEntries))
}

func TestBootstrapServer_IncompatibleVersion(t *testing.T) {
	dataPath, metaPath, cleanup := setupTestEnvironment(t)
	defer cleanup()

	// Initialize empty KV stores
	err := kv.CreatePool(dataPath, metaPath)
	if err != nil {
		t.Fatalf("Failed to create KV pool: %v", err)
	}

	// Set up gRPC server
	lis := bufconn.Listen(1024 * 1024)
	server := grpc.NewServer()
	RegisterBootstrapServer(server, &Server{})

	go func() {
		if err := server.Serve(lis); err != nil {
			t.Logf("Server error: %v", err)
		}
	}()
	defer server.Stop()

	ctx := context.Background()
	//nolint:staticcheck // grpc.DialContext is deprecated but still needed for tests
	conn, err := grpc.DialContext(ctx, "bufnet",
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
			return lis.Dial()
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("Failed to dial server: %v", err)
	}
	defer func() { _ = conn.Close() }()

	client := NewBootstrapClient(conn)

	// Test incompatible version
	stream, err := client.GetBootstrapData(ctx, &BootstrapRequest{Version: 2})
	if err != nil {
		t.Fatalf("GetBootstrapData failed: %v", err)
	}

	response, err := stream.Recv()
	if err != nil {
		t.Fatalf("Failed to receive response: %v", err)
	}

	if response.GetIncompatibleVersion() == nil {
		t.Fatal("Expected incompatible version response")
	}

	if response.GetIncompatibleVersion().GetNeedsVersion() != 1 {
		t.Errorf("Expected needs version 1, got %d",
			response.GetIncompatibleVersion().GetNeedsVersion())
	}
}

func TestDoBootstrap_Integration(t *testing.T) {
	ctx := context.Background()

	// Setup source environment with test data
	sourceDataPath, sourceMetaPath, sourceCleanup := setupTestEnvironment(t)
	defer sourceCleanup()

	// Initialize source KV stores with test data
	err := kv.CreatePool(sourceDataPath, sourceMetaPath)
	if err != nil {
		t.Fatalf("Failed to create source KV pool: %v", err)
	}

	sourcePool := kv.GetPool()
	if sourcePool == nil {
		t.Fatal("Source KV pool is nil after creation")
	}

	// Populate source with test data
	metaStore := sourcePool.MetaStore()
	if metaStore == nil {
		t.Fatal("Source metadata store is nil")
	}

	dataStore := sourcePool.DataStore()
	if dataStore == nil {
		t.Fatal("Source data store is nil")
	}

	err = metaStore.Put(ctx, []byte("source-meta-key"), []byte("source-meta-value"))
	if err != nil {
		t.Fatalf("Failed to put source meta data: %v", err)
	}

	err = dataStore.Put(ctx, []byte("source-data-key"), []byte("source-data-value"))
	if err != nil {
		t.Fatalf("Failed to put source data: %v", err)
	}

	// Start bootstrap server
	lis := bufconn.Listen(1024 * 1024)
	server := grpc.NewServer()
	RegisterBootstrapServer(server, &Server{})

	go func() {
		if err := server.Serve(lis); err != nil {
			t.Logf("Server error: %v", err)
		}
	}()
	defer server.Stop()

	// Setup target environment paths

	// Create a custom dialer for the test
	customDialer := func(context.Context, string) (net.Conn, error) {
		return lis.Dial()
	}

	// Mock the DoBootstrap by directly calling with custom connection
	//nolint:staticcheck // grpc.DialContext is deprecated but still needed for tests
	conn, err := grpc.DialContext(ctx, "bufnet",
		grpc.WithContextDialer(customDialer),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("Failed to dial server: %v", err)
	}
	defer func() { _ = conn.Close() }()

	client := NewBootstrapClient(conn)
	stream, err := client.GetBootstrapData(ctx, &BootstrapRequest{Version: 1})
	if err != nil {
		t.Fatalf("GetBootstrapData failed: %v", err)
	}

	// Manually perform bootstrap process (similar to DoBootstrap)
	var completeData []byte
	for {
		response, err := stream.Recv()
		if err != nil {
			t.Fatalf("Failed to receive response: %v", err)
		}

		if response.GetIncompatibleVersion() != nil {
			t.Fatalf("Incompatible version: %d", response.GetIncompatibleVersion().GetNeedsVersion())
		}

		data := response.GetBootstrapData().GetData()
		if len(data) == 0 {
			break
		}

		completeData = append(completeData, data...)
	}

	// Parse snapshot
	var snapshot DatabaseSnapshot
	err = proto.Unmarshal(completeData, &snapshot)
	if err != nil {
		t.Fatalf("Failed to unmarshal snapshot: %v", err)
	}

	// Setup target environment with separate paths
	targetTempDir := t.TempDir()
	targetDataPath := filepath.Join(targetTempDir, "target-data")
	targetMetaPath := filepath.Join(targetTempDir, "target-meta")

	// Clean up source pool before creating target (fixed sync.Once issue)
	sourceCleanup()

	// Create target KV stores
	err = kv.CreatePool(targetDataPath, targetMetaPath)
	if err != nil {
		t.Fatalf("Failed to create target KV pool: %v", err)
	}

	targetPool := kv.GetPool()
	if targetPool == nil {
		t.Fatal("Target KV pool is nil")
	}

	targetMetaStore := targetPool.MetaStore()
	if targetMetaStore == nil {
		t.Fatal("Target metadata store is nil")
	}

	targetDataStore := targetPool.DataStore()
	if targetDataStore == nil {
		t.Fatal("Target data store is nil")
	}

	// Apply metadata entries to the fresh target stores (simulates bootstrap restore)
	t.Logf("Applying %d metadata entries", len(snapshot.MetaEntries))
	for _, entry := range snapshot.MetaEntries {
		if entry.Key == nil {
			t.Fatalf("Entry key is nil")
		}
		err = targetMetaStore.Put(ctx, entry.Key, entry.Value)
		if err != nil {
			t.Fatalf("Failed to apply meta entry: %v", err)
		}
	}

	// Apply data entries
	t.Logf("Applying %d data entries", len(snapshot.DataEntries))
	for _, entry := range snapshot.DataEntries {
		if entry.Key == nil {
			t.Fatalf("Entry key is nil")
		}
		err = targetDataStore.Put(ctx, entry.Key, entry.Value)
		if err != nil {
			t.Fatalf("Failed to apply data entry: %v", err)
		}
	}

	// Verify target stores contain the expected data
	metaValue, err := targetMetaStore.Get(ctx, []byte("source-meta-key"))
	if err != nil {
		t.Fatalf("Failed to get meta value from target: %v", err)
	}
	if string(metaValue) != "source-meta-value" {
		t.Errorf("Meta value mismatch: expected 'source-meta-value', got %s", string(metaValue))
	}

	dataValue, err := targetDataStore.Get(ctx, []byte("source-data-key"))
	if err != nil {
		t.Fatalf("Failed to get data value from target: %v", err)
	}
	if string(dataValue) != "source-data-value" {
		t.Errorf("Data value mismatch: expected 'source-data-value', got %s", string(dataValue))
	}

	t.Log("Bootstrap integration test completed successfully")
}

