package server

import (
	"context"
	"fmt"
	"net"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/bottledcode/atlas-db/pkg/client"
	"github.com/bottledcode/atlas-db/pkg/config"
	"github.com/bottledcode/atlas-db/proto/atlas"
)

func setupTestServer(t *testing.T) (*Server, string, func()) {
	// Create temporary directory for test data
	tmpDir := t.TempDir()

	// Find available port first
	listener, err := net.Listen("tcp", "localhost:0")
	if err != nil {
		t.Fatalf("Failed to listen: %v", err)
	}
	address := listener.Addr().String()
	port := listener.Addr().(*net.TCPAddr).Port
	listener.Close()

	// Create test configuration with actual port
	cfg := &config.Config{
		Node: config.NodeConfig{
			ID:       "test-node-1",
			Address:  address,
			DataDir:  tmpDir,
			RegionID: 1,
		},
		Storage: config.StorageConfig{
			SyncWrites:       true,
			NumVersions:      10,
			GCInterval:       5 * time.Minute,
			ValueLogGCRatio:  0.5,
			MaxTableSize:     64 << 20,
			LevelOneSize:     256 << 20,
			ValueLogFileSize: 1 << 30,
		},
		Transport: config.TransportConfig{
			Port:                port,
			MaxRecvMsgSize:      4 << 20,
			MaxSendMsgSize:      4 << 20,
			ConnectionTimeout:   10 * time.Second,
			KeepAliveTime:       30 * time.Second,
			KeepAliveTimeout:    5 * time.Second,
			MaxConnectionIdle:   15 * time.Minute,
			MaxConnectionAge:    30 * time.Minute,
			MaxConcurrentStream: 100,
		},
		Consensus: config.ConsensusConfig{
			PrepareTimeout:    2 * time.Second,
			AcceptTimeout:     2 * time.Second,
			CommitTimeout:     1 * time.Second,
			HeartbeatInterval: 1 * time.Second,
			ElectionTimeout:   5 * time.Second,
		},
	}

	// Create server
	srv, err := NewServer(cfg)
	if err != nil {
		t.Fatalf("Failed to create server: %v", err)
	}

	// Start server
	err = srv.Start()
	if err != nil {
		t.Fatalf("Failed to start server: %v", err)
	}

	// Wait a bit for server to start
	time.Sleep(100 * time.Millisecond)

	cleanup := func() {
		if err := srv.Stop(); err != nil {
			t.Errorf("Failed to stop server: %v", err)
		}
	}

	return srv, address, cleanup
}

func TestServer_StartStop(t *testing.T) {
	srv, _, cleanup := setupTestServer(t)
	defer cleanup()

	if !srv.IsRunning() {
		t.Error("Expected server to be running")
	}

	err := srv.Stop()
	if err != nil {
		t.Fatalf("Stop failed: %v", err)
	}

	if srv.IsRunning() {
		t.Error("Expected server to be stopped")
	}

	// Second stop should not fail
	err = srv.Stop()
	if err != nil {
		t.Errorf("Second stop failed: %v", err)
	}
}

func TestServer_BasicDatabaseOperations(t *testing.T) {
	_, address, cleanup := setupTestServer(t)
	defer cleanup()

	// Connect via gRPC directly
	conn, err := grpc.Dial(address, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("Failed to connect: %v", err)
	}
	defer conn.Close()

	client := atlas.NewAtlasDBClient(conn)
	ctx := context.Background()

	key := "test-key"
	value := []byte("test-value")

	// Test Put
	putResp, err := client.Put(ctx, &atlas.PutRequest{
		Key:   key,
		Value: value,
	})
	if err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	if !putResp.Success {
		t.Errorf("Put failed: %s", putResp.Error)
	}

	if putResp.Version <= 0 {
		t.Error("Expected positive version")
	}

	// Test Get
	getResp, err := client.Get(ctx, &atlas.GetRequest{
		Key: key,
	})
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}

	if !getResp.Found {
		t.Error("Expected key to be found")
	}

	if string(getResp.Value) != string(value) {
		t.Errorf("Expected value %s, got %s", string(value), string(getResp.Value))
	}

	// Test Delete
	delResp, err := client.Delete(ctx, &atlas.DeleteRequest{
		Key: key,
	})
	if err != nil {
		t.Fatalf("Delete failed: %v", err)
	}

	if !delResp.Success {
		t.Errorf("Delete failed: %s", delResp.Error)
	}

	// Verify deletion
	getResp, err = client.Get(ctx, &atlas.GetRequest{
		Key: key,
	})
	if err != nil {
		t.Fatalf("Get after delete failed: %v", err)
	}

	if getResp.Found {
		t.Error("Expected key to be deleted")
	}
}

func TestServer_ScanOperation(t *testing.T) {
	_, address, cleanup := setupTestServer(t)
	defer cleanup()

	// Connect via gRPC
	conn, err := grpc.Dial(address, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("Failed to connect: %v", err)
	}
	defer conn.Close()

	client := atlas.NewAtlasDBClient(conn)
	ctx := context.Background()

	// Put test data
	testData := map[string]string{
		"key1": "value1",
		"key2": "value2",
		"key3": "value3",
	}

	for k, v := range testData {
		_, err := client.Put(ctx, &atlas.PutRequest{
			Key:   k,
			Value: []byte(v),
		})
		if err != nil {
			t.Fatalf("Put failed for %s: %v", k, err)
		}
	}

	// Test scan
	stream, err := client.Scan(ctx, &atlas.ScanRequest{
		StartKey: "",
		EndKey:   "",
		Limit:    10,
	})
	if err != nil {
		t.Fatalf("Scan failed: %v", err)
	}

	results := make(map[string]string)
	for {
		resp, err := stream.Recv()
		if err != nil {
			break // End of stream
		}

		results[resp.Key] = string(resp.Value)
	}

	if len(results) != len(testData) {
		t.Errorf("Expected %d results, got %d", len(testData), len(results))
	}

	for k, v := range testData {
		if results[k] != v {
			t.Errorf("Expected %s=%s, got %s=%s", k, v, k, results[k])
		}
	}
}

func TestServer_BatchOperation(t *testing.T) {
	_, address, cleanup := setupTestServer(t)
	defer cleanup()

	// Connect via gRPC
	conn, err := grpc.Dial(address, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("Failed to connect: %v", err)
	}
	defer conn.Close()

	client := atlas.NewAtlasDBClient(conn)
	ctx := context.Background()

	// Test batch operations
	operations := []*atlas.Operation{
		{Type: atlas.Operation_PUT, Key: "batch1", Value: []byte("value1")},
		{Type: atlas.Operation_PUT, Key: "batch2", Value: []byte("value2")},
		{Type: atlas.Operation_GET, Key: "batch1"},
		{Type: atlas.Operation_DELETE, Key: "batch2"},
	}

	resp, err := client.Batch(ctx, &atlas.BatchRequest{
		Operations: operations,
	})
	if err != nil {
		t.Fatalf("Batch failed: %v", err)
	}

	if len(resp.Results) != len(operations) {
		t.Errorf("Expected %d results, got %d", len(operations), len(resp.Results))
	}

	// Check Put results
	for i := range 2 {
		if !resp.Results[i].Success {
			t.Errorf("Expected Put operation %d to succeed: %s", i, resp.Results[i].Error)
		}
	}

	// Check Get result
	if !resp.Results[2].Success {
		t.Errorf("Expected Get operation to succeed: %s", resp.Results[2].Error)
	}
	if string(resp.Results[2].Value) != "value1" {
		t.Errorf("Expected Get to return 'value1', got %s", string(resp.Results[2].Value))
	}

	// Check Delete result
	if !resp.Results[3].Success {
		t.Errorf("Expected Delete operation to succeed: %s", resp.Results[3].Error)
	}
}

func TestServer_ClientLibraryIntegration(t *testing.T) {
	_, address, cleanup := setupTestServer(t)
	defer cleanup()

	// Connect using the client library
	db, err := client.Connect(address)
	if err != nil {
		t.Fatalf("Client connect failed: %v", err)
	}
	defer db.Close()

	ctx := context.Background()
	key := "client-test-key"
	value := []byte("client-test-value")

	// Test via client library
	putResult, err := db.Put(ctx, key, value)
	if err != nil {
		t.Fatalf("Client Put failed: %v", err)
	}

	getResult, err := db.Get(ctx, key)
	if err != nil {
		t.Fatalf("Client Get failed: %v", err)
	}

	if !getResult.Found {
		t.Error("Expected key to be found")
	}

	if string(getResult.Value) != string(value) {
		t.Errorf("Expected value %s, got %s", string(value), string(getResult.Value))
	}

	if getResult.Version != putResult.Version {
		t.Errorf("Expected version %d, got %d", putResult.Version, getResult.Version)
	}

	err = db.Delete(ctx, key)
	if err != nil {
		t.Fatalf("Client Delete failed: %v", err)
	}

	getResult, err = db.Get(ctx, key)
	if err != nil {
		t.Fatalf("Client Get after delete failed: %v", err)
	}

	if getResult.Found {
		t.Error("Expected key to be deleted")
	}
}

func TestServer_ConcurrentClients(t *testing.T) {
	_, address, cleanup := setupTestServer(t)
	defer cleanup()

	numClients := 5
	numOperations := 10

	// Create multiple clients
	clients := make([]client.Client, numClients)
	for i := range numClients {
		db, err := client.Connect(address)
		if err != nil {
			t.Fatalf("Client %d connect failed: %v", i, err)
		}
		clients[i] = db
		defer db.Close()
	}

	// Run concurrent operations
	ctx := context.Background()
	errChan := make(chan error, numClients)

	for i := range numClients {
		go func(clientID int) {
			db := clients[clientID]
			for j := range numOperations {
				key := fmt.Sprintf("client-%d-key-%d", clientID, j)
				value := fmt.Appendf(nil, "client-%d-value-%d", clientID, j)

				_, err := db.Put(ctx, key, value)
				if err != nil {
					errChan <- fmt.Errorf("client %d put %d failed: %w", clientID, j, err)
					return
				}

				result, err := db.Get(ctx, key)
				if err != nil {
					errChan <- fmt.Errorf("client %d get %d failed: %w", clientID, j, err)
					return
				}

				if !result.Found || string(result.Value) != string(value) {
					errChan <- fmt.Errorf("client %d verification %d failed", clientID, j)
					return
				}
			}
			errChan <- nil
		}(i)
	}

	// Wait for all clients
	for range numClients {
		if err := <-errChan; err != nil {
			t.Fatalf("Concurrent client test failed: %v", err)
		}
	}
}

func TestServer_Stats(t *testing.T) {
	srv, address, cleanup := setupTestServer(t)
	defer cleanup()

	// Add some data first
	db, err := client.Connect(address)
	if err != nil {
		t.Fatalf("Client connect failed: %v", err)
	}
	defer db.Close()

	ctx := context.Background()
	for i := range 10 {
		key := fmt.Sprintf("stats-key-%d", i)
		value := fmt.Appendf(nil, "stats-value-%d", i)
		_, err := db.Put(ctx, key, value)
		if err != nil {
			t.Fatalf("Put failed: %v", err)
		}
	}

	// Get stats
	stats, err := srv.GetStats()
	if err != nil {
		t.Fatalf("GetStats failed: %v", err)
	}

	if stats.NodeID != "test-node-1" {
		t.Errorf("Expected node ID 'test-node-1', got %s", stats.NodeID)
	}

	if stats.RegionID != 1 {
		t.Errorf("Expected region ID 1, got %d", stats.RegionID)
	}

	if !stats.Running {
		t.Error("Expected server to be running")
	}

	// Storage size might be 0 for small amounts of data in BadgerDB
	if stats.StorageStats == nil {
		t.Error("Expected storage stats to be present")
	} else {
		t.Logf("Storage stats: TotalSize=%d", stats.StorageStats.TotalSize)
	}
}

func TestServer_MultipleStartStop(t *testing.T) {
	tmpDir := t.TempDir()

	cfg := &config.Config{
		Node: config.NodeConfig{
			ID:       "test-node-multi",
			Address:  "localhost:0",
			DataDir:  tmpDir,
			RegionID: 1,
		},
		Storage: config.StorageConfig{
			SyncWrites:       true,
			NumVersions:      10,
			GCInterval:       5 * time.Minute,
			ValueLogGCRatio:  0.5,
			MaxTableSize:     64 << 20,
			LevelOneSize:     256 << 20,
			ValueLogFileSize: 1 << 30,
		},
		Transport: config.TransportConfig{Port: 0},
		Consensus: config.ConsensusConfig{},
	}

	srv, err := NewServer(cfg)
	if err != nil {
		t.Fatalf("Failed to create server: %v", err)
	}

	// Start and stop multiple times
	for i := range 3 {
		err := srv.Start()
		if err != nil {
			t.Fatalf("Start %d failed: %v", i, err)
		}

		if !srv.IsRunning() {
			t.Errorf("Expected server to be running after start %d", i)
		}

		err = srv.Stop()
		if err != nil {
			t.Fatalf("Stop %d failed: %v", i, err)
		}

		if srv.IsRunning() {
			t.Errorf("Expected server to be stopped after stop %d", i)
		}
	}
}

func TestServer_InvalidConfig(t *testing.T) {
	// Test with invalid data directory
	cfg := &config.Config{
		Node: config.NodeConfig{
			ID:       "test-node-invalid",
			Address:  "localhost:0",
			DataDir:  "/invalid/path/that/does/not/exist",
			RegionID: 1,
		},
	}

	_, err := NewServer(cfg)
	if err == nil {
		t.Error("Expected server creation to fail with invalid config")
	}
}

func TestServer_AddRemoveNodes(t *testing.T) {
	srv, _, cleanup := setupTestServer(t)
	defer cleanup()

	nodeInfo := &atlas.NodeInfo{
		NodeId:   "test-node-2",
		Address:  "localhost:8081",
		RegionId: 2,
		IsLeader: false,
	}

	// Add node
	srv.AddNode(nodeInfo)

	// Remove node
	srv.RemoveNode("test-node-2", 2)

	// No direct way to verify, but should not crash
}

func BenchmarkServer_PutGet(b *testing.B) {
	// Create a testing.T wrapper for setupTestServer
	t := &testing.T{}
	_, address, cleanup := setupTestServer(t)
	defer cleanup()

	db, err := client.Connect(address)
	if err != nil {
		b.Fatalf("Client connect failed: %v", err)
	}
	defer db.Close()

	ctx := context.Background()

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			key := fmt.Sprintf("bench-key-%d", i)
			value := fmt.Appendf(nil, "bench-value-%d", i)

			_, err := db.Put(ctx, key, value)
			if err != nil {
				b.Fatalf("Put failed: %v", err)
			}

			result, err := db.Get(ctx, key)
			if err != nil {
				b.Fatalf("Get failed: %v", err)
			}

			if !result.Found {
				b.Fatal("Key not found")
			}

			i++
		}
	})
}
