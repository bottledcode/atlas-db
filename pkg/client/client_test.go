package client

import (
	"context"
	"fmt"
	"net"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"

	"github.com/bottledcode/atlas-db/proto/atlas"
)

// Mock Atlas DB server for testing
type mockAtlasServer struct {
	atlas.UnimplementedAtlasDBServer
	data map[string]*mockValue
}

type mockValue struct {
	value   []byte
	version int64
}

func newMockServer() *mockAtlasServer {
	return &mockAtlasServer{
		data: make(map[string]*mockValue),
	}
}

func (m *mockAtlasServer) Get(ctx context.Context, req *atlas.GetRequest) (*atlas.GetResponse, error) {
	value, exists := m.data[req.Key]
	if !exists {
		return &atlas.GetResponse{Found: false}, nil
	}

	return &atlas.GetResponse{
		Value:   value.value,
		Found:   true,
		Version: value.version,
	}, nil
}

func (m *mockAtlasServer) Put(ctx context.Context, req *atlas.PutRequest) (*atlas.PutResponse, error) {
	// Check expected version if specified
	if req.ExpectedVersion > 0 {
		existing, exists := m.data[req.Key]
		if exists && existing.version != req.ExpectedVersion {
			return &atlas.PutResponse{
				Success: false,
				Error:   fmt.Sprintf("version mismatch: expected %d, got %d", req.ExpectedVersion, existing.version),
			}, nil
		}
	}

	newVersion := time.Now().UnixNano()
	m.data[req.Key] = &mockValue{
		value:   req.Value,
		version: newVersion,
	}

	return &atlas.PutResponse{
		Success: true,
		Version: newVersion,
	}, nil
}

func (m *mockAtlasServer) Delete(ctx context.Context, req *atlas.DeleteRequest) (*atlas.DeleteResponse, error) {
	// Check expected version if specified
	if req.ExpectedVersion > 0 {
		existing, exists := m.data[req.Key]
		if !exists {
			return &atlas.DeleteResponse{
				Success: false,
				Error:   "key not found",
			}, nil
		}
		if existing.version != req.ExpectedVersion {
			return &atlas.DeleteResponse{
				Success: false,
				Error:   fmt.Sprintf("version mismatch: expected %d, got %d", req.ExpectedVersion, existing.version),
			}, nil
		}
	}

	delete(m.data, req.Key)
	return &atlas.DeleteResponse{Success: true}, nil
}

func (m *mockAtlasServer) Scan(req *atlas.ScanRequest, stream atlas.AtlasDB_ScanServer) error {
	count := 0
	for key, value := range m.data {
		// Simple key filtering
		if req.StartKey != "" && key < req.StartKey {
			continue
		}
		if req.EndKey != "" && key >= req.EndKey {
			continue
		}

		if req.Limit > 0 && int32(count) >= req.Limit {
			break
		}

		resp := &atlas.ScanResponse{
			Key:     key,
			Value:   value.value,
			Version: value.version,
		}

		if err := stream.Send(resp); err != nil {
			return err
		}
		count++
	}

	return nil
}

func (m *mockAtlasServer) Batch(ctx context.Context, req *atlas.BatchRequest) (*atlas.BatchResponse, error) {
	results := make([]*atlas.OperationResult, len(req.Operations))

	for i, op := range req.Operations {
		switch op.Type {
		case atlas.Operation_GET:
			value, exists := m.data[op.Key]
			if exists {
				results[i] = &atlas.OperationResult{
					Success: true,
					Value:   value.value,
					Version: value.version,
				}
			} else {
				results[i] = &atlas.OperationResult{
					Success: false,
				}
			}

		case atlas.Operation_PUT:
			newVersion := time.Now().UnixNano()
			m.data[op.Key] = &mockValue{
				value:   op.Value,
				version: newVersion,
			}
			results[i] = &atlas.OperationResult{
				Success: true,
				Version: newVersion,
			}

		case atlas.Operation_DELETE:
			delete(m.data, op.Key)
			results[i] = &atlas.OperationResult{
				Success: true,
			}

		default:
			results[i] = &atlas.OperationResult{
				Success: false,
				Error:   "unsupported operation",
			}
		}
	}

	return &atlas.BatchResponse{Results: results}, nil
}

func setupTestServer(t *testing.T) (*grpc.Server, *bufconn.Listener, func()) {
	buffer := 1024 * 1024
	listener := bufconn.Listen(buffer)

	server := grpc.NewServer()
	mockServer := newMockServer()
	atlas.RegisterAtlasDBServer(server, mockServer)

	go func() {
		if err := server.Serve(listener); err != nil {
			t.Logf("Server serve error: %v", err)
		}
	}()

	cleanup := func() {
		server.Stop()
		listener.Close()
	}

	return server, listener, cleanup
}

func setupTestClient(t *testing.T, listener *bufconn.Listener) (Client, func()) {
	conn, err := grpc.Dial("bufnet",
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
			return listener.Dial()
		}),
		grpc.WithInsecure(),
	)
	if err != nil {
		t.Fatalf("Failed to dial bufnet: %v", err)
	}

	client := &client{
		conn: conn,
		grpc: atlas.NewAtlasDBClient(conn),
	}

	cleanup := func() {
		client.Close()
	}

	return client, cleanup
}

func TestClient_BasicOperations(t *testing.T) {
	_, listener, serverCleanup := setupTestServer(t)
	defer serverCleanup()

	client, clientCleanup := setupTestClient(t, listener)
	defer clientCleanup()

	ctx := context.Background()
	key := "test-key"
	value := []byte("test-value")

	// Test Put
	putResult, err := client.Put(ctx, key, value)
	if err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	if putResult.Version <= 0 {
		t.Error("Expected positive version")
	}

	// Test Get
	getResult, err := client.Get(ctx, key)
	if err != nil {
		t.Fatalf("Get failed: %v", err)
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

	// Test Delete
	err = client.Delete(ctx, key)
	if err != nil {
		t.Fatalf("Delete failed: %v", err)
	}

	// Verify deletion
	getResult, err = client.Get(ctx, key)
	if err != nil {
		t.Fatalf("Get after delete failed: %v", err)
	}

	if getResult.Found {
		t.Error("Expected key to be deleted")
	}
}

func TestClient_GetNonExistent(t *testing.T) {
	_, listener, serverCleanup := setupTestServer(t)
	defer serverCleanup()

	client, clientCleanup := setupTestClient(t, listener)
	defer clientCleanup()

	ctx := context.Background()

	result, err := client.Get(ctx, "non-existent")
	if err != nil {
		t.Fatalf("Get non-existent failed: %v", err)
	}

	if result.Found {
		t.Error("Expected key not to be found")
	}
}

func TestClient_ConditionalUpdate(t *testing.T) {
	_, listener, serverCleanup := setupTestServer(t)
	defer serverCleanup()

	client, clientCleanup := setupTestClient(t, listener)
	defer clientCleanup()

	ctx := context.Background()
	key := "conditional-key"
	value1 := []byte("value1")
	value2 := []byte("value2")

	// Put initial value
	putResult, err := client.Put(ctx, key, value1)
	if err != nil {
		t.Fatalf("Initial put failed: %v", err)
	}

	// Update with correct version
	_, err = client.Put(ctx, key, value2, WithExpectedVersion(putResult.Version))
	if err != nil {
		t.Fatalf("Conditional update failed: %v", err)
	}

	// Try to update with wrong version
	_, err = client.Put(ctx, key, []byte("value3"), WithExpectedVersion(putResult.Version))
	if err == nil {
		t.Error("Expected conditional update with wrong version to fail")
	}
}

func TestClient_ConditionalDelete(t *testing.T) {
	_, listener, serverCleanup := setupTestServer(t)
	defer serverCleanup()

	client, clientCleanup := setupTestClient(t, listener)
	defer clientCleanup()

	ctx := context.Background()
	key := "conditional-delete-key"
	value := []byte("test-value")

	// Put initial value
	putResult, err := client.Put(ctx, key, value)
	if err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	// Delete with correct version
	err = client.Delete(ctx, key, WithExpectedVersionForDelete(putResult.Version))
	if err != nil {
		t.Fatalf("Conditional delete failed: %v", err)
	}

	// Verify deletion
	getResult, err := client.Get(ctx, key)
	if err != nil {
		t.Fatalf("Get after delete failed: %v", err)
	}

	if getResult.Found {
		t.Error("Expected key to be deleted")
	}
}

func TestClient_Scan(t *testing.T) {
	_, listener, serverCleanup := setupTestServer(t)
	defer serverCleanup()

	client, clientCleanup := setupTestClient(t, listener)
	defer clientCleanup()

	ctx := context.Background()

	// Put test data
	testData := map[string]string{
		"key1":     "value1",
		"key2":     "value2",
		"key3":     "value3",
		"prefix_a": "value_a",
		"prefix_b": "value_b",
	}

	for k, v := range testData {
		_, err := client.Put(ctx, k, []byte(v))
		if err != nil {
			t.Fatalf("Put failed for %s: %v", k, err)
		}
	}

	// Test scan all
	result, err := client.Scan(ctx, WithLimit(10))
	if err != nil {
		t.Fatalf("Scan failed: %v", err)
	}

	if len(result.Items) != len(testData) {
		t.Errorf("Expected %d items, got %d", len(testData), len(result.Items))
	}

	// Test scan with prefix
	result, err = client.Scan(ctx,
		WithStartKey("prefix_"),
		WithEndKey("prefix_z"),
		WithLimit(10))
	if err != nil {
		t.Fatalf("Prefix scan failed: %v", err)
	}

	if len(result.Items) != 2 {
		t.Errorf("Expected 2 prefix items, got %d", len(result.Items))
	}

	// Test scan with limit
	result, err = client.Scan(ctx, WithLimit(2))
	if err != nil {
		t.Fatalf("Limited scan failed: %v", err)
	}

	if len(result.Items) != 2 {
		t.Errorf("Expected 2 limited items, got %d", len(result.Items))
	}
}

func TestClient_Batch(t *testing.T) {
	_, listener, serverCleanup := setupTestServer(t)
	defer serverCleanup()

	client, clientCleanup := setupTestClient(t, listener)
	defer clientCleanup()

	ctx := context.Background()

	// Test batch operations
	result, err := client.Batch(ctx,
		Put("batch1", []byte("value1")),
		Put("batch2", []byte("value2")),
		Put("batch3", []byte("value3")),
		Get("batch1"),
		Delete("batch2"),
	)
	if err != nil {
		t.Fatalf("Batch failed: %v", err)
	}

	if len(result.Results) != 5 {
		t.Errorf("Expected 5 results, got %d", len(result.Results))
	}

	// Check Put results
	for i := range 3 {
		if !result.Results[i].Success {
			t.Errorf("Expected Put operation %d to succeed", i)
		}
		if result.Results[i].Version <= 0 {
			t.Errorf("Expected positive version for operation %d", i)
		}
	}

	// Check Get result
	if !result.Results[3].Success {
		t.Error("Expected Get operation to succeed")
	}
	if string(result.Results[3].Value) != "value1" {
		t.Errorf("Expected Get to return 'value1', got %s", string(result.Results[3].Value))
	}

	// Check Delete result
	if !result.Results[4].Success {
		t.Error("Expected Delete operation to succeed")
	}

	// Verify batch2 was deleted
	getResult, err := client.Get(ctx, "batch2")
	if err != nil {
		t.Fatalf("Get after batch delete failed: %v", err)
	}
	if getResult.Found {
		t.Error("Expected batch2 to be deleted")
	}
}

func TestClient_ConsistentRead(t *testing.T) {
	_, listener, serverCleanup := setupTestServer(t)
	defer serverCleanup()

	client, clientCleanup := setupTestClient(t, listener)
	defer clientCleanup()

	ctx := context.Background()
	key := "consistent-key"
	value := []byte("consistent-value")

	// Put value
	_, err := client.Put(ctx, key, value)
	if err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	// Test consistent read
	result, err := client.Get(ctx, key, WithConsistentRead())
	if err != nil {
		t.Fatalf("Consistent read failed: %v", err)
	}

	if !result.Found {
		t.Error("Expected key to be found")
	}

	if string(result.Value) != string(value) {
		t.Errorf("Expected value %s, got %s", string(value), string(result.Value))
	}
}

func TestClient_ConnectionFailure(t *testing.T) {
	// Try to connect to non-existent server with very short timeout
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()

	conn, err := grpc.DialContext(ctx, "localhost:99999", grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithBlock())
	if err == nil {
		conn.Close()
		t.Error("Expected connection to fail")
	}
}

func TestClient_ClosedClient(t *testing.T) {
	_, listener, serverCleanup := setupTestServer(t)
	defer serverCleanup()

	client, _ := setupTestClient(t, listener)

	// Close the client
	err := client.Close()
	if err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	ctx := context.Background()

	// Operations should fail after close
	_, err = client.Get(ctx, "key")
	if err == nil {
		t.Error("Expected Get to fail on closed client")
	}

	_, err = client.Put(ctx, "key", []byte("value"))
	if err == nil {
		t.Error("Expected Put to fail on closed client")
	}

	err = client.Delete(ctx, "key")
	if err == nil {
		t.Error("Expected Delete to fail on closed client")
	}

	_, err = client.Scan(ctx)
	if err == nil {
		t.Error("Expected Scan to fail on closed client")
	}

	_, err = client.Batch(ctx, Get("key"))
	if err == nil {
		t.Error("Expected Batch to fail on closed client")
	}

	// Second close should not fail
	err = client.Close()
	if err != nil {
		t.Errorf("Second close failed: %v", err)
	}
}

func TestClient_Timeout(t *testing.T) {
	_, listener, serverCleanup := setupTestServer(t)
	defer serverCleanup()

	client, clientCleanup := setupTestClient(t, listener)
	defer clientCleanup()

	// Create context with very short timeout
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Nanosecond)
	defer cancel()

	// Operation should timeout
	_, err := client.Get(ctx, "key")
	if err == nil {
		t.Error("Expected operation to timeout")
	}
}

func TestClient_BatchOperationTypes(t *testing.T) {
	// Test batch operation constructors
	getOp := Get("test-key")
	putOp := Put("test-key", []byte("test-value"))
	putWithVersionOp := PutWithVersion("test-key", []byte("test-value"), 123)
	deleteOp := Delete("test-key")
	deleteWithVersionOp := DeleteWithVersion("test-key", 456)

	// Convert to proto and verify
	getProto := getOp.toProto()
	if getProto.Type != atlas.Operation_GET || getProto.Key != "test-key" {
		t.Error("Get operation proto incorrect")
	}

	putProto := putOp.toProto()
	if putProto.Type != atlas.Operation_PUT || putProto.Key != "test-key" || string(putProto.Value) != "test-value" {
		t.Error("Put operation proto incorrect")
	}

	putWithVersionProto := putWithVersionOp.toProto()
	if putWithVersionProto.ExpectedVersion != 123 {
		t.Error("Put with version operation proto incorrect")
	}

	deleteProto := deleteOp.toProto()
	if deleteProto.Type != atlas.Operation_DELETE || deleteProto.Key != "test-key" {
		t.Error("Delete operation proto incorrect")
	}

	deleteWithVersionProto := deleteWithVersionOp.toProto()
	if deleteWithVersionProto.ExpectedVersion != 456 {
		t.Error("Delete with version operation proto incorrect")
	}
}
