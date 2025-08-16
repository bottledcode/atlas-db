package storage

import (
	"context"
	"fmt"
	"testing"
)

func setupTestStorage(t *testing.T) (*BadgerStorage, func()) {
	tmpDir := t.TempDir()
	storage, err := NewBadgerStorage(tmpDir)
	if err != nil {
		t.Fatalf("Failed to create test storage: %v", err)
	}

	cleanup := func() {
		if err := storage.Close(); err != nil {
			t.Errorf("Failed to close storage: %v", err)
		}
	}

	return storage, cleanup
}

func TestBadgerStorage_BasicOperations(t *testing.T) {
	storage, cleanup := setupTestStorage(t)
	defer cleanup()

	ctx := context.Background()
	key := "test-key"
	value := []byte("test-value")

	// Test Put
	err := storage.Put(ctx, key, value)
	if err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	// Test Get
	result, err := storage.Get(ctx, key)
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}

	if result == nil {
		t.Fatal("Expected result to be non-nil")
	}

	if result.Key != key {
		t.Errorf("Expected key %s, got %s", key, result.Key)
	}

	if string(result.Value) != string(value) {
		t.Errorf("Expected value %s, got %s", string(value), string(result.Value))
	}

	if result.Version <= 0 {
		t.Errorf("Expected positive version, got %d", result.Version)
	}

	// Test Delete
	err = storage.Delete(ctx, key)
	if err != nil {
		t.Fatalf("Delete failed: %v", err)
	}

	// Verify deletion
	result, err = storage.Get(ctx, key)
	if err != nil {
		t.Fatalf("Get after delete failed: %v", err)
	}

	if result != nil {
		t.Error("Expected key to be deleted")
	}
}

func TestBadgerStorage_GetNonExistent(t *testing.T) {
	storage, cleanup := setupTestStorage(t)
	defer cleanup()

	ctx := context.Background()

	result, err := storage.Get(ctx, "non-existent-key")
	if err != nil {
		t.Fatalf("Get non-existent key failed: %v", err)
	}

	if result != nil {
		t.Error("Expected nil result for non-existent key")
	}
}

func TestBadgerStorage_PutWithVersion(t *testing.T) {
	storage, cleanup := setupTestStorage(t)
	defer cleanup()

	ctx := context.Background()
	key := "version-test-key"
	value1 := []byte("value1")
	value2 := []byte("value2")

	// Put initial value
	err := storage.Put(ctx, key, value1)
	if err != nil {
		t.Fatalf("Initial put failed: %v", err)
	}

	// Get initial version
	result, err := storage.Get(ctx, key)
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}

	initialVersion := result.Version

	// Update with correct version
	err = storage.PutWithVersion(ctx, key, value2, initialVersion)
	if err != nil {
		t.Fatalf("PutWithVersion with correct version failed: %v", err)
	}

	// Try to update with wrong version (should fail)
	err = storage.PutWithVersion(ctx, key, []byte("value3"), initialVersion)
	if err == nil {
		t.Error("Expected PutWithVersion with wrong version to fail")
	}
}

func TestBadgerStorage_DeleteWithVersion(t *testing.T) {
	storage, cleanup := setupTestStorage(t)
	defer cleanup()

	ctx := context.Background()
	key := "delete-version-test"
	value := []byte("test-value")

	// Put initial value
	err := storage.Put(ctx, key, value)
	if err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	// Get version
	result, err := storage.Get(ctx, key)
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}

	version := result.Version

	// Delete with correct version
	err = storage.DeleteWithVersion(ctx, key, version)
	if err != nil {
		t.Fatalf("DeleteWithVersion failed: %v", err)
	}

	// Verify deletion
	result, err = storage.Get(ctx, key)
	if err != nil {
		t.Fatalf("Get after delete failed: %v", err)
	}

	if result != nil {
		t.Error("Expected key to be deleted")
	}
}

func TestBadgerStorage_Scan(t *testing.T) {
	storage, cleanup := setupTestStorage(t)
	defer cleanup()

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
		err := storage.Put(ctx, k, []byte(v))
		if err != nil {
			t.Fatalf("Put failed for %s: %v", k, err)
		}
	}

	// Test scan all
	opts := ScanOptions{
		StartKey: "",
		EndKey:   "",
		Limit:    10,
	}

	results, err := storage.Scan(ctx, opts)
	if err != nil {
		t.Fatalf("Scan failed: %v", err)
	}

	if len(results) != len(testData) {
		t.Errorf("Expected %d results, got %d", len(testData), len(results))
	}

	// Test scan with prefix
	opts = ScanOptions{
		StartKey: "prefix_",
		EndKey:   "prefix_z",
		Limit:    10,
	}

	results, err = storage.Scan(ctx, opts)
	if err != nil {
		t.Fatalf("Prefix scan failed: %v", err)
	}

	if len(results) != 2 {
		t.Errorf("Expected 2 prefix results, got %d", len(results))
	}

	// Test scan with limit
	opts = ScanOptions{
		StartKey: "",
		EndKey:   "",
		Limit:    2,
	}

	results, err = storage.Scan(ctx, opts)
	if err != nil {
		t.Fatalf("Limited scan failed: %v", err)
	}

	if len(results) != 2 {
		t.Errorf("Expected 2 limited results, got %d", len(results))
	}
}

func TestBadgerStorage_Batch(t *testing.T) {
	storage, cleanup := setupTestStorage(t)
	defer cleanup()

	ctx := context.Background()

	// Prepare batch operations
	operations := []BatchOperation{
		{Type: BatchOpPut, Key: "batch1", Value: []byte("value1")},
		{Type: BatchOpPut, Key: "batch2", Value: []byte("value2")},
		{Type: BatchOpPut, Key: "batch3", Value: []byte("value3")},
	}

	// Execute batch
	err := storage.Batch(ctx, operations)
	if err != nil {
		t.Fatalf("Batch operation failed: %v", err)
	}

	// Verify all keys were written
	for _, op := range operations {
		result, err := storage.Get(ctx, op.Key)
		if err != nil {
			t.Fatalf("Get failed for key %s: %v", op.Key, err)
		}

		if result == nil {
			t.Errorf("Expected key %s to exist", op.Key)
			continue
		}

		if string(result.Value) != string(op.Value) {
			t.Errorf("Expected value %s for key %s, got %s",
				string(op.Value), op.Key, string(result.Value))
		}
	}

	// Test batch with delete
	deleteOps := []BatchOperation{
		{Type: BatchOpDelete, Key: "batch1"},
		{Type: BatchOpPut, Key: "batch4", Value: []byte("value4")},
	}

	err = storage.Batch(ctx, deleteOps)
	if err != nil {
		t.Fatalf("Batch delete operation failed: %v", err)
	}

	// Verify deletion
	result, err := storage.Get(ctx, "batch1")
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}

	if result != nil {
		t.Error("Expected batch1 to be deleted")
	}

	// Verify new key
	result, err = storage.Get(ctx, "batch4")
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}

	if result == nil || string(result.Value) != "value4" {
		t.Error("Expected batch4 to be created with value4")
	}
}

func TestBadgerStorage_ConsensusInterface(t *testing.T) {
	storage, cleanup := setupTestStorage(t)
	defer cleanup()

	key := "consensus-key"
	value := []byte("consensus-value")

	// Test Store
	err := storage.Store(key, value)
	if err != nil {
		t.Fatalf("Store failed: %v", err)
	}

	// Test Load
	loadedValue, err := storage.Load(key)
	if err != nil {
		t.Fatalf("Load failed: %v", err)
	}

	if string(loadedValue) != string(value) {
		t.Errorf("Expected loaded value %s, got %s", string(value), string(loadedValue))
	}

	// Test DeleteKey
	err = storage.DeleteKey(key)
	if err != nil {
		t.Fatalf("DeleteKey failed: %v", err)
	}

	// Verify deletion
	_, err = storage.Load(key)
	if err == nil {
		t.Error("Expected Load to fail after DeleteKey")
	}
}

func TestBadgerStorage_RegionAdapter(t *testing.T) {
	storage, cleanup := setupTestStorage(t)
	defer cleanup()

	adapter := NewRegionAdapter(storage)
	ctx := context.Background()

	key := "region-key"
	value := []byte("region-value")

	// Test Put via adapter
	err := adapter.Put(ctx, key, value)
	if err != nil {
		t.Fatalf("Adapter Put failed: %v", err)
	}

	// Test Get via adapter
	result, err := adapter.Get(ctx, key)
	if err != nil {
		t.Fatalf("Adapter Get failed: %v", err)
	}

	if result == nil {
		t.Fatal("Expected result to be non-nil")
	}

	if result.Key != key {
		t.Errorf("Expected key %s, got %s", key, result.Key)
	}

	if string(result.Value) != string(value) {
		t.Errorf("Expected value %s, got %s", string(value), string(result.Value))
	}

	// Test Delete via adapter
	err = adapter.Delete(ctx, key)
	if err != nil {
		t.Fatalf("Adapter Delete failed: %v", err)
	}

	// Verify deletion
	result, err = adapter.Get(ctx, key)
	if err != nil {
		t.Fatalf("Adapter Get after delete failed: %v", err)
	}

	if result != nil {
		t.Error("Expected key to be deleted")
	}
}

func TestBadgerStorage_Stats(t *testing.T) {
	storage, cleanup := setupTestStorage(t)
	defer cleanup()

	ctx := context.Background()

	// Add some data
	for i := range 10 {
		key := fmt.Sprintf("stats-key-%d", i)
		value := fmt.Appendf(nil, "stats-value-%d", i)
		err := storage.Put(ctx, key, value)
		if err != nil {
			t.Fatalf("Put failed: %v", err)
		}
	}

	// Get stats
	stats, err := storage.Stats()
	if err != nil {
		t.Fatalf("Stats failed: %v", err)
	}

	if stats.TotalSize < 0 {
		t.Error("Expected non-negative total size")
	}

	if stats.TotalSize != stats.LSMSize+stats.VLogSize {
		t.Error("Total size should equal LSM + VLog size")
	}
}

func TestBadgerStorage_GC(t *testing.T) {
	storage, cleanup := setupTestStorage(t)
	defer cleanup()

	// GC may not find anything to clean up with empty database
	// This is normal behavior, not an error
	err := storage.GC()
	if err != nil && err.Error() != "Value log GC attempt didn't result in any cleanup" {
		t.Fatalf("Unexpected GC error: %v", err)
	}
}

func TestBadgerStorage_ClosedOperations(t *testing.T) {
	tmpDir := t.TempDir()
	storage, err := NewBadgerStorage(tmpDir)
	if err != nil {
		t.Fatalf("Failed to create storage: %v", err)
	}

	// Close the storage
	err = storage.Close()
	if err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	ctx := context.Background()

	// Operations should fail after close
	err = storage.Put(ctx, "key", []byte("value"))
	if err == nil {
		t.Error("Expected Put to fail on closed storage")
	}

	_, err = storage.Get(ctx, "key")
	if err == nil {
		t.Error("Expected Get to fail on closed storage")
	}

	err = storage.Delete(ctx, "key")
	if err == nil {
		t.Error("Expected Delete to fail on closed storage")
	}

	// Second close should not fail
	err = storage.Close()
	if err != nil {
		t.Errorf("Second close failed: %v", err)
	}
}

func TestBadgerStorage_ConcurrentAccess(t *testing.T) {
	storage, cleanup := setupTestStorage(t)
	defer cleanup()

	ctx := context.Background()
	numGoroutines := 10
	numOperations := 100

	// Test concurrent writes
	errChan := make(chan error, numGoroutines)

	for i := range numGoroutines {
		go func(goroutineID int) {
			for j := range numOperations {
				key := fmt.Sprintf("concurrent-%d-%d", goroutineID, j)
				value := fmt.Appendf(nil, "value-%d-%d", goroutineID, j)

				if err := storage.Put(ctx, key, value); err != nil {
					errChan <- err
					return
				}
			}
			errChan <- nil
		}(i)
	}

	// Wait for all goroutines
	for range numGoroutines {
		if err := <-errChan; err != nil {
			t.Fatalf("Concurrent write failed: %v", err)
		}
	}

	// Verify all data was written
	expectedKeys := numGoroutines * numOperations
	opts := ScanOptions{
		StartKey: "concurrent-",
		EndKey:   "concurrent-z",
		Limit:    expectedKeys + 10,
	}

	results, err := storage.Scan(ctx, opts)
	if err != nil {
		t.Fatalf("Scan failed: %v", err)
	}

	if len(results) != expectedKeys {
		t.Errorf("Expected %d keys, got %d", expectedKeys, len(results))
	}
}
