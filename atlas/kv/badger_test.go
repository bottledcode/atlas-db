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

package kv

import (
	"context"
	"fmt"
	"os"
	"testing"
)

func TestBadgerTransaction_Begin_ReturnsNestedTransactionError(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "badger_test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer func() {
		if err := os.RemoveAll(tmpDir); err != nil {
			t.Logf("Failed to remove temp dir: %v", err)
		}
	}()

	store, err := NewBadgerStore(tmpDir)
	if err != nil {
		t.Fatalf("Failed to create BadgerStore: %v", err)
	}
	defer func() {
		if err := store.Close(); err != nil {
			t.Logf("Failed to close store: %v", err)
		}
	}()

	// Create initial transaction
	txn, err := store.Begin(false)
	if err != nil {
		t.Fatalf("Failed to create transaction: %v", err)
	}
	defer txn.Discard()

	// Attempt to create nested transaction - should return ErrNestedTransaction
	nestedTxn, err := txn.Begin(false)
	if err != ErrNestedTransaction {
		t.Errorf("Expected ErrNestedTransaction, got: %v", err)
	}
	if nestedTxn != nil {
		t.Errorf("Expected nil transaction, got: %v", nestedTxn)
	}

	// Try with writable nested transaction - should also return ErrNestedTransaction
	nestedWritableTxn, err := txn.Begin(true)
	if err != ErrNestedTransaction {
		t.Errorf("Expected ErrNestedTransaction for writable nested transaction, got: %v", err)
	}
	if nestedWritableTxn != nil {
		t.Errorf("Expected nil writable transaction, got: %v", nestedWritableTxn)
	}
}

func TestBadgerTransaction_Begin_WritableTransactionAlsoReturnsError(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "badger_test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer func() {
		if err := os.RemoveAll(tmpDir); err != nil {
			t.Logf("Failed to remove temp dir: %v", err)
		}
	}()

	store, err := NewBadgerStore(tmpDir)
	if err != nil {
		t.Fatalf("Failed to create BadgerStore: %v", err)
	}
	defer func() {
		if err := store.Close(); err != nil {
			t.Logf("Failed to close store: %v", err)
		}
	}()

	// Create initial writable transaction
	txn, err := store.Begin(true)
	if err != nil {
		t.Fatalf("Failed to create writable transaction: %v", err)
	}
	defer txn.Discard()

	// Attempt to create nested transaction from writable transaction
	nestedTxn, err := txn.Begin(false)
	if err != ErrNestedTransaction {
		t.Errorf("Expected ErrNestedTransaction from writable transaction, got: %v", err)
	}
	if nestedTxn != nil {
		t.Errorf("Expected nil transaction from writable transaction, got: %v", nestedTxn)
	}
}

func TestBadgerBatch_SliceSafety(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "badger_test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer func() {
		if err := os.RemoveAll(tmpDir); err != nil {
			t.Logf("Failed to remove temp dir: %v", err)
		}
	}()

	store, err := NewBadgerStore(tmpDir)
	if err != nil {
		t.Fatalf("Failed to create BadgerStore: %v", err)
	}
	defer func() {
		if err := store.Close(); err != nil {
			t.Logf("Failed to close store: %v", err)
		}
	}()

	batch := store.NewBatch()

	// Test that batch copies slices and mutations don't affect stored values
	key := []byte("test-key")
	value := []byte("test-value")

	err = batch.Set(key, value)
	if err != nil {
		t.Fatalf("Failed to set value in batch: %v", err)
	}

	// Mutate original slices
	key[0] = 'X'
	value[0] = 'X'

	err = batch.Flush()
	if err != nil {
		t.Fatalf("Failed to flush batch: %v", err)
	}

	// Verify original values were stored, not mutated ones
	ctx := context.Background()
	storedValue, err := store.Get(ctx, []byte("test-key"))
	if err != nil {
		t.Fatalf("Failed to get value: %v", err)
	}

	expectedValue := "test-value"
	if string(storedValue) != expectedValue {
		t.Errorf("Expected %q, got %q", expectedValue, string(storedValue))
	}
}

func TestBadgerBatch_ExplicitOperations(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "badger_test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer func() {
		if err := os.RemoveAll(tmpDir); err != nil {
			t.Logf("Failed to remove temp dir: %v", err)
		}
	}()

	store, err := NewBadgerStore(tmpDir)
	if err != nil {
		t.Fatalf("Failed to create BadgerStore: %v", err)
	}
	defer func() {
		if err := store.Close(); err != nil {
			t.Logf("Failed to close store: %v", err)
		}
	}()

	ctx := context.Background()

	// First, set a value directly to test deletion
	err = store.Put(ctx, []byte("delete-me"), []byte("initial-value"))
	if err != nil {
		t.Fatalf("Failed to put initial value: %v", err)
	}

	batch := store.NewBatch()

	// Test explicit set operation
	err = batch.Set([]byte("set-key"), []byte("set-value"))
	if err != nil {
		t.Fatalf("Failed to set in batch: %v", err)
	}

	// Test explicit delete operation
	err = batch.Delete([]byte("delete-me"))
	if err != nil {
		t.Fatalf("Failed to delete in batch: %v", err)
	}

	err = batch.Flush()
	if err != nil {
		t.Fatalf("Failed to flush batch: %v", err)
	}

	// Verify set operation worked
	setValue, err := store.Get(ctx, []byte("set-key"))
	if err != nil {
		t.Fatalf("Failed to get set value: %v", err)
	}
	if string(setValue) != "set-value" {
		t.Errorf("Expected 'set-value', got %q", string(setValue))
	}

	// Verify delete operation worked
	_, err = store.Get(ctx, []byte("delete-me"))
	if err != ErrKeyNotFound {
		t.Errorf("Expected ErrKeyNotFound for deleted key, got: %v", err)
	}
}

func TestBadgerBatch_LargeBatch(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "badger_test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer func() {
		if err := os.RemoveAll(tmpDir); err != nil {
			t.Logf("Failed to remove temp dir: %v", err)
		}
	}()

	store, err := NewBadgerStore(tmpDir)
	if err != nil {
		t.Fatalf("Failed to create BadgerStore: %v", err)
	}
	defer func() {
		if err := store.Close(); err != nil {
			t.Logf("Failed to close store: %v", err)
		}
	}()

	batch := store.NewBatch()
	ctx := context.Background()

	// Create a large batch that would exceed single transaction limits
	const numOperations = 2500
	for i := 0; i < numOperations; i++ {
		key := []byte(fmt.Sprintf("key-%d", i))
		value := []byte(fmt.Sprintf("value-%d", i))

		err = batch.Set(key, value)
		if err != nil {
			t.Fatalf("Failed to set value %d in batch: %v", i, err)
		}
	}

	// This should not fail with ErrTxnTooBig due to chunking
	err = batch.Flush()
	if err != nil {
		t.Fatalf("Failed to flush large batch: %v", err)
	}

	// Verify a few random values were written correctly
	testIndices := []int{0, numOperations / 2, numOperations - 1}
	for _, i := range testIndices {
		key := []byte(fmt.Sprintf("key-%d", i))
		expectedValue := fmt.Sprintf("value-%d", i)

		value, err := store.Get(ctx, key)
		if err != nil {
			t.Fatalf("Failed to get key-%d: %v", i, err)
		}
		if string(value) != expectedValue {
			t.Errorf("For key-%d: expected %q, got %q", i, expectedValue, string(value))
		}
	}
}

func TestBadgerBatch_Reset(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "badger_test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer func() {
		if err := os.RemoveAll(tmpDir); err != nil {
			t.Logf("Failed to remove temp dir: %v", err)
		}
	}()

	store, err := NewBadgerStore(tmpDir)
	if err != nil {
		t.Fatalf("Failed to create BadgerStore: %v", err)
	}
	defer func() {
		if err := store.Close(); err != nil {
			t.Logf("Failed to close store: %v", err)
		}
	}()

	batch := store.NewBatch()
	ctx := context.Background()

	// Add some operations
	err = batch.Set([]byte("key1"), []byte("value1"))
	if err != nil {
		t.Fatalf("Failed to set key1: %v", err)
	}
	err = batch.Set([]byte("key2"), []byte("value2"))
	if err != nil {
		t.Fatalf("Failed to set key2: %v", err)
	}

	// Reset the batch
	batch.Reset()

	// Add different operations after reset
	err = batch.Set([]byte("key3"), []byte("value3"))
	if err != nil {
		t.Fatalf("Failed to set key3 after reset: %v", err)
	}

	// Flush should only write key3, not key1 or key2
	err = batch.Flush()
	if err != nil {
		t.Fatalf("Failed to flush batch after reset: %v", err)
	}

	// Verify key3 exists
	value3, err := store.Get(ctx, []byte("key3"))
	if err != nil {
		t.Fatalf("Failed to get key3: %v", err)
	}
	if string(value3) != "value3" {
		t.Errorf("Expected 'value3', got %q", string(value3))
	}

	// Verify key1 and key2 don't exist
	_, err = store.Get(ctx, []byte("key1"))
	if err != ErrKeyNotFound {
		t.Errorf("Expected ErrKeyNotFound for key1, got: %v", err)
	}

	_, err = store.Get(ctx, []byte("key2"))
	if err != ErrKeyNotFound {
		t.Errorf("Expected ErrKeyNotFound for key2, got: %v", err)
	}
}

func TestBadgerBatch_EmptyFlush(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "badger_test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer func() {
		if err := os.RemoveAll(tmpDir); err != nil {
			t.Logf("Failed to remove temp dir: %v", err)
		}
	}()

	store, err := NewBadgerStore(tmpDir)
	if err != nil {
		t.Fatalf("Failed to create BadgerStore: %v", err)
	}
	defer func() {
		if err := store.Close(); err != nil {
			t.Logf("Failed to close store: %v", err)
		}
	}()

	batch := store.NewBatch()

	// Flush empty batch should not error
	err = batch.Flush()
	if err != nil {
		t.Errorf("Expected no error for empty batch flush, got: %v", err)
	}
}
