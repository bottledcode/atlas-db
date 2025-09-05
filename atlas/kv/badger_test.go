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