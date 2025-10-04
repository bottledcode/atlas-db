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
	"io"
)

// Store defines the key-value storage interface for Atlas-DB
type Store interface {
	// Basic operations
	Get(ctx context.Context, key []byte) ([]byte, error)
	Put(ctx context.Context, key, value []byte) error
	Delete(ctx context.Context, key []byte) error
	PrefixScan(ctx context.Context, prefix []byte) ([][]byte, error)

	// Batch operations for atomic writes
	NewBatch() Batch

	// Iteration support for range queries
	NewIterator(opts IteratorOptions) Iterator

	// Transaction support
	Begin(writable bool) (Transaction, error)

	// Lifecycle
	Close() error

	// Statistics and maintenance
	Size() (int64, error)
	Sync() error
}

// Transaction provides ACID transaction support
type Transaction interface {
	Store
	Commit() error
	Discard()
}

// Batch provides atomic batch operations
type Batch interface {
	Set(key, value []byte) error
	Delete(key []byte) error
	Flush() error
	Reset()
}

// Iterator provides ordered key-value iteration
type Iterator interface {
	io.Closer

	// Navigation
	Rewind()
	Seek(key []byte)
	Next()
	Valid() bool

	// Data access
	Item() Item
}

// Item represents a key-value pair during iteration
type Item interface {
	Key() []byte
	KeyCopy() []byte
	Value() ([]byte, error)
	ValueCopy() ([]byte, error)
}

// IteratorOptions configures iteration behavior
type IteratorOptions struct {
	Prefix         []byte
	Reverse        bool
	PrefetchValues bool
	PrefetchSize   int
}

// ErrKeyNotFound is returned when a key doesn't exist
var ErrKeyNotFound = &KeyNotFoundError{}

// ErrNestedTransaction is returned when attempting to create nested transactions
var ErrNestedTransaction = &NestedTransactionError{}

type KeyNotFoundError struct{}

func (e *KeyNotFoundError) Error() string {
	return "key not found"
}

type NestedTransactionError struct{}

func (e *NestedTransactionError) Error() string {
	return "nested transactions are not supported"
}
