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

package kv

import (
	"context"

	"github.com/dgraph-io/badger/v2"
)

// BadgerStore implements Store interface using BadgerDB
type BadgerStore struct {
	db *badger.DB
}

// NewBadgerStore creates a new BadgerDB-backed store
func NewBadgerStore(path string) (*BadgerStore, error) {
	opts := badger.DefaultOptions(path)

	// Optimize for Atlas-DB use case
	opts.Logger = nil      // Disable BadgerDB logging to avoid conflicts with zap
	opts.SyncWrites = true // Ensure durability for consensus
	opts.CompactL0OnClose = true

	// Memory optimization for distributed edge deployment
	opts.ValueThreshold = 1024 // Store values < 1KB in LSM tree
	opts.NumMemtables = 2
	opts.NumLevelZeroTables = 2
	opts.NumLevelZeroTablesStall = 4

	db, err := badger.Open(opts)
	if err != nil {
		return nil, err
	}

	return &BadgerStore{db: db}, nil
}

func (s *BadgerStore) Get(ctx context.Context, key []byte) ([]byte, error) {
	var value []byte
	err := s.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(key)
		if err != nil {
			if err == badger.ErrKeyNotFound {
				return ErrKeyNotFound
			}
			return err
		}

		value, err = item.ValueCopy(nil)
		return err
	})

	return value, err
}

func (s *BadgerStore) Put(ctx context.Context, key, value []byte) error {
	return s.db.Update(func(txn *badger.Txn) error {
		return txn.Set(key, value)
	})
}

func (s *BadgerStore) Delete(ctx context.Context, key []byte) error {
	return s.db.Update(func(txn *badger.Txn) error {
		return txn.Delete(key)
	})
}

func (s *BadgerStore) NewBatch() Batch {
	return &BadgerBatch{
		db:         s.db,
		operations: make([]batchOperation, 0),
	}
}

func (s *BadgerStore) NewIterator(opts IteratorOptions) Iterator {
	badgerOpts := badger.DefaultIteratorOptions
	badgerOpts.Reverse = opts.Reverse
	badgerOpts.PrefetchValues = opts.PrefetchValues
	badgerOpts.PrefetchSize = opts.PrefetchSize
	badgerOpts.Prefix = opts.Prefix

	txn := s.db.NewTransaction(false)
	iter := txn.NewIterator(badgerOpts)

	return &BadgerIterator{
		iter: iter,
		txn:  txn,
	}
}

func (s *BadgerStore) Begin(writable bool) (Transaction, error) {
	txn := s.db.NewTransaction(writable)
	return &BadgerTransaction{
		txn:      txn,
		writable: writable,
		store:    s,
	}, nil
}

func (s *BadgerStore) Close() error {
	return s.db.Close()
}

func (s *BadgerStore) Size() (int64, error) {
	lsm, vlog := s.db.Size()
	return lsm + vlog, nil
}

func (s *BadgerStore) Sync() error {
	return s.db.Sync()
}

// BadgerTransaction wraps BadgerDB transaction
type BadgerTransaction struct {
	txn      *badger.Txn
	writable bool
	store    *BadgerStore
}

func (t *BadgerTransaction) Get(ctx context.Context, key []byte) ([]byte, error) {
	item, err := t.txn.Get(key)
	if err != nil {
		if err == badger.ErrKeyNotFound {
			return nil, ErrKeyNotFound
		}
		return nil, err
	}

	return item.ValueCopy(nil)
}

func (t *BadgerTransaction) Put(ctx context.Context, key, value []byte) error {
	if !t.writable {
		return badger.ErrReadOnlyTxn
	}
	return t.txn.Set(key, value)
}

func (t *BadgerTransaction) Delete(ctx context.Context, key []byte) error {
	if !t.writable {
		return badger.ErrReadOnlyTxn
	}
	return t.txn.Delete(key)
}

func (t *BadgerTransaction) NewBatch() Batch {
	return &BadgerTxnBatch{txn: t.txn}
}

func (t *BadgerTransaction) NewIterator(opts IteratorOptions) Iterator {
	badgerOpts := badger.DefaultIteratorOptions
	badgerOpts.Reverse = opts.Reverse
	badgerOpts.PrefetchValues = opts.PrefetchValues
	badgerOpts.PrefetchSize = opts.PrefetchSize
	badgerOpts.Prefix = opts.Prefix

	iter := t.txn.NewIterator(badgerOpts)
	return &BadgerIterator{iter: iter}
}

func (t *BadgerTransaction) Begin(writable bool) (Transaction, error) {
	return nil, ErrNestedTransaction
}

func (t *BadgerTransaction) Close() error {
	t.txn.Discard()
	return nil
}

func (t *BadgerTransaction) Size() (int64, error) {
	return t.store.Size()
}

func (t *BadgerTransaction) Sync() error {
	return nil // Sync happens on commit
}

func (t *BadgerTransaction) Commit() error {
	return t.txn.Commit()
}

func (t *BadgerTransaction) Discard() {
	t.txn.Discard()
}

// batchOperation represents a single operation in a batch
type batchOperation struct {
	opType opType
	key    []byte
	value  []byte // nil only for delete operations
}

type opType int

const (
	opSet opType = iota
	opDelete
)

// BadgerBatch implements atomic batch operations
type BadgerBatch struct {
	db         *badger.DB
	operations []batchOperation
}

func (b *BadgerBatch) Set(key, value []byte) error {
	// Copy caller slices to prevent mutations
	keyCopy := make([]byte, len(key))
	copy(keyCopy, key)
	valueCopy := make([]byte, len(value))
	copy(valueCopy, value)

	b.operations = append(b.operations, batchOperation{
		opType: opSet,
		key:    keyCopy,
		value:  valueCopy,
	})
	return nil
}

func (b *BadgerBatch) Delete(key []byte) error {
	// Copy caller slice to prevent mutations
	keyCopy := make([]byte, len(key))
	copy(keyCopy, key)

	b.operations = append(b.operations, batchOperation{
		opType: opDelete,
		key:    keyCopy,
		value:  nil, // Explicit nil for delete operations
	})
	return nil
}

func (b *BadgerBatch) Flush() error {
	if len(b.operations) == 0 {
		return nil
	}

	// Use WriteBatch for better performance and to avoid ErrTxnTooBig
	// Split operations into chunks to prevent hitting transaction size limits
	const maxOpsPerBatch = 1000 // Conservative limit to avoid ErrTxnTooBig

	for i := 0; i < len(b.operations); i += maxOpsPerBatch {
		end := min(i+maxOpsPerBatch, len(b.operations))

		wb := b.db.NewWriteBatch()
		defer wb.Cancel() // Ensure cleanup even if error occurs

		// Apply operations to this batch chunk
		for j := i; j < end; j++ {
			op := b.operations[j]
			switch op.opType {
			case opSet:
				if err := wb.Set(op.key, op.value); err != nil {
					return err
				}
			case opDelete:
				if err := wb.Delete(op.key); err != nil {
					return err
				}
			}
		}

		// Flush this chunk
		if err := wb.Flush(); err != nil {
			return err
		}
	}

	return nil
}

func (b *BadgerBatch) Reset() {
	b.operations = b.operations[:0]
}

// BadgerTxnBatch implements batch operations within a transaction
type BadgerTxnBatch struct {
	txn *badger.Txn
}

func (b *BadgerTxnBatch) Set(key, value []byte) error {
	return b.txn.Set(key, value)
}

func (b *BadgerTxnBatch) Delete(key []byte) error {
	return b.txn.Delete(key)
}

func (b *BadgerTxnBatch) Flush() error {
	return nil // Operations already applied to transaction
}

func (b *BadgerTxnBatch) Reset() {
	// Cannot reset a transaction batch
}

// BadgerIterator wraps BadgerDB iterator
type BadgerIterator struct {
	iter *badger.Iterator
	txn  *badger.Txn // Only set for standalone iterators
}

func (i *BadgerIterator) Close() error {
	i.iter.Close()
	if i.txn != nil {
		i.txn.Discard()
	}
	return nil
}

func (i *BadgerIterator) Rewind() {
	i.iter.Rewind()
}

func (i *BadgerIterator) Seek(key []byte) {
	i.iter.Seek(key)
}

func (i *BadgerIterator) Next() {
	i.iter.Next()
}

func (i *BadgerIterator) Valid() bool {
	return i.iter.Valid()
}

func (i *BadgerIterator) Item() Item {
	return &BadgerItem{item: i.iter.Item()}
}

// BadgerItem wraps BadgerDB item
type BadgerItem struct {
	item *badger.Item
}

func (i *BadgerItem) Key() []byte {
	return i.item.Key()
}

func (i *BadgerItem) KeyCopy() []byte {
	return i.item.KeyCopy(nil)
}

func (i *BadgerItem) Value() ([]byte, error) {
	return i.item.ValueCopy(nil)
}

func (i *BadgerItem) ValueCopy() ([]byte, error) {
	return i.item.ValueCopy(nil)
}
