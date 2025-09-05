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
	"fmt"
	"sync"
	"time"
)

// Pool manages multiple key-value store instances
type Pool struct {
	dataStore Store
	metaStore Store
	mutex     sync.RWMutex
	closed    bool
}

var (
	globalPool *Pool
	poolMutex  sync.Mutex
)

// CreatePool initializes the global KV store pool
func CreatePool(dataPath, metaPath string) error {
	poolMutex.Lock()
	defer poolMutex.Unlock()

	// Close existing pool if it exists
	if globalPool != nil {
		if err := globalPool.Close(); err != nil {
			return fmt.Errorf("failed to close existing pool: %w", err)
		}
	}

	// Create new pool
	var err error
	globalPool, err = NewPool(dataPath, metaPath)
	if err != nil {
		globalPool = nil
		return err
	}

	return nil
}

// GetPool returns the global pool instance
func GetPool() *Pool {
	poolMutex.Lock()
	defer poolMutex.Unlock()
	return globalPool
}

// NewPool creates a new pool with data and metadata stores
func NewPool(dataPath, metaPath string) (*Pool, error) {
	// Create data store (main user data)
	dataStore, err := NewBadgerStore(dataPath)
	if err != nil {
		return nil, fmt.Errorf("failed to create data store: %w", err)
	}

	// Create metadata store (consensus, ownership, migrations)
	metaStore, err := NewBadgerStore(metaPath)
	if err != nil {
		_ = dataStore.Close()
		return nil, fmt.Errorf("failed to create meta store: %w", err)
	}

	return &Pool{
		dataStore: dataStore,
		metaStore: metaStore,
	}, nil
}

// DataStore returns the main data store
func (p *Pool) DataStore() Store {
	p.mutex.RLock()
	defer p.mutex.RUnlock()

	if p.closed {
		return nil
	}
	return p.dataStore
}

// MetaStore returns the metadata store
func (p *Pool) MetaStore() Store {
	p.mutex.RLock()
	defer p.mutex.RUnlock()

	if p.closed {
		return nil
	}
	return p.metaStore
}

// Close closes all stores in the pool
func (p *Pool) Close() error {
	p.mutex.Lock()
	defer p.mutex.Unlock()

	if p.closed {
		return nil
	}

	var errs []error

	if err := p.dataStore.Close(); err != nil {
		errs = append(errs, fmt.Errorf("data store close error: %w", err))
	}

	if err := p.metaStore.Close(); err != nil {
		errs = append(errs, fmt.Errorf("meta store close error: %w", err))
	}

	p.closed = true

	if len(errs) > 0 {
		return fmt.Errorf("pool close errors: %v", errs)
	}

	return nil
}

// Sync synchronizes both stores to disk
func (p *Pool) Sync() error {
	p.mutex.RLock()
	defer p.mutex.RUnlock()

	if p.closed {
		return fmt.Errorf("pool is closed")
	}

	if err := p.dataStore.Sync(); err != nil {
		return fmt.Errorf("data store sync error: %w", err)
	}

	if err := p.metaStore.Sync(); err != nil {
		return fmt.Errorf("meta store sync error: %w", err)
	}

	return nil
}

// Size returns the total size of all stores
func (p *Pool) Size() (dataSize, metaSize int64, err error) {
	p.mutex.RLock()
	defer p.mutex.RUnlock()

	if p.closed {
		return 0, 0, fmt.Errorf("pool is closed")
	}

	dataSize, err = p.dataStore.Size()
	if err != nil {
		return 0, 0, fmt.Errorf("data store size error: %w", err)
	}

	metaSize, err = p.metaStore.Size()
	if err != nil {
		return 0, 0, fmt.Errorf("meta store size error: %w", err)
	}

	return dataSize, metaSize, nil
}

// DrainPool closes the global pool
func DrainPool() error {
	poolMutex.Lock()
	defer poolMutex.Unlock()

	if globalPool != nil {
		err := globalPool.Close()
		globalPool = nil
		return err
	}
	return nil
}

// StoreConnection represents a connection to a specific store
type StoreConnection struct {
	store Store
	txn   Transaction
}

// NewDataConnection creates a connection to the data store
func (p *Pool) NewDataConnection(ctx context.Context, writable bool) (*StoreConnection, error) {
	store := p.DataStore()
	if store == nil {
		return nil, fmt.Errorf("pool is closed")
	}

	txn, err := store.Begin(writable)
	if err != nil {
		return nil, fmt.Errorf("failed to begin transaction: %w", err)
	}

	return &StoreConnection{
		store: store,
		txn:   txn,
	}, nil
}

// NewMetaConnection creates a connection to the metadata store
func (p *Pool) NewMetaConnection(ctx context.Context, writable bool) (*StoreConnection, error) {
	store := p.MetaStore()
	if store == nil {
		return nil, fmt.Errorf("pool is closed")
	}

	txn, err := store.Begin(writable)
	if err != nil {
		return nil, fmt.Errorf("failed to begin transaction: %w", err)
	}

	return &StoreConnection{
		store: store,
		txn:   txn,
	}, nil
}

// Transaction returns the active transaction
func (c *StoreConnection) Transaction() Transaction {
	return c.txn
}

// Store returns the underlying store
func (c *StoreConnection) Store() Store {
	return c.store
}

// Commit commits the transaction
func (c *StoreConnection) Commit() error {
	return c.txn.Commit()
}

// Rollback discards the transaction
func (c *StoreConnection) Rollback() {
	c.txn.Discard()
}

// Close discards the transaction (alias for Rollback)
func (c *StoreConnection) Close() error {
	c.txn.Discard()
	return nil
}

// BackgroundMaintenance runs periodic maintenance tasks
func (p *Pool) BackgroundMaintenance(ctx context.Context) {
	ticker := time.NewTicker(5 * time.Minute)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			// Force garbage collection in BadgerDB
			if badgerData, ok := p.dataStore.(*BadgerStore); ok {
				// BadgerDB's RunValueLogGC can be called here if needed
				_ = badgerData // Placeholder for GC calls
			}

			if badgerMeta, ok := p.metaStore.(*BadgerStore); ok {
				// BadgerDB's RunValueLogGC can be called here if needed
				_ = badgerMeta // Placeholder for GC calls
			}
		}
	}
}
