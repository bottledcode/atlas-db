package storage

import (
	"context"
	"fmt"
	"path/filepath"
	"sync"

	"github.com/dgraph-io/badger/v4"
)

type BadgerStorage struct {
	db       *badger.DB
	path     string
	mu       sync.RWMutex
	closed   bool
}

type ScanOptions struct {
	StartKey string
	EndKey   string
	Limit    int
	Reverse  bool
}

type KVPair struct {
	Key     string
	Value   []byte
	Version int64
}

func NewBadgerStorage(dataDir string) (*BadgerStorage, error) {
	dbPath := filepath.Join(dataDir, "badger")
	
	opts := badger.DefaultOptions(dbPath).
		WithLogger(nil). // Disable badger logging
		WithSyncWrites(true).
		WithNumVersionsToKeep(10)

	db, err := badger.Open(opts)
	if err != nil {
		return nil, fmt.Errorf("failed to open badger db: %w", err)
	}

	return &BadgerStorage{
		db:   db,
		path: dbPath,
	}, nil
}

func (bs *BadgerStorage) Get(ctx context.Context, key string) (*KVPair, error) {
	bs.mu.RLock()
	defer bs.mu.RUnlock()

	if bs.closed {
		return nil, fmt.Errorf("storage is closed")
	}

	var result *KVPair
	err := bs.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get([]byte(key))
		if err != nil {
			if err == badger.ErrKeyNotFound {
				return nil // result will be nil
			}
			return err
		}

		value, err := item.ValueCopy(nil)
		if err != nil {
			return err
		}

		result = &KVPair{
			Key:     key,
			Value:   value,
			Version: int64(item.Version()),
		}
		return nil
	})

	return result, err
}

func (bs *BadgerStorage) Put(ctx context.Context, key string, value []byte) error {
	bs.mu.Lock()
	defer bs.mu.Unlock()

	if bs.closed {
		return fmt.Errorf("storage is closed")
	}

	return bs.db.Update(func(txn *badger.Txn) error {
		entry := badger.NewEntry([]byte(key), value).WithMeta(0)
		return txn.SetEntry(entry)
	})
}

func (bs *BadgerStorage) PutWithVersion(ctx context.Context, key string, value []byte, expectedVersion int64) error {
	bs.mu.Lock()
	defer bs.mu.Unlock()

	if bs.closed {
		return fmt.Errorf("storage is closed")
	}

	return bs.db.Update(func(txn *badger.Txn) error {
		// Check current version if expectedVersion is specified
		if expectedVersion > 0 {
			item, err := txn.Get([]byte(key))
			if err != nil && err != badger.ErrKeyNotFound {
				return err
			}
			
			if err == nil && int64(item.Version()) != expectedVersion {
				return fmt.Errorf("version mismatch: expected %d, got %d", expectedVersion, item.Version())
			}
		}

		entry := badger.NewEntry([]byte(key), value).WithMeta(0)
		return txn.SetEntry(entry)
	})
}

func (bs *BadgerStorage) Delete(ctx context.Context, key string) error {
	bs.mu.Lock()
	defer bs.mu.Unlock()

	if bs.closed {
		return fmt.Errorf("storage is closed")
	}

	return bs.db.Update(func(txn *badger.Txn) error {
		return txn.Delete([]byte(key))
	})
}

func (bs *BadgerStorage) DeleteWithVersion(ctx context.Context, key string, expectedVersion int64) error {
	bs.mu.Lock()
	defer bs.mu.Unlock()

	if bs.closed {
		return fmt.Errorf("storage is closed")
	}

	return bs.db.Update(func(txn *badger.Txn) error {
		// Check current version if expectedVersion is specified
		if expectedVersion > 0 {
			item, err := txn.Get([]byte(key))
			if err != nil {
				if err == badger.ErrKeyNotFound {
					return fmt.Errorf("key not found")
				}
				return err
			}
			
			if int64(item.Version()) != expectedVersion {
				return fmt.Errorf("version mismatch: expected %d, got %d", expectedVersion, item.Version())
			}
		}

		return txn.Delete([]byte(key))
	})
}

func (bs *BadgerStorage) Scan(ctx context.Context, opts ScanOptions) ([]*KVPair, error) {
	bs.mu.RLock()
	defer bs.mu.RUnlock()

	if bs.closed {
		return nil, fmt.Errorf("storage is closed")
	}

	var results []*KVPair
	
	err := bs.db.View(func(txn *badger.Txn) error {
		iterOpts := badger.DefaultIteratorOptions
		iterOpts.Reverse = opts.Reverse
		iter := txn.NewIterator(iterOpts)
		defer iter.Close()

		startKey := []byte(opts.StartKey)
		endKey := []byte(opts.EndKey)
		
		count := 0
		for iter.Seek(startKey); iter.Valid(); iter.Next() {
			if opts.Limit > 0 && count >= opts.Limit {
				break
			}

			item := iter.Item()
			key := item.Key()
			
			// Check if we've passed the end key
			if len(endKey) > 0 {
				if opts.Reverse {
					if string(key) < opts.EndKey {
						break
					}
				} else {
					if string(key) >= opts.EndKey {
						break
					}
				}
			}

			value, err := item.ValueCopy(nil)
			if err != nil {
				return err
			}

			results = append(results, &KVPair{
				Key:     string(key),
				Value:   value,
				Version: int64(item.Version()),
			})
			count++
		}
		return nil
	})

	return results, err
}

func (bs *BadgerStorage) Batch(ctx context.Context, operations []BatchOperation) error {
	bs.mu.Lock()
	defer bs.mu.Unlock()

	if bs.closed {
		return fmt.Errorf("storage is closed")
	}

	return bs.db.Update(func(txn *badger.Txn) error {
		for _, op := range operations {
			switch op.Type {
			case BatchOpPut:
				entry := badger.NewEntry([]byte(op.Key), op.Value).WithMeta(0)
				if err := txn.SetEntry(entry); err != nil {
					return err
				}
			case BatchOpDelete:
				if err := txn.Delete([]byte(op.Key)); err != nil {
					return err
				}
			default:
				return fmt.Errorf("unsupported batch operation: %v", op.Type)
			}
		}
		return nil
	})
}

func (bs *BadgerStorage) Close() error {
	bs.mu.Lock()
	defer bs.mu.Unlock()

	if bs.closed {
		return nil
	}

	bs.closed = true
	return bs.db.Close()
}

func (bs *BadgerStorage) GC() error {
	bs.mu.Lock()
	defer bs.mu.Unlock()

	if bs.closed {
		return fmt.Errorf("storage is closed")
	}

	// Run garbage collection
	return bs.db.RunValueLogGC(0.5)
}

func (bs *BadgerStorage) Stats() (*StorageStats, error) {
	bs.mu.RLock()
	defer bs.mu.RUnlock()

	if bs.closed {
		return nil, fmt.Errorf("storage is closed")
	}

	lsm, vlog := bs.db.Size()
	
	return &StorageStats{
		LSMSize:   lsm,
		VLogSize:  vlog,
		TotalSize: lsm + vlog,
	}, nil
}

// Store implements the consensus.Storage interface
func (bs *BadgerStorage) Store(key string, value []byte) error {
	return bs.Put(context.Background(), key, value)
}

// Load implements the consensus.Storage interface
func (bs *BadgerStorage) Load(key string) ([]byte, error) {
	kv, err := bs.Get(context.Background(), key)
	if err != nil {
		return nil, err
	}
	if kv == nil {
		return nil, fmt.Errorf("key not found")
	}
	return kv.Value, nil
}

// DeleteKey implements the consensus.Storage interface for consensus  
func (bs *BadgerStorage) DeleteKey(key string) error {
	return bs.Delete(context.Background(), key)
}

// RegionAdapter adapts BadgerStorage to region.Storage interface
type RegionAdapter struct {
	*BadgerStorage
}

// NewRegionAdapter creates a new region storage adapter
func NewRegionAdapter(bs *BadgerStorage) *RegionAdapter {
	return &RegionAdapter{BadgerStorage: bs}
}

// RegionKVPair represents a key-value pair for region interface
type RegionKVPair struct {
	Key     string
	Value   []byte
	Version int64
}

// Get implements region.Storage interface
func (ra *RegionAdapter) Get(ctx context.Context, key string) (*RegionKVPair, error) {
	kv, err := ra.BadgerStorage.Get(ctx, key)
	if err != nil {
		return nil, err
	}
	if kv == nil {
		return nil, nil
	}
	return &RegionKVPair{
		Key:     kv.Key,
		Value:   kv.Value,
		Version: kv.Version,
	}, nil
}

type BatchOpType int

const (
	BatchOpPut BatchOpType = iota
	BatchOpDelete
)

type BatchOperation struct {
	Type  BatchOpType
	Key   string
	Value []byte
}

type StorageStats struct {
	LSMSize   int64
	VLogSize  int64
	TotalSize int64
}