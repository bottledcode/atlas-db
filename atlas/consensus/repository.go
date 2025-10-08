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

package consensus

import (
	"context"
	"errors"
	"reflect"

	"github.com/bottledcode/atlas-db/atlas/kv"
	"google.golang.org/protobuf/proto"
)

type Key interface {
	Raw() []byte
	Prefix(size int) Prefix
}

type GenericKey struct {
	raw []byte
}

func (k GenericKey) Raw() []byte {
	return k.raw
}

func (k GenericKey) Prefix(size int) Prefix {
	return Prefix{
		raw: k.raw[:size],
	}
}

type Prefix struct {
	raw []byte
}

type Repository[M proto.Message, K Key] interface {
	GetByKey(k K, txn *kv.Transaction) (M, error)
	GetKeys(obj M) *StructuredKey
	CreateKey(b []byte) K
	Put(obj M) error
	Delete(obj M, txn *kv.Transaction) error
	DeleteByKey(k K) error
	PrefixScan(cursor []byte, write bool, prefix Prefix, c func(K, M, *kv.Transaction) error) error
	ScanIndex(prefix Prefix, write bool, callback func(primaryKey []byte, txn *kv.Transaction) error) error
}

type BaseRepository[M proto.Message, K Key] struct {
	store kv.Store
	ctx   context.Context
	repo  Repository[M, K] // Reference to concrete repository for polymorphic method calls
}

func (r *BaseRepository[M, K]) DeleteByKey(k K) error {
	txn, err := r.store.Begin(true)
	if err != nil {
		return err
	}
	defer txn.Discard()
	m, err := r.GetByKey(k, &txn)
	if err != nil {
		return err
	}
	err = r.Delete(m, &txn)
	if err != nil {
		return err
	}
	return txn.Commit()
}

func (r *BaseRepository[M, K]) GetByKey(k K, txn *kv.Transaction) (M, error) {
	var m M
	if txn == nil {
		t, err := r.store.Begin(false)
		if err != nil {
			return m, err
		}
		txn = &t
		defer t.Discard()
	}

	val, err := (*txn).Get(r.ctx, k.Raw())
	if err != nil {
		return m, err
	}
	// Use reflection to create a new instance of the message type
	msgType := reflect.TypeOf(m).Elem()
	m = reflect.New(msgType).Interface().(M)
	err = proto.Unmarshal(val, m)
	return m, err
}

type StructuredKey struct {
	PrimaryKey      []byte
	IndexKeys       [][]byte
	RemoveIndexKeys [][]byte
}

func (r *BaseRepository[M, K]) GetKeys(obj M) *StructuredKey {
	panic("implement me")
}

func (r *BaseRepository[M, K]) CreateKey(b []byte) K {
	panic("implement me")
}

func (r *BaseRepository[M, K]) Update(key K, cb func(M, kv.Transaction) M) (err error) {
	txn, err := r.store.Begin(true)
	if err != nil {
		return err
	}
	defer func() {
		if err != nil {
			txn.Discard()
		}
	}()

	existing, err := txn.Get(r.ctx, key.Raw())
	if err != nil && !errors.Is(err, kv.ErrKeyNotFound) {
		return err
	}

	batch := txn.NewBatch()
	defer func() {
		if err != nil {
			batch.Reset()
		}
	}()

	var obj M

	if existing != nil {
		var m M
		msgType := reflect.TypeOf(m).Elem()
		m = reflect.New(msgType).Interface().(M)
		err = proto.Unmarshal(existing, m)
		if err != nil {
			return err
		}
		delKeys := r.repo.GetKeys(m)
		for _, key := range delKeys.IndexKeys {
			err = batch.Delete(key)
			if err != nil {
				return err
			}
		}
		obj = cb(m, txn)
	} else {
		var m M
		obj = cb(m, txn)
	}
	val, err := proto.Marshal(obj)
	if err != nil {
		return err
	}
	keys := r.repo.GetKeys(obj)

	err = batch.Set(keys.PrimaryKey, val)
	if err != nil {
		return err
	}
	for _, key := range keys.IndexKeys {
		err = batch.Set(key, keys.PrimaryKey)
		if err != nil {
			return err
		}
	}
	for _, key := range keys.RemoveIndexKeys {
		err = batch.Delete(key)
		if err != nil {
			return err
		}
	}
	err = batch.Flush()
	if err != nil {
		return err
	}
	return txn.Commit()
}

func (r *BaseRepository[M, K]) Put(obj M) error {
	keys := r.repo.GetKeys(obj)
	return r.Update(r.repo.CreateKey(keys.PrimaryKey), func(m M, transaction kv.Transaction) M {
		return obj
	})
}

func (r *BaseRepository[M, K]) Delete(obj M, txn *kv.Transaction) (err error) {
	keys := r.repo.GetKeys(obj)
	if txn == nil {
		t, err := r.store.Begin(true)
		if err != nil {
			return err
		}
		defer t.Discard()
		txn = &t
	}

	batch := (*txn).NewBatch()
	defer func() {
		if err != nil {
			batch.Reset()
		}
	}()
	err = batch.Delete(keys.PrimaryKey)
	if err != nil {
		return err
	}
	for _, key := range keys.IndexKeys {
		err = batch.Delete(key)
		if err != nil {
			return err
		}
	}
	for _, key := range keys.RemoveIndexKeys {
		err = batch.Delete(key)
		if err != nil {
			return err
		}
	}
	return batch.Flush()
}

func (r *BaseRepository[M, K]) PrefixScan(cursor []byte, write bool, prefix Prefix, c func(K, M, *kv.Transaction) error) error {
	txn, err := r.store.Begin(write)
	if err != nil {
		return err
	}
	defer txn.Discard()

	it := r.store.NewIterator(kv.IteratorOptions{
		PrefetchValues: false,
		Prefix:         prefix.raw,
	})
	it.Rewind()
	if len(cursor) > 0 {
		it.Seek(cursor)
	}

	for ; it.Valid(); it.Next() {
		var m M
		val, err := it.Item().ValueCopy()
		if err != nil {
			nerr := it.Close()
			if nerr != nil {
				err = errors.Join(err, nerr)
			}
			return err
		}
		// Use reflection to create a new instance of the message type
		msgType := reflect.TypeOf(m).Elem()
		m = reflect.New(msgType).Interface().(M)
		err = proto.Unmarshal(val, m)
		if err != nil {
			nerr := it.Close()
			if nerr != nil {
				err = errors.Join(err, nerr)
			}
			return err
		}
		k := r.repo.CreateKey(it.Item().KeyCopy())
		err = c(k, m, &txn)
		if err != nil {
			nerr := it.Close()
			if nerr != nil {
				err = errors.Join(err, nerr)
			}
			return err
		}
	}

	err = it.Close()
	if err != nil {
		return err
	}
	return txn.Commit()
}

func (r *BaseRepository[M, K]) CountPrefix(prefix Prefix) (int64, error) {
	it := r.store.NewIterator(kv.IteratorOptions{
		PrefetchValues: false,
		Prefix:         prefix.raw,
	})

	count := int64(0)
	for it.Rewind(); it.Valid(); it.Next() {
		count++
	}

	return count, it.Close()
}

// ScanIndex scans secondary index keys where values are primary keys, not full messages
func (r *BaseRepository[M, K]) ScanIndex(prefix Prefix, write bool, callback func(primaryKey []byte, txn *kv.Transaction) error) error {
	txn, err := r.store.Begin(write)
	if err != nil {
		return err
	}
	defer txn.Discard()

	it := txn.NewIterator(kv.IteratorOptions{
		PrefetchValues: true,
		Prefix:         prefix.raw,
	})

	it.Rewind()
	var scanErr error
	for ; it.Valid(); it.Next() {
		primaryKey, err := it.Item().ValueCopy()
		if err != nil {
			scanErr = err
			break
		}
		if err := callback(primaryKey, &txn); err != nil {
			scanErr = err
			break
		}
	}

	err = errors.Join(scanErr, it.Close())
	if err != nil {
		return err
	}
	return txn.Commit()
}
