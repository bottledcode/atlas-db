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
	GetByKey(k K) (M, error)
	GetKeys(obj M) *StructuredKey
	CreateKey(b []byte) K
	Put(obj M) error
	Delete(obj M) error
	DeleteByKey(k K) error
	PrefixScan(cursor []byte, prefix Prefix, c func(K, M) error) error
}

type BaseRepository[M proto.Message, K Key] struct {
	store kv.Store
	ctx   context.Context
	repo  Repository[M, K] // Reference to concrete repository for polymorphic method calls
}

func (r *BaseRepository[M, K]) DeleteByKey(k K) error {
	m, err := r.GetByKey(k)
	if err != nil {
		return err
	}
	return r.Delete(m)
}

func (r *BaseRepository[M, K]) GetByKey(k K) (M, error) {
	val, err := r.store.Get(r.ctx, k.Raw())
	var m M
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

func (r *BaseRepository[M, K]) Put(obj M) (err error) {
	keys := r.repo.GetKeys(obj)
	val, err := proto.Marshal(obj)
	if err != nil {
		return err
	}
	batch := r.store.NewBatch()
	defer func() {
		if err != nil {
			batch.Reset()
		}
	}()
	err = batch.Set(keys.PrimaryKey, val)
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
	return batch.Flush()
}

func (r *BaseRepository[M, K]) Delete(obj M) (err error) {
	keys := r.repo.GetKeys(obj)
	batch := r.store.NewBatch()
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

func (r *BaseRepository[M, K]) PrefixScan(cursor []byte, prefix Prefix, c func(K, M) error) error {
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
		err = c(k, m)
		if err != nil {
			nerr := it.Close()
			if nerr != nil {
				err = errors.Join(err, nerr)
			}
			return err
		}
	}

	return it.Close()
}

func (r *BaseRepository[M, K]) CountPrefix(prefix Prefix) (int64, error) {
	it := r.store.NewIterator(kv.IteratorOptions{
		PrefetchValues: false,
		Prefix:         prefix.raw,
	})
	defer it.Close()

	count := int64(0)
	for it.Rewind(); it.Valid(); it.Next() {
		count++
	}
	return count, nil
}

// ScanIndex scans secondary index keys where values are primary keys, not full messages
func (r *BaseRepository[M, K]) ScanIndex(prefix Prefix, callback func(primaryKey []byte) error) error {
	it := r.store.NewIterator(kv.IteratorOptions{
		PrefetchValues: true,
		Prefix:         prefix.raw,
	})
	defer it.Close()

	it.Rewind()
	for ; it.Valid(); it.Next() {
		primaryKey, err := it.Item().ValueCopy()
		if err != nil {
			return err
		}
		if err := callback(primaryKey); err != nil {
			return err
		}
	}
	return nil
}
