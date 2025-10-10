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
	"fmt"

	"github.com/bottledcode/atlas-db/atlas/kv"
	"github.com/zeebo/blake3"
	"google.golang.org/protobuf/proto"
)

type DataRepository interface {
	ProcessIncomingMigration(*Migration) (*Migration, error)
	ProcessOutgoingMigration(*Migration) (*Migration, error)
	Dereference(record *Record) (*Record, error)
}

func NewDataRepository(ctx context.Context, store kv.Store) DataRepository {
	repo := &DataR{
		BaseRepository[*Record, DataKey]{
			store: store,
			ctx:   ctx,
		},
	}
	repo.repo = repo

	return repo
}

type DataKey struct {
	GenericKey
}

type DataR struct {
	BaseRepository[*Record, DataKey]
}

func (d *DataR) CreateKey(k []byte) DataKey {
	return DataKey{
		GenericKey{raw: k},
	}
}

func (d *DataR) GetKeys(record *Record) *StructuredKey {
	// Extract the reference from the record to build the key
	var checksum []byte
	switch data := record.GetData().(type) {
	case *Record_Ref:
		checksum = data.Ref.GetChecksum()
	case *Record_Value:
		// If it's a value, hash it to get the checksum
		hasher := blake3.New()
		_, _ = hasher.Write(data.Value.GetData())
		checksum = hasher.Sum(nil)
	}

	primaryKey := kv.NewKeyBuilder().Meta().Append("ref").Append(fmt.Sprintf("%x", checksum)).Build()

	return &StructuredKey{
		PrimaryKey: primaryKey,
	}
}

func (d *DataR) hashData(data *RawData) (*DataReference, *Record) {
	hasher := blake3.New()
	_, _ = hasher.Write(data.GetData())
	hash := hasher.Sum(nil)
	return &DataReference{
			Key:      kv.NewKeyBuilder().Meta().Append("ref").Append(fmt.Sprintf("%x", hash)).Build(),
			Checksum: hash,
		}, &Record{
			Data: &Record_Value{
				Value: data,
			},
		}
}

func (d *DataR) ProcessOutgoingMigration(m *Migration) (*Migration, error) {
	switch migration := m.GetMigration().(type) {
	case *Migration_None:
		return m, nil
	case *Migration_Schema:
		return m, nil
	case *Migration_Data:
		switch op := migration.Data.GetChange().GetOperation().(type) {
		case *KVChange_Acl:
			return m, nil
		case *KVChange_Data:
			return m, nil
		case *KVChange_Del:
			return m, nil
		case *KVChange_Notification:
			return m, nil
		case *KVChange_Sub:
			return m, nil
		case *KVChange_Set:
			switch data := op.Set.GetData().GetData().(type) {
			case *Record_Ref:
				key := data.Ref.GetKey()
				next := proto.Clone(m).(*Migration)
				rec, err := d.GetByKey(DataKey{GenericKey{key}}, nil)
				if err != nil {
					return nil, err
				}
				next.GetData().GetChange().GetSet().Data = rec
				return next, nil
			case *Record_Value:
				return m, nil
			default:
				panic("unknown data type")
			}
		default:
			panic("unknown operation type")
		}
	default:
		panic("unknown migration type")
	}
}

func (d *DataR) ProcessIncomingMigration(m *Migration) (*Migration, error) {
	switch migration := m.GetMigration().(type) {
	case *Migration_None:
		return m, nil
	case *Migration_Schema:
		return m, nil
	case *Migration_Data:
		switch op := migration.Data.GetChange().GetOperation().(type) {
		case *KVChange_Acl:
			return m, nil
		case *KVChange_Data:
			return m, nil
		case *KVChange_Del:
			return m, nil
		case *KVChange_Set:
			switch data := op.Set.GetData().GetData().(type) {
			case *Record_Ref:
				return m, nil
			case *Record_Value:
				if len(data.Value.Data) < 64 {
					return m, nil
				}

				next := proto.Clone(m).(*Migration)

				ref, store := d.hashData(data.Value)
				next.GetData().GetChange().GetSet().Data.Data = &Record_Ref{
					Ref: ref,
				}
				err := d.Put(store)
				if err != nil {
					return nil, err
				}
				return next, nil
			default:
				panic("unknown data type")
			}
		default:
			panic("unknown operation type")
		}
	default:
		panic("unknown migration type")
	}
}

func (d *DataR) Dereference(record *Record) (*Record, error) {
	switch data := record.GetData().(type) {
	case *Record_Ref:
		return d.GetByKey(DataKey{GenericKey{data.Ref.GetKey()}}, nil)
	case *Record_Value:
		return record, nil
	default:
		panic("unknown data type")
	}
}
