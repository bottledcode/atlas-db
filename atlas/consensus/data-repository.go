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

	"github.com/bottledcode/atlas-db/atlas/kv"
	"github.com/zeebo/blake3"
)

type DataRepository interface {
	Repository[*Data, DataKey]
	GetPrefix(reference *DataReference) Prefix
}

func NewDataRepository(ctx context.Context) DataRepository {
	store := kv.GetPool().MetaStore()
	repo := &DataR{
		BaseRepository[*Data, DataKey]{
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
	BaseRepository[*Data, DataKey]
}

func (d *DataR) CreateKey(k []byte) DataKey {
	return DataKey{
		GenericKey{raw: k},
	}
}

func (d *DataR) GetKeys(record *Data) *StructuredKey {
	checksum := record.GetKey()

	return &StructuredKey{
		PrimaryKey: checksum,
	}
}

func (d *DataR) GetPrefix(reference *DataReference) Prefix {
	return Prefix{reference.GetAddress()}
}

func (d *DataR) hashData(data []byte) (*DataReference, *Data) {
	hasher := blake3.New()
	_, _ = hasher.Write(data)
	hash := hasher.Sum(nil)
	return &DataReference{
			Address: hash,
		}, &Data{
			Key:   hash,
			Value: data,
		}
}
