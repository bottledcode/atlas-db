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

// MigrationRepository is an interface that allows getting and maintaining migrations,
// which are uniquely identified by a table, table version, migration version, and sender.
type MigrationRepository interface {
	// GetUncommittedMigrations returns all uncommitted migrations -- from when this node was part of a previous quorum -- for a given table.
	GetUncommittedMigrations(table *Table) ([]*Migration, error)
	// AddMigration adds a migration to the migration table.
	AddMigration(migration *Migration) error
	// GetMigrationVersion returns all migrations for a given version.
	GetMigrationVersion(version *MigrationVersion) ([]*Migration, error)
	// CommitAllMigrations commits all migrations for a given table.
	CommitAllMigrations(table string) error
	// CommitMigrationExact commits a migration for a given version.
	CommitMigrationExact(version *MigrationVersion) error
	// AddGossipMigration adds a migration to the migration table as a gossiped migration.
	AddGossipMigration(migration *Migration) error
	// GetNextVersion returns the next version for a given table.
	GetNextVersion(table string) (int64, error)
}

// NewMigrationRepositoryKV creates a new KV-based migration repository
func NewMigrationRepositoryKV(ctx context.Context, store kv.Store) MigrationRepository {
	dataRepo := &DataR{
		BaseRepository[*Record, DataKey]{
			store: store,
			ctx:   ctx,
		},
	}
	dataRepo.repo = dataRepo

	repo := &MigrationR{
		BaseRepository: BaseRepository[*StoredMigrationBatch, MigrationKey]{
			store: store,
			ctx:   ctx,
		},
		data: dataRepo,
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

type MigrationKey struct {
	GenericKey
}

type MigrationR struct {
	BaseRepository[*StoredMigrationBatch, MigrationKey]
	data *DataR
}

func (m *MigrationR) CreateKey(k []byte) MigrationKey {
	return MigrationKey{
		GenericKey{raw: k},
	}
}

func (m *MigrationR) getMigrationKey(version *MigrationVersion) MigrationKey {
	key := kv.NewKeyBuilder().Meta().
		Migration(version.GetTableName(), version.GetMigrationVersion()).
		TableVersion(version.GetTableVersion()).
		Node(version.GetNodeId()).
		Build()
	return m.CreateKey(key)
}

func (m *MigrationR) getMigrationPrefix(version *MigrationVersion) Prefix {
	key := kv.NewKeyBuilder().Meta().
		Migration(version.GetTableName(), version.GetMigrationVersion())

	if version.GetNodeId() > 0 {
		key = key.Node(version.GetNodeId())
		if version.GetTableVersion() > 0 {
			key = key.TableVersion(version.GetTableVersion())
		}
	}

	return Prefix{raw: key.Build()}
}

type gossipValue int

const (
	gossipValueTrue  gossipValue = 1
	gossipValueFalse gossipValue = 0
	gossipValueUnset gossipValue = -1
)

func gossipFromBool(b bool) gossipValue {
	if b {
		return gossipValueTrue
	}
	return gossipValueFalse
}

func (m *MigrationR) getUncommittedMigrationPrefix(table string, version int64, node int64, gossip gossipValue) Prefix {
	key := kv.NewKeyBuilder().Meta().Index().
		Append("migu").
		Append("t").Append(table)

	if gossip == gossipValueTrue {
		key.Append("g")
	} else if gossip == gossipValueFalse {
		key.Append("a")
	}

	if version > 0 && gossip != gossipValueUnset {
		key = key.Append("v").Append(fmt.Sprintf("%d", version))
		if node > 0 {
			key = key.Append("n").Append(fmt.Sprintf("%d", node))
		}
	}

	return Prefix{
		raw: key.Build(),
	}
}

func (m *MigrationR) GetKeys(migration *StoredMigrationBatch) *StructuredKey {

	version := migration.GetMigration().GetVersion()

	var indexKeys [][]byte
	var removeIndexKeys [][]byte

	uncommitted := m.getUncommittedMigrationPrefix(
		version.GetTableName(),
		version.GetTableVersion(),
		version.GetNodeId(),
		gossipFromBool(migration.GetGossip()),
	).raw

	if !migration.GetCommitted() {
		indexKeys = append(indexKeys, uncommitted)
	} else {
		removeIndexKeys = append(removeIndexKeys, uncommitted)
	}

	key := &StructuredKey{
		PrimaryKey:      m.getMigrationKey(version).raw,
		IndexKeys:       indexKeys,
		RemoveIndexKeys: removeIndexKeys,
	}
	return key
}

func (m *MigrationR) GetUncommittedMigrations(table *Table) ([]*Migration, error) {
	batch := make([]*Migration, 0)

	err := m.ScanIndex(
		m.getUncommittedMigrationPrefix(table.GetName(), 0, 0, gossipValueFalse),
		false,
		func(primaryKey []byte, txn *kv.Transaction) error {
		r, err := m.GetByKey(MigrationKey{GenericKey{raw: primaryKey}}, txn)
		if err != nil {
			return err
		}
		batch = append(batch, r.GetMigration())
		return nil
	})
	if err != nil {
		return batch, err
	}
	return batch, nil
}

func (m *MigrationR) AddMigration(migration *Migration) (err error) {
	migration, err = m.data.ProcessIncomingMigration(migration)
	if err != nil {
		return err
	}

	storedMigration := &StoredMigrationBatch{
		Migration: migration,
		Committed: false,
		Gossip:    false,
	}

	return m.Put(storedMigration)
}

func (m *MigrationR) GetMigrationVersion(version *MigrationVersion) ([]*Migration, error) {
	if version == nil {
		return nil, fmt.Errorf("version is nil")
	}
	prefix := m.getMigrationPrefix(version)
	migrations := make([]*Migration, 0)
	err := m.PrefixScan(nil, false, prefix, func(key MigrationKey, batch *StoredMigrationBatch, txn *kv.Transaction) error {
		migrations = append(migrations, batch.GetMigration())
		return nil
	})
	if err != nil {
		return migrations, err
	}
	return migrations, nil
}

func (m *MigrationR) CommitAllMigrations(table string) error {
	prefix := m.getUncommittedMigrationPrefix(table, 0, 0, gossipValueFalse)
	err := m.ScanIndex(prefix, true, func(primaryKey []byte, txn *kv.Transaction) error {
		return m.Update(MigrationKey{GenericKey{raw: primaryKey}}, func(batch *StoredMigrationBatch, txn kv.Transaction) *StoredMigrationBatch {
			batch.Committed = true
			return batch
		})
	})
	return err
}

func (m *MigrationR) CommitMigrationExact(version *MigrationVersion) error {
	key := m.getMigrationKey(version)
	r, err := m.GetByKey(key, nil)
	if err != nil {
		return err
	}
	r.Committed = true
	return m.Put(r)
}

func (m *MigrationR) AddGossipMigration(migration *Migration) (err error) {
	migration, err = m.data.ProcessIncomingMigration(migration)
	if err != nil {
		return err
	}

	storedMigration := &StoredMigrationBatch{
		Migration: migration,
		Committed: false,
		Gossip:    true,
	}

	return m.Put(storedMigration)
}

func (m *MigrationR) GetNextVersion(table string) (int64, error) {
	prefix := m.getMigrationPrefix(&MigrationVersion{
		TableName: table,
	})
	lastVersion := int64(0)
	err := m.PrefixScan(nil, false, prefix, func(key MigrationKey, batch *StoredMigrationBatch, txn *kv.Transaction) error {
		if version := batch.GetMigration().GetVersion().GetMigrationVersion(); version > lastVersion {
			lastVersion = version
		}
		return nil
	})
	if err != nil {
		return lastVersion, err
	}
	return lastVersion + 1, nil
}
