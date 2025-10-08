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
func NewMigrationRepositoryKV(ctx context.Context, store kv.Store, dataRepository DataRepository) MigrationRepository {
	repo := &MigrationR{
		BaseRepository: BaseRepository[*StoredMigrationBatch, MigrationKey]{
			store: store,
			ctx:   ctx,
		},
		data: dataRepository,
	}
	repo.repo = repo
	return repo
}

type MigrationKey struct {
	GenericKey
}

type MigrationR struct {
	BaseRepository[*StoredMigrationBatch, MigrationKey]
	data DataRepository
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

	switch gossip {
	case gossipValueUnset:
		break
	case gossipValueTrue:
		key.Append("g")
	case gossipValueFalse:
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
			mig, err := m.data.ProcessOutgoingMigration(r.GetMigration())
			if err != nil {
				return err
			}
			batch = append(batch, mig)
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
		mig, err := m.data.ProcessOutgoingMigration(batch.GetMigration())
		if err != nil {
			return err
		}
		migrations = append(migrations, mig)
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
		return m.Update(MigrationKey{GenericKey{raw: primaryKey}}, func(batch *StoredMigrationBatch, txn kv.Transaction) (*StoredMigrationBatch, error) {
			batch.Committed = true
			return batch, nil
		})
	})
	return err
}

func (m *MigrationR) CommitMigrationExact(version *MigrationVersion) error {
	key := m.getMigrationKey(version)
	return m.Update(key, func(batch *StoredMigrationBatch, txn kv.Transaction) (*StoredMigrationBatch, error) {
		if batch == nil {
			return nil, fmt.Errorf("migration not found")
		}
		batch.Committed = true
		return batch, nil
	})
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
