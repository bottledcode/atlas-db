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
	"github.com/bottledcode/atlas-db/atlas"
	"zombiezen.com/go/sqlite"
)

// MigrationRepository is an interface that allows getting and maintaining migrations,
// which are uniquely identified by a table, table version, migration version, and sender.
type MigrationRepository interface {
	GetUncommittedMigrations(table *Table) ([]*Migration, error)
	AddMigration(migration *Migration) error
	GetMigrationVersion(version *MigrationVersion) ([]*Migration, error)
	CommitAllMigrations(table string) error
	CommitMigrationExact(version *MigrationVersion) error
	AddGossipMigration(migration *Migration) error
	GetNextVersion(table string) (int64, error)
}

func GetDefaultMigrationRepository(ctx context.Context, conn *sqlite.Conn) MigrationRepository {
	return &migrationRepository{
		ctx:  ctx,
		conn: conn,
	}
}

type migrationRepository struct {
	ctx  context.Context
	conn *sqlite.Conn
}

func (m *migrationRepository) GetNextVersion(table string) (int64, error) {
	results, err := atlas.ExecuteSQL(m.ctx, `select max(version) + 1 as version from migrations where table_id = :table_id`, m.conn, false, atlas.Param{
		Name:  "table_id",
		Value: table,
	})
	if err != nil {
		return 0, err
	}

	return results.Rows[0].GetColumn("version").GetInt(), nil
}

func (m *migrationRepository) GetMigrationVersion(version *MigrationVersion) ([]*Migration, error) {
	results, err := atlas.ExecuteSQL(m.ctx, `
select table_id, table_version, version, committed, batch_part, by_node_id, command, data
from migrations
where table_id = :table_id
  and table_version = :table_version
  and version = :version
  and by_node_id = :by_node_id
order by batch_part`, m.conn, false, atlas.Param{
		Name:  "table_id",
		Value: version.GetTableName(),
	}, atlas.Param{
		Name:  "version",
		Value: version.GetMigrationVersion(),
	}, atlas.Param{
		Name:  "by_node_id",
		Value: version.GetNodeId(),
	}, atlas.Param{
		Name:  "table_version",
		Value: version.GetTableVersion(),
	})
	if err != nil {
		return nil, err
	}

	return m.fromResults(results), nil
}

func (m *migrationRepository) fromResults(results *atlas.Rows) []*Migration {
	migrations := make([]*Migration, 0, len(results.Rows))
	currentBatch := -1
	for _, row := range results.Rows {
		if row.GetColumn("batch_part").GetInt() < int64(currentBatch) {
			migrations = append(migrations, &Migration{
				Version: &MigrationVersion{
					TableVersion:     row.GetColumn("table_version").GetInt(),
					MigrationVersion: row.GetColumn("version").GetInt(),
					NodeId:           row.GetColumn("by_node_id").GetInt(),
					TableName:        row.GetColumn("table_id").GetString(),
				},
				Migration: nil,
			})
			currentBatch += 1
		}

		if row.GetColumn("data").IsNull() {
			// this is a schema migration
			migrations[currentBatch].Migration = &Migration_Schema{
				Schema: &SchemaMigration{
					Commands: []string{
						row.GetColumn("command").GetString(),
					},
				},
			}
		} else {
			// this is a data migration
			migrations[currentBatch].Migration = &Migration_Data{
				Data: &DataMigration{
					Session: [][]byte{
						*row.GetColumn("data").GetBlob(),
					},
				},
			}
		}
	}
	return migrations
}

func (m *migrationRepository) CommitAllMigrations(table string) error {
	_, err := atlas.ExecuteSQL(m.ctx, "update migrations set committed = 1 where table_id = :table_id", m.conn, false, atlas.Param{
		Name:  "table_id",
		Value: table,
	})
	return err
}

func (m *migrationRepository) CommitMigrationExact(version *MigrationVersion) error {
	_, err := atlas.ExecuteSQL(m.ctx, `
update migrations
set committed = 1
where table_id = :table_id
  and version = :version
  and by_node_id = :by_node_id
  and table_version = :table_version`, m.conn, false, atlas.Param{
		Name:  "table_id",
		Value: version.GetTableName(),
	}, atlas.Param{
		Name:  "version",
		Value: version.GetMigrationVersion(),
	}, atlas.Param{
		Name:  "by_node_id",
		Value: version.GetNodeId(),
	}, atlas.Param{
		Name:  "table_version",
		Value: version.GetTableVersion(),
	})
	return err
}

func (m *migrationRepository) GetUncommittedMigrations(table *Table) ([]*Migration, error) {
	results, err := atlas.ExecuteSQL(m.ctx, `
select table_id, table_version, version, committed, batch_part, by_node_id, command, data from migrations where table_id = :table_id and committed = 0 and gossip = 0 order by batch_part
`, m.conn, false, atlas.Param{
		Name:  "table_id",
		Value: table.Name,
	})
	if err != nil {
		return nil, err
	}

	migrations := m.fromResults(results)

	return migrations, nil
}

func (m *migrationRepository) insertMigration(migration *Migration, batchPart int, command *string, data *[]byte, gossip bool) error {
	_, err := atlas.ExecuteSQL(m.ctx, `
insert into migrations (table_id, table_version, version, batch_part, by_node_id, command, data, committed, gossip)
values (:table_id, :table_version, :version, :batch_part, :by_node_id, :command, :data, :committed, :gossip)
on conflict do nothing
`, m.conn, false, atlas.Param{
		Name:  "table_id",
		Value: migration.GetVersion().GetTableName(),
	}, atlas.Param{
		Name:  "version",
		Value: migration.GetVersion().GetMigrationVersion(),
	}, atlas.Param{
		Name:  "batch_part",
		Value: batchPart,
	}, atlas.Param{
		Name:  "by_node_id",
		Value: migration.GetVersion().GetNodeId(),
	}, atlas.Param{
		Name:  "command",
		Value: command,
	}, atlas.Param{
		Name:  "data",
		Value: data,
	}, atlas.Param{
		Name:  "committed",
		Value: false,
	}, atlas.Param{
		Name:  "gossip",
		Value: gossip,
	}, atlas.Param{
		Name:  "table_version",
		Value: migration.GetVersion().GetTableVersion(),
	})
	return err
}

func (m *migrationRepository) AddMigration(migration *Migration) error {
	batch := 0

	switch migration.GetMigration().(type) {
	case *Migration_Schema:
		for _, command := range migration.GetSchema().GetCommands() {
			err := m.insertMigration(migration, batch, &command, nil, false)
			if err != nil {
				return err
			}
			batch++
		}
	case *Migration_Data:
		for _, data := range migration.GetData().GetSession() {
			err := m.insertMigration(migration, batch, nil, &data, false)
			if err != nil {
				return err
			}
			batch++
		}
	}

	return nil
}

// AddGossipMigration adds a migration to the migration table as a gossiped migration.
func (m *migrationRepository) AddGossipMigration(migration *Migration) error {
	batch := 0
	switch migration.GetMigration().(type) {
	case *Migration_Schema:
		for _, command := range migration.GetSchema().GetCommands() {
			err := m.insertMigration(migration, batch, &command, nil, true)
			if err != nil {
				return err
			}
			batch++
		}
	case *Migration_Data:
		for _, data := range migration.GetData().GetSession() {
			err := m.insertMigration(migration, batch, nil, &data, true)
			if err != nil {
				return err
			}
			batch++
		}
	}
	return nil
}
