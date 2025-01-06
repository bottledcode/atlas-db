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

type MigrationRepository interface {
	GetUncommittedMigrations(table *Table) ([]*Migration, error)
	AddMigration(migration *Migration, sender *Node) error
	GetMigrationVersion(table string, version int64) ([]*Migration, error)
	CommitMigration(table string, version int64) error
	CommitMigrationExact(table string, sender *Node, version int64) error
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

func (m *migrationRepository) GetMigrationVersion(table string, version int64) ([]*Migration, error) {
	results, err := atlas.ExecuteSQL(m.ctx, `
select table_id, version, committed, batch_part, by_node_id, command, data from migrations where table_id = :table_id and version = :version
`, m.conn, false, atlas.Param{
		Name:  "table_id",
		Value: table,
	}, atlas.Param{
		Name:  "version",
		Value: version,
	})
	if err != nil {
		return nil, err
	}

	return m.fromResults(results), nil
}

func (m *migrationRepository) fromResults(results *atlas.Rows) []*Migration {
	migrations := make([]*Migration, len(results.Rows))
	for i, row := range results.Rows {
		migrations[i] = &Migration{
			TableId:   row.GetColumn("table_id").GetString(),
			Version:   row.GetColumn("version").GetInt(),
			Migration: nil,
		}

		if row.GetColumn("data").IsNull() {
			// this is a schema migration
			migrations[i].Migration = &Migration_Schema{
				Schema: &SchemaMigration{
					Commands: []string{
						row.GetColumn("command").GetString(),
					},
				},
			}
		} else {
			// this is a data migration
			migrations[i].Migration = &Migration_Data{
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

func (m *migrationRepository) CommitMigration(table string, version int64) error {
	_, err := atlas.ExecuteSQL(m.ctx, "update migrations set committed = 1 where table_id = :table_id and version <= :version", m.conn, false, atlas.Param{
		Name:  "table_id",
		Value: table,
	}, atlas.Param{
		Name:  "version",
		Value: version,
	})
	return err
}

func (m *migrationRepository) CommitMigrationExact(table string, sender *Node, version int64) error {
	_, err := atlas.ExecuteSQL(m.ctx, "update migrations set committed = 1 where table_id = :table_id and version = :version and by_node_id = :by_node_id", m.conn, false, atlas.Param{
		Name:  "table_id",
		Value: table,
	}, atlas.Param{
		Name:  "version",
		Value: version,
	}, atlas.Param{
		Name:  "by_node_id",
		Value: sender.Id,
	})
	return err
}

func (m *migrationRepository) GetUncommittedMigrations(table *Table) ([]*Migration, error) {
	results, err := atlas.ExecuteSQL(m.ctx, `
select table_id, version, committed, batch_part, by_node_id, command, data from migrations where table_id = :table_id and committed = 0
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

func (m *migrationRepository) insertMigration(migration *Migration, batchPart int, sender *Node, command *string, data *[]byte) error {
	_, err := atlas.ExecuteSQL(m.ctx, `
insert into migrations (table_id, version, batch_part, by_node_id, command, data, committed)
values (:table_id, :version, :batch_part, :by_node_id, :command, :data, :committed)
on conflict do nothing
`, m.conn, false, atlas.Param{
		Name:  "table_id",
		Value: migration.GetTableId(),
	}, atlas.Param{
		Name:  "version",
		Value: migration.GetVersion(),
	}, atlas.Param{
		Name:  "batch_part",
		Value: batchPart,
	}, atlas.Param{
		Name:  "by_node_id",
		Value: sender.Id,
	}, atlas.Param{
		Name:  "command",
		Value: command,
	}, atlas.Param{
		Name:  "data",
		Value: data,
	}, atlas.Param{
		Name:  "committed",
		Value: false,
	})
	return err
}

func (m *migrationRepository) AddMigration(migration *Migration, sender *Node) error {
	batch := 0

	switch migration.GetMigration().(type) {
	case *Migration_Schema:
		for _, command := range migration.GetSchema().GetCommands() {
			err := m.insertMigration(migration, batch, sender, &command, nil)
			if err != nil {
				return err
			}
			batch++
		}
	case *Migration_Data:
		for _, data := range migration.GetData().GetSession() {
			err := m.insertMigration(migration, batch, sender, nil, &data)
			if err != nil {
				return err
			}
			batch++
		}
	}

	return nil
}
