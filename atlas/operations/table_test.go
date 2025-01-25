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

package operations_test

import (
	"testing"

	"github.com/bottledcode/atlas-db/atlas/commands"
	"github.com/bottledcode/atlas-db/atlas/consensus"
	"github.com/bottledcode/atlas-db/atlas/operations"
	"github.com/stretchr/testify/assert"
)

func TestCreateTable(t *testing.T) {
	tests := []struct {
		name     string
		command  string
		expected []*consensus.Table
		err      string
	}{
		{
			name:    "Missing table keyword",
			command: "CREATE",
			err:     "CREATE TABLE: missing table keyword",
		},
		{
			name:    "Unknown replication level",
			command: "CREATE UNKNOWN TABLE test",
			err:     "CREATE TABLE: unknown replication level",
		},
		{
			name:    "Temporary tables not supported",
			command: "CREATE TEMP TABLE test",
			err:     "CREATE TABLE: temporary tables are not supported",
		},
		{
			name:    "Create global table",
			command: "CREATE GLOBAL TABLE test",
			expected: []*consensus.Table{
				{
					Name:             "MAIN.TEST",
					ReplicationLevel: consensus.ReplicationLevel_global,
					Type:             consensus.TableType_table,
					Version:          1,
				},
			},
		},
		{
			name:    "Create regional table",
			command: "CREATE REGIONAL TABLE test",
			expected: []*consensus.Table{
				{
					Name:             "MAIN.TEST",
					ReplicationLevel: consensus.ReplicationLevel_regional,
					Type:             consensus.TableType_table,
					Version:          1,
				},
			},
		},
		{
			name:    "Create local table",
			command: "CREATE LOCAL TABLE test",
			expected: []*consensus.Table{
				{
					Name:             "MAIN.TEST",
					ReplicationLevel: consensus.ReplicationLevel_local,
					Type:             consensus.TableType_table,
					Version:          1,
				},
			},
		},
		{
			name:    "Create table with group",
			command: "CREATE TABLE test GROUP group1",
			expected: []*consensus.Table{
				{
					Name:             "GROUP1",
					ReplicationLevel: consensus.ReplicationLevel_global,
					Type:             consensus.TableType_group,
					Version:          1,
				},
				{
					Name:             "MAIN.TEST",
					ReplicationLevel: consensus.ReplicationLevel_global,
					Group:            "GROUP1",
					Type:             consensus.TableType_table,
					Version:          1,
				},
			},
		},
		{
			name:    "Create table with shard",
			command: "CREATE TABLE test SHARD BY PRINCIPAL key",
			expected: []*consensus.Table{
				{
					Name:             "MAIN.TEST",
					ReplicationLevel: consensus.ReplicationLevel_global,
					Type:             consensus.TableType_sharded,
					ShardPrincipals:  []string{"KEY"},
					Version:          1,
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cmd := commands.CommandFromString(tt.command)
			tables, err := operations.CreateTable(cmd)

			if tt.err != "" {
				assert.EqualError(t, err, tt.err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.expected, tables)
			}
		})
	}
}

func TestAlterTable(t *testing.T) {
	tests := []struct {
		name          string
		command       string
		existingTable *consensus.Table
		expected      []*consensus.Table
		err           string
	}{
		{
			name:    "Missing table keyword",
			command: "ALTER",
			err:     "ALTER TABLE: missing table keyword",
			existingTable: &consensus.Table{
				Name: "test",
			},
		},
		{
			name:    "Missing table name",
			command: "ALTER TABLE",
			err:     "ALTER TABLE: missing table name",
			existingTable: &consensus.Table{
				Name: "test",
			},
		},
		{
			name:    "Table does not exist",
			command: "ALTER TABLE test ADD GROUP group1",
			err:     "ALTER TABLE: table does not exist",
		},
		{
			name:    "Table type does not match",
			command: "ALTER TABLE test ADD GROUP group1",
			existingTable: &consensus.Table{
				Name: "MAIN.TEST",
				Type: consensus.TableType_view,
			},
			err: "ALTER TABLE: table type does not match an existing table",
		},
		{
			name:    "Add group to table",
			command: "ALTER TABLE test ADD GROUP group1",
			existingTable: &consensus.Table{
				Name: "MAIN.TEST",
				Type: consensus.TableType_table,
			},
			expected: []*consensus.Table{
				{
					Name:             "GROUP1",
					ReplicationLevel: consensus.ReplicationLevel_global,
					Type:             consensus.TableType_group,
					Version:          1,
				},
				{
					Name:  "MAIN.TEST",
					Type:  consensus.TableType_table,
					Group: "GROUP1",
				},
			},
		},
		{
			name:    "Drop group from table",
			command: "ALTER TABLE test DROP GROUP group1",
			existingTable: &consensus.Table{
				Name:  "MAIN.TEST",
				Type:  consensus.TableType_table,
				Group: "GROUP1",
			},
			expected: []*consensus.Table{
				{
					Name:  "MAIN.TEST",
					Type:  consensus.TableType_table,
					Group: "",
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cmd := commands.CommandFromString(tt.command)
			tables, err := operations.AlterTable(cmd, tt.existingTable)

			if tt.err != "" {
				assert.EqualError(t, err, tt.err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.expected, tables)
			}
		})
	}
}
