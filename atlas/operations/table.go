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

package operations

import (
	"errors"
	"github.com/bottledcode/atlas-db/atlas/commands"
	"github.com/bottledcode/atlas-db/atlas/consensus"
)

const (
	CreateTableTableOrReplication = 1
	CreateTableNameOrIfNotExists  = 2
	CreateTableName               = 5
	CreateTableGroup              = -2
	CreateTableGroupName          = -1
	CreateTableShard              = -4
	CreateTableShardName          = -1
)

const (
	AlterTableType      = 1
	AlterTableName      = 2
	AlterTableAddDrop   = 3
	AlterTableGroup     = 4
	AlterTableGroupName = 5
)

func extractGroup(c commands.Command) (string, commands.Command) {
	g, ok := c.SelectNormalizedCommand(CreateTableGroup)
	if !ok {
		return "", c
	}

	if g != "GROUP" {
		return "", c
	}

	group, _ := c.SelectNormalizedCommand(CreateTableGroupName)
	c = c.RemoveAfter(CreateTableGroup)
	return group, c
}

func extractShard(c commands.Command) (string, commands.Command) {
	s, ok := c.SelectNormalizedCommand(CreateTableShard)
	if !ok {
		return "", c
	}

	if s != "SHARD" {
		return "", c
	}

	if by, _ := c.SelectNormalizedCommand(CreateTableShard + 1); by != "BY" {
		return "", c
	}

	if p, _ := c.SelectNormalizedCommand(CreateTableShard + 2); p != "PRINCIPAL" {
		return "", c
	}

	principal, _ := c.SelectNormalizedCommand(CreateTableShardName)
	c = c.RemoveAfter(-4)
	return principal, c
}

// CreateTable parses a SQL query and creates an appropriate Table(s) object for proposing to the cluster.
// It accepts a command structure like:
// CREATE [REPLICATION] TABLE [IF NOT EXISTS] table_name (...) [GROUP group_name] [SHARD BY PRINCIPAL key]
func CreateTable(c commands.Command) ([]*consensus.Table, error) {
	// first, we determine what l of replication we desire
	l, ok := c.SelectNormalizedCommand(CreateTableTableOrReplication)
	if !ok {
		return nil, errors.New("CREATE TABLE: missing table keyword")
	}

	// if we are creating a temp table, so we can just ignore it; all temporary tables are node-only
	if l == "TEMP" || l == "TEMPORARY" {
		return nil, errors.New("CREATE TABLE: temporary tables are not supported")
	}

	level := consensus.ReplicationLevel_global
	var tableType consensus.TableType

	switch l {
	case "GLOBAL": // global table; the default
		c = c.ReplaceCommand("CREATE GLOBAL", "CREATE")
	case "REGIONAL": // regional table
		level = consensus.ReplicationLevel_regional
		c = c.ReplaceCommand("CREATE REGIONAL", "CREATE")
	case "LOCAL": // local table
		level = consensus.ReplicationLevel_local
		c = c.ReplaceCommand("CREATE LOCAL", "CREATE")
	case "TABLE": // global table; the default
		break
	case "TRIGGER":
		break
	case "VIEW":
		break
	default:
		return nil, errors.New("CREATE TABLE: unknown replication level")
	}

	// and now, determine the type of table
	switch l, _ = c.SelectNormalizedCommand(CreateTableTableOrReplication); l {
	case "TABLE":
		tableType = consensus.TableType_table
	case "TRIGGER":
		tableType = consensus.TableType_trigger
	case "VIEW":
		tableType = consensus.TableType_view
	default:
		return nil, errors.New("CREATE TABLE: unknown table type")
	}

	name, _ := c.SelectNormalizedCommand(CreateTableNameOrIfNotExists)
	if name == "IF" {
		// we have an "IF NOT EXISTS" clause
		name, _ = c.SelectNormalizedCommand(CreateTableName)
	} else {
		name, _ = c.SelectNormalizedCommand(CreateTableNameOrIfNotExists)
	}
	name = c.NormalizeName(name)
	if name == "" {
		return nil, errors.New("CREATE TABLE: missing table name")
	}

	var groups []string
	var shards []string
	for {
		var group string
		var shard string
		group, c = extractGroup(c)
		shard, c = extractShard(c)
		if shard == "" && group == "" {
			break
		}
		if shard != "" {
			shards = append(shards, shard)
		}
		if group != "" {
			groups = append(groups, group)
		}
	}

	if len(groups) > 1 {
		return nil, errors.New("CREATE TABLE: multiple groups are not supported")
	}

	var tables []*consensus.Table

	if len(groups) == 1 {
		tables = append(tables, &consensus.Table{
			Name:              groups[0],
			ReplicationLevel:  level,
			Owner:             nil,
			CreatedAt:         nil,
			Version:           1,
			AllowedRegions:    nil,
			RestrictedRegions: nil,
			Group:             "",
			Type:              consensus.TableType_group,
			ShardPrincipals:   nil,
		})
	} else {
		groups = append(groups, "")
	}

	if len(shards) > 0 && tableType == consensus.TableType_table {
		tableType = consensus.TableType_sharded
		// todo: potentially modify the query to include the shard key?
	} else if len(shards) > 0 && tableType != consensus.TableType_table {
		return nil, errors.New("CREATE TABLE: sharded tables can only be of type TABLE")
	}

	tables = append(tables, &consensus.Table{
		Name:              name,
		ReplicationLevel:  level,
		Owner:             nil,
		CreatedAt:         nil,
		Version:           1,
		AllowedRegions:    nil,
		RestrictedRegions: nil,
		Group:             groups[0],
		Type:              tableType,
		ShardPrincipals:   shards,
	})

	return tables, nil
}

// AlterTable parses a SQL query and creates an appropriate Table(s) object for proposing to the cluster.
// It accepts a command structure like:
// ALTER TABLE table_name [ADD|DROP] GROUP
func AlterTable(c commands.Command, existingTable *consensus.Table) ([]*consensus.Table, error) {
	if existingTable == nil {
		return nil, errors.New("ALTER TABLE: table does not exist")
	}

	// get the table type
	t, ok := c.SelectNormalizedCommand(AlterTableType)
	if !ok {
		return nil, errors.New("ALTER TABLE: missing table keyword")
	}
	switch t {
	case "TABLE":
		if existingTable.Type != consensus.TableType_table {
			return nil, errors.New("ALTER TABLE: table type does not match an existing table")
		}
	case "TRIGGER":
		if existingTable.Type != consensus.TableType_trigger {
			return nil, errors.New("ALTER TABLE: table type does not match an existing table")
		}
	case "VIEW":
		if existingTable.Type != consensus.TableType_view {
			return nil, errors.New("ALTER TABLE: table type does not match an existing table")
		}
	default:
		return []*consensus.Table{existingTable}, nil
	}

	// now extract the table name
	name, ok := c.SelectNormalizedCommand(AlterTableName)
	name = c.NormalizeName(name)
	if !ok {
		return nil, errors.New("ALTER TABLE: missing table name")
	}
	if existingTable.Name != name {
		return nil, errors.New("ALTER TABLE: table name does not match an existing table")
	}

	// get the operation
	op, ok := c.SelectNormalizedCommand(AlterTableAddDrop)
	if !ok {
		return nil, errors.New("ALTER TABLE: missing ADD or DROP keyword")
	}

	var tables []*consensus.Table

	if op == "ADD" {
		// get the group name
		group, ok := c.SelectNormalizedCommand(AlterTableGroup)
		if ok && group == "GROUP" {
			// get the group name
			groupName, ok := c.SelectNormalizedCommand(AlterTableGroupName)
			if !ok {
				return nil, errors.New("ALTER TABLE: missing group name")
			}
			existingTable.Group = groupName

			tables = append(tables, &consensus.Table{
				Name:              groupName,
				ReplicationLevel:  consensus.ReplicationLevel_global,
				Owner:             nil,
				CreatedAt:         nil,
				Version:           1,
				AllowedRegions:    nil,
				RestrictedRegions: nil,
				Group:             "",
				Type:              consensus.TableType_group,
				ShardPrincipals:   nil,
			}, existingTable)

			return tables, nil
		}
	}

	if op == "DROP" {
		// get the group name
		group, ok := c.SelectNormalizedCommand(AlterTableGroup)
		if ok && group == "GROUP" {
			groupName, ok := c.SelectNormalizedCommand(AlterTableGroupName)
			if !ok {
				return nil, errors.New("ALTER TABLE: missing group name")
			}

			if groupName != existingTable.Group {
				return nil, errors.New("ALTER TABLE: group name does not match an existing group")
			}

			existingTable.Group = ""
			return []*consensus.Table{existingTable}, nil
		}
	}

	return []*consensus.Table{existingTable}, nil
}
