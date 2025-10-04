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

type TableRepository interface {
	// GetTable returns a table by name.
	GetTable(name string) (*Table, error)
	// GetTablesBatch returns multiple tables by name in a single operation.
	// Returns a slice of tables in the same order as the input names.
	// Nil entries indicate table not found for that name.
	GetTablesBatch(names []string) ([]*Table, error)
	// UpdateTable updates a table.
	UpdateTable(*Table) error
	// InsertTable inserts a table.
	InsertTable(*Table) error
	// GetGroup returns a group by name.
	GetGroup(string) (*TableGroup, error)
	// UpdateGroup updates a group.
	UpdateGroup(*TableGroup) error
	// InsertGroup inserts a group.
	InsertGroup(*TableGroup) error
	// GetShard returns a shard of a table, given the principal.
	GetShard(*Table, []*Principal) (*Shard, error)
	// UpdateShard updates a shard metadata.
	UpdateShard(*Shard) error
	// InsertShard inserts a shard metadata.
	// Ensure principals are set and the shard meta-name will be updated before inserting.
	InsertShard(*Shard) error
}
