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
