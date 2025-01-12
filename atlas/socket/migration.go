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

package socket

import (
	"context"
	"zombiezen.com/go/sqlite"
)

func maybeWatchTable(ctx context.Context, query *commandString, session *sqlite.Session) error {
	if query.selectNormalizedCommand(0) != "CREATE" {
		return nil
	}

	var tableName string

	switch query.selectNormalizedCommand(1) {
	case "TABLE":
		tableName = query.selectNormalizedCommand(2)
	case "REGIONAL":
		tableName = query.selectNormalizedCommand(3)
	}

	if tableName == "" {
		// this is a local table

		return nil
	}

	// watch the table
	err := session.Attach(tableName)
	if err != nil {
		return err
	}

	return nil
}

// isQueryReadOnly returns whether a query is read-only or not
func isQueryReadOnly(query *commandString) bool {
	switch query.selectCommand(0) {
	case "ALTER":
		return false
	case "CREATE":
		return false
	case "DELETE":
		return false
	case "DROP":
		return false
	case "INSERT":
		return false
	case "REINDEX":
		return false
	case "REPLACE":
		return false
	case "UPDATE":
		return false
	case "VACUUM":
		return false
	case "PRAGMA":
		return false
	}

	return true
}

func nonAllowedQuery(query *commandString) bool {
	switch query.selectCommand(0) {
	case "BEGIN":
		return true
	case "COMMIT":
		return true
	case "ROLLBACK":
		return true
	case "SAVEPOINT":
		return true
	case "RELEASE":
		return true
	case "DETACH":
		return true
	case "ATTACH":
		return true
	case "PRAGMA":
		return true
	}

	return false
}

func isQueryChangeSchema(query *commandString) bool {
	switch query.selectCommand(0) {
	case "ALTER":
		return true
	case "CREATE":
		return true
	case "DROP":
		return true
	}

	return false
}
