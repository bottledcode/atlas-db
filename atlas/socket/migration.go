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
	"github.com/bottledcode/atlas-db/atlas/commands"
	"zombiezen.com/go/sqlite"
)

func maybeWatchTable(ctx context.Context, query *commands.SqlCommand, session *sqlite.Session) error {
	if s, _ := query.SelectNormalizedCommand(0); s != "CREATE" {
		return nil
	}

	var tableName string

	switch s, _ := query.SelectNormalizedCommand(1); s {
	case "TABLE":
		tableName, _ = query.SelectNormalizedCommand(2)
	case "REGIONAL":
		tableName, _ = query.SelectNormalizedCommand(3)
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
