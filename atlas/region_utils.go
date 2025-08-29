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

package atlas

import (
	"context"

	"zombiezen.com/go/sqlite"
)

// GetOrAddRegion If the region does not exist, it is created.
func GetOrAddRegion(ctx context.Context, conn *sqlite.Conn, name string) (string, error) {
	results, err := ExecuteSQL(ctx, "select * from regions where name = :name", conn, false, Param{
		Name:  "name",
		Value: name,
	})
	if err != nil {
		return name, err
	}

	if len(results.Rows) > 0 {
		return results.GetIndex(0).GetColumn("name").GetString(), nil
	}

	_, err = ExecuteSQL(ctx, "insert into regions (name) values (:name)", conn, false, Param{
		Name:  "name",
		Value: name,
	})
	if err != nil {
		return name, err
	}

	return name, nil
}
