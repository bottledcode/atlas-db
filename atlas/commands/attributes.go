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

package commands

type SqlCommand struct {
	CommandString
}

// IsQueryReadOnly returns whether a query is read-only or not
func (c *SqlCommand) IsQueryReadOnly() bool {
	switch d, _ := c.SelectNormalizedCommand(0); d {
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

func (c *SqlCommand) NonAllowedQuery() bool {
	switch q, _ := c.SelectNormalizedCommand(0); q {
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

func (c *SqlCommand) IsQueryChangeSchema() bool {
	switch q, _ := c.SelectNormalizedCommand(0); q {
	case "ALTER":
		return true
	case "CREATE":
		return true
	case "DROP":
		return true
	}

	return false
}
