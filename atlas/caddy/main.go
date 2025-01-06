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
 */

package main

import (
	caddycmd "github.com/caddyserver/caddy/v2/cmd"

	_ "github.com/bottledcode/atlas-db/atlas/caddy/module"
	_ "github.com/caddyserver/caddy/v2/modules/standard"
)

func main() {
	caddycmd.Main()
}
