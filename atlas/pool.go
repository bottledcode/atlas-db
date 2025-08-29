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
	_ "embed"
	"runtime"
	"strings"
	"sync"

	"zombiezen.com/go/sqlite"
	"zombiezen.com/go/sqlite/sqlitemigration"
)

//go:embed bootstrap/migrations.sql
var migrations string

var Pool *sqlitemigration.Pool
var MigrationsPool *sqlitemigration.Pool

var authorizers = map[*sqlite.Conn]*Authorizer{}

// CreatePool creates a new connection pool for the database and the migrations database.
func CreatePool(options *Options) {
	if Pool != nil {
		return
	}

	authorizers = make(map[*sqlite.Conn]*Authorizer)
	amu := sync.Mutex{}

	Pool = sqlitemigration.NewPool(options.DbFilename, sqlitemigration.Schema{}, sqlitemigration.Options{
		Flags:    sqlite.OpenReadWrite | sqlite.OpenCreate | sqlite.OpenWAL,
		PoolSize: runtime.NumCPU() * 2,
		PrepareConn: func(conn *sqlite.Conn) (err error) {
			auth := &Authorizer{}

			amu.Lock()
			authorizers[conn] = auth
			amu.Unlock()

			err = conn.SetAuthorizer(auth)
			if err != nil {
				return
			}

			err = conn.SetDefensive(true)
			if err != nil {
				return
			}

			se := func(query string) {
				st, _, err := conn.PrepareTransient(query)
				if err != nil {
					panic("Error preparing transient statement: " + err.Error())
				}
				defer func() {
					err = st.Finalize()
					if err != nil {
						panic("Error finalizing transient statement: " + err.Error())
					}
				}()
				_, err = st.Step()
				if err != nil {
					panic("Error executing transient statement: " + err.Error())
				}
			}

			se("PRAGMA foreign_keys = ON;")
			se("PRAGMA synchronous = NORMAL;")
			se("PRAGMA cache_size = -2000;")
			se("attach database '" + options.MetaFilename + "' as atlas")

			return
		},
	})

	// strip comments from migrations
	migs := strings.Split(migrations, ";")
	deleting := false
	for i, mig := range migs {
		// remove all substrings between /* and */
		if strings.Contains(mig, "/*") {
			deleting = true
		}
		if strings.Contains(mig, "*/") {
			deleting = false
			end := strings.Index(mig, "*/") + 2
			migs[i] = mig[end:]
			continue
		}
		if deleting {
			migs[i] = ""
		}
	}

	MigrationsPool = sqlitemigration.NewPool(options.MetaFilename, sqlitemigration.Schema{
		Migrations: migs,
	}, sqlitemigration.Options{
		Flags:    sqlite.OpenReadWrite | sqlite.OpenCreate | sqlite.OpenWAL,
		PoolSize: 10,
		PrepareConn: func(conn *sqlite.Conn) (err error) {
			err = conn.SetDefensive(true)
			if err != nil {
				return
			}
			_, err = conn.Prep("PRAGMA foreign_keys = ON;").Step()
			if err != nil {
				return
			}

			return
		},
	})
}

func DrainPool() {
	if Pool != nil {
		Pool.Close()
		Pool = nil
	}

	if MigrationsPool != nil {
		MigrationsPool.Close()
		MigrationsPool = nil
	}
}

type Param struct {
	Name  string
	Value any
}

// ExecuteSQL executes a SQL query on the given connection. Internal use only.
func ExecuteSQL(ctx context.Context, query string, conn *sqlite.Conn, output bool, params ...Param) (*Rows, error) {
	return CaptureChanges(query, conn, output, params...)
}
