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
				defer st.Finalize()
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

	MigrationsPool = sqlitemigration.NewPool(options.MetaFilename, sqlitemigration.Schema{
		Migrations: strings.Split(migrations, ";"),
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

// replaceCommand replaces command in query with newPrefix.
func replaceCommand(query, command, newPrefix string) string {
	fields := strings.Fields(command)
	if len(fields) == 0 {
		return query
	}

	for _, field := range fields {
		// consume the field from the query
		endpos := strings.Index(strings.ToUpper(query), strings.ToUpper(field)) + len(field)
		query = query[endpos:]
	}

	return newPrefix + query
}

func removeCommand(query string, num int) string {
	fields := strings.Fields(query)
	// count whitespace at the end of string
	whitespace := 0
	for i := len(query) - 1; i >= 0; i-- {
		if query[i] == ' ' {
			whitespace++
		} else {
			break
		}
	}
	if whitespace >= 2 {
		fields = append(fields, strings.Repeat(" ", whitespace-1))
	}

	for i := 0; i < num; i++ {
		endpos := strings.Index(query, fields[i]) + len(fields[i])
		query = query[endpos:]
	}

	return query[1:]
}

type Param struct {
	Name  string
	Value interface{}
}

// ExecuteSQL executes a SQL query on the given connection. Internal use only.
func ExecuteSQL(ctx context.Context, query string, conn *sqlite.Conn, output bool, params ...Param) (*Rows, error) {
	return CaptureChanges(query, conn, output, params...)
}
