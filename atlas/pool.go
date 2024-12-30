package atlas

import (
	"context"
	_ "embed"
	"runtime"
	"strings"
	"zombiezen.com/go/sqlite"
	"zombiezen.com/go/sqlite/sqlitemigration"
)

//go:embed bootstrap/migrations.sql
var migrations string

var Pool *sqlitemigration.Pool
var MigrationsPool *sqlitemigration.Pool

// CreatePool creates a new connection pool for the database and the migrations database.
func CreatePool(options *Options) {
	if Pool != nil {
		return
	}

	Pool = sqlitemigration.NewPool(options.DbFilename, sqlitemigration.Schema{
		Migrations: strings.Split(migrations, ";"),
	}, sqlitemigration.Options{
		Flags:    sqlite.OpenReadWrite | sqlite.OpenCreate | sqlite.OpenWAL,
		PoolSize: runtime.NumCPU() * 2,
		PrepareConn: func(conn *sqlite.Conn) (err error) {
			// todo: err = conn.SetAuthorizer(authPrinter{})
			return
		},
	})

	MigrationsPool = sqlitemigration.NewPool(options.MetaFilename, sqlitemigration.Schema{
		Migrations: strings.Split(migrations, ";"),
	}, sqlitemigration.Options{
		Flags:    sqlite.OpenReadWrite | sqlite.OpenCreate | sqlite.OpenWAL,
		PoolSize: 10,
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
