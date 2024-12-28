package atlas

import (
	"context"
	_ "embed"
	"fmt"
	"os"
	"runtime"
	"strings"
	"zombiezen.com/go/sqlite"
	"zombiezen.com/go/sqlite/sqlitemigration"
)

//go:embed bootstrap/migrations.sql
var migrations string

var Pool *sqlitemigration.Pool
var MigrationsPool *sqlitemigration.Pool

type tableType string

const (
	localTable    tableType = "local"
	regionalTable tableType = "regional"
	globalTable   tableType = "globalTable"
)

func CreatePool() {
	if Pool != nil {
		return
	}

	Pool = sqlitemigration.NewPool(CurrentOptions.DbFilename, sqlitemigration.Schema{
		Migrations: strings.Split(migrations, ";"),
	}, sqlitemigration.Options{
		Flags:    sqlite.OpenReadWrite | sqlite.OpenCreate | sqlite.OpenWAL,
		PoolSize: runtime.NumCPU() * 2,
		PrepareConn: func(conn *sqlite.Conn) (err error) {
			// todo: err = conn.SetAuthorizer(authPrinter{})
			return
		},
	})

	MigrationsPool = sqlitemigration.NewPool(CurrentOptions.MetaFilename, sqlitemigration.Schema{
		Migrations: strings.Split(migrations, ";"),
	}, sqlitemigration.Options{
		Flags:    sqlite.OpenReadWrite | sqlite.OpenCreate | sqlite.OpenWAL,
		PoolSize: 10,
	})
}

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

func replicateCommand(query string, table string, kind tableType) error {
	// todo: actually replicate
	conn, err := MigrationsPool.Take(context.Background())
	if err != nil {
		return err
	}
	defer MigrationsPool.Put(conn)

	_, err = conn.Prep("begin").Step()
	if err != nil {
		return err
	}

	isLocal := false
	isRegional := false
	switch kind {
	case localTable:
		isLocal = true
	case regionalTable:
		isRegional = true
	}

	stmt := conn.Prep("insert into tables (table_name, is_local, is_regional) values (:table_name, :is_local, :is_regional)")
	stmt.SetText(":table_name", table)
	stmt.SetBool(":is_local", isLocal)
	stmt.SetBool(":is_regional", isRegional)
	_, err = stmt.Step()
	if err != nil {
		return err
	}
	tableId := conn.LastInsertRowID()

	stmt = conn.Prep("insert into table_migrations (command, executed, table_id) values (:command, 1, :table_id)")
	stmt.SetText(":command", query)
	stmt.SetInt64(":table_id", tableId)
	_, err = stmt.Step()
	if err != nil {
		return err
	}

	_, err = conn.Prep("commit").Step()
	if err != nil {
		return err
	}

	return nil
}

func ExecuteSQL(ctx context.Context, query string, conn *sqlite.Conn, output bool) (*Rows, error) {
	// normalize query
	normalized := strings.ToUpper(query)

	// make all whitespace into a single space
	parts := strings.Fields(normalized)
	normalized = strings.Join(parts, " ")

	if strings.HasPrefix(normalized, "CREATE LOCAL TABLE") {
		// we are creating a local table to not be persisted, but the schema will be replicated
		query = replaceCommand(query, "CREATE LOCAL TABLE", "CREATE TABLE")
		err := replicateCommand(query, parts[3], localTable)
		if err != nil {
			fmt.Println("Error replicating command:", err)
			return nil, err
		}
	} else if strings.HasPrefix(normalized, "CREATE REGIONAL TABLE") {
		// we are creating a regional table to be persisted, and the schema will be replicated
		query = "CREATE TABLE" + query[len("CREATE REGIONAL TABLE"):]
		query = replaceCommand(query, "CREATE REGIONAL TABLE", "CREATE TABLE")
		err := replicateCommand(query, parts[3], regionalTable)
		if err != nil {
			fmt.Println("Error replicating command:", err)
			return nil, err
		}
	} else if strings.HasPrefix(normalized, "CREATE TABLE") {
		// we are creating a table to be replicated, but the schema will be replicated
		err := replicateCommand(query, parts[2], globalTable)
		if err != nil {
			fmt.Println("Error replicating command:", err)
			return nil, err
		}
	}

	if strings.HasPrefix(normalized, "ALTER TABLE") {
		table := strings.Fields(query)[2]
		after, _ := strings.CutPrefix(normalized, "ALTER TABLE"+strings.ToUpper(table))
		if strings.HasPrefix(after, "LOCK REGION") {
			// lock a table to a region, it cannot migrate out of the region
		}
		if strings.HasPrefix(after, "UNLOCK REGION") {
			// unlock a table, it can migrate out of the region
		}
		if strings.HasPrefix(after, "MIGRATE REGION") {
			// migrate a table to a different region
		}

		// todo: tables cannot be altered once replicated
	}

	// todo: handle deletes and truncates

	// other special commands?:
	// - ALTER TABLE <table> LOCK REGION <region>[,<region...]: lock a table to a region, it cannot migrate out of the region
	// - ALTER TABLE <table> UNLOCK REGION: unlock a table, it can migrate out of the region
	// - ALTER TABLE <table> MIGRATE REGION <region>: migrate a table to a different region

	switch normalized {
	case "WRITE_PATCH":
		WritePatchset(ctx)
	case "APPLY_PATCH":
		ApplyPatchset(conn)
	case "SERIALIZE":
		data, err := conn.Serialize("atlas")
		if err != nil {
			panic(err)
		}
		f, _ := os.Create("serialized.db")
		f.Write(data)
		f.Close()
		fmt.Println("Serialized and written to file")
	default:
		return CaptureChanges(query, conn, output)
	}

	return nil, nil
}
