package main

import (
	"atlas-db/atlas"
	"bufio"
	"context"
	"fmt"
	"os"
	"strings"
	"zombiezen.com/go/sqlite"
	"zombiezen.com/go/sqlite/sqlitemigration"
)

type authPrinter struct {
	conn *sqlite.Conn
}

type tableType string

const (
	localTable    tableType = "local"
	regionalTable tableType = "regional"
	globalTable   tableType = "globalTable"
)

func (a authPrinter) Authorize(action sqlite.Action) sqlite.AuthResult {
	op := action.String()
	op = strings.Fields(op)[0]

	switch op {
	case "SQLITE_CREATE_TABLE":
		// todo: handle this here?
	}

	return sqlite.AuthResultOK
}

var pool *sqlitemigration.Pool

func main() {
	fmt.Println("Simple REPL for Testing SQLite")
	fmt.Println("Type 'exit' to quit")

	filename := "/tmp/atlas.db"

	if len(os.Args) > 1 {
		filename = os.Args[1]
	}

	// Initialize SQLite
	pool = sqlitemigration.NewPool(filename, sqlitemigration.Schema{
		Migrations: []string{
			"create table __migrations (id integer not null primary key, command  TEXT not null, executed INT default 0 not null);",
			"create table __tables (id integer not null primary key, table_name text not null, is_local int not null default 0, is_regional int not null default 0);",
			"create table __regions (id integer not null primary key, name text not null);",
			"create table __nodes (id integer not null primary key, address text not null, port int not null, region_id int not null constraint __nodes___regions_id_fk references __regions);",
			"create table __table_nodes (id integer not null primary key, owner integer not null, table_id integer not null constraint __table_nodes___table_config_id_fk references __tables, node_id integer not null constraint __table_nodes___nodes_id_fk references __nodes);",
		},
	}, sqlitemigration.Options{
		Flags:    sqlite.OpenReadWrite | sqlite.OpenCreate | sqlite.OpenWAL,
		PoolSize: 10,
		PrepareConn: func(conn *sqlite.Conn) (err error) {
			err = conn.SetAuthorizer(authPrinter{})
			executeSQL("PRAGMA journal_mode=WAL;", conn, false)
			if err != nil {
				return err
			}
			err = atlas.InitializeSession(conn)
			return
		},
	})
	defer pool.Close()

	// Start REPL loop
	repl(pool)
}

func repl(conn *sqlitemigration.Pool) {
	reader := bufio.NewReader(os.Stdin)
	db, _ := conn.Take(context.Background())
	for {
		fmt.Print(">> ")
		input, err := reader.ReadString('\n')
		if err != nil {
			fmt.Println("Error reading input:", err)
			continue
		}
		input = strings.TrimSpace(input)
		if input == "exit" {
			break
		}
		processInput(input, db)
	}
}

func processInput(input string, conn *sqlite.Conn) {
	executeSQL(input, conn, true)
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
	conn, err := pool.Take(context.Background())
	if err != nil {
		return err
	}
	defer pool.Put(conn)

	_, err = conn.Prep("begin").Step()
	if err != nil {
		return err
	}

	stmt := conn.Prep("insert into __migrations (command, executed) values (:command, 1)")
	stmt.SetText(":command", query)
	_, err = stmt.Step()
	if err != nil {
		return err
	}

	stmt = conn.Prep("insert into __table_config (table_name, is_owner, mode) values (:table_name, 1, :mode)")
	stmt.SetText(":table_name", table)
	stmt.SetText(":mode", string(kind))
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

func executeSQL(query string, conn *sqlite.Conn, output bool) {
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
			return
		}
	} else if strings.HasPrefix(normalized, "CREATE REGIONAL TABLE") {
		// we are creating a regional table to be persisted, and the schema will be replicated
		query = "CREATE TABLE" + query[len("CREATE REGIONAL TABLE"):]
		query = replaceCommand(query, "CREATE REGIONAL TABLE", "CREATE TABLE")
		err := replicateCommand(query, parts[3], regionalTable)
		if err != nil {
			fmt.Println("Error replicating command:", err)
			return
		}
	} else if strings.HasPrefix(normalized, "CREATE TABLE") {
		// we are creating a table to be replicated, but the schema will be replicated
		err := replicateCommand(query, parts[2], globalTable)
		if err != nil {
			fmt.Println("Error replicating command:", err)
			return
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

	switch query {
	case "WRITE_PATCH":
		atlas.WritePatchset()
	case "APPLY_PATCH":
		atlas.ApplyPatchset(conn)
	default:
		atlas.CaptureChanges(query, conn, output)
	}
}
