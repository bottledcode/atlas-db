package main

import (
	"atlas-db/atlas"
	"bufio"
	"context"
	_ "embed"
	"fmt"
	"os"
	"strings"
	"zombiezen.com/go/sqlite"
	"zombiezen.com/go/sqlite/sqlitemigration"
)

//go:embed meta/migrations.sql
var migrations string

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

type options struct {
	dbFilename       string
	metaFilename     string
	doReset          bool
	bootstrapConnect string
}

var currentOptions *options

func parseOptions(opts []string, defaults *options) (opt *options) {
	if len(opts) == 0 {
		return defaults
	}

	opt = defaults

	for i, field := range opts {
		value := ""
		if strings.Contains(field, "=") {
			value = strings.Split(field, "=")[1]
			field = strings.Split(field, "=")[0]
		} else {
			if i+1 < len(opts) {
				value = opts[i+1]
			}
		}
		if strings.HasPrefix(field, "--") {
			switch field {
			case "--db":
				opt.dbFilename = value
			case "--meta":
				opt.metaFilename = value
			case "--reset":
				opt.doReset = true
			case "--connect":
				opt.bootstrapConnect = value
			}
		}
	}

	return
}

func main() {
	fmt.Println("Simple REPL for Testing SQLite")
	fmt.Println("Type 'exit' to quit")

	currentOptions = parseOptions(os.Args[1:], &options{
		dbFilename:   "/tmp/atlas.db",
		metaFilename: "/tmp/atlas.meta",
		doReset:      false,
	})

	if currentOptions.doReset {
		os.Remove(currentOptions.dbFilename)
		os.Remove(currentOptions.metaFilename)
		fmt.Println("Database reset")
		return
	}

	// Initialize Atlas
	if currentOptions.bootstrapConnect != "" {
		// we are connecting to a bootstrap server
		
	}

	if _, err := os.Stat(currentOptions.metaFilename); err != nil {
		f, err := os.Create(currentOptions.metaFilename)
		if err != nil {
			fmt.Println("Error creating meta database:", err)
			return
		}
		f.Close()
	}

	pool = sqlitemigration.NewPool(currentOptions.dbFilename, sqlitemigration.Schema{
		Migrations: strings.Split(migrations, ";"),
	}, sqlitemigration.Options{
		Flags:    sqlite.OpenReadWrite | sqlite.OpenCreate | sqlite.OpenWAL,
		PoolSize: 10,
		PrepareConn: func(conn *sqlite.Conn) (err error) {
			err = conn.SetAuthorizer(authPrinter{})
			executeSQL("attach database '"+currentOptions.metaFilename+"' as atlas;", conn, false)
			executeSQL("PRAGMA journal_mode=WAL;", conn, false)
			if err != nil {
				return err
			}
			err = atlas.InitializeSession(conn)
			if err != nil {
				fmt.Println("Error initializing session:", err)
			}
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

	isLocal := false
	isRegional := false
	switch kind {
	case localTable:
		isLocal = true
	case regionalTable:
		isRegional = true
	}

	stmt := conn.Prep("insert into atlas.tables (table_name, is_local, is_regional) values (:table_name, :is_local, :is_regional)")
	stmt.SetText(":table_name", table)
	stmt.SetBool(":is_local", isLocal)
	stmt.SetBool(":is_regional", isRegional)
	_, err = stmt.Step()
	if err != nil {
		return err
	}
	tableId := conn.LastInsertRowID()

	stmt = conn.Prep("insert into atlas.migrations (command, executed, table_id) values (:command, 1, :table_id)")
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

	switch normalized {
	case "WRITE_PATCH":
		atlas.WritePatchset()
	case "APPLY_PATCH":
		atlas.ApplyPatchset(conn)
	default:
		atlas.CaptureChanges(query, conn, output)
	}
}
