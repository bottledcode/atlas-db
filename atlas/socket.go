package atlas

import (
	"bufio"
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"go.uber.org/zap"
	"net"
	"strconv"
	"strings"
	"zombiezen.com/go/sqlite"
)

func ServeSocket(ctx context.Context) (func() error, error) {
	// create the unix socket
	ln, err := net.Listen("unix", CurrentOptions.SocketPath)
	if err != nil {
		return nil, err
	}

	// start the server
	go func() {
		done := ctx.Done()
		for {
			select {
			case <-done:
				return
			default:
				conn, err := ln.Accept()
				if err != nil {
					Logger.Error("Error accepting connection", zap.Error(err))
					continue
				}
				go handleConnection(conn)
			}
		}
	}()

	return func() error {
		return ln.Close()
	}, nil
}

const EOL = "\r\n"

type ErrorCode string

const (
	OK      ErrorCode = "OK"
	Info              = "INFO"
	Warning           = "WARN"
	Fatal             = "ERROR"
)

type commandString struct {
	normalized string
	parts      []string
	raw        string
	rawParts   []string
}

func commandFromString(command string) *commandString {
	normalized := strings.ToUpper(command)
	parts := strings.Fields(normalized)
	normalized = strings.Join(parts, " ")
	rawParts := strings.Fields(command)

	// count whitespace at the end of string
	whitespace := 0
	for i := len(command) - 1; i >= 0; i-- {
		if command[i] == ' ' {
			whitespace++
		} else {
			break
		}
	}
	if whitespace >= 2 {
		parts = append(parts, strings.Repeat(" ", whitespace-1))
		rawParts = append(rawParts, strings.Repeat(" ", whitespace-1))
	}

	return &commandString{
		normalized: normalized,
		parts:      parts,
		raw:        command,
		rawParts:   rawParts,
	}
}

func (c *commandString) validate(expected int) error {
	if len(c.parts) < expected {
		return errors.New(c.raw + " expects " + strconv.Itoa(expected) + " arguments")
	}
	return nil
}

func (c *commandString) validateExact(expected int) error {
	if len(c.parts) != expected {
		return errors.New(c.raw + " expects exactly " + strconv.Itoa(expected) + " arguments")
	}
	return nil
}

func (c *commandString) removeCommand(start int) *commandString {
	str := removeCommand(c.raw, start)
	return commandFromString(str)
}

func (c *commandString) selectCommand(k int) string {
	return c.rawParts[k]
}

func (c *commandString) selectNormalizedCommand(k int) string {
	return c.parts[k]
}

func (c *commandString) replaceCommand(original, new string) *commandString {
	str := replaceCommand(c.raw, original, new)
	return commandFromString(str)
}

var emptyCommandString *commandString = &commandString{}

func handleConnection(conn net.Conn) {
	defer conn.Close()

	reader := bufio.NewReader(conn)
	writer := bufio.NewWriter(conn)
	var commandBuilder strings.Builder
	inTransaction := false
	var sql *sqlite.Conn
	ctx := context.Background()
	hasFatalled := false

	var writeMessage func(msg string)

	writeMessage = func(msg string) {
		n, err := writer.WriteString(msg + EOL)
		if err != nil {
			// todo: handle full buffer?
			Logger.Error("Error writing to connection", zap.Error(err))
		}
		if n < len(msg) {
			writeMessage(msg[n:])
		}
	}

	writeError := func(code ErrorCode, err error) {
		writeMessage("ERROR " + string(code) + " " + err.Error())
		_ = writer.Flush()
	}

	writeOk := func(code ErrorCode) {
		writeMessage(string(code))
		_ = writer.Flush()
	}

	connect := func() {
		if sql == nil {
			CreatePool(CurrentOptions)
			var err error
			sql, err = Pool.Take(ctx)
			if err != nil {
				Logger.Error("Error taking connection from pool", zap.Error(err))
				writeError(Fatal, err)
				hasFatalled = true
				return
			}
		}
	}

	maybeStartTransaction := func(ctx context.Context, command *commandString) context.Context {
		if !inTransaction {
			connect()

			if command == emptyCommandString {
				command = commandFromString("BEGIN")
			}

			_, err := ExecuteSQL(ctx, command.raw, sql, false)
			if err != nil {
				Logger.Error("Error starting transaction", zap.Error(err))
				writeError(Fatal, err)
				hasFatalled = true
				return ctx
			}

			ctx, err = InitializeSession(ctx, sql, "atlas")
			if err != nil {
				Logger.Error("Error initializing session", zap.Error(err))
				writeError(Fatal, err)
				hasFatalled = true
				return ctx
			}

			inTransaction = true
		}
		return ctx
	}

	consumeLine := func() string {
		for {
			n, err := reader.ReadString('\n')
			if err != nil {
				Logger.Error("Error reading from connection", zap.Error(err))
				hasFatalled = true
				return ""
			}
			commandBuilder.WriteString(n)

			// consume a command
			if command, next, found := strings.Cut(commandBuilder.String(), "\r\n"); found {
				commandBuilder.Reset()
				commandBuilder.Write([]byte(next))
				command = strings.TrimSpace(command)
				return command
			}
		}
	}

	executeQuery := func(stmt *sqlite.Stmt) {
		rowNum := 0

		// write out the column names
		writeMessage("META COLUMN_COUNT" + strconv.Itoa(stmt.ColumnCount()))
		for i := 0; i < stmt.ColumnCount(); i++ {
			writeMessage("META COLUMN_NAME " + strconv.Itoa(i) + stmt.ColumnName(i))
		}

		for {
			hasRow, err := stmt.Step()
			if err != nil {
				writeError(Warning, err)
			}
			if !hasRow {
				break
			}
			rowNum += 1
			cols := stmt.ColumnCount()
			r := "ROW " + strconv.Itoa(rowNum)
			for i := 0; i < cols; i++ {
				switch stmt.ColumnType(i) {
				case sqlite.TypeText:
					writeMessage(r + " TEXT " + stmt.ColumnText(i))
				case sqlite.TypeInteger:
					writeMessage(r + " INTEGER " + strconv.FormatInt(stmt.ColumnInt64(i), 10))
				case sqlite.TypeFloat:
					writeMessage(r + " FLOAT " + strconv.FormatFloat(stmt.ColumnFloat(i), 'f', -1, 64))
				case sqlite.TypeNull:
					writeMessage(r + " NULL")
				case sqlite.TypeBlob:
					writeMessage(r + " BLOB")
				}
			}
		}
		writeMessage("META LAST_INSERT_ID" + strconv.FormatInt(sql.LastInsertRowID(), 10))
		writeMessage("META ROWS_AFFECTED" + strconv.Itoa(sql.Changes()))

		err := stmt.ClearBindings()
		if err != nil {
			writeError(Warning, err)
		}
	}

	stmts := make(map[string]*sqlite.Stmt)
	defer func() {
		for _, stmt := range stmts {
			_ = stmt.Finalize()
		}
	}()

	defer func() {
		sess := GetCurrentSession(ctx)
		if sess != nil {
			sess.Delete()
		}
	}()

	for {
		command := commandFromString(consumeLine())
		if len(command.parts) == 0 {
			if hasFatalled {
				break
			}
			continue
		}

		switch command.parts[0] {
		case "PREPARE":
			ctx = maybeStartTransaction(ctx, emptyCommandString)
			if hasFatalled {
				break
			}
			if err := command.validate(3); err != nil {
				writeError(Warning, err)
				continue
			}
			id := command.selectNormalizedCommand(1)
			sqlCommand := command.removeCommand(2)
			stmt, err := sql.Prepare(sqlCommand.raw)
			if err != nil {
				writeError(Warning, err)
				continue
			}
			stmts[id] = stmt
			writeOk(OK)

		case "EXECUTE":
			ctx = maybeStartTransaction(ctx, emptyCommandString)
			if hasFatalled {
				break
			}
			if err := command.validate(2); err != nil {
				writeError(Warning, err)
				continue
			}
			id := command.selectNormalizedCommand(1)
			stmt, ok := stmts[id]
			if !ok {
				writeError(Warning, errors.New("No statement with id "+id))
				continue
			}
			executeQuery(stmt)
			writeOk(OK)
		case "QUERY":
			ctx = maybeStartTransaction(ctx, emptyCommandString)
			if hasFatalled {
				break
			}
			if err := command.validate(2); err != nil {
				writeError(Warning, err)
				continue
			}
			sqlCommand := command.removeCommand(1)
			stmt, err := sql.Prepare(sqlCommand.raw)
			if err != nil {
				writeError(Warning, err)
				continue
			}
			executeQuery(stmt)
			err = stmt.Finalize()
			if err != nil {
				writeError(Warning, err)
				continue
			}
			writeOk(OK)
		case "FINALIZE":
			if err := command.validate(2); err != nil {
				writeError(Warning, err)
				continue
			}
			id := command.selectNormalizedCommand(1)
			stmt, ok := stmts[id]
			if !ok {
				writeError(Warning, errors.New("No statement with id "+id))
				continue
			}
			err := stmt.Finalize()
			if err != nil {
				writeError(Warning, err)
				continue
			}
			delete(stmts, id)
			writeOk(OK)
		case "BIND":
			if err := command.validate(4); err != nil {
				writeError(Warning, err)
				continue
			}
			id := command.selectNormalizedCommand(1)
			stmt, ok := stmts[id]
			if !ok {
				writeError(Warning, errors.New("No statement with id "+id))
				continue
			}
			param := command.selectCommand(2)
			// check if numeric
			if _, err := strconv.Atoi(param); err == nil {
				i, _ := strconv.Atoi(param)
				switch command.selectNormalizedCommand(3) {
				case "TEXT":
					stmt.BindText(i, command.removeCommand(4).raw)
				case "INT":
					fallthrough
				case "INTEGER":
					v, err := strconv.ParseInt(command.removeCommand(4).raw, 10, 64)
					if err != nil {
						writeError(Warning, err)
						continue
					}
					stmt.BindInt64(i, v)
				case "FLOAT":
					v, err := strconv.ParseFloat(command.removeCommand(4).raw, 64)
					if err != nil {
						writeError(Warning, err)
						continue
					}
					stmt.BindFloat(i, v)
				case "NULL":
					stmt.BindNull(i)
				case "BLOB":
					size := command.selectCommand(4)
					v, err := strconv.Atoi(size)
					if err != nil {
						writeError(Warning, fmt.Errorf("invalid size %s", size))
						continue
					}
					if v == 0 {
						stmt.BindZeroBlob(i, 0)
						continue
					}
					blobBytes, err := base64.StdEncoding.DecodeString(consumeLine())
					if err != nil {
						writeError(Warning, err)
						continue
					}
					if len(blobBytes) != v {
						writeError(Warning, fmt.Errorf("expected %d bytes, got %d", v, len(blobBytes)))
						continue
					}
					stmt.BindBytes(i, blobBytes)
				}
			} else {
				switch command.selectNormalizedCommand(3) {
				case "TEXT":
					stmt.SetText(param, command.removeCommand(4).raw)
				case "INT":
					fallthrough
				case "INTEGER":
					v, err := strconv.ParseInt(command.removeCommand(4).raw, 10, 64)
					if err != nil {
						writeError(Warning, err)
						continue
					}
					stmt.SetInt64(param, v)
				case "FLOAT":
					v, err := strconv.ParseFloat(command.removeCommand(4).raw, 64)
					if err != nil {
						writeError(Warning, err)
						continue
					}
					stmt.SetFloat(param, v)
				case "NULL":
					stmt.SetNull(param)
				case "BLOB":
					size := command.selectNormalizedCommand(4)
					v, err := strconv.Atoi(size)
					if err != nil {
						writeError(Warning, fmt.Errorf("invalid size %s", size))
						continue
					}
					if v == 0 {
						stmt.SetZeroBlob(param, 0)
						continue
					}
					blobBytes, err := base64.StdEncoding.DecodeString(consumeLine())
					if err != nil {
						writeError(Warning, err)
						continue
					}
					if len(blobBytes) != v {
						writeError(Warning, fmt.Errorf("expected %d bytes, got %d", v, len(blobBytes)))
						continue
					}
					stmt.SetBytes(param, blobBytes)
				}
				writeOk(OK)
			}
		case "BEGIN":
			if inTransaction {
				writeOk(OK)
				continue
			}
			if err := command.validate(1); err != nil {
				writeError(Warning, err)
				continue
			}
			ctx = maybeStartTransaction(ctx, command)
			if hasFatalled {
				break
			}
			writeOk(OK)
		case "COMMIT":
			if !inTransaction {
				writeError(Fatal, errors.New("no transaction to commit"))
				hasFatalled = true
				break
			}
			if err := command.validateExact(1); err != nil {
				writeError(Warning, err)
				continue
			}
			_, err := ExecuteSQL(ctx, "COMMIT", sql, false)
			if err != nil {
				writeError(Fatal, err)
				hasFatalled = true
				break
			}
			inTransaction = false

			// todo: capture session changes

			writeOk(OK)
		case "ROLLBACK":
			if !inTransaction {
				writeError(Fatal, errors.New("no transaction to rollback"))
				hasFatalled = true
				break
			}
			if err := command.validate(3); err != nil {
				// we are probably executing a savepoint
				if command.selectNormalizedCommand(1) == "TO" {
					_, err := ExecuteSQL(ctx, "ROLLBACK TO "+command.selectCommand(3), sql, false)
					if err != nil {
						writeError(Fatal, err)
						hasFatalled = true
						break
					}
					writeOk(OK)
					break
				} else {
					writeError(Warning, err)
					continue
				}
			}

			_, err := ExecuteSQL(ctx, "ROLLBACK", sql, false)
			if err != nil {
				writeError(Fatal, err)
				hasFatalled = true
				break
			}
			inTransaction = false
			// reset the session
			GetCurrentSession(ctx).Delete()

			writeOk(OK)
		case "SAVEPOINT":
			ctx = maybeStartTransaction(ctx, emptyCommandString)
			if err := command.validate(2); err != nil {
				writeError(Warning, err)
				continue
			}
			name := command.selectCommand(1)
			_, err := ExecuteSQL(ctx, "SAVEPOINT "+name, sql, false)
			if err != nil {
				writeError(Fatal, err)
				hasFatalled = true
				break
			}
			writeOk(OK)

		case "RELEASE":
			if err := command.validate(2); err != nil {
				writeError(Warning, err)
				continue
			}
			name := command.selectCommand(1)
			_, err := ExecuteSQL(ctx, "RELEASE "+name, sql, false)
			if err != nil {
				writeError(Fatal, err)
				hasFatalled = true
				break
			}
			writeOk(OK)
		default:
			writeError(Warning, errors.New("Unknown command "+command.selectNormalizedCommand(0)))
		}

		if hasFatalled {
			break
		}
	}
}
