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

	maybeStartTransaction := func(ctx context.Context, command string) context.Context {
		if !inTransaction {
			connect()

			if command == "" {
				command = "BEGIN"
			}

			_, err := ExecuteSQL(ctx, command, sql, false)
			if err != nil {
				Logger.Error("Error starting transaction", zap.Error(err))
				writeError(Fatal, err)
				hasFatalled = true
				return ctx
			}

			ctx, err = InitializeSession(ctx, sql)
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

	validate := func(command string, parts []string, expected int) bool {
		if len(parts) < expected {
			writeError(Warning, errors.New(command+" expects "+strconv.Itoa(expected)+" arguments"))
			return false
		}
		return true
	}

	executeQuery := func(stmt *sqlite.Stmt) {
		rowNum := 0
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
		command := consumeLine()
		normalized := strings.ToUpper(command)
		parts := strings.Fields(normalized)
		normalized = strings.Join(parts, " ")
		if len(parts) == 0 {
			if hasFatalled {
				break
			}
			continue
		}

		switch parts[0] {
		case "PREPARE":
			ctx = maybeStartTransaction(ctx, "")
			if hasFatalled {
				break
			}
			if validate(command, parts, 3) {
				id := parts[1]
				sqlStr := removeCommand(command, 2)
				stmt, err := sql.Prepare(sqlStr)
				if err != nil {
					writeError(Warning, err)
					continue
				}
				stmts[id] = stmt
				writeOk(OK)
			}
		case "EXECUTE":
			ctx = maybeStartTransaction(ctx, "")
			if hasFatalled {
				break
			}
			if validate(command, parts, 2) {
				id := parts[1]
				stmt, ok := stmts[id]
				if !ok {
					writeError(Warning, errors.New("No statement with id "+id))
					continue
				}
				executeQuery(stmt)
				writeOk(OK)
			}
		case "QUERY":
			ctx = maybeStartTransaction(ctx, "")
			if hasFatalled {
				break
			}
			if validate(command, parts, 2) {
				sqlStr := removeCommand(command, 1)
				stmt, err := sql.Prepare(sqlStr)
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
			}
		case "FINALIZE":
			if validate(command, parts, 2) {
				id := parts[1]
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
			}
		case "BIND":
			if validate(command, parts, 4) {
				id := parts[1]
				stmt, ok := stmts[id]
				if !ok {
					writeError(Warning, errors.New("No statement with id "+id))
					continue
				}
				param := selectCommand(command, 2)
				// check if numeric
				if _, err := strconv.Atoi(param); err == nil {
					i, _ := strconv.Atoi(param)
					switch parts[3] {
					case "TEXT":
						stmt.BindText(i, removeCommand(command, 4))
					case "INT":
						fallthrough
					case "INTEGER":
						v, err := strconv.ParseInt(removeCommand(command, 4), 10, 64)
						if err != nil {
							writeError(Warning, err)
							continue
						}
						stmt.BindInt64(i, v)
					case "FLOAT":
						v, err := strconv.ParseFloat(removeCommand(command, 4), 64)
						if err != nil {
							writeError(Warning, err)
							continue
						}
						stmt.BindFloat(i, v)
					case "NULL":
						stmt.BindNull(i)
					case "BLOB":
						size := parts[4]
						v, err := strconv.Atoi(size)
						if err != nil {
							writeError(Warning, fmt.Errorf("Invalid size %s", size))
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
							writeError(Warning, fmt.Errorf("Expected %d bytes, got %d", v, len(blobBytes)))
							continue
						}
						stmt.BindBytes(i, blobBytes)
					}
				} else {
					switch parts[3] {
					case "TEXT":
						stmt.SetText(param, removeCommand(command, 4))
					case "INT":
						fallthrough
					case "INTEGER":
						v, err := strconv.ParseInt(removeCommand(command, 4), 10, 64)
						if err != nil {
							writeError(Warning, err)
							continue
						}
						stmt.SetInt64(param, v)
					case "FLOAT":
						v, err := strconv.ParseFloat(removeCommand(command, 4), 64)
						if err != nil {
							writeError(Warning, err)
							continue
						}
						stmt.SetFloat(param, v)
					case "NULL":
						stmt.SetNull(param)
					case "BLOB":
						size := parts[4]
						v, err := strconv.Atoi(size)
						if err != nil {
							writeError(Warning, fmt.Errorf("Invalid size %s", size))
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
							writeError(Warning, fmt.Errorf("Expected %d bytes, got %d", v, len(blobBytes)))
							continue
						}
						stmt.SetBytes(param, blobBytes)
					}
				}
				writeOk(OK)
			}
		case "BEGIN":
			if inTransaction {
				writeOk(OK)
				continue
			}
			if strings.Contains(command, ";") {
				writeError(Warning, errors.New("BEGIN does not take arguments"))
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
			if len(parts) >= 2 && parts[1] == "TO" {
				if validate(command, parts, 3) {
					_, err := ExecuteSQL(ctx, "ROLLBACK TO "+parts[2], sql, false)
					if err != nil {
						writeError(Fatal, err)
						hasFatalled = true
						break
					}
					writeOk(OK)
					break
				}
			}

			if validate(command, parts, 1) {
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
			}
		case "SAVEPOINT":
			ctx = maybeStartTransaction(ctx, "")
			if validate(command, parts, 2) {
				name := parts[1]
				_, err := ExecuteSQL(ctx, "SAVEPOINT "+name, sql, false)
				if err != nil {
					writeError(Fatal, err)
					hasFatalled = true
					break
				}
				writeOk(OK)
			}
		case "RELEASE":
			if validate(command, parts, 2) {
				name := parts[1]
				_, err := ExecuteSQL(ctx, "RELEASE "+name, sql, false)
				if err != nil {
					writeError(Fatal, err)
					hasFatalled = true
					break
				}
				writeOk(OK)
			}
		default:
			writeError(Warning, errors.New("Unknown command "+parts[0]))
		}

		if hasFatalled {
			break
		}
	}
}
