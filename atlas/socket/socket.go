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

package socket

import (
	"bufio"
	"bytes"
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"github.com/bottledcode/atlas-db/atlas"
	"github.com/bottledcode/atlas-db/atlas/consensus"
	"go.uber.org/zap"
	"net"
	"os"
	"strconv"
	"strings"
	"zombiezen.com/go/sqlite"
)

func ServeSocket(ctx context.Context) (func() error, error) {
	// create the unix socket
	ln, err := net.Listen("unix", atlas.CurrentOptions.SocketPath)
	if err != nil {
		// try to remove the socket file if it exists
		_ = os.Remove(atlas.CurrentOptions.SocketPath)
		ln, err = net.Listen("unix", atlas.CurrentOptions.SocketPath)
		if err != nil {
			return nil, err
		}
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
					atlas.Logger.Error("Error accepting connection", zap.Error(err))
					continue
				}
				c := &SH{}
				go c.handleConnection(conn, ctx)
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
	Info    ErrorCode = "INFO"
	Warning ErrorCode = "WARN"
	Fatal   ErrorCode = "ERROR"
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

type queryMode int

const (
	normalQueryMode queryMode = iota
	localQueryMode
)

func (qm *queryMode) String() string {
	switch *qm {
	case normalQueryMode:
		return "normal"
	case localQueryMode:
		return "local"
	}
	return "unknown"
}

type SH struct {
	writer         *bufio.Writer
	reader         *bufio.Reader
	sql            *sqlite.Conn
	hasFatalError  bool
	inTransaction  bool
	session        *sqlite.Session
	commandBuilder strings.Builder
}

func (s *SH) writeRawMessage(msg string) error {
	n, err := s.writer.WriteString(msg)
	if err != nil {
		return err
	}
	if n < len(msg) {
		return s.writeRawMessage(msg[n:])
	}
	return nil
}

func (s *SH) writeMessage(msg string) error {
	return s.writeRawMessage(msg + EOL)
}

func (s *SH) writeError(code ErrorCode, err error) error {
	e := s.writeMessage("ERROR " + string(code) + " " + err.Error())
	if e != nil {
		return e
	}
	return s.writer.Flush()
}

func (s *SH) writeOk(code ErrorCode) error {
	err := s.writeMessage(string(code))
	if err != nil {
		return err
	}
	return s.writer.Flush()
}

func (s *SH) connect(ctx context.Context) error {
	if s.sql == nil {
		atlas.CreatePool(atlas.CurrentOptions)
		var err error
		s.sql, err = atlas.Pool.Take(ctx)
		if err != nil {
			s.hasFatalError = true
			atlas.Logger.Error("Error taking connection from pool", zap.Error(err))
			e := s.writeError(Fatal, err)
			return errors.Join(err, e)
		}
	}
	return nil
}

func (s *SH) maybeStartTransaction(ctx context.Context, command *commandString) error {
	if !s.inTransaction {
		err := s.connect(ctx)
		if err != nil {
			return err
		}

		if command == emptyCommandString {
			command = commandFromString("BEGIN")
		}

		_, err = atlas.ExecuteSQL(ctx, command.raw, s.sql, false)
		if err != nil {
			atlas.Logger.Error("Error starting transaction", zap.Error(err))
			e := s.writeError(Fatal, err)
			s.hasFatalError = true
			return errors.Join(err, e)
		}

		s.session, err = atlas.InitializeSession(ctx, s.sql)
		if err != nil {
			atlas.Logger.Error("Error initializing session", zap.Error(err))
			e := s.writeError(Fatal, err)
			s.hasFatalError = true
			return errors.Join(err, e)
		}

		s.inTransaction = true
	}
	return nil
}

func (s *SH) consumeLine() string {
	for {
		n, err := s.reader.ReadString('\n')
		if err != nil {
			atlas.Logger.Error("Error reading from connection", zap.Error(err))
			s.hasFatalError = true
			return ""
		}
		s.commandBuilder.WriteString(n)

		// consume a command
		if command, next, found := strings.Cut(s.commandBuilder.String(), "\r\n"); found {
			s.commandBuilder.Reset()
			s.commandBuilder.Write([]byte(next))
			command = strings.TrimSpace(command)
			return command
		}
	}
}

func (s *SH) syntheticQuery(kv map[string]string) error {
	rowNum := 0

	err := s.writeMessage("META COLUMN_COUNT 2")
	if err != nil {
		return err
	}
	err = s.writeMessage("META COLUMN_NAME 0 key")
	if err != nil {
		return err
	}
	err = s.writeMessage("META COLUMN_NAME 1 value")
	if err != nil {
		return err
	}

	for key, value := range kv {
		rowNum += 1
		err = s.writeMessage("ROW " + strconv.Itoa(rowNum) + " TEXT " + key)
		if err != nil {
			return err
		}
		err = s.writeMessage("ROW " + strconv.Itoa(rowNum) + " TEXT " + value)
		if err != nil {
			return err
		}
	}

	err = s.writeMessage("META LAST_INSERT_ID 0")
	if err != nil {
		return err
	}
	err = s.writeMessage("META ROWS_AFFECTED " + strconv.Itoa(rowNum))
	if err != nil {
		return err
	}

	return nil
}

func (s *SH) executeQuery(stmt *sqlite.Stmt) error {
	rowNum := 0
	err := s.writeMessage("META COLUMN_COUNT " + strconv.Itoa(stmt.ColumnCount()))
	if err != nil {
		return err
	}
	for i := 0; i < stmt.ColumnCount(); i++ {
		err = s.writeMessage("META COLUMN_NAME " + strconv.Itoa(i) + " " + stmt.ColumnName(i))
		if err != nil {
			return err
		}
	}

	for {
		hasRow, err := stmt.Step()
		if err != nil {
			err = s.writeError(Warning, err)
			if err != nil {
				return err
			}
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
				err = s.writeMessage(r + " TEXT " + stmt.ColumnText(i))
			case sqlite.TypeInteger:
				err = s.writeMessage(r + " INT " + strconv.FormatInt(stmt.ColumnInt64(i), 10))
			case sqlite.TypeFloat:
				err = s.writeMessage(r + " FLOAT " + strconv.FormatFloat(stmt.ColumnFloat(i), 'f', -1, 64))
			case sqlite.TypeNull:
				err = s.writeMessage(r + " NULL")
			case sqlite.TypeBlob:
				var b []byte
				ir := stmt.ColumnBytes(i, b)
				err = s.writeMessage(r + " BLOB " + strconv.Itoa(ir) + " " + base64.StdEncoding.EncodeToString(b))
			}
			if err != nil {
				return err
			}
		}
	}
	err = s.writeMessage("META LAST_INSERT_ID " + strconv.FormatInt(s.sql.LastInsertRowID(), 10))
	if err != nil {
		return err
	}
	err = s.writeMessage("META ROWS_AFFECTED " + strconv.Itoa(s.sql.Changes()))
	if err != nil {
		return err
	}

	err = stmt.ClearBindings()
	if err != nil {
		e := s.writeError(Warning, err)
		return errors.Join(err, e)
	}

	return nil
}

func (s *SH) handleConnection(conn net.Conn, ctx context.Context) {
	defer conn.Close()

	s.reader = bufio.NewReader(conn)
	s.writer = bufio.NewWriter(conn)

	qm := normalQueryMode

	commandMigrations := &consensus.SchemaMigration{
		Commands: []string{},
	}
	sessionMigrations := &consensus.DataMigration{
		Session: [][]byte{},
	}
	requiresMigration := false

	stmts := make(map[string]*sqlite.Stmt)
	defer func() {
		for _, stmt := range stmts {
			_ = stmt.Finalize()
		}
	}()

	for {
		command := commandFromString(s.consumeLine())
		if len(command.parts) == 0 {
			if s.hasFatalError {
				break
			}
			continue
		}

		switch command.parts[0] {
		case "PREPARE":
			err := s.maybeStartTransaction(ctx, emptyCommandString)
			if err != nil {
				atlas.Logger.Error("Error starting transaction", zap.Error(err))
				break
			}
			if s.hasFatalError {
				break
			}
			if err = command.validate(3); err != nil {
				e := s.writeError(Warning, err)
				if e != nil {
					atlas.Logger.Error("Error writing error", zap.Error(errors.Join(err, e)))
					break
				}
				continue
			}
			id := command.selectNormalizedCommand(1)
			sqlCommand := command.removeCommand(2)

			if nonAllowedQuery(sqlCommand) {
				err = s.writeError(Warning, errors.New("query not allowed"))
				if err != nil {
					atlas.Logger.Error("Error writing error", zap.Error(err))
					break
				}
				continue
			}

			if !isQueryReadOnly(sqlCommand) {
				// ensure we are running in normal mode
				if qm == localQueryMode {
					err = s.writeError(Warning, errors.New("write query is not allowed in local query mode"))
					if err != nil {
						atlas.Logger.Error("Error writing error", zap.Error(err))
						break
					}
					continue
				}

				// determine if this is a schema changing migration
				if isQueryChangeSchema(sqlCommand) {
					commandMigrations.Commands = append(commandMigrations.Commands, sqlCommand.raw)
					err = maybeWatchTable(ctx, sqlCommand, s.session)
					if err != nil {
						e := s.writeError(Warning, err)
						if e != nil {
							atlas.Logger.Error("Error writing error", zap.Error(errors.Join(err, e)))
							break
						}
						continue
					}
				}
				requiresMigration = true
			}

			stmt, err := s.sql.Prepare(sqlCommand.raw)
			if err != nil {
				e := s.writeError(Warning, err)
				if e != nil {
					atlas.Logger.Error("Error writing error", zap.Error(errors.Join(err, e)))
					break
				}
				continue
			}
			stmts[id] = stmt
			err = s.writeOk(OK)
			if err != nil {
				atlas.Logger.Error("Error writing ok", zap.Error(err))
				break
			}

		case "EXECUTE":
			err := s.maybeStartTransaction(ctx, emptyCommandString)
			if err != nil {
				atlas.Logger.Error("Error starting transaction", zap.Error(err))
				break
			}
			if s.hasFatalError {
				break
			}
			if err = command.validate(2); err != nil {
				e := s.writeError(Warning, err)
				if e != nil {
					atlas.Logger.Error("Error writing error", zap.Error(errors.Join(err, e)))
					break
				}
				continue
			}
			id := command.selectNormalizedCommand(1)
			stmt, ok := stmts[id]
			if !ok {
				err = s.writeError(Warning, errors.New("No statement with id "+id))
				if err != nil {
					atlas.Logger.Error("Error writing error", zap.Error(err))
					break
				}
				continue
			}
			err = s.executeQuery(stmt)
			if err != nil {
				e := s.writeError(Warning, err)
				if e != nil {
					atlas.Logger.Error("Error writing error", zap.Error(errors.Join(err, e)))
					break
				}
				continue
			}
			err = s.writeOk(OK)
			if err != nil {
				atlas.Logger.Error("Error writing ok", zap.Error(err))
				break
			}
		case "QUERY":
			err := s.maybeStartTransaction(ctx, emptyCommandString)
			if err != nil {
				atlas.Logger.Error("Error starting transaction", zap.Error(err))
				break
			}
			if s.hasFatalError {
				break
			}
			if err = command.validate(2); err != nil {
				e := s.writeError(Warning, err)
				if e != nil {
					atlas.Logger.Error("Error writing error", zap.Error(errors.Join(err, e)))
					break
				}
				continue
			}
			sqlCommand := command.removeCommand(1)

			if nonAllowedQuery(sqlCommand) {
				err = s.writeError(Warning, errors.New("query not allowed"))
				if err != nil {
					atlas.Logger.Error("Error writing error", zap.Error(err))
					break
				}
				continue
			}

			if !isQueryReadOnly(sqlCommand) {
				// ensure we are running in normal mode
				if qm == localQueryMode {
					err = s.writeError(Warning, errors.New("write query is not allowed in local query mode"))
					if err != nil {
						atlas.Logger.Error("Error writing error", zap.Error(err))
						break
					}
					continue
				}

				// determine if this is a schema changing migration
				if isQueryChangeSchema(sqlCommand) {
					commandMigrations.Commands = append(commandMigrations.Commands, sqlCommand.raw)
					err = maybeWatchTable(ctx, sqlCommand, s.session)
					if err != nil {
						e := s.writeError(Warning, err)
						if e != nil {
							atlas.Logger.Error("Error writing error", zap.Error(errors.Join(err, e)))
							break
						}
						continue
					}
				}
				requiresMigration = true
			}

			stmt, err := s.sql.Prepare(sqlCommand.raw)
			if err != nil {
				e := s.writeError(Warning, err)
				if e != nil {
					atlas.Logger.Error("Error writing error", zap.Error(errors.Join(err, e)))
					break
				}
				continue
			}
			err = s.executeQuery(stmt)
			if err != nil {
				e := s.writeError(Warning, err)
				if e != nil {
					atlas.Logger.Error("Error writing error", zap.Error(errors.Join(err, e)))
					break
				}
				continue
			}
			err = stmt.Finalize()
			if err != nil {
				e := s.writeError(Warning, err)
				if e != nil {
					atlas.Logger.Error("Error writing error", zap.Error(errors.Join(err, e)))
					break
				}
				continue
			}
			err = s.writeOk(OK)
			if err != nil {
				atlas.Logger.Error("Error writing ok", zap.Error(err))
				break
			}
		case "FINALIZE":
			if err := command.validate(2); err != nil {
				e := s.writeError(Warning, err)
				if e != nil {
					atlas.Logger.Error("Error writing error", zap.Error(errors.Join(err, e)))
					break
				}
				continue
			}
			id := command.selectNormalizedCommand(1)
			stmt, ok := stmts[id]
			if !ok {
				err := s.writeError(Warning, errors.New("No statement with id "+id))
				if err != nil {
					atlas.Logger.Error("Error writing error", zap.Error(err))
					break
				}
				continue
			}
			err := stmt.Finalize()
			if err != nil {
				e := s.writeError(Warning, err)
				if e != nil {
					atlas.Logger.Error("Error writing error", zap.Error(errors.Join(err, e)))
					break
				}
				continue
			}
			delete(stmts, id)
			err = s.writeOk(OK)
			if err != nil {
				atlas.Logger.Error("Error writing ok", zap.Error(err))
				break
			}
		case "BIND":
			if err := command.validate(4); err != nil {
				e := s.writeError(Warning, err)
				if e != nil {
					atlas.Logger.Error("Error writing error", zap.Error(errors.Join(err, e)))
					break
				}
				continue
			}
			id := command.selectNormalizedCommand(1)
			stmt, ok := stmts[id]
			if !ok {
				err := s.writeError(Warning, errors.New("No statement with id "+id))
				if err != nil {
					atlas.Logger.Error("Error writing error", zap.Error(err))
					break
				}
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
						e := s.writeError(Warning, err)
						if e != nil {
							atlas.Logger.Error("Error writing error", zap.Error(errors.Join(err, e)))
							break
						}
						continue
					}
					stmt.BindInt64(i, v)
				case "FLOAT":
					v, err := strconv.ParseFloat(command.removeCommand(4).raw, 64)
					if err != nil {
						e := s.writeError(Warning, err)
						if e != nil {
							atlas.Logger.Error("Error writing error", zap.Error(errors.Join(err, e)))
							break
						}
						continue
					}
					stmt.BindFloat(i, v)
				case "NULL":
					stmt.BindNull(i)
				case "BLOB":
					size := command.selectCommand(4)
					v, err := strconv.Atoi(size)
					if err != nil {
						e := s.writeError(Warning, fmt.Errorf("invalid size %s", size))
						if e != nil {
							atlas.Logger.Error("Error writing error", zap.Error(errors.Join(err, e)))
							break
						}
						continue
					}
					if v == 0 {
						stmt.BindZeroBlob(i, 0)
						continue
					}
					blobBytes, err := base64.StdEncoding.DecodeString(s.consumeLine())
					if err != nil {
						e := s.writeError(Warning, err)
						if e != nil {
							atlas.Logger.Error("Error writing error", zap.Error(errors.Join(err, e)))
							break
						}
						continue
					}
					if len(blobBytes) != v {
						e := s.writeError(Warning, fmt.Errorf("expected %d bytes, got %d", v, len(blobBytes)))
						if e != nil {
							atlas.Logger.Error("Error writing error", zap.Error(errors.Join(err, e)))
							break
						}
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
						e := s.writeError(Warning, err)
						if e != nil {
							atlas.Logger.Error("Error writing error", zap.Error(errors.Join(err, e)))
							break
						}
						continue
					}
					stmt.SetInt64(param, v)
				case "FLOAT":
					v, err := strconv.ParseFloat(command.removeCommand(4).raw, 64)
					if err != nil {
						e := s.writeError(Warning, err)
						if e != nil {
							atlas.Logger.Error("Error writing error", zap.Error(errors.Join(err, e)))
							break
						}
						continue
					}
					stmt.SetFloat(param, v)
				case "NULL":
					stmt.SetNull(param)
				case "BLOB":
					size := command.selectNormalizedCommand(4)
					v, err := strconv.Atoi(size)
					if err != nil {
						e := s.writeError(Warning, fmt.Errorf("invalid size %s", size))
						if e != nil {
							atlas.Logger.Error("Error writing error", zap.Error(errors.Join(err, e)))
							break
						}
						continue
					}
					if v == 0 {
						stmt.SetZeroBlob(param, 0)
						continue
					}
					blobBytes, err := base64.StdEncoding.DecodeString(s.consumeLine())
					if err != nil {
						e := s.writeError(Warning, err)
						if e != nil {
							atlas.Logger.Error("Error writing error", zap.Error(errors.Join(err, e)))
							break
						}
						continue
					}
					if len(blobBytes) != v {
						err := s.writeError(Warning, fmt.Errorf("expected %d bytes, got %d", v, len(blobBytes)))
						if err != nil {
							atlas.Logger.Error("Error writing error", zap.Error(err))
							break
						}
						continue
					}
					stmt.SetBytes(param, blobBytes)
				}
				err = s.writeOk(OK)
				if err != nil {
					atlas.Logger.Error("Error writing ok", zap.Error(err))
					break
				}
			}
		case "BEGIN":
			if s.inTransaction {
				err := s.writeOk(OK)
				if err != nil {
					atlas.Logger.Error("Error writing ok", zap.Error(err))
					break
				}
				continue
			}
			if err := command.validate(1); err != nil {
				e := s.writeError(Warning, err)
				if e != nil {
					atlas.Logger.Error("Error writing error", zap.Error(errors.Join(err, e)))
					break
				}
				continue
			}
			err := s.maybeStartTransaction(ctx, command)
			if err != nil {
				atlas.Logger.Error("Error starting transaction", zap.Error(err))
				break
			}
			if s.hasFatalError {
				break
			}
			err = s.writeOk(OK)
			if err != nil {
				atlas.Logger.Error("Error writing ok", zap.Error(err))
				break
			}
		case "COMMIT":
			if !s.inTransaction {
				err := s.writeError(Fatal, errors.New("no transaction to commit"))
				if err != nil {
					atlas.Logger.Error("Error writing error", zap.Error(err))
					break
				}
				s.hasFatalError = true
				break
			}
			if err := command.validateExact(1); err != nil {
				e := s.writeError(Warning, err)
				if e != nil {
					atlas.Logger.Error("Error writing error", zap.Error(errors.Join(err, e)))
					break
				}
				continue
			}

			if requiresMigration {
				if s.session == nil {
					err := s.writeError(Fatal, errors.New("no session"))
					if err != nil {
						atlas.Logger.Error("Error writing error", zap.Error(err))
						break
					}
					s.hasFatalError = true
					break
				}
				var sessionData []byte
				sessionWriter := bytes.NewBuffer(sessionData)
				err := s.session.WritePatchset(sessionWriter)
				if err != nil {
					e := s.writeError(Fatal, err)
					if e != nil {
						atlas.Logger.Error("Error writing error", zap.Error(errors.Join(err, e)))
						break
					}
					s.hasFatalError = true
					break
				}
				sessionMigrations.Session = append(sessionMigrations.Session, sessionData)

				// todo: perform a quorum commit
			}

			_, err := atlas.ExecuteSQL(ctx, "COMMIT", s.sql, false)
			if err != nil {
				e := s.writeError(Fatal, err)
				if e != nil {
					atlas.Logger.Error("Error writing error", zap.Error(errors.Join(err, e)))
					break
				}
				s.hasFatalError = true
				break
			}
			s.inTransaction = false

			err = s.writeOk(OK)
			if err != nil {
				atlas.Logger.Error("Error writing ok", zap.Error(err))
				break
			}
		case "ROLLBACK":
			if !s.inTransaction {
				err := s.writeError(Fatal, errors.New("no transaction to rollback"))
				if err != nil {
					atlas.Logger.Error("Error writing error", zap.Error(err))
					break
				}
				s.hasFatalError = true
				break
			}
			if err := command.validate(3); err == nil {
				// we are probably executing a savepoint
				if command.selectNormalizedCommand(1) == "TO" {
					_, err = atlas.ExecuteSQL(ctx, "ROLLBACK TO "+command.selectCommand(3), s.sql, false)
					if err != nil {
						e := s.writeError(Fatal, err)
						if e != nil {
							atlas.Logger.Error("Error writing error", zap.Error(errors.Join(err, e)))
							break
						}
						s.hasFatalError = true
						break
					}
					err = s.writeOk(OK)
					if err != nil {
						atlas.Logger.Error("Error writing ok", zap.Error(err))
						break
					}
					break
				} else {
					err = s.writeError(Warning, err)
					if err != nil {
						atlas.Logger.Error("Error writing error", zap.Error(err))
						break
					}
					continue
				}
			}

			_, err := atlas.ExecuteSQL(ctx, "ROLLBACK", s.sql, false)
			if err != nil {
				e := s.writeError(Fatal, err)
				if e != nil {
					atlas.Logger.Error("Error writing error", zap.Error(errors.Join(err, e)))
					break
				}
				s.hasFatalError = true
				break
			}
			s.inTransaction = false
			// reset the session
			s.session.Delete()
			s.session = nil

			// reset migrations
			commandMigrations = &consensus.SchemaMigration{
				Commands: []string{},
			}
			sessionMigrations = &consensus.DataMigration{
				Session: [][]byte{},
			}

			err = s.writeOk(OK)
			if err != nil {
				atlas.Logger.Error("Error writing ok", zap.Error(err))
				break
			}
		case "SAVEPOINT":
			err := s.maybeStartTransaction(ctx, emptyCommandString)
			if err != nil {
				atlas.Logger.Error("Error starting transaction", zap.Error(err))
				break
			}
			if err = command.validate(2); err != nil {
				e := s.writeError(Warning, err)
				if e != nil {
					atlas.Logger.Error("Error writing error", zap.Error(errors.Join(err, e)))
					break
				}
				continue
			}
			name := command.selectCommand(1)
			_, err = atlas.ExecuteSQL(ctx, "SAVEPOINT "+name, s.sql, false)
			if err != nil {
				e := s.writeError(Fatal, err)
				if e != nil {
					atlas.Logger.Error("Error writing error", zap.Error(errors.Join(err, e)))
					break
				}
				s.hasFatalError = true
				break
			}
			err = s.writeOk(OK)
			if err != nil {
				atlas.Logger.Error("Error writing ok", zap.Error(err))
				break
			}

		case "RELEASE":
			if err := command.validate(2); err != nil {
				e := s.writeError(Warning, err)
				if e != nil {
					atlas.Logger.Error("Error writing error", zap.Error(errors.Join(err, e)))
					break
				}
				continue
			}
			name := command.selectCommand(1)
			_, err := atlas.ExecuteSQL(ctx, "RELEASE "+name, s.sql, false)
			if err != nil {
				e := s.writeError(Fatal, err)
				if e != nil {
					atlas.Logger.Error("Error writing error", zap.Error(errors.Join(err, e)))
					break
				}
				s.hasFatalError = true
				break
			}
			err = s.writeOk(OK)
			if err != nil {
				atlas.Logger.Error("Error writing ok", zap.Error(err))
				break
			}
		case "PRAGMA":
			if command.selectNormalizedCommand(1) == "ATLAS_QUERY_MODE" {
				if err := command.validate(3); err != nil {
					e := s.syntheticQuery(map[string]string{
						"atlas_query_mode": qm.String(),
					})
					if e != nil {
						atlas.Logger.Error("Error writing error", zap.Error(e))
						break
					}
				}
				switch command.selectNormalizedCommand(2) {
				case "NORMAL":
					qm = normalQueryMode
				case "LOCAL":
					qm = localQueryMode
				}
				err := s.writeOk(OK)
				if err != nil {
					atlas.Logger.Error("Error writing ok", zap.Error(err))
					break
				}
				continue
			}
			// todo: prevent certain pragma commands from executing; once we know what they are

			err := s.maybeStartTransaction(ctx, emptyCommandString)
			if err != nil {
				atlas.Logger.Error("Error starting transaction", zap.Error(err))
				break
			}
			if s.hasFatalError {
				break
			}
			stmt, err := s.sql.Prepare(command.raw)
			if err != nil {
				e := s.writeError(Warning, err)
				if e != nil {
					atlas.Logger.Error("Error writing error", zap.Error(errors.Join(err, e)))
					break
				}
				continue
			}
			err = s.executeQuery(stmt)
			if err != nil {
				e := s.writeError(Warning, err)
				if e != nil {
					atlas.Logger.Error("Error writing error", zap.Error(errors.Join(err, e)))
					break
				}
				continue
			}
			err = stmt.Finalize()
			if err != nil {
				e := s.writeError(Warning, err)
				if e != nil {
					atlas.Logger.Error("Error writing error", zap.Error(errors.Join(err, e)))
					break
				}
				continue
			}
			err = s.writeOk(OK)
			if err != nil {
				atlas.Logger.Error("Error writing ok", zap.Error(err))
				break
			}
		default:
			err := s.writeError(Warning, errors.New("Unknown command "+command.selectNormalizedCommand(0)))
			if err != nil {
				atlas.Logger.Error("Error writing error", zap.Error(err))
				break
			}
		}

		if s.hasFatalError {
			break
		}
	}
}
