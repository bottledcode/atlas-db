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
	"context"
	"errors"
	"fmt"
	"github.com/bottledcode/atlas-db/atlas"
	"github.com/bottledcode/atlas-db/atlas/commands"
	"github.com/bottledcode/atlas-db/atlas/consensus"
	"go.uber.org/zap"
	"modernc.org/strutil"
	"net"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"zombiezen.com/go/sqlite"
)

type Socket struct {
	writer        *bufio.Writer
	reader        *bufio.Reader
	sql           *sqlite.Conn
	inTransaction bool
	session       *sqlite.Session
	activeStmts   map[string]*Query
	streams       []*sqlite.Stmt
	principals    []*consensus.Principal
}

func (s *Socket) Cleanup() {
	for _, query := range s.activeStmts {
		err := query.stmt.Finalize()
		if err != nil {
			atlas.Logger.Error("Error closing statement", zap.Error(err))
		}
	}
}

func (s *Socket) writeRawMessage(msg string) error {
	n, err := s.writer.WriteString(msg)
	if err != nil {
		return err
	}
	if n < len(msg) {
		return s.writeRawMessage(msg[n:])
	}
	return nil
}

func (s *Socket) writeMessage(msg string) error {
	return s.writeRawMessage(msg + EOL)
}

func (s *Socket) writeError(code ErrorCode, err error) error {
	e := s.writeMessage("ERROR " + string(code) + " " + err.Error())
	if e != nil {
		return e
	}
	return s.writer.Flush()
}

func (s *Socket) writeOk(code ErrorCode) error {
	err := s.writeMessage(string(code))
	if err != nil {
		return err
	}
	return s.writer.Flush()
}

func (s *Socket) outputMetaHeaders(stmt *sqlite.Stmt) (err error) {
	// output metadata
	columns := stmt.ColumnCount()
	if columns == 0 {
		return nil
	}
	err = s.writeMessage("META COLUMN_COUNT " + strconv.Itoa(columns))
	if err != nil {
		return
	}
	for i := 0; i < columns; i++ {
		name := stmt.ColumnName(i)
		err = s.writeMessage("META COLUMN_NAME " + strconv.Itoa(i) + " " + name)
		if err != nil {
			return
		}
	}

	lastRow := s.sql.LastInsertRowID()
	if lastRow != 0 {
		err = s.writeMessage("META LAST_INSERT_ID " + strconv.FormatInt(lastRow, 10))
		if err != nil {
			return
		}
	}

	affected := s.sql.Changes()
	if affected != 0 {
		err = s.writeMessage("META AFFECTED_ROWS " + strconv.Itoa(affected))
		if err != nil {
			return
		}
	}

	return nil
}

const ProtoVersion = "1.0"

// todo: set during build
const ServerVersion = "Chronalys/1.0"

var FatalErr = errors.New("fatal error")

func makeFatal(err error) error {
	return fmt.Errorf("%w: %v", FatalErr, err)
}

func (s *Socket) rollback(ctx context.Context, err error) error {
	_, e := atlas.ExecuteSQL(ctx, "ROLLBACK", s.sql, false)
	return errors.Join(err, e)
}

func (s *Socket) rollbackAutoTransaction(ctx context.Context, err error) error {
	if s.inTransaction {
		return s.rollback(ctx, err)
	}
	return err
}

func (s *Socket) HandleConnection(conn net.Conn, ctx context.Context) {
	defer func() {
		err := conn.Close()
		if err != nil {
			atlas.Logger.Error("Error closing connection", zap.Error(err))
		}
	}()

	s.reader = bufio.NewReader(conn)
	s.writer = bufio.NewWriter(conn)

	err := s.writeMessage("WELCOME " + ProtoVersion + " " + ServerVersion)
	if err != nil {
		atlas.Logger.Error("Error writing welcome message", zap.Error(err))
		return
	}
	handshakePart := 0

	// perform the handshake
	scanner := bufio.NewScanner(s.reader)
	for scanner.Scan() {
		if scanner.Err() != nil {
			atlas.Logger.Error("Error reading from connection", zap.Error(scanner.Err()))
			return
		}
		handshake := commands.CommandFromString(scanner.Text())
		switch handshakePart {
		case 0:
			if handshake.CheckExactLen(3) != nil {
				err := s.writeError(Fatal, errors.New("invalid handshake"))
				if err != nil {
					atlas.Logger.Error("Error writing error message", zap.Error(err))
					return
				}
			}

			if p, _ := handshake.SelectNormalizedCommand(0); p != "HELLO" {
				err := s.writeError(Fatal, errors.New("invalid handshake"))
				if err != nil {
					atlas.Logger.Error("Error writing error message", zap.Error(err))
					return
				}
			}

			if v, _ := handshake.SelectNormalizedCommand(1); v != ProtoVersion {
				err := s.writeError(Fatal, errors.New("invalid protocol version"))
				if err != nil {
					atlas.Logger.Error("Error writing error message", zap.Error(err))
					return
				}
			}

			// ignore client version for now
			// ignore authentication for now
			err := s.writeMessage("READY")
			if err != nil {
				atlas.Logger.Error("Error writing ready message", zap.Error(err))
				return
			}
			goto ready
		}
	}

ready:

	var changes []*commands.SqlCommand

	s.sql, err = atlas.Pool.Take(ctx)
	if err != nil {
		atlas.Logger.Error("Error taking connection from pool", zap.Error(err))
		return
	}
	defer atlas.Pool.Put(s.sql)

	for scanner.Scan() {
		if scanner.Err() != nil {
			atlas.Logger.Error("Error reading from connection", zap.Error(scanner.Err()))
			return
		}
		cmd := commands.CommandFromString(scanner.Text())
		if cmd.CheckMinLen(1) != nil {
			err := s.writeError(Fatal, errors.New("invalid command"))
			if err != nil {
				atlas.Logger.Error("Error writing error message", zap.Error(err))
				return
			}
			continue
		}

		switch k, _ := cmd.SelectNormalizedCommand(0); k {
		case "PREPARE":
			_, err = s.PerformPrepare(cmd)
			goto handleError
		case "EXECUTE":
			_, err = s.PerformExecute(ctx, cmd)
			goto handleError
		case "QUERY":
			_, err = s.PerformQuery(ctx, cmd)
			goto handleError
		case "FINALIZE":
			err = s.PerformFinalize(cmd)
			goto handleError
		case "BIND":
			err = s.PerformBind(cmd)
			goto handleError
		case "BEGIN":
			if s.inTransaction {
				err = makeFatal(errors.New("the transaction is already in progress"))
				goto handleError
			}
			if t, ok := cmd.SelectNormalizedCommand(1); ok && t == "IMMEDIATE" {
				_, err = atlas.ExecuteSQL(ctx, "BEGIN IMMEDIATE", s.sql, false)
			} else {
				_, err = atlas.ExecuteSQL(ctx, "BEGIN", s.sql, false)
			}
			s.inTransaction = true

			// todo: attach session and authenticator?

			err = s.writeOk(OK)
			goto handleError
		case "SAVEPOINT":
			if !s.inTransaction {
				err = makeFatal(errors.New("no transaction in progress"))
				goto handleError
			}
			if name, ok := cmd.SelectNormalizedCommand(1); ok {
				_, err = atlas.ExecuteSQL(ctx, "SAVEPOINT "+name, s.sql, false)
				if err != nil {
					goto handleError
				}
				err = s.writeOk(OK)
			} else {
				err = makeFatal(errors.New("invalid savepoint name"))
			}
			goto handleError
		case "RELEASE":
			if !s.inTransaction {
				err = makeFatal(errors.New("no transaction in progress"))
				goto handleError
			}
			if name, ok := cmd.SelectNormalizedCommand(1); ok {
				_, err = atlas.ExecuteSQL(ctx, "RELEASE "+name, s.sql, false)
				if err != nil {
					goto handleError
				}
				err = s.writeOk(OK)
			} else {
				err = makeFatal(errors.New("invalid savepoint name"))
			}
			goto handleError
		case "PRAGMA":
			if !s.inTransaction {
				err = makeFatal(errors.New("no transaction in progress"))
				goto handleError
			}
			// todo: parse special pragmas
			_, err = atlas.ExecuteSQL(ctx, cmd.Raw(), s.sql, false)
			goto handleError
		case "PRINCIPLE":
			var principal *Principal
			if principal, err = ParsePrincipal(cmd); err != nil {
				goto handleError
			}
			if err = principal.Handle(s); err != nil {
				goto handleError
			}
			err = s.writeOk(OK)
			goto handleError
		case "SCROLL":
			var scroll *Scroll
			if scroll, err = ParseScroll(cmd); err != nil {
				goto handleError
			}
			if err = scroll.Handle(s); err != nil {
				goto handleError
			}
			err = s.writeOk(OK)
			goto handleError
		case "RESET":
			if err = cmd.CheckExactLen(2); err != nil {
				goto handleError
			}
			id, _ := cmd.SelectNormalizedCommand(1)
			if stmt, ok := s.activeStmts[id]; ok {
				if idx := slices.Index(s.streams, stmt.stmt); idx >= 0 {
					s.streams = append(s.streams[:idx], s.streams[idx+1:]...)
				}
				err = stmt.stmt.Reset()
				if err != nil {
					goto handleError
				}
			} else {
				err = errors.New("unknown statement")
				goto handleError
			}
			err = s.writeOk(OK)
			goto handleError
		case "ClearBindings":
			if err = cmd.CheckExactLen(2); err != nil {
				goto handleError
			}
			id, _ := cmd.SelectNormalizedCommand(1)
			if stmt, ok := s.activeStmts[id]; ok {
				err = stmt.stmt.ClearBindings()
				if err != nil {
					goto handleError
				}
			} else {
				err = errors.New("unknown statement")
				goto handleError
			}
			err = s.writeOk(OK)
			goto handleError
		case "COMMIT":
			goto commit
		}

	commit:

	handleError:
		if err != nil && errors.Is(err, FatalErr) {
			err := s.writeError(Fatal, err)
			if err != nil {
				atlas.Logger.Error("Error writing error message", zap.Error(err))
				return
			}
			return
		} else if err != nil {
			err := s.writeError(Warning, err)
			if err != nil {
				atlas.Logger.Error("Error writing error message", zap.Error(err))
				return
			}
		}
	}
}

func (s *Socket) PerformFinalize(cmd *commands.CommandString) (err error) {
	if err = cmd.CheckExactLen(2); err != nil {
		return
	}
	id, _ := cmd.SelectNormalizedCommand(1)
	if stmt, ok := s.activeStmts[id]; ok {
		if idx := slices.Index(s.streams, stmt.stmt); idx >= 0 {
			s.streams = append(s.streams[:idx], s.streams[idx+1:]...)
		}
		err = stmt.stmt.Finalize()
		if err != nil {
			return
		}
		delete(s.activeStmts, id)
	} else {
		err = errors.New("unknown statement")
		return
	}
	err = s.writeOk(OK)
	return
}

// PerformPrepare parses a SQL query and creates an appropriate Prepare object for execution.
func (s *Socket) PerformPrepare(cmd *commands.CommandString) (prepare *Prepare, err error) {
	if prepare, err = ParsePrepare(cmd); err != nil {
		return
	}
	if err = prepare.Handle(s); err != nil {
		return
	}
	err = s.writeOk(OK)
	return
}

func (s *Socket) PerformQuery(ctx context.Context, cmd *commands.CommandString) (query *Query, err error) {
	if !s.inTransaction {
		_, err = atlas.ExecuteSQL(ctx, "BEGIN", s.sql, false)
		if err != nil {
			return
		}
	}
	if query, err = ParseQuery(cmd); err != nil {
		err = s.rollbackAutoTransaction(ctx, err)
		return
	}

	if !s.inTransaction && !query.query.IsQueryReadOnly() {
		err = s.rollbackAutoTransaction(ctx, errors.New("cannot execute a non-read-only query outside transaction"))
		return
	}

	if err = query.Handle(s); err != nil {
		err = s.rollbackAutoTransaction(ctx, err)
		return
	}
	f, err := os.CreateTemp("", "temp_*")
	if err != nil {
		err = s.rollbackAutoTransaction(ctx, err)
		return
	}
	f.Close()
	os.Remove(f.Name())
	streamId := filepath.Base(f.Name())
	s.activeStmts[streamId] = query
	if err = s.writeMessage(fmt.Sprintf("STREAM %s", streamId)); err != nil {
		err = s.rollbackAutoTransaction(ctx, err)
		return
	}
	if err = s.outputMetaHeaders(query.stmt); err != nil {
		err = makeFatal(s.rollbackAutoTransaction(ctx, err))
		return
	}
	if err = s.writeOk(OK); err != nil {
		err = makeFatal(s.rollbackAutoTransaction(ctx, err))
		return
	}

	err = s.rollbackAutoTransaction(ctx, nil)
	return
}

func (s *Socket) PerformExecute(ctx context.Context, cmd *commands.CommandString) (execute *Execute, err error) {
	if !s.inTransaction {
		_, err = atlas.ExecuteSQL(ctx, "BEGIN", s.sql, false)
		if err != nil {
			return
		}
	}
	if execute, err = ParseExecute(cmd); err != nil {
		err = s.rollbackAutoTransaction(ctx, err)
		return
	}

	if !s.activeStmts[execute.id].query.IsQueryReadOnly() && !s.inTransaction {
		err = s.rollbackAutoTransaction(ctx, errors.New("cannot execute a non-read-only query outside transaction"))
		return
	}

	if err = execute.Handle(s); err != nil {
		err = s.rollbackAutoTransaction(ctx, err)
		return
	}
	streamId := execute.id
	if err = s.writeMessage(fmt.Sprintf("STREAM %s", streamId)); err != nil {
		err = s.rollbackAutoTransaction(ctx, err)
		return
	}
	if err = s.outputMetaHeaders(s.activeStmts[streamId].stmt); err != nil {
		err = s.rollbackAutoTransaction(ctx, err)
		return
	}
	if err = s.writeOk(OK); err != nil {
		err = s.rollbackAutoTransaction(ctx, err)
		return
	}

	err = s.rollbackAutoTransaction(ctx, nil)
	return
}

func (s *Socket) PerformBind(cmd *commands.CommandString) (err error) {
	err = cmd.CheckMinLen(5)
	if err != nil {
		return
	}
	id, _ := cmd.SelectNormalizedCommand(1)
	param, _ := cmd.SelectNormalizedCommand(2)
	typ, _ := cmd.SelectNormalizedCommand(3)
	value := cmd.From(4).Raw()
	if stmt, ok := s.activeStmts[id]; ok {
		var i int
		if i, err = strconv.Atoi(param); err != nil {
			param = stmt.stmt.BindParamName(i)
			if param == "" {
				err = errors.New("unknown parameter")
				return
			}
		}
		switch typ {
		case "TEXT":
			stmt.stmt.SetText(param, value)
		case "BYTE":
			var bytes []byte
			if bytes, err = strutil.Base64Decode([]byte(value)); err != nil {
				stmt.stmt.SetBytes(param, bytes)
			} else {
				return
			}
		case "INT":
			fallthrough
		case "INTEGER":
			var i int64
			if i, err = strconv.ParseInt(value, 10, 64); err != nil {
				stmt.stmt.SetInt64(param, i)
			} else {
				return
			}
		case "FLOAT":
			var f float64
			if f, err = strconv.ParseFloat(value, 64); err != nil {
				stmt.stmt.SetFloat(param, f)
			} else {
				return
			}
		case "NULL":
			stmt.stmt.SetNull(param)
		case "BOOL":
			var b bool
			if b, err = strconv.ParseBool(value); err != nil {
				stmt.stmt.SetBool(param, b)
			} else {
				return
			}
		case "ZERO":
			var l int64
			if l, err = strconv.ParseInt(value, 10, 64); err != nil {
				stmt.stmt.SetZeroBlob(param, l)
			} else {
				return
			}
		}
	} else {
		err = errors.New("unknown statement")
		return
	}
	err = s.writeOk(OK)
	return
}
