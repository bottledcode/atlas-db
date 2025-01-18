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
	"go.uber.org/zap"
	"net"
	"strconv"
	"zombiezen.com/go/sqlite"
)

type Socket struct {
	writer        *bufio.Writer
	reader        *bufio.Reader
	sql           *sqlite.Conn
	inTransaction bool
	session       *sqlite.Session
	activeStmts   map[string]*sqlite.Stmt
	streams       []*sqlite.Stmt
}

func (s *Socket) Cleanup() {
	for _, stmt := range s.activeStmts {
		err := stmt.Finalize()
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
	return nil
}

const ProtoVersion = "1.0"

// todo: set during build
const ServerVersion = "Chronalys/1.0"

var FatalErr = errors.New("fatal error")

func makeFatal(err error) error {
	return fmt.Errorf("%w: %v", FatalErr, err)
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
			var prepare *Prepare
			if prepare, err = ParsePrepare(cmd); err != nil {
				goto handleError
			}
			if err = prepare.Handle(s); err != nil {
				goto handleError
			}
			if err = s.writeOk(OK); err != nil {
				goto handleError
			}
		case "EXECUTE":
			var execute *Execute
			if execute, err = ParseExecute(cmd); err != nil {
				goto handleError
			}
			if err = execute.Handle(s); err != nil {
				goto handleError
			}
			streamId := len(s.streams) - 1
			if err = s.writeMessage(fmt.Sprintf("STREAM %d", streamId)); err != nil {
				goto handleError
			}
			if err = s.outputMetaHeaders(s.streams[streamId]); err != nil {
				goto handleError
			}
			if err = s.writeOk(OK); err != nil {
				goto handleError
			}
		}

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
