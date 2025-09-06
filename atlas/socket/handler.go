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
	"net"
	"time"

	"github.com/bottledcode/atlas-db/atlas/kv"
	"github.com/bottledcode/atlas-db/atlas/options"
	"go.uber.org/zap"
)

type Socket struct {
	writer  *bufio.ReadWriter
	conn    net.Conn
	timeout time.Duration
}

func (s *Socket) Cleanup() {
}

func (s *Socket) writeRawMessage(msg ...[]byte) error {
	for _, m := range msg {
		n, err := s.writer.Write(m)
		if err != nil {
			return err
		}
		if n < len(m) {
			return s.writeRawMessage(m[n:])
		}
	}
	err := s.writer.Flush()
	if err != nil {
		return err
	}
	return nil
}

const EOL = "\r\n"

func (s *Socket) writeMessage(msg []byte) error {
	err := s.setTimeout(s.timeout)
	if err != nil {
		return err
	}
	err = s.writeRawMessage(msg, []byte(EOL))
	if err != nil {
		return err
	}
	return s.writer.Flush()
}

func (s *Socket) writeError(code ErrorCode, err error) error {
	e := s.writeMessage([]byte("ERROR " + string(code) + " " + err.Error()))
	if e != nil {
		return e
	}
	return s.writer.Flush()
}

func (s *Socket) writeOk(code ErrorCode) error {
	err := s.writeMessage([]byte(code))
	if err != nil {
		return err
	}
	return s.writer.Flush()
}

const ProtoVersion = "1.0"

var ServerVersion = "Chronalys/1.0"

func (s *Socket) setTimeout(t time.Duration) error {
	return s.conn.SetDeadline(time.Now().Add(t))
}

func (s *Socket) HandleConnection(conn net.Conn, ctx context.Context) {
	defer func() {
		err := conn.Close()
		if err != nil {
			options.Logger.Error("Error closing connection", zap.Error(err))
		}
	}()

	s.conn = conn
	s.writer = bufio.NewReadWriter(bufio.NewReader(conn), bufio.NewWriter(conn))

	err := s.writeMessage([]byte("WELCOME " + ProtoVersion + " " + ServerVersion))
	if err != nil {
		options.Logger.Error("Error writing welcome message", zap.Error(err))
		return
	}
	handshakePart := 0

	scanner := NewScanner(s.writer)
	cmds, errs := scanner.Scan(ctx)

	// perform the handshake
	for {
		select {
		case <-ctx.Done():
			return
		case err = <-errs:
			options.Logger.Error("Error reading from connection", zap.Error(err))
			return
		case handshake := <-cmds:
			switch handshakePart {
			case 0:
				if handshake.CheckExactLen(3) != nil {
					err := s.writeError(Fatal, errors.New("invalid handshake"))
					if err != nil {
						options.Logger.Error("Error writing error message", zap.Error(err))
					}
					return
				}

				if p, _ := handshake.SelectNormalizedCommand(0); p != "HELLO" {
					err := s.writeError(Fatal, errors.New("invalid handshake"))
					if err != nil {
						options.Logger.Error("Error writing error message", zap.Error(err))
					}
					return
				}

				if v, _ := handshake.SelectNormalizedCommand(1); v != ProtoVersion {
					err := s.writeError(Fatal, errors.New("invalid protocol version"))
					if err != nil {
						options.Logger.Error("Error writing error message", zap.Error(err))
					}
					return
				}

				// ignore client version for now
				// ignore authentication for now
				err := s.writeMessage([]byte("READY"))
				if err != nil {
					options.Logger.Error("Error writing ready message", zap.Error(err))
					return
				}
				goto ready
			}
		}
	}

ready:

	pool := kv.GetPool()
	if pool == nil {
		options.Logger.Error("KV pool is nil after creation")
		return
	}

	for {
		select {
		case <-ctx.Done():
			return
		case err = <-errs:
			options.Logger.Error("Error reading from client connection", zap.Error(err))
			return
		case cmd := <-cmds:
			if cmd == nil {
				continue
			}

			if cmd.CheckMinLen(1) != nil {
				err := s.writeError(Fatal, errors.New("invalid command"))
				if err != nil {
					options.Logger.Error("Error writing error message", zap.Error(err))
					return
				}
			}

			command, err := cmd.GetNext()
			if err != nil {
				options.Logger.Error("Error reading command", zap.Error(err))
				err = s.writeError(Warning, err)
				if err != nil {
					options.Logger.Error("Error writing error message", zap.Error(err))
					return
				}
				continue
			}
			resp, err := command.Execute(ctx)
			if err != nil {
				options.Logger.Error("Error executing command", zap.Error(err))
				err = s.writeError(Warning, err)
				if err != nil {
					options.Logger.Error("Error writing error message", zap.Error(err))
					return
				}
				continue
			}
			err = s.writeMessage(resp)
			if err != nil {
				options.Logger.Error("Error writing response", zap.Error(err))
				return
			}
			err = s.writeOk(OK)
			if err != nil {
				options.Logger.Error("Error writing OK response", zap.Error(err))
				return
			}
		}
	}
}
