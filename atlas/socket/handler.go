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
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/bottledcode/atlas-db/atlas/commands"
	"github.com/bottledcode/atlas-db/atlas/kv"
	"github.com/bottledcode/atlas-db/atlas/options"
	"go.uber.org/zap"
	"google.golang.org/grpc/metadata"
)

// gRPC metadata key for the session principal; must be lowercase per grpc-go.
const atlasPrincipalKey = "atlas-principal"

type Socket struct {
	writer    *bufio.ReadWriter
	conn      net.Conn
	timeout   time.Duration
	principal string
	writeMu   sync.Mutex // protects concurrent writes to writer
}

// validatePrincipal ensures the provided principal name is safe for use in the
// line-oriented protocol and for propagation via metadata. It trims surrounding
// whitespace, rejects control characters (including CR/LF and Unicode line
// breaks), and enforces a 1..256 length in both bytes and runes.
func validatePrincipal(name string) (string, error) {
	n := strings.TrimSpace(name)
	if n == "" {
		return "", errors.New("empty principal")
	}
	if len(n) > 256 { // bytes
		return "", errors.New("principal too long")
	}
	runeCount := 0
	for _, r := range n {
		runeCount++
		switch r {
		case '\r', '\n', '\u0085', '\u2028', '\u2029':
			return "", errors.New("invalid character in principal")
		}
		if r < 0x20 || r == 0x7f { // ASCII controls and DEL
			return "", errors.New("invalid character in principal")
		}
	}
	if runeCount == 0 || runeCount > 256 {
		return "", errors.New("principal length out of range")
	}
	return n, nil
}

func (s *Socket) Cleanup() {
}

func (s *Socket) writeRawMessage(msg ...[]byte) error {
	for _, m := range msg {
		for len(m) > 0 {
			n, err := s.writer.Write(m)
			if err != nil {
				return err
			}
			m = m[n:]
		}
	}
	return nil
}

const EOL = "\r\n"

func (s *Socket) writeMessage(msg []byte) error {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()

	err := s.setWriteTimeout(s.timeout)
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
	return s.writeMessage([]byte("ERROR " + string(code) + " " + err.Error()))
}

func (s *Socket) writeOk(code ErrorCode) error {
	return s.writeMessage([]byte(code))
}

// writeTaggedMessage writes a message with an optional request ID prefix.
// Format: [ID:123] MESSAGE
func (s *Socket) writeTaggedMessage(requestID string, msg []byte) error {
	if requestID != "" {
		tagged := append([]byte("[ID:"+requestID+"] "), msg...)
		return s.writeMessage(tagged)
	}
	return s.writeMessage(msg)
}

// writeTaggedError writes an error with an optional request ID prefix.
func (s *Socket) writeTaggedError(requestID string, code ErrorCode, err error) error {
	msg := []byte("ERROR " + string(code) + " " + err.Error())
	return s.writeTaggedMessage(requestID, msg)
}

// writeTaggedOk writes an OK response with an optional request ID prefix.
func (s *Socket) writeTaggedOk(requestID string, code ErrorCode) error {
	return s.writeTaggedMessage(requestID, []byte(code))
}

const ProtoVersion = "1.0"

var ServerVersion = "Chronalys/1.0"

// setTimeout sets both read and write deadlines on the connection.
//
//nolint:unused // kept for contexts that require a full deadline; writeMessage uses setWriteTimeout.
func (s *Socket) setTimeout(t time.Duration) error {
	if t > 0 {
		return s.conn.SetDeadline(time.Now().Add(t))
	}
	return nil
}

// setWriteTimeout sets only the write deadline on the connection so that
// read deadlines remain untouched for the scanner goroutine.
func (s *Socket) setWriteTimeout(t time.Duration) error {
	if t > 0 {
		return s.conn.SetWriteDeadline(time.Now().Add(t))
	}
	return nil
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
		case <-errs:
			//options.Logger.Error("Error reading from client connection", zap.Error(err))
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

			// Session-level principal commands handled here
			if first, _ := cmd.SelectNormalizedCommand(0); first == "PRINCIPAL" {
				if cmd.CheckMinLen(2) != nil {
					if err := s.writeError(Warning, errors.New("invalid PRINCIPAL command")); err != nil {
						options.Logger.Error("Error writing error message", zap.Error(err))
						return
					}
					continue
				}
				action, _ := cmd.SelectNormalizedCommand(1)
				switch action {
				case "WHOAMI":
					who := s.principal
					if who == "" {
						who = "(none)"
					}
					if err := s.writeMessage([]byte("PRINCIPAL " + who)); err != nil {
						options.Logger.Error("Error writing WHOAMI response", zap.Error(err))
						return
					}
					if err := s.writeOk(OK); err != nil {
						options.Logger.Error("Error writing OK response", zap.Error(err))
						return
					}
					continue
				case "ASSUME":
					if cmd.CheckMinLen(3) != nil {
						if err := s.writeError(Warning, errors.New("usage: PRINCIPAL ASSUME <name>")); err != nil {
							options.Logger.Error("Error writing error message", zap.Error(err))
							return
						}
						continue
					}
					// Validate and set the session principal
					candidate := cmd.SelectCommand(2)
					validated, vErr := validatePrincipal(candidate)
					if vErr != nil {
						if err := s.writeError(Warning, errors.New("usage: PRINCIPAL ASSUME <name>")); err != nil {
							options.Logger.Error("Error writing error message", zap.Error(err))
							return
						}
						continue
					}
					s.principal = validated
					if err := s.writeOk(OK); err != nil {
						options.Logger.Error("Error writing OK response", zap.Error(err))
						return
					}
					continue
				default:
					if err := s.writeError(Warning, errors.New("unknown PRINCIPAL action")); err != nil {
						options.Logger.Error("Error writing error message", zap.Error(err))
						return
					}
					continue
				}
			}

			command, err := cmd.GetNext()
			if err != nil {
				options.Logger.Error("Error reading command", zap.Error(err))
				requestID := cmd.GetRequestID()
				err = s.writeTaggedError(requestID, Warning, err)
				if err != nil {
					options.Logger.Error("Error writing error message", zap.Error(err))
					return
				}
				continue
			}

			// Check if this is an async command (has request ID)
			requestID := cmd.GetRequestID()
			if requestID != "" {
				// Capture principal at time of command submission
				principal := s.principal
				// Execute asynchronously in a goroutine with captured principal
				go s.executeCommandAsync(ctx, cmd, command, requestID, principal)
			} else {
				// Execute synchronously (backward compatible)
				if err := s.executeCommand(ctx, cmd, command, "", s.principal); err != nil {
					options.Logger.Error("Error in synchronous command execution", zap.Error(err))
					return
				}
			}
		}
	}
}

// executeCommand executes a command and writes the response.
// Returns an error if writing fails (connection should close).
// The principal parameter should be the principal value captured at command submission time.
func (s *Socket) executeCommand(ctx context.Context, cmd *commands.CommandString, command commands.Command, requestID string, principal string) error {
	execCtx := ctx
	if principal != "" {
		execCtx = metadata.NewOutgoingContext(ctx, metadata.Pairs(atlasPrincipalKey, principal))
	}

	resp, err := command.Execute(execCtx)
	if err != nil {
		options.Logger.Error("Error executing command", zap.Error(err))
		code, msg := formatCommandError(err, s.principal)
		err = s.writeTaggedError(requestID, code, errors.New(msg))
		if err != nil {
			return err
		}
		return nil
	}

	if isBlobGetCommand(cmd) {
		err = s.writeTaggedBlobResponse(requestID, resp)
		if err != nil {
			return err
		}
	} else {
		err = s.writeTaggedMessage(requestID, resp)
		if err != nil {
			return err
		}
	}

	err = s.writeTaggedOk(requestID, OK)
	if err != nil {
		return err
	}

	return nil
}

// executeCommandAsync executes a command asynchronously.
// Errors are logged but don't close the connection.
// The principal parameter should be captured at command submission time to avoid race conditions.
func (s *Socket) executeCommandAsync(ctx context.Context, cmd *commands.CommandString, command commands.Command, requestID string, principal string) {
	if err := s.executeCommand(ctx, cmd, command, requestID, principal); err != nil {
		options.Logger.Error("Error in async command execution",
			zap.String("requestID", requestID),
			zap.String("principal", principal),
			zap.Error(err))
	}
}

func isBlobGetCommand(cmd *commands.CommandString) bool {
	if cmd.NormalizedLen() < 3 {
		return false
	}
	p0, ok0 := cmd.SelectNormalizedCommand(0)
	p1, ok1 := cmd.SelectNormalizedCommand(1)
	p2, ok2 := cmd.SelectNormalizedCommand(2)
	return ok0 && ok1 && ok2 && p0 == "KEY" && p1 == "BLOB" && p2 == "GET"
}

//nolint:unused // used in tests
func (s *Socket) writeBlobResponse(binaryData []byte) error {
	return s.writeTaggedBlobResponse("", binaryData)
}

// writeTaggedBlobResponse writes a blob response with an optional request ID prefix.
func (s *Socket) writeTaggedBlobResponse(requestID string, binaryData []byte) error {
	if binaryData == nil {
		return s.writeTaggedMessage(requestID, []byte("EMPTY"))
	}

	s.writeMu.Lock()
	defer s.writeMu.Unlock()

	err := s.setWriteTimeout(s.timeout)
	if err != nil {
		return err
	}

	header := []byte("BLOB " + strconv.Itoa(len(binaryData)))
	if requestID != "" {
		header = append([]byte("[ID:"+requestID+"] "), header...)
	}

	err = s.writeRawMessage(header, []byte(EOL))
	if err != nil {
		return err
	}

	err = s.writeRawMessage(binaryData)
	if err != nil {
		return err
	}

	return s.writer.Flush()
}
