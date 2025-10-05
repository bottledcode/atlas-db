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
	"io"
	"strconv"
	"strings"

	"github.com/bottledcode/atlas-db/atlas/commands"
)

type Scanner struct {
	reader *bufio.ReadWriter
}

func NewScanner(reader *bufio.ReadWriter) *Scanner {
	return &Scanner{
		reader: reader,
	}
}

const maxScannerLength = 128 * 1024 * 1024 // 128mb

func (s *Scanner) Scan(ctx context.Context) (chan *commands.CommandString, chan error) {
	out := make(chan *commands.CommandString)
	errs := make(chan error)

	go func() {
		defer close(out)
		defer close(errs)

		buf := strings.Builder{}

		for {
			select {
			case <-ctx.Done():
				buf.Reset()
				return
			default:
				line, isPrefix, err := s.reader.ReadLine()
				if err != nil {
					errs <- err
					return
				}
				buf.WriteString(string(line))
				if buf.Len() > maxScannerLength {
					errs <- errors.New("command exceeded maximum length")
					return
				}

				if !isPrefix {
					cmdStr := buf.String()
					cmd := commands.CommandFromString(cmdStr)
					buf.Reset()

					if isBlobSetCommand(cmd) {
						binaryData, err := s.readBinaryData(cmd)
						if err != nil {
							errs <- err
							return
						}
						cmd.SetBinaryData(binaryData)
					}

					out <- cmd
				}
			}
		}
	}()

	return out, errs
}

// isBlobSetCommand checks if the command is "KEY BLOB SET <key> <length>"
func isBlobSetCommand(cmd *commands.CommandString) bool {
	if cmd.NormalizedLen() < 5 {
		return false
	}
	p0, ok0 := cmd.SelectNormalizedCommand(0)
	p1, ok1 := cmd.SelectNormalizedCommand(1)
	p2, ok2 := cmd.SelectNormalizedCommand(2)
	return ok0 && ok1 && ok2 && p0 == "KEY" && p1 == "BLOB" && p2 == "SET"
}

// readBinaryData reads exactly <length> bytes of binary data for BLOB commands
func (s *Scanner) readBinaryData(cmd *commands.CommandString) ([]byte, error) {
	if cmd.NormalizedLen() < 5 {
		return nil, errors.New("KEY BLOB SET requires <key> and <length>")
	}

	lengthStr := cmd.SelectCommand(4)
	length, err := strconv.ParseInt(lengthStr, 10, 64)
	if err != nil {
		return nil, errors.New("invalid length in KEY BLOB SET: " + err.Error())
	}

	if length < 0 {
		return nil, errors.New("negative length not allowed")
	}

	if length > maxScannerLength {
		return nil, errors.New("binary data exceeds maximum length")
	}

	data := make([]byte, length)
	_, err = io.ReadFull(s.reader, data)
	if err != nil {
		return nil, errors.New("failed to read binary data: " + err.Error())
	}

	return data, nil
}
