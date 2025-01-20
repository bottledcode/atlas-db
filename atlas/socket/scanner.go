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
	"github.com/bottledcode/atlas-db/atlas/commands"
	"strings"
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
					out <- commands.CommandFromString(buf.String())
					buf.Reset()
				}
			}
		}
	}()

	return out, errs
}
