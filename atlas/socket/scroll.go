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
	"encoding/base64"
	"errors"
	"fmt"
	"github.com/bottledcode/atlas-db/atlas/commands"
	"slices"
	"strconv"
	"zombiezen.com/go/sqlite"
)

type Scroll struct {
	id   string
	rows int
}

func ParseScroll(cmd *commands.CommandString) (scroll *Scroll, err error) {
	if err = cmd.CheckExactLen(3); err != nil {
		return
	}

	id, _ := cmd.SelectNormalizedCommand(1)
	rows, _ := cmd.SelectNormalizedCommand(2)
	r, err := strconv.Atoi(rows)
	if err != nil {
		return
	}
	return &Scroll{id: id, rows: r}, nil
}

func (c *Scroll) outputRow(stmt *sqlite.Stmt, s *Socket) (complete bool, err error) {
	var hasNext bool

	if hasNext, err = stmt.Step(); hasNext {
		for i := 0; i < stmt.ColumnCount(); i++ {
			switch stmt.ColumnType(i) {
			case sqlite.TypeNull:
				err = s.writeMessage(fmt.Sprintf("ROW %d NULL", i))
				if err != nil {
					return
				}
			case sqlite.TypeText:
				err = s.writeMessage(fmt.Sprintf("ROW %d TEXT %s", i, stmt.ColumnText(i)))
				if err != nil {
					return
				}
			case sqlite.TypeInteger:
				err = s.writeMessage(fmt.Sprintf("ROW %d INT %d", i, stmt.ColumnInt(i)))
				if err != nil {
					return
				}
			case sqlite.TypeFloat:
				err = s.writeMessage(fmt.Sprintf("ROW %d FLOAT %f", i, stmt.ColumnFloat(i)))
				if err != nil {
					return
				}
			case sqlite.TypeBlob:
				var bytes []byte
				stmt.ColumnBytes(i, bytes)
				str := base64.StdEncoding.EncodeToString(bytes)
				err = s.writeMessage(fmt.Sprintf("ROW %d BLOB %d %s", i, len(str), str))
				if err != nil {
					return
				}
			}
		}
	} else {
		complete = true
	}
	return
}

var ErrComplete = errors.New("scroll complete")

func (c *Scroll) Handle(s *Socket) error {
	if stmt, ok := s.activeStmts[c.id]; ok && slices.Contains(s.streams, stmt.stmt) {
		for i := 0; i < c.rows; i++ {
			if complete, err := c.outputRow(stmt.stmt, s); err != nil {
				return err
			} else if complete && !stmt.query.IsQueryReadOnly() {
				err = s.outputTrailerHeaders()
				if err != nil {
					return err
				}
				return ErrComplete
			} else if complete {
				return ErrComplete
			}
		}
	} else {
		return errors.New("statement not found")
	}
	return nil
}
