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
	"errors"
	"github.com/bottledcode/atlas-db/atlas/commands"
	"strings"
)

type Begin struct {
	isReadonly bool
	tables     []string
	views      []string
	triggers   []string
}

func sanitizeBegin(cmd commands.Command) (tables, views, triggers []string, err error) {
	extractList := func() (list []string, err error) {
		var rip []string
		n := 4
		expectingFirst := true
		expectingLast := true
		for {
			first, _ := cmd.SelectNormalizedCommand(n)
			rip = append(rip, first)
			n++
			isLast := false
			first = strings.TrimSuffix(first, ",")
			if first == "" {
				break
			}
			if first == "(" {
				expectingFirst = false
				continue
			}
			if first == ")" {
				expectingLast = false
				break
			}
			if strings.HasPrefix(first, "(") {
				first = strings.TrimPrefix(first, "(")
				expectingFirst = false
			}
			if strings.HasSuffix(first, ")") {
				expectingLast = false
				first = strings.TrimSuffix(first, ")")
				isLast = true
			}
			if expectingFirst {
				err = errors.New("expected table name in parentheses")
				return
			}

			if first == "," {
				continue
			}

			list = append(list, cmd.NormalizeName(first))
			if isLast {
				break
			}
		}
		if expectingFirst || expectingLast {
			err = errors.New("expected table name in parentheses")
			return
		}
		cmd = cmd.ReplaceCommand(strings.Join(rip, " "), "BEGIN IMMEDIATE WITH")
		return
	}

	if t, ok := cmd.SelectNormalizedCommand(1); ok && t == "IMMEDIATE" {
		if err = cmd.CheckMinLen(3); err != nil {
			return
		}
		for {
			switch t, _ = cmd.SelectNormalizedCommand(3); t {
			case "TABLE":
				if err = cmd.CheckMinLen(4); err != nil {
					return
				}
				tables, err = extractList()
				if err != nil {
					return
				}
			case "VIEW":
				if err = cmd.CheckMinLen(4); err != nil {
					return
				}
				views, err = extractList()
				if err != nil {
					return
				}
			case "TRIGGER":
				if err = cmd.CheckMinLen(4); err != nil {
					return
				}
				triggers, err = extractList()
				if err != nil {
					return
				}
			case "":
				return
			default:
				err = errors.New("expected TABLE, VIEW, or TRIGGER")
				return
			}
		}
	}
	return
}

func ParseBegin(cmd commands.Command) (*Begin, error) {
	if t, ok := cmd.SelectNormalizedCommand(1); ok && t == "IMMEDIATE" {
		tables, views, triggers, err := sanitizeBegin(cmd)
		if err != nil {
			return nil, err
		}

		return &Begin{
			isReadonly: false,
			tables:     tables,
			views:      views,
			triggers:   triggers,
		}, nil
	}
	return &Begin{
		isReadonly: true,
	}, nil
}

func (b *Begin) Handle(s *Socket) error {
	s.authorizer.Reset()
	s.authorizer.ForceReadonly = b.isReadonly

	// todo: capture tables, views, and triggers

	return nil
}
