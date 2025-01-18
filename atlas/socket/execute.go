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
	"slices"
)

type Execute struct {
	id string
}

func ParseExecute(cmd commands.Command) (*Execute, error) {
	if err := cmd.CheckExactLen(2); err != nil {
		return nil, err
	}
	id, _ := cmd.SelectNormalizedCommand(1)
	return &Execute{id: id}, nil
}

func (e *Execute) Handle(s *Socket) error {
	stmt, ok := s.activeStmts[e.id]
	if !ok {
		return errors.New("statement not found")
	}
	if slices.Contains(s.streams, stmt) {
		return errors.New("statement already executing")
	}
	s.streams = append(s.streams, stmt)
	return nil
}
