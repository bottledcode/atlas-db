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
	"github.com/bottledcode/atlas-db/atlas/commands"
	"github.com/bottledcode/atlas-db/atlas/consensus"
)

type Principal struct {
	name  string
	value string
}

func ParsePrincipal(cmd *commands.CommandString) (principal *Principal, err error) {
	if err = cmd.CheckExactLen(2); err != nil {
		return
	}

	name, _ := cmd.SelectNormalizedCommand(2)
	value := cmd.SelectCommand(3)
	return &Principal{name: name, value: value}, nil
}

func (p *Principal) Handle(s *Socket) error {
	principal := &consensus.Principal{
		Name:  p.name,
		Value: p.value,
	}
	s.principals = append(s.principals, principal)
	return nil
}
