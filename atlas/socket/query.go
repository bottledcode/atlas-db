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
	"github.com/bottledcode/atlas-db/atlas"
	"github.com/bottledcode/atlas-db/atlas/commands"
	"github.com/bottledcode/atlas-db/atlas/consensus"
	"github.com/bottledcode/atlas-db/atlas/operations"
	"go.uber.org/zap"
	"zombiezen.com/go/sqlite"
)

type Query struct {
	stmt   *sqlite.Stmt
	query  *commands.SqlCommand
	tables []*consensus.Table
}

func ParseQuery(cmd *commands.CommandString) (query *Query, err error) {
	if err := cmd.CheckMinLen(2); err != nil {
		return nil, err
	}

	q := cmd.From(1)

	query = &Query{
		query: q,
		stmt:  nil,
	}

	return query, nil
}

func (q *Query) Handle(s *Socket) (err error) {
	s.authorizer.Reset()
	q.stmt, _, err = s.sql.PrepareTransient(q.query.Raw())
	if err != nil {
		return makeFatal(err)
	}
	tables := s.authorizer.LastTables
	atlas.Logger.Info("tables", zap.Any("tables", tables))

	if !q.query.IsQueryReadOnly() {
		first, _ := q.query.SelectNormalizedCommand(0)
		if first == "CREATE" {
			q.tables, err = operations.CreateTable(q.query)
			if err != nil {
				return nil
			}
		}

		// todo: handle alters
	}

	s.streams = append(s.streams, q.stmt)
	return nil
}
