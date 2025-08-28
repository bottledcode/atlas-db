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

package server

import (
	"bytes"
	"context"
	"fmt"
	"github.com/bottledcode/atlas-db/atlas"
	"github.com/bottledcode/atlas-db/atlas/consensus"
	"strings"
	"zombiezen.com/go/sqlite"
)

type principalKey struct {
	Name string
}

type principalValue struct {
	Value string
}

func AttachPrincipal(ctx context.Context, name, value string) context.Context {
	return context.WithValue(ctx, principalKey{Name: name}, principalValue{Value: value})
}

func getTable(ctx context.Context, table string) (*consensus.Table, error) {
	conn, err := atlas.MigrationsPool.Take(ctx)
	if err != nil {
		return nil, err
	}
	defer atlas.MigrationsPool.Put(conn)
	tr := consensus.GetDefaultTableRepository(ctx, conn)
	t, err := tr.GetTable(table)
	if err != nil {
		return nil, err
	}

	if t != nil && len(t.GetShardPrincipals()) > 0 {
		shards := make([]*consensus.Principal, 0, len(t.GetShardPrincipals()))
		for _, p := range t.GetShardPrincipals() {
			if v, ok := ctx.Value(principalKey{Name: p}).(principalValue); !ok {
				return nil, fmt.Errorf("missing principal: %s", p)
			} else {
				shards = append(shards, &consensus.Principal{Name: p, Value: v.Value})
			}
		}
		shard, err := tr.GetShard(t, shards)
		if err != nil {
			return nil, err
		}
		if shard == nil {
			return nil, fmt.Errorf("shard isn't found: %s", table)
		}

		return shard.GetShard(), nil
	}

	return t, nil
}

func StealTableByName(ctx context.Context, tableName string) (consensus.Quorum, error) {
	table, err := getTable(ctx, tableName)
	if err != nil {
		return nil, err
	}

	if table == nil {
		return nil, fmt.Errorf("table isn't found: %s", tableName)
	}

	// resolve groups
goUp:
	if table.GetGroup() != "" {
		table, err = getTable(ctx, table.GetGroup())
		if err != nil {
			return nil, err
		}
		goto goUp
	}

	return stealTable(ctx, table)
}

func stealTable(ctx context.Context, table *consensus.Table) (consensus.Quorum, error) {
	req := &consensus.StealTableOwnershipRequest{
		Sender: consensus.ConstructCurrentNode(),
		Reason: consensus.StealReason_queryReason,
		Table:  table,
	}

	qm := consensus.GetDefaultQuorumManager(ctx)
	q, err := qm.GetQuorum(ctx, table.GetName())
	if err != nil {
		return nil, err
	}

	response, err := q.StealTableOwnership(ctx, req)
	if err != nil {
		return nil, err
	}

	if !response.GetPromised() {
		return nil, fmt.Errorf("unable to steal ownership for table: %s", table.GetName())
	}

	var conn *sqlite.Conn
	if strings.HasPrefix(table.GetName(), "ATLAS.") {
		conn, err = atlas.MigrationsPool.Take(ctx)
		if err != nil {
			return nil, err
		}
		defer atlas.MigrationsPool.Put(conn)
	} else {
		conn, err = atlas.Pool.Take(ctx)
		if err != nil {
			return nil, err
		}
		defer atlas.Pool.Put(conn)
	}

	// apply any missing migrations
	_, err = atlas.ExecuteSQL(ctx, "BEGIN IMMEDIATE", conn, false)
	if err != nil {
		return nil, err
	}
	defer func() {
		if err != nil {
			_, _ = atlas.ExecuteSQL(ctx, "ROLLBACK", conn, false)
		}
	}()

	nextVersion := int64(1)

	for _, missing := range response.GetSuccess().GetMissingMigrations() {
		nextVersion = missing.GetVersion().GetMigrationVersion() + 1
		switch missing.GetMigration().(type) {
		case *consensus.Migration_Data:
			for _, data := range missing.GetData().GetSession() {
				reader := bytes.NewReader(data)
				err = conn.ApplyChangeset(reader, nil, func(conflictType sqlite.ConflictType, iterator *sqlite.ChangesetIterator) sqlite.ConflictAction {
					return sqlite.ChangesetReplace
				})
				if err != nil {
					return nil, err
				}
			}
		case *consensus.Migration_Schema:
			var stmt *sqlite.Stmt
			hasRow := true
			for _, command := range missing.GetSchema().GetCommands() {
				stmt, _, err = conn.PrepareTransient(command)
				if err != nil {
					return nil, err
				}
				for hasRow {
					hasRow, err = stmt.Step()
					if err != nil {
						return nil, err
					}
					if !hasRow {
						break
					}
				}
				err = stmt.Finalize()
				if err != nil {
					return nil, err
				}
			}
		}
	}

	q.SetNextMigrationVersion(nextVersion)

	_, err = atlas.ExecuteSQL(ctx, "COMMIT", conn, false)
	if err != nil {
		return nil, err
	}

	return q, nil
}
