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

package atlas

import (
	"context"
	"errors"

	"github.com/bottledcode/atlas-db/atlas/consensus"
	"github.com/bottledcode/atlas-db/atlas/kv"
	"github.com/bottledcode/atlas-db/atlas/options"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func WriteKey(ctx context.Context, builder *kv.KeyBuilder, value []byte) error {
	pool := kv.GetPool()
	if pool == nil {
		return errors.New("pool is nil")
	}

	qm := consensus.GetDefaultQuorumManager(ctx)
	tr := consensus.NewTableRepositoryKV(ctx, pool.MetaStore())
	nr := consensus.NewNodeRepositoryKV(ctx, pool.MetaStore())

	key := builder.Build()

	t, err := tr.GetTable(string(key))
	if err != nil {
		return err
	}

	n, err := nr.GetNodeById(options.CurrentOptions.ServerId)
	if err != nil {
		return err
	}
	if n == nil {
		return errors.New("node not yet part of cluster")
	}

	if t == nil {
		t = &consensus.Table{
			Name:             string(key),
			ReplicationLevel: consensus.ReplicationLevel_global,
			Owner:            n,
			CreatedAt:        timestamppb.Now(),
			Version:          1,
		}
		err = tr.InsertTable(t)
		if err != nil {
			return err
		}
	}

	q, err := qm.GetQuorum(ctx, string(key))
	if err != nil {
		return err
	}

	tableOwnership, err := q.StealTableOwnership(ctx, &consensus.StealTableOwnershipRequest{
		Sender: n,
		Reason: consensus.StealReason_queryReason,
		Table:  t,
	})
	if err != nil {
		return err
	}

	if tableOwnership.Promised {
		// we own the table?
	}

	mr := consensus.NewMigrationRepositoryKV(ctx, pool.MetaStore())
	v, err := mr.GetNextVersion(string(key))
	if err != nil {
		return err
	}

	migration := &consensus.WriteMigrationRequest{
		Sender: n,
		Migration: &consensus.Migration{
			Version: &consensus.MigrationVersion{
				TableVersion:     t.GetVersion(),
				MigrationVersion: v,
				NodeId:           n.GetId(),
				TableName:        string(key),
			},
			Migration: &consensus.Migration_Data{
				Data: &consensus.DataMigration{
					Session: &consensus.DataMigration_Change{
						Change: &consensus.KVChange{
							Operation: &consensus.KVChange_Set{
								Set: &consensus.SetChange{
									Key:   key,
									Value: value,
								},
							},
						},
					},
				},
			},
		},
	}

	mres, err := q.WriteMigration(ctx, migration)
	if err != nil {
		return err
	}

	if !mres.GetSuccess() {
		return errors.New("todo: migration failed due to outdated table")
	}

	_, err = q.AcceptMigration(ctx, migration)
	if err != nil {
		return err
	}

	return nil
}

func GetKey(ctx context.Context, builder *kv.KeyBuilder) ([]byte, error) {
	pool := kv.GetPool()
	if pool == nil {
		return nil, errors.New("pool is nil")
	}

	qm := consensus.GetDefaultQuorumManager(ctx)
	tr := consensus.NewTableRepositoryKV(ctx, pool.MetaStore())
	nr := consensus.NewNodeRepositoryKV(ctx, pool.MetaStore())

	key := builder.Build()

	// Get table information to determine current owner/leader
	t, err := tr.GetTable(string(key))
	if err != nil {
		return nil, err
	}

	if t == nil {
		// Table doesn't exist, key not found
		return nil, nil
	}

	// Check if we are the current leader for this table
	currentNode, err := nr.GetNodeById(options.CurrentOptions.ServerId)
	if err != nil {
		return nil, err
	}
	if currentNode == nil {
		return nil, errors.New("node not yet part of cluster")
	}

	// If we're already the owner, read locally
	if t.Owner.Id == currentNode.Id {
		dataStore := pool.DataStore()
		if dataStore == nil {
			return nil, errors.New("data store not available")
		}

		value, err := dataStore.Get(ctx, key)
		if err != nil {
			if errors.Is(err, kv.ErrKeyNotFound) {
				return nil, nil
			}
			return nil, err
		}
		return value, nil
	}

	// We're not the owner, attempt to get read intent (steal ownership)
	q, err := qm.GetQuorum(ctx, string(key))
	if err != nil {
		return nil, err
	}

	tableOwnership, err := q.StealTableOwnership(ctx, &consensus.StealTableOwnershipRequest{
		Sender: currentNode,
		Reason: consensus.StealReason_queryReason,
		Table:  t,
	})
	if err != nil {
		return nil, err
	}

	if tableOwnership.Promised {
		// We got ownership, read locally
		dataStore := pool.DataStore()
		if dataStore == nil {
			return nil, errors.New("data store not available")
		}

		value, err := dataStore.Get(ctx, key)
		if err != nil {
			if errors.Is(err, kv.ErrKeyNotFound) {
				return nil, nil
			}
			return nil, err
		}
		return value, nil
	}

	// We didn't get ownership, need to read from the current leader
	// The response should contain the current leader's information
	leader := tableOwnership.GetFailure().GetTable().GetOwner()
	if leader == nil {
		return nil, errors.New("no leader information available")
	}

	// TODO: Implement remote read from leader node
	// For now, return an error indicating we need to implement remote reads
	return nil, errors.New("remote reads from leader not yet implemented")
}
