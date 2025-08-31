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
	"fmt"

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
	keyString := string(key)

	// Get table information to determine current owner/leader
	t, err := tr.GetTable(keyString)
	if err != nil {
		return err
	}

	// Check if we are the current node
	currentNode, err := nr.GetNodeById(options.CurrentOptions.ServerId)
	if err != nil {
		return err
	}
	if currentNode == nil {
		return errors.New("node not yet part of cluster")
	}

	// Create table if it doesn't exist
	if t == nil {
		t = &consensus.Table{
			Name:             keyString,
			ReplicationLevel: consensus.ReplicationLevel_global,
			Owner:            currentNode,
			CreatedAt:        timestamppb.Now(),
			Version:          1,
		}
		err = tr.InsertTable(t)
		if err != nil {
			return err
		}
	}

	// If we're already the owner, execute consensus locally
	if t.Owner.Id == currentNode.Id {
		return executeWriteConsensus(ctx, pool, qm, currentNode, t, keyString, key, value)
	}

	// We're not the owner, attempt to steal ownership as write intent
	q, err := qm.GetQuorum(ctx, keyString)
	if err != nil {
		return err
	}

	tableOwnership, err := q.StealTableOwnership(ctx, &consensus.StealTableOwnershipRequest{
		Sender: currentNode,
		Reason: consensus.StealReason_queryReason,
		Table:  t,
	})
	if err != nil {
		return err
	}

	if tableOwnership.Promised {
		// We got ownership, execute consensus locally
		return executeWriteConsensus(ctx, pool, qm, currentNode, t, keyString, key, value)
	}

	// We didn't get ownership, need to forward to the current leader
	// The response should contain the current leader's information
	leader := tableOwnership.GetFailure().GetTable().GetOwner()
	if leader == nil {
		return errors.New("no leader information available")
	}

	// Forward write to the leader
	connectionManager := consensus.GetNodeConnectionManager(ctx)
	if connectionManager == nil {
		return errors.New("connection manager not available")
	}

	err = connectionManager.ExecuteOnNode(leader.Id, func(client consensus.ConsensusClient) error {
		writeReq := &consensus.WriteKeyRequest{
			Sender: currentNode,
			Key:    keyString,
			Table:  keyString, // Using key as table name for KV operations
			Value:  value,
		}

		response, err := client.WriteKey(ctx, writeReq)
		if err != nil {
			return err
		}

		if !response.Success {
			return fmt.Errorf("remote write failed: %s", response.Error)
		}

		return nil
	})

	if err != nil {
		return fmt.Errorf("remote write failed: %w", err)
	}

	return nil
}

// executeWriteConsensus performs the full consensus protocol for a write operation
func executeWriteConsensus(ctx context.Context, pool *kv.Pool, qm consensus.QuorumManager, currentNode *consensus.Node, table *consensus.Table, tableName string, key []byte, value []byte) error {
	q, err := qm.GetQuorum(ctx, tableName)
	if err != nil {
		return err
	}

	// Get next migration version
	mr := consensus.NewMigrationRepositoryKV(ctx, pool.MetaStore())
	version, err := mr.GetNextVersion(tableName)
	if err != nil {
		return err
	}

	// Create migration request
	migration := &consensus.WriteMigrationRequest{
		Sender: currentNode,
		Migration: &consensus.Migration{
			Version: &consensus.MigrationVersion{
				TableVersion:     table.GetVersion(),
				MigrationVersion: version,
				NodeId:           currentNode.GetId(),
				TableName:        tableName,
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

	// Write migration phase
	mres, err := q.WriteMigration(ctx, migration)
	if err != nil {
		return err
	}

	if !mres.GetSuccess() {
		return errors.New("migration failed due to outdated table")
	}

	// Accept migration phase
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

	// Perform remote read from the leader
	connectionManager := consensus.GetNodeConnectionManager(ctx)
	if connectionManager == nil {
		return nil, errors.New("connection manager not available")
	}

	var remoteValue []byte
	err = connectionManager.ExecuteOnNode(leader.Id, func(client consensus.ConsensusClient) error {
		readReq := &consensus.ReadKeyRequest{
			Sender: currentNode,
			Key:    string(key),
			Table:  string(key), // Using key as table name for KV operations
		}

		response, err := client.ReadKey(ctx, readReq)
		if err != nil {
			return err
		}

		if !response.Success {
			return errors.New(response.Error)
		}

		remoteValue = response.Value
		return nil
	})

	if err != nil {
		return nil, fmt.Errorf("remote read failed: %w", err)
	}

	return remoteValue, nil
}
