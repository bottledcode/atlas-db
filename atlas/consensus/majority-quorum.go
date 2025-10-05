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

package consensus

import (
	"context"
	"errors"
	"sync"

	"github.com/bottledcode/atlas-db/atlas/kv"
	"github.com/bottledcode/atlas-db/atlas/options"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type majorityQuorum struct {
	q1 []*QuorumNode
	q2 []*QuorumNode
}

func (m *majorityQuorum) CurrentNodeInReplicationQuorum() bool {
	for _, node := range m.q2 {
		if node.Id == options.CurrentOptions.ServerId {
			return true
		}
	}
	return false
}

func (m *majorityQuorum) CurrentNodeInMigrationQuorum() bool {
	for _, node := range m.q1 {
		if node.Id == options.CurrentOptions.ServerId {
			return true
		}
	}
	return false
}

var ErrKVPoolNotInitialized = errors.New("KV pool not initialized")
var ErrMetadataStoreClosed = errors.New("metadata store closed")
var ErrCannotStealGroupOwnership = errors.New("cannot steal ownership of a table in a group")

type ErrStealTableOwnershipFailed struct {
	Table *Table
}

func (e ErrStealTableOwnershipFailed) Error() string {
	return "failed to steal ownership of table " + e.Table.String()
}

func (m *majorityQuorum) Gossip(ctx context.Context, in *GossipMigration, opts ...grpc.CallOption) (*emptypb.Empty, error) {
	// Get KV store for metadata operations
	kvPool := kv.GetPool()
	if kvPool == nil {
		return nil, ErrKVPoolNotInitialized
	}

	metaStore := kvPool.MetaStore()
	if metaStore == nil {
		return nil, ErrMetadataStoreClosed
	}

	err := SendGossip(ctx, in, metaStore)
	if err != nil {
		return nil, err
	}

	return &emptypb.Empty{}, nil
}

func broadcast[T any, U any](nodes []*QuorumNode, send func(node *QuorumNode) (T, error), coalesce func(T, U) (U, error)) (U, error) {
	wg := sync.WaitGroup{}
	wg.Add(len(nodes))
	results := make([]T, len(nodes))
	errs := make([]error, len(nodes))
	var nextResult U

	for i, node := range nodes {
		go func(i int, node *QuorumNode) {
			results[i], errs[i] = send(node)
			wg.Done()
		}(i, node)
	}

	wg.Wait()
	err := joinErrs(errs...)
	if err != nil {
		return nextResult, err
	}

	for _, result := range results {
		nextResult, err = coalesce(result, nextResult)
		if err != nil {
			return nextResult, err
		}
	}

	return nextResult, nil
}

func (m *majorityQuorum) StealTableOwnership(ctx context.Context, in *StealTableOwnershipRequest, opts ...grpc.CallOption) (*StealTableOwnershipResponse, error) {
	if in.GetTable().GetGroup() != "" {
		return nil, ErrCannotStealGroupOwnership
	}

	missingMigrations := make([]*Migration, 0)
	highestBallot, err := broadcast(m.q1, func(node *QuorumNode) (*StealTableOwnershipResponse, error) {
		// phase 1a
		return node.StealTableOwnership(ctx, in, opts...)
	}, func(a, b *StealTableOwnershipResponse) (*StealTableOwnershipResponse, error) {
		// phase 1b
		if a.Promised {
			missingMigrations = append(missingMigrations, a.GetSuccess().MissingMigrations...)
			return b, nil
		}

		if a.GetFailure().GetTable().GetVersion() >= in.GetTable().GetVersion() {
			if b != nil && b.GetFailure().GetTable().GetVersion() >= a.GetFailure().GetTable().GetVersion() {
				return b, nil
			}

			return a, nil
		}

		return b, nil
	})
	if err != nil {
		return nil, err
	}

	if highestBallot != nil {
		return &StealTableOwnershipResponse{
			Promised: false,
			Response: &StealTableOwnershipResponse_Failure{
				Failure: &StealTableOwnershipFailure{
					Table: highestBallot.GetFailure().GetTable(),
				},
			},
		}, nil
	}

	return &StealTableOwnershipResponse{
		Promised: true,
		Response: &StealTableOwnershipResponse_Success{
			Success: &StealTableOwnershipSuccess{
				Table:             in.Table,
				MissingMigrations: missingMigrations,
			},
		},
	}, nil
}

func joinErrs(e ...error) error {
	errs := make([]error, 0)
	for _, err := range e {
		if err != nil {
			errs = append(errs, err)
		}
	}
	if len(errs) == 0 {
		return nil
	}

	return errors.Join(errs...)
}

func (m *majorityQuorum) WriteMigration(ctx context.Context, in *WriteMigrationRequest, opts ...grpc.CallOption) (*WriteMigrationResponse, error) {
	failed, err := broadcast(m.q2, func(node *QuorumNode) (*WriteMigrationResponse, error) {
		// phase 2a
		return node.WriteMigration(ctx, in, opts...)
	}, func(a *WriteMigrationResponse, b bool) (bool, error) {
		// phase 2b
		if a.GetSuccess() {
			return b, nil
		}

		return true, nil
	})
	if err != nil {
		return nil, err
	}

	return &WriteMigrationResponse{
		Success: !failed,
	}, nil
}

func (m *majorityQuorum) AcceptMigration(ctx context.Context, in *WriteMigrationRequest, opts ...grpc.CallOption) (*emptypb.Empty, error) {
	return broadcast(m.q2, func(node *QuorumNode) (*emptypb.Empty, error) {
		// phase 3
		return node.AcceptMigration(ctx, in, opts...)
	}, func(a *emptypb.Empty, b *emptypb.Empty) (*emptypb.Empty, error) {
		return a, nil
	})
}

func (m *majorityQuorum) JoinCluster(ctx context.Context, in *Node, opts ...grpc.CallOption) (*JoinClusterResponse, error) {
	return nil, errors.New("no quorum needed to join a cluster")
}

func (m *majorityQuorum) Ping(ctx context.Context, in *PingRequest, opts ...grpc.CallOption) (*PingResponse, error) {
	return nil, errors.New("no quorum needed to ping")
}

func (m *majorityQuorum) ReadKey(ctx context.Context, in *ReadKeyRequest, opts ...grpc.CallOption) (*ReadKeyResponse, error) {
	tr := NewTableRepositoryKV(ctx, kv.GetPool().MetaStore())
	nr := NewNodeRepositoryKV(ctx, kv.GetPool().MetaStore())

	currentNode, err := nr.GetNodeById(options.CurrentOptions.ServerId)
	if err != nil {
		return nil, err
	}

	table, err := tr.GetTable(in.GetTable())
	if err != nil {
		return nil, err
	}

	if table != nil && table.Owner.GetId() == currentNode.GetId() {
		// we are the owner
		s := Server{}
		return s.ReadKey(ctx, in)
	}

	if table == nil {
		table = &Table{
			Name:              in.Table,
			ReplicationLevel:  ReplicationLevel_global,
			Owner:             currentNode,
			CreatedAt:         timestamppb.Now(),
			Version:           -1,
			AllowedRegions:    []string{},
			RestrictedRegions: []string{},
			Group:             "",
			Type:              TableType_table,
		}
	}

	p1r := &StealTableOwnershipRequest{
		Sender: currentNode,
		Reason: StealReason_queryReason,
		Table:  table,
	}

	phase1, err := m.StealTableOwnership(ctx, p1r, opts...)
	if err != nil {
		return nil, err
	}
	if phase1.Promised {
		return nil, ErrStealTableOwnershipFailed{Table: phase1.GetSuccess().GetTable()}
	}

	owner := phase1.GetFailure().GetTable().GetOwner()
	qm := GetDefaultQuorumManager(ctx)
	resp, err := qm.Send(owner, func(node *QuorumNode) (any, error) {
		return node.ReadKey(ctx, in, opts...)
	})
	if err != nil {
		return nil, err
	}
	if resp != nil {
		return resp.(*ReadKeyResponse), nil
	}

	return nil, errors.New("no owner found")
}

func upsertTable(ctx context.Context, tr TableRepository, table *Table) error {
	err := tr.InsertTable(table)
	if err != nil {
		err = tr.UpdateTable(table)
		if err != nil {
			return err
		}
	}
	return nil
}

func (m *majorityQuorum) PrefixScan(ctx context.Context, in *PrefixScanRequest, opts ...grpc.CallOption) (*PrefixScanResponse, error) {
	nr := NewNodeRepositoryKV(ctx, kv.GetPool().MetaStore())
	qm := GetDefaultQuorumManager(ctx)

	var allNodes []*Node
	err := nr.Iterate(func(node *Node) error {
		allNodes = append(allNodes, node)
		return nil
	})
	if err != nil {
		options.Logger.Error("Failed to iterate nodes", zap.Error(err))
		return &PrefixScanResponse{
			Success: false,
			Error:   "failed to get nodes: " + err.Error(),
		}, nil
	}

	// Edge case: no nodes available
	if len(allNodes) == 0 {
		options.Logger.Warn("No nodes available for PrefixScan")
		return &PrefixScanResponse{
			Success: true,
			Keys:    []string{},
		}, nil
	}

	allKeys := make(map[string]bool)
	var mu sync.Mutex
	wg := sync.WaitGroup{}
	wg.Add(len(allNodes))
	errs := make([]error, len(allNodes))

	for i, node := range allNodes {
		go func(i int, node *Node) {
			defer wg.Done()

			resp, err := qm.Send(node, func(quorumNode *QuorumNode) (any, error) {
				return quorumNode.PrefixScan(ctx, in, opts...)
			})

			if err != nil {
				errs[i] = err
				return
			}

			if scanResp, ok := resp.(*PrefixScanResponse); ok && scanResp.Success {
				mu.Lock()
				for _, key := range scanResp.Keys {
					allKeys[key] = true
				}
				mu.Unlock()
			}
		}(i, node)
	}

	wg.Wait()

	// Inspect errors to determine if the broadcast succeeded
	var nonNilErrs []error
	successCount := 0
	for _, err := range errs {
		if err != nil {
			nonNilErrs = append(nonNilErrs, err)
		} else {
			successCount++
		}
	}

	keys := make([]string, 0, len(allKeys))
	for key := range allKeys {
		keys = append(keys, key)
	}

	// If all nodes failed, return failure
	if successCount == 0 && len(nonNilErrs) > 0 {
		joinedErr := errors.Join(nonNilErrs...)
		options.Logger.Error("PrefixScan failed on all nodes",
			zap.Int("total_nodes", len(allNodes)),
			zap.Error(joinedErr))
		return &PrefixScanResponse{
			Success: false,
			Error:   joinedErr.Error(),
		}, nil
	}

	// If some nodes failed but some succeeded, log the partial failure
	if len(nonNilErrs) > 0 {
		joinedErr := errors.Join(nonNilErrs...)
		options.Logger.Warn("PrefixScan succeeded on some nodes but failed on others",
			zap.Int("success_count", successCount),
			zap.Int("error_count", len(nonNilErrs)),
			zap.Int("total_nodes", len(allNodes)),
			zap.Error(joinedErr))
	}

	return &PrefixScanResponse{
		Success: true,
		Keys:    keys,
	}, nil
}

func (m *majorityQuorum) WriteKey(ctx context.Context, in *WriteKeyRequest, opts ...grpc.CallOption) (*WriteKeyResponse, error) {
	tr := NewTableRepositoryKV(ctx, kv.GetPool().MetaStore())
	nr := NewNodeRepositoryKV(ctx, kv.GetPool().MetaStore())

	currentNode, err := nr.GetNodeById(options.CurrentOptions.ServerId)
	if err != nil {
		return nil, err
	}

	table, err := tr.GetTable(in.GetTable())
	if err != nil {
		return nil, err
	}
	if table == nil {
		table = &Table{
			Name:              in.Table,
			ReplicationLevel:  ReplicationLevel_global,
			Owner:             currentNode,
			CreatedAt:         timestamppb.Now(),
			Version:           0,
			AllowedRegions:    []string{},
			RestrictedRegions: []string{},
			Group:             "",
			Type:              TableType_table,
			ShardPrincipals:   []string{},
		}
	}

	table.Owner = currentNode
	table.Version++

	p1r := &StealTableOwnershipRequest{
		Sender: currentNode,
		Reason: StealReason_writeReason,
		Table:  table,
	}

	phase1, err := m.StealTableOwnership(ctx, p1r, opts...)
	if err != nil {
		return nil, err
	}
	if !phase1.Promised {
		table = phase1.GetFailure().GetTable()
		// we are not the leader, so update our tr with the new table information
		err = upsertTable(ctx, tr, table)
		if err != nil {
			return nil, err
		}
		return nil, ErrStealTableOwnershipFailed{Table: table}
	}

	// we are promised the table, but we may be missing migrations
	err = upsertTable(ctx, tr, table)
	if err != nil {
		return nil, err
	}

	s := Server{}
	for _, migration := range phase1.GetSuccess().GetMissingMigrations() {
		_, err = s.AcceptMigration(ctx, &WriteMigrationRequest{
			Sender:    currentNode,
			Migration: migration,
		})
		if err != nil {
			return nil, err
		}
	}
	mr := NewMigrationRepositoryKV(ctx, kv.GetPool().MetaStore())

	version, err := mr.GetNextVersion(in.GetTable())
	if err != nil {
		return nil, err
	}

	// we have completed phase 1, now we move on to phase 2
	p2r := &WriteMigrationRequest{
		Sender: currentNode,
		Migration: &Migration{
			Version: &MigrationVersion{
				TableVersion:     table.GetVersion(),
				MigrationVersion: version,
				NodeId:           currentNode.GetId(),
				TableName:        in.Table,
			},
			Migration: &Migration_Data{
				Data: &DataMigration{
					Time: timestamppb.Now(),
					Session: &DataMigration_Change{
						Change: in.GetValue(),
					},
				},
			},
		},
	}

	p2, err := m.WriteMigration(ctx, p2r, opts...)
	if err != nil {
		return nil, err
	}
	if !p2.Success {
		return nil, ErrStealTableOwnershipFailed{Table: p2.GetTable()}
	}

	_, err = m.AcceptMigration(ctx, p2r, opts...)
	if err != nil {
		return nil, err
	}

	return &WriteKeyResponse{Success: true}, nil
}

// DeleteKey deletes a key using the same ownership-steal and migration path as WriteKey,
// but emits a KVChange_Del instead of KVChange_Set.
func (m *majorityQuorum) DeleteKey(ctx context.Context, in *WriteKeyRequest, opts ...grpc.CallOption) (*WriteKeyResponse, error) {
	tr := NewTableRepositoryKV(ctx, kv.GetPool().MetaStore())
	nr := NewNodeRepositoryKV(ctx, kv.GetPool().MetaStore())

	currentNode, err := nr.GetNodeById(options.CurrentOptions.ServerId)
	if err != nil {
		return nil, err
	}

	table, err := tr.GetTable(in.GetTable())
	if err != nil {
		return nil, err
	}
	if table == nil {
		table = &Table{
			Name:              in.Table,
			ReplicationLevel:  ReplicationLevel_global,
			Owner:             currentNode,
			CreatedAt:         timestamppb.Now(),
			Version:           0,
			AllowedRegions:    []string{},
			RestrictedRegions: []string{},
			Group:             "",
			Type:              TableType_table,
			ShardPrincipals:   []string{},
		}
	}

	table.Owner = currentNode
	table.Version++

	p1r := &StealTableOwnershipRequest{
		Sender: currentNode,
		Reason: StealReason_writeReason,
		Table:  table,
	}

	phase1, err := m.StealTableOwnership(ctx, p1r, opts...)
	if err != nil {
		return nil, err
	}
	if !phase1.Promised {
		table = phase1.GetFailure().GetTable()
		// we are not the leader, so update our tr with the new table information
		err = upsertTable(ctx, tr, table)
		if err != nil {
			return nil, err
		}
		return nil, ErrStealTableOwnershipFailed{Table: table}
	}

	// we are promised the table, but we may be missing migrations
	err = upsertTable(ctx, tr, table)
	if err != nil {
		return nil, err
	}

	s := Server{}
	for _, migration := range phase1.GetSuccess().GetMissingMigrations() {
		_, err = s.AcceptMigration(ctx, &WriteMigrationRequest{
			Sender:    currentNode,
			Migration: migration,
		})
		if err != nil {
			return nil, err
		}
	}
	mr := NewMigrationRepositoryKV(ctx, kv.GetPool().MetaStore())
	version, err := mr.GetNextVersion(in.GetTable())
	if err != nil {
		return nil, err
	}

	// phase 2: broadcast delete migration
	p2r := &WriteMigrationRequest{
		Sender: currentNode,
		Migration: &Migration{
			Version: &MigrationVersion{
				TableVersion:     table.GetVersion(),
				MigrationVersion: version,
				NodeId:           currentNode.GetId(),
				TableName:        in.Table,
			},
			Migration: &Migration_Data{
				Data: &DataMigration{
					Time: timestamppb.Now(),
					Session: &DataMigration_Change{
						Change: in.GetValue(),
					},
				},
			},
		},
	}

	p2, err := m.WriteMigration(ctx, p2r, opts...)
	if err != nil {
		return nil, err
	}
	if !p2.Success {
		return nil, ErrStealTableOwnershipFailed{Table: p2.GetTable()}
	}

	_, err = m.AcceptMigration(ctx, p2r, opts...)
	if err != nil {
		return nil, err
	}

	return &WriteKeyResponse{Success: true}, nil
}
