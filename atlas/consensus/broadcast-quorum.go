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
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"

	"github.com/bottledcode/atlas-db/atlas/options"
	"go.uber.org/zap"
	"golang.org/x/exp/slices"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/emptypb"
)

// A broadcastQuorum sends requests to all nodes in the cluster.
// It requires ALL nodes to agree on operations to succeed.
type broadcastQuorum struct {
	nodes []*QuorumNode
}

func (b *broadcastQuorum) broadcast(f func(node *QuorumNode) error) error {
	errs := make([]error, len(b.nodes))

	wg := &sync.WaitGroup{}
	for i, node := range b.nodes {
		wg.Add(1)
		go func(i int, node *QuorumNode) {
			defer wg.Done()
			err := f(node)
			if err != nil {
				errs[i] = err
			}
		}(i, node)
	}
	wg.Wait()

	return errors.Join(errs...)
}

func (b *broadcastQuorum) StealTableOwnership(ctx context.Context, in *StealTableOwnershipRequest, opts ...grpc.CallOption) (*StealTableOwnershipResponse, error) {
	var successCount atomic.Int32
	results := sync.Map{}

	err := b.broadcast(func(node *QuorumNode) error {
		result, err := node.StealTableOwnership(ctx, in, opts...)
		if err != nil {
			return err
		}

		if result.Promised {
			successCount.Add(1)
		}
		results.Store(node.Id, result)
		return nil
	})
	if err != nil {
		return nil, err
	}

	if int(successCount.Load()) != len(b.nodes) {
		return &StealTableOwnershipResponse{Promised: false}, nil
	}

	// Merge results from all nodes
	mergedResult := &StealTableOwnershipResponse{Promised: true}
	results.Range(func(key, value any) bool {
		res := value.(*StealTableOwnershipResponse)
		if res.HighestBallot != nil {
			if mergedResult.HighestBallot == nil || mergedResult.HighestBallot.Less(res.HighestBallot) {
				mergedResult.HighestBallot = res.HighestBallot
			}
		}
		for _, entry := range res.MissingRecords {
			mergedResult.MissingRecords = append(mergedResult.MissingRecords, entry)
		}
		return true
	})

	// dedupe mergedResult.MissingRecords
	recordMap := make(map[string]map[uint64]*RecordMutation)
	for _, rec := range mergedResult.MissingRecords {
		recordMap[string(rec.Slot.Key)][rec.Slot.Id] = rec
	}
	mergedResult.MissingRecords = make([]*RecordMutation, 0, len(recordMap))
	for _, rec := range recordMap {
		for _, rec := range rec {
			mergedResult.MissingRecords = append(mergedResult.MissingRecords, rec)
		}
	}

	slices.SortFunc(mergedResult.MissingRecords, func(a, b *RecordMutation) int {
		return int(a.Slot.Id - b.Slot.Id)
	})

	return mergedResult, nil
}

func (b *broadcastQuorum) WriteMigration(ctx context.Context, in *WriteMigrationRequest, opts ...grpc.CallOption) (*WriteMigrationResponse, error) {
	var successCount atomic.Int32
	results := sync.Map{}

	err := b.broadcast(func(node *QuorumNode) error {
		result, err := node.WriteMigration(ctx, in, opts...)
		if err != nil {
			return err
		}

		if result.Accepted {
			successCount.Add(1)
		}
		results.Store(node.Id, result)
		return nil
	})
	if err != nil {
		return nil, err
	}

	if int(successCount.Load()) != len(b.nodes) {
		return &WriteMigrationResponse{Accepted: false}, nil
	}

	return &WriteMigrationResponse{Accepted: true}, nil
}

func (b *broadcastQuorum) AcceptMigration(ctx context.Context, in *WriteMigrationRequest, opts ...grpc.CallOption) (*emptypb.Empty, error) {
	err := b.broadcast(func(node *QuorumNode) error {
		_, err := node.AcceptMigration(ctx, in, opts...)
		return err
	})
	return &emptypb.Empty{}, err
}

func (b *broadcastQuorum) Replicate(ctx context.Context, opts ...grpc.CallOption) (grpc.ClientStreamingClient[ReplicationRequest, ReplicationResponse], error) {
	clients := sync.Map{}

	err := b.broadcast(func(node *QuorumNode) error {
		client, err := node.Replicate(ctx, opts...)
		if err != nil {
			return err
		}
		clients.Store(node.Id, client)
	})
	if err != nil {
		return nil, err
	}

	return &broadcastClientStreamingClient[ReplicationRequest, ReplicationResponse]{clients: &clients}, nil
}

type broadcastClientStreamingClient[Req any, Res any] struct {
	clients map[uint64]grpc.ClientStreamingClient[Req, Res]
	ctx     context.Context
}

func (b *broadcastClientStreamingClient[Req, Res]) Send(req *Req) error {
	errs := sync.Map{}
	var wg sync.WaitGroup
	for id, client := range b.clients {
		wg.Add(1)
		go func(id uint64, client grpc.ClientStreamingClient[Req, Res]) {
			defer wg.Done()
			err := client.Send(req)
			if err != nil {
				errs.Store(id, err)
			}
		}(id, client)
	}
	wg.Wait()

	err := make([]error, 0)
	errs.Range(func(key, value interface{}) bool {
		err = append(err, value.(error))
		return true
	})
	return errors.Join(err...)
}

func (b *broadcastClientStreamingClient[Req, Res]) CloseAndRecv() (*Res, error) {
	errs := sync.Map{}
	var wg sync.WaitGroup
	responses := sync.Map{}
	for id, client := range b.clients {
		wg.Add(1)
		go func(id uint64, client grpc.ClientStreamingClient[Req, Res]) {
			defer wg.Done()
			resp, err := client.CloseAndRecv()
			if err != nil {
				errs.Store(id, err)
			}
			responses.Store(id, resp)
		}(id, client)
	}
	wg.Wait()

	err := make([]error, 0)
	errs.Range(func(key, value interface{}) bool {
		err = append(err, value.(error))
		return true
	})
	if len(err) > 0 {
		return nil, errors.Join(err...)
	}

	// For simplicity, return the response from the first client
	var firstResp *Res
	responses.Range(func(key, value interface{}) bool {
		firstResp = value.(*Res)
		return false
	})
	return firstResp, nil
}

func (b *broadcastClientStreamingClient[Req, Res]) Header() (metadata.MD, error) {
	panic("not implemented")
}

func (b *broadcastClientStreamingClient[Req, Res]) Trailer() metadata.MD {
	panic("not implemented")
}

func (b *broadcastClientStreamingClient[Req, Res]) CloseSend() error {
	errs := sync.Map{}
	var wg sync.WaitGroup
	for id, client := range b.clients {
		wg.Add(1)
		go func(id uint64, client grpc.ClientStreamingClient[Req, Res]) {
			defer wg.Done()
			err := client.CloseSend()
			if err != nil {
				errs.Store(id, err)
			}
		}(id, client)
	}
	wg.Wait()

	err := make([]error, 0)
	errs.Range(func(key, value interface{}) bool {
		err = append(err, value.(error))
		return true
	})
	return errors.Join(err...)
}

func (b *broadcastClientStreamingClient[Req, Res]) Context() context.Context {
	return b.ctx
}

func (b *broadcastClientStreamingClient[Req, Res]) SendMsg(m any) error {
	errs := sync.Map{}
	var wg sync.WaitGroup
	for id, client := range b.clients {
		wg.Add(1)
		go func(id uint64, client grpc.ClientStreamingClient[Req, Res]) {
			defer wg.Done()
			err := client.SendMsg(m)
			if err != nil {
				errs.Store(id, err)
			}
		}(id, client)
	}
	wg.Wait()

	err := make([]error, 0)
	errs.Range(func(key, value interface{}) bool {
		err = append(err, value.(error))
		return true
	})
	return errors.Join(err...)
}

func (b *broadcastClientStreamingClient[Req, Res]) RecvMsg(m any) error {
	panic("not implemented")
}

func (b *broadcastQuorum) DeReference(ctx context.Context, in *DereferenceRequest, opts ...grpc.CallOption) (grpc.ServerStreamingClient[DereferenceResponse], error) {
	return nil, fmt.Errorf("cannot DeReference on broadcast quorum")
}

func (b *broadcastQuorum) Ping(ctx context.Context, in *PingRequest, opts ...grpc.CallOption) (*PingResponse, error) {
	return nil, fmt.Errorf("cannot Ping on broadcast quorum")
}

func (b *broadcastQuorum) aggressiveSteal(ctx context.Context, key []byte) error {
	number := uint64(1)

retry:
	// no leader running... so we need to aggressively steal it
	theft, err := b.StealTableOwnership(ctx, &StealTableOwnershipRequest{
		Ballot: &Ballot{
			Key:  key,
			Id:   number,
			Node: options.CurrentOptions.ServerId,
		},
	})
	if err != nil {
		return err
	}

	if !theft.Promised {
		number = theft.HighestBallot.Id + 1
		runtime.Gosched()
		goto retry
	}

	slices.SortFunc(theft.MissingRecords, func(a, b *RecordMutation) int {
		return int(a.Slot.Id - b.Slot.Id)
	})

	for _, rec := range theft.MissingRecords {
		// we now need to adopt these records as our own
		accepted, err := b.WriteMigration(ctx, &WriteMigrationRequest{
			Record: rec,
		})
		// todo: what happens if nodes already have something in this slot?
		if err != nil {
			options.Logger.Warn("failed to write migration record during read key theft: %v", zap.Error(err))
			continue
		}
		if !accepted.Accepted {
			options.Logger.Warn("migration record not accepted during read key theft")
			continue
		}
		_, err = b.AcceptMigration(ctx, &WriteMigrationRequest{
			Record: rec,
		})
		if err != nil {
			options.Logger.Warn("failed to commit migration record during read key theft: %v", zap.Error(err))
			continue
		}
	}

	return nil
}

func (b *broadcastQuorum) ReadKey(ctx context.Context, in *ReadKeyRequest, opts ...grpc.CallOption) (*ReadKeyResponse, error) {
spin:
	result := &ReadKeyResponse{}
	_ = b.broadcast(func(node *QuorumNode) error {
		r, err := node.ReadKey(ctx, in, opts...)
		if err != nil {
			return err
		}
		// there should only be a single node that actually replies
		result = r
		return nil
	})
	if result.Success {
		return result, nil
	}

	err := b.aggressiveSteal(ctx, in.Key)
	if err != nil {
		return nil, err
	}

	// now just return our cached value
	ret, _ := stateMachine.Load(in.Key)
	record := ret.(*Record)

	if record.MaxSlot < in.Watermark {
		runtime.Gosched()
		goto spin
	}

	return &ReadKeyResponse{
		Success: true,
		Value:   record.Data,
	}, nil
}

func (b *broadcastQuorum) softSteal(ctx context.Context, key []byte) error {
	ownVal, _ := ownership.LoadOrStore(key, &OwnershipState{
		mu: sync.RWMutex{},
		promised: &Ballot{
			Key:  key,
			Id:   0,
			Node: options.CurrentOptions.ServerId,
		},
		owned:          false,
		maxAppliedSlot: 0,
	})
	owned := ownVal.(*OwnershipState)

	return b.broadcast(func(node *QuorumNode) error {
		p, err := node.StealTableOwnership(ctx, &StealTableOwnershipRequest{
			Ballot: &Ballot{
				Key:  key,
				Id:   0,
				Node: 0,
			},
		})
		if err != nil {
			return err
		}
		if p.Promised {
			panic("soft steal should not succeed")
		}

		owned.mu.Lock()
		defer owned.mu.Unlock()

		if owned.promised.Less(p.HighestBallot) {
			owned.promised = p.HighestBallot
			owned.maxAppliedSlot = p.HighestSlot.Id
		}

		return nil
	})
}

func (b *broadcastQuorum) friendlySteal(ctx context.Context, key []byte) (bool, error) {
	ownVal, _ := ownership.LoadOrStore(key, &OwnershipState{
		mu: sync.RWMutex{},
		promised: &Ballot{
			Key:  key,
			Id:   0,
			Node: options.CurrentOptions.ServerId,
		},
		owned:          false,
		maxAppliedSlot: 0,
	})
	owned := ownVal.(*OwnershipState)

	success := atomic.Int32{}
	highestBallot := owned.promised
	highestSlot := uint64(0)
	nextBallot := proto.Clone(highestBallot).(*Ballot)
	nextBallot.Id += 1
	nextBallot.Node = options.CurrentOptions.ServerId
	mu := sync.Mutex{}

	missing := make(map[string]map[uint64]*RecordMutation)

	err := b.broadcast(func(node *QuorumNode) error {
		p, err := node.StealTableOwnership(ctx, &StealTableOwnershipRequest{
			Ballot: nextBallot,
		})
		if err != nil {
			return err
		}

		if p.Promised {
			success.Add(1)
		}

		mu.Lock()
		if highestBallot.Less(p.HighestBallot) {
			highestBallot = p.HighestBallot
			nextBallot.Id = p.HighestSlot.Id + 1
		}

		if p.HighestSlot.Id > highestSlot {
			highestSlot = p.HighestSlot.Id
		}

		for _, rec := range p.MissingRecords {
			missing[string(rec.Slot.Key)] = make(map[uint64]*RecordMutation)
			missing[string(rec.Slot.Key)][rec.Slot.Id] = rec
		}
		mu.Unlock()
		return nil
	})
	if err != nil {
		return false, err
	}

	if int(success.Load()) != len(b.nodes) {
		return false, nil
	}

	for _, rec := range missing {
		for _, rec := range rec {
			accepted, err := b.WriteMigration(ctx, &WriteMigrationRequest{
				Record: rec,
			})
			if err != nil {
				options.Logger.Warn("failed to write migration record during friendly steal: %v", zap.Error(err))
				continue
			}
			if !accepted.Accepted {
				options.Logger.Warn("migration record not accepted during friendly steal")
				continue
			}
			_, err = b.AcceptMigration(ctx, &WriteMigrationRequest{
				Record: rec,
			})
			if err != nil {
				options.Logger.Warn("failed to commit migration record during friendly steal: %v", zap.Error(err))
				continue
			}
		}
	}

	owned.mu.Lock()
	defer owned.mu.Unlock()
	owned.owned = true

	return true, nil
}

func (b *broadcastQuorum) WriteKey(ctx context.Context, in *WriteKeyRequest, opts ...grpc.CallOption) (*WriteKeyResponse, error) {
	ownedVal, _ := ownership.LoadOrStore(in.Key, &OwnershipState{
		mu: sync.RWMutex{},
		promised: &Ballot{
			Key:  in.Key,
			Id:   0,
			Node: options.CurrentOptions.ServerId,
		},
		owned:          false,
		maxAppliedSlot: 0,
	})
	owned := ownedVal.(*OwnershipState)

	if !owned.owned {
		if theft, err := b.friendlySteal(ctx, in.Key); err != nil {
			return nil, err
		} else if !theft {
			// forward write to the leader
			leader := owned.promised.Node
			for _, node := range b.nodes {
				if uint64(node.Id) == leader {
					return node.WriteKey(ctx, in, opts...)
				}
			}
		}
	}

	accepts := atomic.Int32{}

	// per wpaxos, if we are already the owner, we can skip directly to phase2
	err := b.broadcast(func(node *QuorumNode) error {
		resp, err := node.WriteMigration(ctx, &WriteMigrationRequest{
			Record: in.Mutation,
		})
		if err != nil {
			return err
		}
		if resp.Accepted {
			accepts.Add(1)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}

	if int(accepts.Load()) != len(b.nodes) {
		return &WriteKeyResponse{Success: false}, nil
	}

	return &WriteKeyResponse{Success: true}, nil
}

func (b *broadcastQuorum) PrefixScan(ctx context.Context, in *PrefixScanRequest, opts ...grpc.CallOption) (*PrefixScanResponse, error) {
	keys := sync.Map{}

	err := b.broadcast(func(node *QuorumNode) error {
		resp, err := node.PrefixScan(ctx, in, opts...)
		if err != nil {
			return err
		}
		for _, v := range resp.Keys {
			keys.Store(v, nil)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}

	result := &PrefixScanResponse{Keys: make([][]byte, 0)}
	keys.Range(func(key, value any) bool {
		result.Keys = append(result.Keys, key.([]byte))
		return true
	})

	return result, nil
}

func (b *broadcastQuorum) CurrentNodeInReplicationQuorum() bool {
	return true
}

func (b *broadcastQuorum) CurrentNodeInMigrationQuorum() bool {
	return true
}
