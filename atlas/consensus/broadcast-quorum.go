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
	"time"

	"github.com/bottledcode/atlas-db/atlas/options"
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
		// Phase 1a failed - find highest ballot from rejections for retry
		var highestBallot *Ballot
		results.Range(func(key, value any) bool {
			res := value.(*StealTableOwnershipResponse)
			if !res.Promised && res.HighestBallot != nil {
				if highestBallot == nil || highestBallot.Less(res.HighestBallot) {
					highestBallot = res.HighestBallot
				}
			}
			return true
		})
		return &StealTableOwnershipResponse{
			Promised:      false,
			HighestBallot: highestBallot,
		}, nil
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

	// dedupe mergedResult.MissingRecords by key and slot
	recordMap := make(map[string]map[uint64]*RecordMutation)
	for _, rec := range mergedResult.MissingRecords {
		key := string(rec.Slot.Key)
		if recordMap[key] == nil {
			recordMap[key] = make(map[uint64]*RecordMutation)
		}
		recordMap[key][rec.Slot.Id] = rec
	}
	mergedResult.MissingRecords = make([]*RecordMutation, 0)
	for _, slotMap := range recordMap {
		for _, rec := range slotMap {
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
		return nil
	})
	if err != nil {
		return nil, err
	}

	return &broadcastClientStreamingClient[ReplicationRequest, ReplicationResponse]{
		clients: &clients,
		ctx:     ctx,
	}, nil
}

type broadcastClientStreamingClient[Req any, Res any] struct {
	clients *sync.Map
	ctx     context.Context
}

func (b *broadcastClientStreamingClient[Req, Res]) Send(req *Req) error {
	errs := sync.Map{}
	var wg sync.WaitGroup

	b.clients.Range(func(key, value any) bool {
		wg.Add(1)
		go func(id uint64, client grpc.ClientStreamingClient[Req, Res]) {
			defer wg.Done()
			err := client.Send(req)
			if err != nil {
				errs.Store(id, err)
			}
		}(key.(uint64), value.(grpc.ClientStreamingClient[Req, Res]))
		return true
	})

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

	b.clients.Range(func(key, value any) bool {
		wg.Add(1)
		go func(id uint64, client grpc.ClientStreamingClient[Req, Res]) {
			defer wg.Done()
			resp, err := client.CloseAndRecv()
			if err != nil {
				errs.Store(id, err)
			}
			responses.Store(id, resp)
		}(key.(uint64), value.(grpc.ClientStreamingClient[Req, Res]))
		return true
	})

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

	b.clients.Range(func(key, value any) bool {
		wg.Add(1)
		go func(id uint64, client grpc.ClientStreamingClient[Req, Res]) {
			defer wg.Done()
			err := client.CloseSend()
			if err != nil {
				errs.Store(id, err)
			}
		}(key.(uint64), value.(grpc.ClientStreamingClient[Req, Res]))
		return true
	})

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

	b.clients.Range(func(key, value any) bool {
		wg.Add(1)
		go func(id uint64, client grpc.ClientStreamingClient[Req, Res]) {
			defer wg.Done()
			err := client.SendMsg(m)
			if err != nil {
				errs.Store(id, err)
			}
		}(key.(uint64), value.(grpc.ClientStreamingClient[Req, Res]))
		return true
	})

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

// aggressiveSteal tries to steal ownership of the given key by
// repeatedly attempting to steal with an intent of succeeding.
func (b *broadcastQuorum) aggressiveSteal(ctx context.Context, key []byte) error {
	number := uint64(1)
	const maxRetries = 10

	for retry := 0; retry < maxRetries; retry++ {
		// Attempt to steal ownership with increasing ballot
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
			// Retry with higher ballot
			if theft.HighestBallot != nil {
				number = theft.HighestBallot.Id + 1
			} else {
				number++
			}
			runtime.Gosched()
			continue
		}

		// Successfully stole ownership - now replay missing records
		slices.SortFunc(theft.MissingRecords, func(a, b *RecordMutation) int {
			return int(a.Slot.Id - b.Slot.Id)
		})

		for _, rec := range theft.MissingRecords {
			// Phase 2a: Accept the missing record
			accepted, err := b.WriteMigration(ctx, &WriteMigrationRequest{
				Record: rec,
			})
			if err != nil {
				return fmt.Errorf("failed to write missing migration record during steal: %w", err)
			}
			if !accepted.Accepted {
				return fmt.Errorf("missing migration record not accepted during steal")
			}

			// Phase 3: Commit the missing record
			_, err = b.AcceptMigration(ctx, &WriteMigrationRequest{
				Record: rec,
			})
			if err != nil {
				return fmt.Errorf("failed to commit missing migration record during steal: %w", err)
			}
		}

		return nil
	}

	return fmt.Errorf("failed to steal ownership after %d retries", maxRetries)
}

func (b *broadcastQuorum) ReadKey(ctx context.Context, in *ReadKeyRequest, opts ...grpc.CallOption) (*ReadKeyResponse, error) {
	// Try to find current owner and read from them
	var result atomic.Value
	var foundOwner atomic.Bool

	_ = b.broadcast(func(node *QuorumNode) error {
		r, err := node.ReadKey(ctx, in, opts...)
		if err != nil {
			return nil // Don't fail broadcast if one node doesn't own the key
		}
		if r.Success && foundOwner.CompareAndSwap(false, true) {
			result.Store(r)
		}
		return nil
	})

	if foundOwner.Load() {
		return result.Load().(*ReadKeyResponse), nil
	}

	// No owner found - aggressively steal ownership to serve the read
	err := b.aggressiveSteal(ctx, in.Key)
	if err != nil {
		return nil, err
	}

	// Read from our local state machine after stealing
	ret, ok := stateMachine.Load(in.Key)
	if !ok {
		return &ReadKeyResponse{
			Success: false,
		}, fmt.Errorf("key not found after stealing ownership: %v", string(in.Key))
	}
	record := ret.(*Record)

	// Wait for watermark if specified
	if in.Watermark > 0 {
		const maxSpins = 100
		const spinDelay = 10 * time.Millisecond

		for attempt := 0; attempt < maxSpins; attempt++ {
			if record.MaxSlot >= in.Watermark {
				break
			}
			time.Sleep(spinDelay)

			// Re-read in case it was updated
			ret, ok = stateMachine.Load(in.Key)
			if !ok {
				return nil, fmt.Errorf("key disappeared during watermark wait")
			}
			record = ret.(*Record)
		}

		if record.MaxSlot < in.Watermark {
			return nil, fmt.Errorf("timeout waiting for watermark %d, current slot: %d",
				in.Watermark, record.MaxSlot)
		}
	}

	// Check ACLs before returning
	if !canRead(ctx, record) {
		return &ReadKeyResponse{
			Success: false,
		}, fmt.Errorf("principal isn't allowed to read this key")
	}

	return &ReadKeyResponse{
		Success: true,
		Value:   record.Data,
	}, nil
}

// softSteal tries to query all nodes for the highest ballot
// and updates local ownership state accordingly without
// actually stealing ownership.
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

// friendlySteal tries to steal ownership of the given key by
// requesting all nodes to hand over ownership cooperatively.
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

	owned.mu.RLock()
	nextBallot := proto.Clone(owned.promised).(*Ballot)
	owned.mu.RUnlock()

	nextBallot.Id += 1
	nextBallot.Node = options.CurrentOptions.ServerId

	success := atomic.Int32{}
	mu := sync.Mutex{}
	highestBallot := nextBallot
	highestSlot := uint64(0)

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
		defer mu.Unlock()

		// Track the highest ballot seen for potential retry
		if p.HighestBallot != nil && highestBallot.Less(p.HighestBallot) {
			highestBallot = p.HighestBallot
		}

		// Track the highest slot for log coordination
		if p.HighestSlot != nil && p.HighestSlot.Id > highestSlot {
			highestSlot = p.HighestSlot.Id
		}

		// Collect missing records
		for _, rec := range p.MissingRecords {
			recKey := string(rec.Slot.Key)
			if missing[recKey] == nil {
				missing[recKey] = make(map[uint64]*RecordMutation)
			}
			missing[recKey][rec.Slot.Id] = rec
		}

		return nil
	})
	if err != nil {
		return false, err
	}

	if int(success.Load()) != len(b.nodes) {
		// Failed to get unanimous promise
		return false, nil
	}

	// Replay all missing records in order
	allRecords := make([]*RecordMutation, 0)
	for _, slotMap := range missing {
		for _, rec := range slotMap {
			allRecords = append(allRecords, rec)
		}
	}

	slices.SortFunc(allRecords, func(a, b *RecordMutation) int {
		return int(a.Slot.Id - b.Slot.Id)
	})

	for _, rec := range allRecords {
		// Phase 2a: Accept missing record
		accepted, err := b.WriteMigration(ctx, &WriteMigrationRequest{
			Record: rec,
		})
		if err != nil {
			return false, fmt.Errorf("failed to write missing migration during friendly steal: %w", err)
		}
		if !accepted.Accepted {
			return false, fmt.Errorf("missing migration not accepted during friendly steal")
		}

		// Phase 3: Commit missing record
		_, err = b.AcceptMigration(ctx, &WriteMigrationRequest{
			Record: rec,
		})
		if err != nil {
			return false, fmt.Errorf("failed to commit missing migration during friendly steal: %w", err)
		}
	}

	// Mark ourselves as owner
	owned.mu.Lock()
	defer owned.mu.Unlock()
	owned.owned = true
	owned.promised = nextBallot
	owned.maxAppliedSlot = highestSlot

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

	// Ensure we own the key before proceeding
	if !owned.owned {
		theft, err := b.friendlySteal(ctx, in.Key)
		if err != nil {
			return nil, err
		}
		if !theft {
			// For broadcast quorum, we cannot forward - we need all nodes
			return &WriteKeyResponse{
				Success: false,
			}, fmt.Errorf("failed to steal ownership for write on broadcast quorum")
		}
	}

	// Assign slot and ballot for this write
	owned.mu.Lock()
	slotId := owned.maxAppliedSlot + 1
	owned.maxAppliedSlot = slotId
	ballot := proto.Clone(owned.promised).(*Ballot)
	owned.mu.Unlock()

	// Prepare the mutation with proper slot and ballot
	mutation := proto.Clone(in.Mutation).(*RecordMutation)
	mutation.Slot = &Slot{
		Key:  in.Key,
		Id:   slotId,
		Node: options.CurrentOptions.ServerId,
	}
	mutation.Ballot = ballot

	// Phase 2a: Broadcast write to all nodes
	accepts := atomic.Int32{}
	err := b.broadcast(func(node *QuorumNode) error {
		resp, err := node.WriteMigration(ctx, &WriteMigrationRequest{
			Record: mutation,
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

	// Phase 3: Commit the write to all nodes
	_, err = b.AcceptMigration(ctx, &WriteMigrationRequest{
		Record: mutation,
	})
	if err != nil {
		return nil, err
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
