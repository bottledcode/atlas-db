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
	"slices"
	"sync"
	"sync/atomic"

	"github.com/bottledcode/atlas-db/atlas/options"
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

func (b *broadcastQuorum) RequestSlots(ctx context.Context, in *SlotRequest, opts ...grpc.CallOption) (grpc.ServerStreamingClient[RecordMutation], error) {
	//TODO implement me
	panic("implement me")
}

func (b *broadcastQuorum) Follow(ctx context.Context, in *SlotRequest, opts ...grpc.CallOption) (grpc.ServerStreamingClient[RecordMutation], error) {
	//TODO implement me
	panic("implement me")
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
		mergedResult.MissingRecords = append(mergedResult.MissingRecords, res.MissingRecords...)
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
		if a.Slot.Id < b.Slot.Id {
			return -1
		} else if a.Slot.Id > b.Slot.Id {
			return 1
		}
		return 0
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
	var localErr error
	localId := options.CurrentOptions.ServerId

	b.clients.Range(func(key, value any) bool {
		id := key.(uint64)
		client := value.(grpc.ClientStreamingClient[Req, Res])

		if id == localId {
			// Local node: send synchronously
			localErr = client.Send(req)
		} else {
			// Remote nodes: fire-and-forget
			go func() {
				_ = client.Send(req)
			}()
		}
		return true
	})

	return localErr
}

func (b *broadcastClientStreamingClient[Req, Res]) CloseAndRecv() (*Res, error) {
	var localResp *Res
	var localErr error
	localId := options.CurrentOptions.ServerId

	b.clients.Range(func(key, value any) bool {
		id := key.(uint64)
		client := value.(grpc.ClientStreamingClient[Req, Res])

		if id == localId {
			// Local node: close synchronously and get response
			localResp, localErr = client.CloseAndRecv()
		} else {
			// Remote nodes: fire-and-forget
			go func() {
				_, _ = client.CloseAndRecv()
			}()
		}
		return true
	})

	return localResp, localErr
}

func (b *broadcastClientStreamingClient[Req, Res]) Header() (metadata.MD, error) {
	panic("not implemented")
}

func (b *broadcastClientStreamingClient[Req, Res]) Trailer() metadata.MD {
	panic("not implemented")
}

func (b *broadcastClientStreamingClient[Req, Res]) CloseSend() error {
	var localErr error
	localId := options.CurrentOptions.ServerId

	b.clients.Range(func(key, value any) bool {
		id := key.(uint64)
		client := value.(grpc.ClientStreamingClient[Req, Res])

		if id == localId {
			localErr = client.CloseSend()
		} else {
			go func() {
				_ = client.CloseSend()
			}()
		}
		return true
	})

	return localErr
}

func (b *broadcastClientStreamingClient[Req, Res]) Context() context.Context {
	return b.ctx
}

func (b *broadcastClientStreamingClient[Req, Res]) SendMsg(m any) error {
	var localErr error
	localId := options.CurrentOptions.ServerId

	b.clients.Range(func(key, value any) bool {
		id := key.(uint64)
		client := value.(grpc.ClientStreamingClient[Req, Res])

		if id == localId {
			localErr = client.SendMsg(m)
		} else {
			go func() {
				_ = client.SendMsg(m)
			}()
		}
		return true
	})

	return localErr
}

func (b *broadcastClientStreamingClient[Req, Res]) RecvMsg(m any) error {
	panic("not implemented")
}

func (b *broadcastQuorum) DeReference(ctx context.Context, in *DereferenceRequest, opts ...grpc.CallOption) (grpc.ServerStreamingClient[DereferenceResponse], error) {
	// Sort nodes by RTT (nearest first)
	sortedNodes := make([]*QuorumNode, len(b.nodes))
	copy(sortedNodes, b.nodes)

	slices.SortFunc(sortedNodes, func(a, b *QuorumNode) int {
		aRtt := safeRttDuration(a)
		bRtt := safeRttDuration(b)
		if aRtt < bRtt {
			return -1
		}
		if aRtt > bRtt {
			return 1
		}
		return 0
	})

	// Try each node in order until one succeeds
	var lastErr error
	for _, node := range sortedNodes {
		stream, err := node.DeReference(ctx, in, opts...)
		if err == nil {
			return stream, nil
		}
		lastErr = err
	}

	// All nodes failed
	if lastErr != nil {
		return nil, fmt.Errorf("all nodes failed to dereference: %w", lastErr)
	}
	return nil, fmt.Errorf("no nodes available for dereference")
}

func (b *broadcastQuorum) Ping(ctx context.Context, in *PingRequest, opts ...grpc.CallOption) (*PingResponse, error) {
	return nil, fmt.Errorf("cannot Ping on broadcast quorum")
}

func getCurrentOwnershipState(key []byte) *OwnershipState {
	v, _ := ownership.LoadOrStore(string(key), &OwnershipState{
		mu: sync.RWMutex{},
		promised: &Ballot{
			Key:  key,
			Id:   0,
			Node: options.CurrentOptions.ServerId,
		},
		owned:          false,
		maxAppliedSlot: 0,
		followers:      make([]chan *RecordMutation, 0),
	})
	return v.(*OwnershipState)
}

// FriendlySteal tries to steal ownership of the given key by
// requesting all nodes to hand over ownership cooperatively.
// Returns (success, highestBallotSeen, error).
func (b *broadcastQuorum) FriendlySteal(ctx context.Context, key []byte) (bool, *Ballot, error) {
	owned := getCurrentOwnershipState(key)

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
		return false, nil, err
	}

	if int(success.Load()) != len(b.nodes) {
		// Failed to get unanimous promise - return highest ballot seen for efficient retry
		return false, highestBallot, nil
	}

	// Replay all missing records in order
	allRecords := make([]*RecordMutation, 0)
	for _, slotMap := range missing {
		for _, rec := range slotMap {
			allRecords = append(allRecords, rec)
		}
	}

	slices.SortFunc(allRecords, func(a, b *RecordMutation) int {
		if a.Slot.Id < b.Slot.Id {
			return -1
		} else if a.Slot.Id > b.Slot.Id {
			return 1
		}
		return 0
	})

	for _, rec := range allRecords {
		// Re-propose using the new leadership ballot to satisfy promised quorums
		proposal := proto.Clone(rec).(*RecordMutation)
		proposal.Ballot = proto.Clone(nextBallot).(*Ballot)
		proposal.Committed = false

		// Phase 2a: Accept missing record
		accepted, err := b.WriteMigration(ctx, &WriteMigrationRequest{
			Record: proposal,
		})
		if err != nil {
			return false, nil, fmt.Errorf("failed to write missing migration during friendly steal: %w", err)
		}
		if !accepted.Accepted {
			return false, nil, fmt.Errorf("missing migration not accepted during friendly steal")
		}

		// Phase 3: Commit missing record
		_, err = b.AcceptMigration(ctx, &WriteMigrationRequest{
			Record: proposal,
		})
		if err != nil {
			return false, nil, fmt.Errorf("failed to commit missing migration during friendly steal: %w", err)
		}
	}

	// Mark ourselves as owner
	owned.mu.Lock()
	defer owned.mu.Unlock()
	owned.owned = true
	owned.promised = nextBallot
	owned.maxAppliedSlot = highestSlot

	return true, nextBallot, nil
}

func (b *broadcastQuorum) PrefixScan(ctx context.Context, in *PrefixScanRequest, opts ...grpc.CallOption) (*PrefixScanResponse, error) {
	keys := sync.Map{}

	err := b.broadcast(func(node *QuorumNode) error {
		resp, err := node.PrefixScan(ctx, in, opts...)
		if err != nil {
			return err
		}
		for _, v := range resp.Keys {
			// Store as string since []byte is not hashable
			keys.Store(string(v), nil)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}

	result := &PrefixScanResponse{Keys: make([][]byte, 0)}
	keys.Range(func(key, value any) bool {
		result.Keys = append(result.Keys, []byte(key.(string)))
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

func (b *broadcastQuorum) ReadRecord(ctx context.Context, in *ReadRecordRequest, opts ...grpc.CallOption) (*ReadRecordResponse, error) {
	// For broadcast quorum, we can read from any node (they should all have the data)
	// Try the first responsive node
	for _, node := range b.nodes {
		resp, err := node.ReadRecord(ctx, in, opts...)
		if err == nil && resp.Success {
			return resp, nil
		}
	}
	return &ReadRecordResponse{Success: false}, fmt.Errorf("no nodes responded successfully")
}
