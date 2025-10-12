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

	"github.com/bottledcode/atlas-db/atlas/options"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/emptypb"
)

var ErrUnbroadcastableQuorum = errors.New("may not be broadcast")

type broadcastQuorum struct {
	nodes map[RegionName][]*QuorumNode
}

func (b *broadcastQuorum) StealTableOwnership(ctx context.Context, in *StealTableOwnershipRequest, opts ...grpc.CallOption) (*StealTableOwnershipResponse, error) {
	return nil, ErrUnbroadcastableQuorum
}

func (b *broadcastQuorum) WriteMigration(ctx context.Context, in *WriteMigrationRequest, opts ...grpc.CallOption) (*WriteMigrationResponse, error) {
	return nil, ErrUnbroadcastableQuorum
}

func (b *broadcastQuorum) AcceptMigration(ctx context.Context, in *WriteMigrationRequest, opts ...grpc.CallOption) (*emptypb.Empty, error) {
	return nil, ErrUnbroadcastableQuorum
}

func (b *broadcastQuorum) JoinCluster(ctx context.Context, in *Node, opts ...grpc.CallOption) (*JoinClusterResponse, error) {
	return nil, ErrUnbroadcastableQuorum
}

func (b *broadcastQuorum) Gossip(ctx context.Context, in *GossipMigration, opts ...grpc.CallOption) (*emptypb.Empty, error) {
	return nil, ErrUnbroadcastableQuorum
}

func (b *broadcastQuorum) Ping(ctx context.Context, in *PingRequest, opts ...grpc.CallOption) (*PingResponse, error) {
	return nil, ErrUnbroadcastableQuorum
}

func (b *broadcastQuorum) ReadKey(ctx context.Context, in *ReadKeyRequest, opts ...grpc.CallOption) (*ReadKeyResponse, error) {
	return nil, ErrUnbroadcastableQuorum
}

func (b *broadcastQuorum) WriteKey(ctx context.Context, in *WriteKeyRequest, opts ...grpc.CallOption) (*WriteKeyResponse, error) {
	return nil, ErrUnbroadcastableQuorum
}

func (b *broadcastQuorum) DeleteKey(ctx context.Context, in *WriteKeyRequest, opts ...grpc.CallOption) (*WriteKeyResponse, error) {
	return nil, ErrUnbroadcastableQuorum
}

func (b *broadcastQuorum) PrefixScan(ctx context.Context, in *PrefixScanRequest, opts ...grpc.CallOption) (*PrefixScanResponse, error) {
	wg := sync.WaitGroup{}
	errs := []error{}
	mu := sync.Mutex{}
	allKeys := make(map[string]bool)
	for _, nodes := range b.nodes {
		for _, node := range nodes {
			wg.Add(1)
			go func(node *QuorumNode) {
				defer wg.Done()
				resp, err := node.PrefixScan(ctx, in, opts...)
				if err != nil {
					mu.Lock()
					errs = append(errs, err)
					mu.Unlock()
					return
				}
				if resp.GetSuccess() {
					mu.Lock()
					for _, key := range resp.GetKeys() {
						allKeys[string(key)] = true
					}
					mu.Unlock()
				}
			}(node)
		}
	}

	wg.Wait()

	keys := make([][]byte, 0, len(allKeys))
	for key := range allKeys {
		keys = append(keys, []byte(key))
	}

	joinedErr := errors.Join(errs...)

	// If some nodes failed but some succeeded, log the partial failure
	if joinedErr != nil && len(errs) > 0 {
		options.Logger.Warn("PrefixScan succeeded on some nodes but failed on others",
			zap.Int("error_count", len(errs)),
			zap.Error(joinedErr))
	}

	return &PrefixScanResponse{
		Success: true,
		Keys:    keys,
	}, joinedErr
}

func (b *broadcastQuorum) CurrentNodeInReplicationQuorum() bool {
	return true
}

func (b *broadcastQuorum) CurrentNodeInMigrationQuorum() bool {
	return true
}
