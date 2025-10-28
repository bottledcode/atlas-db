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

	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/emptypb"
)

type majorityQuorum struct {
	q1 *broadcastQuorum
	q2 *broadcastQuorum
}

func (m *majorityQuorum) Replicate(ctx context.Context, opts ...grpc.CallOption) (grpc.ClientStreamingClient[ReplicationRequest, ReplicationResponse], error) {
	panic("cannot replicate to a majority quorum")
}

func (m *majorityQuorum) DeReference(ctx context.Context, in *DereferenceRequest, opts ...grpc.CallOption) (grpc.ServerStreamingClient[DereferenceResponse], error) {
	panic("cannot replicate to a majority quorum")
}

func (m *majorityQuorum) CurrentNodeInReplicationQuorum() bool {
	return true
}

func (m *majorityQuorum) CurrentNodeInMigrationQuorum() bool {
	return true
}

func (m *majorityQuorum) StealTableOwnership(ctx context.Context, in *StealTableOwnershipRequest, opts ...grpc.CallOption) (*StealTableOwnershipResponse, error) {
	return m.q1.StealTableOwnership(ctx, in, opts...)
}

func (m *majorityQuorum) WriteMigration(ctx context.Context, in *WriteMigrationRequest, opts ...grpc.CallOption) (*WriteMigrationResponse, error) {
	return m.q2.WriteMigration(ctx, in, opts...)
}

func (m *majorityQuorum) AcceptMigration(ctx context.Context, in *WriteMigrationRequest, opts ...grpc.CallOption) (*emptypb.Empty, error) {
	return m.q2.AcceptMigration(ctx, in, opts...)
}

func (m *majorityQuorum) Ping(ctx context.Context, in *PingRequest, opts ...grpc.CallOption) (*PingResponse, error) {
	return nil, errors.New("no quorum needed to ping")
}

func (m *majorityQuorum) ReadKey(ctx context.Context, in *ReadKeyRequest, opts ...grpc.CallOption) (*ReadKeyResponse, error) {
	return m.q2.ReadKey(ctx, in, opts...)
}

func (m *majorityQuorum) PrefixScan(ctx context.Context, in *PrefixScanRequest, opts ...grpc.CallOption) (*PrefixScanResponse, error) {
	panic("must use broadcast to prefix scan")
}

func (m *majorityQuorum) WriteKey(ctx context.Context, in *WriteKeyRequest, opts ...grpc.CallOption) (*WriteKeyResponse, error) {
	return m.q2.WriteKey(ctx, in, opts...)
}