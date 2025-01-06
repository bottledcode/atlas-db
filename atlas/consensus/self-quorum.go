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
 */

package consensus

import (
	"context"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/emptypb"
)

type selfQuorum struct {
	server *Server
}

func (s *selfQuorum) StealTableOwnership(ctx context.Context, in *StealTableOwnershipRequest, opts ...grpc.CallOption) (*StealTableOwnershipResponse, error) {
	return s.server.StealTableOwnership(ctx, in)
}

func (s *selfQuorum) WriteMigration(ctx context.Context, in *WriteMigrationRequest, opts ...grpc.CallOption) (*WriteMigrationResponse, error) {
	return s.server.WriteMigration(ctx, in)
}

func (s *selfQuorum) AcceptMigration(ctx context.Context, in *WriteMigrationRequest, opts ...grpc.CallOption) (*emptypb.Empty, error) {
	return s.server.AcceptMigration(ctx, in)
}

func (s *selfQuorum) LearnMigration(ctx context.Context, in *LearnMigrationRequest, opts ...grpc.CallOption) (Consensus_LearnMigrationClient, error) {
	panic("cannot learn migrations from self")
}

func (s *selfQuorum) JoinCluster(ctx context.Context, in *Node, opts ...grpc.CallOption) (*JoinClusterResponse, error) {
	return s.server.JoinCluster(ctx, in)
}
