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
