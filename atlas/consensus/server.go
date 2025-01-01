package consensus

import (
	"context"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
)

type Server struct {
	UnimplementedConsensusServer
}

func (s *Server) StealTableOwnership(ctx context.Context, req *StealTableOwnershipRequest) (*StealTableOwnershipRequest, error) {

}
func (s *Server) WriteMigration(context.Context, *WriteMigrationRequest) (*WriteMigrationResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method WriteMigration not implemented")
}
func (s *Server) AcceptMigration(context.Context, *WriteMigrationRequest) (*emptypb.Empty, error) {
	return nil, status.Errorf(codes.Unimplemented, "method AcceptMigration not implemented")
}
func (s *Server) LearnMigration(*LearnMigrationRequest, Consensus_LearnMigrationServer) error {
	return status.Errorf(codes.Unimplemented, "method LearnMigration not implemented")
}
