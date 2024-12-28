package consensus

import (
	"context"
	"github.com/bottledcode/atlas-db/atlas"
)

type Server struct {
	UnimplementedConsensusServer
}

func (s *Server) ProposeTopologyChange(ctx context.Context, request *ProposeTopologyChangeRequest) (*PromiseTopologyChange, error) {
	conn, err := atlas.MigrationsPool.Take(ctx)
	if err != nil {
		return nil, err
	}
	defer atlas.MigrationsPool.Put(conn)

	switch request.GetChange().(type) {
	case *ProposeTopologyChangeRequest_NodeChange:
	case *ProposeTopologyChangeRequest_RegionChange:
	}
}

func (s *Server) AcceptTopologyChange(ctx context.Context, node *Node) (*Node, error) {
	//TODO implement me
	panic("implement me")
}
