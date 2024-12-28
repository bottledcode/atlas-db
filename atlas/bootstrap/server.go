package bootstrap

import (
	"context"
	"github.com/bottledcode/atlas-db/atlas"
)

type Server struct {
	UnimplementedBootstrapServer
}

func (b *Server) GetBootstrapData(ctx context.Context, request *BootstrapRequest) (*BootstrapResponse, error) {
	if request.GetVersion() != 1 {
		return &BootstrapResponse{
			Response: &BootstrapResponse_IncompatibleVersion{
				IncompatibleVersion: &IncompatibleVersion{
					NeedsVersion: 1,
				},
			},
		}, nil
	}

	atlas.CreatePool()

	conn, err := atlas.Pool.Take(ctx)
	if err != nil {
		return nil, err
	}
	defer atlas.Pool.Put(conn)

	_, err = conn.Prep("begin").Step()
	if err != nil {
		return nil, err
	}

	data, err := conn.Serialize("atlas")
	if err != nil {
		return nil, err
	}

	_, err = conn.Prep("rollback").Step()
	if err != nil {
		return nil, err
	}

	return &BootstrapResponse{
		Response: &BootstrapResponse_BootstrapData{
			BootstrapData: &BootstrapData{
				Version: 1,
				Data:    data,
			},
		},
	}, nil
}
