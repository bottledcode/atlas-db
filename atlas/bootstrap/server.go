package bootstrap

import (
	"context"
	"zombiezen.com/go/sqlite/sqlitemigration"
)

type Server struct {
	Pool *sqlitemigration.Pool
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

	conn, err := b.Pool.Take(ctx)
	if err != nil {
		return nil, err
	}
	defer b.Pool.Put(conn)

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
