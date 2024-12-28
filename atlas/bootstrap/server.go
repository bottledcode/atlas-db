package bootstrap

import (
	"context"
	"github.com/bottledcode/atlas-db/atlas"
	"io"
	"os"
)

type Server struct {
	UnimplementedBootstrapServer
}

func (b *Server) GetBootstrapData(request *BootstrapRequest, stream Bootstrap_GetBootstrapDataServer) (err error) {
	if request.GetVersion() != 1 {
		return stream.Send(&BootstrapResponse{
			Response: &BootstrapResponse_IncompatibleVersion{
				IncompatibleVersion: &IncompatibleVersion{
					NeedsVersion: 1,
				},
			},
		})
	}

	atlas.CreatePool()

	ctx := context.Background()

	conn, err := atlas.MigrationsPool.Take(ctx)
	if err != nil {
		return err
	}
	defer atlas.MigrationsPool.Put(conn)

	_, err = atlas.ExecuteSQL(ctx, "BEGIN IMMEDIATE", conn, false)
	if err != nil {
		return err
	}
	defer func() {
		_, err = atlas.ExecuteSQL(ctx, "ROLLBACK", conn, false)
	}()

	// create a temporary file to store the data
	f, err := os.CreateTemp("", "atlas-*.db")
	if err != nil {
		return err
	}
	f.Close()
	defer os.Remove(f.Name())

	_, err = atlas.ExecuteSQL(ctx, "VACCUM INTO '"+f.Name()+"'", conn, false)
	if err != nil {
		return
	}

	// stream the data to the client
	file, err := os.Open(f.Name())
	if err != nil {
		return err
	}
	defer file.Close()

	buf := make([]byte, 1024*1024)
	for {
		n, err := file.Read(buf)
		if err != nil && err != io.EOF {
			return err
		}
		if n == 0 {
			break
		}

		if err := stream.Send(&BootstrapResponse{
			Response: &BootstrapResponse_BootstrapData{
				BootstrapData: &BootstrapData{
					Data: buf[:n],
				},
			},
		}); err != nil {
			return err
		}
	}

	return nil
}
