package bootstrap_test

import (
	"io"
	"net"
	"os"
	"testing"

	"github.com/bottledcode/atlas-db/atlas/bootstrap"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
)

type mockBootstrapServer struct {
	bootstrap.UnimplementedBootstrapServer
}

func (m *mockBootstrapServer) GetBootstrapData(req *bootstrap.BootstrapRequest, stream bootstrap.Bootstrap_GetBootstrapDataServer) error {
	if req.GetVersion() != 1 {
		return stream.Send(&bootstrap.BootstrapResponse{
			Response: &bootstrap.BootstrapResponse_IncompatibleVersion{
				IncompatibleVersion: &bootstrap.IncompatibleVersion{
					NeedsVersion: 1,
				},
			},
		})
	}

	data := []byte("test data")
	for i := 0; i < 3; i++ {
		if err := stream.Send(&bootstrap.BootstrapResponse{
			Response: &bootstrap.BootstrapResponse_BootstrapData{
				BootstrapData: &bootstrap.BootstrapData{
					Data: data,
				},
			},
		}); err != nil {
			return err
		}
	}
	return nil
}

func startMockServer(t *testing.T) (string, func()) {
	lis, err := net.Listen("tcp", "localhost:0")
	require.NoError(t, err)

	s := grpc.NewServer()
	bootstrap.RegisterBootstrapServer(s, &mockBootstrapServer{})

	go func() {
		if err := s.Serve(lis); err != nil {
			t.Fatalf("failed to serve: %v", err)
		}
	}()

	return lis.Addr().String(), func() {
		s.Stop()
		lis.Close()
	}
}

func TestDoBootstrap(t *testing.T) {
	serverAddr, cleanup := startMockServer(t)
	defer cleanup()

	metaFilename := "test_meta.db"
	defer os.Remove(metaFilename)

	err := bootstrap.DoBootstrap(serverAddr, metaFilename)
	require.NoError(t, err)

	file, err := os.Open(metaFilename)
	require.NoError(t, err)
	defer file.Close()

	data, err := io.ReadAll(file)
	require.NoError(t, err)
	require.Equal(t, []byte("test datatest datatest data"), data)
}

func TestDoBootstrap_IncompatibleVersion(t *testing.T) {
	serverAddr, cleanup := startMockServer(t)
	defer cleanup()

	metaFilename := "test_meta.db"
	defer os.Remove(metaFilename)

	err := bootstrap.DoBootstrap(serverAddr, metaFilename)
	require.Error(t, err)
	require.Contains(t, err.Error(), "incompatible version")
}
