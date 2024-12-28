package bootstrap_test

import (
	"context"
	"github.com/bottledcode/atlas-db/atlas/bootstrap"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"testing"
)

func TestGetBootstrapData(t *testing.T) {
	server := &bootstrap.Server{}

	t.Run("Incompatible Version", func(t *testing.T) {
		req := &bootstrap.BootstrapRequest{Version: 2}
		stream := &mockBootstrapStream{}

		err := server.GetBootstrapData(req, stream)
		require.NoError(t, err)
		require.Len(t, stream.responses, 1)
		resp := stream.responses[0].GetIncompatibleVersion()
		require.NotNil(t, resp)
		require.Equal(t, int64(1), resp.NeedsVersion)
	})

	t.Run("Successful Data Retrieval", func(t *testing.T) {
		req := &bootstrap.BootstrapRequest{Version: 1}
		stream := &mockBootstrapStream{}

		err := server.GetBootstrapData(req, stream)
		require.NoError(t, err)
		require.Greater(t, len(stream.responses), 0)
		for _, resp := range stream.responses {
			data := resp.GetBootstrapData()
			require.NotNil(t, data)
			require.NotEmpty(t, data.Data)
		}
	})
}

type mockBootstrapStream struct {
	grpc.ServerStream
	responses []*bootstrap.BootstrapResponse
}

func (m *mockBootstrapStream) Send(resp *bootstrap.BootstrapResponse) error {
	m.responses = append(m.responses, resp)
	return nil
}

func (m *mockBootstrapStream) Context() context.Context {
	return context.Background()
}
