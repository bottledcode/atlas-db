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

package bootstrap_test

import (
	"context"
	"os"
	"testing"

	"github.com/bottledcode/atlas-db/atlas/bootstrap"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
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

		os.Remove("atlas.db")
		os.Remove("atlas.db-shm")
		os.Remove("atlas.db-wal")
		os.Remove("atlas.meta")
		os.Remove("atlas.meta-shm")
		os.Remove("atlas.meta-wal")
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
