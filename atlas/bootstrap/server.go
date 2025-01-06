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

package bootstrap

import (
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

	atlas.CreatePool(atlas.CurrentOptions)

	ctx := stream.Context()

	conn, err := atlas.MigrationsPool.Take(ctx)
	if err != nil {
		return err
	}
	defer atlas.MigrationsPool.Put(conn)

	// create a temporary file to store the data
	f, err := os.CreateTemp("", "atlas-*.db")
	if err != nil {
		return err
	}
	f.Close()
	defer os.Remove(f.Name())

	_, err = atlas.ExecuteSQL(ctx, "VACUUM INTO '"+f.Name()+"'", conn, false)
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
