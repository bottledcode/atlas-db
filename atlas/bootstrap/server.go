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

package bootstrap

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

	// TODO: Implement KV-based bootstrap data streaming in later phase
	// For now, just return empty success response
	return stream.Send(&BootstrapResponse{
		Response: &BootstrapResponse_BootstrapData{
			BootstrapData: &BootstrapData{
				Data: []byte("KV bootstrap stubbed"),
			},
		},
	})
}
