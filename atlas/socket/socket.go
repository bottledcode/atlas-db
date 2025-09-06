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

package socket

import (
	"context"
	"net"
	"os"
	"time"

	"github.com/bottledcode/atlas-db/atlas/options"
	"go.uber.org/zap"
)

func ServeSocket(ctx context.Context) (func() error, error) {
	// create the unix socket
	ln, err := net.Listen("unix", options.CurrentOptions.SocketPath)
	if err != nil {
		// try to remove the socket file if it exists
		_ = os.Remove(options.CurrentOptions.SocketPath)
		ln, err = net.Listen("unix", options.CurrentOptions.SocketPath)
		if err != nil {
			return nil, err
		}
	}

	// start the server
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			default:
				conn, err := ln.Accept()
				if err != nil {
					options.Logger.Error("Error accepting connection", zap.Error(err))
					continue
				}
				c := &Socket{
					timeout: 5 * time.Minute,
				}
				go func() {
					ctx, done := context.WithCancel(ctx)
					defer done()
					c.HandleConnection(conn, ctx)
				}()
			}
		}
	}()

	return func() error {
		return ln.Close()
	}, nil
}

type ErrorCode string

const (
	OK      ErrorCode = "OK"
	Info    ErrorCode = "INFO"
	Warning ErrorCode = "WARN"
	Fatal   ErrorCode = "FATAL"
)
