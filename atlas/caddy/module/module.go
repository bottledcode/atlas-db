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

package module

import (
	"bufio"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"strings"

	"github.com/bottledcode/atlas-db/atlas"
	"github.com/bottledcode/atlas-db/atlas/bootstrap"
	"github.com/bottledcode/atlas-db/atlas/consensus"
	"github.com/bottledcode/atlas-db/atlas/socket"
	"github.com/caddyserver/caddy/v2"
	"github.com/caddyserver/caddy/v2/caddyconfig/caddyfile"
	"github.com/caddyserver/caddy/v2/caddyconfig/httpcaddyfile"
	caddycmd "github.com/caddyserver/caddy/v2/cmd"
	"github.com/caddyserver/caddy/v2/modules/caddyhttp"
	"github.com/chzyer/readline"
	"github.com/spf13/cobra"
	"go.uber.org/zap"
	"google.golang.org/grpc"
)

type Module struct {
	bootstrapServer *grpc.Server
	destroySocket   func() error
}

func (m *Module) Cleanup() error {
	if m.destroySocket != nil {
		return m.destroySocket()
	}
	return nil
}

var m Module

func (m *Module) CaddyModule() caddy.ModuleInfo {
	return caddy.ModuleInfo{
		ID: "http.handlers.atlas",
		New: func() caddy.Module {
			return m
		},
	}
}

func (m *Module) Provision(ctx caddy.Context) (err error) {
	atlas.Logger = caddy.Log()

	if atlas.CurrentOptions.BootstrapConnect != "" {
		atlas.Logger.Info("🚀 Bootstrapping Atlas...")
		//err = bootstrap.DoBootstrap(ctx, atlas.CurrentOptions.BootstrapConnect, atlas.CurrentOptions.MetaFilename)
		//if err != nil {
		//	return
		//}
		atlas.Logger.Info("🚀 Bootstrapping Complete")
		atlas.Logger.Info("☄️ Joining Atlas Cluster...")
		//atlas.CreatePool(atlas.CurrentOptions)
		//err = bootstrap.JoinCluster(ctx)
		//if err != nil {
		//	return
		//}

		atlas.Logger.Info("☄️ Atlas Cluster Joined", zap.Int64("NodeID", atlas.CurrentOptions.ServerId))
	} else {
		//atlas.CreatePool(atlas.CurrentOptions)
		//err = bootstrap.InitializeMaybe(ctx)
		//if err != nil {
		//	return
		//}
	}

	m.bootstrapServer = grpc.NewServer()
	bootstrap.RegisterBootstrapServer(m.bootstrapServer, &bootstrap.Server{})
	consensus.RegisterConsensusServer(m.bootstrapServer, &consensus.Server{})

	m.destroySocket, err = socket.ServeSocket(ctx)
	if err != nil {
		return
	}

	atlas.Logger.Info("🌐 Atlas Started")

	return nil
}

func (m *Module) ServeHTTP(w http.ResponseWriter, r *http.Request, next caddyhttp.Handler) error {
	fmt.Println("ServeHTTP called")
	if r.ProtoMajor == 2 && r.Header.Get("content-type") == "application/grpc" {
		// check authorization
		authHeader := r.Header.Get("Authorization")
		if authHeader != "Bearer "+atlas.CurrentOptions.ApiKey {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return nil
		}

		serviceHeader := r.Header.Get("Atlas-Service")
		switch serviceHeader {
		case "Bootstrap":
			fallthrough
		case "Consensus":
			m.bootstrapServer.ServeHTTP(w, r)
		default:
			http.Error(w, "unknown Atlas service", http.StatusNotFound)
		}
	}
	return next.ServeHTTP(w, r)
}

func (m *Module) UnmarshalCaddyfile(d *caddyfile.Dispenser) (err error) {
	for d.Next() {
		for d.NextBlock(0) {
			switch d.Val() {
			case "connect":
				var url string
				if !d.Args(&url) {
					return d.ArgErr()
				}
				atlas.CurrentOptions.BootstrapConnect = url
			case "credentials":
				var key string
				if !d.Args(&key) {
					return d.ArgErr()
				}
				atlas.CurrentOptions.ApiKey = key
			case "db_path":
				var path string
				if !d.Args(&path) {
					return d.ArgErr()
				}
				err = os.Mkdir(path, 0755)
				if err != nil && !os.IsExist(err) {
					return d.Errf("db_path: %v", err)
				} else if err != nil && os.IsExist(err) {
					err = nil
				}

				atlas.CurrentOptions.DbFilename = path + atlas.CurrentOptions.DbFilename
				atlas.CurrentOptions.MetaFilename = path + atlas.CurrentOptions.MetaFilename
			case "region":
				var region string
				if !d.Args(&region) {
					return d.ArgErr()
				}
				atlas.CurrentOptions.Region = region
			case "advertise":
				var address string
				if !d.Args(&address) {
					return d.ArgErr()
				}
				parts, err := caddy.ParseNetworkAddressWithDefaults(address, "tcp", 443)
				if err != nil {
					return d.Errf("advertise: %v", err)
				}
				atlas.CurrentOptions.AdvertiseAddress = parts.Host
				atlas.CurrentOptions.AdvertisePort = parts.StartPort
			case "socket":
				var path string
				if !d.Args(&path) {
					return d.ArgErr()
				}
				atlas.CurrentOptions.SocketPath = path
			default:
				return d.Errf("unknown option: %s", d.Val())
			}
		}
	}
	return
}

func init() {
	caddy.RegisterModule(&Module{})

	httpcaddyfile.RegisterHandlerDirective("atlas", func(h httpcaddyfile.Helper) (caddyhttp.MiddlewareHandler, error) {
		m = Module{}
		err := m.UnmarshalCaddyfile(h.Dispenser)

		return &m, err
	})

	httpcaddyfile.RegisterDirectiveOrder("atlas", "before", "file_server")
	caddycmd.RegisterCommand(caddycmd.Command{
		Name:  "atlas",
		Usage: "<socket_path>",
		Short: "Connect to an Atlas cluster to run commands",
		Long:  "Connect to an Atlas cluster to run commands",
		CobraFunc: func(command *cobra.Command) {
			command.Args = cobra.ExactArgs(1)
			command.RunE = caddycmd.WrapCommandFuncForCobra(func(flags caddycmd.Flags) (int, error) {
				atlas.Logger = caddy.Log()

				socketPath := flags.Arg(0)
				conn, err := net.Dial("unix", socketPath)
				writer := bufio.NewWriter(conn)
				reader := bufio.NewReader(conn)
				if err != nil {
					return 1, err
				}
				defer conn.Close()

				// perform handshake
				handshakePart := 0
				scanner := bufio.NewScanner(reader)
				for scanner.Scan() {
					switch handshakePart {
					case 0:
						welcome := scanner.Text()
						if !strings.HasPrefix(welcome, "WELCOME 1.0") {
							return 1, errors.New("unexpected welcome message")
						}
						_, _ = writer.WriteString("HELLO 1.0 clientId=repl" + socket.EOL)
						_ = writer.Flush()
						handshakePart++
					case 1:
						ready := scanner.Text()
						if !strings.HasPrefix(ready, "READY") {
							return 1, errors.New("unexpected ready message")
						}
						goto ready
					}
				}

			ready:

				atlas.Logger.Info("🌐 Atlas Client Started")

				rl, err := readline.New("> ")
				if err != nil {
					return 1, err
				}
				defer rl.Close()

				for {
					line, err := rl.Readline()
					if err != nil {
						if errors.Is(err, readline.ErrInterrupt) {
							return 0, nil
						}
						return 1, err
					}
					if strings.HasPrefix(strings.ToUpper(line), "EXIT") {
						return 0, nil
					}

					_, err = writer.WriteString(line + socket.EOL)
					if err != nil {
						return 1, err
					}
					err = writer.Flush()
					if err != nil {
						return 1, err
					}

					buf := strings.Builder{}

				keepReading:
					response, err := reader.ReadString('\n')
					if err != nil {
						return 1, err
					}
					buf.WriteString(response)
					if !strings.HasSuffix(response, socket.EOL) {
						goto keepReading
					}
					fmt.Println(strings.TrimSpace(buf.String()))

					if strings.HasPrefix(buf.String(), string(socket.OK)) {
						continue
					}
					if strings.HasPrefix(buf.String(), "ERROR") {
						fields := strings.Fields(buf.String())
						switch fields[1] {
						case string(socket.Fatal):
							return 1, errors.New(strings.Join(fields[2:], " "))
						case string(socket.Warning):
							continue
						case string(socket.Info):
							buf.Reset()
							goto keepReading
						case string(socket.OK):
							continue
						}
					}

					buf.Reset()

					goto keepReading
				}
			})
		},
	})
}

// Interface guards
var (
	_ caddy.Provisioner           = (*Module)(nil)
	_ caddyhttp.MiddlewareHandler = (*Module)(nil)
	_ caddyfile.Unmarshaler       = (*Module)(nil)
	_ caddy.CleanerUpper          = (*Module)(nil)
)
