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
	"path/filepath"
	"strings"

	"github.com/bottledcode/atlas-db/atlas/bootstrap"
	"github.com/bottledcode/atlas-db/atlas/consensus"
	"github.com/bottledcode/atlas-db/atlas/kv"
	"github.com/bottledcode/atlas-db/atlas/options"
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
	options.Logger = caddy.Log()

	// Ensure directory structure exists
	_ = os.MkdirAll(filepath.Dir(options.CurrentOptions.DbFilename), 0755)
	_ = os.MkdirAll(filepath.Dir(options.CurrentOptions.MetaFilename), 0755)

	if options.CurrentOptions.BootstrapConnect != "" {
		options.Logger.Info("🚀 Starting Atlas bootstrap process...")

		// Complete bootstrap: download cluster state and join as new node
		err = bootstrap.BootstrapAndJoin(ctx,
			options.CurrentOptions.BootstrapConnect,
			options.CurrentOptions.DbFilename,
			options.CurrentOptions.MetaFilename)
		if err != nil {
			return fmt.Errorf("bootstrap and cluster join failed: %w", err)
		}

		options.Logger.Info("☄️ Atlas bootstrap completed successfully",
			zap.Int64("NodeID", options.CurrentOptions.ServerId))
	} else {
		options.Logger.Info("🌱 Initializing new Atlas cluster...")

		// Initialize KV stores for new cluster
		err = kv.CreatePool(options.CurrentOptions.DbFilename, options.CurrentOptions.MetaFilename)
		if err != nil {
			return fmt.Errorf("failed to create KV pool: %w", err)
		}

		// Initialize as first node in cluster if database is empty
		err = bootstrap.InitializeMaybe(ctx)
		if err != nil {
			return fmt.Errorf("cluster initialization failed: %w", err)
		}

		options.Logger.Info("🌱 New Atlas cluster initialized",
			zap.Int64("NodeID", options.CurrentOptions.ServerId))
	}

	// Start gRPC servers for bootstrap and consensus
	m.bootstrapServer = grpc.NewServer()
	bootstrap.RegisterBootstrapServer(m.bootstrapServer, &bootstrap.Server{})
	consensus.RegisterConsensusServer(m.bootstrapServer, consensus.NewServer())

	// Start socket interface
	m.destroySocket, err = socket.ServeSocket(ctx)
	if err != nil {
		return fmt.Errorf("failed to start socket interface: %w", err)
	}

	options.Logger.Info("🌐 Atlas started successfully")
	return nil
}

func (m *Module) ServeHTTP(w http.ResponseWriter, r *http.Request, next caddyhttp.Handler) error {
	fmt.Println("ServeHTTP called")
	if r.ProtoMajor == 2 && r.Header.Get("content-type") == "application/grpc" {
		// check authorization
		authHeader := r.Header.Get("Authorization")
		if authHeader != "Bearer "+options.CurrentOptions.ApiKey {
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
				options.CurrentOptions.BootstrapConnect = url
			case "credentials":
				var key string
				if !d.Args(&key) {
					return d.ArgErr()
				}
				options.CurrentOptions.ApiKey = key
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

				options.CurrentOptions.DbFilename = path + options.CurrentOptions.DbFilename
				options.CurrentOptions.MetaFilename = path + options.CurrentOptions.MetaFilename
			case "region":
				var region string
				if !d.Args(&region) {
					return d.ArgErr()
				}
				options.CurrentOptions.Region = region
			case "advertise":
				var address string
				if !d.Args(&address) {
					return d.ArgErr()
				}
				parts, err := caddy.ParseNetworkAddressWithDefaults(address, "tcp", 443)
				if err != nil {
					return d.Errf("advertise: %v", err)
				}
				options.CurrentOptions.AdvertiseAddress = parts.Host
				options.CurrentOptions.AdvertisePort = parts.StartPort
			case "socket":
				var path string
				if !d.Args(&path) {
					return d.ArgErr()
				}
				options.CurrentOptions.SocketPath = path
			case "development_mode":
				var mode string
				if !d.Args(&mode) {
					return d.ArgErr()
				}
				switch mode {
				case "true", "on", "yes":
					options.CurrentOptions.DevelopmentMode = true
				case "false", "off", "no":
					options.CurrentOptions.DevelopmentMode = false
				default:
					return d.Errf("development_mode must be true/false, on/off, or yes/no, got: %s", mode)
				}
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
				options.Logger = caddy.Log()

				socketPath := flags.Arg(0)
				conn, err := net.Dial("unix", socketPath)
				writer := bufio.NewWriter(conn)
				reader := bufio.NewReader(conn)
				if err != nil {
					return 1, err
				}
				defer func() { _ = conn.Close() }()

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

				options.Logger.Info("🌐 Atlas Client Started")

				rl, err := readline.New("> ")
				if err != nil {
					return 1, err
				}
				defer func() { _ = rl.Close() }()

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
