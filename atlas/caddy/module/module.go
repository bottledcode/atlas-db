package module

import (
	"fmt"
	"github.com/bottledcode/atlas-db/atlas"
	"github.com/bottledcode/atlas-db/atlas/bootstrap"
	"github.com/caddyserver/caddy/v2"
	"github.com/caddyserver/caddy/v2/caddyconfig/caddyfile"
	"github.com/caddyserver/caddy/v2/caddyconfig/httpcaddyfile"
	"github.com/caddyserver/caddy/v2/modules/caddyhttp"
	"google.golang.org/grpc"
	"net/http"
	"os"
	"strconv"
)

type Module struct {
	bootstrapServer *grpc.Server
	ctx             caddy.Context
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
		err = bootstrap.DoBootstrap(atlas.CurrentOptions.BootstrapConnect, atlas.CurrentOptions.MetaFilename)
		if err != nil {
			return
		}
		atlas.Logger.Info("🚀 Bootstrapping Complete")
	}

	m.bootstrapServer = grpc.NewServer()
	bootstrap.RegisterBootstrapServer(m.bootstrapServer, &bootstrap.Server{})

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
			case "server_id":
				var id string
				if !d.Args(&id) {
					return d.ArgErr()
				}
				atlas.CurrentOptions.ServerId, err = strconv.Atoi(id)
				if err != nil {
					return d.Errf("server_id: %v", err)
				}
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
}

// Interface guards
var (
	//_ caddy.App                   = (*FrankenPHPApp)(nil)
	_ caddy.Provisioner           = (*Module)(nil)
	_ caddyhttp.MiddlewareHandler = (*Module)(nil)
	_ caddyfile.Unmarshaler       = (*Module)(nil)
)
