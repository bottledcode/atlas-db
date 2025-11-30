//go:build integration

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

package harness

import (
	"fmt"
	"path/filepath"
)

type NodeConfig struct {
	ID              int
	HTTPSPort       int
	Region          string
	BootstrapURL    string
	Credentials     string
	DBPath          string
	SocketPath      string
	DevelopmentMode bool
	LatencyPreset   string // Latency injection preset: local, single-continent, global, high-latency
}

func GenerateCaddyfile(cfg NodeConfig) string {
	// Enable admin API for pprof profiling (port = HTTPS port + 1000)
	adminPort := cfg.HTTPSPort + 1000
	caddyfile := `{
	admin localhost:%d
	auto_https disable_redirects
	local_certs
}

https://localhost:%d {
	atlas {
		advertise localhost:%d
		region %s
		credentials %s
		db_path %s
		socket %s
		development_mode %t`

	caddyfile = fmt.Sprintf(caddyfile,
		adminPort,
		cfg.HTTPSPort,
		cfg.HTTPSPort,
		cfg.Region,
		cfg.Credentials,
		cfg.DBPath,
		cfg.SocketPath,
		cfg.DevelopmentMode,
	)

	if cfg.BootstrapURL != "" {
		caddyfile += fmt.Sprintf("\n\t\tconnect %s", cfg.BootstrapURL)
	}

	caddyfile += `
	}
}
`

	return caddyfile
}

func NewNodeConfig(id int, basePort int, tempDir string, region string) NodeConfig {
	return NodeConfig{
		ID:              id,
		HTTPSPort:       basePort + id,
		Region:          region,
		Credentials:     "integration-test-shared-secret",
		DBPath:          filepath.Join(tempDir, fmt.Sprintf("node%d", id)),
		SocketPath:      filepath.Join(tempDir, fmt.Sprintf("node%d", id), "socket"),
		DevelopmentMode: true,
	}
}
