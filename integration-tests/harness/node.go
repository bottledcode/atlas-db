//go:build integration
// +build integration

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
	"bufio"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"time"
)

type Node struct {
	Config      NodeConfig
	cmd         *exec.Cmd
	caddyfile   string
	stdout      io.ReadCloser
	stderr      io.ReadCloser
	client      *SocketClient
	mu          sync.Mutex
	started     bool
	logFile     *os.File
	caddyBinary string
}

func NewNode(config NodeConfig, caddyBinary string) (*Node, error) {
	if err := os.MkdirAll(config.DBPath, 0755); err != nil {
		return nil, fmt.Errorf("create db path: %w", err)
	}

	caddyfileContent := GenerateCaddyfile(config)
	caddyfilePath := filepath.Join(config.DBPath, "Caddyfile")

	if err := os.WriteFile(caddyfilePath, []byte(caddyfileContent), 0644); err != nil {
		return nil, fmt.Errorf("write caddyfile: %w", err)
	}

	return &Node{
		Config:      config,
		caddyfile:   caddyfilePath,
		client:      NewSocketClient(config.SocketPath),
		caddyBinary: caddyBinary,
	}, nil
}

func (n *Node) Start() error {
	n.mu.Lock()
	defer n.mu.Unlock()

	if n.started {
		return fmt.Errorf("node already started")
	}

	logFilePath := filepath.Join(n.Config.DBPath, "caddy.log")
	var err error
	n.logFile, err = os.Create(logFilePath)
	if err != nil {
		return fmt.Errorf("create log file: %w", err)
	}

	n.cmd = exec.Command(n.caddyBinary, "run", "--config", n.caddyfile)
	n.cmd.Dir = n.Config.DBPath

	n.stdout, err = n.cmd.StdoutPipe()
	if err != nil {
		return fmt.Errorf("create stdout pipe: %w", err)
	}

	n.stderr, err = n.cmd.StderrPipe()
	if err != nil {
		return fmt.Errorf("create stderr pipe: %w", err)
	}

	go n.logOutput("stdout", n.stdout)
	go n.logOutput("stderr", n.stderr)

	if err := n.cmd.Start(); err != nil {
		return fmt.Errorf("start caddy: %w", err)
	}

	n.started = true

	if err := n.waitForSocket(); err != nil {
		_ = n.Stop()
		return fmt.Errorf("wait for socket: %w", err)
	}

	return nil
}

func (n *Node) logOutput(prefix string, reader io.Reader) {
	scanner := bufio.NewScanner(reader)
	for scanner.Scan() {
		line := scanner.Text()
		timestamp := time.Now().Format("15:04:05.000")
		logLine := fmt.Sprintf("[%s][node-%d][%s] %s\n", timestamp, n.Config.ID, prefix, line)

		if n.logFile != nil {
			_, _ = n.logFile.WriteString(logLine)
		}
	}
}

func (n *Node) waitForSocket() error {
	timeout := time.After(10 * time.Second)
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-timeout:
			return fmt.Errorf("timeout waiting for socket %s", n.Config.SocketPath)
		case <-ticker.C:
			if _, err := os.Stat(n.Config.SocketPath); err == nil {
				time.Sleep(200 * time.Millisecond)
				return nil
			}
		}
	}
}

func (n *Node) Stop() error {
	n.mu.Lock()
	defer n.mu.Unlock()

	if !n.started {
		return nil
	}

	if n.client != nil {
		_ = n.client.Close()
	}

	if n.cmd != nil && n.cmd.Process != nil {
		if err := n.cmd.Process.Kill(); err != nil {
			return fmt.Errorf("kill process: %w", err)
		}

		_ = n.cmd.Wait()
	}

	if n.logFile != nil {
		_ = n.logFile.Close()
	}

	n.started = false
	return nil
}

func (n *Node) Client() *SocketClient {
	return n.client
}

func (n *Node) IsRunning() bool {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.started && n.cmd != nil && n.cmd.Process != nil
}

func (n *Node) GetLogPath() string {
	return filepath.Join(n.Config.DBPath, "caddy.log")
}

func (n *Node) WaitForStartup(timeout time.Duration) error {
	deadline := time.Now().Add(timeout)

	for time.Now().Before(deadline) {
		if n.IsRunning() {
			if err := n.client.Connect(); err == nil {
				return nil
			}
		}
		time.Sleep(100 * time.Millisecond)
	}

	return fmt.Errorf("node %d did not start within timeout", n.Config.ID)
}
