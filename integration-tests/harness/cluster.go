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
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

type Cluster struct {
	nodes       []*Node
	t           *testing.T
	tempDir     string
	basePort    int
	caddyBinary string
	mu          sync.Mutex
}

func findCaddyBinary() string {
	cwd, err := os.Getwd()
	if err != nil {
		return ""
	}

	dir := cwd
	for {
		caddyPath := filepath.Join(dir, "caddy")
		if _, err := os.Stat(caddyPath); err == nil {
			return caddyPath
		}

		parent := filepath.Dir(dir)
		if parent == dir {
			break
		}
		dir = parent
	}

	return ""
}

type ClusterConfig struct {
	NumNodes    int
	Regions     []string
	BasePort    int
	CaddyBinary string
}

func NewCluster(t *testing.T, cfg ClusterConfig) (*Cluster, error) {
	t.Helper()

	if cfg.BasePort == 0 {
		cfg.BasePort = 10000
	}

	if cfg.CaddyBinary == "" {
		caddyPath := findCaddyBinary()
		if caddyPath == "" {
			return nil, fmt.Errorf("caddy binary not found; please build with 'make caddy' first")
		}
		cfg.CaddyBinary = caddyPath
	} else if !filepath.IsAbs(cfg.CaddyBinary) {
		absPath, err := filepath.Abs(cfg.CaddyBinary)
		if err != nil {
			return nil, fmt.Errorf("resolve caddy binary path: %w", err)
		}
		cfg.CaddyBinary = absPath
	}

	tempDir := t.TempDir()

	cluster := &Cluster{
		nodes:       make([]*Node, 0, cfg.NumNodes),
		t:           t,
		tempDir:     tempDir,
		basePort:    cfg.BasePort,
		caddyBinary: cfg.CaddyBinary,
	}

	t.Cleanup(func() {
		_ = cluster.Stop()
	})

	for i := 0; i < cfg.NumNodes; i++ {
		region := "us-east-1"
		if len(cfg.Regions) > 0 {
			region = cfg.Regions[i%len(cfg.Regions)]
		}

		nodeConfig := NewNodeConfig(i, cfg.BasePort, tempDir, region)

		if i > 0 {
			bootstrapNode := cluster.nodes[0]
			nodeConfig.BootstrapURL = fmt.Sprintf("localhost:%d", bootstrapNode.Config.HTTPSPort)
		}

		node, err := NewNode(nodeConfig, cfg.CaddyBinary)
		if err != nil {
			return nil, fmt.Errorf("create node %d: %w", i, err)
		}

		cluster.nodes = append(cluster.nodes, node)
	}

	return cluster, nil
}

func (c *Cluster) Start() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	for i, node := range c.nodes {
		c.t.Logf("Starting node %d (region: %s, port: %d)", i, node.Config.Region, node.Config.HTTPSPort)

		if err := node.Start(); err != nil {
			return fmt.Errorf("start node %d: %w", i, err)
		}

		if err := node.WaitForStartup(10 * time.Second); err != nil {
			return fmt.Errorf("node %d startup: %w", i, err)
		}

		c.t.Logf("Node %d started successfully", i)

		time.Sleep(2 * time.Second)
	}

	return nil
}

func (c *Cluster) StartNode(index int) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if index < 0 || index >= len(c.nodes) {
		return fmt.Errorf("invalid node index: %d", index)
	}

	node := c.nodes[index]
	if err := node.Start(); err != nil {
		return fmt.Errorf("start node %d: %w", index, err)
	}

	if err := node.WaitForStartup(10 * time.Second); err != nil {
		return fmt.Errorf("node %d startup: %w", index, err)
	}

	return nil
}

func (c *Cluster) StopNode(index int) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if index < 0 || index >= len(c.nodes) {
		return fmt.Errorf("invalid node index: %d", index)
	}

	return c.nodes[index].Stop()
}

func (c *Cluster) Stop() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	var lastErr error
	for i, node := range c.nodes {
		if err := node.Stop(); err != nil {
			c.t.Logf("Error stopping node %d: %v", i, err)
			lastErr = err
		}
	}

	return lastErr
}

func (c *Cluster) GetNode(index int) (*Node, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if index < 0 || index >= len(c.nodes) {
		return nil, fmt.Errorf("invalid node index: %d", index)
	}

	return c.nodes[index], nil
}

func (c *Cluster) GetNodes() []*Node {
	c.mu.Lock()
	defer c.mu.Unlock()

	nodesCopy := make([]*Node, len(c.nodes))
	copy(nodesCopy, c.nodes)
	return nodesCopy
}

func (c *Cluster) NumNodes() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.nodes)
}

func (c *Cluster) WaitForBootstrap(timeout time.Duration) error {
	deadline := time.Now().Add(timeout)

	for _, node := range c.nodes {
		remainingTime := time.Until(deadline)
		if remainingTime <= 0 {
			return fmt.Errorf("timeout waiting for cluster bootstrap")
		}

		if err := node.WaitForStartup(remainingTime); err != nil {
			return fmt.Errorf("node %d bootstrap: %w", node.Config.ID, err)
		}
	}

	time.Sleep(500 * time.Millisecond)

	return nil
}

func (c *Cluster) WaitForKeyPropagation(key string, expectedValue string, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)

	for _, node := range c.nodes {
		remainingTime := time.Until(deadline)
		if remainingTime <= 0 {
			return fmt.Errorf("timeout waiting for key %s to propagate to all nodes", key)
		}

		if err := node.Client().WaitForValue(key, expectedValue, remainingTime); err != nil {
			return fmt.Errorf("node %d: %w", node.Config.ID, err)
		}
	}

	return nil
}

func (c *Cluster) AddNode(region string) (*Node, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	nodeID := len(c.nodes)
	nodeConfig := NewNodeConfig(nodeID, c.basePort, c.tempDir, region)

	if len(c.nodes) > 0 {
		bootstrapNode := c.nodes[0]
		nodeConfig.BootstrapURL = fmt.Sprintf("localhost:%d", bootstrapNode.Config.HTTPSPort)
	}

	node, err := NewNode(nodeConfig, c.caddyBinary)
	if err != nil {
		return nil, fmt.Errorf("create node %d: %w", nodeID, err)
	}

	c.nodes = append(c.nodes, node)
	return node, nil
}
