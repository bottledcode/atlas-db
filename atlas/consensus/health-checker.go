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

package consensus

import (
	"context"
	"fmt"
	"math/rand"
	"strconv"
	"time"

	"github.com/bottledcode/atlas-db/atlas/options"
	"go.uber.org/zap"
)

// HealthChecker monitors node health and maintains active node lists
type HealthChecker struct {
	manager       *NodeConnectionManager
	interval      time.Duration
	timeout       time.Duration
	maxFailures   int64
	healthTimeout time.Duration // Timeout before considering a ping needed
	maxJitter     time.Duration // Maximum random jitter for ping timing
	stopChan      chan struct{}
}

func NewHealthChecker(manager *NodeConnectionManager) *HealthChecker {
	return &HealthChecker{
		manager:       manager,
		interval:      30 * time.Second, // Check every 30 seconds
		timeout:       5 * time.Second,  // 5 second timeout per check
		maxFailures:   3,                // Remove after 3 consecutive failures
		healthTimeout: 60 * time.Second, // Consider ping needed after 60s of no contact
		maxJitter:     10 * time.Second, // Up to 10s random delay to avoid thundering herd
		stopChan:      make(chan struct{}),
	}
}

// NewHealthCheckerForTesting creates a health checker with no jitter for testing
func NewHealthCheckerForTesting(manager *NodeConnectionManager) *HealthChecker {
	return &HealthChecker{
		manager:       manager,
		interval:      30 * time.Second,
		timeout:       5 * time.Second,
		maxFailures:   3,
		healthTimeout: 0, // Always ping in tests
		maxJitter:     0, // No jitter in tests to avoid race conditions
		stopChan:      make(chan struct{}),
	}
}

// Start begins the health checking routine
func (hc *HealthChecker) Start(ctx context.Context) {
	ticker := time.NewTicker(hc.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-hc.stopChan:
			return
		case <-ticker.C:
			hc.checkAllNodes(ctx)
		}
	}
}

// Stop halts the health checking routine
func (hc *HealthChecker) Stop() {
	close(hc.stopChan)
}

func (hc *HealthChecker) checkAllNodes(ctx context.Context) {
	hc.manager.mu.RLock()
	nodes := make([]*ManagedNode, 0, len(hc.manager.nodes))
	currentNodeId := options.CurrentOptions.ServerId
	skipped := 0
	needsPing := 0

	for _, node := range hc.manager.nodes {
		if node.GetStatus() == NodeStatusRemoved {
			continue
		}
		if node.Id == currentNodeId {
			skipped++
			continue
		}

		// Only ping nodes that haven't been contacted within healthTimeout
		node.mu.RLock()
		lastSeen := node.lastSeen
		node.mu.RUnlock()

		if time.Since(lastSeen) > hc.healthTimeout {
			nodes = append(nodes, node)
			needsPing++
		}
	}
	totalNodes := len(hc.manager.nodes)
	hc.manager.mu.RUnlock()

	options.Logger.Debug("Health check evaluation",
		zap.Int64("current_node_id", currentNodeId),
		zap.Int("skipped_self", skipped),
		zap.Int("needs_ping", needsPing),
		zap.Int("total_nodes", totalNodes))

	for _, node := range nodes {
		// In testing mode, skip jitter to avoid race conditions
		var jitter time.Duration
		if hc.maxJitter > 0 {
			jitter = time.Duration(rand.Int63n(int64(hc.maxJitter)))
		}

		go func(n *ManagedNode, delay time.Duration) {
			if delay > 0 {
				time.Sleep(delay)
			}
			hc.checkNode(ctx, n)
		}(node, jitter)
	}
}

func (hc *HealthChecker) checkNode(ctx context.Context, node *ManagedNode) {
	// Create context with timeout
	checkCtx, cancel := context.WithTimeout(ctx, hc.timeout)
	defer cancel()

	start := time.Now()

	// Add more detailed logging for debugging
	options.Logger.Debug("Starting health check for node",
		zap.Int64("node_id", node.Id),
		zap.String("address", node.GetAddress()),
		zap.Int64("port", node.GetPort()),
		zap.String("full_address", node.GetAddress()+":"+strconv.Itoa(int(node.GetPort()))),
		zap.Duration("timeout", hc.timeout))

	// Ping the node
	err := hc.manager.pingNode(checkCtx, node)
	rtt := time.Since(start)

	if err != nil {
		options.Logger.Debug("Health check failed with details",
			zap.Int64("node_id", node.Id),
			zap.String("address", node.GetAddress()),
			zap.Duration("elapsed", rtt),
			zap.Error(err),
			zap.String("error_type", fmt.Sprintf("%T", err)))
		hc.handleNodeFailure(ctx, node, err)
	} else {
		options.Logger.Debug("Health check succeeded, calling handleNodeSuccess",
			zap.Int64("node_id", node.Id),
			zap.String("address", node.GetAddress()),
			zap.Duration("rtt", rtt))
		hc.handleNodeSuccess(ctx, node, rtt)
	}
}

func (hc *HealthChecker) handleNodeFailure(ctx context.Context, node *ManagedNode, err error) {
	currentStatus := node.GetStatus()

	// Update status first, which increments failures safely
	if currentStatus == NodeStatusActive {
		// First failure, mark as failed but keep trying
		node.UpdateStatus(NodeStatusFailed)
		hc.manager.removeFromActiveNodes(node.Id)
	} else {
		// Already failed, just increment failures
		node.UpdateStatus(NodeStatusFailed)
	}

	failures := node.GetFailures()
	options.Logger.Warn("Node health check failed",
		zap.Int64("node_id", node.Id),
		zap.String("address", node.GetAddress()),
		zap.Error(err),
		zap.Int64("failures", failures))

	// If we've exceeded max failures, remove from quorum permanently
	if failures >= hc.maxFailures {
		options.Logger.Info("Node exceeded max failures, removing from quorum permanently",
			zap.Int64("node_id", node.Id),
			zap.String("address", node.GetAddress()),
			zap.Int64("max_failures", hc.maxFailures))

		// Remove the node from quorum calculations
		quorumManager := GetDefaultQuorumManager(ctx)
		if quorumManager != nil {
			err := quorumManager.RemoveNode(node.Id)
			if err != nil {
				options.Logger.Warn("Failed to remove node from quorum manager",
					zap.Int64("node_id", node.Id),
					zap.Error(err))
			}
		}

		// Close existing connection - node is considered permanently gone
		node.Close()
	}
}

func (hc *HealthChecker) handleNodeSuccess(ctx context.Context, node *ManagedNode, rtt time.Duration) {
	currentStatus := node.GetStatus()

	// Record RTT measurement
	node.AddRTTMeasurement(rtt)

	// Reset lastSeen time so this node won't need pinging for another healthTimeout
	node.mu.Lock()
	node.lastSeen = time.Now()
	node.mu.Unlock()

	if currentStatus != NodeStatusActive {
		options.Logger.Info("Node recovered",
			zap.Int64("node_id", node.Id),
			zap.String("address", node.GetAddress()),
			zap.Duration("rtt", rtt))

		node.UpdateStatus(NodeStatusActive)
		hc.manager.addToActiveNodes(node)

		// Re-add the node to quorum manager if it was previously removed due to failures
		quorumManager := GetDefaultQuorumManager(ctx)
		if quorumManager != nil {
			err := quorumManager.AddNode(ctx, node.Node)
			if err != nil {
				options.Logger.Warn("Failed to re-add recovered node to quorum manager",
					zap.Int64("node_id", node.Id),
					zap.Error(err))
			} else {
				options.Logger.Info("Successfully re-added recovered node to quorum manager",
					zap.Int64("node_id", node.Id),
					zap.String("address", node.GetAddress()))
			}
		}
	}
}

// GetHealthStats returns health statistics for monitoring
type HealthStats struct {
	TotalNodes    int64            `json:"total_nodes"`
	ActiveNodes   int64            `json:"active_nodes"`
	FailedNodes   int64            `json:"failed_nodes"`
	RegionStats   map[string]int64 `json:"region_stats"`
	AverageRTT    time.Duration    `json:"average_rtt"`
	LastCheckTime time.Time        `json:"last_check_time"`
}

func (hc *HealthChecker) GetHealthStats() *HealthStats {
	hc.manager.mu.RLock()
	defer hc.manager.mu.RUnlock()

	stats := &HealthStats{
		RegionStats:   make(map[string]int64),
		LastCheckTime: time.Now(),
	}

	totalRTT := time.Duration(0)
	rttCount := 0

	for _, node := range hc.manager.nodes {
		stats.TotalNodes++

		status := node.GetStatus()
		switch status {
		case NodeStatusActive:
			stats.ActiveNodes++
			regionName := node.GetRegion().GetName()
			stats.RegionStats[regionName]++

			// Add to RTT calculation
			avgRTT := node.GetAverageRTT()
			if avgRTT > 0 {
				totalRTT += avgRTT
				rttCount++
			}
		case NodeStatusFailed, NodeStatusConnecting:
			stats.FailedNodes++
		}
	}

	if rttCount > 0 {
		stats.AverageRTT = totalRTT / time.Duration(rttCount)
	}

	return stats
}
