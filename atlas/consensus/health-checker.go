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
	"strconv"
	"time"

	"github.com/bottledcode/atlas-db/atlas/options"
	"go.uber.org/zap"
)

// HealthChecker monitors node health and maintains active node lists
type HealthChecker struct {
	manager        *NodeConnectionManager
	interval       time.Duration
	timeout        time.Duration
	maxFailures    int64
	stopChan       chan struct{}
}

func NewHealthChecker(manager *NodeConnectionManager) *HealthChecker {
	return &HealthChecker{
		manager:     manager,
		interval:    30 * time.Second, // Check every 30 seconds
		timeout:     5 * time.Second,  // 5 second timeout per check
		maxFailures: 3,                // Remove after 3 consecutive failures
		stopChan:    make(chan struct{}),
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
	
	for _, node := range hc.manager.nodes {
		if node.GetStatus() == NodeStatusRemoved {
			continue
		}
		if node.Id == currentNodeId {
			skipped++
			continue
		}
		nodes = append(nodes, node)
	}
	hc.manager.mu.RUnlock()
	
	if skipped > 0 {
		options.Logger.Debug("Skipped self-health checks",
			zap.Int64("current_node_id", currentNodeId),
			zap.Int("skipped_count", skipped),
			zap.Int("checking_count", len(nodes)))
	}

	for _, node := range nodes {
		go hc.checkNode(ctx, node)
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
		hc.handleNodeFailure(node, err)
	} else {
		hc.handleNodeSuccess(node, rtt)
	}
}

func (hc *HealthChecker) handleNodeFailure(node *ManagedNode, err error) {
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

	// If we've exceeded max failures, try to reconnect
	if failures >= hc.maxFailures {
		options.Logger.Info("Attempting to reconnect to failed node",
			zap.Int64("node_id", node.Id),
			zap.String("address", node.GetAddress()))
			
		// Close existing connection and try to reconnect  
		node.Close()
		// Use context.TODO() to indicate we need proper context propagation here
		go hc.manager.connectToNode(context.TODO(), node)
	}
}

func (hc *HealthChecker) handleNodeSuccess(node *ManagedNode, rtt time.Duration) {
	currentStatus := node.GetStatus()
	
	// Record RTT measurement
	node.AddRTTMeasurement(rtt)
	
	if currentStatus != NodeStatusActive {
		options.Logger.Info("Node recovered",
			zap.Int64("node_id", node.Id),
			zap.String("address", node.GetAddress()),
			zap.Duration("rtt", rtt))
		
		node.UpdateStatus(NodeStatusActive)
		hc.manager.addToActiveNodes(node)
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