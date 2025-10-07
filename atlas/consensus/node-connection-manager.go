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
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/bottledcode/atlas-db/atlas/kv"
	"github.com/bottledcode/atlas-db/atlas/options"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type NodeStatus int

const (
	NodeStatusUnknown NodeStatus = iota
	NodeStatusConnecting
	NodeStatusActive
	NodeStatusFailed
	NodeStatusRemoved
)

// ManagedNode represents a node with its connection state
type ManagedNode struct {
	*Node
	connection *grpc.ClientConn
	client     ConsensusClient
	closer     func()
	lastSeen   time.Time
	failures   int64
	status     NodeStatus
	rttHistory []time.Duration
	mu         sync.RWMutex
}

func (m *ManagedNode) GetStatus() NodeStatus {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.status
}

func (m *ManagedNode) UpdateStatus(status NodeStatus) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.status = status
	switch status {
	case NodeStatusActive:
		m.lastSeen = time.Now()
		m.failures = 0
	case NodeStatusFailed:
		m.failures++
	}
}

func (m *ManagedNode) GetFailures() int64 {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.failures
}

func (m *ManagedNode) GetAverageRTT() time.Duration {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if len(m.rttHistory) == 0 {
		return m.Rtt.AsDuration()
	}

	total := time.Duration(0)
	for _, rtt := range m.rttHistory {
		total += rtt
	}
	return total / time.Duration(len(m.rttHistory))
}

func (m *ManagedNode) AddRTTMeasurement(rtt time.Duration) {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Keep only last 10 measurements
	if len(m.rttHistory) >= 10 {
		m.rttHistory = m.rttHistory[1:]
	}
	m.rttHistory = append(m.rttHistory, rtt)

	// Update protobuf RTT with average
	if len(m.rttHistory) > 0 {
		avg := time.Duration(0)
		for _, r := range m.rttHistory {
			avg += r
		}
		avg /= time.Duration(len(m.rttHistory))
		m.Rtt = durationpb.New(avg)
	}
}

func (m *ManagedNode) Close() {
	m.mu.Lock()
	defer m.mu.Unlock()

	options.Logger.Info("Closing managed node connection",
		zap.Int64("node_id", m.Id),
		zap.String("address", m.GetAddress()),
		zap.Bool("closer_nil", m.closer == nil),
		zap.Bool("client_nil", m.client == nil))

	if m.closer != nil {
		m.closer()
		m.closer = nil
		m.connection = nil
		m.client = nil
	}
	m.status = NodeStatusRemoved
}

// NodeConnectionManager centralizes all node connection management
type NodeConnectionManager struct {
	mu          sync.RWMutex
	nodes       map[int64]*ManagedNode
	activeNodes map[string][]*ManagedNode // keyed by region name
	storage     NodeRepository
	healthCheck *HealthChecker
	ctx         context.Context
	cancel      context.CancelFunc
}

var (
	connectionManager     *NodeConnectionManager
	connectionManagerOnce sync.Once
)

// GetNodeConnectionManager returns the singleton connection manager
func GetNodeConnectionManager(ctx context.Context) *NodeConnectionManager {
	connectionManagerOnce.Do(func() {
		kvPool := kv.GetPool()
		if kvPool != nil {
			metaStore := kvPool.MetaStore()
			if metaStore != nil {
				storage := NewNodeRepository(ctx, metaStore)

				// Create context with cancellation for cleanup
				managerCtx, cancel := context.WithCancel(ctx)

				connectionManager = &NodeConnectionManager{
					nodes:       make(map[int64]*ManagedNode),
					activeNodes: make(map[string][]*ManagedNode),
					storage:     storage,
					ctx:         managerCtx,
					cancel:      cancel,
				}

				// Start health checker
				connectionManager.healthCheck = NewHealthChecker(connectionManager)
				go connectionManager.healthCheck.Start(managerCtx)

				// Load existing nodes from storage
				connectionManager.loadNodesFromStorage(managerCtx)
			}
		}
	})
	return connectionManager
}

func (ncm *NodeConnectionManager) loadNodesFromStorage(ctx context.Context) {
	_ = ncm.storage.Iterate(func(node *Node) error {
		managedNode := &ManagedNode{
			Node:       node,
			status:     NodeStatusUnknown,
			rttHistory: make([]time.Duration, 0),
		}

		ncm.mu.Lock()
		ncm.nodes[node.Id] = managedNode
		ncm.mu.Unlock()

		// Try to establish connection in background
		go ncm.connectToNode(ctx, managedNode)

		return nil
	})
}

// AddNode registers a new node and attempts connection
func (ncm *NodeConnectionManager) AddNode(ctx context.Context, node *Node) error {
	managedNode := &ManagedNode{
		Node:       node,
		status:     NodeStatusConnecting,
		rttHistory: make([]time.Duration, 0),
	}

	ncm.mu.Lock()
	ncm.nodes[node.Id] = managedNode
	ncm.mu.Unlock()

	// Attempt connection in background
	go ncm.connectToNode(ctx, managedNode)

	return nil
}

func (ncm *NodeConnectionManager) connectToNode(ctx context.Context, node *ManagedNode) {
	node.UpdateStatus(NodeStatusConnecting)

	address := node.GetAddress() + ":" + strconv.Itoa(int(node.GetPort()))

	options.Logger.Info("Attempting to connect to node",
		zap.Int64("node_id", node.Id),
		zap.String("address", address))

	client, closer, err := getNewClient(address)
	if err != nil {
		options.Logger.Error("Failed to establish gRPC connection to node",
			zap.Int64("node_id", node.Id),
			zap.String("address", address),
			zap.Error(err),
			zap.String("error_type", fmt.Sprintf("%T", err)))
		node.UpdateStatus(NodeStatusFailed)
		return
	}

	node.mu.Lock()
	node.connection = nil // We don't expose the connection directly
	node.client = client
	node.closer = closer
	node.mu.Unlock()

	options.Logger.Info("Client successfully assigned to node",
		zap.Int64("node_id", node.Id),
		zap.String("address", address),
		zap.Bool("client_nil", client == nil))

	// Mark as active immediately without testing - health checker will validate later
	node.UpdateStatus(NodeStatusActive)
	ncm.addToActiveNodes(node)

	options.Logger.Info("Successfully connected to node",
		zap.Int64("node_id", node.Id),
		zap.String("address", address))
}

func (ncm *NodeConnectionManager) pingNode(ctx context.Context, node *ManagedNode) error {
	// Use dedicated Ping RPC for health checks
	node.mu.RLock()
	client := node.client
	status := node.status
	closer := node.closer
	node.mu.RUnlock()

	options.Logger.Info("Ping attempt - checking client state",
		zap.Int64("node_id", node.Id),
		zap.String("address", node.GetAddress()),
		zap.Bool("client_nil", client == nil),
		zap.Bool("closer_nil", closer == nil),
		zap.Int("status", int(status)))

	if client == nil {
		options.Logger.Debug("Ping failed: no gRPC client available",
			zap.Int64("node_id", node.Id),
			zap.String("address", node.GetAddress()))
		return errors.New("no client available")
	}

	options.Logger.Debug("Sending Ping to node",
		zap.Int64("node_id", node.Id),
		zap.String("address", node.GetAddress()))

	// Measure RTT by capturing start time
	start := time.Now()

	// Send ping request
	pingReq := &PingRequest{
		SenderNodeId: options.CurrentOptions.ServerId,
		Timestamp:    timestamppb.Now(),
	}

	response, err := client.Ping(ctx, pingReq)
	rtt := time.Since(start)

	if err != nil {
		options.Logger.Debug("Ping failed",
			zap.Int64("node_id", node.Id),
			zap.String("address", node.GetAddress()),
			zap.Error(err))
		return err
	}

	options.Logger.Debug("Ping successful",
		zap.Int64("node_id", node.Id),
		zap.String("address", node.GetAddress()),
		zap.Bool("success", response.Success),
		zap.Int64("responder_id", response.ResponderNodeId),
		zap.Duration("rtt", rtt))

	// Record RTT measurement on successful ping
	node.AddRTTMeasurement(rtt)

	// Update lastSeen time on successful ping
	node.mu.Lock()
	node.lastSeen = time.Now()
	node.mu.Unlock()

	return nil
}

func (ncm *NodeConnectionManager) addToActiveNodes(node *ManagedNode) {
	regionName := node.GetRegion().GetName()

	ncm.mu.Lock()
	defer ncm.mu.Unlock()

	// Remove from any existing region (in case region changed)
	for region, nodes := range ncm.activeNodes {
		for i, n := range nodes {
			if n.Id == node.Id {
				ncm.activeNodes[region] = append(nodes[:i], nodes[i+1:]...)
				break
			}
		}
	}

	// Add to current region
	if ncm.activeNodes[regionName] == nil {
		ncm.activeNodes[regionName] = make([]*ManagedNode, 0)
	}
	ncm.activeNodes[regionName] = append(ncm.activeNodes[regionName], node)
}

func (ncm *NodeConnectionManager) removeFromActiveNodes(nodeID int64) {
	ncm.mu.Lock()
	defer ncm.mu.Unlock()

	for region, nodes := range ncm.activeNodes {
		for i, node := range nodes {
			if node.Id == nodeID {
				ncm.activeNodes[region] = append(nodes[:i], nodes[i+1:]...)
				return
			}
		}
	}
}

// GetActiveNodesByRegion returns currently reachable nodes in a region
func (ncm *NodeConnectionManager) GetActiveNodesByRegion(region string) []*ManagedNode {
	ncm.mu.RLock()
	defer ncm.mu.RUnlock()

	nodes := ncm.activeNodes[region]
	result := make([]*ManagedNode, len(nodes))
	copy(result, nodes)
	return result
}

// GetAllActiveNodes returns all currently reachable nodes
func (ncm *NodeConnectionManager) GetAllActiveNodes() map[string][]*ManagedNode {
	ncm.mu.RLock()
	defer ncm.mu.RUnlock()

	result := make(map[string][]*ManagedNode)
	for region, nodes := range ncm.activeNodes {
		result[region] = make([]*ManagedNode, len(nodes))
		copy(result[region], nodes)
	}
	return result
}

// ExecuteOnNode executes a consensus operation on a specific node
func (ncm *NodeConnectionManager) ExecuteOnNode(nodeID int64, operation func(ConsensusClient) error) error {
	ncm.mu.RLock()
	node, exists := ncm.nodes[nodeID]
	ncm.mu.RUnlock()

	if !exists {
		return fmt.Errorf("node %d not found", nodeID)
	}

	if node.GetStatus() != NodeStatusActive {
		return fmt.Errorf("node %d is not active (status: %d)", nodeID, node.GetStatus())
	}

	node.mu.RLock()
	client := node.client
	node.mu.RUnlock()

	if client == nil {
		return fmt.Errorf("no client available for node %d", nodeID)
	}

	err := operation(client)
	if err != nil {
		// Check if this is a connection-related error
		if isConnectionError(err) {
			options.Logger.Warn("Connection error detected during operation, marking node as failed",
				zap.Int64("node_id", nodeID),
				zap.Error(err))

			// Mark node as failed and remove from active list
			node.UpdateStatus(NodeStatusFailed)
			ncm.removeFromActiveNodes(nodeID)

			// Trigger reconnection attempt in background
			go ncm.connectToNode(ncm.ctx, node)
		}
	}

	return err
}

// isConnectionError determines if an error is related to connection issues
func isConnectionError(err error) bool {
	if err == nil {
		return false
	}

	// Check for common gRPC connection errors
	errorStr := err.Error()
	connectionErrors := []string{
		"connection refused",
		"connection reset",
		"broken pipe",
		"no such host",
		"network is unreachable",
		"transport is closing",
		"context deadline exceeded",
		"rpc error",
	}

	for _, connErr := range connectionErrors {
		if strings.Contains(strings.ToLower(errorStr), connErr) {
			return true
		}
	}

	return false
}

// Shutdown gracefully closes all connections and stops background processes
func (ncm *NodeConnectionManager) Shutdown() {
	options.Logger.Info("Shutting down NodeConnectionManager")

	// Stop health checker
	if ncm.healthCheck != nil {
		ncm.healthCheck.Stop()
	}

	// Cancel context to stop background operations
	if ncm.cancel != nil {
		ncm.cancel()
	}

	// Close all node connections
	ncm.mu.Lock()
	for nodeID, node := range ncm.nodes {
		options.Logger.Debug("Closing connection to node during shutdown",
			zap.Int64("node_id", nodeID))
		node.Close()
	}

	// Clear all maps
	ncm.nodes = make(map[int64]*ManagedNode)
	ncm.activeNodes = make(map[string][]*ManagedNode)
	ncm.mu.Unlock()

	options.Logger.Info("NodeConnectionManager shutdown complete")
}
