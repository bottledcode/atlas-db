package region

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/bottledcode/atlas-db/pkg/storage"
	"github.com/bottledcode/atlas-db/proto/atlas"
)

type WriteOptimizer struct {
	regionID    int32
	localNodes  map[string]*atlas.NodeInfo
	remoteNodes map[int32]map[string]*atlas.NodeInfo // regionID -> nodeID -> nodeInfo
	transport   Transport
	storage     Storage
	latencyMap  map[string]time.Duration // nodeID -> average latency
	mu          sync.RWMutex
}

type Transport interface {
	SendGet(ctx context.Context, nodeID string, req *atlas.GetRequest) (*atlas.GetResponse, error)
	SendPut(ctx context.Context, nodeID string, req *atlas.PutRequest) (*atlas.PutResponse, error)
	SendDelete(ctx context.Context, nodeID string, req *atlas.DeleteRequest) (*atlas.DeleteResponse, error)
	GetActiveNodes(regionID int32) []string
	GetAllRegions() []int32
}

type Storage interface {
	Get(ctx context.Context, key string) (*storage.RegionKVPair, error)
	Put(ctx context.Context, key string, value []byte) error
	Delete(ctx context.Context, key string) error
}

type ReadStrategy int

const (
	ReadLocal ReadStrategy = iota
	ReadNearest
	ReadConsistent
)

type WriteStrategy int

const (
	WriteLocal WriteStrategy = iota
	WriteGlobal
)

func NewWriteOptimizer(regionID int32, transport Transport, storage Storage) *WriteOptimizer {
	return &WriteOptimizer{
		regionID:    regionID,
		localNodes:  make(map[string]*atlas.NodeInfo),
		remoteNodes: make(map[int32]map[string]*atlas.NodeInfo),
		transport:   transport,
		storage:     storage,
		latencyMap:  make(map[string]time.Duration),
	}
}

func (wo *WriteOptimizer) AddNode(nodeInfo *atlas.NodeInfo) {
	wo.mu.Lock()
	defer wo.mu.Unlock()

	if nodeInfo.RegionId == wo.regionID {
		wo.localNodes[nodeInfo.NodeId] = nodeInfo
	} else {
		if wo.remoteNodes[nodeInfo.RegionId] == nil {
			wo.remoteNodes[nodeInfo.RegionId] = make(map[string]*atlas.NodeInfo)
		}
		wo.remoteNodes[nodeInfo.RegionId][nodeInfo.NodeId] = nodeInfo
	}
}

func (wo *WriteOptimizer) RemoveNode(nodeID string, regionID int32) {
	wo.mu.Lock()
	defer wo.mu.Unlock()

	if regionID == wo.regionID {
		delete(wo.localNodes, nodeID)
	} else {
		if regionNodes, exists := wo.remoteNodes[regionID]; exists {
			delete(regionNodes, nodeID)
		}
	}
	delete(wo.latencyMap, nodeID)
}

func (wo *WriteOptimizer) OptimizedGet(ctx context.Context, key string, strategy ReadStrategy) (*atlas.GetResponse, error) {
	switch strategy {
	case ReadLocal:
		return wo.getLocal(ctx, key)
	case ReadNearest:
		return wo.getNearest(ctx, key)
	case ReadConsistent:
		return wo.getConsistent(ctx, key)
	default:
		return wo.getLocal(ctx, key)
	}
}

func (wo *WriteOptimizer) getLocal(ctx context.Context, key string) (*atlas.GetResponse, error) {
	// Try local storage first
	kv, err := wo.storage.Get(ctx, key)
	if err != nil {
		return nil, err
	}

	if kv != nil {
		return &atlas.GetResponse{
			Value:   kv.Value,
			Found:   true,
			Version: kv.Version,
		}, nil
	}

	// If not found locally, try local nodes
	wo.mu.RLock()
	localNodes := make([]*atlas.NodeInfo, 0, len(wo.localNodes))
	for _, node := range wo.localNodes {
		localNodes = append(localNodes, node)
	}
	wo.mu.RUnlock()

	req := &atlas.GetRequest{
		Key:            key,
		ConsistentRead: false,
	}

	for _, node := range localNodes {
		resp, err := wo.transport.SendGet(ctx, node.NodeId, req)
		if err != nil {
			continue
		}
		if resp.Found {
			return resp, nil
		}
	}

	return &atlas.GetResponse{Found: false}, nil
}

func (wo *WriteOptimizer) getNearest(ctx context.Context, key string) (*atlas.GetResponse, error) {
	// Start with local
	resp, err := wo.getLocal(ctx, key)
	if err != nil || resp.Found {
		return resp, err
	}

	// Try nearest regions based on latency
	nearestNodes := wo.getNearestNodes()

	req := &atlas.GetRequest{
		Key:            key,
		ConsistentRead: false,
	}

	for _, nodeID := range nearestNodes {
		resp, err := wo.transport.SendGet(ctx, nodeID, req)
		if err != nil {
			continue
		}
		if resp.Found {
			return resp, nil
		}
	}

	return &atlas.GetResponse{Found: false}, nil
}

func (wo *WriteOptimizer) getConsistent(ctx context.Context, key string) (*atlas.GetResponse, error) {
	// For consistent reads, we need to contact a quorum of nodes
	allNodes := wo.getAllNodes()

	req := &atlas.GetRequest{
		Key:            key,
		ConsistentRead: true,
	}

	type response struct {
		resp *atlas.GetResponse
		err  error
	}

	results := make(chan response, len(allNodes))

	// Send requests to all nodes in parallel
	for _, nodeID := range allNodes {
		go func(nid string) {
			resp, err := wo.transport.SendGet(ctx, nid, req)
			results <- response{resp: resp, err: err}
		}(nodeID)
	}

	// Collect responses
	var latestResp *atlas.GetResponse
	var latestVersion int64 = -1
	responses := 0
	quorum := len(allNodes)/2 + 1

	for range allNodes {
		select {
		case result := <-results:
			if result.err != nil {
				continue
			}
			responses++

			if result.resp.Found && result.resp.Version > latestVersion {
				latestResp = result.resp
				latestVersion = result.resp.Version
			}

			if responses >= quorum {
				if latestResp != nil {
					return latestResp, nil
				}
				return &atlas.GetResponse{Found: false}, nil
			}
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}

	return nil, fmt.Errorf("insufficient responses for consistent read")
}

func (wo *WriteOptimizer) OptimizedPut(ctx context.Context, key string, value []byte, strategy WriteStrategy) (*atlas.PutResponse, error) {
	switch strategy {
	case WriteLocal:
		return wo.putLocal(ctx, key, value)
	case WriteGlobal:
		return wo.putGlobal(ctx, key, value)
	default:
		return wo.putLocal(ctx, key, value)
	}
}

func (wo *WriteOptimizer) putLocal(ctx context.Context, key string, value []byte) (*atlas.PutResponse, error) {
	// Store locally first for fast response
	if err := wo.storage.Put(ctx, key, value); err != nil {
		return &atlas.PutResponse{
			Success: false,
			Error:   err.Error(),
		}, err
	}

	// Asynchronously replicate to other local nodes
	wo.mu.RLock()
	localNodes := make([]*atlas.NodeInfo, 0, len(wo.localNodes))
	for _, node := range wo.localNodes {
		localNodes = append(localNodes, node)
	}
	wo.mu.RUnlock()

	req := &atlas.PutRequest{
		Key:   key,
		Value: value,
	}

	// Fire and forget to local nodes
	for _, node := range localNodes {
		go func(nid string) {
			wo.transport.SendPut(context.Background(), nid, req)
		}(node.NodeId)
	}

	return &atlas.PutResponse{
		Success: true,
		Version: time.Now().UnixNano(),
	}, nil
}

func (wo *WriteOptimizer) putGlobal(ctx context.Context, key string, value []byte) (*atlas.PutResponse, error) {
	// This should go through consensus for strong consistency
	// For now, we'll implement a simple majority write

	allNodes := wo.getAllNodes()

	req := &atlas.PutRequest{
		Key:   key,
		Value: value,
	}

	type response struct {
		resp *atlas.PutResponse
		err  error
	}

	results := make(chan response, len(allNodes))

	// Send to all nodes in parallel
	for _, nodeID := range allNodes {
		go func(nid string) {
			resp, err := wo.transport.SendPut(ctx, nid, req)
			results <- response{resp: resp, err: err}
		}(nodeID)
	}

	// Wait for majority
	successes := 0
	failures := 0
	quorum := len(allNodes)/2 + 1

	for range allNodes {
		select {
		case result := <-results:
			if result.err != nil || !result.resp.Success {
				failures++
			} else {
				successes++
			}

			if successes >= quorum {
				return &atlas.PutResponse{
					Success: true,
					Version: time.Now().UnixNano(),
				}, nil
			}

			if failures > len(allNodes)-quorum {
				return &atlas.PutResponse{
					Success: false,
					Error:   "insufficient nodes for write quorum",
				}, fmt.Errorf("write failed")
			}
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}

	return &atlas.PutResponse{
		Success: false,
		Error:   "timeout waiting for write quorum",
	}, fmt.Errorf("write timeout")
}

func (wo *WriteOptimizer) measureLatency(nodeID string) {
	start := time.Now()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Send a simple heartbeat-like request
	req := &atlas.GetRequest{
		Key:            "__ping__",
		ConsistentRead: false,
	}

	_, err := wo.transport.SendGet(ctx, nodeID, req)
	if err != nil {
		return // Don't update latency on error
	}

	latency := time.Since(start)

	wo.mu.Lock()
	defer wo.mu.Unlock()

	// Simple exponential moving average
	if existing, exists := wo.latencyMap[nodeID]; exists {
		wo.latencyMap[nodeID] = time.Duration(float64(existing)*0.8 + float64(latency)*0.2)
	} else {
		wo.latencyMap[nodeID] = latency
	}
}

func (wo *WriteOptimizer) getNearestNodes() []string {
	wo.mu.RLock()
	defer wo.mu.RUnlock()

	type nodeLatency struct {
		nodeID  string
		latency time.Duration
	}

	var nodes []nodeLatency

	// Collect all remote nodes with their latencies
	for _, regionNodes := range wo.remoteNodes {
		for nodeID := range regionNodes {
			if latency, exists := wo.latencyMap[nodeID]; exists {
				nodes = append(nodes, nodeLatency{nodeID: nodeID, latency: latency})
			}
		}
	}

	// Sort by latency (simple bubble sort for small datasets)
	for i := 0; i < len(nodes); i++ {
		for j := i + 1; j < len(nodes); j++ {
			if nodes[i].latency > nodes[j].latency {
				nodes[i], nodes[j] = nodes[j], nodes[i]
			}
		}
	}

	// Return sorted node IDs
	result := make([]string, len(nodes))
	for i, node := range nodes {
		result[i] = node.nodeID
	}
	return result
}

func (wo *WriteOptimizer) getAllNodes() []string {
	wo.mu.RLock()
	defer wo.mu.RUnlock()

	var nodes []string

	// Add local nodes
	for nodeID := range wo.localNodes {
		nodes = append(nodes, nodeID)
	}

	// Add remote nodes
	for _, regionNodes := range wo.remoteNodes {
		for nodeID := range regionNodes {
			nodes = append(nodes, nodeID)
		}
	}

	return nodes
}

// StartLatencyMonitoring starts a background goroutine to measure latencies
func (wo *WriteOptimizer) StartLatencyMonitoring() {
	go func() {
		ticker := time.NewTicker(30 * time.Second)
		defer ticker.Stop()

		for range ticker.C {
			allNodes := wo.getAllNodes()
			for _, nodeID := range allNodes {
				go wo.measureLatency(nodeID)
			}
		}
	}()
}
