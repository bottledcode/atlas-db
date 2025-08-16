package consensus

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/bottledcode/atlas-db/proto/atlas"
)

// Mock transport for testing
type mockTransport struct {
	nodes    map[string]*mockNode
	regions  map[int32][]string
	prepares chan *prepareCall
	accepts  chan *acceptCall
	commits  chan *commitCall
	mu       sync.RWMutex
}

type mockNode struct {
	nodeID   string
	regionID int32
	wpaxos   *WPaxos
}

type prepareCall struct {
	nodeID string
	req    *atlas.PrepareRequest
	resp   chan *atlas.PrepareResponse
	err    chan error
}

type acceptCall struct {
	nodeID string
	req    *atlas.AcceptRequest
	resp   chan *atlas.AcceptResponse
	err    chan error
}

type commitCall struct {
	nodeID string
	req    *atlas.CommitRequest
	resp   chan *atlas.CommitResponse
	err    chan error
}

func newMockTransport() *mockTransport {
	return &mockTransport{
		nodes:    make(map[string]*mockNode),
		regions:  make(map[int32][]string),
		prepares: make(chan *prepareCall, 100),
		accepts:  make(chan *acceptCall, 100),
		commits:  make(chan *commitCall, 100),
	}
}

func (mt *mockTransport) addNode(nodeID string, regionID int32, wpaxos *WPaxos) {
	mt.mu.Lock()
	defer mt.mu.Unlock()

	mt.nodes[nodeID] = &mockNode{
		nodeID:   nodeID,
		regionID: regionID,
		wpaxos:   wpaxos,
	}

	if mt.regions[regionID] == nil {
		mt.regions[regionID] = make([]string, 0)
	}
	mt.regions[regionID] = append(mt.regions[regionID], nodeID)
}

func (mt *mockTransport) SendPrepare(ctx context.Context, nodeID string, req *atlas.PrepareRequest) (*atlas.PrepareResponse, error) {
	mt.mu.RLock()
	node, exists := mt.nodes[nodeID]
	mt.mu.RUnlock()

	if !exists {
		return nil, nil // Node not found, simulate network failure
	}

	return node.wpaxos.HandlePrepare(ctx, req)
}

func (mt *mockTransport) SendAccept(ctx context.Context, nodeID string, req *atlas.AcceptRequest) (*atlas.AcceptResponse, error) {
	mt.mu.RLock()
	node, exists := mt.nodes[nodeID]
	mt.mu.RUnlock()

	if !exists {
		return nil, nil // Node not found
	}

	return node.wpaxos.HandleAccept(ctx, req)
}

func (mt *mockTransport) SendCommit(ctx context.Context, nodeID string, req *atlas.CommitRequest) (*atlas.CommitResponse, error) {
	mt.mu.RLock()
	node, exists := mt.nodes[nodeID]
	mt.mu.RUnlock()

	if !exists {
		return nil, nil // Node not found
	}

	return node.wpaxos.HandleCommit(ctx, req)
}

func (mt *mockTransport) GetActiveNodes(regionID int32) []string {
	mt.mu.RLock()
	defer mt.mu.RUnlock()

	nodes, exists := mt.regions[regionID]
	if !exists {
		return []string{}
	}

	// Return copy
	result := make([]string, len(nodes))
	copy(result, nodes)
	return result
}

func (mt *mockTransport) GetAllRegions() []int32 {
	mt.mu.RLock()
	defer mt.mu.RUnlock()

	regions := make([]int32, 0, len(mt.regions))
	for regionID := range mt.regions {
		regions = append(regions, regionID)
	}
	return regions
}

// Mock storage for testing
type mockStorage struct {
	data map[string][]byte
	mu   sync.RWMutex
}

func newMockStorage() *mockStorage {
	return &mockStorage{
		data: make(map[string][]byte),
	}
}

func (ms *mockStorage) Store(key string, value []byte) error {
	ms.mu.Lock()
	defer ms.mu.Unlock()
	ms.data[key] = make([]byte, len(value))
	copy(ms.data[key], value)
	return nil
}

func (ms *mockStorage) Load(key string) ([]byte, error) {
	ms.mu.RLock()
	defer ms.mu.RUnlock()

	value, exists := ms.data[key]
	if !exists {
		return nil, nil
	}

	result := make([]byte, len(value))
	copy(result, value)
	return result, nil
}

func (ms *mockStorage) DeleteKey(key string) error {
	ms.mu.Lock()
	defer ms.mu.Unlock()
	delete(ms.data, key)
	return nil
}

func TestWPaxos_SingleNodeProposal(t *testing.T) {
	transport := newMockTransport()
	storage1 := newMockStorage()
	storage2 := newMockStorage()

	// Create two nodes for proper quorum
	wp1 := NewWPaxos("node1", 1, transport, storage1)
	wp2 := NewWPaxos("node2", 1, transport, storage2)
	transport.addNode("node1", 1, wp1)
	transport.addNode("node2", 1, wp2)

	ctx := context.Background()
	instanceID := "test-instance-1"
	value := []byte("test-value")

	err := wp1.Propose(ctx, instanceID, value)
	if err != nil {
		t.Fatalf("Single node proposal failed: %v", err)
	}

	// Verify value was stored in proposing node
	storedValue, err := storage1.Load(instanceID)
	if err != nil {
		t.Fatalf("Failed to load stored value: %v", err)
	}

	if string(storedValue) != string(value) {
		t.Errorf("Expected stored value %s, got %s", string(value), string(storedValue))
	}
}

func TestWPaxos_MultiNodeProposal(t *testing.T) {
	transport := newMockTransport()

	// Create 3 nodes in same region
	nodes := make([]*WPaxos, 3)
	storages := make([]*mockStorage, 3)

	for i := 0; i < 3; i++ {
		storages[i] = newMockStorage()
		nodeID := fmt.Sprintf("node%d", i+1)
		nodes[i] = NewWPaxos(nodeID, 1, transport, storages[i])
		transport.addNode(nodeID, 1, nodes[i])
	}

	ctx := context.Background()
	instanceID := "test-instance-multi"
	value := []byte("multi-node-value")

	// Propose from first node
	err := nodes[0].Propose(ctx, instanceID, value)
	if err != nil {
		t.Fatalf("Multi-node proposal failed: %v", err)
	}

	// Verify proposing node has the value (it always commits to itself)
	storedValue, err := storages[0].Load(instanceID)
	if err != nil {
		t.Fatalf("Failed to load from proposing node: %v", err)
	}

	if string(storedValue) != string(value) {
		t.Errorf("Proposing node: expected %s, got %s", string(value), string(storedValue))
	}

	// Count how many nodes have the value (should be at least majority)
	nodesWithValue := 1 // proposing node always has it
	for i := 1; i < len(storages); i++ {
		storedValue, err := storages[i].Load(instanceID)
		if err == nil && string(storedValue) == string(value) {
			nodesWithValue++
		}
	}

	if nodesWithValue < len(storages)/2+1 {
		t.Errorf("Expected at least majority (%d) to have value, got %d", len(storages)/2+1, nodesWithValue)
	}
}

func TestWPaxos_MultiRegionProposal(t *testing.T) {
	transport := newMockTransport()

	// Create nodes in different regions
	nodes := make([]*WPaxos, 4)
	storages := make([]*mockStorage, 4)

	// 2 nodes in region 1, 2 nodes in region 2
	regions := []int32{1, 1, 2, 2}

	for i := 0; i < 4; i++ {
		storages[i] = newMockStorage()
		nodeID := fmt.Sprintf("node%d", i+1)
		nodes[i] = NewWPaxos(nodeID, regions[i], transport, storages[i])
		transport.addNode(nodeID, regions[i], nodes[i])
	}

	ctx := context.Background()
	instanceID := "test-instance-multi-region"
	value := []byte("multi-region-value")

	// Propose from node in region 1
	err := nodes[0].Propose(ctx, instanceID, value)
	if err != nil {
		t.Fatalf("Multi-region proposal failed: %v", err)
	}

	// Verify proposing node has the value
	storedValue, err := storages[0].Load(instanceID)
	if err != nil {
		t.Fatalf("Failed to load from proposing node: %v", err)
	}

	if string(storedValue) != string(value) {
		t.Errorf("Proposing node: expected %s, got %s", string(value), string(storedValue))
	}

	// Count nodes with the value (should be at least majority)
	successCount := 1 // proposing node
	for i := 1; i < len(storages); i++ {
		storedValue, err := storages[i].Load(instanceID)
		if err == nil && string(storedValue) == string(value) {
			successCount++
			t.Logf("Node %d has the value", i+1)
		} else if err != nil {
			t.Logf("Node %d failed to load: %v", i+1, err)
		} else {
			t.Logf("Node %d does not have the value", i+1)
		}
	}

	// In W-Paxos grid quorum, fast path requires:
	// - Local majority (2 nodes in region 1: proposer + 1 other)
	// - At least one node from each other region (1 node in region 2)
	// Total expected: 3 nodes (2 from region 1 + 1 from region 2)
	numRegions := 2
	localRegionSize := 2                                   // Region 1 has 2 nodes
	expectedFastPath := localRegionSize + (numRegions - 1) // Local majority + 1 per other region
	globalMajority := len(nodes)/2 + 1                     // 3 nodes

	if successCount >= globalMajority {
		t.Logf("W-Paxos grid quorum achieved: %d nodes have the value", successCount)
	} else {
		t.Errorf("Expected W-Paxos grid quorum (%d nodes: local majority + one per region), got %d", expectedFastPath, successCount)
	}
}

func TestWPaxos_HandlePrepare(t *testing.T) {
	transport := newMockTransport()
	storage := newMockStorage()

	wp := NewWPaxos("node1", 1, transport, storage)

	ctx := context.Background()
	instanceID := "test-prepare"

	// First prepare request
	req1 := &atlas.PrepareRequest{
		ProposalNumber: 1,
		InstanceId:     instanceID,
		RegionId:       1,
	}

	resp1, err := wp.HandlePrepare(ctx, req1)
	if err != nil {
		t.Fatalf("HandlePrepare failed: %v", err)
	}

	if !resp1.Promise {
		t.Error("Expected promise for first prepare")
	}

	if resp1.HighestProposal != 1 {
		t.Errorf("Expected highest proposal 1, got %d", resp1.HighestProposal)
	}

	// Second prepare with higher number
	req2 := &atlas.PrepareRequest{
		ProposalNumber: 2,
		InstanceId:     instanceID,
		RegionId:       1,
	}

	resp2, err := wp.HandlePrepare(ctx, req2)
	if err != nil {
		t.Fatalf("HandlePrepare failed: %v", err)
	}

	if !resp2.Promise {
		t.Error("Expected promise for higher prepare")
	}

	// Third prepare with lower number (should be rejected)
	req3 := &atlas.PrepareRequest{
		ProposalNumber: 1,
		InstanceId:     instanceID,
		RegionId:       1,
	}

	resp3, err := wp.HandlePrepare(ctx, req3)
	if err != nil {
		t.Fatalf("HandlePrepare failed: %v", err)
	}

	if resp3.Promise {
		t.Error("Expected rejection for lower prepare")
	}

	if resp3.HighestProposal != 2 {
		t.Errorf("Expected highest proposal 2, got %d", resp3.HighestProposal)
	}
}

func TestWPaxos_HandleAccept(t *testing.T) {
	transport := newMockTransport()
	storage := newMockStorage()

	wp := NewWPaxos("node1", 1, transport, storage)

	ctx := context.Background()
	instanceID := "test-accept"
	value := []byte("accept-value")

	// Prepare first
	prepReq := &atlas.PrepareRequest{
		ProposalNumber: 1,
		InstanceId:     instanceID,
		RegionId:       1,
	}

	_, err := wp.HandlePrepare(ctx, prepReq)
	if err != nil {
		t.Fatalf("HandlePrepare failed: %v", err)
	}

	// Accept request
	accReq := &atlas.AcceptRequest{
		ProposalNumber: 1,
		InstanceId:     instanceID,
		Value:          value,
		RegionId:       1,
	}

	resp, err := wp.HandleAccept(ctx, accReq)
	if err != nil {
		t.Fatalf("HandleAccept failed: %v", err)
	}

	if !resp.Accepted {
		t.Error("Expected accept to succeed")
	}

	if resp.ProposalNumber != 1 {
		t.Errorf("Expected proposal number 1, got %d", resp.ProposalNumber)
	}

	// Accept with lower proposal number should fail
	accReq2 := &atlas.AcceptRequest{
		ProposalNumber: 0,
		InstanceId:     instanceID,
		Value:          value,
		RegionId:       1,
	}

	resp2, err := wp.HandleAccept(ctx, accReq2)
	if err != nil {
		t.Fatalf("HandleAccept failed: %v", err)
	}

	if resp2.Accepted {
		t.Error("Expected accept with lower proposal to fail")
	}
}

func TestWPaxos_HandleCommit(t *testing.T) {
	transport := newMockTransport()
	storage := newMockStorage()

	wp := NewWPaxos("node1", 1, transport, storage)

	ctx := context.Background()
	instanceID := "test-commit"
	value := []byte("commit-value")

	req := &atlas.CommitRequest{
		InstanceId:     instanceID,
		Value:          value,
		ProposalNumber: 1,
	}

	resp, err := wp.HandleCommit(ctx, req)
	if err != nil {
		t.Fatalf("HandleCommit failed: %v", err)
	}

	if !resp.Committed {
		t.Error("Expected commit to succeed")
	}

	// Verify value was stored
	storedValue, err := storage.Load(instanceID)
	if err != nil {
		t.Fatalf("Failed to load committed value: %v", err)
	}

	if string(storedValue) != string(value) {
		t.Errorf("Expected stored value %s, got %s", string(value), string(storedValue))
	}
}

func TestWPaxos_ConcurrentProposals(t *testing.T) {
	transport := newMockTransport()

	// Create 3 nodes
	nodes := make([]*WPaxos, 3)
	storages := make([]*mockStorage, 3)

	for i := 0; i < 3; i++ {
		storages[i] = newMockStorage()
		nodeID := fmt.Sprintf("node%d", i+1)
		nodes[i] = NewWPaxos(nodeID, 1, transport, storages[i])
		transport.addNode(nodeID, 1, nodes[i])
	}

	ctx := context.Background()
	numProposals := 10

	// Create channel to collect results
	results := make(chan error, numProposals)

	// Start concurrent proposals
	for i := 0; i < numProposals; i++ {
		go func(proposalID int) {
			instanceID := fmt.Sprintf("concurrent-instance-%d", proposalID)
			value := []byte(fmt.Sprintf("value-%d", proposalID))

			// Use different nodes for proposals
			nodeIndex := proposalID % len(nodes)
			err := nodes[nodeIndex].Propose(ctx, instanceID, value)
			results <- err
		}(i)
	}

	// Wait for all proposals to complete
	successCount := 0
	for i := 0; i < numProposals; i++ {
		err := <-results
		if err == nil {
			successCount++
		} else {
			t.Logf("Proposal %d failed: %v", i, err)
		}
	}

	// Most proposals should succeed (some may fail due to conflicts)
	if successCount < numProposals/2 {
		t.Errorf("Expected at least %d successful proposals, got %d",
			numProposals/2, successCount)
	}

	t.Logf("Concurrent test: %d/%d proposals succeeded", successCount, numProposals)
}

func TestWPaxos_ProposalTimeout(t *testing.T) {
	transport := newMockTransport()
	storage := newMockStorage()

	// Create node and add a second node to force needing external responses
	wp1 := NewWPaxos("node1", 1, transport, storage)
	_ = NewWPaxos("node2", 1, transport, newMockStorage()) // Create but don't add to transport
	transport.addNode("node1", 1, wp1)
	// Don't add node2 to simulate network partition

	// Add node2 to regions so quorum calculation includes it, but don't add to transport
	transport.mu.Lock()
	transport.regions[1] = []string{"node1", "node2"}
	transport.mu.Unlock()

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	instanceID := "timeout-test"
	value := []byte("timeout-value")

	err := wp1.Propose(ctx, instanceID, value)

	// Should fail due to insufficient responses from partitioned node
	if err == nil {
		t.Error("Expected proposal to fail due to insufficient responses")
	}
}

func TestWPaxos_ProposalStates(t *testing.T) {
	transport := newMockTransport()
	storage1 := newMockStorage()
	storage2 := newMockStorage()

	// Need at least 2 nodes for quorum
	wp1 := NewWPaxos("node1", 1, transport, storage1)
	wp2 := NewWPaxos("node2", 1, transport, storage2)
	transport.addNode("node1", 1, wp1)
	transport.addNode("node2", 1, wp2)

	// Check initial state
	wp1.mu.RLock()
	if len(wp1.proposals) != 0 {
		t.Error("Expected no proposals initially")
	}
	wp1.mu.RUnlock()

	ctx := context.Background()
	instanceID := "state-test"
	value := []byte("state-value")

	err := wp1.Propose(ctx, instanceID, value)
	if err != nil {
		t.Fatalf("Proposal failed: %v", err)
	}

	// After successful proposal, it should be cleaned up
	wp1.mu.RLock()
	_, exists := wp1.proposals[instanceID]
	wp1.mu.RUnlock()

	if exists {
		t.Error("Expected proposal to be cleaned up after commit")
	}
}
