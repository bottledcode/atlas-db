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

package consensus

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"slices"
	"sort"
	"strconv"
	"sync"
	"time"

	"github.com/bottledcode/atlas-db/atlas/kv"
	"github.com/bottledcode/atlas-db/atlas/options"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/emptypb"
)

// safeRttDuration returns the RTT duration, or max duration if RTT is nil.
// This ensures unmeasured nodes sort last in RTT-based ordering.
func safeRttDuration(node *QuorumNode) time.Duration {
	if rtt := node.GetRtt(); rtt != nil {
		return rtt.AsDuration()
	}
	return time.Duration(1<<63 - 1) // max duration
}

type QuorumManager interface {
	GetQuorum(ctx context.Context, table string) (Quorum, error)
	GetBroadcastQuorum(ctx context.Context, table []byte) (Quorum, error)
	AddNode(ctx context.Context, node *Node) error
	RemoveNode(nodeID uint64) error
	Send(node *Node, do func(quorumNode *QuorumNode) (any, error)) (any, error)
}

var manager *defaultQuorumManager
var managerOnce sync.Once

func GetDefaultQuorumManager(ctx context.Context) QuorumManager {
	managerOnce.Do(func() {
		manager = &defaultQuorumManager{
			nodes:             make(map[RegionName][]*QuorumNode),
			connectionManager: GetNodeConnectionManager(ctx),
		}
	})

	return manager
}

type RegionName string

type defaultQuorumManager struct {
	mu                sync.RWMutex
	nodes             map[RegionName][]*QuorumNode
	connectionManager *NodeConnectionManager
}

func (q *defaultQuorumManager) GetBroadcastQuorum(ctx context.Context, table []byte) (Quorum, error) {
	q.mu.RLock()
	defer q.mu.RUnlock()

	nodes := make([]*QuorumNode, 0)
	for _, list := range q.nodes {
		nodes = append(nodes, list...)
	}

	return &broadcastQuorum{nodes: nodes}, nil
}

func (q *defaultQuorumManager) Send(node *Node, do func(quorumNode *QuorumNode) (any, error)) (any, error) {
	q.mu.RLock()
	defer q.mu.RUnlock()

	for _, n := range q.nodes[RegionName(node.GetRegion().GetName())] {
		if n.GetId() == node.GetId() {
			return do(n)
		}
	}

	return nil, nil
}

func (q *defaultQuorumManager) AddNode(ctx context.Context, node *Node) error {
	q.mu.Lock()
	defer q.mu.Unlock()

	if _, ok := q.nodes[RegionName(node.GetRegion().GetName())]; !ok {
		q.nodes[RegionName(node.GetRegion().GetName())] = make([]*QuorumNode, 0)
	}

	qn := &QuorumNode{
		Node:   node,
		closer: nil,
		client: nil,
	}

	// prevent duplication
	for _, n := range q.nodes[RegionName(node.GetRegion().GetName())] {
		if n.GetId() == node.GetId() {
			return nil
		}
	}

	// special handling for self
	if node.GetId() == options.CurrentOptions.ServerId {
		q.nodes[RegionName(node.GetRegion().GetName())] = append(q.nodes[RegionName(node.GetRegion().GetName())], qn)
		return nil
	}

	// Connection will be established lazily on first use
	q.nodes[RegionName(node.GetRegion().GetName())] = append(q.nodes[RegionName(node.GetRegion().GetName())], qn)

	// Also add to the connection manager for health monitoring
	if q.connectionManager != nil {
		_ = q.connectionManager.AddNode(ctx, node)
	}

	return nil
}

func (q *defaultQuorumManager) RemoveNode(nodeID uint64) error {
	q.mu.Lock()
	defer q.mu.Unlock()

	// Find and remove the node from all regions
	for region, nodes := range q.nodes {
		for i, node := range nodes {
			if node.Id == nodeID {
				// Close the connection if it exists
				node.Close()

				// Remove the node from the slice
				q.nodes[region] = append(nodes[:i], nodes[i+1:]...)

				// If region has no nodes left, remove the region
				if len(q.nodes[region]) == 0 {
					delete(q.nodes, region)
				}

				options.Logger.Info("Removed node from quorum manager",
					zap.Uint64("node_id", nodeID),
					zap.String("region", string(region)))

				return nil
			}
		}
	}

	// Node not found - this is not necessarily an error
	options.Logger.Debug("Node not found in quorum manager for removal",
		zap.Uint64("node_id", nodeID))

	return nil
}

type Quorum interface {
	ConsensusClient
	FriendlySteal(ctx context.Context, key []byte) (bool, *Ballot, error)
}

type QuorumNode struct {
	*Node
	closer     func()
	client     ConsensusClient
	clientOnce sync.Once
	clientErr  error
}

// ensureClient lazily initializes the gRPC client with latency injection support.
func (q *QuorumNode) ensureClient() error {
	q.clientOnce.Do(func() {
		q.client, q.closer, q.clientErr = getNewClient(q.GetAddress() + ":" + strconv.Itoa(int(q.GetPort())))
		if q.clientErr != nil {
			return
		}
		// Wrap with latency injection for integration testing
		q.client = WrapWithLatency(q.client, q.GetRegion().GetName())
	})
	return q.clientErr
}

func (q *QuorumNode) RequestSlots(ctx context.Context, in *SlotRequest, opts ...grpc.CallOption) (grpc.ServerStreamingClient[RecordMutation], error) {
	if err := q.ensureClient(); err != nil {
		return nil, err
	}
	return q.client.RequestSlots(ctx, in, opts...)
}

func (q *QuorumNode) Follow(ctx context.Context, in *SlotRequest, opts ...grpc.CallOption) (grpc.ServerStreamingClient[RecordMutation], error) {
	if err := q.ensureClient(); err != nil {
		return nil, err
	}
	return q.client.Follow(ctx, in, opts...)
}

func (q *QuorumNode) Replicate(ctx context.Context, opts ...grpc.CallOption) (grpc.ClientStreamingClient[ReplicationRequest, ReplicationResponse], error) {
	if err := q.ensureClient(); err != nil {
		return nil, err
	}
	return q.client.Replicate(ctx, opts...)
}

func (q *QuorumNode) DeReference(ctx context.Context, in *DereferenceRequest, opts ...grpc.CallOption) (grpc.ServerStreamingClient[DereferenceResponse], error) {
	if err := q.ensureClient(); err != nil {
		return nil, err
	}
	return q.client.DeReference(ctx, in, opts...)
}

func (q *QuorumNode) StealTableOwnership(ctx context.Context, in *StealTableOwnershipRequest, opts ...grpc.CallOption) (*StealTableOwnershipResponse, error) {
	if err := q.ensureClient(); err != nil {
		return nil, err
	}
	return q.client.StealTableOwnership(ctx, in, opts...)
}

func (q *QuorumNode) WriteMigration(ctx context.Context, in *WriteMigrationRequest, opts ...grpc.CallOption) (*WriteMigrationResponse, error) {
	if err := q.ensureClient(); err != nil {
		return nil, err
	}
	return q.client.WriteMigration(ctx, in, opts...)
}

func (q *QuorumNode) AcceptMigration(ctx context.Context, in *WriteMigrationRequest, opts ...grpc.CallOption) (*emptypb.Empty, error) {
	if err := q.ensureClient(); err != nil {
		return nil, err
	}
	return q.client.AcceptMigration(ctx, in, opts...)
}

func (q *QuorumNode) Ping(ctx context.Context, in *PingRequest, opts ...grpc.CallOption) (*PingResponse, error) {
	if err := q.ensureClient(); err != nil {
		return nil, err
	}
	return q.client.Ping(ctx, in, opts...)
}

func (q *QuorumNode) PrefixScan(ctx context.Context, in *PrefixScanRequest, opts ...grpc.CallOption) (*PrefixScanResponse, error) {
	if err := q.ensureClient(); err != nil {
		return nil, err
	}
	return q.client.PrefixScan(ctx, in, opts...)
}

func (q *QuorumNode) ReadRecord(ctx context.Context, in *ReadRecordRequest, opts ...grpc.CallOption) (*ReadRecordResponse, error) {
	if err := q.ensureClient(); err != nil {
		return nil, err
	}
	return q.client.ReadRecord(ctx, in, opts...)
}

func (q *QuorumNode) Close() {
	if q.closer != nil {
		q.closer()
	}
}

// calculateLfn calculates (l-Fn) for the given region name, used in calculating Q2 quorums.
func (q *defaultQuorumManager) calculateLfn(nodes map[RegionName][]*QuorumNode, region RegionName, Fn int64) int64 {
	q.mu.RLock()
	defer q.mu.RUnlock()

	return int64(len(nodes[region])) - Fn
}

// calculateN calculates the total number of nodes in the cluster, used in validating quorums.
func (q *defaultQuorumManager) calculateN(nodes map[RegionName][]*QuorumNode) int64 {
	q.mu.RLock()
	defer q.mu.RUnlock()

	total := int64(0)
	for _, nodes := range nodes {
		total += int64(len(nodes))
	}

	return total
}

// calculateTotalNodesPerZoneQ1 calculates the total number of nodes per zone required for a Q1 quorum.
func (q *defaultQuorumManager) calculateTotalNodesPerZoneQ1(Fn int64) int64 {
	return Fn + 1
}

// calculateNumberZonesQ1 calculates the number of zones required for a Q1 quorum.
func (q *defaultQuorumManager) calculateNumberZonesQ1(nodes map[RegionName][]*QuorumNode, Fz int64) int64 {
	q.mu.RLock()
	defer q.mu.RUnlock()

	totalRegions := len(nodes)
	return int64(totalRegions) - Fz
}

// calculateQ1Size calculates the size required for a Q1 quorum
func (q *defaultQuorumManager) calculateQ1Size(nodes map[RegionName][]*QuorumNode, Fz, Fn int64) int64 {
	return q.calculateNumberZonesQ1(nodes, Fz) * q.calculateTotalNodesPerZoneQ1(Fn)
}

// calculateNumberZonesQ2 calculates the number of zones required for a Q2 quorum.
func (q *defaultQuorumManager) calculateNumberZonesQ2(Fz int64) int64 {
	return Fz + 1
}

// calculateQ2Size calculates the size required for a Q2 quorum.
func (q *defaultQuorumManager) calculateQ2Size(nodes map[RegionName][]*QuorumNode, Fn int64, regions ...RegionName) int64 {
	lfn := int64(0)
	for _, region := range regions {
		lfn += q.calculateLfn(nodes, region, Fn)
	}
	return lfn
}

// calculateFmin calculates the minimum number of targeted failures that will disrupt the quorum.
func (q *defaultQuorumManager) calculateFmin(q1Size, q2Size int64) int64 {
	return min(q1Size, q2Size) - 1
}

// calculateFmax calculates the minimum number of random failures that will disrupt the quorum.
func (q *defaultQuorumManager) calculateFmax(nodes map[RegionName][]*QuorumNode, q1Size, q2Size, Fz, Fn int64) int64 {
	return q.calculateN(nodes) - q1Size - q2Size + (Fz+1)*(Fn+1)
}

// getClosestRegions returns a list of regions sorted by the average rtt of each node.
func (q *defaultQuorumManager) getClosestRegions(nodes map[RegionName][]*QuorumNode) []RegionName {
	q.mu.RLock()
	defer q.mu.RUnlock()

	// extract the list of regions from the node map
	regions := make([]RegionName, 0, len(nodes))
	for region := range nodes {
		regions = append(regions, region)
	}

	// sort the regions by the average rtt of each node
	sort.SliceStable(regions, func(i, j int) bool {
		// calculate the average rtt for each region
		iRtt := time.Duration(0)
		jRtt := time.Duration(0)
		for _, node := range nodes[regions[i]] {
			iRtt += safeRttDuration(node)
		}
		for _, node := range nodes[regions[j]] {
			jRtt += safeRttDuration(node)
		}

		return iRtt < jRtt
	})

	return regions
}

// GetQuorum returns the quorum for stealing a table. It uses a grid-based approach to determine the best solution.
func (q *defaultQuorumManager) GetQuorum(ctx context.Context, table string) (Quorum, error) {
	// get the number of regions we have active nodes in
	q.mu.RLock()
	defer q.mu.RUnlock()

	Fz := options.CurrentOptions.GetFz()
	Fn := options.CurrentOptions.GetFn()

	// Create a shallow copy of q.nodes to avoid mutating the original map
	nodes := make(map[RegionName][]*QuorumNode)
	maps.Copy(nodes, q.nodes)

	// Filter nodes to only include healthy ones from connection manager
	if q.connectionManager != nil {
		nodes = q.filterHealthyNodes(nodes)
	}

recalculate:

	// before we can calculate the quorum, we need to validate the quorum is possible
	q1RegionCount := q.calculateNumberZonesQ1(nodes, Fz)
	if q1RegionCount < 1 {
		Fz = Fz - 1
		if Fz < 0 {
			return nil, errors.New("unable to form a quorum")
		}
		goto recalculate
	}

	farRegions := q.getClosestRegions(nodes)
	slices.Reverse(farRegions)

	// since we don't steal very often, we will select regions from the farthest away first
	selectedQ1Regions := make([]RegionName, 0, int(q1RegionCount))
	nodesPerQ1Region := q.calculateTotalNodesPerZoneQ1(Fn)

	for _, region := range farRegions {
		if int64(len(q.nodes[region])) < nodesPerQ1Region {
			// this region cannot be selected, so we skip it
			continue
		}
		if int64(len(selectedQ1Regions)) >= q1RegionCount {
			// we have enough regions, so we can stop
			break
		}

		selectedQ1Regions = append(selectedQ1Regions, region)
	}
	if int64(len(selectedQ1Regions)) < q1RegionCount {
		// we don't have enough regions to form a Q1 quorum, try reducing Fn
		Fn = Fn - 1
		if Fn < 0 {
			return nil, errors.New("unable to form a quorum")
		}
		goto recalculate
	}

	// we have now selected our Q1 regions, so we can calculate Q2
	q2RegionCount := q.calculateNumberZonesQ2(Fz)
	selectedQ2Regions := make([]RegionName, 0, q2RegionCount)
	slices.Reverse(farRegions)

	// we will select regions from the closest first
	for _, region := range farRegions {
		lfn := q.calculateLfn(nodes, region, Fn)
		if lfn == 0 {
			// this region cannot be selected, so we skip it
			continue
		}
		if int64(len(selectedQ2Regions)) >= q2RegionCount {
			// we have enough regions, so we can stop
			break
		}

		selectedQ2Regions = append(selectedQ2Regions, region)
	}
	if int64(len(selectedQ2Regions)) < q2RegionCount {
		// we don't have enough regions to form a Q2 quorum, try reducing Fn
		Fn = Fn - 1
		if Fn < 0 {
			return nil, errors.New("unable to form a quorum")
		}
		goto recalculate
	}

	// we have now selected our Q2 regions, so we can now validate the quorum
	q1S := q.calculateQ1Size(nodes, Fz, Fn)
	q2S := q.calculateQ2Size(nodes, Fn, selectedQ2Regions...)
	Fmax := q.calculateFmax(nodes, q1S, q2S, Fz, Fn)
	Fmin := q.calculateFmin(q1S, q2S)

	if Fmax < 0 || Fmin < 0 {
		// we cannot form a quorum with the current settings, so we need to reduce Fz
		next := Fz - 1
		if next < 0 {
			return nil, errors.New("unable to form a quorum")
		}
		goto recalculate
	}

	// todo: dynamically adjust Fz and Fn up here?

	// we have now validated the quorum, so we can construct the quorum object
	q1 := make([]*QuorumNode, 0, q1S)
	q2 := make([]*QuorumNode, 0, q2S)

	for _, region := range selectedQ1Regions {
		for i := range nodesPerQ1Region {
			q1 = append(q1, q.nodes[region][i])
		}
	}

	for _, region := range selectedQ2Regions {
		for i := int64(0); i < q.calculateLfn(nodes, region, Fn); i++ {
			q2 = append(q2, q.nodes[region][i])
		}
	}

	// validate the sizes
	if int64(len(q1)) != q1S || int64(len(q2)) != q2S {
		return nil, errors.New("quorum size mismatch")
	}

	return &majorityQuorum{
		q1: &broadcastQuorum{nodes: q1},
		q2: &broadcastQuorum{nodes: q2},
	}, nil
}

// filterHealthyNodes removes nodes that are not currently healthy according to the connection manager
func (q *defaultQuorumManager) filterHealthyNodes(nodes map[RegionName][]*QuorumNode) map[RegionName][]*QuorumNode {
	activeNodes := q.connectionManager.GetAllActiveNodes()
	filteredNodes := make(map[RegionName][]*QuorumNode)

	for region, quorumNodes := range nodes {
		activeInRegion, hasActiveNodes := activeNodes[string(region)]

		// Create a map of active node IDs for quick lookup
		activeNodeIDs := make(map[uint64]bool)
		if hasActiveNodes {
			for _, activeNode := range activeInRegion {
				activeNodeIDs[activeNode.Id] = true
			}
		}

		// Filter quorum nodes to only include active ones or the current node
		var healthyNodes []*QuorumNode
		for _, qn := range quorumNodes {
			if activeNodeIDs[qn.Id] || qn.Id == options.CurrentOptions.ServerId {
				healthyNodes = append(healthyNodes, qn)
			}
		}

		// Always include regions that have the current node, even if no other nodes are active
		if len(healthyNodes) > 0 {
			filteredNodes[region] = healthyNodes
		}
	}

	return filteredNodes
}

// describeQuorumDiagnostic implements quorum computation for diagnostic purposes,
// treating all known nodes as active to show the complete potential quorum structure.
// This method is thread-safe and does not modify shared state.
func (q *defaultQuorumManager) describeQuorumDiagnostic(ctx context.Context, table string) (q1 []*QuorumNode, q2 []*QuorumNode, err error) {
	// Snapshot current nodes under read lock
	q.mu.RLock()
	nodesCopy := make(map[RegionName][]*QuorumNode)
	for region, nodes := range q.nodes {
		nodesCopy[region] = make([]*QuorumNode, len(nodes))
		copy(nodesCopy[region], nodes)
	}
	q.mu.RUnlock()

	// Get table configuration
	Fz := options.CurrentOptions.GetFz()
	Fn := options.CurrentOptions.GetFn()

	kvPool := kv.GetPool()
	if kvPool == nil {
		return nil, nil, fmt.Errorf("KV pool not initialized")
	}

	metaStore := kvPool.MetaStore()
	if metaStore == nil {
		return nil, nil, fmt.Errorf("metaStore is closed")
	}

	// For diagnostic purposes, treat all nodes as healthy (no filtering by connection manager)
	// This shows the complete potential quorum structure

recalculate:
	// Validate quorum is possible
	q1RegionCount := q.calculateNumberZonesQ1(nodesCopy, Fz)
	if q1RegionCount < 1 {
		Fz = Fz - 1
		if Fz < 0 {
			return nil, nil, errors.New("unable to form a quorum")
		}
		goto recalculate
	}

	farRegions := q.getClosestRegionsDiagnostic(nodesCopy)
	slices.Reverse(farRegions)

	// Select Q1 regions from farthest away first
	selectedQ1Regions := make([]RegionName, 0, int(q1RegionCount))
	nodesPerQ1Region := q.calculateTotalNodesPerZoneQ1(Fn)

	for _, region := range farRegions {
		if int64(len(nodesCopy[region])) < nodesPerQ1Region {
			continue
		}
		if int64(len(selectedQ1Regions)) >= q1RegionCount {
			break
		}
		selectedQ1Regions = append(selectedQ1Regions, region)
	}

	if int64(len(selectedQ1Regions)) < q1RegionCount {
		Fn = Fn - 1
		if Fn < 0 {
			return nil, nil, errors.New("unable to form a quorum")
		}
		goto recalculate
	}

	// Select Q2 regions
	q2RegionCount := q.calculateNumberZonesQ2(Fz)
	selectedQ2Regions := make([]RegionName, 0, q2RegionCount)
	slices.Reverse(farRegions)

	for _, region := range farRegions {
		lfn := q.calculateLfnDiagnostic(nodesCopy, region, Fn)
		if lfn == 0 {
			continue
		}
		if int64(len(selectedQ2Regions)) >= q2RegionCount {
			break
		}
		selectedQ2Regions = append(selectedQ2Regions, region)
	}

	if int64(len(selectedQ2Regions)) < q2RegionCount {
		Fn = Fn - 1
		if Fn < 0 {
			return nil, nil, errors.New("unable to form a quorum")
		}
		goto recalculate
	}

	// Validate quorum sizes
	q1S := q.calculateQ1SizeDiagnostic(nodesCopy, Fz, Fn)
	q2S := q.calculateQ2SizeDiagnostic(nodesCopy, Fn, selectedQ2Regions...)
	Fmax := q.calculateFmaxDiagnostic(nodesCopy, q1S, q2S, Fz, Fn)
	Fmin := q.calculateFmin(q1S, q2S)

	if Fmax < 0 || Fmin < 0 {
		next := Fz - 1
		if next < 0 {
			return nil, nil, errors.New("unable to form a quorum")
		}
		Fz = next
		goto recalculate
	}

	// Construct quorum node lists
	q1Nodes := make([]*QuorumNode, 0, q1S)
	q2Nodes := make([]*QuorumNode, 0, q2S)

	for _, region := range selectedQ1Regions {
		for i := range nodesPerQ1Region {
			if i < int64(len(nodesCopy[region])) {
				q1Nodes = append(q1Nodes, nodesCopy[region][i])
			}
		}
	}

	for _, region := range selectedQ2Regions {
		lfn := q.calculateLfnDiagnostic(nodesCopy, region, Fn)
		for i := int64(0); i < lfn && i < int64(len(nodesCopy[region])); i++ {
			q2Nodes = append(q2Nodes, nodesCopy[region][i])
		}
	}

	return q1Nodes, q2Nodes, nil
}

// Helper methods for diagnostic quorum calculation that operate on local copies
func (q *defaultQuorumManager) getClosestRegionsDiagnostic(nodes map[RegionName][]*QuorumNode) []RegionName {
	regions := make([]RegionName, 0, len(nodes))
	for region := range nodes {
		regions = append(regions, region)
	}

	sort.SliceStable(regions, func(i, j int) bool {
		iRtt := time.Duration(0)
		jRtt := time.Duration(0)
		for _, node := range nodes[regions[i]] {
			iRtt += safeRttDuration(node)
		}
		for _, node := range nodes[regions[j]] {
			jRtt += safeRttDuration(node)
		}
		return iRtt < jRtt
	})

	return regions
}

func (q *defaultQuorumManager) calculateLfnDiagnostic(nodes map[RegionName][]*QuorumNode, region RegionName, Fn int64) int64 {
	return int64(len(nodes[region])) - Fn
}

func (q *defaultQuorumManager) calculateNDiagnostic(nodes map[RegionName][]*QuorumNode) int64 {
	total := int64(0)
	for _, nodes := range nodes {
		total += int64(len(nodes))
	}
	return total
}

func (q *defaultQuorumManager) calculateQ1SizeDiagnostic(nodes map[RegionName][]*QuorumNode, Fz, Fn int64) int64 {
	q1RegionCount := q.calculateNumberZonesQ1(nodes, Fz)
	return q1RegionCount * q.calculateTotalNodesPerZoneQ1(Fn)
}

func (q *defaultQuorumManager) calculateQ2SizeDiagnostic(nodes map[RegionName][]*QuorumNode, Fn int64, regions ...RegionName) int64 {
	lfn := int64(0)
	for _, region := range regions {
		lfn += q.calculateLfnDiagnostic(nodes, region, Fn)
	}
	return lfn
}

func (q *defaultQuorumManager) calculateFmaxDiagnostic(nodes map[RegionName][]*QuorumNode, q1Size, q2Size, Fz, Fn int64) int64 {
	return q.calculateNDiagnostic(nodes) - q1Size - q2Size + (Fz+1)*(Fn+1)
}

// DescribeQuorum computes and returns diagnostic information about the potential quorum
// for a given table, showing all known nodes regardless of their current health status.
// This is intended for diagnostic purposes only.
func DescribeQuorum(ctx context.Context, table string) (q1 []*QuorumNode, q2 []*QuorumNode, err error) {
	qm := GetDefaultQuorumManager(ctx)
	dqm, ok := qm.(*defaultQuorumManager)
	if !ok {
		return nil, nil, fmt.Errorf("unsupported quorum manager type")
	}

	return dqm.describeQuorumDiagnostic(ctx, table)
}
