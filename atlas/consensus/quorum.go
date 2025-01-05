package consensus

import (
	"context"
	"errors"
	"github.com/bottledcode/atlas-db/atlas"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/emptypb"
	"slices"
	"sort"
	"strconv"
	"sync"
	"time"
)

type QuorumManager interface {
	GetQuorum() (Quorum, error)
	AddNode(node *Node)
}

var manager *defaultQuorumManager

func GetDefaultQuorumManager(ctx context.Context) QuorumManager {
	if manager != nil {
		return manager
	}

	manager = &defaultQuorumManager{
		nodes: make(map[RegionName][]*QuorumNode),
	}

	// control loop for handling the system's quorum and node membership
	go manager.controlLoop(ctx)

	return manager
}

type RegionName string

type defaultQuorumManager struct {
	mu    sync.RWMutex
	nodes map[RegionName][]*QuorumNode
}

func (q *defaultQuorumManager) AddNode(node *Node) {
	q.mu.Lock()
	defer q.mu.Unlock()

	if _, ok := q.nodes[RegionName(node.GetRegion().GetName())]; !ok {
		q.nodes[RegionName(node.GetRegion().GetName())] = make([]*QuorumNode, 0)
	}

	q.nodes[RegionName(node.GetRegion().GetName())] = append(q.nodes[RegionName(node.GetRegion().GetName())], &QuorumNode{
		Node:   node,
		closer: nil,
		client: nil,
	})
}

func (q *defaultQuorumManager) controlLoop(ctx context.Context) {
	nodeChanges := atlas.Ownership.Subscribe(NodeTable)

	conn, err := atlas.MigrationsPool.Take(ctx)
	if err != nil {
		atlas.Logger.Fatal("failed to get a connection from the migration pool", zap.Error(err))
	}
	defer atlas.MigrationsPool.Put(conn)

	nodeRepo := GetDefaultNodeRepository(ctx, conn)

	for {
		select {
		case <-ctx.Done():
			return
		case want := <-atlas.Ownership.Wants:
			switch want.Ownership {
			case false:
				// todo: implement replicating a table
			case true:
				// todo: an in-progress commit needs ownership of a table
			}
		case node := <-nodeChanges:
			// we are only interested in changes to the node table
			if node.OwnershipType != atlas.Change {
				continue
			}

			// construct a new node map
			regions, err := nodeRepo.GetRegions()
			if err != nil {
				atlas.Logger.Fatal("failed to get regions", zap.Error(err))
				continue
			}

			newNodes := make(map[RegionName][]*QuorumNode)

			// what follows is a very inefficient algorithm for reconciling the new node list with the current one.
			// it uses goto to prevent allocations and to break out of nested loops.
			for _, region := range regions {
				list, err := nodeRepo.GetNodesByRegion(region.GetName())
				if err != nil {
					atlas.Logger.Fatal("failed to get nodes by region", zap.Error(err))
					continue
				}
				newNodes[RegionName(region.GetName())] = make([]*QuorumNode, len(list))
				for i, node := range list {
					// search for an already active node in the current list
					if currentList, ok := q.nodes[RegionName(region.GetName())]; ok {
						for _, currentNode := range currentList {
							if currentNode.GetId() == node.GetId() {
								newNodes[RegionName(region.GetName())][i] = currentNode
								// continue the outer loop
								goto next
							}
						}
						// the node isn't found, so create a new one
						goto createNew
					next:
						continue
					}

				createNew:

					// we explicitly do not create a client yet, as we don't want to connect to the node until we need to
					newNodes[RegionName(region.GetName())][i] = &QuorumNode{
						Node:   node,
						closer: nil,
						client: nil,
					}
				}
			}

			// now we have constructed the new node list, we can replace the old one
			q.mu.Lock()
			q.nodes = newNodes
			q.mu.Unlock()
		}
	}
}

type Quorum interface {
	ConsensusClient
}

type QuorumNode struct {
	*Node
	closer func()
	client ConsensusClient
}

func (q *QuorumNode) StealTableOwnership(ctx context.Context, in *StealTableOwnershipRequest, opts ...grpc.CallOption) (*StealTableOwnershipResponse, error) {
	var err error
	if q.client == nil {
		q.client, err, q.closer = getNewClient(q.GetAddress() + ":" + strconv.Itoa(int(q.GetPort())))
		if err != nil {
			return nil, err
		}
	}
	return q.client.StealTableOwnership(ctx, in, opts...)
}

func (q *QuorumNode) WriteMigration(ctx context.Context, in *WriteMigrationRequest, opts ...grpc.CallOption) (*WriteMigrationResponse, error) {
	var err error
	if q.client == nil {
		q.client, err, q.closer = getNewClient(q.GetAddress() + ":" + strconv.Itoa(int(q.GetPort())))
		if err != nil {
			return nil, err
		}
	}
	return q.client.WriteMigration(ctx, in, opts...)
}

func (q *QuorumNode) AcceptMigration(ctx context.Context, in *WriteMigrationRequest, opts ...grpc.CallOption) (*emptypb.Empty, error) {
	var err error
	if q.client == nil {
		q.client, err, q.closer = getNewClient(q.GetAddress() + ":" + strconv.Itoa(int(q.GetPort())))
		if err != nil {
			return nil, err
		}
	}
	return q.client.AcceptMigration(ctx, in, opts...)
}

func (q *QuorumNode) LearnMigration(ctx context.Context, in *LearnMigrationRequest, opts ...grpc.CallOption) (Consensus_LearnMigrationClient, error) {
	var err error
	if q.client == nil {
		q.client, err, q.closer = getNewClient(q.GetAddress() + ":" + strconv.Itoa(int(q.GetPort())))
		if err != nil {
			return nil, err
		}
	}
	return q.client.LearnMigration(ctx, in, opts...)
}

func (q *QuorumNode) JoinCluster(ctx context.Context, in *Node, opts ...grpc.CallOption) (*JoinClusterResponse, error) {
	var err error
	if q.client == nil {
		q.client, err, q.closer = getNewClient(q.GetAddress() + ":" + strconv.Itoa(int(q.GetPort())))
		if err != nil {
			return nil, err
		}
	}
	return q.client.JoinCluster(ctx, in, opts...)
}

func (q *QuorumNode) Close() {
	if q.closer != nil {
		q.closer()
	}
}

// calculateLfn calculates (l-Fn) for the given region name, used in calculating Q2 quorums.
func (q *defaultQuorumManager) calculateLfn(region RegionName, Fn int64) int64 {
	q.mu.RLock()
	defer q.mu.RUnlock()

	return int64(len(q.nodes[region])) - Fn
}

// calculateN calculates the total number of nodes in the cluster, used in validating quorums.
func (q *defaultQuorumManager) calculateN() int64 {
	q.mu.RLock()
	defer q.mu.RUnlock()

	total := int64(0)
	for _, nodes := range q.nodes {
		total += int64(len(nodes))
	}

	return total
}

// calculateTotalNodesPerZoneQ1 calculates the total number of nodes per zone required for a Q1 quorum.
func (q *defaultQuorumManager) calculateTotalNodesPerZoneQ1(Fn int64) int64 {
	return Fn + 1
}

// calculateNumberZonesQ1 calculates the number of zones required for a Q1 quorum.
func (q *defaultQuorumManager) calculateNumberZonesQ1(Fz int64) int64 {
	q.mu.RLock()
	defer q.mu.RUnlock()

	totalRegions := len(q.nodes)
	return int64(totalRegions) - Fz
}

// calculateQ1Size calculates the size required for a Q1 quorum
func (q *defaultQuorumManager) calculateQ1Size(Fz, Fn int64) int64 {
	return q.calculateNumberZonesQ1(Fz) * q.calculateTotalNodesPerZoneQ1(Fn)
}

// calculateNumberZonesQ2 calculates the number of zones required for a Q2 quorum.
func (q *defaultQuorumManager) calculateNumberZonesQ2(Fz int64) int64 {
	return Fz + 1
}

// calculateQ2Size calculates the size required for a Q2 quorum.
func (q *defaultQuorumManager) calculateQ2Size(Fn int64, regions ...RegionName) int64 {
	lfn := int64(0)
	for _, region := range regions {
		lfn += q.calculateLfn(region, Fn)
	}
	return lfn
}

// calculateFmin calculates the minimum number of targeted failures that will disrupt the quorum.
func (q *defaultQuorumManager) calculateFmin(q1Size, q2Size int64) int64 {
	return min(q1Size, q2Size) - 1
}

// calculateFmax calculates the minimum number of random failures that will disrupt the quorum.
func (q *defaultQuorumManager) calculateFmax(q1Size, q2Size, Fz, Fn int64) int64 {
	return q.calculateN() - q1Size - q2Size + (Fz+1)*(Fn+1)
}

// getClosestRegions returns a list of regions sorted by the average rtt of each node.
func (q *defaultQuorumManager) getClosestRegions() []RegionName {
	q.mu.RLock()
	defer q.mu.RUnlock()

	// extract the list of regions from the node map
	regions := make([]RegionName, 0, len(q.nodes))
	for region := range q.nodes {
		regions = append(regions, region)
	}

	// sort the regions by the average rtt of each node
	sort.SliceStable(regions, func(i, j int) bool {
		// calculate the average rtt for each region
		iRtt := time.Duration(0)
		jRtt := time.Duration(0)
		for _, node := range q.nodes[regions[i]] {
			iRtt += node.GetRtt().AsDuration()
		}
		for _, node := range q.nodes[regions[j]] {
			jRtt += node.GetRtt().AsDuration()
		}

		return iRtt < jRtt
	})

	return regions
}

// GetQuorum returns the quorum for stealing a table. It uses a grid-based approach to determine the best solution.
func (q *defaultQuorumManager) GetQuorum() (Quorum, error) {
	// get the number of regions we have active nodes in
	q.mu.RLock()
	defer q.mu.RUnlock()

	Fz := atlas.CurrentOptions.GetFz()
	Fn := atlas.CurrentOptions.GetFn()

recalculate:

	// before we can calculate the quorum, we need to validate the quorum is possible
	q1RegionCount := q.calculateNumberZonesQ1(Fz)
	if q1RegionCount < 1 {
		Fz = Fz - 1
		if Fz < 0 {
			return nil, errors.New("unable to form a quorum")
		}
		goto recalculate
	}

	farRegions := q.getClosestRegions()
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
		lfn := q.calculateLfn(region, Fn)
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
	q1S := q.calculateQ1Size(Fz, Fn)
	q2S := q.calculateQ2Size(Fn, selectedQ2Regions...)
	Fmax := q.calculateFmax(q1S, q2S, Fz, Fn)
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
		for i := int64(0); i < nodesPerQ1Region; i++ {
			q1 = append(q1, q.nodes[region][i])
		}
	}

	for _, region := range selectedQ2Regions {
		for i := int64(0); i < q.calculateLfn(region, Fn); i++ {
			q2 = append(q2, q.nodes[region][i])
		}
	}

	// validate the sizes
	if int64(len(q1)) != q1S || int64(len(q2)) != q2S {
		return nil, errors.New("quorum size mismatch")
	}

	return &majorityQuorum{
		q1: q1,
		q2: q2,
	}, nil
}
