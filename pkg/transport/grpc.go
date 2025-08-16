package transport

import (
	"context"
	"fmt"
	"net"
	"sync"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/withinboredom/atlas-db-2/proto/atlas"
)

type GRPCTransport struct {
	atlas.UnimplementedConsensusServer
	atlas.UnimplementedAtlasDBServer
	nodeID       string
	address      string
	regionID     int32
	server       *grpc.Server
	connections  map[string]*grpc.ClientConn
	nodes        map[string]*atlas.NodeInfo
	regions      map[int32][]string // regionID -> nodeIDs
	consensus    ConsensusHandler
	database     DatabaseHandler
	mu           sync.RWMutex
}

type ConsensusHandler interface {
	HandlePrepare(ctx context.Context, req *atlas.PrepareRequest) (*atlas.PrepareResponse, error)
	HandleAccept(ctx context.Context, req *atlas.AcceptRequest) (*atlas.AcceptResponse, error)
	HandleCommit(ctx context.Context, req *atlas.CommitRequest) (*atlas.CommitResponse, error)
}

type DatabaseHandler interface {
	HandleGet(ctx context.Context, req *atlas.GetRequest) (*atlas.GetResponse, error)
	HandlePut(ctx context.Context, req *atlas.PutRequest) (*atlas.PutResponse, error)
	HandleDelete(ctx context.Context, req *atlas.DeleteRequest) (*atlas.DeleteResponse, error)
	HandleScan(req *atlas.ScanRequest, stream atlas.AtlasDB_ScanServer) error
	HandleBatch(ctx context.Context, req *atlas.BatchRequest) (*atlas.BatchResponse, error)
}

func NewGRPCTransport(nodeID, address string, regionID int32) *GRPCTransport {
	return &GRPCTransport{
		nodeID:      nodeID,
		address:     address,
		regionID:    regionID,
		connections: make(map[string]*grpc.ClientConn),
		nodes:       make(map[string]*atlas.NodeInfo),
		regions:     make(map[int32][]string),
	}
}

func (gt *GRPCTransport) Start(consensus ConsensusHandler, database DatabaseHandler) error {
	gt.consensus = consensus
	gt.database = database

	lis, err := net.Listen("tcp", gt.address)
	if err != nil {
		return fmt.Errorf("failed to listen: %w", err)
	}

	gt.server = grpc.NewServer()
	
	// Register services
	atlas.RegisterConsensusServer(gt.server, gt)
	atlas.RegisterAtlasDBServer(gt.server, gt)

	go func() {
		if err := gt.server.Serve(lis); err != nil {
			// Log error in production
			fmt.Printf("Server failed: %v\n", err)
		}
	}()

	return nil
}

func (gt *GRPCTransport) Stop() {
	if gt.server != nil {
		gt.server.GracefulStop()
	}

	gt.mu.Lock()
	defer gt.mu.Unlock()

	for _, conn := range gt.connections {
		conn.Close()
	}
}

func (gt *GRPCTransport) AddNode(nodeInfo *atlas.NodeInfo) {
	gt.mu.Lock()
	defer gt.mu.Unlock()

	gt.nodes[nodeInfo.NodeId] = nodeInfo
	
	regionNodes, exists := gt.regions[nodeInfo.RegionId]
	if !exists {
		regionNodes = make([]string, 0)
	}
	
	// Add node if not already present
	found := false
	for _, id := range regionNodes {
		if id == nodeInfo.NodeId {
			found = true
			break
		}
	}
	
	if !found {
		gt.regions[nodeInfo.RegionId] = append(regionNodes, nodeInfo.NodeId)
	}
}

func (gt *GRPCTransport) RemoveNode(nodeID string) {
	gt.mu.Lock()
	defer gt.mu.Unlock()

	nodeInfo, exists := gt.nodes[nodeID]
	if !exists {
		return
	}

	delete(gt.nodes, nodeID)

	// Remove from region
	regionNodes := gt.regions[nodeInfo.RegionId]
	for i, id := range regionNodes {
		if id == nodeID {
			gt.regions[nodeInfo.RegionId] = append(regionNodes[:i], regionNodes[i+1:]...)
			break
		}
	}

	// Close connection if exists
	if conn, exists := gt.connections[nodeID]; exists {
		conn.Close()
		delete(gt.connections, nodeID)
	}
}

func (gt *GRPCTransport) getConnection(nodeID string) (*grpc.ClientConn, error) {
	gt.mu.Lock()
	defer gt.mu.Unlock()

	if conn, exists := gt.connections[nodeID]; exists {
		return conn, nil
	}

	nodeInfo, exists := gt.nodes[nodeID]
	if !exists {
		return nil, fmt.Errorf("node %s not found", nodeID)
	}

	conn, err := grpc.Dial(nodeInfo.Address, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, fmt.Errorf("failed to connect to %s: %w", nodeInfo.Address, err)
	}

	gt.connections[nodeID] = conn
	return conn, nil
}

// Consensus interface implementation
func (gt *GRPCTransport) SendPrepare(ctx context.Context, nodeID string, req *atlas.PrepareRequest) (*atlas.PrepareResponse, error) {
	conn, err := gt.getConnection(nodeID)
	if err != nil {
		return nil, err
	}

	client := atlas.NewConsensusClient(conn)
	return client.Prepare(ctx, req)
}

func (gt *GRPCTransport) SendAccept(ctx context.Context, nodeID string, req *atlas.AcceptRequest) (*atlas.AcceptResponse, error) {
	conn, err := gt.getConnection(nodeID)
	if err != nil {
		return nil, err
	}

	client := atlas.NewConsensusClient(conn)
	return client.Accept(ctx, req)
}

func (gt *GRPCTransport) SendCommit(ctx context.Context, nodeID string, req *atlas.CommitRequest) (*atlas.CommitResponse, error) {
	conn, err := gt.getConnection(nodeID)
	if err != nil {
		return nil, err
	}

	client := atlas.NewConsensusClient(conn)
	return client.Commit(ctx, req)
}

func (gt *GRPCTransport) GetActiveNodes(regionID int32) []string {
	gt.mu.RLock()
	defer gt.mu.RUnlock()

	if nodes, exists := gt.regions[regionID]; exists {
		// Return copy to avoid race conditions
		result := make([]string, len(nodes))
		copy(result, nodes)
		return result
	}
	return []string{}
}

func (gt *GRPCTransport) GetAllRegions() []int32 {
	gt.mu.RLock()
	defer gt.mu.RUnlock()

	regions := make([]int32, 0, len(gt.regions))
	for regionID := range gt.regions {
		regions = append(regions, regionID)
	}
	return regions
}

// gRPC server implementations

// Consensus service implementation
func (gt *GRPCTransport) Prepare(ctx context.Context, req *atlas.PrepareRequest) (*atlas.PrepareResponse, error) {
	if gt.consensus == nil {
		return nil, fmt.Errorf("consensus handler not set")
	}
	return gt.consensus.HandlePrepare(ctx, req)
}

func (gt *GRPCTransport) Accept(ctx context.Context, req *atlas.AcceptRequest) (*atlas.AcceptResponse, error) {
	if gt.consensus == nil {
		return nil, fmt.Errorf("consensus handler not set")
	}
	return gt.consensus.HandleAccept(ctx, req)
}

func (gt *GRPCTransport) Commit(ctx context.Context, req *atlas.CommitRequest) (*atlas.CommitResponse, error) {
	if gt.consensus == nil {
		return nil, fmt.Errorf("consensus handler not set")
	}
	return gt.consensus.HandleCommit(ctx, req)
}

func (gt *GRPCTransport) Heartbeat(ctx context.Context, req *atlas.HeartbeatRequest) (*atlas.HeartbeatResponse, error) {
	gt.mu.RLock()
	defer gt.mu.RUnlock()

	// Update node's last seen time
	if nodeInfo, exists := gt.nodes[req.NodeId]; exists {
		nodeInfo.LastSeen = req.Timestamp
	}

	// Return list of active nodes in the same region
	activeNodes := gt.GetActiveNodes(req.RegionId)
	
	return &atlas.HeartbeatResponse{
		Alive:       true,
		ActiveNodes: activeNodes,
	}, nil
}

// Database service implementation
func (gt *GRPCTransport) Get(ctx context.Context, req *atlas.GetRequest) (*atlas.GetResponse, error) {
	if gt.database == nil {
		return nil, fmt.Errorf("database handler not set")
	}
	return gt.database.HandleGet(ctx, req)
}

func (gt *GRPCTransport) Put(ctx context.Context, req *atlas.PutRequest) (*atlas.PutResponse, error) {
	if gt.database == nil {
		return nil, fmt.Errorf("database handler not set")
	}
	return gt.database.HandlePut(ctx, req)
}

func (gt *GRPCTransport) Delete(ctx context.Context, req *atlas.DeleteRequest) (*atlas.DeleteResponse, error) {
	if gt.database == nil {
		return nil, fmt.Errorf("database handler not set")
	}
	return gt.database.HandleDelete(ctx, req)
}

func (gt *GRPCTransport) Scan(req *atlas.ScanRequest, stream atlas.AtlasDB_ScanServer) error {
	if gt.database == nil {
		return fmt.Errorf("database handler not set")
	}
	return gt.database.HandleScan(req, stream)
}

func (gt *GRPCTransport) Batch(ctx context.Context, req *atlas.BatchRequest) (*atlas.BatchResponse, error) {
	if gt.database == nil {
		return nil, fmt.Errorf("database handler not set")
	}
	return gt.database.HandleBatch(ctx, req)
}

// Client methods for database operations
func (gt *GRPCTransport) SendGet(ctx context.Context, nodeID string, req *atlas.GetRequest) (*atlas.GetResponse, error) {
	conn, err := gt.getConnection(nodeID)
	if err != nil {
		return nil, err
	}

	client := atlas.NewAtlasDBClient(conn)
	return client.Get(ctx, req)
}

func (gt *GRPCTransport) SendPut(ctx context.Context, nodeID string, req *atlas.PutRequest) (*atlas.PutResponse, error) {
	conn, err := gt.getConnection(nodeID)
	if err != nil {
		return nil, err
	}

	client := atlas.NewAtlasDBClient(conn)
	return client.Put(ctx, req)
}

func (gt *GRPCTransport) SendDelete(ctx context.Context, nodeID string, req *atlas.DeleteRequest) (*atlas.DeleteResponse, error) {
	conn, err := gt.getConnection(nodeID)
	if err != nil {
		return nil, err
	}

	client := atlas.NewAtlasDBClient(conn)
	return client.Delete(ctx, req)
}