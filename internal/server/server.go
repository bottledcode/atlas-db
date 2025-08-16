package server

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/withinboredom/atlas-db-2/internal/region"
	"github.com/withinboredom/atlas-db-2/pkg/config"
	"github.com/withinboredom/atlas-db-2/pkg/consensus"
	"github.com/withinboredom/atlas-db-2/pkg/storage"
	"github.com/withinboredom/atlas-db-2/pkg/transport"
	"github.com/withinboredom/atlas-db-2/proto/atlas"
)

type Server struct {
	config     *config.Config
	storage    *storage.BadgerStorage
	transport  *transport.GRPCTransport
	consensus  *consensus.WPaxos
	optimizer  *region.WriteOptimizer
	running    bool
	mu         sync.RWMutex
}

func NewServer(cfg *config.Config) (*Server, error) {
	// Initialize storage
	store, err := storage.NewBadgerStorage(cfg.Node.DataDir)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize storage: %w", err)
	}

	// Initialize transport - use port from transport config
	listenAddr := fmt.Sprintf(":%d", cfg.Transport.Port)
	if cfg.Transport.Port == 0 {
		// Use the address from node config if port is 0 (for tests)
		listenAddr = cfg.Node.Address
	}
	tr := transport.NewGRPCTransport(cfg.Node.ID, listenAddr, cfg.Node.RegionID)

	// Initialize consensus
	wp := consensus.NewWPaxos(cfg.Node.ID, cfg.Node.RegionID, tr, store)

	// Initialize region optimizer
	regionStorage := storage.NewRegionAdapter(store)
	opt := region.NewWriteOptimizer(cfg.Node.RegionID, tr, regionStorage)

	return &Server{
		config:    cfg,
		storage:   store,
		transport: tr,
		consensus: wp,
		optimizer: opt,
	}, nil
}

func (s *Server) Start() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.running {
		return fmt.Errorf("server is already running")
	}

	// Start transport layer
	if err := s.transport.Start(s, s); err != nil {
		return fmt.Errorf("failed to start transport: %w", err)
	}

	// Start region optimizer latency monitoring
	s.optimizer.StartLatencyMonitoring()

	// Start garbage collection routine
	go s.startGCRoutine()

	s.running = true
	return nil
}

func (s *Server) Stop() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if !s.running {
		return nil
	}

	s.transport.Stop()
	s.storage.Close()
	s.running = false
	return nil
}

func (s *Server) IsRunning() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.running
}

func (s *Server) AddNode(nodeInfo *atlas.NodeInfo) {
	s.transport.AddNode(nodeInfo)
	s.optimizer.AddNode(nodeInfo)
}

func (s *Server) RemoveNode(nodeID string, regionID int32) {
	s.transport.RemoveNode(nodeID)
	s.optimizer.RemoveNode(nodeID, regionID)
}

func (s *Server) startGCRoutine() {
	ticker := time.NewTicker(s.config.Storage.GCInterval)
	defer ticker.Stop()

	for range ticker.C {
		if err := s.storage.GC(); err != nil {
			// Log error in production
			fmt.Printf("GC error: %v\n", err)
		}
	}
}

// Database service implementation
func (s *Server) HandleGet(ctx context.Context, req *atlas.GetRequest) (*atlas.GetResponse, error) {
	strategy := region.ReadLocal
	if req.ConsistentRead {
		strategy = region.ReadConsistent
	} else {
		switch s.config.Region.ReadStrategy {
		case "nearest":
			strategy = region.ReadNearest
		case "consistent":
			strategy = region.ReadConsistent
		default:
			strategy = region.ReadLocal
		}
	}

	return s.optimizer.OptimizedGet(ctx, req.Key, strategy)
}

func (s *Server) HandlePut(ctx context.Context, req *atlas.PutRequest) (*atlas.PutResponse, error) {
	// Store directly to storage for now (can be enhanced with consensus later)
	if err := s.storage.Put(ctx, req.Key, req.Value); err != nil {
		return &atlas.PutResponse{
			Success: false,
			Error:   err.Error(),
		}, nil
	}

	// Get the stored version from storage
	kv, err := s.storage.Get(ctx, req.Key)
	if err != nil {
		return &atlas.PutResponse{
			Success: false,
			Error:   err.Error(),
		}, nil
	}

	return &atlas.PutResponse{
		Success: true,
		Version: kv.Version,
	}, nil
}

func (s *Server) HandleDelete(ctx context.Context, req *atlas.DeleteRequest) (*atlas.DeleteResponse, error) {
	// Delete directly from storage for now (can be enhanced with consensus later)
	if err := s.storage.Delete(ctx, req.Key); err != nil {
		return &atlas.DeleteResponse{
			Success: false,
			Error:   err.Error(),
		}, nil
	}

	return &atlas.DeleteResponse{Success: true}, nil
}

func (s *Server) HandleScan(req *atlas.ScanRequest, stream atlas.AtlasDB_ScanServer) error {
	ctx := stream.Context()
	
	opts := storage.ScanOptions{
		StartKey: req.StartKey,
		EndKey:   req.EndKey,
		Limit:    int(req.Limit),
		Reverse:  req.Reverse,
	}

	results, err := s.storage.Scan(ctx, opts)
	if err != nil {
		return err
	}

	for _, result := range results {
		resp := &atlas.ScanResponse{
			Key:     result.Key,
			Value:   result.Value,
			Version: result.Version,
		}
		
		if err := stream.Send(resp); err != nil {
			return err
		}
	}

	return nil
}

func (s *Server) HandleBatch(ctx context.Context, req *atlas.BatchRequest) (*atlas.BatchResponse, error) {
	results := make([]*atlas.OperationResult, len(req.Operations))

	// For simplicity, execute operations sequentially
	// In production, we could optimize this
	for i, op := range req.Operations {
		switch op.Type {
		case atlas.Operation_GET:
			getReq := &atlas.GetRequest{Key: op.Key}
			getResp, err := s.HandleGet(ctx, getReq)
			if err != nil {
				results[i] = &atlas.OperationResult{
					Success: false,
					Error:   err.Error(),
				}
			} else {
				results[i] = &atlas.OperationResult{
					Success: getResp.Found,
					Value:   getResp.Value,
					Version: getResp.Version,
				}
			}

		case atlas.Operation_PUT:
			putReq := &atlas.PutRequest{
				Key:             op.Key,
				Value:           op.Value,
				ExpectedVersion: op.ExpectedVersion,
			}
			putResp, err := s.HandlePut(ctx, putReq)
			if err != nil {
				results[i] = &atlas.OperationResult{
					Success: false,
					Error:   err.Error(),
				}
			} else {
				results[i] = &atlas.OperationResult{
					Success: putResp.Success,
					Version: putResp.Version,
					Error:   putResp.Error,
				}
			}

		case atlas.Operation_DELETE:
			delReq := &atlas.DeleteRequest{
				Key:             op.Key,
				ExpectedVersion: op.ExpectedVersion,
			}
			delResp, err := s.HandleDelete(ctx, delReq)
			if err != nil {
				results[i] = &atlas.OperationResult{
					Success: false,
					Error:   err.Error(),
				}
			} else {
				results[i] = &atlas.OperationResult{
					Success: delResp.Success,
					Error:   delResp.Error,
				}
			}

		default:
			results[i] = &atlas.OperationResult{
				Success: false,
				Error:   "unsupported operation type",
			}
		}
	}

	return &atlas.BatchResponse{Results: results}, nil
}

// Consensus service implementation
func (s *Server) HandlePrepare(ctx context.Context, req *atlas.PrepareRequest) (*atlas.PrepareResponse, error) {
	return s.consensus.HandlePrepare(ctx, req)
}

func (s *Server) HandleAccept(ctx context.Context, req *atlas.AcceptRequest) (*atlas.AcceptResponse, error) {
	return s.consensus.HandleAccept(ctx, req)
}

func (s *Server) HandleCommit(ctx context.Context, req *atlas.CommitRequest) (*atlas.CommitResponse, error) {
	return s.consensus.HandleCommit(ctx, req)
}

// GetStats returns server statistics
func (s *Server) GetStats() (*ServerStats, error) {
	storageStats, err := s.storage.Stats()
	if err != nil {
		return nil, err
	}

	return &ServerStats{
		NodeID:       s.config.Node.ID,
		RegionID:     s.config.Node.RegionID,
		StorageStats: storageStats,
		Running:      s.IsRunning(),
	}, nil
}

type ServerStats struct {
	NodeID       string                  `json:"node_id"`
	RegionID     int32                   `json:"region_id"`
	StorageStats *storage.StorageStats   `json:"storage_stats"`
	Running      bool                    `json:"running"`
}