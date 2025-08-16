package consensus

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/withinboredom/atlas-db-2/proto/atlas"
)

type ProposalState int

const (
	ProposalPreparing ProposalState = iota
	ProposalAccepting
	ProposalCommitted
	ProposalAborted
)

type Proposal struct {
	ID           string
	Number       int64
	Value        []byte
	State        ProposalState
	RegionID     int32
	FastPath     bool
	Promises     map[string]*atlas.PrepareResponse
	Accepts      map[string]*atlas.AcceptResponse
	CreatedAt    time.Time
	mu           sync.RWMutex
}

type WPaxos struct {
	nodeID       string
	regionID     int32
	proposals    map[string]*Proposal
	acceptedVals map[string][]byte
	promisedNum  map[string]int64
	transport    Transport
	storage      Storage
	mu           sync.RWMutex
}

type Transport interface {
	SendPrepare(ctx context.Context, nodeID string, req *atlas.PrepareRequest) (*atlas.PrepareResponse, error)
	SendAccept(ctx context.Context, nodeID string, req *atlas.AcceptRequest) (*atlas.AcceptResponse, error)
	SendCommit(ctx context.Context, nodeID string, req *atlas.CommitRequest) (*atlas.CommitResponse, error)
	GetActiveNodes(regionID int32) []string
	GetAllRegions() []int32
}

type Storage interface {
	Store(key string, value []byte) error
	Load(key string) ([]byte, error)
	DeleteKey(key string) error
}

func NewWPaxos(nodeID string, regionID int32, transport Transport, storage Storage) *WPaxos {
	return &WPaxos{
		nodeID:       nodeID,
		regionID:     regionID,
		proposals:    make(map[string]*Proposal),
		acceptedVals: make(map[string][]byte),
		promisedNum:  make(map[string]int64),
		transport:    transport,
		storage:      storage,
	}
}

func (wp *WPaxos) Propose(ctx context.Context, instanceID string, value []byte) error {
	wp.mu.Lock()
	proposalNum := time.Now().UnixNano()
	proposal := &Proposal{
		ID:        instanceID,
		Number:    proposalNum,
		Value:     value,
		State:     ProposalPreparing,
		RegionID:  wp.regionID,
		FastPath:  true,
		Promises:  make(map[string]*atlas.PrepareResponse),
		Accepts:   make(map[string]*atlas.AcceptResponse),
		CreatedAt: time.Now(),
	}
	wp.proposals[instanceID] = proposal
	wp.mu.Unlock()

	// Phase 1: Prepare
	if err := wp.prepare(ctx, proposal); err != nil {
		return fmt.Errorf("prepare phase failed: %w", err)
	}

	// Phase 2: Accept
	if err := wp.accept(ctx, proposal); err != nil {
		return fmt.Errorf("accept phase failed: %w", err)
	}

	// Phase 3: Commit
	return wp.commit(ctx, proposal)
}

func (wp *WPaxos) prepare(ctx context.Context, proposal *Proposal) error {
	proposal.mu.Lock()
	defer proposal.mu.Unlock()

	req := &atlas.PrepareRequest{
		ProposalNumber: proposal.Number,
		InstanceId:     proposal.ID,
		RegionId:       wp.regionID,
	}

	// Get nodes in local region first for fast path
	localNodes := wp.transport.GetActiveNodes(wp.regionID)
	
	// Send prepare to local region nodes
	promises := 0
	for _, nodeID := range localNodes {
		if nodeID == wp.nodeID {
			continue // Skip self
		}
		
		resp, err := wp.transport.SendPrepare(ctx, nodeID, req)
		if err != nil || resp == nil {
			continue // Node might be down
		}
		
		if resp.Promise {
			proposal.Promises[nodeID] = resp
			promises++
		}
	}

	// Check if we have fast quorum (majority in local region)
	// Include self in quorum calculation since we always "promise" to ourselves
	fastQuorum := len(localNodes)/2 + 1
	localMajority := promises+1 >= fastQuorum // +1 for self
	
	// For W-Paxos fast path, we also need at least one node from every other region
	allRegions := wp.transport.GetAllRegions()
	regionsWithPromises := make(map[int32]bool)
	regionsWithPromises[wp.regionID] = localMajority // Local region has majority
	
	// Contact other regions to get grid quorum
	for _, regionID := range allRegions {
		if regionID == wp.regionID {
			continue // Already contacted local region
		}
		
		regionNodes := wp.transport.GetActiveNodes(regionID)
		regionPromises := 0
		for _, nodeID := range regionNodes {
			resp, err := wp.transport.SendPrepare(ctx, nodeID, req)
			if err != nil || resp == nil {
				continue
			}
			
			if resp.Promise {
				proposal.Promises[nodeID] = resp
				promises++
				regionPromises++
				// For fast path, we only need one node per region
				break
			}
		}
		regionsWithPromises[regionID] = regionPromises > 0
	}
	
	// Check if we can use fast path (local majority + at least one from each region)
	canUseFastPath := localMajority
	for _, regionID := range allRegions {
		if !regionsWithPromises[regionID] {
			canUseFastPath = false
			break
		}
	}
	
	if canUseFastPath {
		proposal.FastPath = true
		return nil
	}
	
	// Fall back to slow path - need global majority
	proposal.FastPath = false

	// Check for global majority
	totalNodes := 0
	for _, regionID := range allRegions {
		totalNodes += len(wp.transport.GetActiveNodes(regionID))
	}
	
	if promises+1 < totalNodes/2+1 { // +1 for self
		proposal.State = ProposalAborted
		return fmt.Errorf("insufficient promises: got %d, need %d", promises+1, totalNodes/2+1)
	}

	return nil
}

func (wp *WPaxos) accept(ctx context.Context, proposal *Proposal) error {
	proposal.mu.Lock()
	defer proposal.mu.Unlock()

	proposal.State = ProposalAccepting

	req := &atlas.AcceptRequest{
		ProposalNumber: proposal.Number,
		InstanceId:     proposal.ID,
		Value:          proposal.Value,
		RegionId:       wp.regionID,
		FastPath:       proposal.FastPath,
	}

	accepts := 0
	
	if proposal.FastPath {
		// Fast path - need local majority + at least one from each other region (grid quorum)
		localNodes := wp.transport.GetActiveNodes(wp.regionID)
		localAccepts := 0
		
		// Get accepts from local region
		for _, nodeID := range localNodes {
			if nodeID == wp.nodeID {
				continue
			}
			
			resp, err := wp.transport.SendAccept(ctx, nodeID, req)
			if err != nil || resp == nil {
				continue
			}
			
			if resp.Accepted {
				proposal.Accepts[nodeID] = resp
				accepts++
				localAccepts++
			}
		}
		
		// Check local majority
		localMajority := localAccepts+1 >= len(localNodes)/2+1 // +1 for self
		if !localMajority {
			// Fast path failed, fall through to slow path
			proposal.FastPath = false
		} else {
			// Get accepts from other regions (at least one per region)
			allRegions := wp.transport.GetAllRegions()
			gridComplete := true
			
			for _, regionID := range allRegions {
				if regionID == wp.regionID {
					continue // Already handled local region
				}
				
				regionNodes := wp.transport.GetActiveNodes(regionID)
				regionAccepts := 0
				
				for _, nodeID := range regionNodes {
					resp, err := wp.transport.SendAccept(ctx, nodeID, req)
					if err != nil || resp == nil {
						continue
					}
					
					if resp.Accepted {
						proposal.Accepts[nodeID] = resp
						accepts++
						regionAccepts++
						// For fast path, we only need one node per region
						break
					}
				}
				
				if regionAccepts == 0 {
					gridComplete = false
					break
				}
			}
			
			if gridComplete {
				return nil // Fast path success
			}
			
			// Grid not complete, fall through to slow path
			proposal.FastPath = false
		}
	}

	// Slow path or fast path failed
	proposal.FastPath = false
	req.FastPath = false
	
	allRegions := wp.transport.GetAllRegions()
	totalNodes := 0
	
	for _, regionID := range allRegions {
		regionNodes := wp.transport.GetActiveNodes(regionID)
		totalNodes += len(regionNodes)
		
		for _, nodeID := range regionNodes {
			if nodeID == wp.nodeID {
				continue
			}
			
			resp, err := wp.transport.SendAccept(ctx, nodeID, req)
			if err != nil || resp == nil {
				continue
			}
			
			if resp.Accepted {
				proposal.Accepts[nodeID] = resp
				accepts++
			}
		}
	}

	if accepts+1 < totalNodes/2+1 { // +1 for self
		proposal.State = ProposalAborted
		return fmt.Errorf("insufficient accepts: got %d, need %d", accepts+1, totalNodes/2+1)
	}

	return nil
}

func (wp *WPaxos) commit(ctx context.Context, proposal *Proposal) error {
	proposal.mu.Lock()
	defer proposal.mu.Unlock()

	req := &atlas.CommitRequest{
		InstanceId:     proposal.ID,
		Value:          proposal.Value,
		ProposalNumber: proposal.Number,
	}

	// Store locally first
	if err := wp.storage.Store(proposal.ID, proposal.Value); err != nil {
		return fmt.Errorf("failed to store locally: %w", err)
	}

	// Send commit to all nodes that accepted (synchronously to ensure completion)
	for nodeID := range proposal.Accepts {
		_, err := wp.transport.SendCommit(ctx, nodeID, req)
		if err != nil {
			// Log error in production, but don't fail the commit
			fmt.Printf("Failed to send commit to %s: %v\n", nodeID, err)
		}
	}

	proposal.State = ProposalCommitted
	
	// Clean up the proposal from the proposing node
	wp.mu.Lock()
	delete(wp.proposals, proposal.ID)
	wp.mu.Unlock()
	
	return nil
}

// Handle incoming prepare requests
func (wp *WPaxos) HandlePrepare(ctx context.Context, req *atlas.PrepareRequest) (*atlas.PrepareResponse, error) {
	wp.mu.Lock()
	defer wp.mu.Unlock()

	instanceID := req.InstanceId
	proposalNum := req.ProposalNumber

	// Check if we've already promised a higher number
	if promised, exists := wp.promisedNum[instanceID]; exists && promised > proposalNum {
		return &atlas.PrepareResponse{
			Promise:          false,
			HighestProposal:  promised,
		}, nil
	}

	// Promise this proposal
	wp.promisedNum[instanceID] = proposalNum

	resp := &atlas.PrepareResponse{
		Promise:         true,
		HighestProposal: proposalNum,
	}

	// If we've accepted a value, include it
	if value, exists := wp.acceptedVals[instanceID]; exists {
		resp.AcceptedValue = value
		resp.AcceptedProposal = wp.promisedNum[instanceID]
	}

	return resp, nil
}

// Handle incoming accept requests
func (wp *WPaxos) HandleAccept(ctx context.Context, req *atlas.AcceptRequest) (*atlas.AcceptResponse, error) {
	wp.mu.Lock()
	defer wp.mu.Unlock()

	instanceID := req.InstanceId
	proposalNum := req.ProposalNumber

	// Check if we've promised a higher number
	if promised, exists := wp.promisedNum[instanceID]; exists && promised > proposalNum {
		return &atlas.AcceptResponse{
			Accepted:       false,
			ProposalNumber: promised,
		}, nil
	}

	// Accept the proposal
	wp.acceptedVals[instanceID] = req.Value
	wp.promisedNum[instanceID] = proposalNum

	return &atlas.AcceptResponse{
		Accepted:       true,
		ProposalNumber: proposalNum,
	}, nil
}

// Handle incoming commit requests
func (wp *WPaxos) HandleCommit(ctx context.Context, req *atlas.CommitRequest) (*atlas.CommitResponse, error) {
	// Store the committed value
	if err := wp.storage.Store(req.InstanceId, req.Value); err != nil {
		return &atlas.CommitResponse{Committed: false}, err
	}

	// Clean up local state
	wp.mu.Lock()
	delete(wp.proposals, req.InstanceId)
	wp.mu.Unlock()

	return &atlas.CommitResponse{Committed: true}, nil
}