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
	"fmt"

	"github.com/bottledcode/atlas-db/atlas/options"
)

// ReadKey reads a key from the consensus layer.
// It returns the current Record for the key, which contains:
// - DataReference (CAS address to the actual value)
// - ACL information
//
// Without watermarks, reads must go through the current owner/leader.
// This function:
// 1. Checks if we own the key (fast path - read locally)
// 2. If not, performs Phase-1 discovery with <0,0> ballot to find leader
// 3. If we're the leader, read locally
// 4. Otherwise, steals ownership and reads (ensures linearizability)
func ReadKey(ctx context.Context, key []byte) (*Record, error) {
	// Fast path: if we own the key, read locally
	if IsOwned(key) {
		return readLocal(ctx, key)
	}

	// Get quorum manager
	qm := GetDefaultQuorumManager(ctx)

	// Get majority quorum
	q, err := qm.GetQuorum(ctx, string(key))
	if err != nil {
		return nil, fmt.Errorf("failed to get quorum: %w", err)
	}

	// Phase-1 discovery with <0,0> ballot to find the current leader
	// This uses Q1 (via majorityQuorum.StealTableOwnership -> q1.StealTableOwnership)
	discoveryResp, err := q.StealTableOwnership(ctx, &StealTableOwnershipRequest{
		Ballot: &Ballot{
			Key:  key,
			Id:   0,
			Node: 0,
		},
	})
	if err != nil {
		return nil, fmt.Errorf("phase-1 discovery failed: %w", err)
	}

	// Check if we are the leader
	var leaderNodeID uint64
	if discoveryResp.HighestBallot != nil {
		leaderNodeID = discoveryResp.HighestBallot.Node
	}

	// If we are the leader (or there's no leader yet), read locally
	if leaderNodeID == options.CurrentOptions.ServerId || leaderNodeID == 0 {
		return readLocal(ctx, key)
	}

	// We're not the leader - forward the read to the specific leader node
	// Get the connection manager to find the leader node
	connectionManager := GetNodeConnectionManager(ctx)
	if connectionManager == nil {
		return nil, fmt.Errorf("no connection manager available")
	}

	connectionManager.mu.RLock()
	leaderNode, exists := connectionManager.nodes[leaderNodeID]
	connectionManager.mu.RUnlock()

	if !exists {
		return nil, fmt.Errorf("leader node %d not found in connection manager", leaderNodeID)
	}

	// Forward read to the leader using its client
	readResp, err := leaderNode.client.ReadRecord(ctx, &ReadRecordRequest{
		Key: key,
	})
	if err == nil && readResp.Success {
		return readResp.Record, nil
	}

	// Leader not available or read failed - perform a real steal to become the new leader
	// Use WriteMutation to properly steal with Q1/Q2
	// But we just want to steal, not write anything, so we'll do Phase-1 manually
	owned := getCurrentOwnershipState(key)
	owned.mu.RLock()
	nextBallot := &Ballot{
		Key:  key,
		Id:   owned.promised.Id + 1,
		Node: options.CurrentOptions.ServerId,
	}
	owned.mu.RUnlock()

	// Perform Phase-1 on Q1 to steal ownership
	stealResp, err := q.StealTableOwnership(ctx, &StealTableOwnershipRequest{
		Ballot: nextBallot,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to steal ownership after leader unavailable: %w", err)
	}

	if !stealResp.Promised {
		return nil, fmt.Errorf("failed to acquire ownership, highest ballot: %v", stealResp.HighestBallot)
	}

	// Mark ourselves as owner
	owned.mu.Lock()
	owned.owned = true
	owned.promised = nextBallot
	if stealResp.HighestSlot != nil {
		owned.maxAppliedSlot = stealResp.HighestSlot.Id
	}
	owned.mu.Unlock()

	// Now we own it, read locally
	return readLocal(ctx, key)
}

// readLocal reads a key from the local state machine
func readLocal(ctx context.Context, key []byte) (*Record, error) {
	// Try to get from cache first
	if record, ok := stateMachine.Get(key); ok {
		if canRead(ctx, record) {
			return record, nil
		}

		return nil, nil
	}

	// Not in cache, need to recover from log
	_, release, err := NewServer().recoverKey(key)
	if err != nil {
		return nil, fmt.Errorf("failed to recover key: %w", err)
	}
	defer release()

	// Now get from cache
	record, ok := stateMachine.Get(key)
	if !ok {
		return nil, fmt.Errorf("key not found")
	}

	if canRead(ctx, record) {
		return record, nil
	}

	return nil, nil
}
