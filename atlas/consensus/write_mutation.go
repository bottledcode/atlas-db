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
	"sync/atomic"

	"github.com/bottledcode/atlas-db/atlas/options"
	"google.golang.org/protobuf/proto"
)

// WriteMutation writes a mutation to the consensus log using the WPaxos protocol.
// It encapsulates the complete flow:
// - Phase 1 (if not owned): Steal ownership via Q1 broadcast quorum
// - Phase 2: Write to Q2 majority quorum
// - Phase 3: Commit to Q2 majority quorum
//
// The mutation's Slot and Ballot fields are populated internally.
func WriteMutation(ctx context.Context, key []byte, mutation *RecordMutation) error {
	// Get quorum manager
	qm := GetDefaultQuorumManager(ctx)

	// Get majority quorum (contains both Q1 and Q2)
	mq, err := qm.GetQuorum(ctx, string(key))
	if err != nil {
		return fmt.Errorf("failed to get quorum: %w", err)
	}

	// Type assert to get access to q1 and q2
	majorityQuorum, ok := mq.(*majorityQuorum)
	if !ok {
		return fmt.Errorf("expected majorityQuorum, got %T", mq)
	}

	// Check ownership state
	owned := getCurrentOwnershipState(key)

	// Phase 1: Steal ownership if not owned
	if !IsOwned(key) {
		owned.mu.RLock()
		nextBallot := proto.Clone(owned.promised).(*Ballot)
		owned.mu.RUnlock()

		nextBallot.Id += 1
		nextBallot.Node = options.CurrentOptions.ServerId

		// Perform Phase-1a on Q1 (broadcast to all nodes)
		success := atomic.Int32{}
		highestSlot := uint64(0)

		err := majorityQuorum.q1.broadcast(func(node *QuorumNode) error {
			resp, err := node.StealTableOwnership(ctx, &StealTableOwnershipRequest{
				Ballot: nextBallot,
			})
			if err != nil {
				return err
			}

			if resp.Promised {
				success.Add(1)
			}

			// Track the highest slot for log coordination
			if resp.HighestSlot != nil && resp.HighestSlot.Id > highestSlot {
				highestSlot = resp.HighestSlot.Id
			}

			return nil
		})
		if err != nil {
			return fmt.Errorf("phase-1 broadcast failed: %w", err)
		}

		if int(success.Load()) != len(majorityQuorum.q1.nodes) {
			return fmt.Errorf("phase-1 failed: did not get unanimous promises from Q1")
		}

		// Mark ourselves as owner
		owned.mu.Lock()
		owned.owned = true
		owned.promised = nextBallot
		owned.maxAppliedSlot = highestSlot
		owned.mu.Unlock()
	}

	// Get updated ownership state (ballot and slot)
	owned.mu.Lock()

	// Allocate next slot
	nextSlot := owned.maxAppliedSlot + 1
	owned.maxAppliedSlot = nextSlot

	// Use current ballot
	ballot := proto.Clone(owned.promised).(*Ballot)

	owned.mu.Unlock()

	// Populate mutation with slot and ballot
	mutation.Slot = &Slot{
		Key:  key,
		Id:   nextSlot,
		Node: options.CurrentOptions.ServerId,
	}
	mutation.Ballot = ballot
	mutation.Committed = false

	// Phase 2: Write to Q2 majority quorum (NOT Q1!)
	resp, err := majorityQuorum.q2.WriteMigration(ctx, &WriteMigrationRequest{
		Record: mutation,
	})
	if err != nil {
		return fmt.Errorf("phase-2 write to Q2 failed: %w", err)
	}
	if !resp.Accepted {
		return fmt.Errorf("phase-2 write rejected by Q2")
	}

	// Phase 3: Commit to Q2 majority quorum (NOT Q1!)
	mutation.Committed = true
	_, err = majorityQuorum.q2.AcceptMigration(ctx, &WriteMigrationRequest{
		Record: mutation,
	})
	if err != nil {
		return fmt.Errorf("phase-3 commit to Q2 failed: %w", err)
	}

	// Notify followers
	owned.mu.RLock()
	for _, follower := range owned.followers {
		select {
		case follower <- mutation:
		default:
			// Don't block if follower channel is full
		}
	}
	owned.mu.RUnlock()

	return nil
}
