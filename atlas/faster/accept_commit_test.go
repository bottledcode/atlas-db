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

package faster

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// TestAcceptCommitGuarantee verifies that once Accept() succeeds, Commit() MUST succeed.
// This is a fundamental requirement for consensus: we no longer have the value after Accept,
// so the entry must remain available until Commit.
func TestAcceptCommitGuarantee(t *testing.T) {
	dir := t.TempDir()

	// Use a larger buffer so we don't block
	cfg := Config{
		Path:         dir + "/test.log",
		MutableSize:  256 * 1024, // 256KB
		SyncOnCommit: false,
	}

	log, err := NewFasterLog(cfg)
	if err != nil {
		t.Fatalf("Failed to create log: %v", err)
	}
	defer log.Close()

	ballot := Ballot{ID: 1, NodeID: 1}
	value := []byte("important-value")

	// Accept and commit entries 1-100
	for i := uint64(1); i <= 100; i++ {
		err = log.Accept(i, ballot, value)
		if err != nil {
			t.Fatalf("Accept(slot=%d) failed: %v", i, err)
		}
		err = log.Commit(i)
		if err != nil {
			t.Fatalf("Commit(slot=%d) failed: %v", i, err)
		}
	}

	// Accept entry 101 but DON'T commit it yet
	err = log.Accept(101, ballot, value)
	if err != nil {
		t.Fatalf("Accept(slot=101) failed: %v", err)
	}
	t.Log("Accept(slot=101) succeeded, not committing yet")

	// Accept and commit many more entries to trigger tail advancement and potential reset
	for i := uint64(200); i <= 500; i++ {
		err = log.Accept(i, ballot, value)
		if err != nil {
			t.Logf("Accept(slot=%d) failed (expected if buffer full): %v", i, err)
			break
		}
		err = log.Commit(i)
		if err != nil {
			t.Logf("Commit(slot=%d) failed: %v", i, err)
		}
	}

	// NOW try to commit slot 101
	// This MUST succeed because Accept succeeded
	err = log.Commit(101)
	if err != nil {
		t.Fatalf("GUARANTEE VIOLATED: Accept(slot=101) succeeded but Commit(slot=101) failed: %v", err)
	}
	t.Log("Commit(slot=101) succeeded - guarantee upheld!")

	// Verify we can read it
	entry, err := log.ReadCommittedOnly(101)
	if err != nil {
		t.Fatalf("ReadCommittedOnly(slot=101) failed: %v", err)
	}
	if string(entry.Value) != string(value) {
		t.Fatalf("Value mismatch: got %q, want %q", entry.Value, value)
	}
}

// TestAcceptCommitGuaranteeWithReset tests the specific scenario where
// resetIfDrained is called while an uncommitted entry exists.
func TestAcceptCommitGuaranteeWithReset(t *testing.T) {
	// Test directly on the ring buffer to isolate the issue
	rb := NewRingBuffer(10 * 1024) // 10KB

	// Step 1: Append an entry
	entry1 := &LogEntry{
		Slot:      1,
		Ballot:    Ballot{ID: 1, NodeID: 1},
		Value:     []byte("must-not-lose-this"),
		Committed: false,
	}

	offset1, err := rb.Append(entry1)
	if err != nil {
		t.Fatalf("Append(entry1) failed: %v", err)
	}
	t.Logf("Step 1: Appended entry1 at offset %d", offset1)

	// Step 2: Verify we can read it
	readEntry, err := rb.Read(offset1)
	if err != nil {
		t.Fatalf("Read(offset1) failed before reset: %v", err)
	}
	if string(readEntry.Value) != string(entry1.Value) {
		t.Fatalf("Value mismatch before reset")
	}
	t.Log("Step 2: Read(offset1) succeeded before any reset attempt")

	// Step 3: Try to trigger a reset
	// First, mark the entry as "committed" and advance tail past it
	// This simulates what happens when Commit() runs for OTHER entries

	// Fill buffer with more entries
	for i := uint64(100); i < 200; i++ {
		entry := &LogEntry{
			Slot:      i,
			Ballot:    Ballot{ID: 1, NodeID: 1},
			Value:     []byte("filler"),
			Committed: true, // Already committed
		}
		_, err := rb.Append(entry)
		if err != nil {
			break // Buffer full
		}
	}

	// Advance tail past all entries (simulating they're all committed)
	rb.TryAdvanceTail(func(slot uint64) bool {
		// Return false for all slots = "all moved to tail"
		// This is incorrect for slot 1, but simulates what happens
		// when indexCheck has a bug or race
		return false
	})

	t.Logf("Step 3: Advanced tail, buffer state: reserved=%d, published=%d, tail=%d",
		rb.reserved.Load(), rb.published.Load(), rb.tail.Load())

	// Step 4: Now try to read entry1 again
	// If buffer reset, this will fail or return garbage
	readEntry2, err := rb.Read(offset1)
	if err != nil {
		t.Fatalf("GUARANTEE VIOLATED: Read(offset1) failed after tail advance: %v", err)
	}
	if string(readEntry2.Value) != string(entry1.Value) {
		t.Fatalf("GUARANTEE VIOLATED: Value corrupted after tail advance: got %q, want %q",
			readEntry2.Value, entry1.Value)
	}
	t.Log("Step 4: Read(offset1) still returns correct value - guarantee upheld!")
}

// TestResetOnlyWhenTrulyEmpty verifies that reset only happens when
// there are absolutely no uncommitted entries.
func TestResetOnlyWhenTrulyEmpty(t *testing.T) {
	rb := NewRingBuffer(10 * 1024)

	// Append an uncommitted entry
	entry := &LogEntry{
		Slot:      1,
		Ballot:    Ballot{ID: 1, NodeID: 1},
		Value:     []byte("uncommitted"),
		Committed: false,
	}

	offset, err := rb.Append(entry)
	if err != nil {
		t.Fatalf("Append failed: %v", err)
	}

	initialReserved := rb.reserved.Load()
	initialPublished := rb.published.Load()

	t.Logf("After append: offset=%d, reserved=%d, published=%d, tail=%d",
		offset, initialReserved, initialPublished, rb.tail.Load())

	// Try to reset - should NOT succeed because there's an uncommitted entry
	resetHappened := rb.resetIfDrained()

	if resetHappened {
		t.Fatal("GUARANTEE VIOLATED: resetIfDrained() succeeded with uncommitted entry in buffer")
	}

	// Verify buffer state unchanged
	if rb.reserved.Load() != initialReserved {
		t.Fatalf("reserved changed: was %d, now %d", initialReserved, rb.reserved.Load())
	}
	if rb.published.Load() != initialPublished {
		t.Fatalf("published changed: was %d, now %d", initialPublished, rb.published.Load())
	}

	t.Log("resetIfDrained() correctly refused to reset with uncommitted entry")
}

// TestResetPublishRaceCondition tests the specific race condition from the code review:
//
// The race scenario:
// 1. Buffer state: reserved=X, published=X, tail=X (fully drained)
// 2. Thread A (resetter): reserved.CompareAndSwap(X, 0) succeeds → reserved=0
// 3. Thread B (writer): enters Append, sees reserved=0, CAS claims space → reserved=dataLen
// 4. Thread B: copies data, tries published.CompareAndSwap(0, dataLen)
//    BUT published is still X! CAS fails, Thread B spins waiting.
// 5. Thread A: published.Store(0) → published=0
// 6. Thread B: CAS(0, dataLen) succeeds → published=dataLen
//
// This test verifies Thread B doesn't deadlock permanently - it should recover
// once Thread A completes the reset sequence.
func TestResetPublishRaceCondition(t *testing.T) {
	rb := NewRingBuffer(64 * 1024) // 64KB

	// Phase 1: Fill and drain the buffer to get to a "reset-able" state
	value := make([]byte, 100)
	entriesWritten := 0
	for i := 0; i < 100; i++ {
		entry := &LogEntry{
			Slot:      uint64(i),
			Ballot:    Ballot{ID: 1, NodeID: 1},
			Value:     value,
			Committed: true,
		}
		_, err := rb.Append(entry)
		if err != nil {
			break
		}
		entriesWritten++
	}
	t.Logf("Phase 1: Wrote %d entries, reserved=%d, published=%d",
		entriesWritten, rb.reserved.Load(), rb.published.Load())

	// Advance tail to drain all entries (simulating they were all flushed)
	rb.TryAdvanceTail(func(slot uint64) bool {
		return false // All entries "moved to immutable tail"
	})

	t.Logf("After drain: reserved=%d, published=%d, tail=%d",
		rb.reserved.Load(), rb.published.Load(), rb.tail.Load())

	// Phase 2: Now create high contention between resetIfDrained and Append
	// The race window is very small, so we need many iterations to hit it

	stopFlag := atomic.Bool{}
	successfulWrites := atomic.Uint64{}
	successfulResets := atomic.Uint64{}

	var workersWg sync.WaitGroup
	var drainerWg sync.WaitGroup

	// Background drainer - prevents buffer exhaustion
	drainerWg.Add(1)
	go func() {
		defer drainerWg.Done()
		for !stopFlag.Load() {
			rb.TryAdvanceTail(func(slot uint64) bool {
				return false // All entries considered flushed
			})
			time.Sleep(1 * time.Millisecond)
		}
	}()

	// Goroutines that aggressively call resetIfDrained
	for r := 0; r < 2; r++ {
		workersWg.Add(1)
		go func() {
			defer workersWg.Done()
			for i := 0; i < 10000 && !stopFlag.Load(); i++ {
				if rb.resetIfDrained() {
					successfulResets.Add(1)
				}
			}
		}()
	}

	// Goroutines that aggressively write
	for w := 0; w < 4; w++ {
		workersWg.Add(1)
		go func(workerID int) {
			defer workersWg.Done()
			baseSlot := uint64(1000 + workerID*10000)
			for i := 0; i < 5000 && !stopFlag.Load(); i++ {
				entry := &LogEntry{
					Slot:      baseSlot + uint64(i),
					Ballot:    Ballot{ID: 1, NodeID: 1},
					Value:     value,
					Committed: true, // Mark committed so they can be drained
				}
				_, err := rb.Append(entry)
				if err == nil {
					successfulWrites.Add(1)
				}
			}
		}(w)
	}

	// Wait for workers with timeout
	workersDone := make(chan bool, 1)
	go func() {
		workersWg.Wait()
		workersDone <- true
	}()

	// Monitor buffer state while waiting
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()

	timeout := time.After(10 * time.Second)
	for {
		select {
		case <-workersDone:
			// Workers completed, stop the drainer
			stopFlag.Store(true)
			drainerWg.Wait()
			t.Logf("Completed: %d writes, %d resets",
				successfulWrites.Load(), successfulResets.Load())
			return
		case <-ticker.C:
			t.Logf("Status: writes=%d, resets=%d, reserved=%d, published=%d, tail=%d",
				successfulWrites.Load(), successfulResets.Load(),
				rb.reserved.Load(), rb.published.Load(), rb.tail.Load())
		case <-timeout:
			stopFlag.Store(true)
			t.Fatalf("DEADLOCK DETECTED: Test timed out - buffer state: reserved=%d, published=%d, tail=%d",
				rb.reserved.Load(), rb.published.Load(), rb.tail.Load())
		}
	}
}

// TestResetRaceWithManualTiming attempts to hit the exact race window using
// careful coordination. This is a more targeted version of the above test.
func TestResetRaceWithManualTiming(t *testing.T) {
	// We'll run many iterations since the race window is small
	for iteration := 0; iteration < 100; iteration++ {
		rb := NewRingBuffer(10 * 1024)

		// Fill buffer to ~75% to trigger reset conditions
		value := make([]byte, 50)
		for i := 0; i < 100; i++ {
			entry := &LogEntry{
				Slot:      uint64(i),
				Ballot:    Ballot{ID: 1, NodeID: 1},
				Value:     value,
				Committed: true,
			}
			if _, err := rb.Append(entry); err != nil {
				break // Buffer full
			}
		}

		// Drain everything
		rb.TryAdvanceTail(func(slot uint64) bool { return false })

		// Now we should have reserved=published=tail=X where X > 75% of size
		// This is the condition where resetIfDrained will attempt a full reset

		// Launch concurrent operations
		done := make(chan bool)
		var writeErr atomic.Value

		go func() {
			// Writer: try to append
			entry := &LogEntry{
				Slot:      9999,
				Ballot:    Ballot{ID: 1, NodeID: 1},
				Value:     value,
				Committed: false,
			}
			_, err := rb.Append(entry)
			if err != nil {
				writeErr.Store(err)
			}
			done <- true
		}()

		go func() {
			// Resetter: try to reset
			rb.resetIfDrained()
			done <- true
		}()

		// Wait for both with timeout
		for i := 0; i < 2; i++ {
			select {
			case <-done:
				// One operation completed
			case <-time.After(1 * time.Second):
				t.Fatalf("Iteration %d: DEADLOCK - goroutine stuck (reserved=%d, published=%d, tail=%d)",
					iteration, rb.reserved.Load(), rb.published.Load(), rb.tail.Load())
			}
		}
	}
	t.Log("Completed 100 iterations without deadlock")
}

// TestResetPublishRaceExact targets the exact race the reviewer identified:
// Writer claims space (reserved CAS succeeds) but spins forever on published CAS
// because resetIfDrained zeroed reserved but hasn't zeroed published yet.
//
// This test uses direct manipulation of atomics to force the exact interleaving.
func TestResetPublishRaceExact(t *testing.T) {
	rb := NewRingBuffer(10 * 1024)

	// Step 1: Set up a "drained" buffer state manually
	// In production this happens after all entries are flushed
	rb.reserved.Store(8000)
	rb.published.Store(8000)
	rb.tail.Store(8000)

	t.Logf("Initial state: reserved=%d, published=%d, tail=%d",
		rb.reserved.Load(), rb.published.Load(), rb.tail.Load())

	// Step 2: Simulate the race condition manually
	// Thread A (resetter): CAS reserved from 8000 to 0
	if !rb.reserved.CompareAndSwap(8000, 0) {
		t.Fatal("Setup failed: couldn't CAS reserved")
	}
	t.Log("Simulated resetter: reserved.CAS(8000, 0) succeeded")

	// Now: reserved=0, published=8000, tail=8000
	// This is the dangerous state! A writer entering now will:
	// 1. Read reserved=0
	// 2. CAS reserved from 0 to dataLen (succeeds)
	// 3. Try to CAS published from 0 to dataLen (FAILS because published=8000)
	// 4. Spin forever waiting for published to become 0

	// Step 3: Start a writer in this dangerous state
	writerDone := make(chan bool)
	var writerErr error

	go func() {
		entry := &LogEntry{
			Slot:      1,
			Ballot:    Ballot{ID: 1, NodeID: 1},
			Value:     []byte("test-value"),
			Committed: false,
		}
		_, writerErr = rb.Append(entry)
		writerDone <- true
	}()

	// Step 4: Wait briefly for writer to get stuck in spin loop
	time.Sleep(10 * time.Millisecond)

	// Check state - writer should have claimed space but not published yet
	t.Logf("After writer starts: reserved=%d, published=%d, tail=%d",
		rb.reserved.Load(), rb.published.Load(), rb.tail.Load())

	// Step 5: Complete the reset (what the resetter would do)
	// This should unblock the spinning writer
	rb.published.Store(0)
	rb.tail.Store(0)
	t.Log("Simulated resetter completing: published.Store(0), tail.Store(0)")

	// Step 6: Writer should now complete
	select {
	case <-writerDone:
		if writerErr != nil {
			t.Fatalf("Writer failed: %v", writerErr)
		}
		t.Logf("Writer completed successfully. Final state: reserved=%d, published=%d, tail=%d",
			rb.reserved.Load(), rb.published.Load(), rb.tail.Load())
	case <-time.After(1 * time.Second):
		t.Fatalf("DEADLOCK: Writer stuck even after reset completed! State: reserved=%d, published=%d, tail=%d",
			rb.reserved.Load(), rb.published.Load(), rb.tail.Load())
	}
}

// TestResetPublishRaceWithoutRecovery tests what happens if the resetter
// DOESN'T complete the published.Store(0). This should cause a permanent deadlock.
// This test intentionally creates a deadlock to verify our understanding.
func TestResetPublishRaceWithoutRecovery(t *testing.T) {
	rb := NewRingBuffer(10 * 1024)

	// Set up drained state
	rb.reserved.Store(8000)
	rb.published.Store(8000)
	rb.tail.Store(8000)

	// Resetter: only do the first CAS, don't complete the reset
	rb.reserved.CompareAndSwap(8000, 0)
	// NOTE: We intentionally DON'T do published.Store(0)

	// Now: reserved=0, published=8000, tail=8000

	// Writer should get stuck
	writerDone := make(chan bool)
	go func() {
		entry := &LogEntry{
			Slot:      1,
			Ballot:    Ballot{ID: 1, NodeID: 1},
			Value:     []byte("test-value"),
			Committed: false,
		}
		rb.Append(entry) // This should spin forever
		writerDone <- true
	}()

	// Wait a bit and verify writer is stuck
	select {
	case <-writerDone:
		// If we get here, the writer completed despite the inconsistent state.
		// This is unexpected and indicates a bug or change in behavior that
		// needs investigation - the writer should be spinning on published CAS.
		t.Fatalf("UNEXPECTED: Writer completed despite incomplete reset! "+
			"State: reserved=%d, published=%d, tail=%d. "+
			"Expected writer to spin waiting for published to become 0.",
			rb.reserved.Load(), rb.published.Load(), rb.tail.Load())
	case <-time.After(100 * time.Millisecond):
		// Expected: writer is stuck in spin loop
		t.Logf("Writer stuck as expected (reserved=%d, published=%d, tail=%d)",
			rb.reserved.Load(), rb.published.Load(), rb.tail.Load())

		// Now let's unblock it by completing the reset
		rb.published.Store(0)
		rb.tail.Store(0)

		select {
		case <-writerDone:
			t.Log("Writer unblocked after completing reset")
		case <-time.After(1 * time.Second):
			t.Fatal("Writer still stuck even after reset completed")
		}
	}
}
