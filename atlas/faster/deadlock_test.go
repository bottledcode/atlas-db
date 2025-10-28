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

// TestNoDeadlockDuringReset verifies that reset doesn't deadlock in-flight writers
// This test would hang forever before the fix
func TestNoDeadlockDuringReset(t *testing.T) {
	dir := t.TempDir()

	cfg := Config{
		Path:         dir + "/test.log",
		MutableSize:  50 * 1024, // Small buffer to trigger resets
		SyncOnCommit: false,
	}

	log, err := NewFasterLog(cfg)
	if err != nil {
		t.Fatalf("Failed to create log: %v", err)
	}
	defer log.Close()

	// Use a timeout to detect deadlocks
	done := make(chan bool, 1)
	deadlocked := atomic.Bool{}

	go func() {
		defer func() {
			done <- true
		}()

		value := make([]byte, 100)
		numWrites := 2000

		for i := 0; i < numWrites; i++ {
			slot := uint64(i)

			// Accept - this might trigger a reset during the reserve-publish window
			err := log.Accept(slot, Ballot{ID: 1, NodeID: 1}, value)
			if err != nil {
				t.Logf("Failed to accept slot %d: %v", slot, err)
				// Don't fail immediately - might just be buffer full
				time.Sleep(10 * time.Millisecond)
				i-- // Retry
				continue
			}

			// Commit immediately to trigger aggressive resets
			err = log.Commit(slot)
			if err != nil {
				t.Errorf("Failed to commit slot %d: %v", slot, err)
				return
			}

			// Periodically check if we're deadlocked
			if i%100 == 0 && deadlocked.Load() {
				t.Errorf("Deadlock detected at iteration %d", i)
				return
			}
		}
	}()

	// Wait for completion with timeout
	select {
	case <-done:
		// Success!
		t.Log("Completed without deadlock")
	case <-time.After(5 * time.Second):
		deadlocked.Store(true)
		t.Fatal("Deadlock detected - test timed out after 5 seconds")
	}
}

// TestConcurrentAcceptDuringReset tests the specific race condition:
// - Writer reserves space (reserved > published)
// - Reset happens
// - Writer tries to publish (should not deadlock)
func TestConcurrentAcceptDuringReset(t *testing.T) {
	dir := t.TempDir()

	cfg := Config{
		Path:         dir + "/test.log",
		MutableSize:  500 * 1024, // Larger buffer to avoid filling up
		SyncOnCommit: false,
	}

	log, err := NewFasterLog(cfg)
	if err != nil {
		t.Fatalf("Failed to create log: %v", err)
	}
	defer log.Close()

	var wg sync.WaitGroup
	errors := make(chan error, 1000)
	value := make([]byte, 100)

	// Writer goroutines
	numWriters := 10
	writesPerWriter := 200

	for w := 0; w < numWriters; w++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()

			for i := 0; i < writesPerWriter; i++ {
				slot := uint64(workerID*writesPerWriter + i)

				// Accept and commit
				err := log.Accept(slot, Ballot{ID: 1, NodeID: 1}, value)
				if err != nil {
					errors <- err
					return
				}

				err = log.Commit(slot)
				if err != nil {
					errors <- err
					return
				}
			}
		}(w)
	}

	// Wait with timeout
	done := make(chan bool, 1)
	go func() {
		wg.Wait()
		done <- true
	}()

	select {
	case <-done:
		close(errors)
		// Check for errors
		for err := range errors {
			t.Errorf("Writer error: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("Deadlock detected - concurrent writes timed out")
	}
}

// TestResetOnlyWhenNoInflightWrites verifies the guard condition
func TestResetOnlyWhenNoInflightWrites(t *testing.T) {
	// This is a lower-level test of the ring buffer itself
	rb := NewRingBuffer(10 * 1024)

	value := make([]byte, 100)

	// Accept many entries and commit them
	for i := 0; i < 50; i++ {
		entry := &LogEntry{
			Slot:      uint64(i),
			Ballot:    Ballot{ID: 1, NodeID: 1},
			Value:     value,
			Committed: false,
		}

		_, err := rb.Append(entry)
		if err != nil {
			t.Fatalf("Failed to append entry %d: %v", i, err)
		}
	}

	// Simulate a scenario where reserved > published
	// (In practice this happens during the copy phase of Append)

	// Get current state
	published := rb.published.Load()
	reserved := rb.reserved.Load()

	t.Logf("State before reset attempt: reserved=%d, published=%d", reserved, published)

	if reserved == published {
		t.Skip("Test setup failed - reserved == published, can't test the guard")
	}

	// Try to advance tail (which might attempt a reset)
	// With the fix, this should NOT reset because reserved > published
	indexCheck := func(slot uint64) bool {
		// Simulate all entries moved to tail
		return false
	}

	reclaimed := rb.TryAdvanceTail(indexCheck)

	// Check that we didn't do a full reset
	newReserved := rb.reserved.Load()
	newPublished := rb.published.Load()

	t.Logf("State after TryAdvanceTail: reserved=%d, published=%d, reclaimed=%d",
		newReserved, newPublished, reclaimed)

	// The key assertion: if there were in-flight writes (reserved > published),
	// we should NOT have reset to 0
	if reserved > published && newReserved == 0 {
		t.Error("BUG: Reset happened even though reserved > published (in-flight writes exist)")
	}
}

// TestResetWhenSafeToReset verifies that reset DOES happen when safe
func TestResetWhenSafeToReset(t *testing.T) {
	rb := NewRingBuffer(10 * 1024)

	value := make([]byte, 100)

	// Fill up most of the buffer
	for i := 0; i < 60; i++ {
		entry := &LogEntry{
			Slot:      uint64(i),
			Ballot:    Ballot{ID: 1, NodeID: 1},
			Value:     value,
			Committed: false,
		}

		_, err := rb.Append(entry)
		if err != nil {
			// Buffer might be full, that's okay
			break
		}
	}

	// Get current state
	published := rb.published.Load()
	reserved := rb.reserved.Load()

	t.Logf("State: reserved=%d, published=%d, size=%d", reserved, published, rb.size)

	if reserved != published {
		t.Skip("Test setup: reserved != published, waiting not implemented in test")
	}

	// Simulate all entries moved to tail
	indexCheck := func(slot uint64) bool {
		return false // All entries flushed
	}

	// This should trigger a reset because:
	// 1. All entries are flushed (tail >= published)
	// 2. No writers in-flight (reserved == published)
	// 3. Buffer is >75% full
	reclaimed := rb.TryAdvanceTail(indexCheck)

	newReserved := rb.reserved.Load()
	newPublished := rb.published.Load()
	newTail := rb.tail.Load()

	t.Logf("After TryAdvanceTail: reserved=%d, published=%d, tail=%d, reclaimed=%d",
		newReserved, newPublished, newTail, reclaimed)

	// If conditions were met, we should have reset
	if reserved > rb.size*3/4 && reserved == published {
		if newReserved != 0 || newPublished != 0 || newTail != 0 {
			t.Error("Reset should have happened but didn't")
		}
	}
}
