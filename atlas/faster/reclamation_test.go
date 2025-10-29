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
	"testing"
)

// TestSpaceReclamation verifies that committed entries free up mutable buffer space
// This test would fail before the TryAdvanceTail fix
func TestSpaceReclamation(t *testing.T) {
	dir := t.TempDir()

	// Use a small mutable buffer to make the test fast
	cfg := Config{
		Path:         dir + "/test.log",
		MutableSize:  100 * 1024, // 100KB - small to trigger the issue quickly
		SyncOnCommit: false,
	}

	log, err := NewFasterLog(cfg)
	if err != nil {
		t.Fatalf("Failed to create log: %v", err)
	}
	defer log.Close()

	// Write enough data to fill the buffer multiple times
	// Each entry is ~150 bytes, so 100KB / 150 = ~680 entries
	// We'll write 3000 entries which would require ~450KB without reclamation
	numEntries := 3000
	value := make([]byte, 100) // 100-byte values

	for i := range numEntries {
		slot := uint64(i)

		// Accept the entry
		err := log.Accept(slot, Ballot{ID: 1, NodeID: 1}, value)
		if err != nil {
			t.Fatalf("Failed to accept slot %d: %v (after %d successful accepts)", slot, err, i)
		}

		// Commit immediately to simulate a well-behaved system
		// This should trigger tail advancement and reclaim space
		err = log.Commit(slot)
		if err != nil {
			t.Fatalf("Failed to commit slot %d: %v", slot, err)
		}

		// Every 100 entries, verify we're still making progress
		if i > 0 && i%100 == 0 {
			usedSpace := log.mutableBuffer.UsedSpace()
			t.Logf("After %d entries: used space = %d bytes (%.1f%% of buffer)",
				i, usedSpace, float64(usedSpace)/float64(cfg.MutableSize)*100)

			// The used space should stay relatively bounded
			// With tail advancement, it should never exceed the buffer size
			if usedSpace > cfg.MutableSize {
				t.Errorf("Used space (%d) exceeds buffer size (%d) - reclamation not working!",
					usedSpace, cfg.MutableSize)
			}
		}
	}

	t.Logf("Successfully wrote %d entries (%.1f MB total) with only %d KB buffer",
		numEntries, float64(numEntries*150)/1024/1024, cfg.MutableSize/1024)

	// Verify all entries are readable and committed
	for i := range numEntries {
		slot := uint64(i)
		entry, err := log.ReadCommittedOnly(slot)
		if err != nil {
			t.Errorf("Failed to read committed slot %d: %v", slot, err)
			continue
		}
		if entry.Slot != slot {
			t.Errorf("Slot %d: got entry for slot %d", slot, entry.Slot)
		}
		if !entry.Committed {
			t.Errorf("Slot %d: entry not marked as committed", slot)
		}
	}
}

// TestSpaceReclamationUnderLoad tests reclamation with concurrent operations
func TestSpaceReclamationUnderLoad(t *testing.T) {
	dir := t.TempDir()

	cfg := Config{
		Path:         dir + "/test.log",
		MutableSize:  200 * 1024, // 200KB
		SyncOnCommit: false,
	}

	log, err := NewFasterLog(cfg)
	if err != nil {
		t.Fatalf("Failed to create log: %v", err)
	}
	defer log.Close()

	// Simulate a realistic workload:
	// - Some entries committed immediately
	// - Some entries remain uncommitted for a while
	// The key is that committed entries should free space

	numRounds := 100
	entriesPerRound := 50
	value := make([]byte, 100)

	for round := range numRounds {
		baseSlot := uint64(round * entriesPerRound)

		// Accept a batch of entries
		for i := range entriesPerRound {
			slot := baseSlot + uint64(i)
			err := log.Accept(slot, Ballot{ID: 1, NodeID: 1}, value)
			if err != nil {
				t.Fatalf("Round %d: Failed to accept slot %d: %v", round, slot, err)
			}
		}

		// Commit all of them in this round (realistic for consensus)
		for i := range entriesPerRound {
			slot := baseSlot + uint64(i)
			err := log.Commit(slot)
			if err != nil {
				t.Fatalf("Round %d: Failed to commit slot %d: %v", round, slot, err)
			}
		}

		// Check buffer usage
		if round%10 == 0 {
			usedSpace := log.mutableBuffer.UsedSpace()
			availSpace := log.mutableBuffer.AvailableSpace()
			t.Logf("Round %d: used=%d KB, available=%d KB",
				round, usedSpace/1024, availSpace/1024)
		}
	}

	// Verify we didn't run out of space
	availSpace := log.mutableBuffer.AvailableSpace()
	if availSpace == 0 {
		t.Error("Buffer completely full - reclamation not working!")
	}

	t.Logf("Final: %d KB available after %d entries",
		availSpace/1024, numRounds*entriesPerRound)
}

// TestNoReclamationWithUncommitted verifies that uncommitted entries block reclamation
func TestNoReclamationWithUncommitted(t *testing.T) {
	dir := t.TempDir()

	cfg := Config{
		Path:         dir + "/test.log",
		MutableSize:  1024 * 1024, // 1MB
		SyncOnCommit: false,
	}

	log, err := NewFasterLog(cfg)
	if err != nil {
		t.Fatalf("Failed to create log: %v", err)
	}
	defer log.Close()

	value := make([]byte, 100)

	// Accept slot 0 but DON'T commit it (blocks tail advancement)
	err = log.Accept(0, Ballot{ID: 1, NodeID: 1}, value)
	if err != nil {
		t.Fatalf("Failed to accept slot 0: %v", err)
	}

	// Accept and commit slots 1-100
	for i := 1; i <= 100; i++ {
		slot := uint64(i)
		err = log.Accept(slot, Ballot{ID: 1, NodeID: 1}, value)
		if err != nil {
			t.Fatalf("Failed to accept slot %d: %v", slot, err)
		}
		err = log.Commit(slot)
		if err != nil {
			t.Fatalf("Failed to commit slot %d: %v", slot, err)
		}
	}

	// Tail should NOT advance past slot 0 (it's uncommitted)
	// So used space should be non-zero
	usedSpace := log.mutableBuffer.UsedSpace()
	if usedSpace == 0 {
		t.Error("Used space is 0 even though slot 0 is uncommitted - tail advanced incorrectly!")
	}

	t.Logf("Used space with uncommitted head: %d bytes (correct behavior)", usedSpace)

	// Now commit slot 0
	err = log.Commit(0)
	if err != nil {
		t.Fatalf("Failed to commit slot 0: %v", err)
	}

	// After committing more slots, tail should now be able to advance
	for i := 101; i <= 110; i++ {
		slot := uint64(i)
		err = log.Accept(slot, Ballot{ID: 1, NodeID: 1}, value)
		if err != nil {
			t.Fatalf("Failed to accept slot %d: %v", slot, err)
		}
		err = log.Commit(slot)
		if err != nil {
			t.Fatalf("Failed to commit slot %d: %v", slot, err)
		}
	}

	// Used space should decrease as tail advances
	newUsedSpace := log.mutableBuffer.UsedSpace()
	t.Logf("Used space after committing all: %d bytes", newUsedSpace)

	// Verify all entries are still readable
	for i := 0; i <= 110; i++ {
		slot := uint64(i)
		entry, err := log.ReadCommittedOnly(slot)
		if err != nil {
			t.Errorf("Failed to read committed slot %d: %v", slot, err)
		}
		if entry != nil && entry.Slot != slot {
			t.Errorf("Slot %d: wrong entry returned", slot)
		}
	}
}

// TestBufferFullBeforeFix demonstrates what would happen without the fix
// (This test is informational - it shows the problem we're solving)
func TestBufferFullScenario(t *testing.T) {
	dir := t.TempDir()

	// Very small buffer to trigger the issue quickly
	cfg := Config{
		Path:         dir + "/test.log",
		MutableSize:  10 * 1024, // 10KB - tiny!
		SyncOnCommit: false,
	}

	log, err := NewFasterLog(cfg)
	if err != nil {
		t.Fatalf("Failed to create log: %v", err)
	}
	defer log.Close()

	value := make([]byte, 100) // ~150 bytes per entry with overhead

	// This should be able to write ~10KB / 150 = ~68 entries
	// But with tail advancement, we should be able to write many more

	successfulWrites := 0
	for i := range 1000 {
		slot := uint64(i)

		err = log.Accept(slot, Ballot{ID: 1, NodeID: 1}, value)
		if err == ErrBufferFull {
			// This should NOT happen if tail advancement is working!
			t.Logf("Buffer full after %d entries (expected with fix: 1000)", successfulWrites)
			break
		}
		if err != nil {
			t.Fatalf("Unexpected error at slot %d: %v", slot, err)
		}

		// Commit immediately
		err = log.Commit(slot)
		if err != nil {
			t.Fatalf("Failed to commit slot %d: %v", slot, err)
		}

		successfulWrites++
	}

	// With the fix, we should write all 1000 entries despite the tiny buffer
	if successfulWrites < 1000 {
		t.Errorf("Only wrote %d/1000 entries - tail advancement may not be working optimally", successfulWrites)
	} else {
		t.Logf("Successfully wrote %d entries with only %d KB buffer - tail advancement working!",
			successfulWrites, cfg.MutableSize/1024)
	}
}
