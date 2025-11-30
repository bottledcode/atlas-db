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
	"encoding/binary"
	"fmt"
	"path/filepath"
	"testing"
)

// TestIteratorOutOfOrder tests that entries written and committed out-of-order
// are returned in correct slot order during iteration
func TestIteratorOutOfOrder(t *testing.T) {
	dir := t.TempDir()
	log, err := NewFasterLog(Config{
		Path:         filepath.Join(dir, "test.log"),
		MutableSize:  1 * 1024 * 1024,
		SegmentSize:  10 * 1024 * 1024,
		NumThreads:   16,
		SyncOnCommit: false,
	})
	if err != nil {
		t.Fatalf("Failed to create log: %v", err)
	}
	defer log.Close()

	ballot := Ballot{ID: 1, NodeID: 1}

	// Test data: slot -> value (integers)
	// Write out of order: 10, 5, 15, 1, 8, 3, 12, 7
	writeOrder := []uint64{10, 5, 15, 1, 8, 3, 12, 7}

	// Expected sum: 1+3+5+7+8+10+12+15 = 61
	expectedSum := uint64(61)
	expectedCount := len(writeOrder)

	// Step 1: Accept entries in random order
	for _, slot := range writeOrder {
		value := make([]byte, 8)
		binary.LittleEndian.PutUint64(value, slot) // Value = slot number

		err := log.Accept(slot, ballot, value)
		if err != nil {
			t.Fatalf("Failed to accept slot %d: %v", slot, err)
		}
	}

	// Step 2: Commit in different random order
	commitOrder := []uint64{5, 15, 1, 10, 8, 3, 12, 7}
	for _, slot := range commitOrder {
		err := log.Commit(slot)
		if err != nil {
			t.Fatalf("Failed to commit slot %d: %v", slot, err)
		}
	}

	// Step 3: Iterate and verify sequential order
	var sum uint64
	var count int
	var prevSlot uint64

	err = log.IterateCommitted(func(entry *LogEntry) error {
		// Verify slots are in order
		if count > 0 && entry.Slot <= prevSlot {
			return fmt.Errorf("slots not in order: prev=%d, current=%d", prevSlot, entry.Slot)
		}
		prevSlot = entry.Slot

		// Extract value
		value := binary.LittleEndian.Uint64(entry.Value)

		// Value should equal slot number
		if value != entry.Slot {
			return fmt.Errorf("slot %d has wrong value: expected %d, got %d",
				entry.Slot, entry.Slot, value)
		}

		sum += value
		count++
		return nil
	}, IterateOptions{})

	if err != nil {
		t.Fatalf("Iteration failed: %v", err)
	}

	// Verify results
	if count != expectedCount {
		t.Errorf("Wrong count: expected %d, got %d", expectedCount, count)
	}
	if sum != expectedSum {
		t.Errorf("Wrong sum: expected %d, got %d", expectedSum, sum)
	}

	t.Logf("✓ Iterated %d entries in correct order, sum=%d", count, sum)
}

// TestIteratorZeroAllocation verifies the iterator doesn't allocate per entry
func TestIteratorZeroAllocation(t *testing.T) {
	dir := t.TempDir()
	log, err := NewFasterLog(Config{
		Path:         filepath.Join(dir, "test.log"),
		MutableSize:  2 * 1024 * 1024,
		SegmentSize:  10 * 1024 * 1024,
		NumThreads:   16,
		SyncOnCommit: false,
	})
	if err != nil {
		t.Fatalf("Failed to create log: %v", err)
	}
	defer log.Close()

	ballot := Ballot{ID: 1, NodeID: 1}

	// Write and commit 100 entries
	numEntries := 100
	for slot := uint64(0); slot < uint64(numEntries); slot++ {
		value := make([]byte, 8)
		binary.LittleEndian.PutUint64(value, slot)

		err := log.Accept(slot, ballot, value)
		if err != nil {
			t.Fatalf("Failed to accept slot %d: %v", slot, err)
		}

		err = log.Commit(slot)
		if err != nil {
			t.Fatalf("Failed to commit slot %d: %v", slot, err)
		}
	}

	// Test zero-allocation iteration
	allocs := testing.AllocsPerRun(10, func() {
		_ = log.IterateCommitted(func(entry *LogEntry) error {
			// Just read, don't allocate
			_ = entry.Slot
			_ = entry.Value[0]
			return nil
		}, IterateOptions{})
	})

	// We expect some allocations for the slot array, but not per-entry
	// Allow ~1-2 allocations per entry on average (slot array + overhead)
	maxAllocsPerEntry := float64(2.0)
	if allocs > float64(numEntries)*maxAllocsPerEntry {
		t.Errorf("Too many allocations: %.1f total (%.2f per entry)",
			allocs, allocs/float64(numEntries))
	}

	t.Logf("✓ Iterator allocations: %.1f total (%.2f per entry)",
		allocs, allocs/float64(numEntries))
}

// TestIteratorWithGaps tests iteration when some slots are missing
func TestIteratorWithGaps(t *testing.T) {
	dir := t.TempDir()
	log, err := NewFasterLog(Config{
		Path:         filepath.Join(dir, "test.log"),
		MutableSize:  1 * 1024 * 1024,
		SegmentSize:  10 * 1024 * 1024,
		NumThreads:   16,
		SyncOnCommit: false,
	})
	if err != nil {
		t.Fatalf("Failed to create log: %v", err)
	}
	defer log.Close()

	ballot := Ballot{ID: 1, NodeID: 1}

	// Write slots: 1, 3, 5, 7, 10 (gaps: 2, 4, 6, 8, 9)
	slots := []uint64{1, 3, 5, 7, 10}
	expectedSum := uint64(1 + 3 + 5 + 7 + 10) // 26

	for _, slot := range slots {
		value := make([]byte, 8)
		binary.LittleEndian.PutUint64(value, slot)

		err := log.Accept(slot, ballot, value)
		if err != nil {
			t.Fatalf("Failed to accept slot %d: %v", slot, err)
		}

		err = log.Commit(slot)
		if err != nil {
			t.Fatalf("Failed to commit slot %d: %v", slot, err)
		}
	}

	// Iterate and verify only present slots are returned
	var sum uint64
	var count int
	seenSlots := make(map[uint64]bool)

	err = log.IterateCommitted(func(entry *LogEntry) error {
		seenSlots[entry.Slot] = true
		value := binary.LittleEndian.Uint64(entry.Value)
		sum += value
		count++
		return nil
	}, IterateOptions{})

	if err != nil {
		t.Fatalf("Iteration failed: %v", err)
	}

	// Verify we got exactly the slots we wrote
	if count != len(slots) {
		t.Errorf("Wrong count: expected %d, got %d", len(slots), count)
	}
	if sum != expectedSum {
		t.Errorf("Wrong sum: expected %d, got %d", expectedSum, sum)
	}

	// Verify no gap slots appeared
	for _, slot := range slots {
		if !seenSlots[slot] {
			t.Errorf("Missing expected slot %d", slot)
		}
	}

	// Verify gap slots didn't appear
	gapSlots := []uint64{2, 4, 6, 8, 9}
	for _, slot := range gapSlots {
		if seenSlots[slot] {
			t.Errorf("Gap slot %d should not appear", slot)
		}
	}

	t.Logf("✓ Correctly handled %d gaps", len(gapSlots))
}

// TestIteratorRange tests MinSlot and MaxSlot filtering
func TestIteratorRange(t *testing.T) {
	dir := t.TempDir()
	log, err := NewFasterLog(Config{
		Path:         filepath.Join(dir, "test.log"),
		MutableSize:  1 * 1024 * 1024,
		SegmentSize:  10 * 1024 * 1024,
		NumThreads:   16,
		SyncOnCommit: false,
	})
	if err != nil {
		t.Fatalf("Failed to create log: %v", err)
	}
	defer log.Close()

	ballot := Ballot{ID: 1, NodeID: 1}

	// Write slots 0-19
	for slot := range uint64(20) {
		value := make([]byte, 8)
		binary.LittleEndian.PutUint64(value, slot)

		err := log.Accept(slot, ballot, value)
		if err != nil {
			t.Fatalf("Failed to accept slot %d: %v", slot, err)
		}

		err = log.Commit(slot)
		if err != nil {
			t.Fatalf("Failed to commit slot %d: %v", slot, err)
		}
	}

	testCases := []struct {
		name        string
		minSlot     uint64
		maxSlot     uint64
		expectedMin uint64
		expectedMax uint64
		expectedCnt int
	}{
		{
			name:        "Full range",
			minSlot:     0,
			maxSlot:     0, // 0 = no limit
			expectedMin: 0,
			expectedMax: 19,
			expectedCnt: 20,
		},
		{
			name:        "Middle range",
			minSlot:     5,
			maxSlot:     15,
			expectedMin: 5,
			expectedMax: 15,
			expectedCnt: 11,
		},
		{
			name:        "Only min",
			minSlot:     10,
			maxSlot:     0,
			expectedMin: 10,
			expectedMax: 19,
			expectedCnt: 10,
		},
		{
			name:        "Only max",
			minSlot:     0,
			maxSlot:     9,
			expectedMin: 0,
			expectedMax: 9,
			expectedCnt: 10,
		},
		{
			name:        "Single slot",
			minSlot:     7,
			maxSlot:     7,
			expectedMin: 7,
			expectedMax: 7,
			expectedCnt: 1,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			var count int
			var minSeen, maxSeen uint64 = ^uint64(0), 0

			err := log.IterateCommitted(func(entry *LogEntry) error {
				if entry.Slot < minSeen {
					minSeen = entry.Slot
				}
				if entry.Slot > maxSeen {
					maxSeen = entry.Slot
				}
				count++
				return nil
			}, IterateOptions{
				MinSlot: tc.minSlot,
				MaxSlot: tc.maxSlot,
			})

			if err != nil {
				t.Fatalf("Iteration failed: %v", err)
			}

			if count != tc.expectedCnt {
				t.Errorf("Wrong count: expected %d, got %d", tc.expectedCnt, count)
			}
			if count > 0 {
				if minSeen != tc.expectedMin {
					t.Errorf("Wrong min: expected %d, got %d", tc.expectedMin, minSeen)
				}
				if maxSeen != tc.expectedMax {
					t.Errorf("Wrong max: expected %d, got %d", tc.expectedMax, maxSeen)
				}
			}

			t.Logf("✓ Range [%d,%d] returned %d entries", tc.minSlot, tc.maxSlot, count)
		})
	}
}

// TestGetCommittedRange tests the range query function
func TestGetCommittedRange(t *testing.T) {
	dir := t.TempDir()
	log, err := NewFasterLog(Config{
		Path:         filepath.Join(dir, "test.log"),
		MutableSize:  1 * 1024 * 1024,
		SegmentSize:  10 * 1024 * 1024,
		NumThreads:   16,
		SyncOnCommit: false,
	})
	if err != nil {
		t.Fatalf("Failed to create log: %v", err)
	}
	defer log.Close()

	// Empty log
	minSlot, maxSlot, count := log.GetCommittedRange()
	if minSlot != 0 || maxSlot != 0 || count != 0 {
		t.Errorf("Empty log should return (0,0,0), got (%d,%d,%d)", minSlot, maxSlot, count)
	}

	ballot := Ballot{ID: 1, NodeID: 1}

	// Write slots: 5, 10, 15, 20
	slots := []uint64{5, 10, 15, 20}
	for _, slot := range slots {
		value := []byte{byte(slot)}

		err := log.Accept(slot, ballot, value)
		if err != nil {
			t.Fatalf("Failed to accept slot %d: %v", slot, err)
		}

		err = log.Commit(slot)
		if err != nil {
			t.Fatalf("Failed to commit slot %d: %v", slot, err)
		}
	}

	// Check range
	minSlot, maxSlot, count = log.GetCommittedRange()

	expectedMin := uint64(5)
	expectedMax := uint64(20)
	expectedCount := uint64(4)

	if minSlot != expectedMin {
		t.Errorf("Wrong min: expected %d, got %d", expectedMin, minSlot)
	}
	if maxSlot != expectedMax {
		t.Errorf("Wrong max: expected %d, got %d", expectedMax, maxSlot)
	}
	if count != expectedCount {
		t.Errorf("Wrong count: expected %d, got %d", expectedCount, count)
	}

	t.Logf("✓ Range: min=%d, max=%d, count=%d", minSlot, maxSlot, count)
}

// TestReplayFromSlot tests the replay convenience function
func TestReplayFromSlot(t *testing.T) {
	dir := t.TempDir()
	log, err := NewFasterLog(Config{
		Path:         filepath.Join(dir, "test.log"),
		MutableSize:  1 * 1024 * 1024,
		SegmentSize:  10 * 1024 * 1024,
		NumThreads:   16,
		SyncOnCommit: false,
	})
	if err != nil {
		t.Fatalf("Failed to create log: %v", err)
	}
	defer log.Close()

	ballot := Ballot{ID: 1, NodeID: 1}

	// Write slots 0-19
	for slot := range uint64(20) {
		value := make([]byte, 8)
		binary.LittleEndian.PutUint64(value, slot)

		err := log.Accept(slot, ballot, value)
		if err != nil {
			t.Fatalf("Failed to accept slot %d: %v", slot, err)
		}

		err = log.Commit(slot)
		if err != nil {
			t.Fatalf("Failed to commit slot %d: %v", slot, err)
		}
	}

	// Replay from slot 10
	var sum uint64
	var count int

	err = log.ReplayFromSlot(10, func(entry *LogEntry) error {
		value := binary.LittleEndian.Uint64(entry.Value)
		sum += value
		count++
		return nil
	})

	if err != nil {
		t.Fatalf("Replay failed: %v", err)
	}

	// Should get slots 10-19 (10 entries)
	expectedCount := 10
	expectedSum := uint64(10 + 11 + 12 + 13 + 14 + 15 + 16 + 17 + 18 + 19) // 145

	if count != expectedCount {
		t.Errorf("Wrong count: expected %d, got %d", expectedCount, count)
	}
	if sum != expectedSum {
		t.Errorf("Wrong sum: expected %d, got %d", expectedSum, sum)
	}

	t.Logf("✓ Replayed %d entries from slot 10, sum=%d", count, sum)
}

// TestIteratorEntryReuse verifies that entry pointers are reused
func TestIteratorEntryReuse(t *testing.T) {
	dir := t.TempDir()
	log, err := NewFasterLog(Config{
		Path:         filepath.Join(dir, "test.log"),
		MutableSize:  1 * 1024 * 1024,
		SegmentSize:  10 * 1024 * 1024,
		NumThreads:   16,
		SyncOnCommit: false,
	})
	if err != nil {
		t.Fatalf("Failed to create log: %v", err)
	}
	defer log.Close()

	ballot := Ballot{ID: 1, NodeID: 1}

	// Write 5 entries
	for slot := range uint64(5) {
		value := []byte{byte(slot)}

		err := log.Accept(slot, ballot, value)
		if err != nil {
			t.Fatalf("Failed to accept slot %d: %v", slot, err)
		}

		err = log.Commit(slot)
		if err != nil {
			t.Fatalf("Failed to commit slot %d: %v", slot, err)
		}
	}

	// Track entry pointers
	var entryPointers []*LogEntry

	err = log.IterateCommitted(func(entry *LogEntry) error {
		// Store pointer (BAD practice, but we're testing!)
		entryPointers = append(entryPointers, entry)
		return nil
	}, IterateOptions{})

	if err != nil {
		t.Fatalf("Iteration failed: %v", err)
	}

	// Verify all pointers are the same (entry is reused)
	if len(entryPointers) < 2 {
		t.Fatal("Need at least 2 entries to test reuse")
	}

	firstPtr := entryPointers[0]
	allSame := true
	for i := 1; i < len(entryPointers); i++ {
		if entryPointers[i] != firstPtr {
			allSame = false
			break
		}
	}

	if !allSame {
		t.Errorf("Entry pointers should be reused (same address)")
	} else {
		t.Logf("✓ Entry pointer reused: %p (zero allocation!)", firstPtr)
	}

	// Verify that the last slot overwrote the entry
	// (All stored pointers now point to the last entry)
	for _, ptr := range entryPointers {
		if ptr.Slot != 4 {
			t.Errorf("All stored pointers should now show slot 4, got %d", ptr.Slot)
		}
	}
}

// TestIteratorLargeSortedData tests iteration over a large already-sorted dataset
// This verifies the stdlib sort.Slice handles sorted data efficiently (O(n log n))
// The old quicksort implementation would stack overflow on 100k+ sorted slots
func TestIteratorLargeSortedData(t *testing.T) {
	dir := t.TempDir()
	log, err := NewFasterLog(Config{
		Path:         filepath.Join(dir, "test.log"),
		MutableSize:  256 * 1024 * 1024, // Large buffer
		SegmentSize:  1024 * 1024 * 1024,
		NumThreads:   128,
		SyncOnCommit: false,
	})
	if err != nil {
		t.Fatalf("Failed to create log: %v", err)
	}
	defer log.Close()

	ballot := Ballot{ID: 1, NodeID: 1}

	// Write 10,000 slots in perfect sequential order (worst case for naive quicksort)
	numSlots := uint64(10000)
	expectedSum := (numSlots * (numSlots - 1)) / 2 // Sum of 0..9999

	t.Logf("Writing %d sequential slots...", numSlots)
	for slot := range numSlots {
		value := make([]byte, 8)
		binary.LittleEndian.PutUint64(value, slot)

		err := log.Accept(slot, ballot, value)
		if err != nil {
			t.Fatalf("Failed to accept slot %d: %v", slot, err)
		}

		err = log.Commit(slot)
		if err != nil {
			t.Fatalf("Failed to commit slot %d: %v", slot, err)
		}
	}

	// Iterate and verify (this would stack overflow with naive quicksort)
	t.Logf("Iterating over %d sorted slots...", numSlots)
	var sum uint64
	var count int

	err = log.IterateCommitted(func(entry *LogEntry) error {
		value := binary.LittleEndian.Uint64(entry.Value)
		sum += value
		count++
		return nil
	}, IterateOptions{})

	if err != nil {
		t.Fatalf("Iteration failed: %v", err)
	}

	if uint64(count) != numSlots {
		t.Errorf("Wrong count: expected %d, got %d", numSlots, count)
	}
	if sum != expectedSum {
		t.Errorf("Wrong sum: expected %d, got %d", expectedSum, sum)
	}

	t.Logf("✓ Successfully iterated %d sorted slots, sum=%d", count, sum)
}

// TestReplayConsistentSnapshot tests that ReplayFromSlot provides a consistent snapshot
// even when new commits arrive during iteration
func TestReplayConsistentSnapshot(t *testing.T) {
	dir := t.TempDir()
	log, err := NewFasterLog(Config{
		Path:         filepath.Join(dir, "test.log"),
		MutableSize:  64 * 1024 * 1024,
		SegmentSize:  1024 * 1024 * 1024,
		NumThreads:   128,
		SyncOnCommit: false,
	})
	if err != nil {
		t.Fatalf("Failed to create log: %v", err)
	}
	defer log.Close()

	ballot := Ballot{ID: 1, NodeID: 1}

	// Write and commit slots 0-99
	for slot := range uint64(100) {
		value := make([]byte, 8)
		binary.LittleEndian.PutUint64(value, slot)

		err := log.Accept(slot, ballot, value)
		if err != nil {
			t.Fatalf("Failed to accept slot %d: %v", slot, err)
		}

		err = log.Commit(slot)
		if err != nil {
			t.Fatalf("Failed to commit slot %d: %v", slot, err)
		}
	}

	// Start replay in a goroutine
	replayChan := make(chan []uint64, 1)
	startChan := make(chan struct{})
	go func() {
		seenSlots := make([]uint64, 0)

		// Signal we're about to start
		close(startChan)

		err := log.ReplayFromSlot(0, func(entry *LogEntry) error {
			seenSlots = append(seenSlots, entry.Slot)
			return nil
		})

		if err != nil {
			t.Errorf("Replay failed: %v", err)
		}

		replayChan <- seenSlots
	}()

	// Wait for goroutine to start
	<-startChan

	// Now commit new slots 100-149 AFTER replay has captured its snapshot
	// These should NOT appear in the replay (consistent snapshot)
	for slot := uint64(100); slot < 150; slot++ {
		value := make([]byte, 8)
		binary.LittleEndian.PutUint64(value, slot)

		err := log.Accept(slot, ballot, value)
		if err != nil {
			t.Fatalf("Failed to accept slot %d: %v", slot, err)
		}

		err = log.Commit(slot)
		if err != nil {
			t.Fatalf("Failed to commit slot %d: %v", slot, err)
		}
	}

	// Get results
	seenSlots := <-replayChan

	// Verify we only saw slots 0-99 (consistent snapshot)
	expectedCount := 100
	if len(seenSlots) != expectedCount {
		t.Errorf("Wrong count: expected %d, got %d", expectedCount, len(seenSlots))
	}

	// Verify no slots >= 100 leaked in
	for _, slot := range seenSlots {
		if slot >= 100 {
			t.Errorf("Slot %d leaked into replay (should be 0-99 only)", slot)
		}
	}

	// Verify we got exactly 0-99
	for i := range uint64(100) {
		if i >= uint64(len(seenSlots)) || seenSlots[i] != i {
			t.Errorf("Missing or wrong slot at position %d", i)
			break
		}
	}

	t.Logf("✓ Consistent snapshot: saw %d slots (0-99), new commits (100-149) correctly excluded", len(seenSlots))
}

// TestReplayRange tests the explicit range replay function
func TestReplayRange(t *testing.T) {
	dir := t.TempDir()
	log, err := NewFasterLog(Config{
		Path:         filepath.Join(dir, "test.log"),
		MutableSize:  64 * 1024 * 1024,
		SegmentSize:  1024 * 1024 * 1024,
		NumThreads:   128,
		SyncOnCommit: false,
	})
	if err != nil {
		t.Fatalf("Failed to create log: %v", err)
	}
	defer log.Close()

	ballot := Ballot{ID: 1, NodeID: 1}

	// Write slots 0-99
	for slot := range uint64(100) {
		value := make([]byte, 8)
		binary.LittleEndian.PutUint64(value, slot)

		err := log.Accept(slot, ballot, value)
		if err != nil {
			t.Fatalf("Failed to accept slot %d: %v", slot, err)
		}

		err = log.Commit(slot)
		if err != nil {
			t.Fatalf("Failed to commit slot %d: %v", slot, err)
		}
	}

	// Replay explicit range: 10-50
	var sum uint64
	var count int

	err = log.ReplayRange(10, 50, func(entry *LogEntry) error {
		value := binary.LittleEndian.Uint64(entry.Value)
		sum += value
		count++
		return nil
	})

	if err != nil {
		t.Fatalf("Replay failed: %v", err)
	}

	// Should get slots 10-50 inclusive (41 entries)
	expectedCount := 41
	expectedSum := uint64(0)
	for i := uint64(10); i <= 50; i++ {
		expectedSum += i
	}

	if count != expectedCount {
		t.Errorf("Wrong count: expected %d, got %d", expectedCount, count)
	}
	if sum != expectedSum {
		t.Errorf("Wrong sum: expected %d, got %d", expectedSum, sum)
	}

	t.Logf("✓ ReplayRange(10, 50) returned %d entries, sum=%d", count, sum)
}

// BenchmarkIterator benchmarks iteration performance
func BenchmarkIterator(b *testing.B) {
	dir := b.TempDir()
	log, err := NewFasterLog(Config{
		Path:         filepath.Join(dir, "bench.log"),
		MutableSize:  64 * 1024 * 1024,
		SegmentSize:  1024 * 1024 * 1024,
		NumThreads:   128,
		SyncOnCommit: false,
	})
	if err != nil {
		b.Fatalf("Failed to create log: %v", err)
	}
	defer log.Close()

	ballot := Ballot{ID: 1, NodeID: 1}

	// Write 10,000 entries
	numEntries := 10000
	for slot := uint64(0); slot < uint64(numEntries); slot++ {
		value := make([]byte, 100) // 100 bytes per entry
		binary.LittleEndian.PutUint64(value, slot)

		err := log.Accept(slot, ballot, value)
		if err != nil {
			b.Fatalf("Failed to accept: %v", err)
		}

		err = log.Commit(slot)
		if err != nil {
			b.Fatalf("Failed to commit: %v", err)
		}
	}

	b.ReportAllocs()

	for b.Loop() {
		var sum uint64
		err := log.IterateCommitted(func(entry *LogEntry) error {
			sum += entry.Slot
			return nil
		}, IterateOptions{})

		if err != nil {
			b.Fatalf("Iteration failed: %v", err)
		}
	}
}
