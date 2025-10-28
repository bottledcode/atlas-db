package faster

import (
	"fmt"
	"sync"
	"testing"
)

func TestConcurrentAccepts(t *testing.T) {
	dir := t.TempDir()

	cfg := Config{
		Path:         dir + "/test.log",
		MutableSize:  10 * 1024 * 1024, // 10MB
		SyncOnCommit: false,
	}

	log, err := NewFasterLog(cfg)
	if err != nil {
		t.Fatalf("Failed to create log: %v", err)
	}
	defer log.Close()

	// Concurrently accept entries
	numGoroutines := 10
	entriesPerGoroutine := 100

	var wg sync.WaitGroup
	wg.Add(numGoroutines)

	for g := 0; g < numGoroutines; g++ {
		go func(goroutineID int) {
			defer wg.Done()

			for i := 0; i < entriesPerGoroutine; i++ {
				slot := uint64(goroutineID*entriesPerGoroutine + i)
				ballot := Ballot{ID: uint64(i), NodeID: uint64(goroutineID)}
				value := []byte(fmt.Sprintf("g%d-i%d", goroutineID, i))

				err := log.Accept(slot, ballot, value)
				if err != nil {
					t.Errorf("Goroutine %d: failed to accept slot %d: %v", goroutineID, slot, err)
					return
				}
			}
		}(g)
	}

	wg.Wait()

	// Verify all entries are readable
	totalEntries := numGoroutines * entriesPerGoroutine
	for i := 0; i < totalEntries; i++ {
		slot := uint64(i)
		entry, err := log.Read(slot)
		if err != nil {
			t.Errorf("Failed to read slot %d: %v", slot, err)
		}
		if entry.Slot != slot {
			t.Errorf("Slot %d: expected slot %d, got %d", i, slot, entry.Slot)
		}
	}
}

func TestConcurrentAcceptAndCommit(t *testing.T) {
	dir := t.TempDir()

	cfg := Config{
		Path:         dir + "/test.log",
		MutableSize:  10 * 1024 * 1024,
		SyncOnCommit: false,
	}

	log, err := NewFasterLog(cfg)
	if err != nil {
		t.Fatalf("Failed to create log: %v", err)
	}
	defer log.Close()

	numSlots := 1000

	// Accept all entries first
	for i := 0; i < numSlots; i++ {
		slot := uint64(i)
		err := log.Accept(slot, Ballot{ID: 1, NodeID: 1}, []byte(fmt.Sprintf("value-%d", i)))
		if err != nil {
			t.Fatalf("Failed to accept slot %d: %v", slot, err)
		}
	}

	// Concurrently commit entries
	numGoroutines := 10
	slotsPerGoroutine := numSlots / numGoroutines

	var wg sync.WaitGroup
	wg.Add(numGoroutines)

	errors := make(chan error, numGoroutines)

	for g := 0; g < numGoroutines; g++ {
		go func(goroutineID int) {
			defer wg.Done()

			start := goroutineID * slotsPerGoroutine
			end := start + slotsPerGoroutine

			for i := start; i < end; i++ {
				slot := uint64(i)
				err := log.Commit(slot)
				if err != nil {
					errors <- fmt.Errorf("goroutine %d: failed to commit slot %d: %w", goroutineID, slot, err)
					return
				}
			}
		}(g)
	}

	wg.Wait()
	close(errors)

	// Check for errors
	for err := range errors {
		t.Error(err)
	}

	// Verify all entries are committed
	for i := 0; i < numSlots; i++ {
		slot := uint64(i)
		entry, err := log.ReadCommittedOnly(slot)
		if err != nil {
			t.Errorf("Failed to read committed slot %d: %v", slot, err)
		}
		if !entry.Committed {
			t.Errorf("Slot %d not committed", slot)
		}
	}
}

func TestConcurrentReads(t *testing.T) {
	dir := t.TempDir()

	cfg := Config{
		Path:         dir + "/test.log",
		MutableSize:  10 * 1024 * 1024,
		SyncOnCommit: true,
	}

	log, err := NewFasterLog(cfg)
	if err != nil {
		t.Fatalf("Failed to create log: %v", err)
	}
	defer log.Close()

	// Accept and commit entries
	numEntries := 100
	for i := 0; i < numEntries; i++ {
		slot := uint64(i)
		err := log.Accept(slot, Ballot{ID: 1, NodeID: 1}, []byte(fmt.Sprintf("value-%d", i)))
		if err != nil {
			t.Fatalf("Failed to accept slot %d: %v", slot, err)
		}
		err = log.Commit(slot)
		if err != nil {
			t.Fatalf("Failed to commit slot %d: %v", slot, err)
		}
	}

	// Concurrently read entries (lock-free reads from tail!)
	numGoroutines := 20
	readsPerGoroutine := 100

	var wg sync.WaitGroup
	wg.Add(numGoroutines)

	errors := make(chan error, numGoroutines*readsPerGoroutine)

	for g := 0; g < numGoroutines; g++ {
		go func(goroutineID int) {
			defer wg.Done()

			for i := 0; i < readsPerGoroutine; i++ {
				slot := uint64(i % numEntries)
				entry, err := log.ReadCommittedOnly(slot)
				if err != nil {
					errors <- fmt.Errorf("goroutine %d: failed to read slot %d: %w", goroutineID, slot, err)
					continue
				}

				expectedValue := fmt.Sprintf("value-%d", slot)
				if string(entry.Value) != expectedValue {
					errors <- fmt.Errorf("goroutine %d: slot %d: expected value %s, got %s",
						goroutineID, slot, expectedValue, entry.Value)
				}
			}
		}(g)
	}

	wg.Wait()
	close(errors)

	// Check for errors
	errorCount := 0
	for err := range errors {
		t.Error(err)
		errorCount++
		if errorCount >= 10 {
			t.Fatal("Too many errors, stopping")
		}
	}
}

func TestConcurrentAcceptCommitRead(t *testing.T) {
	dir := t.TempDir()

	cfg := Config{
		Path:         dir + "/test.log",
		MutableSize:  10 * 1024 * 1024,
		SyncOnCommit: false,
	}

	log, err := NewFasterLog(cfg)
	if err != nil {
		t.Fatalf("Failed to create log: %v", err)
	}
	defer log.Close()

	numSlots := 500
	var wg sync.WaitGroup

	// Accept goroutine
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < numSlots; i++ {
			slot := uint64(i)
			err := log.Accept(slot, Ballot{ID: 1, NodeID: 1}, []byte(fmt.Sprintf("val%d", i)))
			if err != nil {
				t.Errorf("Failed to accept slot %d: %v", slot, err)
			}
		}
	}()

	// Commit goroutine (slightly delayed to let some accepts happen first)
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < numSlots; i++ {
			slot := uint64(i)
			// Keep trying until accept completes
			for {
				err := log.Commit(slot)
				if err == ErrSlotNotFound {
					// Not accepted yet, retry
					continue
				}
				if err != nil {
					t.Errorf("Failed to commit slot %d: %v", slot, err)
				}
				break
			}
		}
	}()

	// Read goroutine
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < numSlots; i++ {
			slot := uint64(i)
			// Keep trying until slot exists
			for {
				_, err := log.Read(slot)
				if err == ErrSlotNotFound {
					// Not yet available
					continue
				}
				if err != nil {
					t.Errorf("Failed to read slot %d: %v", slot, err)
				}
				break
			}
		}
	}()

	wg.Wait()

	// Final verification: all should be committed
	for i := 0; i < numSlots; i++ {
		slot := uint64(i)
		entry, err := log.ReadCommittedOnly(slot)
		if err != nil {
			t.Errorf("Final check: slot %d not committed: %v", slot, err)
		}
		expectedValue := fmt.Sprintf("val%d", i)
		if string(entry.Value) != expectedValue {
			t.Errorf("Final check: slot %d value mismatch: got %s, want %s",
				slot, entry.Value, expectedValue)
		}
	}
}
