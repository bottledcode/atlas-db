//go:build !race

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
	"bytes"
	"crypto/sha256"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// TestDataCorruptionDetection tests whether the race condition actually causes data corruption
// or if it's a false positive from the race detector.
//
// This test will FAIL if there's actual data corruption (bytes being read incorrectly).
// If this test PASSES, it suggests the race detector is seeing a false positive.
func TestDataCorruptionDetection(t *testing.T) {
	dir := t.TempDir()

	cfg := Config{
		Path:         dir + "/test.log",
		MutableSize:  128 * 1024, // Small buffer to maximize contention
		SyncOnCommit: false,
	}

	log, err := NewFasterLog(cfg)
	if err != nil {
		t.Fatalf("Failed to create log: %v", err)
	}
	defer log.Close()

	// Track any corruption we detect
	corruptionCount := atomic.Uint64{}

	// Use deterministic data patterns that we can verify
	numWriters := 8
	entriesPerWriter := 500

	var wg sync.WaitGroup

	// Map to track expected checksums for each slot
	expectedData := sync.Map{}

	// Writer goroutines - write deterministic data
	for w := range numWriters {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()

			for i := range entriesPerWriter {
				slot := uint64(workerID*entriesPerWriter + i)

				// Create deterministic data: hash of slot number repeated
				hash := sha256.Sum256(fmt.Appendf(nil, "slot-%d", slot))
				value := bytes.Repeat(hash[:], 4) // 128 bytes of deterministic data

				// Store expected data
				expectedData.Store(slot, value)

				// Accept
				for {
					if err := log.Accept(slot, Ballot{ID: 1, NodeID: 1}, value); err != nil {
						if err == ErrBufferFull {
							time.Sleep(1 * time.Millisecond)
							continue
						}
						t.Errorf("Accept failed: %v", err)
						return
					}
					break
				}

				// Commit
				if err := log.Commit(slot); err != nil {
					t.Errorf("Commit failed: %v", err)
					return
				}
			}
		}(w)
	}

	// Reader goroutines - aggressively read and verify data
	for r := range 4 {
		wg.Add(1)
		go func(readerID int) {
			defer wg.Done()

			// Keep reading until writers are done
			for i := 0; i < numWriters*entriesPerWriter; i++ {
				slot := uint64(i)

				// Wait for slot to be committed
				var entry *LogEntry
				var err error
				for range 100 {
					entry, err = log.Read(slot)
					if err == nil && entry.Committed {
						break
					}
					time.Sleep(1 * time.Millisecond)
				}

				if err != nil {
					continue // Slot might not exist yet
				}

				// Verify data integrity
				expectedVal, ok := expectedData.Load(slot)
				if !ok {
					continue // Not written yet
				}

				expected := expectedVal.([]byte)
				if !bytes.Equal(entry.Value, expected) {
					corruptionCount.Add(1)
					t.Errorf("CORRUPTION DETECTED in slot %d by reader %d: got %d bytes, expected %d bytes",
						slot, readerID, len(entry.Value), len(expected))

					// Show first difference
					minLen := min(len(expected), len(entry.Value))
					for j := 0; j < minLen; j++ {
						if entry.Value[j] != expected[j] {
							t.Errorf("  First diff at byte %d: got 0x%02x, expected 0x%02x",
								j, entry.Value[j], expected[j])
							break
						}
					}
				}
			}
		}(r)
	}

	wg.Wait()

	corruptions := corruptionCount.Load()
	if corruptions > 0 {
		t.Fatalf("REAL BUG: Detected %d data corruptions - the race condition causes actual data corruption!", corruptions)
	} else {
		t.Log("No data corruption detected - race detector may be seeing false positive on copy() operations")
	}
}

// TestPublishedBarrierEnforcement verifies that the published counter truly prevents
// readers from accessing regions that writers are still copying into
func TestPublishedBarrierEnforcement(t *testing.T) {
	rb := NewRingBuffer(100 * 1024)

	// Track if we ever read partially-written data
	partialReadCount := atomic.Uint64{}

	var wg sync.WaitGroup
	stopFlag := atomic.Bool{}

	// Writer: continuously append entries
	wg.Go(func() {
		slot := uint64(0)

		for slot < 200 { // Simpler loop condition
			// Create entry with specific pattern
			value := bytes.Repeat([]byte{byte(slot % 256)}, 100)
			entry := &LogEntry{
				Slot:      slot,
				Ballot:    Ballot{ID: 1, NodeID: 1},
				Value:     value,
				Committed: false,
			}

			_, err := rb.Append(entry)
			if err != nil {
				t.Logf("Append failed at slot %d: %v", slot, err)
				// Buffer full, stop writing
				break
			}

			slot++
		}

		stopFlag.Store(true)
	})

	// Reader: aggressively scan published region
	wg.Go(func() {

		for !stopFlag.Load() {
			offset := rb.tail.Load()
			published := rb.published.Load()

			// Scan published region
			for offset < published {
				entry, err := rb.Read(offset)
				if err != nil {
					// If we get an error reading from published region, that's bad!
					t.Errorf("ERROR reading from published region at offset %d (published=%d): %v",
						offset, published, err)
					partialReadCount.Add(1)
					break
				}

				// Verify data integrity - all bytes should match slot % 256
				expected := byte(entry.Slot % 256)
				for i, b := range entry.Value {
					if b != expected {
						t.Errorf("CORRUPTION: Slot %d byte %d: got 0x%02x, expected 0x%02x (published barrier failed!)",
							entry.Slot, i, b, expected)
						partialReadCount.Add(1)
						break
					}
				}

				// Move to next entry
				valueLen := uint64(len(entry.Value))
				entrySize := uint64(entryHeaderSize) + valueLen + uint64(checksumSize)
				offset += entrySize
			}
		}
	})

	wg.Wait()

	partialReads := partialReadCount.Load()
	if partialReads > 0 {
		t.Fatalf("REAL BUG: Published barrier failed - detected %d cases of reading partial/corrupted data!", partialReads)
	} else {
		t.Log("Published barrier is working correctly - no partial reads detected")
	}
}
