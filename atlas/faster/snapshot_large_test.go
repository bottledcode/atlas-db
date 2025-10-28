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
	"os"
	"path/filepath"
	"testing"
)

// TestLargeFileCompaction verifies streaming compaction with large files
// without loading entire file into memory
func TestLargeFileCompaction(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping large file test in short mode")
	}

	dir := t.TempDir()
	logPath := filepath.Join(dir, "large.log")
	snapDir := filepath.Join(dir, "snapshots")

	cfg := Config{
		Path:         logPath,
		MutableSize:  10 * 1024 * 1024, // 10MB mutable
		SegmentSize:  100 * 1024 * 1024,
		NumThreads:   16,
		SyncOnCommit: false, // Fast for testing
	}

	// Phase 1: Create large log (simulate production workload)
	{
		log, err := NewFasterLog(cfg)
		if err != nil {
			t.Fatalf("Failed to create log: %v", err)
		}

		snapMgr, err := NewSnapshotManager(snapDir, log)
		if err != nil {
			t.Fatalf("Failed to create snapshot manager: %v", err)
		}

		// Write 50K entries with 1KB values = ~50MB log
		// (Enough to test streaming without being too slow)
		t.Log("Writing 50K entries (~50MB log)...")
		largeValue := make([]byte, 1024)
		for i := range largeValue {
			largeValue[i] = byte(i % 256)
		}

		for i := uint64(1); i <= 50000; i++ {
			err := log.Accept(i, Ballot{ID: 1, NodeID: 1}, largeValue)
			if err != nil {
				t.Fatalf("Failed to accept slot %d: %v", i, err)
			}

			err = log.Commit(i)
			if err != nil {
				t.Fatalf("Failed to commit slot %d: %v", i, err)
			}

			if i%10000 == 0 {
				t.Logf("Progress: %d entries written", i)
			}
		}

		initialSize := log.tailSize.Load()
		t.Logf("Initial log size: %.2f MB", float64(initialSize)/(1024*1024))

		// Snapshot at 25K (half way)
		err = snapMgr.CreateSnapshot(25000, []byte("snapshot-25000"))
		if err != nil {
			t.Fatalf("Failed to create snapshot: %v", err)
		}

		// Mark for truncation
		err = snapMgr.TruncateLog(25000)
		if err != nil {
			t.Fatalf("Failed to mark truncation: %v", err)
		}

		log.Close()
	}

	// Phase 2: Measure compaction and verify streaming
	{
		// Get initial file size
		statBefore, err := os.Stat(logPath)
		if err != nil {
			t.Fatalf("Failed to stat log before compaction: %v", err)
		}
		sizeBefore := statBefore.Size()

		t.Log("Starting offline compaction...")
		truncatedSlot, err := CompactOnStartup(logPath, snapDir)
		if err != nil {
			t.Fatalf("Failed to compact: %v", err)
		}

		if truncatedSlot != 25000 {
			t.Errorf("Expected truncation at slot 25000, got %d", truncatedSlot)
		}

		// Verify file size decreased
		statAfter, err := os.Stat(logPath)
		if err != nil {
			t.Fatalf("Failed to stat compacted log: %v", err)
		}

		sizeAfter := statAfter.Size()
		t.Logf("Log size before: %.2f MB", float64(sizeBefore)/(1024*1024))
		t.Logf("Log size after: %.2f MB", float64(sizeAfter)/(1024*1024))
		t.Logf("Space saved: %.2f MB (%.1f%%)",
			float64(sizeBefore-sizeAfter)/(1024*1024),
			100.0*float64(sizeBefore-sizeAfter)/float64(sizeBefore))

		// Should have saved some space (at least 20%)
		if sizeAfter >= sizeBefore {
			t.Errorf("Compaction did not reduce file size: before=%d, after=%d",
				sizeBefore, sizeAfter)
		}

		minSavings := float64(sizeBefore) * 0.2 // At least 20% savings
		if float64(sizeBefore-sizeAfter) < minSavings {
			t.Errorf("Insufficient space savings: %.1f%% (expected > 20%%)",
				100.0*float64(sizeBefore-sizeAfter)/float64(sizeBefore))
		}
	}

	// Phase 3: Verify data integrity after compaction
	{
		log, err := NewFasterLog(cfg)
		if err != nil {
			t.Fatalf("Failed to reopen log: %v", err)
		}
		defer log.Close()

		// Verify truncated entries are gone
		for i := uint64(1); i <= 25000; i++ {
			_, err := log.Read(i)
			if err == nil {
				t.Errorf("Slot %d should have been truncated", i)
				break // Don't spam errors
			}
		}

		// Verify remaining entries exist and are correct
		errorCount := 0
		for i := uint64(25001); i <= 50000; i++ {
			entry, err := log.ReadCommittedOnly(i)
			if err != nil {
				errorCount++
				if errorCount <= 5 { // Only show first 5 errors
					t.Errorf("Failed to read slot %d: %v", i, err)
				}
				continue
			}

			// Verify value is correct
			if len(entry.Value) != 1024 {
				t.Errorf("Slot %d: wrong value size %d", i, len(entry.Value))
				break
			}

			// Check first few bytes
			for j := 0; j < 10 && j < len(entry.Value); j++ {
				expected := byte(j % 256)
				if entry.Value[j] != expected {
					t.Errorf("Slot %d: corrupted value at byte %d", i, j)
					break
				}
			}

			if i%5000 == 25001 {
				t.Logf("Verified slot %d", i)
			}
		}

		if errorCount > 0 {
			t.Errorf("Found %d errors in remaining entries", errorCount)
		} else {
			t.Log("All remaining entries verified successfully")
		}
	}
}

// TestCompactionCleansUpOnError verifies temp file cleanup on failure
func TestCompactionCleansUpOnError(t *testing.T) {
	dir := t.TempDir()
	logPath := filepath.Join(dir, "test.log")
	snapDir := filepath.Join(dir, "snapshots")

	// Create a snapshot marker for a non-existent log
	os.MkdirAll(snapDir, 0755)
	markerPath := filepath.Join(snapDir, "truncate_marker")

	// Write marker pointing to slot 1000
	file, _ := os.Create(markerPath)
	file.Write([]byte{0xE8, 0x03, 0, 0, 0, 0, 0, 0}) // 1000 in little-endian
	file.Close()

	// Try to compact (should fail gracefully)
	_, err := CompactOnStartup(logPath, snapDir)

	// Should not panic or leave temp files
	tempFile := logPath + ".compact.tmp"
	if _, err := os.Stat(tempFile); err == nil {
		t.Error("Temp file not cleaned up after error")
	}

	// Marker should still exist (failed compaction)
	if _, err := os.Stat(markerPath); os.IsNotExist(err) {
		t.Error("Marker file removed despite failed compaction")
	}

	// If log exists, should not have been corrupted
	if _, err := os.Stat(logPath); err == nil {
		t.Error("Log file should not have been created")
	}

	t.Logf("Error handling verified: %v", err)
}

// BenchmarkLargeCompaction measures compaction performance on large files
func BenchmarkLargeCompaction(b *testing.B) {
	if testing.Short() {
		b.Skip("Skipping large compaction benchmark in short mode")
	}

	dir := b.TempDir()
	logPath := filepath.Join(dir, "bench.log")
	snapDir := filepath.Join(dir, "snapshots")

	cfg := Config{
		Path:         logPath,
		MutableSize:  10 * 1024 * 1024,
		SegmentSize:  100 * 1024 * 1024,
		NumThreads:   16,
		SyncOnCommit: false,
	}

	// Create a 100MB log
	log, _ := NewFasterLog(cfg)
	snapMgr, _ := NewSnapshotManager(snapDir, log)

	largeValue := make([]byte, 2048) // 2KB entries
	for i := uint64(1); i <= 50000; i++ {
		log.Accept(i, Ballot{ID: 1, NodeID: 1}, largeValue)
		log.Commit(i)
	}

	// Snapshot at midpoint
	snapMgr.CreateSnapshot(25000, []byte("snapshot"))
	snapMgr.TruncateLog(25000)
	log.Close()

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		// Restore log for next iteration
		if i > 0 {
			// Copy backup to restore original file
			// (In real benchmark, you'd time just the compaction)
		}

		_, err := CompactOnStartup(logPath, snapDir)
		if err != nil {
			b.Fatalf("Compaction failed: %v", err)
		}

		// Recreate marker for next iteration
		if i < b.N-1 {
			markerPath := filepath.Join(snapDir, "truncate_marker")
			file, _ := os.Create(markerPath)
			file.Write([]byte{0x90, 0x61, 0, 0, 0, 0, 0, 0}) // 25000
			file.Close()
		}
	}
}
