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
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

func TestSnapshotBasic(t *testing.T) {
	// Setup
	dir := t.TempDir()
	logPath := filepath.Join(dir, "test.log")
	snapDir := filepath.Join(dir, "snapshots")

	cfg := Config{
		Path:         logPath,
		MutableSize:  1024 * 1024,
		SegmentSize:  10 * 1024 * 1024,
		NumThreads:   16,
		SyncOnCommit: false,
	}

	log, err := NewFasterLog(cfg)
	if err != nil {
		t.Fatalf("Failed to create log: %v", err)
	}
	defer log.Close()

	snapMgr, err := NewSnapshotManager(snapDir, log)
	if err != nil {
		t.Fatalf("Failed to create snapshot manager: %v", err)
	}

	// Write some entries
	for i := uint64(1); i <= 100; i++ {
		err := log.Accept(i, Ballot{ID: 1, NodeID: 1}, fmt.Appendf(nil, "value-%d", i))
		if err != nil {
			t.Fatalf("Failed to accept slot %d: %v", i, err)
		}

		err = log.Commit(i)
		if err != nil {
			t.Fatalf("Failed to commit slot %d: %v", i, err)
		}
	}

	// Create a snapshot at slot 50
	snapshotData := []byte("snapshot-at-50")
	err = snapMgr.CreateSnapshot(50, snapshotData)
	if err != nil {
		t.Fatalf("Failed to create snapshot: %v", err)
	}

	// Retrieve the snapshot
	snapshot, err := snapMgr.GetLatestSnapshot()
	if err != nil {
		t.Fatalf("Failed to get snapshot: %v", err)
	}

	if snapshot.Slot != 50 {
		t.Errorf("Expected snapshot slot 50, got %d", snapshot.Slot)
	}

	if string(snapshot.Data) != "snapshot-at-50" {
		t.Errorf("Expected snapshot data 'snapshot-at-50', got '%s'", snapshot.Data)
	}
}

func TestSnapshotTruncation(t *testing.T) {
	// Setup
	dir := t.TempDir()
	logPath := filepath.Join(dir, "test.log")
	snapDir := filepath.Join(dir, "snapshots")

	cfg := Config{
		Path:         logPath,
		MutableSize:  1024 * 1024,
		SegmentSize:  10 * 1024 * 1024,
		NumThreads:   16,
		SyncOnCommit: true, // Important: sync to ensure data is on disk
	}

	// Phase 1: Write entries and mark for truncation
	{
		log, err := NewFasterLog(cfg)
		if err != nil {
			t.Fatalf("Failed to create log: %v", err)
		}

		snapMgr, err := NewSnapshotManager(snapDir, log)
		if err != nil {
			t.Fatalf("Failed to create snapshot manager: %v", err)
		}

		// Write 100 entries
		for i := uint64(1); i <= 100; i++ {
			err := log.Accept(i, Ballot{ID: 1, NodeID: 1}, fmt.Appendf(nil, "value-%d", i))
			if err != nil {
				t.Fatalf("Failed to accept slot %d: %v", i, err)
			}

			err = log.Commit(i)
			if err != nil {
				t.Fatalf("Failed to commit slot %d: %v", i, err)
			}
		}

		// Get initial tail size
		initialSize := log.tailSize.Load()
		t.Logf("Initial tail size: %d bytes", initialSize)

		// Create snapshot at slot 50
		err = snapMgr.CreateSnapshot(50, []byte("snapshot-50"))
		if err != nil {
			t.Fatalf("Failed to create snapshot: %v", err)
		}

		// Mark for truncation (offline operation)
		err = snapMgr.TruncateLog(50)
		if err != nil {
			t.Fatalf("Failed to mark truncation: %v", err)
		}

		log.Close()
	}

	// Phase 2: Perform offline compaction and verify
	{
		// Compact on startup (before opening log)
		truncatedSlot, err := CompactOnStartup(logPath, snapDir)
		if err != nil {
			t.Fatalf("Failed to compact on startup: %v", err)
		}

		if truncatedSlot != 50 {
			t.Errorf("Expected truncation at slot 50, got %d", truncatedSlot)
		}

		// Reopen log
		log, err := NewFasterLog(cfg)
		if err != nil {
			t.Fatalf("Failed to reopen log: %v", err)
		}
		defer log.Close()

		newSize := log.tailSize.Load()
		t.Logf("New tail size after truncation: %d bytes", newSize)

		// Verify entries <= 50 are gone
		for i := uint64(1); i <= 50; i++ {
			_, err := log.Read(i)
			if err == nil {
				t.Errorf("Expected slot %d to be truncated, but it still exists", i)
			}
		}

		// Verify entries > 50 still exist
		for i := uint64(51); i <= 100; i++ {
			entry, err := log.ReadCommittedOnly(i)
			if err != nil {
				t.Errorf("Expected slot %d to exist after truncation: %v", i, err)
				continue
			}

			expectedValue := fmt.Sprintf("value-%d", i)
			if string(entry.Value) != expectedValue {
				t.Errorf("Slot %d: expected value '%s', got '%s'", i, expectedValue, entry.Value)
			}
		}
	}
}

func TestSnapshotMultiple(t *testing.T) {
	// Setup
	dir := t.TempDir()
	logPath := filepath.Join(dir, "test.log")
	snapDir := filepath.Join(dir, "snapshots")

	cfg := Config{
		Path:         logPath,
		MutableSize:  1024 * 1024,
		SegmentSize:  10 * 1024 * 1024,
		NumThreads:   16,
		SyncOnCommit: false,
	}

	log, err := NewFasterLog(cfg)
	if err != nil {
		t.Fatalf("Failed to create log: %v", err)
	}
	defer log.Close()

	snapMgr, err := NewSnapshotManager(snapDir, log)
	if err != nil {
		t.Fatalf("Failed to create snapshot manager: %v", err)
	}

	// Create multiple snapshots
	snapshots := []uint64{100, 200, 300, 400, 500}
	for _, slot := range snapshots {
		data := fmt.Appendf(nil, "snapshot-%d", slot)
		err := snapMgr.CreateSnapshot(slot, data)
		if err != nil {
			t.Fatalf("Failed to create snapshot at %d: %v", slot, err)
		}
	}

	// Get latest snapshot
	latest, err := snapMgr.GetLatestSnapshot()
	if err != nil {
		t.Fatalf("Failed to get latest snapshot: %v", err)
	}

	if latest.Slot != 500 {
		t.Errorf("Expected latest snapshot at slot 500, got %d", latest.Slot)
	}

	// Get specific snapshot
	snap, err := snapMgr.GetSnapshot(250)
	if err != nil {
		t.Fatalf("Failed to get snapshot at 250: %v", err)
	}

	// Should get snapshot at 200 (nearest before 250)
	if snap.Slot != 200 {
		t.Errorf("Expected snapshot at slot 200, got %d", snap.Slot)
	}

	// Cleanup old snapshots (keep only 2)
	err = snapMgr.CleanupOldSnapshots(2)
	if err != nil {
		t.Fatalf("Failed to cleanup old snapshots: %v", err)
	}

	// Verify only 2 snapshots remain
	remaining, err := snapMgr.listSnapshots()
	if err != nil {
		t.Fatalf("Failed to list snapshots: %v", err)
	}

	if len(remaining) != 2 {
		t.Errorf("Expected 2 snapshots remaining, got %d", len(remaining))
	}

	// Verify they're the latest ones (400, 500)
	snap1, _ := snapMgr.readSnapshot(remaining[0])
	snap2, _ := snapMgr.readSnapshot(remaining[1])

	if snap1.Slot != 400 || snap2.Slot != 500 {
		t.Errorf("Expected snapshots 400 and 500, got %d and %d", snap1.Slot, snap2.Slot)
	}
}

func TestSnapshotRecovery(t *testing.T) {
	// Setup
	dir := t.TempDir()
	logPath := filepath.Join(dir, "test.log")
	snapDir := filepath.Join(dir, "snapshots")

	cfg := Config{
		Path:         logPath,
		MutableSize:  1024 * 1024,
		SegmentSize:  10 * 1024 * 1024,
		NumThreads:   16,
		SyncOnCommit: true,
	}

	// Phase 1: Create log, write entries, create snapshot, mark for truncation
	{
		log, err := NewFasterLog(cfg)
		if err != nil {
			t.Fatalf("Failed to create log: %v", err)
		}

		snapMgr, err := NewSnapshotManager(snapDir, log)
		if err != nil {
			t.Fatalf("Failed to create snapshot manager: %v", err)
		}

		// Write 100 entries
		for i := uint64(1); i <= 100; i++ {
			_ = log.Accept(i, Ballot{ID: 1, NodeID: 1}, fmt.Appendf(nil, "value-%d", i))
			_ = log.Commit(i)
		}

		// Snapshot at 50
		err = snapMgr.CreateSnapshot(50, []byte("snapshot-50"))
		if err != nil {
			t.Fatalf("Failed to create snapshot: %v", err)
		}

		// Mark for truncation
		err = snapMgr.TruncateLog(50)
		if err != nil {
			t.Fatalf("Failed to mark truncation: %v", err)
		}

		log.Close()
	}

	// Phase 2: Compact offline, reopen, and verify recovery
	{
		// Perform offline compaction
		_, err := CompactOnStartup(logPath, snapDir)
		if err != nil {
			t.Fatalf("Failed to compact on startup: %v", err)
		}

		log, err := NewFasterLog(cfg)
		if err != nil {
			t.Fatalf("Failed to reopen log: %v", err)
		}
		defer log.Close()

		snapMgr, err := NewSnapshotManager(snapDir, log)
		if err != nil {
			t.Fatalf("Failed to create snapshot manager: %v", err)
		}

		// Load snapshot
		snapshot, err := snapMgr.GetLatestSnapshot()
		if err != nil {
			t.Fatalf("Failed to get snapshot: %v", err)
		}

		t.Logf("Recovered snapshot at slot %d", snapshot.Slot)

		// Verify we can't read truncated entries
		for i := uint64(1); i <= 50; i++ {
			_, err := log.Read(i)
			if err == nil {
				t.Errorf("Slot %d should have been truncated", i)
			}
		}

		// Verify we can read remaining entries
		for i := uint64(51); i <= 100; i++ {
			entry, err := log.ReadCommittedOnly(i)
			if err != nil {
				t.Errorf("Failed to read slot %d: %v", i, err)
				continue
			}

			expectedValue := fmt.Sprintf("value-%d", i)
			if string(entry.Value) != expectedValue {
				t.Errorf("Slot %d: expected '%s', got '%s'", i, expectedValue, entry.Value)
			}
		}
	}
}

func TestSnapshotCorruption(t *testing.T) {
	// Setup
	dir := t.TempDir()
	logPath := filepath.Join(dir, "test.log")
	snapDir := filepath.Join(dir, "snapshots")

	cfg := Config{
		Path:         logPath,
		MutableSize:  1024 * 1024,
		SegmentSize:  10 * 1024 * 1024,
		NumThreads:   16,
		SyncOnCommit: false,
	}

	log, err := NewFasterLog(cfg)
	if err != nil {
		t.Fatalf("Failed to create log: %v", err)
	}
	defer log.Close()

	snapMgr, err := NewSnapshotManager(snapDir, log)
	if err != nil {
		t.Fatalf("Failed to create snapshot manager: %v", err)
	}

	// Create a snapshot
	err = snapMgr.CreateSnapshot(100, []byte("valid-snapshot"))
	if err != nil {
		t.Fatalf("Failed to create snapshot: %v", err)
	}

	// Corrupt the snapshot file by modifying the data
	// (checksum is at the end, so corrupting data will cause checksum mismatch)
	snapPath := snapMgr.snapshotPath(100)
	file, err := os.OpenFile(snapPath, os.O_RDWR, 0644)
	if err != nil {
		t.Fatalf("Failed to open snapshot file: %v", err)
	}

	// Skip header (24 bytes) and corrupt data in the middle
	file.Seek(30, 0)
	file.Write([]byte{0xFF, 0xFF, 0xFF, 0xFF})
	file.Close()

	// Try to read corrupted snapshot
	_, err = snapMgr.GetLatestSnapshot()
	if err != ErrCorruptedSnapshot {
		t.Errorf("Expected ErrCorruptedSnapshot, got %v", err)
	}
}

func BenchmarkSnapshotCreate(b *testing.B) {
	dir := b.TempDir()
	logPath := filepath.Join(dir, "bench.log")
	snapDir := filepath.Join(dir, "snapshots")

	cfg := Config{
		Path:         logPath,
		MutableSize:  10 * 1024 * 1024,
		SegmentSize:  100 * 1024 * 1024,
		NumThreads:   128,
		SyncOnCommit: false,
	}

	log, _ := NewFasterLog(cfg)
	defer log.Close()

	snapMgr, _ := NewSnapshotManager(snapDir, log)

	// Create a 1MB snapshot
	data := make([]byte, 1024*1024)
	for i := range data {
		data[i] = byte(i % 256)
	}

	for i := 0; b.Loop(); i++ {
		_ = snapMgr.CreateSnapshot(uint64(i), data)
	}
}

func BenchmarkSnapshotRead(b *testing.B) {
	dir := b.TempDir()
	logPath := filepath.Join(dir, "bench.log")
	snapDir := filepath.Join(dir, "snapshots")

	cfg := Config{
		Path:         logPath,
		MutableSize:  10 * 1024 * 1024,
		SegmentSize:  100 * 1024 * 1024,
		NumThreads:   128,
		SyncOnCommit: false,
	}

	log, _ := NewFasterLog(cfg)
	defer log.Close()

	snapMgr, _ := NewSnapshotManager(snapDir, log)

	// Create a snapshot
	data := make([]byte, 1024*1024)
	snapMgr.CreateSnapshot(100, data)

	for b.Loop() {
		_, _ = snapMgr.GetLatestSnapshot()
	}
}
