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
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func TestNewFasterLog(t *testing.T) {
	dir := t.TempDir()
	logPath := filepath.Join(dir, "test.log")

	cfg := Config{
		Path:         logPath,
		MutableSize:  1024 * 1024,      // 1MB
		SegmentSize:  10 * 1024 * 1024, // 10MB
		NumThreads:   16,
		SyncOnCommit: false,
	}

	log, err := NewFasterLog(cfg)
	if err != nil {
		t.Fatalf("Failed to create log: %v", err)
	}
	defer log.Close()

	// Verify file was created
	if _, err := os.Stat(logPath); os.IsNotExist(err) {
		t.Errorf("Log file was not created")
	}
}

func TestAcceptAndRead(t *testing.T) {
	dir := t.TempDir()
	logPath := filepath.Join(dir, "test.log")

	cfg := Config{
		Path:         logPath,
		MutableSize:  1024 * 1024,
		SyncOnCommit: false,
	}

	log, err := NewFasterLog(cfg)
	if err != nil {
		t.Fatalf("Failed to create log: %v", err)
	}
	defer log.Close()

	// Accept an entry
	slot := uint64(100)
	ballot := Ballot{ID: 5, NodeID: 1}
	value := []byte("test value")

	err = log.Accept(slot, ballot, value)
	if err != nil {
		t.Fatalf("Failed to accept entry: %v", err)
	}

	// Read it back
	entry, err := log.Read(slot)
	if err != nil {
		t.Fatalf("Failed to read entry: %v", err)
	}

	// Verify
	if entry.Slot != slot {
		t.Errorf("Slot mismatch: got %d, want %d", entry.Slot, slot)
	}
	if entry.Ballot.ID != ballot.ID || entry.Ballot.NodeID != ballot.NodeID {
		t.Errorf("Ballot mismatch: got %+v, want %+v", entry.Ballot, ballot)
	}
	if string(entry.Value) != string(value) {
		t.Errorf("Value mismatch: got %s, want %s", entry.Value, value)
	}
	if entry.Committed {
		t.Errorf("Entry should not be committed yet")
	}
}

func TestCommit(t *testing.T) {
	dir := t.TempDir()
	logPath := filepath.Join(dir, "test.log")

	cfg := Config{
		Path:         logPath,
		MutableSize:  1024 * 1024,
		SyncOnCommit: true,
	}

	log, err := NewFasterLog(cfg)
	if err != nil {
		t.Fatalf("Failed to create log: %v", err)
	}
	defer log.Close()

	// Accept an entry
	slot := uint64(200)
	ballot := Ballot{ID: 10, NodeID: 2}
	value := []byte("committed value")

	err = log.Accept(slot, ballot, value)
	if err != nil {
		t.Fatalf("Failed to accept entry: %v", err)
	}

	// Commit it
	err = log.Commit(slot)
	if err != nil {
		t.Fatalf("Failed to commit entry: %v", err)
	}

	// Read it back
	entry, err := log.Read(slot)
	if err != nil {
		t.Fatalf("Failed to read entry: %v", err)
	}

	// Verify it's committed
	if !entry.Committed {
		t.Errorf("Entry should be committed")
	}

	// Try ReadCommittedOnly
	entry2, err := log.ReadCommittedOnly(slot)
	if err != nil {
		t.Fatalf("ReadCommittedOnly failed: %v", err)
	}
	if !entry2.Committed {
		t.Errorf("ReadCommittedOnly returned uncommitted entry")
	}
}

func TestReadCommittedOnly_Uncommitted(t *testing.T) {
	dir := t.TempDir()
	logPath := filepath.Join(dir, "test.log")

	cfg := Config{
		Path:         logPath,
		MutableSize:  1024 * 1024,
		SyncOnCommit: false,
	}

	log, err := NewFasterLog(cfg)
	if err != nil {
		t.Fatalf("Failed to create log: %v", err)
	}
	defer log.Close()

	// Accept but don't commit
	slot := uint64(300)
	ballot := Ballot{ID: 15, NodeID: 3}
	value := []byte("uncommitted")

	err = log.Accept(slot, ballot, value)
	if err != nil {
		t.Fatalf("Failed to accept entry: %v", err)
	}

	// Try ReadCommittedOnly - should fail
	_, err = log.ReadCommittedOnly(slot)
	if err != ErrNotCommitted {
		t.Errorf("Expected ErrNotCommitted, got %v", err)
	}
}

func TestScanUncommitted(t *testing.T) {
	dir := t.TempDir()
	logPath := filepath.Join(dir, "test.log")

	cfg := Config{
		Path:         logPath,
		MutableSize:  1024 * 1024,
		SyncOnCommit: false,
	}

	log, err := NewFasterLog(cfg)
	if err != nil {
		t.Fatalf("Failed to create log: %v", err)
	}
	defer log.Close()

	// Accept several entries
	slots := []uint64{400, 401, 402, 403}
	for i, slot := range slots {
		err = log.Accept(slot, Ballot{ID: uint64(i), NodeID: 1}, []byte("value"))
		if err != nil {
			t.Fatalf("Failed to accept slot %d: %v", slot, err)
		}
	}

	// Commit some of them
	err = log.Commit(400)
	if err != nil {
		t.Fatalf("Failed to commit slot 400: %v", err)
	}
	err = log.Commit(402)
	if err != nil {
		t.Fatalf("Failed to commit slot 402: %v", err)
	}

	// Scan uncommitted
	uncommitted, err := log.ScanUncommitted()
	if err != nil {
		t.Fatalf("Failed to scan uncommitted: %v", err)
	}

	// Should have 401 and 403
	uncommittedSlots := make(map[uint64]bool)
	for _, entry := range uncommitted {
		uncommittedSlots[entry.Slot] = true
	}

	if !uncommittedSlots[401] || !uncommittedSlots[403] {
		t.Errorf("Expected slots 401 and 403 to be uncommitted, got %v", uncommittedSlots)
	}
	if uncommittedSlots[400] || uncommittedSlots[402] {
		t.Errorf("Slots 400 and 402 should be committed, got %v", uncommittedSlots)
	}
}

func TestMultipleEntries(t *testing.T) {
	dir := t.TempDir()
	logPath := filepath.Join(dir, "test.log")

	cfg := Config{
		Path:         logPath,
		MutableSize:  1024 * 1024,
		SyncOnCommit: false,
	}

	log, err := NewFasterLog(cfg)
	if err != nil {
		t.Fatalf("Failed to create log: %v", err)
	}
	defer log.Close()

	// Accept and commit many entries
	numEntries := 1000
	for i := range numEntries {
		slot := uint64(i)
		ballot := Ballot{ID: uint64(i / 10), NodeID: uint64(i % 5)}
		value := []byte{byte(i % 256)}

		err = log.Accept(slot, ballot, value)
		if err != nil {
			t.Fatalf("Failed to accept slot %d: %v", slot, err)
		}

		err = log.Commit(slot)
		if err != nil {
			t.Fatalf("Failed to commit slot %d: %v", slot, err)
		}
	}

	// Verify all entries
	for i := range numEntries {
		slot := uint64(i)
		entry, err := log.ReadCommittedOnly(slot)
		if err != nil {
			t.Fatalf("Failed to read slot %d: %v", slot, err)
		}

		if entry.Slot != slot {
			t.Errorf("Slot %d: expected slot %d, got %d", i, slot, entry.Slot)
		}
		if entry.Value[0] != byte(i%256) {
			t.Errorf("Slot %d: expected value %d, got %d", i, byte(i%256), entry.Value[0])
		}
	}
}

func TestPersistence(t *testing.T) {
	dir := t.TempDir()
	logPath := filepath.Join(dir, "test.log")

	cfg := Config{
		Path:         logPath,
		MutableSize:  1024 * 1024,
		SyncOnCommit: true, // Important for persistence
	}

	// Create log and write entries
	log1, err := NewFasterLog(cfg)
	if err != nil {
		t.Fatalf("Failed to create log: %v", err)
	}

	for i := range 100 {
		slot := uint64(i)
		err = log1.Accept(slot, Ballot{ID: 1, NodeID: 1}, []byte{byte(i)})
		if err != nil {
			t.Fatalf("Failed to accept slot %d: %v", slot, err)
		}
		err = log1.Commit(slot)
		if err != nil {
			t.Fatalf("Failed to commit slot %d: %v", slot, err)
		}
	}

	log1.Close()

	// Reopen log
	log2, err := NewFasterLog(cfg)
	if err != nil {
		t.Fatalf("Failed to reopen log: %v", err)
	}
	defer log2.Close()

	// Verify all entries are still there
	for i := range 100 {
		slot := uint64(i)
		entry, err := log2.ReadCommittedOnly(slot)
		if err != nil {
			t.Fatalf("Failed to read slot %d after reopen: %v", slot, err)
		}
		if entry.Value[0] != byte(i) {
			t.Errorf("Slot %d: expected value %d, got %d", i, byte(i), entry.Value[0])
		}
	}
}

func TestCheckpoint(t *testing.T) {
	dir := t.TempDir()
	logPath := filepath.Join(dir, "test.log")

	cfg := Config{
		Path:         logPath,
		MutableSize:  1024 * 1024,
		SyncOnCommit: false,
	}

	log, err := NewFasterLog(cfg)
	if err != nil {
		t.Fatalf("Failed to create log: %v", err)
	}
	defer log.Close()

	// Accept several entries
	for i := range 10 {
		slot := uint64(i)
		err = log.Accept(slot, Ballot{ID: 1, NodeID: 1}, []byte{byte(i)})
		if err != nil {
			t.Fatalf("Failed to accept slot %d: %v", slot, err)
		}
	}

	// Commit some
	for i := range 5 {
		err = log.Commit(uint64(i))
		if err != nil {
			t.Fatalf("Failed to commit slot %d: %v", i, err)
		}
	}

	// Checkpoint (should flush committed entries to tail)
	err = log.Checkpoint()
	if err != nil {
		t.Fatalf("Failed to checkpoint: %v", err)
	}

	// Verify committed entries are still readable
	for i := range 5 {
		entry, err := log.ReadCommittedOnly(uint64(i))
		if err != nil {
			t.Fatalf("Failed to read slot %d after checkpoint: %v", i, err)
		}
		if entry.Value[0] != byte(i) {
			t.Errorf("Slot %d: expected value %d, got %d", i, byte(i), entry.Value[0])
		}
	}

	// Uncommitted entries should still be present
	uncommitted, err := log.ScanUncommitted()
	if err != nil {
		t.Fatalf("Failed to scan uncommitted: %v", err)
	}
	if len(uncommitted) != 5 {
		t.Errorf("Expected 5 uncommitted entries, got %d", len(uncommitted))
	}
}

func TestSlotNotFound(t *testing.T) {
	dir := t.TempDir()
	logPath := filepath.Join(dir, "test.log")

	cfg := Config{
		Path:         logPath,
		MutableSize:  1024 * 1024,
		SyncOnCommit: false,
	}

	log, err := NewFasterLog(cfg)
	if err != nil {
		t.Fatalf("Failed to create log: %v", err)
	}
	defer log.Close()

	// Try to read non-existent slot
	_, err = log.Read(999)
	if err != ErrSlotNotFound {
		t.Errorf("Expected ErrSlotNotFound, got %v", err)
	}

	// Try to commit non-existent slot
	err = log.Commit(999)
	if err != ErrSlotNotFound {
		t.Errorf("Expected ErrSlotNotFound, got %v", err)
	}
}

func TestEntryTooLarge(t *testing.T) {
	dir := t.TempDir()
	logPath := filepath.Join(dir, "test.log")

	cfg := Config{
		Path:         logPath,
		MutableSize:  1024 * 1024, // 1MB mutable buffer
		SyncOnCommit: false,
	}

	log, err := NewFasterLog(cfg)
	if err != nil {
		t.Fatalf("Failed to create log: %v", err)
	}
	defer log.Close()

	// Try to accept an entry that exceeds maxEntryValueSize (64MB)
	hugeValue := make([]byte, 65*1024*1024) // 65MB
	err = log.Accept(1, Ballot{ID: 1, NodeID: 1}, hugeValue)
	if !errors.Is(err, ErrEntryTooLarge) {
		t.Errorf("Expected ErrEntryTooLarge for 65MB value, got %v", err)
	}

	// Verify that entries at the limit still work
	// Note: This would require a much larger mutable buffer in practice
	// so we just verify the error check works
	t.Log("Entry size limit check works correctly")
}
