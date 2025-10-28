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
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

// Snapshot represents a state machine checkpoint at a specific slot
// It captures the reconstructed state at that point in the log
type Snapshot struct {
	// Slot is the last committed slot included in this snapshot
	Slot uint64

	// Data is the serialized state machine state
	// For WPaxos, this would be the reconstructed Record for each key
	Data []byte

	// Metadata for the snapshot
	NumEntries uint64 // Number of log entries represented
	Checksum   uint32 // CRC32 checksum of Data
}

// SnapshotManager handles snapshot creation, storage, and recovery
type SnapshotManager struct {
	dir string // Directory for snapshot files
	log *FasterLog
}

// NewSnapshotManager creates a new snapshot manager
func NewSnapshotManager(snapshotDir string, log *FasterLog) (*SnapshotManager, error) {
	// Create snapshot directory if it doesn't exist
	err := os.MkdirAll(snapshotDir, 0755)
	if err != nil {
		return nil, fmt.Errorf("failed to create snapshot dir: %w", err)
	}

	return &SnapshotManager{
		dir: snapshotDir,
		log: log,
	}, nil
}

// CreateSnapshot creates a snapshot at the given slot
// The caller provides the serialized state machine data
func (sm *SnapshotManager) CreateSnapshot(slot uint64, stateData []byte) error {
	// Calculate checksum
	checksum := crc32.ChecksumIEEE(stateData)

	snapshot := &Snapshot{
		Slot:     slot,
		Data:     stateData,
		Checksum: checksum,
	}

	// Write snapshot to disk
	err := sm.writeSnapshot(snapshot)
	if err != nil {
		return fmt.Errorf("failed to write snapshot: %w", err)
	}

	return nil
}

// GetLatestSnapshot returns the most recent snapshot
func (sm *SnapshotManager) GetLatestSnapshot() (*Snapshot, error) {
	snapshots, err := sm.listSnapshots()
	if err != nil {
		return nil, err
	}

	if len(snapshots) == 0 {
		return nil, ErrNoSnapshot
	}

	// Return the last (highest slot) snapshot
	latestPath := snapshots[len(snapshots)-1]
	return sm.readSnapshot(latestPath)
}

// GetSnapshot returns the snapshot for a specific slot (or nearest before it)
func (sm *SnapshotManager) GetSnapshot(slot uint64) (*Snapshot, error) {
	snapshots, err := sm.listSnapshots()
	if err != nil {
		return nil, err
	}

	if len(snapshots) == 0 {
		return nil, ErrNoSnapshot
	}

	// Find the snapshot with the highest slot ≤ requested slot
	for i := len(snapshots) - 1; i >= 0; i-- {
		snap, err := sm.readSnapshot(snapshots[i])
		if err != nil {
			continue
		}

		if snap.Slot <= slot {
			return snap, nil
		}
	}

	return nil, ErrNoSnapshot
}

// TruncateLog marks a truncation point in metadata
// The actual compaction happens offline when the log is reopened
// IMPORTANT: This does NOT immediately free disk space!
// Call CompactOnStartup() during recovery to perform actual truncation
func (sm *SnapshotManager) TruncateLog(truncateSlot uint64) error {
	// Just record the truncation point - actual compaction is offline
	// We'll create a marker file that recovery can use
	markerPath := filepath.Join(sm.dir, "truncate_marker")

	file, err := os.OpenFile(markerPath, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		return fmt.Errorf("failed to create truncate marker: %w", err)
	}
	defer file.Close()

	// Write truncation slot to marker file
	buf := make([]byte, 8)
	binary.LittleEndian.PutUint64(buf, truncateSlot)
	_, err = file.Write(buf)
	if err != nil {
		return fmt.Errorf("failed to write truncate marker: %w", err)
	}

	return file.Sync()
}

// CleanupOldSnapshots removes all snapshots older than keepCount
func (sm *SnapshotManager) CleanupOldSnapshots(keepCount int) error {
	snapshots, err := sm.listSnapshots()
	if err != nil {
		return err
	}

	if len(snapshots) <= keepCount {
		return nil // Nothing to clean up
	}

	// Remove oldest snapshots
	toRemove := snapshots[:len(snapshots)-keepCount]
	for _, path := range toRemove {
		err := os.Remove(path)
		if err != nil {
			return fmt.Errorf("failed to remove snapshot %s: %w", path, err)
		}
	}

	return nil
}


// CompactOnStartup performs offline log compaction before the log is opened
// This MUST be called before NewFasterLog() if you want to apply pending truncations
// Returns the truncation slot if compaction was performed, 0 if not
func CompactOnStartup(logPath string, snapshotDir string) (uint64, error) {
	// Check for truncation marker
	markerPath := filepath.Join(snapshotDir, "truncate_marker")
	markerData, err := os.ReadFile(markerPath)
	if err != nil {
		if os.IsNotExist(err) {
			return 0, nil // No pending truncation
		}
		return 0, fmt.Errorf("failed to read truncate marker: %w", err)
	}

	if len(markerData) < 8 {
		return 0, fmt.Errorf("invalid truncate marker")
	}

	truncateSlot := binary.LittleEndian.Uint64(markerData)

	// Perform offline compaction
	err = compactLogOffline(logPath, truncateSlot)
	if err != nil {
		return 0, fmt.Errorf("failed to compact log: %w", err)
	}

	// Remove marker file after successful compaction
	err = os.Remove(markerPath)
	if err != nil {
		return 0, fmt.Errorf("failed to remove truncate marker: %w", err)
	}

	return truncateSlot, nil
}

// compactLogOffline performs actual log truncation while log is closed
// Uses streaming I/O with bounded memory (4MB buffer) to handle multi-GB logs
func compactLogOffline(logPath string, truncateSlot uint64) error {
	// Open log file for reading
	file, err := os.Open(logPath)
	if err != nil {
		if os.IsNotExist(err) {
			// Log file doesn't exist - this is an error condition if we're trying to compact
			// The truncation marker should remain so compaction can be retried later
			return fmt.Errorf("log file does not exist: %w", err)
		}
		return fmt.Errorf("failed to open log file: %w", err)
	}
	defer file.Close()

	stat, err := file.Stat()
	if err != nil {
		return fmt.Errorf("failed to stat log file: %w", err)
	}

	if stat.Size() == 0 {
		// Empty log file - this is also an error if we're trying to compact
		// The truncation marker indicates there should be data to compact
		return fmt.Errorf("log file is empty, cannot compact to slot %d", truncateSlot)
	}

	// Find truncation offset by scanning the file
	truncateOffset, err := findTruncateOffsetOffline(file, truncateSlot)
	if err != nil {
		return fmt.Errorf("failed to find truncate offset: %w", err)
	}

	if truncateOffset == 0 {
		return nil // Nothing to truncate
	}

	if truncateOffset >= uint64(stat.Size()) {
		// Truncating entire file - just truncate to empty
		file.Close()
		return os.WriteFile(logPath, nil, 0644)
	}

	// Stream copy to temporary file (bounded memory usage)
	tempPath := logPath + ".compact.tmp"
	err = streamCopyFromOffset(file, tempPath, truncateOffset)
	if err != nil {
		os.Remove(tempPath) // Clean up on error
		return fmt.Errorf("failed to stream copy: %w", err)
	}

	file.Close() // Close source before rename

	// Atomic rename to replace original
	err = os.Rename(tempPath, logPath)
	if err != nil {
		os.Remove(tempPath) // Clean up on error
		return fmt.Errorf("failed to rename compacted log: %w", err)
	}

	return nil
}

// streamCopyFromOffset copies data from source starting at offset to dest file
// Uses 4MB buffer for bounded memory usage (handles multi-GB files safely)
func streamCopyFromOffset(source *os.File, destPath string, offset uint64) error {
	// Seek to truncation point
	_, err := source.Seek(int64(offset), io.SeekStart)
	if err != nil {
		return fmt.Errorf("failed to seek to offset: %w", err)
	}

	// Create temporary destination file
	dest, err := os.OpenFile(destPath, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		return fmt.Errorf("failed to create temp file: %w", err)
	}
	defer dest.Close()

	// Stream copy with 4MB buffer (constant memory usage)
	const bufferSize = 4 * 1024 * 1024 // 4MB
	buffer := make([]byte, bufferSize)

	for {
		n, err := source.Read(buffer)
		if n > 0 {
			_, writeErr := dest.Write(buffer[:n])
			if writeErr != nil {
				return fmt.Errorf("failed to write to temp file: %w", writeErr)
			}
		}

		if err == io.EOF {
			break
		}
		if err != nil {
			return fmt.Errorf("failed to read from source: %w", err)
		}
	}

	// Sync to ensure data is on disk before rename
	err = dest.Sync()
	if err != nil {
		return fmt.Errorf("failed to sync temp file: %w", err)
	}

	return nil
}

// findTruncateOffsetOffline scans a log file to find where to truncate
func findTruncateOffsetOffline(file *os.File, truncateSlot uint64) (uint64, error) {
	var offset uint64 = 0
	var lastSlotOffset uint64 = 0

	// Buffer for reading entries
	headerBuf := make([]byte, entryHeaderSize)

	for {
		// Read entry header
		n, err := file.Read(headerBuf)
		if err == io.EOF {
			break
		}
		if err != nil {
			return 0, fmt.Errorf("failed to read entry header at offset %d: %w", offset, err)
		}
		if n < entryHeaderSize {
			break // Incomplete entry
		}

		// Parse header
		slot := binary.LittleEndian.Uint64(headerBuf[0:8])
		valueLen := binary.LittleEndian.Uint32(headerBuf[24:28])

		entrySize := entryHeaderSize + int(valueLen) + checksumSize

		if slot <= truncateSlot {
			// This entry should be truncated
			lastSlotOffset = offset + uint64(entrySize)
		} else {
			// We've passed truncateSlot, stop here
			break
		}

		// Skip value and checksum
		_, err = file.Seek(int64(valueLen)+checksumSize, io.SeekCurrent)
		if err != nil {
			return 0, fmt.Errorf("failed to seek past entry: %w", err)
		}

		offset += uint64(entrySize)
	}

	return lastSlotOffset, nil
}

// writeSnapshot writes a snapshot to disk
func (sm *SnapshotManager) writeSnapshot(snapshot *Snapshot) error {
	// Snapshot file format:
	// [slot:8][numEntries:8][dataLen:8][data:N][checksum:4]

	path := sm.snapshotPath(snapshot.Slot)
	file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		return fmt.Errorf("failed to create snapshot file: %w", err)
	}
	defer file.Close()

	// Write header
	header := make([]byte, 24)
	binary.LittleEndian.PutUint64(header[0:8], snapshot.Slot)
	binary.LittleEndian.PutUint64(header[8:16], snapshot.NumEntries)
	binary.LittleEndian.PutUint64(header[16:24], uint64(len(snapshot.Data)))

	_, err = file.Write(header)
	if err != nil {
		return fmt.Errorf("failed to write snapshot header: %w", err)
	}

	// Write data
	_, err = file.Write(snapshot.Data)
	if err != nil {
		return fmt.Errorf("failed to write snapshot data: %w", err)
	}

	// Write checksum
	checksumBuf := make([]byte, 4)
	binary.LittleEndian.PutUint32(checksumBuf, snapshot.Checksum)
	_, err = file.Write(checksumBuf)
	if err != nil {
		return fmt.Errorf("failed to write snapshot checksum: %w", err)
	}

	// Sync to disk
	err = file.Sync()
	if err != nil {
		return fmt.Errorf("failed to sync snapshot: %w", err)
	}

	return nil
}

// readSnapshot reads a snapshot from disk
func (sm *SnapshotManager) readSnapshot(path string) (*Snapshot, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("failed to open snapshot: %w", err)
	}
	defer file.Close()

	// Read header
	header := make([]byte, 24)
	_, err = io.ReadFull(file, header)
	if err != nil {
		return nil, fmt.Errorf("failed to read snapshot header: %w", err)
	}

	slot := binary.LittleEndian.Uint64(header[0:8])
	numEntries := binary.LittleEndian.Uint64(header[8:16])
	dataLen := binary.LittleEndian.Uint64(header[16:24])

	// Read data
	data := make([]byte, dataLen)
	_, err = io.ReadFull(file, data)
	if err != nil {
		return nil, fmt.Errorf("failed to read snapshot data: %w", err)
	}

	// Read checksum
	checksumBuf := make([]byte, 4)
	_, err = io.ReadFull(file, checksumBuf)
	if err != nil {
		return nil, fmt.Errorf("failed to read snapshot checksum: %w", err)
	}
	storedChecksum := binary.LittleEndian.Uint32(checksumBuf)

	// Verify checksum
	calculatedChecksum := crc32.ChecksumIEEE(data)
	if storedChecksum != calculatedChecksum {
		return nil, ErrCorruptedSnapshot
	}

	return &Snapshot{
		Slot:       slot,
		NumEntries: numEntries,
		Data:       data,
		Checksum:   storedChecksum,
	}, nil
}

// listSnapshots returns all snapshot file paths sorted by slot
func (sm *SnapshotManager) listSnapshots() ([]string, error) {
	entries, err := os.ReadDir(sm.dir)
	if err != nil {
		return nil, fmt.Errorf("failed to read snapshot dir: %w", err)
	}

	var snapshots []string
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}

		name := entry.Name()
		if strings.HasPrefix(name, "snapshot_") {
			snapshots = append(snapshots, filepath.Join(sm.dir, name))
		}
	}

	// Sort by slot number
	sort.Strings(snapshots)

	return snapshots, nil
}

// snapshotPath returns the file path for a snapshot at the given slot
func (sm *SnapshotManager) snapshotPath(slot uint64) string {
	return filepath.Join(sm.dir, fmt.Sprintf("snapshot_%020d", slot))
}
