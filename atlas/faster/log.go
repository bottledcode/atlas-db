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
	"os"
	"sync"
	"sync/atomic"
	"syscall"
)

// FasterLog implements a FASTER-style hybrid log optimised for consensus protocols
// It has three regions:
// 1. In-memory index (hash map): slot -> offset lookup
// 2. Mutable region (ring buffer): recent uncommitted entries (LOCK-FREE!)
// 3. Immutable tail (mmap file): committed entries, append-only
//
// LOCK-FREE DESIGN:
// - a Mutable region uses atomic CAS for allocation (no locks!)
// - Reads scan buffer sequentially (fast for 64MB region)
// - Epochs protect against use-after-free during concurrent access
// - Only tail writes use a mutex (sequential disk writes)
type FasterLog struct {
	// Configuration
	cfg Config

	// In-memory index: slot -> offset
	// offset has mutableFlag bit set if entry is in mutable region
	// otherwise it's an offset into the immutable tail
	index sync.Map

	// Mutable region for uncommitted entries (LOCK-FREE!)
	mutableBuffer *RingBuffer

	// Immutable tail (committed entries)
	tailFile *os.File
	tailMmap []byte
	tailSize atomic.Uint64
	tailMu   sync.Mutex // Only lock for tail writes (sequential disk I/O)

	// Epoch-based memory management for safe concurrent reads
	epoch        atomic.Uint64
	threadEpochs []atomic.Uint64

	// Closed flag
	closed atomic.Bool
}

// NewFasterLog creates a new FASTER-style log
func NewFasterLog(cfg Config) (*FasterLog, error) {
	// Validate config
	if cfg.MutableSize == 0 {
		cfg.MutableSize = 64 * 1024 * 1024 // 64MB default
	}
	if cfg.SegmentSize == 0 {
		cfg.SegmentSize = 1024 * 1024 * 1024 // 1GB default
	}
	if cfg.NumThreads == 0 {
		cfg.NumThreads = 128 // Default to 128 goroutines
	}

	log := &FasterLog{
		cfg:           cfg,
		mutableBuffer: NewRingBuffer(cfg.MutableSize),
		threadEpochs:  make([]atomic.Uint64, cfg.NumThreads),
	}

	// Open or create the tail file
	file, err := os.OpenFile(
		cfg.Path,
		os.O_RDWR|os.O_CREATE,
		0644,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to open log file: %w", err)
	}
	log.tailFile = file

	// Get current file size
	stat, err := file.Stat()
	if err != nil {
		file.Close()
		return nil, fmt.Errorf("failed to stat log file: %w", err)
	}

	// If file exists, memory-map it
	if stat.Size() > 0 {
		err = log.mmapTail(uint64(stat.Size()))
		if err != nil {
			file.Close()
			return nil, fmt.Errorf("failed to mmap log file: %w", err)
		}
		log.tailSize.Store(uint64(stat.Size()))

		// Rebuild index from tail
		err = log.rebuildIndex()
		if err != nil {
			log.unmapTail()
			file.Close()
			return nil, fmt.Errorf("failed to rebuild index: %w", err)
		}
	}

	return log, nil
}

// Accept writes an accepted (uncommitted) entry to the mutable region
// This corresponds to WPaxos Phase-2b
func (l *FasterLog) Accept(slot uint64, ballot Ballot, value []byte) error {
	if l.closed.Load() {
		return ErrClosed
	}

	// Enter epoch for safe memory access
	threadID := int(slot % uint64(len(l.threadEpochs)))
	currentEpoch := l.epoch.Load()
	l.threadEpochs[threadID].Store(currentEpoch)
	defer l.threadEpochs[threadID].Store(0)

	entry := &LogEntry{
		Slot:      slot,
		Ballot:    ballot,
		Value:     value,
		Committed: false,
	}

	// Write to mutable region (LOCK-FREE with atomic CAS!)
	offset, err := l.mutableBuffer.Append(entry)
	if err != nil {
		return fmt.Errorf("failed to append to mutable buffer: %w", err)
	}

	// Update index atomically (set mutable flag)
	l.index.Store(slot, offset|mutableFlag)

	return nil
}

// Commit marks an entry as committed and flushes it to the immutable tail
// This corresponds to WPaxos Phase-3
// Following FASTER's RCU principle: only committed entries go to the immutable tail.
// The tail is truly immutable - we never modify existing entries in place.
func (l *FasterLog) Commit(slot uint64) error {
	if l.closed.Load() {
		return ErrClosed
	}

	// Look up the entry
	offsetVal, ok := l.index.Load(slot)
	if !ok {
		return ErrSlotNotFound
	}

	offset := offsetVal.(uint64)

	// Check if already in tail (already committed)
	if (offset & mutableFlag) == 0 {
		// Already in tail means already committed
		// FASTER principle: tail is immutable, no updates needed
		return nil
	}

	// Entry is in mutable region, need to flush to tail
	// Read from mutable buffer (LOCK-FREE!)
	entry, err := l.mutableBuffer.Read(offset &^ mutableFlag)
	if err != nil {
		return fmt.Errorf("failed to read from mutable buffer: %w", err)
	}

	// Mark as committed before appending to tail
	// This ensures only committed entries exist in the immutable region
	entry.Committed = true

	// Append to immutable tail (append-only, never modify existing entries)
	tailOffset, err := l.appendToTail(entry)
	if err != nil {
		return fmt.Errorf("failed to append to tail: %w", err)
	}

	// Update index to point to tail (RCU: atomic pointer update)
	l.index.Store(slot, tailOffset)

	// Try to reclaim space in mutable buffer by advancing tail
	// This is critical to prevent ErrBufferFull under load!
	indexCheck := func(checkSlot uint64) bool {
		offsetVal, ok := l.index.Load(checkSlot)
		if !ok {
			return false
		}
		offset := offsetVal.(uint64)
		// Return true if entry is still in mutable region
		return (offset & mutableFlag) != 0
	}

	// Advance tail to reclaim space (returns bytes reclaimed)
	_ = l.mutableBuffer.TryAdvanceTail(indexCheck)

	return nil
}

// Read reads an entry by slot number
func (l *FasterLog) Read(slot uint64) (*LogEntry, error) {
	if l.closed.Load() {
		return nil, ErrClosed
	}

	// Enter epoch for safe memory access
	threadID := int(slot % uint64(len(l.threadEpochs)))
	currentEpoch := l.epoch.Load()
	l.threadEpochs[threadID].Store(currentEpoch)
	defer l.threadEpochs[threadID].Store(0)

	// Look up in index
	offsetVal, ok := l.index.Load(slot)
	if !ok {
		return nil, ErrSlotNotFound
	}

	offset := offsetVal.(uint64)

	// Check if in mutable region or tail
	if (offset & mutableFlag) != 0 {
		// In mutable region - LOCK-FREE read!
		return l.mutableBuffer.Read(offset &^ mutableFlag)
	}

	// In tail - lock-free read from mmap
	return l.readFromTail(offset)
}

// ReadCommittedOnly reads an entry only if it's committed
// This is what the state machine should use
func (l *FasterLog) ReadCommittedOnly(slot uint64) (*LogEntry, error) {
	entry, err := l.Read(slot)
	if err != nil {
		return nil, err
	}

	if !entry.Committed {
		return nil, ErrNotCommitted
	}

	return entry, nil
}

// ScanUncommitted returns all uncommitted entries
// This is used for WPaxos Phase-1 recovery
func (l *FasterLog) ScanUncommitted() ([]*LogEntry, error) {
	if l.closed.Load() {
		return nil, ErrClosed
	}

	// Enter epoch for safe scanning
	threadID := 0 // Use thread 0 for scans
	currentEpoch := l.epoch.Load()
	l.threadEpochs[threadID].Store(currentEpoch)
	defer l.threadEpochs[threadID].Store(0)

	// Create index checker to filter out entries that have been moved to tail
	indexCheck := func(slot uint64) bool {
		offsetVal, ok := l.index.Load(slot)
		if !ok {
			return false // Entry not in index, skip it
		}
		offset := offsetVal.(uint64)
		// Return true only if entry is still in mutable region
		return (offset & mutableFlag) != 0
	}

	return l.mutableBuffer.GetAllUncommittedWithIndex(indexCheck)
}

// appendToTail appends an entry to the immutable tail
func (l *FasterLog) appendToTail(entry *LogEntry) (uint64, error) {
	l.tailMu.Lock()
	defer l.tailMu.Unlock()

	// Serialize entry
	data, err := serializeEntry(entry)
	if err != nil {
		return 0, err
	}

	currentSize := l.tailSize.Load()
	newSize := currentSize + uint64(len(data))

	// Check if we need to grow the mmap
	if newSize > uint64(len(l.tailMmap)) {
		err = l.growTail(newSize)
		if err != nil {
			return 0, err
		}
	}

	// Write to file
	n, err := l.tailFile.WriteAt(data, int64(currentSize))
	if err != nil {
		return 0, fmt.Errorf("failed to write to tail: %w", err)
	}
	if n != len(data) {
		return 0, fmt.Errorf("incomplete write: %d != %d", n, len(data))
	}

	// Sync to disk if configured
	if l.cfg.SyncOnCommit {
		err = l.tailFile.Sync()
		if err != nil {
			return 0, fmt.Errorf("failed to sync tail: %w", err)
		}
	}

	// Update size
	l.tailSize.Store(newSize)

	return currentSize, nil
}

// readFromTail reads an entry from the immutable tail (lock-free)
func (l *FasterLog) readFromTail(offset uint64) (*LogEntry, error) {
	tailSize := l.tailSize.Load()
	if offset >= tailSize {
		return nil, ErrInvalidOffset
	}

	if offset+entryHeaderSize > tailSize {
		return nil, ErrCorruptedEntry
	}

	// Read directly from mmap (no locks!)
	data := l.tailMmap[offset:]
	return deserializeEntry(data)
}

// growTail grows the tail file and remaps it
func (l *FasterLog) growTail(newSize uint64) error {
	// Round up to segment size
	roundedSize := ((newSize + l.cfg.SegmentSize - 1) / l.cfg.SegmentSize) * l.cfg.SegmentSize

	// Unmap current mmap
	if len(l.tailMmap) > 0 {
		err := l.unmapTail()
		if err != nil {
			return err
		}
	}

	// Truncate file to new size
	err := l.tailFile.Truncate(int64(roundedSize))
	if err != nil {
		return fmt.Errorf("failed to truncate tail: %w", err)
	}

	// Remap
	err = l.mmapTail(roundedSize)
	if err != nil {
		return err
	}

	return nil
}

// mmapTail memory-maps the tail file
func (l *FasterLog) mmapTail(size uint64) error {
	mmap, err := syscall.Mmap(
		int(l.tailFile.Fd()),
		0,
		int(size),
		syscall.PROT_READ|syscall.PROT_WRITE,
		syscall.MAP_SHARED,
	)
	if err != nil {
		return fmt.Errorf("failed to mmap: %w", err)
	}
	l.tailMmap = mmap
	return nil
}

// unmapTail unmaps the tail
func (l *FasterLog) unmapTail() error {
	if len(l.tailMmap) == 0 {
		return nil
	}
	err := syscall.Munmap(l.tailMmap)
	if err != nil {
		return fmt.Errorf("failed to munmap: %w", err)
	}
	l.tailMmap = nil
	return nil
}

// rebuildIndex scans the tail and rebuilds the in-memory index
func (l *FasterLog) rebuildIndex() error {
	offset := uint64(0)
	tailSize := l.tailSize.Load()

	for offset < tailSize {
		// Read entry header to get size
		if offset+entryHeaderSize > tailSize {
			// Incomplete entry at end, truncate
			break
		}

		headerData := l.tailMmap[offset : offset+entryHeaderSize]
		slot := binary.LittleEndian.Uint64(headerData[0:8])
		valueLen := binary.LittleEndian.Uint32(headerData[24:28])

		entrySize := uint64(entryHeaderSize) + uint64(valueLen) + uint64(checksumSize)
		if offset+entrySize > tailSize {
			// Incomplete entry, truncate
			break
		}

		// Validate entry
		_, err := deserializeEntry(l.tailMmap[offset:])
		if err != nil {
			// Corrupted entry, stop here
			break
		}

		// Add to index (use slot from header, not from deserialized entry)
		l.index.Store(slot, offset)

		offset += entrySize
	}

	// If we stopped early, truncate the tail
	if offset < tailSize {
		l.tailSize.Store(offset)
	}

	return nil
}

// Checkpoint flushes all committed entries from mutable region to tail
// LOCK-FREE: Scans and drains without locks
func (l *FasterLog) Checkpoint() error {
	if l.closed.Load() {
		return ErrClosed
	}

	// Enter epoch for safe scanning
	threadID := 0
	currentEpoch := l.epoch.Load()
	l.threadEpochs[threadID].Store(currentEpoch)
	defer l.threadEpochs[threadID].Store(0)

	// Drain committed entries (lock-free scan)
	committed, err := l.mutableBuffer.DrainCommitted()
	if err != nil {
		return fmt.Errorf("failed to drain committed entries: %w", err)
	}

	// Flush to tail
	for _, entry := range committed {
		tailOffset, err := l.appendToTail(entry)
		if err != nil {
			return fmt.Errorf("failed to flush entry %d: %w", entry.Slot, err)
		}

		// Update index
		l.index.Store(entry.Slot, tailOffset)
	}

	// Only reset buffer if there are no more uncommitted entries
	// Check this AFTER flushing committed entries
	uncommitted, err := l.mutableBuffer.GetAllUncommittedWithIndex(func(slot uint64) bool {
		offsetVal, ok := l.index.Load(slot)
		if !ok {
			return false
		}
		offset := offsetVal.(uint64)
		return (offset & mutableFlag) != 0
	})
	if err == nil && len(uncommitted) == 0 {
		// Safe to reset - all entries have been committed and flushed
		l.mutableBuffer.Reset()
	}

	return nil
}

// Close closes the log
func (l *FasterLog) Close() error {
	if !l.closed.CompareAndSwap(false, true) {
		return ErrClosed
	}

	// Checkpoint any remaining committed entries
	_ = l.Checkpoint()

	// Unmap tail
	err := l.unmapTail()
	if err != nil {
		return err
	}

	// Close file
	return l.tailFile.Close()
}
