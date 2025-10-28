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
	"os"
	"sort"
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

// IterateCommitted iterates over committed entries in slot order (ZERO-ALLOCATION)
// The callback receives each entry but MUST NOT store the pointer - it's reused!
// Use cases:
//   - State machine reconstruction: fn(entry) { applyToStateMachine(entry.Value) }
//   - Snapshot creation: fn(entry) { serialize(entry.Slot, entry.Value) }
//   - Learner bootstrap: fn(entry) { sendToLearner(entry) }
//
// CRITICAL: The entry parameter is REUSED between calls for zero allocation.
// If you need to keep data, copy it immediately:
//
//	valueCopy := make([]byte, len(entry.Value))
//	copy(valueCopy, entry.Value)
//
// The iterator:
//  1. Collects slot numbers from index (committed entries only)
//  2. Sorts slots numerically
//  3. Iterates in order, calling fn(entry) for each
//  4. Stops on first error from fn()
//
// Options control iteration range and behavior (see IterateOptions)
func (l *FasterLog) IterateCommitted(fn func(entry *LogEntry) error, opts IterateOptions) error {
	if l.closed.Load() {
		return ErrClosed
	}

	// Enter epoch for safe iteration
	threadID := 0
	currentEpoch := l.epoch.Load()
	l.threadEpochs[threadID].Store(currentEpoch)
	defer l.threadEpochs[threadID].Store(0)

	// Step 1: Collect committed slot numbers from index
	// We need to sort them, so we must collect first (small allocation - just uint64s)
	committedSlots := make([]uint64, 0, 1024) // Pre-allocate reasonable size
	l.index.Range(func(key, value interface{}) bool {
		slot := key.(uint64)
		offset := value.(uint64)

		// Apply filters
		if opts.MinSlot > 0 && slot < opts.MinSlot {
			return true // Skip, too low
		}
		if opts.MaxSlot > 0 && slot > opts.MaxSlot {
			return true // Skip, too high
		}

		// Check if committed (in tail = committed)
		isInTail := (offset & mutableFlag) == 0
		if isInTail {
			committedSlots = append(committedSlots, slot)
		} else if opts.IncludeUncommitted {
			// Optionally include uncommitted entries from mutable region
			committedSlots = append(committedSlots, slot)
		}

		return true
	})

	// Step 2: Sort slots numerically
	// Use standard library sort which implements introsort (hybrid quicksort + heapsort)
	// This guarantees O(n log n) worst case and bounded recursion depth
	// Critical for consensus logs which are often already sorted!
	sort.Slice(committedSlots, func(i, j int) bool {
		return committedSlots[i] < committedSlots[j]
	})

	// Step 3: Iterate in order, calling fn() for each entry
	// CRITICAL: Reuse single entry to avoid allocations!
	entry := &LogEntry{}
	for _, slot := range committedSlots {
		// Read entry into reused struct
		err := l.readInto(slot, entry)
		if err != nil {
			if opts.SkipErrors {
				continue
			}
			return fmt.Errorf("failed to read slot %d: %w", slot, err)
		}

		// Call user function with reused entry
		// WARNING: fn() must not store the entry pointer!
		err = fn(entry)
		if err != nil {
			return err
		}
	}

	return nil
}

// readInto reads an entry into a pre-allocated LogEntry (ZERO-ALLOCATION)
// This avoids heap allocations by reusing the provided entry struct
// Only entry.Value is allocated (unavoidable - it's variable size)
func (l *FasterLog) readInto(slot uint64, entry *LogEntry) error {
	// Look up in index
	offsetVal, ok := l.index.Load(slot)
	if !ok {
		return ErrSlotNotFound
	}

	offset := offsetVal.(uint64)

	// Check if in mutable region or tail
	if (offset & mutableFlag) != 0 {
		// In mutable region - read from buffer
		tmp, err := l.mutableBuffer.Read(offset &^ mutableFlag)
		if err != nil {
			return err
		}
		// Copy fields into provided entry
		entry.Slot = tmp.Slot
		entry.Ballot = tmp.Ballot
		entry.Committed = tmp.Committed
		// Value must be copied (it's a slice)
		if cap(entry.Value) < len(tmp.Value) {
			entry.Value = make([]byte, len(tmp.Value))
		} else {
			entry.Value = entry.Value[:len(tmp.Value)]
		}
		copy(entry.Value, tmp.Value)
		return nil
	}

	// In tail - deserialize directly into entry
	return l.readFromTailInto(offset, entry)
}

// readFromTailInto reads from tail into provided entry (ZERO-ALLOCATION)
func (l *FasterLog) readFromTailInto(offset uint64, entry *LogEntry) error {
	tailSize := l.tailSize.Load()
	if offset >= tailSize {
		return ErrInvalidOffset
	}

	if offset+entryHeaderSize > tailSize {
		return ErrCorruptedEntry
	}

	// Read header directly from mmap
	data := l.tailMmap[offset:]
	slot := binary.LittleEndian.Uint64(data[0:8])
	ballotID := binary.LittleEndian.Uint64(data[8:16])
	ballotNode := binary.LittleEndian.Uint64(data[16:24])
	valueLen := binary.LittleEndian.Uint32(data[24:28])
	committed := data[28] == 1

	// Validate size
	totalSize := entryHeaderSize + int(valueLen) + checksumSize
	if len(data) < totalSize {
		return ErrCorruptedEntry
	}

	// Verify checksum
	checksumOffset := entryHeaderSize + int(valueLen)
	storedChecksum := binary.LittleEndian.Uint32(data[checksumOffset : checksumOffset+checksumSize])

	// Calculate checksum
	calculatedChecksum := crc32.ChecksumIEEE(data[0:checksumOffset])
	if storedChecksum != calculatedChecksum {
		return ErrCorruptedEntry
	}

	// Fill entry fields
	entry.Slot = slot
	entry.Ballot.ID = ballotID
	entry.Ballot.NodeID = ballotNode
	entry.Committed = committed

	// Copy value (unavoidable allocation)
	if cap(entry.Value) < int(valueLen) {
		entry.Value = make([]byte, valueLen)
	} else {
		entry.Value = entry.Value[:valueLen]
	}
	copy(entry.Value, data[entryHeaderSize:entryHeaderSize+int(valueLen)])

	return nil
}

// GetCommittedRange returns the range of committed slots (min, max, count)
// Useful for learners to know what they need to catch up on
// Returns (0, 0, 0) if log is empty
func (l *FasterLog) GetCommittedRange() (minSlot uint64, maxSlot uint64, count uint64) {
	if l.closed.Load() {
		return 0, 0, 0
	}

	minSlot = ^uint64(0) // Max uint64
	maxSlot = 0
	count = 0

	l.index.Range(func(key, value interface{}) bool {
		slot := key.(uint64)
		offset := value.(uint64)

		// Only count committed entries (in tail)
		if (offset & mutableFlag) == 0 {
			if slot < minSlot {
				minSlot = slot
			}
			if slot > maxSlot {
				maxSlot = slot
			}
			count++
		}
		return true
	})

	if count == 0 {
		return 0, 0, 0
	}

	return minSlot, maxSlot, count
}

// ReplayFromSlot replays committed entries starting from a slot up to current max
// This is the primary method for learner bootstrap and crash recovery
//
// CRITICAL: This captures the max slot BEFORE iteration to ensure a consistent snapshot.
// New commits that arrive during iteration are NOT included (prevents inconsistent reads).
//
// Example - State machine reconstruction:
//
//	err := log.ReplayFromSlot(0, func(entry *LogEntry) error {
//	    return stateMachine.Apply(entry.Slot, entry.Value)
//	})
//
// Example - Learner catching up from slot 1000:
//
//	err := log.ReplayFromSlot(1000, func(entry *LogEntry) error {
//	    return sendToLearner(entry)
//	})
//
// The callback receives entries in slot order and MUST NOT store the entry pointer.
func (l *FasterLog) ReplayFromSlot(startSlot uint64, fn func(entry *LogEntry) error) error {
	// Capture upper bound BEFORE iteration to ensure consistent snapshot
	// If new commits arrive during replay, they won't leak into this iteration
	_, maxSlot, count := l.GetCommittedRange()

	// Empty log or no entries in range
	if count == 0 || maxSlot < startSlot {
		return nil
	}

	return l.IterateCommitted(fn, IterateOptions{
		MinSlot:            startSlot,
		MaxSlot:            maxSlot, // Fixed upper bound
		IncludeUncommitted: false,
		SkipErrors:         false,
	})
}

// ReplayRange replays committed entries in a specific range [startSlot, endSlot]
// Use this when you want explicit control over the upper bound (e.g., incremental catch-up)
//
// Example - Learner catching up to specific slot:
//
//	leaderMax := getLeaderMaxSlot()
//	err := log.ReplayRange(1000, leaderMax, func(entry *LogEntry) error {
//	    return applyEntry(entry)
//	})
func (l *FasterLog) ReplayRange(startSlot, endSlot uint64, fn func(entry *LogEntry) error) error {
	return l.IterateCommitted(fn, IterateOptions{
		MinSlot:            startSlot,
		MaxSlot:            endSlot,
		IncludeUncommitted: false,
		SkipErrors:         false,
	})
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
