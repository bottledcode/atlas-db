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
	"runtime"
	"sync/atomic"
)

// RingBuffer is a lock-free buffer for the mutable region
// It stores uncommitted log entries in memory before they're flushed to the immutable tail
// Uses atomic CAS operations for true lock-free concurrent writes following FASTER design.
//
// LOCK-FREE DESIGN:
// - Writes use atomic CAS to allocate space (no locks!)
// - Reads scan the buffer sequentially (acceptable since mutable region is small)
// - No index map needed (scanning is fast enough for ~64MB region)
//
// Note: This is not a true "ring" buffer - it's a simple linear allocator that grows
// until entries are flushed. This matches WPaxos semantics better than a circular buffer.
type RingBuffer struct {
	data []byte
	size uint64

	// reserved tracks space allocated via CAS (lock-free allocation)
	// Writers CAS this to claim their write region
	reserved atomic.Uint64

	// published tracks data fully written and ready to read
	// Only entries < published are safe to scan
	// This prevents readers from seeing partially-written entries
	published atomic.Uint64

	// tail tracks the lowest offset still in use (for garbage collection)
	tail atomic.Uint64

	// watermark tracks the highest slot written
	watermark atomic.Uint64
}

// NewRingBuffer creates a new ring buffer of the given size
func NewRingBuffer(size uint64) *RingBuffer {
	return &RingBuffer{
		data: make([]byte, size),
		size: size,
	}
}

// Append adds an entry to the buffer using lock-free CAS allocation
// Returns the offset within the buffer
// LOCK-FREE: Uses atomic CompareAndSwap to reserve space without locks
//
// SAFETY: Uses reserve-write-publish protocol to prevent readers from seeing
// partially-written entries:
// 1. Reserve space via CAS on 'reserved'
// 2. Write data into reserved region
// 3. Publish via CAS on 'published' (waits for sequential order)
func (rb *RingBuffer) Append(entry *LogEntry) (uint64, error) {
	// Serialize the entry first (outside the CAS loop)
	data, err := serializeEntry(entry)
	if err != nil {
		return 0, err
	}

	dataLen := uint64(len(data))
	if dataLen > rb.size {
		return 0, fmt.Errorf("entry too large: %d > %d", dataLen, rb.size)
	}

	// CAS loop to atomically reserve space
	for {
		currentReserved := rb.reserved.Load()
		newReserved := currentReserved + dataLen

		// Check if we have space
		if newReserved > rb.size {
			// Buffer appears full. Try to reclaim if everything has been flushed.
			if rb.resetIfDrained() {
				continue
			}
			// Otherwise wait briefly for space to become available.
			runtime.Gosched()
			continue
		}

		// Try to atomically claim this space
		if rb.reserved.CompareAndSwap(currentReserved, newReserved) {
			// Success! We own the space from currentReserved to newReserved
			// Now write the data (scanners can't see it yet - published < currentReserved)
			copy(rb.data[currentReserved:newReserved], data)

			// CRITICAL: Wait for our turn to publish
			// We must maintain sequential consistency: earlier writes must publish first
			// This spin-wait is typically very short (<10ns) since writes happen in parallel
			for {
				if rb.published.CompareAndSwap(currentReserved, newReserved) {
					// Successfully published! Entry is now visible to scanners
					break
				}
				// Someone ahead of us hasn't published yet
				// Yield to let the publishing goroutine make progress
				runtime.Gosched()
			}

			// Update watermark (track highest slot)
			for {
				current := rb.watermark.Load()
				if entry.Slot <= current {
					break
				}
				if rb.watermark.CompareAndSwap(current, entry.Slot) {
					break
				}
			}

			return currentReserved, nil
		}
		// CAS failed, another thread won - retry
	}
}

// Read reads an entry at the given offset
func (rb *RingBuffer) Read(offset uint64) (*LogEntry, error) {
	if offset >= rb.size {
		return nil, ErrInvalidOffset
	}

	// Read enough bytes for the header first
	headerBuf := make([]byte, entryHeaderSize)
	rb.readBytes(offset, headerBuf)

	// Parse header to get value length
	valueLen := binary.LittleEndian.Uint32(headerBuf[24:28])
	totalSize := entryHeaderSize + int(valueLen) + checksumSize

	// Read the full entry
	fullBuf := make([]byte, totalSize)
	rb.readBytes(offset, fullBuf)

	// Deserialize
	return deserializeEntry(fullBuf)
}

// ReadBySlot reads an entry by slot number
// LOCK-FREE: Scans the buffer sequentially (fast for small mutable regions)
// Only scans published entries to avoid reading partially-written data
func (rb *RingBuffer) ReadBySlot(slot uint64) (*LogEntry, error) {
	// Scan the buffer from tail to published (not reserved!)
	offset := rb.tail.Load()
	published := rb.published.Load()

	for offset < published {
		// Try to read entry at this offset
		entry, err := rb.Read(offset)
		if err != nil {
			// This should never happen with correct serialization
			// since we only scan published entries
			return nil, fmt.Errorf("corrupted entry at offset %d: %w", offset, err)
		}

		if entry.Slot == slot {
			return entry, nil
		}

		// Move to next entry
		// Entry size = header + value + checksum
		valueLen := uint64(len(entry.Value))
		entrySize := uint64(entryHeaderSize) + valueLen + uint64(checksumSize)
		offset += entrySize
	}

	return nil, ErrSlotNotFound
}

// DrainCommitted removes committed entries and returns them
// This is called during checkpoint to flush committed entries to the tail
// LOCK-FREE: Scans the buffer sequentially
// Only scans published entries to avoid reading partially-written data
func (rb *RingBuffer) DrainCommitted() ([]*LogEntry, error) {
	committed := make([]*LogEntry, 0)

	// Scan the buffer from tail to published (not reserved!)
	offset := rb.tail.Load()
	published := rb.published.Load()

	for offset < published {
		entry, err := rb.Read(offset)
		if err != nil {
			// This should never happen with correct serialization
			// since we only scan published entries
			return nil, fmt.Errorf("corrupted entry at offset %d: %w", offset, err)
		}

		if entry.Committed {
			committed = append(committed, entry)
		}

		// Move to next entry
		valueLen := uint64(len(entry.Value))
		entrySize := uint64(entryHeaderSize) + valueLen + uint64(checksumSize)
		offset += entrySize
	}

	return committed, nil
}

// GetAllUncommitted returns all uncommitted entries
// Used for Phase-1 recovery in WPaxos
// LOCK-FREE: Scans the buffer sequentially
// Only scans published entries to avoid reading partially-written data
// indexCheck: optional index to check if entry has been moved to tail
func (rb *RingBuffer) GetAllUncommittedWithIndex(indexCheck func(uint64) bool) ([]*LogEntry, error) {
	uncommitted := make([]*LogEntry, 0)

	// Scan the buffer from tail to published (not reserved!)
	offset := rb.tail.Load()
	published := rb.published.Load()

	for offset < published {
		entry, err := rb.Read(offset)
		if err != nil {
			// This should never happen with correct serialization
			// since we only scan published entries
			return nil, fmt.Errorf("corrupted entry at offset %d: %w", offset, err)
		}

		// Check if entry is uncommitted AND still in mutable buffer
		// (not moved to tail via Commit)
		if !entry.Committed {
			// If we have an index checker, verify entry is still in mutable region
			if indexCheck == nil || indexCheck(entry.Slot) {
				uncommitted = append(uncommitted, entry)
			}
		}

		// Move to next entry
		valueLen := uint64(len(entry.Value))
		entrySize := uint64(entryHeaderSize) + valueLen + uint64(checksumSize)
		offset += entrySize
	}

	return uncommitted, nil
}

// GetAllUncommitted returns all uncommitted entries (legacy wrapper)
func (rb *RingBuffer) GetAllUncommitted() ([]*LogEntry, error) {
	return rb.GetAllUncommittedWithIndex(nil)
}

// MarkCommitted marks an entry as committed in the buffer (IN-PLACE UPDATE)
// WARNING: This is NOT lock-free but safe since we own the epoch
// Used by Commit() to mark an entry before flushing to tail
// Only scans published entries to avoid reading partially-written data
func (rb *RingBuffer) MarkCommitted(slot uint64) error {
	// Find the entry by scanning published region
	offset := rb.tail.Load()
	published := rb.published.Load()

	for offset < published {
		entry, err := rb.Read(offset)
		if err != nil {
			// This should never happen with correct serialization
			// since we only scan published entries
			return fmt.Errorf("corrupted entry at offset %d: %w", offset, err)
		}

		if entry.Slot == slot {
			// Found it! Mark as committed
			entry.Committed = true

			// Rewrite it (in-place update, but safe within epoch)
			data, err := serializeEntry(entry)
			if err != nil {
				return err
			}

			// Write back at same offset
			copy(rb.data[offset:], data)
			return nil
		}

		// Move to next entry
		valueLen := uint64(len(entry.Value))
		entrySize := uint64(entryHeaderSize) + valueLen + uint64(checksumSize)
		offset += entrySize
	}

	return ErrSlotNotFound
}

// Remove is no longer a no-op - it's critical for reclaiming space!
// This should be called after Commit() moves an entry to the tail.
// However, we can't just advance tail arbitrarily - we need to advance it
// to the first uncommitted entry to maintain the invariant that all entries
// before tail have been flushed.
func (rb *RingBuffer) Remove(slot uint64) {
	// Note: This is now handled by TryAdvanceTail() which is called
	// periodically or after commits. Keeping this as a no-op for now
	// but documenting the need for tail advancement.
}

// TryAdvanceTail attempts to advance the tail pointer past committed entries
// that have been moved to the immutable tail, reclaiming space.
// This should be called after commits to incrementally reclaim space.
//
// indexCheck: function that returns true if the slot is still in mutable region
// Returns: number of bytes reclaimed
func (rb *RingBuffer) TryAdvanceTail(indexCheck func(uint64) bool) uint64 {
	currentTail := rb.tail.Load()
	published := rb.published.Load()

	if currentTail >= published {
		// Nothing to reclaim
		return 0
	}

	// Scan from tail to find the first entry still in mutable region
	offset := currentTail
	newTail := currentTail

	for offset < published {
		// Try to read entry at this offset
		entry, err := rb.Read(offset)
		if err != nil {
			// Corrupted entry or end of valid data, stop here
			break
		}

		// Check if this slot is still in mutable region
		if indexCheck != nil && indexCheck(entry.Slot) {
			// This entry is still in mutable region (not yet moved to tail)
			// Stop here - can't advance past it
			break
		}

		// Entry has been moved to tail, we can advance past it
		valueLen := uint64(len(entry.Value))
		entrySize := uint64(entryHeaderSize) + valueLen + uint64(checksumSize)
		newTail = offset + entrySize
		offset += entrySize
	}

	// Advance tail if we found any flushed entries
	if newTail > currentTail {
		if rb.tail.CompareAndSwap(currentTail, newTail) {
			reclaimed := newTail - currentTail

			// If we've reclaimed most of the buffer and tail has caught up to published,
			// we can do a full reset to reclaim all space
			reserved := rb.reserved.Load()
			published := rb.published.Load()

			// CRITICAL: Only reset if reserved == published
			// This ensures no writers are in-flight (between reserve and publish)
			// If reserved > published, there are writers still copying data
			// and they will deadlock if we reset published to 0
			if newTail >= published && reserved == published && reserved > rb.size*3/4 {
				// Attempt a full reset - this is safe because:
				// 1. All entries are flushed (newTail >= published)
				// 2. No writers in-flight (reserved == published)
				// This allows us to reuse the buffer from the beginning
				if rb.reserved.CompareAndSwap(reserved, 0) {
					rb.published.Store(0)
					rb.tail.Store(0)
					// Return the full reclaimed amount
					return reserved
				}
			}

			return reclaimed
		}
	}

	return 0
}

// Reset attempts to reset the buffer when all entries have been flushed.
// If writers are active or the buffer still holds data, the reset is skipped.
func (rb *RingBuffer) Reset() {
	rb.resetIfDrained()
}

// resetIfDrained resets allocator pointers if no in-flight writers exist and
// the mutable buffer has been fully drained. Returns true when a reset occurs.
func (rb *RingBuffer) resetIfDrained() bool {
	for {
		reserved := rb.reserved.Load()
		published := rb.published.Load()
		tail := rb.tail.Load()

		if reserved == 0 && published == 0 && tail == 0 {
			// Already reset
			return true
		}

		// Only reset when no writers are in-flight and the buffer is fully drained.
		if reserved != published || published != tail {
			return false
		}

		// Attempt to claim the reset by moving reserved to 0.
		if rb.reserved.CompareAndSwap(reserved, 0) {
			// Safe to zero published/tail after reserved is cleared.
			rb.published.Store(0)
			rb.tail.Store(0)
			return true
		}

		// Another writer raced the reset; yield and retry.
		runtime.Gosched()
	}
}

// AvailableSpace returns the available space in the buffer
// This accounts for space that has been reclaimed via tail advancement
func (rb *RingBuffer) AvailableSpace() uint64 {
	reserved := rb.reserved.Load()

	// The buffer is a linear allocator
	// Available space is from reserved to end of buffer
	if reserved >= rb.size {
		// Buffer completely full
		return 0
	}

	// Calculate remaining space
	// Note: tail advancement doesn't create new space at the end,
	// it just indicates that earlier space can be reused on Reset()
	return rb.size - reserved
}

// UsedSpace returns the space currently in use (not yet reclaimed)
func (rb *RingBuffer) UsedSpace() uint64 {
	reserved := rb.reserved.Load()
	tail := rb.tail.Load()

	if reserved >= tail {
		return reserved - tail
	}

	return 0
}

// readBytes reads len(buf) bytes starting at offset
func (rb *RingBuffer) readBytes(offset uint64, buf []byte) {
	length := uint64(len(buf))
	copy(buf, rb.data[offset:offset+length])
}
