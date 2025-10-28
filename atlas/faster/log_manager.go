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
	"container/list"
	"crypto/sha256"
	"encoding/base32"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/bottledcode/atlas-db/atlas/options"
)

type ByteSize uint64

const (
	B  ByteSize = 1
	KB ByteSize = 1 << (10 * iota)
	MB
	GB
	TB
	PB
)

const (
	MutableSize = 64 * MB
	SegmentSize = 1 * GB
	NumThreads  = 128
	MaxHotKeys  = 256
)

// logHandle wraps a FasterLog with reference counting and LRU tracking
// Reference counting prevents closing logs that are actively in use
type logHandle struct {
	log        *FasterLog
	refCount   atomic.Int32  // Number of active users
	lastAccess atomic.Int64  // Unix timestamp of last access
	elem       *list.Element // Position in LRU list
	key        string        // Key for this log
}

// acquire increments the reference count (someone is using this log)
func (h *logHandle) acquire() {
	h.refCount.Add(1)
	h.lastAccess.Store(time.Now().Unix())
}

// release decrements the reference count (done using this log)
// Uses compare-and-swap loop to ensure refcount never goes negative
func (h *logHandle) release() {
	for {
		current := h.refCount.Load()
		if current <= 0 {
			// Already at zero (or somehow negative), don't decrement further
			// This shouldn't happen with proper usage, but protects against bugs
			return
		}

		// Try to decrement atomically
		if h.refCount.CompareAndSwap(current, current-1) {
			return
		}
		// CAS failed, retry
	}
}

// canClose returns true if no one is using this log
func (h *logHandle) canClose() bool {
	return h.refCount.Load() == 0
}

// isIdle returns true if the log hasn't been accessed in the given duration
func (h *logHandle) isIdle(idleTimeout int64) bool {
	return time.Now().Unix()-h.lastAccess.Load() > idleTimeout
}

// LogManager manages FASTER logs with LRU eviction and reference counting
// This ensures logs are never closed while in use, preventing use-after-free bugs
type LogManager struct {
	handles sync.Map   // string -> *logHandle (concurrent map for fast lookup)
	lru     *list.List // LRU list of *logHandle (front = oldest, back = newest)
	lruMu   sync.Mutex // Protects LRU list operations
	maxOpen int        // Maximum number of open logs
	closed  atomic.Bool
}

// NewLogManager creates a new LogManager with LRU eviction
func NewLogManager() *LogManager {
	return &LogManager{
		lru:     list.New(),
		maxOpen: MaxHotKeys,
	}
}

// GetLog returns a log handle with acquired reference
// CRITICAL: Caller MUST call the returned release function when done!
// Usage:
//
//	log, release, err := manager.GetLog(key)
//	if err != nil { return err }
//	defer release()
//	// ... use log safely ...
func (l *LogManager) GetLog(key []byte) (*FasterLog, func(), error) {
	if l.closed.Load() {
		return nil, nil, ErrClosed
	}

	keyStr := string(key)

	// Fast path: log already exists
	if val, ok := l.handles.Load(keyStr); ok {
		handle := val.(*logHandle)
		handle.acquire()

		// Move to back of LRU (most recently used)
		l.lruMu.Lock()
		if handle.elem != nil {
			l.lru.MoveToBack(handle.elem)
		}
		l.lruMu.Unlock()

		// Return log and release function
		release := l.makeReleaseFunc(handle)
		return handle.log, release, nil
	}

	// Slow path: need to create log
	l.lruMu.Lock()
	defer l.lruMu.Unlock()

	// Double-check after acquiring lock (another goroutine might have created it)
	if val, ok := l.handles.Load(keyStr); ok {
		handle := val.(*logHandle)
		handle.acquire()
		if handle.elem != nil {
			l.lru.MoveToBack(handle.elem)
		}

		release := l.makeReleaseFunc(handle)
		return handle.log, release, nil
	}

	// Try to evict if at capacity
	l.tryEvict()

	// Create new log
	path := l.generatePath(key)
	log, err := NewFasterLog(Config{
		Path:         path,
		MutableSize:  uint64(MutableSize),
		SegmentSize:  uint64(SegmentSize),
		NumThreads:   NumThreads,
		SyncOnCommit: false,
	})
	if err != nil {
		return nil, nil, err
	}

	// Create handle
	handle := &logHandle{
		log: log,
		key: keyStr,
	}
	handle.refCount.Store(1) // Start with 1 reference
	handle.lastAccess.Store(time.Now().Unix())

	// Add to LRU (at back = most recent)
	handle.elem = l.lru.PushBack(handle)

	// Add to map
	l.handles.Store(keyStr, handle)

	release := l.makeReleaseFunc(handle)
	return log, release, nil
}

// makeReleaseFunc creates a release function that's safe to call multiple times
func (l *LogManager) makeReleaseFunc(handle *logHandle) func() {
	released := atomic.Bool{}
	return func() {
		// Only release once, even if called multiple times
		if released.CompareAndSwap(false, true) {
			handle.release()
		}
	}
}

// tryEvict attempts to evict least-recently-used logs
// Must be called with lruMu held!
func (l *LogManager) tryEvict() {
	const idleTimeout = 300 // 5 minutes

	// Try to evict while we're over capacity
	for l.lru.Len() >= l.maxOpen {
		evicted := false

		// Scan through list looking for an evictable log
		for elem := l.lru.Front(); elem != nil; elem = elem.Next() {
			handle := elem.Value.(*logHandle)

			// Can only evict if no one is using it
			// Idle timeout is risky - a log could be idle but have leaked references
			// So we ONLY evict when refcount is truly zero
			if handle.canClose() {
				// Safe to evict - no active references
				l.lru.Remove(elem)
				handle.elem = nil
				l.handles.Delete(handle.key)

				// Close in background to avoid blocking the GetLog caller
				go func(h *logHandle) {
					// Double-check refcount before closing
					// (paranoid check in case of race)
					if !h.canClose() {
						// References appeared! Don't close.
						// This shouldn't happen since we hold lruMu and removed from LRU,
						// but better safe than sorry.
						return
					}
					_ = h.log.Close()
				}(handle)

				evicted = true
				break // Successfully evicted, exit inner loop
			}
		}

		// If we couldn't evict anything, all logs are in use
		if !evicted {
			// Can't evict, but allow creation anyway
			// The limit is a soft limit when all logs have active references
			break
		}
	}
}

// generatePath creates a filesystem-safe path from a key
func (l *LogManager) generatePath(key []byte) string {
	// SHA256 gives cryptographic collision resistance (2^256 space)
	hash := sha256.Sum256(key)

	// Base32 encoding creates filesystem-safe alphanumeric names
	// Uses A-Z and 2-7 (no ambiguous characters like 0/O or 1/l)
	safeKey := base32.StdEncoding.EncodeToString(hash[:])

	// Remove padding '=' characters for cleaner filenames
	safeKey = strings.TrimRight(safeKey, "=")

	// Truncate to 32 chars (160 bits of entropy - way more than needed)
	if len(safeKey) > 32 {
		safeKey = safeKey[:32]
	}

	return options.CurrentOptions.DbFilename + "." + safeKey + ".log"
}

// CloseAll closes all logs (for shutdown)
// This will wait for active references to drain before closing
// If references don't drain within the timeout, it will NOT force-close
func (l *LogManager) CloseAll() error {
	if !l.closed.CompareAndSwap(false, true) {
		return ErrClosed
	}

	l.lruMu.Lock()
	defer l.lruMu.Unlock()

	var firstErr error
	var wg sync.WaitGroup

	l.handles.Range(func(key, value interface{}) bool {
		handle := value.(*logHandle)

		wg.Add(1)
		go func(h *logHandle) {
			defer wg.Done()

			// Wait for references to drain (up to 5 seconds)
			timeout := time.After(5 * time.Second)
			ticker := time.NewTicker(100 * time.Millisecond)
			defer ticker.Stop()

			for {
				if h.canClose() {
					// Safe to close
					if err := h.log.Close(); err != nil && firstErr == nil {
						firstErr = err
					}
					return
				}

				select {
				case <-timeout:
					// Timeout reached, but don't force close!
					// Log the leak but leave the file open
					if firstErr == nil {
						firstErr = fmt.Errorf("timeout waiting for references to drain on log %q (refcount=%d)",
							h.key, h.refCount.Load())
					}
					return
				case <-ticker.C:
					// Check again
					continue
				}
			}
		}(handle)

		return true
	})

	wg.Wait()
	return firstErr
}

// Stats returns statistics about the LogManager
type LogManagerStats struct {
	TotalLogs  int
	OpenLogs   int
	ActiveRefs int
}

// Stats returns current statistics
func (l *LogManager) Stats() LogManagerStats {
	l.lruMu.Lock()
	openLogs := l.lru.Len()
	l.lruMu.Unlock()

	totalLogs := 0
	activeRefs := 0

	l.handles.Range(func(key, value interface{}) bool {
		totalLogs++
		handle := value.(*logHandle)
		activeRefs += int(handle.refCount.Load())
		return true
	})

	return LogManagerStats{
		TotalLogs:  totalLogs,
		OpenLogs:   openLogs,
		ActiveRefs: activeRefs,
	}
}
