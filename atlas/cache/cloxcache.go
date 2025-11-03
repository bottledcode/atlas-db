// Package cache provides a lock-free adaptive in-memory LRU cache implementation.
// CloxCache combines CLOCK-Pro eviction with TinyLFU admission and adaptive frequency decay.
package cache

import (
	"math/bits"
	"sync"
	"sync/atomic"
	"time"
)

const (
	// maxFrequency is the maximum value for the frequency counter (0-15 range)
	maxFrequency = 15

	// initialFreq is the starting frequency for new keys
	initialFreq = 1

	// maxProbes is the maximum number of slots to check during admission
	maxProbes = 8

	// sweepInterval is the time between CLOCK sweeper ticks
	sweepInterval = 10 * time.Millisecond

	// slotsPerTick is how many slots to process per sweep tick (budgeted scanning)
	slotsPerTick = 256

	// decayInterval is how often to retarget the decay step
	decayInterval = 1 * time.Second
)

// CloxCache is a lock-free adaptive in-memory cache with CLOCK-Pro eviction.
// It stores generic values of type T.
type CloxCache[T any] struct {
	shards    []shard[T]
	numShards int
	shardBits int

	// CLOCK hand position
	hand atomic.Uint64

	// Metrics for adaptive behavior
	hits      atomic.Uint64
	misses    atomic.Uint64
	evictions atomic.Uint64
	pressure  atomic.Uint64

	// Adaptive decay step (1-4)
	decayStep atomic.Uint32

	// Control channels
	stopSweeper chan struct{}
	stopDecay   chan struct{}
}

// shard contains a portion of the cache slots with minimal lock contention
type shard[T any] struct {
	slots []atomic.Pointer[recordNode[T]]
	mu    sync.Mutex // only for insertions and sweeper unlink
}

// recordNode is a cache entry with collision chaining
// Layout optimized for cache-line efficiency
// Note: For pointer types T, we can use atomic.Pointer. For value types, we use atomic.Value.
type recordNode[T any] struct {
	// Cache line 1: Hot fields accessed on every lookup
	value   atomic.Value                     // value stored (generic type, atomic for race-safety)
	next    atomic.Pointer[recordNode[T]]    // 8 bytes - chain traversal
	keyHash uint64                           // 8 bytes - fast comparison
	freq    atomic.Uint32                    // 4 bytes - access frequency (0-15)
	keyLen  uint16                           // 2 bytes - key length
	_       [2]byte                          // 2 bytes - alignment padding

	// Cache line 2 (bytes 32-95): Key data (only accessed on hash match)
	key [64]byte // 64 bytes - full key (MAX 64 bytes!)
}

// Config holds CloxCache configuration
type Config struct {
	NumShards     int // Must be power of 2
	SlotsPerShard int // Must be power of 2
}

// NewCloxCache creates a new cache with the given configuration
func NewCloxCache[T any](cfg Config) *CloxCache[T] {
	// Validate positive values
	if cfg.NumShards <= 0 {
		panic("NumShards must be positive")
	}
	if cfg.SlotsPerShard <= 0 {
		panic("SlotsPerShard must be positive")
	}

	// Validate power-of-2 requirements
	if cfg.NumShards&(cfg.NumShards-1) != 0 {
		panic("NumShards must be a power of 2")
	}
	if cfg.SlotsPerShard&(cfg.SlotsPerShard-1) != 0 {
		panic("SlotsPerShard must be a power of 2")
	}

	c := &CloxCache[T]{
		numShards:   cfg.NumShards,
		shardBits:   bits.Len(uint(cfg.NumShards - 1)),
		shards:      make([]shard[T], cfg.NumShards),
		stopSweeper: make(chan struct{}),
		stopDecay:   make(chan struct{}),
	}

	// Initialize shards
	for i := range c.shards {
		c.shards[i].slots = make([]atomic.Pointer[recordNode[T]], cfg.SlotsPerShard)
	}

	// Start with gentle decay
	c.decayStep.Store(1)

	// Start background goroutines
	go c.sweeper(cfg.SlotsPerShard)
	go c.retargetDecayLoop()

	return c
}

// Close stops background goroutines
func (c *CloxCache[T]) Close() {
	close(c.stopSweeper)
	close(c.stopDecay)
}

// hashKey computes FNV-1a hash for the given key
func hashKey(key []byte) uint64 {
	const offset64 = 14695981039346656037
	const prime64 = 1099511628211

	hash := uint64(offset64)
	for _, b := range key {
		hash ^= uint64(b)
		hash *= prime64
	}
	return hash
}

// Get retrieves a value from the cache (lock-free)
func (c *CloxCache[T]) Get(key []byte) (T, bool) {
	var zero T
	if len(key) > 64 {
		return zero, false // key too long
	}

	hash := hashKey(key)
	shardID := hash & uint64(c.numShards-1)
	slotID := (hash >> c.shardBits) & uint64(len(c.shards[0].slots)-1)

	shard := &c.shards[shardID]
	slot := &shard.slots[slotID]

	// Lock-free chain walk
	node := slot.Load()
	for node != nil {
		if node.keyHash == hash && node.keyLen == uint16(len(key)) {
			// Compare full key (second cache line - only on hash match)
			if bytesEqual(node.key[:node.keyLen], key) {
				// Atomic saturating increment (0-15)
				for {
					f := node.freq.Load()
					if f >= maxFrequency {
						break // already saturated
					}
					if node.freq.CompareAndSwap(f, f+1) {
						break // successfully incremented
					}
				}

				c.hits.Add(1)
				val := node.value.Load()
				if val == nil {
					// Value was nil, treat as miss
					c.misses.Add(1)
					return zero, false
				}
				return val.(T), true
			}
		}
		node = node.next.Load()
	}

	c.misses.Add(1)
	return zero, false
}

// Put inserts or updates a value in the cache
func (c *CloxCache[T]) Put(key []byte, value T) bool {
	if len(key) > 64 {
		return false // key too long
	}

	hash := hashKey(key)
	shardID := hash & uint64(c.numShards-1)
	slotID := (hash >> c.shardBits) & uint64(len(c.shards[0].slots)-1)

	shard := &c.shards[shardID]
	slot := &shard.slots[slotID]

	// First, try to update existing key (lock-free)
	node := slot.Load()
	for node != nil {
		if node.keyHash == hash && node.keyLen == uint16(len(key)) {
			if bytesEqual(node.key[:node.keyLen], key) {
				// Update existing value atomically
				node.value.Store(value)
				// Bump frequency
				for {
					f := node.freq.Load()
					if f >= maxFrequency {
						break
					}
					if node.freq.CompareAndSwap(f, f+1) {
						break
					}
				}
				return true
			}
		}
		node = node.next.Load()
	}

	// New key - check admission policy
	if !c.shouldAdmit() {
		c.pressure.Add(1)
		return false
	}

	// Allocate new node
	newNode := &recordNode[T]{
		keyHash: hash,
		keyLen:  uint16(len(key)),
	}
	newNode.value.Store(value)
	copy(newNode.key[:], key)
	newNode.freq.Store(initialFreq)

	// Try CAS onto head
	shard.mu.Lock()
	defer shard.mu.Unlock()

	// Re-check for existing key under lock
	node = slot.Load()
	for node != nil {
		if node.keyHash == hash && node.keyLen == uint16(len(key)) {
			if bytesEqual(node.key[:node.keyLen], key) {
				// Someone else inserted it
				node.value.Store(value)
				return true
			}
		}
		node = node.next.Load()
	}

	// Insert at head
	head := slot.Load()
	newNode.next.Store(head)
	slot.Store(newNode)

	return true
}

// shouldAdmit implements TinyLFU-style admission gate
func (c *CloxCache[T]) shouldAdmit() bool {
	// Simple policy: always admit if under pressure threshold
	// More sophisticated: probe victim slots
	totalSlots := uint64(c.numShards * len(c.shards[0].slots))
	startSlot := c.hand.Load() % totalSlots
	probes := 0

	for probes < maxProbes {
		slotID := (startSlot + uint64(probes)) % totalSlots
		shardID := slotID % uint64(c.numShards)
		localSlot := slotID / uint64(c.numShards)

		shard := &c.shards[shardID]
		if int(localSlot) >= len(shard.slots) {
			probes++
			continue
		}

		slot := &shard.slots[localSlot]
		victim := slot.Load()

		if victim == nil || victim.freq.Load() <= initialFreq {
			// Found admissible slot/victim
			return true
		}

		probes++
	}

	// All probed victims are hotter - reject
	return false
}

// sweeper is the background CLOCK hand that ages and evicts entries
func (c *CloxCache[T]) sweeper(slotsPerShard int) {
	hand := uint64(0)
	totalSlots := uint64(c.numShards * slotsPerShard)

	ticker := time.NewTicker(sweepInterval)
	defer ticker.Stop()

	for {
		select {
		case <-c.stopSweeper:
			return
		case <-ticker.C:
			// Process a batch of slots
			dec := c.decayStep.Load()

			for i := 0; i < slotsPerTick; i++ {
				slotIdx := hand % totalSlots
				shardID := slotIdx % uint64(c.numShards)
				localSlot := slotIdx / uint64(c.numShards)

				if int(localSlot) >= slotsPerShard {
					hand++
					continue
				}

				c.sweepSlot(int(shardID), int(localSlot), dec)
				hand++
			}

			// Update global hand
			c.hand.Store(hand)
		}
	}
}

// sweepSlot processes a single slot's chain
func (c *CloxCache[T]) sweepSlot(shardID, slotID int, dec uint32) {
	shard := &c.shards[shardID]
	slot := &shard.slots[slotID]

	shard.mu.Lock()
	defer shard.mu.Unlock()

	node := slot.Load()
	var prev *recordNode[T]

	for node != nil {
		f := node.freq.Load()
		if f > 0 {
			// Age down
			var newFreq uint32
			if f > dec {
				newFreq = f - dec
			} else {
				newFreq = 0
			}
			node.freq.Store(newFreq)

			prev = node
			node = node.next.Load()
			continue
		}

		// Evict: freq == 0
		c.evictions.Add(1)

		next := node.next.Load()
		if prev == nil {
			// Head removal
			for !slot.CompareAndSwap(node, next) {
				cur := slot.Load()
				if cur != node {
					// Head changed, restart
					prev = nil
					node = cur
					goto continueBucket
				}
			}
		} else {
			// Middle removal
			prev.next.Store(next)
		}
		node = next
	continueBucket:
	}
}

// retargetDecayLoop periodically adjusts the decay step based on metrics
func (c *CloxCache[T]) retargetDecayLoop() {
	ticker := time.NewTicker(decayInterval)
	defer ticker.Stop()

	for {
		select {
		case <-c.stopDecay:
			return
		case <-ticker.C:
			c.retargetDecay()
		}
	}
}

// retargetDecay adjusts the decay step based on hit rate and pressure
func (c *CloxCache[T]) retargetDecay() {
	hits := c.hits.Load()
	misses := c.misses.Load()
	pressure := c.pressure.Load()

	total := hits + misses
	if total == 0 {
		return
	}

	hitRate := float64(hits) / float64(total)

	// Adaptive decay based on pressure and hit rate
	switch {
	case pressure > 2000 && hitRate < 0.60:
		c.decayStep.Store(4) // aggressive eviction
	case pressure > 1000 && hitRate < 0.70:
		c.decayStep.Store(3)
	case pressure > 200 && hitRate < 0.80:
		c.decayStep.Store(2)
	default:
		c.decayStep.Store(1) // gentle aging
	}

	// Reset counters for sliding window
	c.hits.Store(0)
	c.misses.Store(0)
	c.pressure.Store(0)
}

// Stats returns cache statistics
func (c *CloxCache[T]) Stats() (hits, misses, evictions uint64) {
	return c.hits.Load(), c.misses.Load(), c.evictions.Load()
}

// bytesEqual compares two byte slices without bounds checking
func bytesEqual(a, b []byte) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
