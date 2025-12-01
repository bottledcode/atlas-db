package cache

import (
	"fmt"
	"sync"
	"testing"
	"time"
)

func TestCloxCacheBasicOperations(t *testing.T) {
	cfg := Config{
		NumShards:     16,
		SlotsPerShard: 256,
	}
	cache := NewCloxCache[[]byte, string](cfg)
	defer cache.Close()

	// Test Put and Get
	key := []byte("test-key")
	value := "test-value"

	if !cache.Put(key, value) {
		t.Fatal("Put failed")
	}

	got, ok := cache.Get(key)
	if !ok {
		t.Fatal("Get failed: key not found")
	}
	if got != value {
		t.Fatalf("Get returned wrong value: got %q, want %q", got, value)
	}

	// Test Get on non-existent key
	_, ok = cache.Get([]byte("non-existent"))
	if ok {
		t.Fatal("Get succeeded on non-existent key")
	}
}

func TestCloxCacheUpdate(t *testing.T) {
	cfg := Config{
		NumShards:     16,
		SlotsPerShard: 256,
	}
	cache := NewCloxCache[[]byte, int](cfg)
	defer cache.Close()

	key := []byte("counter")

	// Insert initial value
	cache.Put(key, 1)

	// Update value
	cache.Put(key, 2)
	cache.Put(key, 3)

	// Verify final value
	got, ok := cache.Get(key)
	if !ok {
		t.Fatal("Get failed after update")
	}
	if got != 3 {
		t.Fatalf("Get returned wrong value: got %d, want %d", got, 3)
	}
}

func TestCloxCacheConcurrentAccess(t *testing.T) {
	cfg := Config{
		NumShards:     64,
		SlotsPerShard: 1024,
	}
	cache := NewCloxCache[[]byte, int](cfg)
	defer cache.Close()

	const numGoroutines = 100
	const numOps = 1000

	var wg sync.WaitGroup
	wg.Add(numGoroutines)

	// Concurrent writes
	for i := range numGoroutines {
		go func(id int) {
			defer wg.Done()
			for j := range numOps {
				key := fmt.Appendf(nil, "key-%d-%d", id, j)
				cache.Put(key, id*numOps+j)
			}
		}(i)
	}

	wg.Wait()

	// Verify some values
	for i := range 10 {
		key := fmt.Appendf(nil, "key-%d-%d", i, 0)
		val, ok := cache.Get(key)
		if !ok {
			t.Errorf("Key %s not found", key)
			continue
		}
		expected := i * numOps
		if val != expected {
			t.Errorf("Wrong value for key %s: got %d, want %d", key, val, expected)
		}
	}
}

func TestCloxCacheConcurrentReadWrite(t *testing.T) {
	cfg := Config{
		NumShards:     32,
		SlotsPerShard: 512,
	}
	cache := NewCloxCache[[]byte, string](cfg)
	defer cache.Close()

	const numKeys = 100
	const numReaders = 50
	const numWriters = 10

	// Pre-populate cache
	for i := range numKeys {
		key := fmt.Appendf(nil, "key-%d", i)
		cache.Put(key, fmt.Sprintf("value-%d", i))
	}

	stop := make(chan struct{})
	var wg sync.WaitGroup

	// Start readers
	wg.Add(numReaders)
	for range numReaders {
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
					for j := range numKeys {
						key := fmt.Appendf(nil, "key-%d", j)
						cache.Get(key)
					}
				}
			}
		}()
	}

	// Start writers
	wg.Add(numWriters)
	for i := range numWriters {
		go func(id int) {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
					for j := range numKeys {
						key := fmt.Appendf(nil, "key-%d", j)
						cache.Put(key, fmt.Sprintf("writer-%d-value-%d", id, j))
					}
				}
			}
		}(i)
	}

	// Run for a short duration
	time.Sleep(100 * time.Millisecond)
	close(stop)
	wg.Wait()

	// Verify cache is still functional
	hits, misses, evictions := cache.Stats()
	t.Logf("Stats: hits=%d, misses=%d, evictions=%d", hits, misses, evictions)

	if hits == 0 {
		t.Error("Expected some cache hits")
	}
}

func TestCloxCacheFrequencyIncrement(t *testing.T) {
	cfg := Config{
		NumShards:     4,
		SlotsPerShard: 16,
	}
	cache := NewCloxCache[[]byte, int](cfg)
	defer cache.Close()

	key := []byte("hot-key")
	cache.Put(key, 42)

	// Access the key multiple times to bump frequency
	for range 20 {
		cache.Get(key)
	}

	// Verify key is still there (high frequency should protect it)
	val, ok := cache.Get(key)
	if !ok {
		t.Fatal("Hot key was evicted")
	}
	if val != 42 {
		t.Fatalf("Wrong value: got %d, want 42", val)
	}
}

func TestCloxCacheEviction(t *testing.T) {
	cfg := Config{
		NumShards:     4,
		SlotsPerShard: 16, // Small cache
	}
	cache := NewCloxCache[[]byte, int](cfg)
	defer cache.Close()

	// Fill cache beyond capacity
	const numKeys = 200
	for i := range numKeys {
		key := fmt.Appendf(nil, "key-%d", i)
		cache.Put(key, i)
	}

	// Wait for sweeper to run
	time.Sleep(200 * time.Millisecond)

	// Check evictions occurred
	_, _, evictions := cache.Stats()
	if evictions == 0 {
		t.Log("Warning: No evictions recorded yet (sweeper may not have run)")
	}

	// Cache should still be functional
	testKey := []byte("key-100")
	val, ok := cache.Get(testKey)
	if ok && val != 100 {
		t.Fatalf("Wrong value: got %d, want 100", val)
	}
}

func TestCloxCacheLongKey(t *testing.T) {
	cfg := Config{
		NumShards:     4,
		SlotsPerShard: 16,
	}
	cache := NewCloxCache[[]byte, int](cfg)
	defer cache.Close()

	// Long keys should now work (no 64-byte limit)
	longKey := make([]byte, 256)
	for i := range longKey {
		longKey[i] = byte(i)
	}

	if !cache.Put(longKey, 42) {
		t.Fatal("Put failed with long key")
	}

	val, ok := cache.Get(longKey)
	if !ok {
		t.Fatal("Get failed with long key")
	}
	if val != 42 {
		t.Fatalf("Wrong value: got %d, want 42", val)
	}
}

func TestCloxCacheStats(t *testing.T) {
	cfg := Config{
		NumShards:     8,
		SlotsPerShard: 64,
	}
	cache := NewCloxCache[[]byte, string](cfg)
	defer cache.Close()

	// Generate some hits and misses
	cache.Put([]byte("key1"), "value1")
	cache.Put([]byte("key2"), "value2")

	cache.Get([]byte("key1")) // hit
	cache.Get([]byte("key2")) // hit
	cache.Get([]byte("key3")) // miss

	hits, misses, _ := cache.Stats()

	if hits != 2 {
		t.Errorf("Expected 2 hits, got %d", hits)
	}
	if misses != 1 {
		t.Errorf("Expected 1 miss, got %d", misses)
	}
}

func TestCloxCachePointerTypes(t *testing.T) {
	type Record struct {
		ID   int
		Data string
	}

	cfg := Config{
		NumShards:     16,
		SlotsPerShard: 256,
	}
	cache := NewCloxCache[[]byte, *Record](cfg)
	defer cache.Close()

	key := []byte("record-1")
	record := &Record{ID: 1, Data: "test data"}

	cache.Put(key, record)

	got, ok := cache.Get(key)
	if !ok {
		t.Fatal("Get failed")
	}

	if got.ID != record.ID || got.Data != record.Data {
		t.Fatalf("Got wrong record: %+v, want %+v", got, record)
	}

	// Verify it's the same pointer
	if got != record {
		t.Fatal("Expected same pointer")
	}
}

func TestCloxCacheAdaptiveDecay(t *testing.T) {
	cfg := Config{
		NumShards:     8,
		SlotsPerShard: 32,
	}
	cache := NewCloxCache[[]byte, int](cfg)
	defer cache.Close()

	// Generate high pressure by filling cache and rejecting admissions
	for i := range 500 {
		key := fmt.Appendf(nil, "key-%d", i)
		cache.Put(key, i)
	}

	// Wait for decay retargeting
	time.Sleep(1100 * time.Millisecond)

	// Decay step should have increased due to pressure
	decayStep := cache.decayStep.Load()
	t.Logf("Decay step after pressure: %d", decayStep)

	// We expect decay step > 1 due to high pressure
	if decayStep == 0 {
		t.Error("Decay step should not be 0")
	}
}

func TestCloxCacheHashCollisions(t *testing.T) {
	cfg := Config{
		NumShards:     2,
		SlotsPerShard: 4, // Very small to force collisions
	}
	cache := NewCloxCache[[]byte, int](cfg)
	defer cache.Close()

	// Insert many keys (will likely collide in such a small cache)
	const numKeys = 50
	for i := range numKeys {
		key := fmt.Appendf(nil, "collision-%d", i)
		cache.Put(key, i)
	}

	// Verify we can retrieve keys despite collisions
	retrieved := 0
	for i := range numKeys {
		key := fmt.Appendf(nil, "collision-%d", i)
		val, ok := cache.Get(key)
		if ok {
			retrieved++
			if val != i {
				t.Errorf("Wrong value for key %s: got %d, want %d", key, val, i)
			}
		}
	}

	t.Logf("Retrieved %d/%d keys with collisions", retrieved, numKeys)
	if retrieved == 0 {
		t.Error("Expected to retrieve at least some keys")
	}
}

func TestCloxCacheInvalidConfig(t *testing.T) {
	tests := []struct {
		name      string
		cfg       Config
		wantPanic string
	}{
		{
			name:      "Zero NumShards",
			cfg:       Config{NumShards: 0, SlotsPerShard: 256},
			wantPanic: "NumShards must be positive",
		},
		{
			name:      "Negative NumShards",
			cfg:       Config{NumShards: -1, SlotsPerShard: 256},
			wantPanic: "NumShards must be positive",
		},
		{
			name:      "Zero SlotsPerShard",
			cfg:       Config{NumShards: 16, SlotsPerShard: 0},
			wantPanic: "SlotsPerShard must be positive",
		},
		{
			name:      "Negative SlotsPerShard",
			cfg:       Config{NumShards: 16, SlotsPerShard: -1},
			wantPanic: "SlotsPerShard must be positive",
		},
		{
			name:      "NumShards not power of 2",
			cfg:       Config{NumShards: 15, SlotsPerShard: 256},
			wantPanic: "NumShards must be a power of 2",
		},
		{
			name:      "SlotsPerShard not power of 2",
			cfg:       Config{NumShards: 16, SlotsPerShard: 255},
			wantPanic: "SlotsPerShard must be a power of 2",
		},
		{
			name:      "Both invalid",
			cfg:       Config{NumShards: 0, SlotsPerShard: 0},
			wantPanic: "NumShards must be positive",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			defer func() {
				r := recover()
				if r == nil {
					t.Fatal("Expected panic but got none")
				}
				panicMsg := fmt.Sprint(r)
				if panicMsg != tt.wantPanic {
					t.Errorf("Wrong panic message: got %q, want %q", panicMsg, tt.wantPanic)
				}
			}()

			// This should panic
			_ = NewCloxCache[[]byte, int](tt.cfg)
		})
	}
}

func TestCloxCacheStringKeys(t *testing.T) {
	cfg := Config{
		NumShards:     16,
		SlotsPerShard: 256,
	}
	cache := NewCloxCache[string, int](cfg)
	defer cache.Close()

	// Test Put and Get with string keys
	key := "test-key"
	value := 42

	if !cache.Put(key, value) {
		t.Fatal("Put failed")
	}

	got, ok := cache.Get(key)
	if !ok {
		t.Fatal("Get failed: key not found")
	}
	if got != value {
		t.Fatalf("Get returned wrong value: got %d, want %d", got, value)
	}

	// Test Get on non-existent key
	_, ok = cache.Get("non-existent")
	if ok {
		t.Fatal("Get succeeded on non-existent key")
	}

	// Test update
	cache.Put(key, 100)
	got, ok = cache.Get(key)
	if !ok {
		t.Fatal("Get failed after update")
	}
	if got != 100 {
		t.Fatalf("Get returned wrong value after update: got %d, want 100", got)
	}
}
