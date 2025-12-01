//go:build !race

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
	"crypto/rand"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bottledcode/atlas-db/atlas/options"
)

func TestLogManagerFilenames(t *testing.T) {
	// Setup temp directory
	dir := t.TempDir()
	options.CurrentOptions.DbFilename = filepath.Join(dir, "test")

	lm := NewLogManager()
	defer lm.CloseAll()

	testCases := []struct {
		name string
		key  []byte
	}{
		{"simple key", []byte("users")},
		{"special chars", []byte("table/with/slashes")},
		{"unicode", []byte("таблица")},
		{"binary data", []byte{0x00, 0x01, 0xFF, 0xFE}},
		{"long key", []byte(strings.Repeat("a", 1000))},
	}

	filenames := make(map[string]string)

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			log, release, err := lm.GetLog(tc.key)
			if err != nil {
				t.Fatalf("Failed to get log: %v", err)
			}
			defer release()

			// Extract filename from path
			filename := filepath.Base(log.tailFile.Name())

			t.Logf("Key %q -> filename %q", tc.key, filename)

			// Verify filename is filesystem-safe
			if strings.ContainsAny(filename, "/\\:*?\"<>|") {
				t.Errorf("Filename contains unsafe characters: %s", filename)
			}

			// Verify filename has reasonable length
			if len(filename) > 255 {
				t.Errorf("Filename too long: %d chars", len(filename))
			}

			// Verify filename ends with .log
			if !strings.HasSuffix(filename, ".log") {
				t.Errorf("Filename doesn't end with .log: %s", filename)
			}

			// Check for collisions
			if existingKey, exists := filenames[filename]; exists {
				t.Errorf("COLLISION! Keys %q and %q both map to filename %q",
					existingKey, tc.key, filename)
			}
			filenames[filename] = string(tc.key)
		})
	}

	// Verify we created the expected number of unique files
	if len(filenames) != len(testCases) {
		t.Errorf("Expected %d unique filenames, got %d", len(testCases), len(filenames))
	}
}

func TestLogManagerCaching(t *testing.T) {
	dir := t.TempDir()
	options.CurrentOptions.DbFilename = filepath.Join(dir, "test")

	lm := NewLogManager()
	defer lm.CloseAll()
	key := []byte("test-table")

	// Get log first time
	log1, release1, err := lm.GetLog(key)
	if err != nil {
		t.Fatalf("Failed to get log: %v", err)
	}
	defer release1()

	// Get log second time - should return same instance
	log2, release2, err := lm.GetLog(key)
	if err != nil {
		t.Fatalf("Failed to get log: %v", err)
	}
	defer release2()

	// Should be the exact same pointer
	if log1 != log2 {
		t.Error("Expected same log instance, got different pointers")
	}
}

func TestLogManagerCollisionResistance(t *testing.T) {
	dir := t.TempDir()
	options.CurrentOptions.DbFilename = filepath.Join(dir, "test")

	lm := NewLogManager()
	defer lm.CloseAll()

	// Generate many random keys
	numKeys := 1000
	keys := make([][]byte, numKeys)
	filenames := make(map[string]int)

	for i := range numKeys {
		// Random 32-byte keys
		key := make([]byte, 32)
		_, err := rand.Read(key)
		if err != nil {
			t.Fatalf("Failed to generate random key: %v", err)
		}
		keys[i] = key

		log, release, err := lm.GetLog(key)
		if err != nil {
			t.Fatalf("Failed to get log for key %d: %v", i, err)
		}
		defer release()

		filename := filepath.Base(log.tailFile.Name())
		filenames[filename]++
	}

	// Check for collisions
	collisions := 0
	for filename, count := range filenames {
		if count > 1 {
			t.Errorf("Collision detected: %d keys mapped to filename %s", count, filename)
			collisions++
		}
	}

	if collisions > 0 {
		t.Errorf("Total collisions: %d out of %d keys", collisions, numKeys)
	}

	t.Logf("Successfully created %d unique files for %d keys (0 collisions)", len(filenames), numKeys)
}

func TestLogManagerFilenameFormat(t *testing.T) {
	dir := t.TempDir()
	options.CurrentOptions.DbFilename = filepath.Join(dir, "mydb")

	lm := NewLogManager()
	defer lm.CloseAll()
	key := []byte("test-table")

	log, release, err := lm.GetLog(key)
	if err != nil {
		t.Fatalf("Failed to get log: %v", err)
	}
	defer release()

	filename := filepath.Base(log.tailFile.Name())
	t.Logf("Generated filename: %s", filename)

	// Verify format: mydb.<base32-hash>.log
	if !strings.HasPrefix(filename, "mydb.") {
		t.Errorf("Filename should start with 'mydb.': %s", filename)
	}

	if !strings.HasSuffix(filename, ".log") {
		t.Errorf("Filename should end with '.log': %s", filename)
	}

	// Extract the hash part
	parts := strings.Split(filename, ".")
	if len(parts) != 3 {
		t.Errorf("Expected format 'prefix.hash.log', got %d parts", len(parts))
	} else {
		hashPart := parts[1]

		// Base32 uses A-Z and 2-7
		for _, c := range hashPart {
			if !((c >= 'A' && c <= 'Z') || (c >= '2' && c <= '7')) {
				t.Errorf("Hash contains non-base32 character: %c in %s", c, hashPart)
			}
		}

		// Should be 32 characters (our truncation)
		if len(hashPart) != 32 {
			t.Errorf("Expected hash length 32, got %d: %s", len(hashPart), hashPart)
		}
	}
}

func TestLogManagerDeterministic(t *testing.T) {
	// Same key should always produce same filename
	dir := t.TempDir()
	options.CurrentOptions.DbFilename = filepath.Join(dir, "test")

	key := []byte("consistent-key")

	// Create first log manager and get filename
	lm1 := NewLogManager()
	log1, release1, err := lm1.GetLog(key)
	if err != nil {
		t.Fatalf("Failed to get log: %v", err)
	}
	filename1 := filepath.Base(log1.tailFile.Name())
	release1()
	lm1.CloseAll()

	// Create second log manager and get filename
	lm2 := NewLogManager()
	log2, release2, err := lm2.GetLog(key)
	if err != nil {
		t.Fatalf("Failed to get log: %v", err)
	}
	filename2 := filepath.Base(log2.tailFile.Name())
	release2()
	lm2.CloseAll()

	// Should produce identical filenames
	if filename1 != filename2 {
		t.Errorf("Same key produced different filenames: %s vs %s", filename1, filename2)
	}

	t.Logf("Key %q consistently maps to %s", key, filename1)
}

func TestLogManagerFileCreation(t *testing.T) {
	dir := t.TempDir()
	options.CurrentOptions.DbFilename = filepath.Join(dir, "test")

	lm := NewLogManager()
	defer lm.CloseAll()
	key := []byte("test-table")

	log, release, err := lm.GetLog(key)
	if err != nil {
		t.Fatalf("Failed to get log: %v", err)
	}
	defer release()

	// Verify file was actually created
	filepath := log.tailFile.Name()
	if _, err := os.Stat(filepath); os.IsNotExist(err) {
		t.Errorf("Log file was not created: %s", filepath)
	}

	t.Logf("Successfully created log file: %s", filepath)
}

// Benchmark filename generation
func BenchmarkLogManagerGetLog(b *testing.B) {
	dir := b.TempDir()
	options.CurrentOptions.DbFilename = filepath.Join(dir, "bench")

	lm := NewLogManager()
	defer lm.CloseAll()

	for b.Loop() {
		key := []byte("test-table")
		log, release, err := lm.GetLog(key)
		if err != nil {
			b.Fatalf("Failed to get log: %v", err)
		}
		_ = log
		release()
	}
}

func BenchmarkLogManagerGetLogUnique(b *testing.B) {
	b.Skip("TODO: fix double b.Loop() usage and b.N sizing")
	dir := b.TempDir()
	options.CurrentOptions.DbFilename = filepath.Join(dir, "bench")

	lm := NewLogManager()
	defer lm.CloseAll()

	// Pre-generate keys
	keys := make([][]byte, b.N)
	for i := 0; b.Loop(); i++ {
		keys[i] = fmt.Appendf(nil, "key-%d", i)
	}

	for i := 0; b.Loop(); i++ {
		log, release, err := lm.GetLog(keys[i])
		if err != nil {
			b.Fatalf("Failed to get log: %v", err)
		}
		_ = log
		release()
	}
}

// TestBasicGetAndRelease tests basic acquire/release pattern
func TestBasicGetAndRelease(t *testing.T) {
	dir := t.TempDir()
	options.CurrentOptions.DbFilename = filepath.Join(dir, "test")

	manager := NewLogManager()
	defer manager.CloseAll()

	key := []byte("test-key")

	// Get log
	log, release, err := manager.GetLog(key)
	if err != nil {
		t.Fatalf("GetLog failed: %v", err)
	}
	if log == nil {
		t.Fatal("GetLog returned nil log")
	}

	// Check refcount
	handleVal, ok := manager.handles.Load(string(key))
	if !ok {
		t.Fatal("Handle not found in map")
	}
	handle := handleVal.(*logHandle)
	if handle.refCount.Load() != 1 {
		t.Errorf("Expected refcount=1, got %d", handle.refCount.Load())
	}

	// Release
	release()

	// Check refcount after release
	if handle.refCount.Load() != 0 {
		t.Errorf("Expected refcount=0 after release, got %d", handle.refCount.Load())
	}
}

// TestMultipleReferences tests multiple concurrent references to same log
func TestMultipleReferences(t *testing.T) {
	dir := t.TempDir()
	options.CurrentOptions.DbFilename = filepath.Join(dir, "test")

	manager := NewLogManager()
	defer manager.CloseAll()

	key := []byte("test-key")

	// Acquire 3 references
	log1, release1, err := manager.GetLog(key)
	if err != nil {
		t.Fatalf("GetLog failed: %v", err)
	}

	log2, release2, err := manager.GetLog(key)
	if err != nil {
		t.Fatalf("GetLog failed: %v", err)
	}

	log3, release3, err := manager.GetLog(key)
	if err != nil {
		t.Fatalf("GetLog failed: %v", err)
	}

	// All should return same log
	if log1 != log2 || log2 != log3 {
		t.Error("GetLog returned different log instances")
	}

	// Check refcount
	handleVal, _ := manager.handles.Load(string(key))
	handle := handleVal.(*logHandle)
	if handle.refCount.Load() != 3 {
		t.Errorf("Expected refcount=3, got %d", handle.refCount.Load())
	}

	// Release in order
	release1()
	if handle.refCount.Load() != 2 {
		t.Errorf("Expected refcount=2 after first release, got %d", handle.refCount.Load())
	}

	release2()
	if handle.refCount.Load() != 1 {
		t.Errorf("Expected refcount=1 after second release, got %d", handle.refCount.Load())
	}

	release3()
	if handle.refCount.Load() != 0 {
		t.Errorf("Expected refcount=0 after third release, got %d", handle.refCount.Load())
	}
}

// TestDoubleRelease tests that double-release is safe
func TestDoubleRelease(t *testing.T) {
	dir := t.TempDir()
	options.CurrentOptions.DbFilename = filepath.Join(dir, "test")

	manager := NewLogManager()
	defer manager.CloseAll()

	key := []byte("test-key")

	log, release, err := manager.GetLog(key)
	if err != nil {
		t.Fatalf("GetLog failed: %v", err)
	}
	if log == nil {
		t.Fatal("GetLog returned nil log")
	}

	// Release twice (should be safe)
	release()
	release()

	handleVal, _ := manager.handles.Load(string(key))
	handle := handleVal.(*logHandle)
	if handle.refCount.Load() != 0 {
		t.Errorf("Expected refcount=0 after double release, got %d", handle.refCount.Load())
	}
}

// TestConcurrentAccess tests concurrent access to same log
func TestConcurrentAccess(t *testing.T) {
	dir := t.TempDir()
	options.CurrentOptions.DbFilename = filepath.Join(dir, "test")

	manager := NewLogManager()
	defer manager.CloseAll()

	key := []byte("test-key")
	numGoroutines := 100

	var wg sync.WaitGroup
	errChan := make(chan error, numGoroutines)

	for i := range numGoroutines {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()

			log, release, err := manager.GetLog(key)
			if err != nil {
				errChan <- fmt.Errorf("goroutine %d: GetLog failed: %v", id, err)
				return
			}
			defer release()

			// Use the log (write and read)
			ballot := Ballot{ID: uint64(id), NodeID: 1}
			value := fmt.Appendf(nil, "value-%d", id)

			err = log.Accept(uint64(id), ballot, value)
			if err != nil {
				errChan <- fmt.Errorf("goroutine %d: Accept failed: %v", id, err)
				return
			}

			// Small delay to simulate work
			time.Sleep(time.Millisecond)
		}(i)
	}

	wg.Wait()
	close(errChan)

	// Check for errors
	for err := range errChan {
		t.Error(err)
	}

	// Check final refcount
	handleVal, ok := manager.handles.Load(string(key))
	if !ok {
		t.Fatal("Handle not found")
	}
	handle := handleVal.(*logHandle)
	if handle.refCount.Load() != 0 {
		t.Errorf("Expected final refcount=0, got %d", handle.refCount.Load())
	}
}

// TestLRUEviction tests that LRU eviction respects active references
func TestLRUEviction(t *testing.T) {
	dir := t.TempDir()
	options.CurrentOptions.DbFilename = filepath.Join(dir, "test")

	manager := NewLogManager()
	defer manager.CloseAll()

	// Override max for testing
	manager.maxOpen = 5

	// Create 5 logs
	keys := make([][]byte, 5)
	releases := make([]func(), 5)

	for i := range 5 {
		keys[i] = fmt.Appendf(nil, "key-%d", i)
		log, release, err := manager.GetLog(keys[i])
		if err != nil {
			t.Fatalf("GetLog failed: %v", err)
		}
		if log == nil {
			t.Fatal("GetLog returned nil")
		}
		releases[i] = release
	}

	// All 5 should be open
	stats := manager.Stats()
	if stats.OpenLogs != 5 {
		t.Errorf("Expected 5 open logs, got %d", stats.OpenLogs)
	}

	// Release the first 4 (but keep last one)
	for i := range 4 {
		releases[i]()
	}

	// Create a 6th log - should evict one of the first 4 (not the 5th which is still in use)
	sixthKey := []byte("key-6")
	log6, release6, err := manager.GetLog(sixthKey)
	if err != nil {
		t.Fatalf("GetLog for 6th key failed: %v", err)
	}
	if log6 == nil {
		t.Fatal("GetLog returned nil for 6th key")
	}
	defer release6()

	// Should still have 5 open (evicted one, added one)
	// Give it a moment for background close
	time.Sleep(200 * time.Millisecond)

	stats = manager.Stats()
	if stats.OpenLogs > 5 {
		t.Errorf("Expected <= 5 open logs after eviction, got %d", stats.OpenLogs)
	}

	// The 5th key should still be in handles (it's still referenced)
	if _, ok := manager.handles.Load(string(keys[4])); !ok {
		t.Error("Active log was evicted!")
	}

	// Release the 5th
	releases[4]()
}

// TestNoEvictionWithActiveRefs tests that logs with active refs are never evicted
func TestNoEvictionWithActiveRefs(t *testing.T) {
	dir := t.TempDir()
	options.CurrentOptions.DbFilename = filepath.Join(dir, "test")

	manager := NewLogManager()
	defer manager.CloseAll()

	manager.maxOpen = 3

	// Get 3 logs and hold references
	keys := make([][]byte, 3)
	releases := make([]func(), 3)

	for i := range 3 {
		keys[i] = fmt.Appendf(nil, "key-%d", i)
		_, release, err := manager.GetLog(keys[i])
		if err != nil {
			t.Fatalf("GetLog failed: %v", err)
		}
		releases[i] = release
	}

	// Try to create 3 more logs while holding refs
	for i := 3; i < 6; i++ {
		key := fmt.Appendf(nil, "key-%d", i)
		log, release, err := manager.GetLog(key)
		if err != nil {
			t.Fatalf("GetLog failed: %v", err)
		}
		defer release()
		if log == nil {
			t.Fatal("GetLog returned nil")
		}
	}

	// All original 3 should still be in handles (they have active refs)
	for i := range 3 {
		if _, ok := manager.handles.Load(string(keys[i])); !ok {
			t.Errorf("Log with active ref (key-%d) was evicted!", i)
		}
	}

	// Release all
	for i := range 3 {
		releases[i]()
	}
}

// TestCloseAll tests graceful shutdown
func TestCloseAll(t *testing.T) {
	dir := t.TempDir()
	options.CurrentOptions.DbFilename = filepath.Join(dir, "test")

	manager := NewLogManager()

	// Create multiple logs
	numLogs := 10
	for i := range numLogs {
		key := fmt.Appendf(nil, "key-%d", i)
		log, release, err := manager.GetLog(key)
		if err != nil {
			t.Fatalf("GetLog failed: %v", err)
		}
		if log == nil {
			t.Fatal("GetLog returned nil")
		}
		release()
	}

	// Close all
	err := manager.CloseAll()
	if err != nil {
		t.Errorf("CloseAll failed: %v", err)
	}

	// Manager should be closed
	if !manager.closed.Load() {
		t.Error("Manager not marked as closed")
	}

	// Subsequent GetLog should fail
	_, _, err = manager.GetLog([]byte("new-key"))
	if err != ErrClosed {
		t.Errorf("Expected ErrClosed, got %v", err)
	}
}

// TestStressTest hammers the manager with concurrent access
func TestStressTest(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping stress test in short mode")
	}

	dir := t.TempDir()
	options.CurrentOptions.DbFilename = filepath.Join(dir, "test")

	manager := NewLogManager()
	defer manager.CloseAll()

	numGoroutines := 100
	numKeys := 20
	duration := 2 * time.Second

	var wg sync.WaitGroup
	stop := atomic.Bool{}
	errors := atomic.Int32{}

	// Worker goroutines
	for i := range numGoroutines {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()

			for !stop.Load() {
				// Random key
				keyID := workerID % numKeys
				key := fmt.Appendf(nil, "stress-key-%d", keyID)

				log, release, err := manager.GetLog(key)
				if err != nil {
					t.Logf("Worker %d: GetLog failed: %v", workerID, err)
					errors.Add(1)
					continue
				}

				// Do some work - accept then commit
				slot := uint64(workerID)*1000 + uint64(time.Now().UnixNano()%1000)
				ballot := Ballot{ID: uint64(workerID), NodeID: 1}
				value := []byte{1} // Small payload for 2MB buffer

				err = log.Accept(slot, ballot, value)
				if err != nil {
					t.Logf("Worker %d: Accept(slot=%d) failed: %v", workerID, slot, err)
					errors.Add(1)
					release()
					continue
				}

				// Commit to free up mutable buffer space
				err = log.Commit(slot)
				if err != nil {
					t.Logf("Worker %d: Commit(slot=%d) failed: %v", workerID, slot, err)
					errors.Add(1)
				}

				release()

				// Periodically checkpoint to reclaim buffer space
				if slot%100 == 0 {
					_ = log.Checkpoint()
				}

				// Small random delay
				time.Sleep(time.Millisecond * time.Duration(workerID%10))
			}
		}(i)
	}

	// Let it run
	time.Sleep(duration)
	stop.Store(true)
	wg.Wait()

	if errors.Load() > 0 {
		t.Errorf("Stress test encountered %d errors", errors.Load())
	}

	// Check stats
	stats := manager.Stats()
	if stats.ActiveRefs != 0 {
		t.Errorf("Expected 0 active refs after stress test, got %d", stats.ActiveRefs)
	}

	t.Logf("Stress test stats: TotalLogs=%d, OpenLogs=%d, ActiveRefs=%d",
		stats.TotalLogs, stats.OpenLogs, stats.ActiveRefs)
}

// TestPanicRecovery tests that panics don't leak references
func TestPanicRecovery(t *testing.T) {
	dir := t.TempDir()
	options.CurrentOptions.DbFilename = filepath.Join(dir, "test")

	manager := NewLogManager()
	defer manager.CloseAll()

	key := []byte("panic-key")

	// Function that panics
	func() {
		defer func() {
			if r := recover(); r == nil {
				t.Error("Expected panic")
			}
		}()

		log, release, err := manager.GetLog(key)
		if err != nil {
			t.Fatalf("GetLog failed: %v", err)
		}
		defer release() // Should be called by panic recovery

		_ = log

		panic("test panic")
	}()

	// Check that reference was released
	time.Sleep(100 * time.Millisecond)

	handleVal, ok := manager.handles.Load(string(key))
	if !ok {
		t.Fatal("Handle not found")
	}
	handle := handleVal.(*logHandle)
	if handle.refCount.Load() != 0 {
		t.Errorf("Expected refcount=0 after panic recovery, got %d", handle.refCount.Load())
	}
}

// TestNoForceCloseWithActiveRefs verifies we never close a log with active references
func TestNoForceCloseWithActiveRefs(t *testing.T) {
	dir := t.TempDir()
	options.CurrentOptions.DbFilename = filepath.Join(dir, "test")

	manager := NewLogManager()
	manager.maxOpen = 2 // Very small limit to force eviction

	// Get 2 logs and keep them referenced
	key1 := []byte("key-1")
	log1, release1, err := manager.GetLog(key1)
	if err != nil {
		t.Fatalf("GetLog failed: %v", err)
	}
	// DON'T release yet

	key2 := []byte("key-2")
	log2, release2, err := manager.GetLog(key2)
	if err != nil {
		t.Fatalf("GetLog failed: %v", err)
	}
	// DON'T release yet

	// Try to get a 3rd log - should succeed despite being over limit
	// because we can't evict logs with active refs
	key3 := []byte("key-3")
	log3, release3, err := manager.GetLog(key3)
	if err != nil {
		t.Fatalf("GetLog failed: %v", err)
	}
	defer release3()

	// Verify all 3 logs are still usable
	ballot := Ballot{ID: 1, NodeID: 1}
	if err := log1.Accept(1, ballot, []byte("test1")); err != nil {
		t.Errorf("log1 unusable: %v", err)
	}
	if err := log2.Accept(2, ballot, []byte("test2")); err != nil {
		t.Errorf("log2 unusable: %v", err)
	}
	if err := log3.Accept(3, ballot, []byte("test3")); err != nil {
		t.Errorf("log3 unusable: %v", err)
	}

	// All logs should still be in handles
	if _, ok := manager.handles.Load(string(key1)); !ok {
		t.Error("log1 was evicted despite active ref!")
	}
	if _, ok := manager.handles.Load(string(key2)); !ok {
		t.Error("log2 was evicted despite active ref!")
	}
	if _, ok := manager.handles.Load(string(key3)); !ok {
		t.Error("log3 was evicted despite active ref!")
	}

	// Now release and verify they can be evicted
	release1()
	release2()

	// Get a 4th log - should now evict one of the first two
	key4 := []byte("key-4")
	_, release4, err := manager.GetLog(key4)
	if err != nil {
		t.Fatalf("GetLog failed: %v", err)
	}
	defer release4()

	// Give eviction time to happen
	time.Sleep(200 * time.Millisecond)

	// At least one of the first two should have been evicted
	_, has1 := manager.handles.Load(string(key1))
	_, has2 := manager.handles.Load(string(key2))

	if has1 && has2 {
		t.Error("Expected at least one log to be evicted, but both still present")
	}

	t.Logf("Successfully prevented force-close: has1=%v, has2=%v", has1, has2)
}

// TestCloseAllWithLeakedRefs verifies CloseAll doesn't force-close leaked references
func TestCloseAllWithLeakedRefs(t *testing.T) {
	dir := t.TempDir()
	options.CurrentOptions.DbFilename = filepath.Join(dir, "test")

	manager := NewLogManager()

	key := []byte("leaked-key")
	log, release, err := manager.GetLog(key)
	if err != nil {
		t.Fatalf("GetLog failed: %v", err)
	}

	// Simulate a leaked reference by not calling release
	_ = log
	_ = release // Don't call it!

	// Try to close all - should timeout but not crash
	err = manager.CloseAll()
	if err == nil {
		t.Error("Expected error from CloseAll due to leaked reference")
	}

	// Verify the error message mentions the leak
	if !strings.Contains(err.Error(), "timeout waiting for references") {
		t.Errorf("Expected timeout error, got: %v", err)
	}

	t.Logf("CloseAll correctly detected leaked reference: %v", err)

	// The log should still be usable (not force-closed)
	ballot := Ballot{ID: 1, NodeID: 1}
	err = log.Accept(1, ballot, []byte("after-close-all"))
	if err == nil {
		t.Log("Log still usable after CloseAll timeout (correct - we didn't force-close)")
	} else {
		// This is also acceptable - the manager is closed so new operations may fail
		t.Logf("Log operation failed after CloseAll: %v", err)
	}
}

// TestRefCountNeverNegative verifies that excessive releases don't make refcount negative
func TestRefCountNeverNegative(t *testing.T) {
	dir := t.TempDir()
	options.CurrentOptions.DbFilename = filepath.Join(dir, "test")

	manager := NewLogManager()
	defer manager.CloseAll()

	key := []byte("test-key")
	log, release, err := manager.GetLog(key)
	if err != nil {
		t.Fatalf("GetLog failed: %v", err)
	}

	// Get the handle to check refcount directly
	handleVal, ok := manager.handles.Load(string(key))
	if !ok {
		t.Fatal("Handle not found")
	}
	handle := handleVal.(*logHandle)

	// Initial refcount should be 1
	if handle.refCount.Load() != 1 {
		t.Errorf("Expected initial refcount=1, got %d", handle.refCount.Load())
	}

	// Normal release
	release()

	if handle.refCount.Load() != 0 {
		t.Errorf("Expected refcount=0 after release, got %d", handle.refCount.Load())
	}

	// Call release again (should be safe due to makeReleaseFunc guard)
	release()

	if handle.refCount.Load() != 0 {
		t.Errorf("Expected refcount=0 after double-release, got %d", handle.refCount.Load())
	}

	// Directly call release on handle (bypassing makeReleaseFunc protection)
	handle.release()

	refCount := handle.refCount.Load()
	if refCount < 0 {
		t.Errorf("RefCount went negative: %d", refCount)
	}
	if refCount != 0 {
		t.Errorf("Expected refcount to stay at 0, got %d", refCount)
	}

	// Call release many more times
	for range 100 {
		handle.release()
	}

	refCount = handle.refCount.Load()
	if refCount < 0 {
		t.Errorf("RefCount went negative after 100 releases: %d", refCount)
	}
	if refCount != 0 {
		t.Errorf("Expected refcount to stay at 0, got %d", refCount)
	}

	// Verify the log can still be used (not evicted due to negative refcount)
	log2, release2, err := manager.GetLog(key)
	if err != nil {
		t.Fatalf("GetLog failed after excessive releases: %v", err)
	}
	defer release2()

	if log != log2 {
		t.Error("Got different log instance (original may have been evicted!)")
	}

	t.Log("RefCount successfully protected from going negative")
}

// TestConcurrentReleases verifies concurrent releases don't cause negative refcount
func TestConcurrentReleases(t *testing.T) {
	dir := t.TempDir()
	options.CurrentOptions.DbFilename = filepath.Join(dir, "test")

	manager := NewLogManager()
	defer manager.CloseAll()

	key := []byte("concurrent-key")

	// Acquire multiple references
	numRefs := 100
	releases := make([]func(), numRefs)

	for i := range numRefs {
		_, release, err := manager.GetLog(key)
		if err != nil {
			t.Fatalf("GetLog failed: %v", err)
		}
		releases[i] = release
	}

	// Get handle to check refcount
	handleVal, ok := manager.handles.Load(string(key))
	if !ok {
		t.Fatal("Handle not found")
	}
	handle := handleVal.(*logHandle)

	if handle.refCount.Load() != int32(numRefs) {
		t.Errorf("Expected refcount=%d, got %d", numRefs, handle.refCount.Load())
	}

	// Release all concurrently
	var wg sync.WaitGroup
	for i := range numRefs {
		wg.Add(1)
		go func(release func()) {
			defer wg.Done()
			release()
		}(releases[i])
	}

	wg.Wait()

	// Check final refcount
	finalRefCount := handle.refCount.Load()
	if finalRefCount < 0 {
		t.Errorf("RefCount went negative after concurrent releases: %d", finalRefCount)
	}
	if finalRefCount != 0 {
		t.Errorf("Expected final refcount=0, got %d", finalRefCount)
	}

	t.Logf("Concurrent releases completed successfully, final refcount=%d", finalRefCount)
}
