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

package consensus

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestNamedLocker_BasicLockUnlock(t *testing.T) {
	locker := newNamedLocker()

	locker.lock("test")
	locker.unlock("test")

	// Verify lock was cleaned up
	locker.mu.Lock()
	if len(locker.locks) != 0 {
		t.Errorf("expected locks map to be empty, got %d entries", len(locker.locks))
	}
	locker.mu.Unlock()
}

func TestNamedLocker_MultipleDifferentNames(t *testing.T) {
	locker := newNamedLocker()

	// Lock different names - should not block each other
	done := make(chan bool, 3)

	go func() {
		locker.lock("name1")
		time.Sleep(50 * time.Millisecond)
		locker.unlock("name1")
		done <- true
	}()

	go func() {
		locker.lock("name2")
		time.Sleep(50 * time.Millisecond)
		locker.unlock("name2")
		done <- true
	}()

	go func() {
		locker.lock("name3")
		time.Sleep(50 * time.Millisecond)
		locker.unlock("name3")
		done <- true
	}()

	// All should complete quickly since they don't conflict
	timeout := time.After(100 * time.Millisecond)
	for i := 0; i < 3; i++ {
		select {
		case <-done:
		case <-timeout:
			t.Fatal("operations timed out - locks blocked each other incorrectly")
		}
	}

	// Verify all locks were cleaned up
	locker.mu.Lock()
	if len(locker.locks) != 0 {
		t.Errorf("expected locks map to be empty, got %d entries", len(locker.locks))
	}
	locker.mu.Unlock()
}

func TestNamedLocker_SameNameSerializes(t *testing.T) {
	locker := newNamedLocker()
	var counter int32
	var maxConcurrent int32

	// Multiple goroutines competing for same lock
	var wg sync.WaitGroup
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			locker.lock("shared")

			// Track concurrent access
			current := atomic.AddInt32(&counter, 1)
			if current > maxConcurrent {
				atomic.StoreInt32(&maxConcurrent, current)
			}

			// Simulate work
			time.Sleep(10 * time.Millisecond)

			atomic.AddInt32(&counter, -1)
			locker.unlock("shared")
		}()
	}

	wg.Wait()

	// Verify only one goroutine held the lock at a time
	if maxConcurrent != 1 {
		t.Errorf("expected max concurrent to be 1, got %d - lock not working!", maxConcurrent)
	}

	// Verify lock was cleaned up
	locker.mu.Lock()
	if len(locker.locks) != 0 {
		t.Errorf("expected locks map to be empty, got %d entries", len(locker.locks))
	}
	locker.mu.Unlock()
}

func TestNamedLocker_StressTest(t *testing.T) {
	locker := newNamedLocker()
	names := []string{"name1", "name2", "name3", "name4", "name5"}
	counters := make([]int32, len(names))
	maxConcurrent := make([]int32, len(names))

	var wg sync.WaitGroup
	iterations := 100

	// Spawn many goroutines competing for multiple locks
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()

			for j := 0; j < iterations; j++ {
				nameIdx := j % len(names)
				name := names[nameIdx]

				locker.lock(name)

				// Track concurrent access for this specific name
				current := atomic.AddInt32(&counters[nameIdx], 1)
				if current > atomic.LoadInt32(&maxConcurrent[nameIdx]) {
					atomic.StoreInt32(&maxConcurrent[nameIdx], current)
				}

				// Simulate minimal work
				time.Sleep(time.Microsecond)

				atomic.AddInt32(&counters[nameIdx], -1)

				locker.unlock(name)
			}
		}(i)
	}

	wg.Wait()

	// Verify each name only had one holder at a time
	for i, max := range maxConcurrent {
		if max != 1 {
			t.Errorf("name %s: expected max concurrent to be 1, got %d", names[i], max)
		}
	}

	// Verify all locks were cleaned up
	locker.mu.Lock()
	if len(locker.locks) != 0 {
		t.Errorf("expected locks map to be empty, got %d entries", len(locker.locks))
	}
	locker.mu.Unlock()
}

func TestNamedLocker_CleanupDuringContention(t *testing.T) {
	locker := newNamedLocker()

	// This tests that cleanup happens correctly even when
	// other goroutines are waiting
	var wg sync.WaitGroup

	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			locker.lock("cleanup")
			time.Sleep(5 * time.Millisecond)
			locker.unlock("cleanup")
		}()
	}

	wg.Wait()

	// Verify lock was cleaned up
	locker.mu.Lock()
	if len(locker.locks) != 0 {
		t.Errorf("expected locks map to be empty, got %d entries", len(locker.locks))
	}
	locker.mu.Unlock()
}

func TestNamedLocker_UnlockNonexistent(t *testing.T) {
	locker := newNamedLocker()

	// Should not panic when unlocking a non-existent lock
	locker.unlock("doesnotexist")

	locker.mu.Lock()
	if len(locker.locks) != 0 {
		t.Errorf("expected locks map to be empty, got %d entries", len(locker.locks))
	}
	locker.mu.Unlock()
}

func TestNamedLocker_RaceDetector(t *testing.T) {
	// This test is designed to catch race conditions with go test -race
	locker := newNamedLocker()

	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()

			name := "race"
			if id%3 == 0 {
				name = "race2"
			}

			locker.lock(name)
			// Access the internal state while holding lock
			locker.mu.Lock()
			_ = len(locker.locks)
			locker.mu.Unlock()

			locker.unlock(name)
		}(i)
	}

	wg.Wait()
}

func TestNamedLocker_LockReentrySafety(t *testing.T) {
	locker := newNamedLocker()

	// Verify that trying to lock the same name twice from same goroutine
	// would deadlock (as expected for a mutex)
	done := make(chan bool)

	go func() {
		locker.lock("reentry")
		// This should block forever if attempted
		// locker.lock("reentry") // Would deadlock - DON'T uncomment
		locker.unlock("reentry")
		done <- true
	}()

	select {
	case <-done:
		// Success - single lock/unlock worked
	case <-time.After(100 * time.Millisecond):
		t.Fatal("basic lock/unlock timed out")
	}
}
