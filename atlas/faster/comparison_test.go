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
	"context"
	"fmt"
	"math/rand"
	"testing"

	"github.com/dgraph-io/badger/v4"
)

// BenchmarkComparison_RandomKeys_FASTER benchmarks FASTER with random key writes
// This simulates realistic WPaxos workload: many objects, random access
func BenchmarkComparison_RandomKeys_FASTER(b *testing.B) {
	// Use LogManager to handle multiple logs (prevents FD exhaustion)
	dir := b.TempDir()
	mgr := NewLogManagerWithDir(dir)
	defer mgr.CloseAll()

	ballot := Ballot{ID: 1, NodeID: 1}
	value := []byte{1} // 1 byte payload (WPaxos stores metadata, actual data elsewhere)

	// Use fixed seed for reproducibility
	rng := rand.New(rand.NewSource(42))
	numKeys := 100 // 100 different keys (objects in WPaxos)

	b.ReportAllocs()

	for i := 0; b.Loop(); i++ {
		// Random key (simulates multi-object consensus)
		keyID := rng.Intn(numKeys)
		key := fmt.Appendf(nil, "table:%04d", keyID)

		// Get log for this key (with reference counting)
		log, release, err := mgr.GetLog(key)
		if err != nil {
			b.Fatalf("Failed to get log: %v", err)
		}

		// Use a slot based on iteration (ensures unique slots per key)
		slot := uint64(i*numKeys + keyID)

		// Accept (Phase-2)
		err = log.Accept(slot, ballot, value)
		if err != nil {
			release()
			b.Fatalf("Failed to accept: %v", err)
		}

		// Commit (Phase-3)
		err = log.Commit(slot)
		if err != nil {
			release()
			b.Fatalf("Failed to commit: %v", err)
		}

		release() // Always release when done
	}

	b.StopTimer()
	ops := float64(b.N) / b.Elapsed().Seconds()
	b.ReportMetric(ops, "ops/sec")
}

// BenchmarkComparison_RandomKeys_BadgerDB benchmarks BadgerDB with same workload
func BenchmarkComparison_RandomKeys_BadgerDB(b *testing.B) {
	dir := b.TempDir()

	opts := badger.DefaultOptions(dir)
	opts.SyncWrites = false // Fair comparison (no fsync)
	opts.Logger = nil       // Disable logging

	db, err := badger.Open(opts)
	if err != nil {
		b.Fatalf("Failed to open BadgerDB: %v", err)
	}
	defer db.Close()

	value := []byte{1} // 1 byte payload to match FASTER benchmarks

	// Use fixed seed for reproducibility
	rng := rand.New(rand.NewSource(42))
	keySpace := uint64(10000)

	b.ReportAllocs()

	for b.Loop() {
		slot := uint64(rng.Int63n(int64(keySpace)))
		key := fmt.Appendf(nil, "slot:%020d", slot)

		// Write (Accept + Commit combined in BadgerDB)
		err := db.Update(func(txn *badger.Txn) error {
			return txn.Set(key, value)
		})
		if err != nil {
			b.Fatalf("Failed to write: %v", err)
		}
	}

	b.StopTimer()
	ops := float64(b.N) / b.Elapsed().Seconds()
	b.ReportMetric(ops, "ops/sec")
}

// BenchmarkComparison_SequentialWrites_FASTER tests sequential slot writes
func BenchmarkComparison_SequentialWrites_FASTER(b *testing.B) {
	dir := b.TempDir()
	mgr := NewLogManagerWithDir(dir)
	defer mgr.CloseAll()

	ballot := Ballot{ID: 1, NodeID: 1}
	value := []byte{1} // 1 byte payload to match FASTER benchmarks
	key := []byte("single-table")

	// Get log once (single table, sequential writes)
	log, release, err := mgr.GetLog(key)
	if err != nil {
		b.Fatalf("Failed to get log: %v", err)
	}
	defer release()

	b.ReportAllocs()

	for i := 0; b.Loop(); i++ {
		slot := uint64(i)

		err := log.Accept(slot, ballot, value)
		if err != nil {
			b.Fatalf("Failed to accept: %v", err)
		}

		err = log.Commit(slot)
		if err != nil {
			b.Fatalf("Failed to commit: %v", err)
		}
	}

	b.StopTimer()
	ops := float64(b.N) / b.Elapsed().Seconds()
	b.ReportMetric(ops, "ops/sec")
}

// BenchmarkComparison_SequentialWrites_BadgerDB tests sequential writes
func BenchmarkComparison_SequentialWrites_BadgerDB(b *testing.B) {
	dir := b.TempDir()

	opts := badger.DefaultOptions(dir)
	opts.SyncWrites = false
	opts.Logger = nil

	db, err := badger.Open(opts)
	if err != nil {
		b.Fatalf("Failed to open BadgerDB: %v", err)
	}
	defer db.Close()

	value := []byte{1} // 1 byte payload to match FASTER benchmarks

	b.ReportAllocs()

	for i := 0; b.Loop(); i++ {
		key := fmt.Appendf(nil, "slot:%020d", i)

		err := db.Update(func(txn *badger.Txn) error {
			return txn.Set(key, value)
		})
		if err != nil {
			b.Fatalf("Failed to write: %v", err)
		}
	}

	b.StopTimer()
	ops := float64(b.N) / b.Elapsed().Seconds()
	b.ReportMetric(ops, "ops/sec")
}

// BenchmarkComparison_ReadHeavy_FASTER tests read-heavy workload
// Realistic: State machine reads are common in consensus systems
func BenchmarkComparison_ReadHeavy_FASTER(b *testing.B) {
	dir := b.TempDir()
	mgr := NewLogManagerWithDir(dir)
	defer mgr.CloseAll()

	// Prepare data: 10K committed entries
	ballot := Ballot{ID: 1, NodeID: 1}
	value := []byte{1} // 1 byte payload to match FASTER benchmarks
	numEntries := 10000
	key := []byte("read-table")

	log, release, err := mgr.GetLog(key)
	if err != nil {
		b.Fatalf("Failed to get log: %v", err)
	}

	for i := range numEntries {
		slot := uint64(i)
		log.Accept(slot, ballot, value)
		log.Commit(slot)
	}

	rng := rand.New(rand.NewSource(42))

	b.ReportAllocs()

	for b.Loop() {
		slot := uint64(rng.Int63n(int64(numEntries)))

		_, err := log.ReadCommittedOnly(slot)
		if err != nil {
			b.Fatalf("Failed to read: %v", err)
		}
	}

	b.StopTimer()
	release()
	ops := float64(b.N) / b.Elapsed().Seconds()
	b.ReportMetric(ops, "ops/sec")
}

// BenchmarkComparison_ReadHeavy_BadgerDB tests read-heavy workload
func BenchmarkComparison_ReadHeavy_BadgerDB(b *testing.B) {
	dir := b.TempDir()

	opts := badger.DefaultOptions(dir)
	opts.SyncWrites = false
	opts.Logger = nil

	db, err := badger.Open(opts)
	if err != nil {
		b.Fatalf("Failed to open BadgerDB: %v", err)
	}
	defer db.Close()

	// Prepare data
	value := []byte{1} // 1 byte payload to match FASTER benchmarks
	numEntries := 10000

	for i := range numEntries {
		key := fmt.Appendf(nil, "slot:%020d", i)
		db.Update(func(txn *badger.Txn) error {
			return txn.Set(key, value)
		})
	}

	rng := rand.New(rand.NewSource(42))

	b.ReportAllocs()

	for b.Loop() {
		slot := uint64(rng.Int63n(int64(numEntries)))
		key := fmt.Appendf(nil, "slot:%020d", slot)

		err := db.View(func(txn *badger.Txn) error {
			item, err := txn.Get(key)
			if err != nil {
				return err
			}
			return item.Value(func(val []byte) error {
				return nil
			})
		})
		if err != nil {
			b.Fatalf("Failed to read: %v", err)
		}
	}

	b.StopTimer()
	ops := float64(b.N) / b.Elapsed().Seconds()
	b.ReportMetric(ops, "ops/sec")
}

// BenchmarkComparison_Mixed_FASTER tests mixed read/write workload
// Realistic: 70% reads, 30% writes (typical for databases)
func BenchmarkComparison_Mixed_FASTER(b *testing.B) {
	dir := b.TempDir()
	mgr := NewLogManagerWithDir(dir)
	defer mgr.CloseAll()

	// Prepare some initial data
	ballot := Ballot{ID: 1, NodeID: 1}
	value := []byte{1} // 1 byte payload to match FASTER benchmarks
	initialEntries := 5000
	key := []byte("mixed-table")

	log, release, err := mgr.GetLog(key)
	if err != nil {
		b.Fatalf("Failed to get log: %v", err)
	}

	for i := range initialEntries {
		slot := uint64(i)
		log.Accept(slot, ballot, value)
		log.Commit(slot)
	}

	rng := rand.New(rand.NewSource(42))
	writeSlot := uint64(initialEntries)

	b.ReportAllocs()

	for b.Loop() {
		if rng.Float64() < 0.7 {
			// Read (70%)
			slot := uint64(rng.Int63n(int64(writeSlot)))
			_, _ = log.ReadCommittedOnly(slot)
		} else {
			// Write (30%)
			log.Accept(writeSlot, ballot, value)
			log.Commit(writeSlot)
			writeSlot++
		}
	}

	b.StopTimer()
	release()
	ops := float64(b.N) / b.Elapsed().Seconds()
	b.ReportMetric(ops, "ops/sec")
}

// BenchmarkComparison_Mixed_BadgerDB tests mixed read/write workload
func BenchmarkComparison_Mixed_BadgerDB(b *testing.B) {
	dir := b.TempDir()

	opts := badger.DefaultOptions(dir)
	opts.SyncWrites = false
	opts.Logger = nil

	db, err := badger.Open(opts)
	if err != nil {
		b.Fatalf("Failed to open BadgerDB: %v", err)
	}
	defer db.Close()

	// Prepare initial data
	value := []byte{1} // 1 byte payload to match FASTER benchmarks
	initialEntries := 5000

	for i := range initialEntries {
		key := fmt.Appendf(nil, "slot:%020d", i)
		db.Update(func(txn *badger.Txn) error {
			return txn.Set(key, value)
		})
	}

	rng := rand.New(rand.NewSource(42))
	writeSlot := initialEntries

	b.ReportAllocs()

	for b.Loop() {
		if rng.Float64() < 0.7 {
			// Read (70%)
			slot := rng.Intn(writeSlot)
			key := fmt.Appendf(nil, "slot:%020d", slot)

			db.View(func(txn *badger.Txn) error {
				item, err := txn.Get(key)
				if err != nil {
					return err
				}
				return item.Value(func(val []byte) error {
					return nil
				})
			})
		} else {
			// Write (30%)
			key := fmt.Appendf(nil, "slot:%020d", writeSlot)
			db.Update(func(txn *badger.Txn) error {
				return txn.Set(key, value)
			})
			writeSlot++
		}
	}

	b.StopTimer()
	ops := float64(b.N) / b.Elapsed().Seconds()
	b.ReportMetric(ops, "ops/sec")
}

// BenchmarkComparison_ConcurrentWrites_FASTER tests parallel write performance
func BenchmarkComparison_ConcurrentWrites_FASTER(b *testing.B) {
	dir := b.TempDir()
	mgr := NewLogManagerWithDir(dir)
	defer mgr.CloseAll()

	ballot := Ballot{ID: 1, NodeID: 1}
	value := []byte{1} // 1 byte payload to match FASTER benchmarks

	b.ResetTimer()
	b.ReportAllocs()

	// Each goroutine writes to different keys (simulates multi-object WPaxos)
	b.RunParallel(func(pb *testing.PB) {
		localSlot := uint64(0)
		rng := rand.New(rand.NewSource(rand.Int63()))
		goroutineID := rng.Intn(10000)

		for pb.Next() {
			// Each goroutine has its own key (prevents conflicts)
			key := fmt.Appendf(nil, "concurrent-table:%04d", goroutineID)

			log, release, err := mgr.GetLog(key)
			if err != nil {
				b.Fatalf("Failed to get log: %v", err)
			}

			log.Accept(localSlot, ballot, value)
			log.Commit(localSlot)

			localSlot++

			release()
		}
	})

	b.StopTimer()
	ops := float64(b.N) / b.Elapsed().Seconds()
	b.ReportMetric(ops, "ops/sec")
}

// BenchmarkComparison_ConcurrentWrites_BadgerDB tests parallel write performance
func BenchmarkComparison_ConcurrentWrites_BadgerDB(b *testing.B) {
	dir := b.TempDir()

	opts := badger.DefaultOptions(dir)
	opts.SyncWrites = false
	opts.Logger = nil

	db, err := badger.Open(opts)
	if err != nil {
		b.Fatalf("Failed to open BadgerDB: %v", err)
	}
	defer db.Close()

	value := []byte{1} // 1 byte payload to match FASTER benchmarks

	b.ResetTimer()
	b.ReportAllocs()

	b.RunParallel(func(pb *testing.PB) {
		localSlot := uint64(0)
		rng := rand.New(rand.NewSource(rand.Int63()))

		for pb.Next() {
			slot := (uint64(rng.Int63()) << 32) | localSlot
			key := fmt.Appendf(nil, "slot:%020d", slot)

			db.Update(func(txn *badger.Txn) error {
				return txn.Set(key, value)
			})

			localSlot++
		}
	})

	b.StopTimer()
	ops := float64(b.N) / b.Elapsed().Seconds()
	b.ReportMetric(ops, "ops/sec")
}

// BenchmarkComparison_Recovery_FASTER tests recovery time
func BenchmarkComparison_Recovery_FASTER(b *testing.B) {
	// Create and populate log once
	dir := b.TempDir()
	mgr := NewLogManagerWithDir(dir)
	ballot := Ballot{ID: 1, NodeID: 1}
	value := []byte{1} // 1 byte payload to match FASTER benchmarks
	numEntries := 10000
	key := []byte("recovery-table")

	log, release, err := mgr.GetLog(key)
	if err != nil {
		b.Fatalf("Failed to get log: %v", err)
	}

	for i := range numEntries {
		slot := uint64(i)
		log.Accept(slot, ballot, value)
		log.Commit(slot)
	}
	release()
	mgr.CloseAll()

	b.ReportAllocs()

	for b.Loop() {
		// Recovery = reopen log manager and get log (same dir!)
		mgr := NewLogManagerWithDir(dir)
		_, release, err := mgr.GetLog(key)
		if err != nil {
			b.Fatalf("Failed to recover: %v", err)
		}
		release()
		mgr.CloseAll()
	}

	b.StopTimer()
	recoveryTime := b.Elapsed().Seconds() / float64(b.N)
	b.ReportMetric(recoveryTime*1000, "ms/recovery")
}

// BenchmarkComparison_Recovery_BadgerDB tests recovery time
func BenchmarkComparison_Recovery_BadgerDB(b *testing.B) {
	dir := b.TempDir()

	opts := badger.DefaultOptions(dir)
	opts.SyncWrites = true
	opts.Logger = nil

	// Create and populate DB once
	db, err := badger.Open(opts)
	if err != nil {
		b.Fatalf("Failed to open BadgerDB: %v", err)
	}

	value := []byte{1} // 1 byte payload to match FASTER benchmarks
	numEntries := 10000

	for i := range numEntries {
		key := fmt.Appendf(nil, "slot:%020d", i)
		db.Update(func(txn *badger.Txn) error {
			return txn.Set(key, value)
		})
	}
	db.Close()

	b.ReportAllocs()

	for b.Loop() {
		db, err := badger.Open(opts)
		if err != nil {
			b.Fatalf("Failed to open: %v", err)
		}
		db.Close()
	}

	b.StopTimer()
	recoveryTime := b.Elapsed().Seconds() / float64(b.N)
	b.ReportMetric(recoveryTime*1000, "ms/recovery")
}

// BenchmarkComparison_ScanUncommitted_FASTER tests Phase-1 recovery scan
func BenchmarkComparison_ScanUncommitted_FASTER(b *testing.B) {
	dir := b.TempDir()
	mgr := NewLogManagerWithDir(dir)
	defer mgr.CloseAll()

	// Create mix of committed and uncommitted entries
	ballot := Ballot{ID: 1, NodeID: 1}
	value := []byte{1} // 1 byte payload to match FASTER benchmarks
	numEntries := 5000
	key := []byte("scan-table")

	log, release, err := mgr.GetLog(key)
	if err != nil {
		b.Fatalf("Failed to get log: %v", err)
	}

	for i := range numEntries {
		slot := uint64(i)
		log.Accept(slot, ballot, value)

		// Only commit 50% (simulates partially committed log)
		if i%2 == 0 {
			log.Commit(slot)
		}
	}

	b.ReportAllocs()

	for b.Loop() {
		uncommitted, err := log.ScanUncommitted()
		if err != nil {
			b.Fatalf("Failed to scan: %v", err)
		}
		if len(uncommitted) == 0 {
			b.Fatal("Expected uncommitted entries")
		}
	}

	b.StopTimer()
	release()
	scans := float64(b.N) / b.Elapsed().Seconds()
	b.ReportMetric(scans, "scans/sec")
}

// BenchmarkComparison_ScanUncommitted_BadgerDB tests scanning for uncommitted entries
func BenchmarkComparison_ScanUncommitted_BadgerDB(b *testing.B) {
	dir := b.TempDir()

	opts := badger.DefaultOptions(dir)
	opts.SyncWrites = false
	opts.Logger = nil

	db, err := badger.Open(opts)
	if err != nil {
		b.Fatalf("Failed to open BadgerDB: %v", err)
	}
	defer db.Close()

	// Store committed flag with each entry
	ctx := context.Background()
	value := []byte{1} // 1 byte payload to match FASTER benchmarks
	numEntries := 5000

	for i := range numEntries {
		key := fmt.Appendf(nil, "slot:%020d", i)
		committed := (i%2 == 0)

		db.Update(func(txn *badger.Txn) error {
			// Store value with committed flag
			e := badger.NewEntry(key, value).WithMeta(byte(0))
			if committed {
				e = e.WithMeta(byte(1))
			}
			return txn.SetEntry(e)
		})
	}

	b.ReportAllocs()

	for b.Loop() {
		uncommitted := make([][]byte, 0)

		err := db.View(func(txn *badger.Txn) error {
			opts := badger.DefaultIteratorOptions
			opts.PrefetchValues = false // Only need metadata

			it := txn.NewIterator(opts)
			defer it.Close()

			for it.Seek([]byte("slot:")); it.ValidForPrefix([]byte("slot:")); it.Next() {
				item := it.Item()
				if item.UserMeta() == 0 { // Uncommitted
					uncommitted = append(uncommitted, item.KeyCopy(nil))
				}
			}
			return nil
		})

		if err != nil {
			b.Fatalf("Failed to scan: %v", err)
		}

		_ = ctx
		_ = uncommitted
	}

	b.StopTimer()
	scans := float64(b.N) / b.Elapsed().Seconds()
	b.ReportMetric(scans, "scans/sec")
}
