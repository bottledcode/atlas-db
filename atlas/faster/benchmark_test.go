package faster

import (
	"fmt"
	"testing"
)

// BenchmarkAccept measures the performance of accepting (writing uncommitted) entries
func BenchmarkAccept(b *testing.B) {
	dir := b.TempDir()

	cfg := Config{
		Path:         dir + "/bench.log",
		MutableSize:  500 * 1024 * 1024, // 500MB - large enough for benchmark
		SyncOnCommit: false,
	}

	log, err := NewFasterLog(cfg)
	if err != nil {
		b.Fatalf("Failed to create log: %v", err)
	}
	defer log.Close()

	ballot := Ballot{ID: 1, NodeID: 1}
	value := make([]byte, 100) // 100 byte values

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		slot := uint64(i)
		err := log.Accept(slot, ballot, value)
		if err != nil {
			b.Fatalf("Failed to accept at slot %d: %v", slot, err)
		}

		// Periodically commit to avoid filling buffer
		if i > 0 && i%10000 == 0 {
			// Commit some old entries
			for j := i - 1000; j < i; j++ {
				log.Commit(uint64(j))
			}
		}
	}

	b.StopTimer()
	ops := float64(b.N) / b.Elapsed().Seconds()
	b.ReportMetric(ops, "ops/sec")
}

// BenchmarkCommit measures the performance of committing entries (no fsync)
// Realistic: Accept and commit happen close together in WPaxos
func BenchmarkCommit(b *testing.B) {
	dir := b.TempDir()

	cfg := Config{
		Path:         dir + "/bench.log",
		MutableSize:  100 * 1024 * 1024,
		SyncOnCommit: false, // No fsync for benchmark
	}

	log, err := NewFasterLog(cfg)
	if err != nil {
		b.Fatalf("Failed to create log: %v", err)
	}
	defer log.Close()

	ballot := Ballot{ID: 1, NodeID: 1}
	value := make([]byte, 100)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		slot := uint64(i)

		// Accept
		err := log.Accept(slot, ballot, value)
		if err != nil {
			b.Fatalf("Failed to accept: %v", err)
		}

		// Commit immediately
		err = log.Commit(slot)
		if err != nil {
			b.Fatalf("Failed to commit: %v", err)
		}

		// Periodically checkpoint to reclaim buffer space
		if i > 0 && i%1000 == 0 {
			log.Checkpoint()
		}
	}

	b.StopTimer()
	ops := float64(b.N) / b.Elapsed().Seconds()
	b.ReportMetric(ops, "ops/sec")
}

// BenchmarkCommitWithSync measures commit performance with fsync
func BenchmarkCommitWithSync(b *testing.B) {
	dir := b.TempDir()

	cfg := Config{
		Path:         dir + "/bench.log",
		MutableSize:  100 * 1024 * 1024,
		SyncOnCommit: true, // fsync on every commit
	}

	log, err := NewFasterLog(cfg)
	if err != nil {
		b.Fatalf("Failed to create log: %v", err)
	}
	defer log.Close()

	// Pre-accept all entries
	ballot := Ballot{ID: 1, NodeID: 1}
	value := make([]byte, 100)
	for i := 0; i < b.N; i++ {
		slot := uint64(i)
		err := log.Accept(slot, ballot, value)
		if err != nil {
			b.Fatalf("Failed to accept: %v", err)
		}
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		slot := uint64(i)
		err := log.Commit(slot)
		if err != nil {
			b.Fatalf("Failed to commit: %v", err)
		}
	}

	b.StopTimer()
	ops := float64(b.N) / b.Elapsed().Seconds()
	b.ReportMetric(ops, "ops/sec")
}

// BenchmarkReadUncommitted measures reading from mutable buffer (with lock)
func BenchmarkReadUncommitted(b *testing.B) {
	dir := b.TempDir()

	cfg := Config{
		Path:         dir + "/bench.log",
		MutableSize:  100 * 1024 * 1024,
		SyncOnCommit: false,
	}

	log, err := NewFasterLog(cfg)
	if err != nil {
		b.Fatalf("Failed to create log: %v", err)
	}
	defer log.Close()

	// Accept entries (don't commit)
	ballot := Ballot{ID: 1, NodeID: 1}
	value := make([]byte, 100)
	numEntries := 10000
	for i := 0; i < numEntries; i++ {
		slot := uint64(i)
		err := log.Accept(slot, ballot, value)
		if err != nil {
			b.Fatalf("Failed to accept: %v", err)
		}
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		slot := uint64(i % numEntries)
		_, err := log.Read(slot)
		if err != nil {
			b.Fatalf("Failed to read: %v", err)
		}
	}

	b.StopTimer()
	ops := float64(b.N) / b.Elapsed().Seconds()
	b.ReportMetric(ops, "ops/sec")
}

// BenchmarkReadCommitted measures lock-free reads from immutable tail
func BenchmarkReadCommitted(b *testing.B) {
	dir := b.TempDir()

	cfg := Config{
		Path:         dir + "/bench.log",
		MutableSize:  100 * 1024 * 1024,
		SyncOnCommit: false,
	}

	log, err := NewFasterLog(cfg)
	if err != nil {
		b.Fatalf("Failed to create log: %v", err)
	}
	defer log.Close()

	// Accept and commit entries
	ballot := Ballot{ID: 1, NodeID: 1}
	value := make([]byte, 100)
	numEntries := 10000
	for i := 0; i < numEntries; i++ {
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

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		slot := uint64(i % numEntries)
		_, err := log.ReadCommittedOnly(slot)
		if err != nil {
			b.Fatalf("Failed to read: %v", err)
		}
	}

	b.StopTimer()
	ops := float64(b.N) / b.Elapsed().Seconds()
	b.ReportMetric(ops, "ops/sec")
}

// BenchmarkAcceptCommitRead measures full workflow with periodic checkpoints
func BenchmarkAcceptCommitRead(b *testing.B) {
	dir := b.TempDir()

	cfg := Config{
		Path:         dir + "/bench.log",
		MutableSize:  100 * 1024 * 1024,
		SyncOnCommit: false,
	}

	log, err := NewFasterLog(cfg)
	if err != nil {
		b.Fatalf("Failed to create log: %v", err)
	}
	defer log.Close()

	ballot := Ballot{ID: 1, NodeID: 1}
	value := make([]byte, 100)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		slot := uint64(i)

		// Accept
		err := log.Accept(slot, ballot, value)
		if err != nil {
			b.Fatalf("Failed to accept: %v", err)
		}

		// Commit
		err = log.Commit(slot)
		if err != nil {
			b.Fatalf("Failed to commit: %v", err)
		}

		// Read
		_, err = log.ReadCommittedOnly(slot)
		if err != nil {
			b.Fatalf("Failed to read: %v", err)
		}

		// Checkpoint periodically to reclaim space
		if i > 0 && i%1000 == 0 {
			log.Checkpoint()
		}
	}

	b.StopTimer()
	ops := float64(b.N) / b.Elapsed().Seconds()
	b.ReportMetric(ops, "ops/sec")
}

// BenchmarkScanUncommitted measures Phase-1 recovery scan performance
func BenchmarkScanUncommitted(b *testing.B) {
	dir := b.TempDir()

	cfg := Config{
		Path:         dir + "/bench.log",
		MutableSize:  100 * 1024 * 1024,
		SyncOnCommit: false,
	}

	log, err := NewFasterLog(cfg)
	if err != nil {
		b.Fatalf("Failed to create log: %v", err)
	}
	defer log.Close()

	// Accept entries (don't commit)
	ballot := Ballot{ID: 1, NodeID: 1}
	value := make([]byte, 100)
	numEntries := 1000
	for i := 0; i < numEntries; i++ {
		slot := uint64(i)
		err := log.Accept(slot, ballot, value)
		if err != nil {
			b.Fatalf("Failed to accept: %v", err)
		}
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		entries, err := log.ScanUncommitted()
		if err != nil {
			b.Fatalf("Failed to scan: %v", err)
		}
		if len(entries) != numEntries {
			b.Fatalf("Expected %d uncommitted entries, got %d", numEntries, len(entries))
		}
	}

	b.StopTimer()
	ops := float64(b.N) / b.Elapsed().Seconds()
	b.ReportMetric(ops, "scans/sec")
}

// BenchmarkValueSizes tests performance with different value sizes
func BenchmarkValueSizes(b *testing.B) {
	sizes := []int{10, 100} // bytes (realistic for consensus metadata)

	for _, size := range sizes {
		b.Run(fmt.Sprintf("Size%d", size), func(b *testing.B) {
			dir := b.TempDir()

			cfg := Config{
				Path:         dir + "/bench.log",
				MutableSize:  500 * 1024 * 1024, // Large buffer
				SyncOnCommit: false,
			}

			log, err := NewFasterLog(cfg)
			if err != nil {
				b.Fatalf("Failed to create log: %v", err)
			}
			defer log.Close()

			ballot := Ballot{ID: 1, NodeID: 1}
			value := make([]byte, size)

			b.ResetTimer()
			b.ReportAllocs()

			for i := 0; i < b.N; i++ {
				slot := uint64(i)
				err := log.Accept(slot, ballot, value)
				if err != nil {
					b.Fatalf("Failed to accept: %v", err)
				}
				err = log.Commit(slot)
				if err != nil {
					b.Fatalf("Failed to commit: %v", err)
				}

				// Checkpoint periodically
				if i > 0 && i%1000 == 0 {
					log.Checkpoint()
				}
			}

			b.StopTimer()
			ops := float64(b.N) / b.Elapsed().Seconds()
			throughputMBps := (ops * float64(size)) / (1024 * 1024)
			b.ReportMetric(ops, "ops/sec")
			b.ReportMetric(throughputMBps, "MB/s")
		})
	}
}

// BenchmarkConcurrentReads measures read performance under concurrent load
func BenchmarkConcurrentReads(b *testing.B) {
	dir := b.TempDir()

	cfg := Config{
		Path:         dir + "/bench.log",
		MutableSize:  100 * 1024 * 1024,
		SyncOnCommit: false,
	}

	log, err := NewFasterLog(cfg)
	if err != nil {
		b.Fatalf("Failed to create log: %v", err)
	}
	defer log.Close()

	// Prepare data
	ballot := Ballot{ID: 1, NodeID: 1}
	value := make([]byte, 100)
	numEntries := 10000
	for i := 0; i < numEntries; i++ {
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

	b.ResetTimer()
	b.ReportAllocs()

	// Run reads in parallel
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			slot := uint64(i % numEntries)
			_, err := log.ReadCommittedOnly(slot)
			if err != nil {
				b.Fatalf("Failed to read: %v", err)
			}
			i++
		}
	})

	b.StopTimer()
	ops := float64(b.N) / b.Elapsed().Seconds()
	b.ReportMetric(ops, "ops/sec")
}

// BenchmarkCheckpoint measures checkpoint performance
func BenchmarkCheckpoint(b *testing.B) {
	dir := b.TempDir()

	cfg := Config{
		Path:         dir + "/bench.log",
		MutableSize:  100 * 1024 * 1024,
		SyncOnCommit: false,
	}

	log, err := NewFasterLog(cfg)
	if err != nil {
		b.Fatalf("Failed to create log: %v", err)
	}
	defer log.Close()

	ballot := Ballot{ID: 1, NodeID: 1}
	value := make([]byte, 100)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		// Accept and commit some entries
		for j := 0; j < 100; j++ {
			slot := uint64(i*100 + j)
			log.Accept(slot, ballot, value)
			log.Commit(slot)
		}

		// Checkpoint
		err := log.Checkpoint()
		if err != nil {
			b.Fatalf("Failed to checkpoint: %v", err)
		}
	}

	b.StopTimer()
}

// BenchmarkRecovery measures recovery time for different log sizes
func BenchmarkRecovery(b *testing.B) {
	sizes := []int{1000, 10000}

	for _, size := range sizes {
		b.Run(fmt.Sprintf("Entries%d", size), func(b *testing.B) {
			// Create initial log once
			dir := b.TempDir()
			logPath := dir + "/bench.log"

			cfg := Config{
				Path:         logPath,
				MutableSize:  100 * 1024 * 1024,
				SyncOnCommit: true,
			}

			// Populate once before benchmark
			log, err := NewFasterLog(cfg)
			if err != nil {
				b.Fatalf("Failed to create log: %v", err)
			}

			ballot := Ballot{ID: 1, NodeID: 1}
			value := make([]byte, 100)
			for i := 0; i < size; i++ {
				slot := uint64(i)
				log.Accept(slot, ballot, value)
				log.Commit(slot)
			}
			log.Close()

			// Benchmark recovery (open existing log)
			b.ResetTimer()
			b.ReportAllocs()

			for i := 0; i < b.N; i++ {
				log, err := NewFasterLog(cfg)
				if err != nil {
					b.Fatalf("Failed to recover log: %v", err)
				}
				log.Close()
			}

			b.StopTimer()
			entriesPerSec := float64(size) / (b.Elapsed().Seconds() / float64(b.N))
			b.ReportMetric(entriesPerSec, "entries/sec")
		})
	}
}
