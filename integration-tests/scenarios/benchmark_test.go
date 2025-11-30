//go:build integration

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

package scenarios

import (
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bottledcode/atlas-db/atlas/consensus"
	"github.com/bottledcode/atlas-db/integration-tests/harness"
)

// BenchmarkConfig holds configuration for benchmark runs.
type BenchmarkConfig struct {
	NumNodes       int
	Regions        []string
	LatencyPreset  consensus.LatencyPreset
	NumOperations  int
	ValueSize      int
	Concurrency    int
}

// BenchmarkResult holds the results of a benchmark run.
type BenchmarkResult struct {
	TotalOperations int64
	TotalDuration   time.Duration
	AvgLatency      time.Duration
	P50Latency      time.Duration
	P95Latency      time.Duration
	P99Latency      time.Duration
	Throughput      float64 // ops/sec
	Errors          int64
}

// DefaultBenchmarkConfigs provides standard benchmark scenarios.
var DefaultBenchmarkConfigs = map[string]BenchmarkConfig{
	"single-region-3-node": {
		NumNodes:      3,
		Regions:       []string{"us-east-1"},
		LatencyPreset: consensus.PresetLocal,
		NumOperations: 1000,
		ValueSize:     100,
		Concurrency:   1,
	},
	"multi-region-3-node": {
		NumNodes:      3,
		Regions:       []string{"us-east-1", "us-west-2", "eu-west-1"},
		LatencyPreset: consensus.PresetGlobal,
		NumOperations: 100,
		ValueSize:     100,
		Concurrency:   1,
	},
	"multi-region-5-node": {
		NumNodes:      5,
		Regions:       []string{"us-east-1", "us-west-2", "eu-west-1"},
		LatencyPreset: consensus.PresetGlobal,
		NumOperations: 100,
		ValueSize:     100,
		Concurrency:   1,
	},
	"high-latency-3-node": {
		NumNodes:      3,
		Regions:       []string{"us-east-1", "eu-west-1", "ap-south-1"},
		LatencyPreset: consensus.PresetHighLatency,
		NumOperations: 50,
		ValueSize:     100,
		Concurrency:   1,
	},
}

// TestBenchmarkSingleRegionWrite benchmarks write operations in a single-region cluster.
func TestBenchmarkSingleRegionWrite(t *testing.T) {
	cfg := DefaultBenchmarkConfigs["single-region-3-node"]
	runWriteBenchmark(t, cfg, "SingleRegionWrite")
}

// TestBenchmarkMultiRegionWrite benchmarks write operations across multiple regions.
func TestBenchmarkMultiRegionWrite(t *testing.T) {
	cfg := DefaultBenchmarkConfigs["multi-region-3-node"]
	runWriteBenchmark(t, cfg, "MultiRegionWrite")
}

// TestBenchmarkHighLatencyWrite benchmarks writes under high-latency conditions.
func TestBenchmarkHighLatencyWrite(t *testing.T) {
	cfg := DefaultBenchmarkConfigs["high-latency-3-node"]
	runWriteBenchmark(t, cfg, "HighLatencyWrite")
}

// TestBenchmarkSingleRegionRead benchmarks read operations in a single-region cluster.
func TestBenchmarkSingleRegionRead(t *testing.T) {
	cfg := DefaultBenchmarkConfigs["single-region-3-node"]
	runReadBenchmark(t, cfg, "SingleRegionRead")
}

// TestBenchmarkMultiRegionRead benchmarks read operations across multiple regions.
func TestBenchmarkMultiRegionRead(t *testing.T) {
	cfg := DefaultBenchmarkConfigs["multi-region-3-node"]
	runReadBenchmark(t, cfg, "MultiRegionRead")
}

// TestBenchmarkMixedWorkload benchmarks a mixed read/write workload.
func TestBenchmarkMixedWorkload(t *testing.T) {
	cfg := DefaultBenchmarkConfigs["multi-region-3-node"]
	runMixedBenchmark(t, cfg, "MixedWorkload", 0.5) // 50% reads, 50% writes
}

// TestBenchmarkReadHeavyWorkload benchmarks a read-heavy workload.
func TestBenchmarkReadHeavyWorkload(t *testing.T) {
	cfg := DefaultBenchmarkConfigs["multi-region-3-node"]
	runMixedBenchmark(t, cfg, "ReadHeavyWorkload", 0.9) // 90% reads, 10% writes
}

// TestBenchmarkConcurrentWrites benchmarks concurrent write operations.
func TestBenchmarkConcurrentWrites(t *testing.T) {
	cfg := DefaultBenchmarkConfigs["multi-region-3-node"]
	cfg.Concurrency = 10
	cfg.NumOperations = 500
	runConcurrentWriteBenchmark(t, cfg, "ConcurrentWrites")
}

// TestBenchmarkReplicationLag measures replication lag across regions.
func TestBenchmarkReplicationLag(t *testing.T) {
	t.Skip("Skipped: CAS replication is now async - not all nodes receive data synchronously")
	cfg := DefaultBenchmarkConfigs["multi-region-3-node"]
	runReplicationLagBenchmark(t, cfg, "ReplicationLag")
}

// TestBenchmarkHighConcurrency simulates many concurrent clients like production.
func TestBenchmarkHighConcurrency(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipped in short mode: takes 2+ minutes")
	}
	configs := []struct {
		name        string
		concurrency int
		preset      consensus.LatencyPreset
		regions     []string
	}{
		{"Local_50clients", 50, consensus.PresetLocal, []string{"us-east-1"}},
		{"Local_100clients", 100, consensus.PresetLocal, []string{"us-east-1"}},
		{"Global_50clients", 50, consensus.PresetGlobal, []string{"us-east-1", "us-west-2", "eu-west-1"}},
		{"Global_100clients", 100, consensus.PresetGlobal, []string{"us-east-1", "us-west-2", "eu-west-1"}},
	}

	for _, cfg := range configs {
		t.Run(cfg.name, func(t *testing.T) {
			runHighConcurrencyBenchmark(t, cfg.concurrency, cfg.preset, cfg.regions, cfg.name)
		})
	}
}

func runHighConcurrencyBenchmark(t *testing.T, concurrency int, preset consensus.LatencyPreset, regions []string, name string) {
	t.Helper()

	cluster, err := harness.NewCluster(t, harness.ClusterConfig{
		NumNodes:      3,
		Regions:       regions,
		LatencyPreset: string(preset),
	})
	if err != nil {
		t.Fatalf("Failed to create cluster: %v", err)
	}

	if err := cluster.Start(); err != nil {
		t.Fatalf("Failed to start cluster: %v", err)
	}

	if err := cluster.WaitForBootstrap(30 * time.Second); err != nil {
		t.Fatalf("Failed to bootstrap cluster: %v", err)
	}

	value := generateValue(100)
	opsPerClient := 20           // Each client does 20 operations
	maxRetries := 3              // Retry on transaction conflict
	testDuration := 30 * time.Second
	numKeys := 1000              // Fits within MaxHotKeys=1024 to avoid LRU eviction

	t.Logf("Running %s: %d concurrent clients, %d ops each, %d shared keys...", name, concurrency, opsPerClient, numKeys)

	// Pre-warm keys to avoid log creation during benchmark
	t.Log("Pre-warming keys...")
	node0, _ := cluster.GetNode(0)
	warmupClient := harness.NewSocketClient(node0.Config.SocketPath)
	for i := 0; i < numKeys; i++ {
		key := fmt.Sprintf("hc-key-%d", i)
		warmupClient.KeyPut(key, fmt.Sprintf("warmup-%d", i))
	}
	warmupClient.Close()
	time.Sleep(1 * time.Second) // Let replication settle
	t.Log("Pre-warming done, starting benchmark...")

	var wg sync.WaitGroup
	latencyChan := make(chan time.Duration, concurrency*opsPerClient)
	var successOps, failedOps, retriedOps atomic.Int64

	start := time.Now()
	deadline := start.Add(testDuration)

	for c := 0; c < concurrency; c++ {
		wg.Add(1)
		go func(clientID int) {
			defer wg.Done()

			// Distribute clients across nodes
			nodeIdx := clientID % cluster.NumNodes()
			node, err := cluster.GetNode(nodeIdx)
			if err != nil {
				return
			}
			// Create a NEW client per goroutine to avoid sharing socket
			client := harness.NewSocketClient(node.Config.SocketPath)
			defer client.Close()

			for i := 0; i < opsPerClient && time.Now().Before(deadline); i++ {
				// Reuse keys from pool, but unique values to avoid blob conflicts
				keyIdx := (clientID*opsPerClient + i) % numKeys
				key := fmt.Sprintf("hc-key-%d", keyIdx)
				uniqueValue := fmt.Sprintf("%s-c%d-i%d-%d", value, clientID, i, time.Now().UnixNano())

				var lastErr error
				for retry := 0; retry <= maxRetries; retry++ {
					opStart := time.Now()
					err := client.KeyPut(key, uniqueValue)
					if err == nil {
						latencyChan <- time.Since(opStart)
						successOps.Add(1)
						if retry > 0 {
							retriedOps.Add(1)
						}
						break
					}
					lastErr = err
					// Brief backoff before retry
					time.Sleep(time.Duration(retry*10) * time.Millisecond)
				}
				if lastErr != nil {
					failedOps.Add(1)
					if failedOps.Load() <= 5 {
						t.Logf("Client %d failed: %v", clientID, lastErr)
					}
				}
			}
		}(c)
	}

	wg.Wait()
	close(latencyChan)
	totalDuration := time.Since(start)

	// Collect latencies
	latencies := make([]time.Duration, 0, concurrency*opsPerClient)
	for lat := range latencyChan {
		latencies = append(latencies, lat)
	}

	// Calculate results
	result := calculateResults(latencies, totalDuration, failedOps.Load())

	t.Logf("\n=== %s Results ===", name)
	t.Logf("Configuration:")
	t.Logf("  Concurrent clients: %d", concurrency)
	t.Logf("  Regions: %v, Latency preset: %s", regions, preset)
	t.Logf("Performance:")
	t.Logf("  Successful ops: %d", successOps.Load())
	t.Logf("  Failed ops: %d", failedOps.Load())
	t.Logf("  Retried ops: %d", retriedOps.Load())
	t.Logf("  Total duration: %v", totalDuration)
	t.Logf("  Throughput: %.2f ops/sec", result.Throughput)
	t.Logf("Latency:")
	t.Logf("  Average: %v", result.AvgLatency)
	t.Logf("  P50: %v", result.P50Latency)
	t.Logf("  P95: %v", result.P95Latency)
	t.Logf("  P99: %v", result.P99Latency)
}

func runWriteBenchmark(t *testing.T, cfg BenchmarkConfig, name string) {
	t.Helper()

	// Reset latency stats (stats are per-process, but useful for local tracking)
	consensus.GetLatencyStats().Reset()

	cluster, err := harness.NewCluster(t, harness.ClusterConfig{
		NumNodes:      cfg.NumNodes,
		Regions:       cfg.Regions,
		LatencyPreset: string(cfg.LatencyPreset), // Pass to node processes
	})
	if err != nil {
		t.Fatalf("Failed to create cluster: %v", err)
	}

	if err := cluster.Start(); err != nil {
		t.Fatalf("Failed to start cluster: %v", err)
	}

	// Wait for cluster to stabilize
	if err := cluster.WaitForBootstrap(30 * time.Second); err != nil {
		t.Fatalf("Failed to bootstrap cluster: %v", err)
	}

	node, err := cluster.GetNode(0)
	if err != nil {
		t.Fatalf("Failed to get node: %v", err)
	}

	client := node.Client()
	value := generateValue(cfg.ValueSize)

	// Warm-up phase
	t.Log("Running warm-up...")
	for i := 0; i < 10; i++ {
		key := fmt.Sprintf("warmup-key-%d", i)
		if err := client.KeyPut(key, value); err != nil {
			t.Logf("Warm-up write failed: %v", err)
		}
	}

	// Benchmark phase
	t.Logf("Running %s benchmark with %d operations...", name, cfg.NumOperations)

	latencies := make([]time.Duration, 0, cfg.NumOperations)
	var errors int64

	start := time.Now()
	for i := 0; i < cfg.NumOperations; i++ {
		key := fmt.Sprintf("bench-key-%d", i)

		opStart := time.Now()
		if err := client.KeyPut(key, value); err != nil {
			errors++
			continue
		}
		latencies = append(latencies, time.Since(opStart))
	}
	totalDuration := time.Since(start)

	result := calculateResults(latencies, totalDuration, errors)
	printBenchmarkResults(t, name, cfg, result)
}

func runReadBenchmark(t *testing.T, cfg BenchmarkConfig, name string) {
	t.Helper()

	consensus.GetLatencyStats().Reset()

	cluster, err := harness.NewCluster(t, harness.ClusterConfig{
		NumNodes:      cfg.NumNodes,
		Regions:       cfg.Regions,
		LatencyPreset: string(cfg.LatencyPreset),
	})
	if err != nil {
		t.Fatalf("Failed to create cluster: %v", err)
	}

	if err := cluster.Start(); err != nil {
		t.Fatalf("Failed to start cluster: %v", err)
	}

	if err := cluster.WaitForBootstrap(30 * time.Second); err != nil {
		t.Fatalf("Failed to bootstrap cluster: %v", err)
	}

	node, err := cluster.GetNode(0)
	if err != nil {
		t.Fatalf("Failed to get node: %v", err)
	}

	client := node.Client()
	value := generateValue(cfg.ValueSize)

	// Pre-populate data
	t.Log("Pre-populating data...")
	for i := 0; i < cfg.NumOperations; i++ {
		key := fmt.Sprintf("bench-key-%d", i)
		if err := client.KeyPut(key, value); err != nil {
			t.Fatalf("Failed to populate key %s: %v", key, err)
		}
	}

	// Wait for replication
	time.Sleep(2 * time.Second)

	// Benchmark reads
	t.Logf("Running %s benchmark with %d operations...", name, cfg.NumOperations)

	latencies := make([]time.Duration, 0, cfg.NumOperations)
	var errors int64

	start := time.Now()
	for i := 0; i < cfg.NumOperations; i++ {
		key := fmt.Sprintf("bench-key-%d", i)

		opStart := time.Now()
		_, err := client.KeyGet(key)
		if err != nil {
			errors++
			continue
		}
		latencies = append(latencies, time.Since(opStart))
	}
	totalDuration := time.Since(start)

	result := calculateResults(latencies, totalDuration, errors)
	printBenchmarkResults(t, name, cfg, result)
}

func runMixedBenchmark(t *testing.T, cfg BenchmarkConfig, name string, readRatio float64) {
	t.Helper()

	consensus.GetLatencyStats().Reset()

	cluster, err := harness.NewCluster(t, harness.ClusterConfig{
		NumNodes:      cfg.NumNodes,
		Regions:       cfg.Regions,
		LatencyPreset: string(cfg.LatencyPreset),
	})
	if err != nil {
		t.Fatalf("Failed to create cluster: %v", err)
	}

	if err := cluster.Start(); err != nil {
		t.Fatalf("Failed to start cluster: %v", err)
	}

	if err := cluster.WaitForBootstrap(30 * time.Second); err != nil {
		t.Fatalf("Failed to bootstrap cluster: %v", err)
	}

	node, err := cluster.GetNode(0)
	if err != nil {
		t.Fatalf("Failed to get node: %v", err)
	}

	client := node.Client()
	value := generateValue(cfg.ValueSize)

	// Pre-populate some data
	numKeys := cfg.NumOperations / 2
	t.Log("Pre-populating data...")
	for i := 0; i < numKeys; i++ {
		key := fmt.Sprintf("bench-key-%d", i)
		if err := client.KeyPut(key, value); err != nil {
			t.Fatalf("Failed to populate key %s: %v", key, err)
		}
	}

	time.Sleep(2 * time.Second)

	// Mixed workload
	t.Logf("Running %s benchmark (%.0f%% reads)...", name, readRatio*100)

	readLatencies := make([]time.Duration, 0)
	writeLatencies := make([]time.Duration, 0)
	var readErrors, writeErrors int64

	start := time.Now()
	for i := 0; i < cfg.NumOperations; i++ {
		key := fmt.Sprintf("bench-key-%d", i%numKeys)

		opStart := time.Now()
		if float64(i%100)/100 < readRatio {
			// Read operation
			_, err := client.KeyGet(key)
			if err != nil {
				readErrors++
				continue
			}
			readLatencies = append(readLatencies, time.Since(opStart))
		} else {
			// Write operation
			if err := client.KeyPut(key, value); err != nil {
				writeErrors++
				continue
			}
			writeLatencies = append(writeLatencies, time.Since(opStart))
		}
	}
	totalDuration := time.Since(start)

	// Print combined results
	allLatencies := append(readLatencies, writeLatencies...)
	result := calculateResults(allLatencies, totalDuration, readErrors+writeErrors)
	printBenchmarkResults(t, name, cfg, result)

	// Print breakdown
	if len(readLatencies) > 0 {
		readResult := calculateResults(readLatencies, totalDuration, readErrors)
		t.Logf("  Read breakdown: avg=%v, p99=%v, errors=%d",
			readResult.AvgLatency, readResult.P99Latency, readErrors)
	}
	if len(writeLatencies) > 0 {
		writeResult := calculateResults(writeLatencies, totalDuration, writeErrors)
		t.Logf("  Write breakdown: avg=%v, p99=%v, errors=%d",
			writeResult.AvgLatency, writeResult.P99Latency, writeErrors)
	}
}

func runConcurrentWriteBenchmark(t *testing.T, cfg BenchmarkConfig, name string) {
	t.Helper()

	consensus.GetLatencyStats().Reset()

	cluster, err := harness.NewCluster(t, harness.ClusterConfig{
		NumNodes:      cfg.NumNodes,
		Regions:       cfg.Regions,
		LatencyPreset: string(cfg.LatencyPreset),
	})
	if err != nil {
		t.Fatalf("Failed to create cluster: %v", err)
	}

	if err := cluster.Start(); err != nil {
		t.Fatalf("Failed to start cluster: %v", err)
	}

	if err := cluster.WaitForBootstrap(30 * time.Second); err != nil {
		t.Fatalf("Failed to bootstrap cluster: %v", err)
	}

	value := generateValue(cfg.ValueSize)
	opsPerWorker := cfg.NumOperations / cfg.Concurrency

	t.Logf("Running %s benchmark with %d concurrent writers...", name, cfg.Concurrency)

	var wg sync.WaitGroup
	latencyChan := make(chan time.Duration, cfg.NumOperations)
	var totalErrors atomic.Int64

	start := time.Now()

	for w := 0; w < cfg.Concurrency; w++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()

			// Each worker uses a different node for distribution
			nodeIdx := workerID % cluster.NumNodes()
			node, err := cluster.GetNode(nodeIdx)
			if err != nil {
				t.Logf("Worker %d: failed to get node: %v", workerID, err)
				return
			}

			// Each worker needs its own connection - sockets aren't thread-safe
			client := node.NewClient()
			defer client.Close()

			for i := 0; i < opsPerWorker; i++ {
				key := fmt.Sprintf("bench-w%d-key-%d", workerID, i)

				opStart := time.Now()
				if err := client.KeyPut(key, value); err != nil {
					totalErrors.Add(1)
					// Log first few errors per worker
					if i < 3 {
						t.Logf("Worker %d: write error on key %s: %v", workerID, key, err)
					}
					continue
				}
				latencyChan <- time.Since(opStart)
			}
		}(w)
	}

	wg.Wait()
	close(latencyChan)
	totalDuration := time.Since(start)

	// Collect latencies
	latencies := make([]time.Duration, 0, cfg.NumOperations)
	for lat := range latencyChan {
		latencies = append(latencies, lat)
	}

	result := calculateResults(latencies, totalDuration, totalErrors.Load())
	printBenchmarkResults(t, name, cfg, result)
}

func runReplicationLagBenchmark(t *testing.T, cfg BenchmarkConfig, name string) {
	t.Helper()

	cluster, err := harness.NewCluster(t, harness.ClusterConfig{
		NumNodes:      cfg.NumNodes,
		Regions:       cfg.Regions,
		LatencyPreset: string(cfg.LatencyPreset),
	})
	if err != nil {
		t.Fatalf("Failed to create cluster: %v", err)
	}

	if err := cluster.Start(); err != nil {
		t.Fatalf("Failed to start cluster: %v", err)
	}

	if err := cluster.WaitForBootstrap(30 * time.Second); err != nil {
		t.Fatalf("Failed to bootstrap cluster: %v", err)
	}

	t.Logf("Running %s benchmark...", name)

	value := generateValue(cfg.ValueSize)
	replicationLags := make([]time.Duration, 0, cfg.NumOperations)

	node0, _ := cluster.GetNode(0)
	client0 := node0.Client()

	for i := 0; i < cfg.NumOperations; i++ {
		key := fmt.Sprintf("repl-key-%d", i)

		// Write to node 0
		writeStart := time.Now()
		if err := client0.KeyPut(key, value); err != nil {
			t.Logf("Write failed: %v", err)
			continue
		}

		// Wait for propagation to all other nodes
		propagationStart := time.Now()
		err := cluster.WaitForKeyPropagation(key, value, 5*time.Second)
		if err != nil {
			t.Logf("Propagation failed for key %s: %v", key, err)
			continue
		}

		replicationLags = append(replicationLags, time.Since(propagationStart))

		// Log progress
		if (i+1)%10 == 0 {
			t.Logf("Completed %d/%d operations (total time: %v)",
				i+1, cfg.NumOperations, time.Since(writeStart))
		}
	}

	// Calculate replication lag stats
	if len(replicationLags) > 0 {
		result := calculateResults(replicationLags, 0, 0)
		t.Logf("\n=== %s Results ===", name)
		t.Logf("Configuration:")
		t.Logf("  Nodes: %d, Regions: %v", cfg.NumNodes, cfg.Regions)
		t.Logf("  Latency preset: %s", cfg.LatencyPreset)
		t.Logf("Replication Lag:")
		t.Logf("  Average: %v", result.AvgLatency)
		t.Logf("  P50: %v", result.P50Latency)
		t.Logf("  P95: %v", result.P95Latency)
		t.Logf("  P99: %v", result.P99Latency)
		t.Logf("  Successful replications: %d/%d", len(replicationLags), cfg.NumOperations)
	}
}

func generateValue(size int) string {
	b := make([]byte, size)
	for i := range b {
		b[i] = byte('a' + (i % 26))
	}
	return string(b)
}

func calculateResults(latencies []time.Duration, totalDuration time.Duration, errors int64) BenchmarkResult {
	if len(latencies) == 0 {
		return BenchmarkResult{Errors: errors}
	}

	// Sort latencies for percentile calculation
	sortedLatencies := make([]time.Duration, len(latencies))
	copy(sortedLatencies, latencies)
	sortDurations(sortedLatencies)

	// Calculate average
	var totalLatency time.Duration
	for _, lat := range latencies {
		totalLatency += lat
	}
	avgLatency := totalLatency / time.Duration(len(latencies))

	// Calculate percentiles
	p50 := sortedLatencies[len(sortedLatencies)*50/100]
	p95 := sortedLatencies[len(sortedLatencies)*95/100]
	p99 := sortedLatencies[len(sortedLatencies)*99/100]

	// Calculate throughput
	var throughput float64
	if totalDuration > 0 {
		throughput = float64(len(latencies)) / totalDuration.Seconds()
	}

	return BenchmarkResult{
		TotalOperations: int64(len(latencies)),
		TotalDuration:   totalDuration,
		AvgLatency:      avgLatency,
		P50Latency:      p50,
		P95Latency:      p95,
		P99Latency:      p99,
		Throughput:      throughput,
		Errors:          errors,
	}
}

func sortDurations(d []time.Duration) {
	for i := 1; i < len(d); i++ {
		for j := i; j > 0 && d[j] < d[j-1]; j-- {
			d[j], d[j-1] = d[j-1], d[j]
		}
	}
}

func printBenchmarkResults(t *testing.T, name string, cfg BenchmarkConfig, result BenchmarkResult) {
	t.Logf("\n=== %s Results ===", name)
	t.Logf("Configuration:")
	t.Logf("  Nodes: %d, Regions: %v", cfg.NumNodes, cfg.Regions)
	t.Logf("  Latency preset: %s", cfg.LatencyPreset)
	t.Logf("  Value size: %d bytes, Concurrency: %d", cfg.ValueSize, cfg.Concurrency)
	t.Logf("Performance:")
	t.Logf("  Total operations: %d", result.TotalOperations)
	t.Logf("  Total duration: %v", result.TotalDuration)
	t.Logf("  Throughput: %.2f ops/sec", result.Throughput)
	t.Logf("Latency:")
	t.Logf("  Average: %v", result.AvgLatency)
	t.Logf("  P50: %v", result.P50Latency)
	t.Logf("  P95: %v", result.P95Latency)
	t.Logf("  P99: %v", result.P99Latency)
	t.Logf("Errors: %d", result.Errors)

	// Print injected latency stats
	stats := consensus.GetLatencyStats()
	if stats.TotalCalls.Load() > 0 {
		t.Logf("Injected Latency Stats:")
		t.Logf("  Total calls with latency: %d", stats.TotalCalls.Load())
		t.Logf("  Average injected latency: %v", stats.AverageLatency())
	}
}
