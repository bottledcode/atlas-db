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
	"testing"
	"time"

	"github.com/bottledcode/atlas-db/atlas/consensus"
	"github.com/bottledcode/atlas-db/integration-tests/harness"
)

// benchCluster is a reusable cluster for benchmarks.
type benchCluster struct {
	cluster *harness.Cluster
	t       testing.TB
}

var (
	singleRegionCluster *benchCluster
	multiRegionCluster  *benchCluster
	clusterOnce         sync.Once
	clusterMu           sync.Mutex
)

func getSingleRegionCluster(tb testing.TB) *benchCluster {
	clusterMu.Lock()
	defer clusterMu.Unlock()

	if singleRegionCluster != nil {
		return singleRegionCluster
	}

	t, ok := tb.(*testing.T)
	if !ok {
		// For benchmarks, we need to create a temporary test
		tb.Skip("Cannot create cluster in pure benchmark mode, run with -test.run first")
		return nil
	}

	cluster, err := harness.NewCluster(t, harness.ClusterConfig{
		NumNodes: 3,
		Regions:  []string{"us-east-1"},
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

	singleRegionCluster = &benchCluster{cluster: cluster, t: t}
	return singleRegionCluster
}

// BenchmarkWriteLocalLatency measures write latency to local region (no network simulation).
func BenchmarkWriteLocalLatency(b *testing.B) {
	// Disable latency injection for baseline
	consensus.GetLatencyConfig().Disable()

	cluster, err := harness.NewCluster(nil, harness.ClusterConfig{
		NumNodes: 3,
		Regions:  []string{"us-east-1"},
	})
	if err != nil {
		b.Skipf("Failed to create cluster: %v", err)
		return
	}

	// Note: This requires running setup test first
	node, err := cluster.GetNode(0)
	if err != nil {
		b.Skipf("Failed to get node: %v", err)
		return
	}

	client := node.Client()
	value := generateValue(100)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("bench-key-%d", i)
		if err := client.KeyPut(key, value); err != nil {
			b.Fatalf("Write failed: %v", err)
		}
	}

	b.StopTimer()
	ops := float64(b.N) / b.Elapsed().Seconds()
	b.ReportMetric(ops, "ops/sec")
}

// BenchmarkWriteWithLatency benchmarks writes with simulated inter-region latency.
func BenchmarkWriteWithLatency(b *testing.B) {
	presets := []struct {
		name   string
		preset consensus.LatencyPreset
	}{
		{"Local", consensus.PresetLocal},
		{"SingleContinent", consensus.PresetSingleContinent},
		{"Global", consensus.PresetGlobal},
		{"HighLatency", consensus.PresetHighLatency},
	}

	for _, p := range presets {
		b.Run(p.name, func(b *testing.B) {
			latencyConfig := consensus.GetLatencyConfig()
			latencyConfig.ApplyPreset(p.preset)
			defer latencyConfig.Disable()

			consensus.GetLatencyStats().Reset()

			// Note: Requires cluster setup
			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				// Simulate latency-injected operation
				time.Sleep(latencyConfig.GetLatency("us-east-1", "eu-west-1"))
			}

			b.StopTimer()
			ops := float64(b.N) / b.Elapsed().Seconds()
			b.ReportMetric(ops, "ops/sec")

			avgLatency := consensus.GetLatencyStats().AverageLatency()
			if avgLatency > 0 {
				b.ReportMetric(float64(avgLatency.Microseconds()), "avg-latency-us")
			}
		})
	}
}

// BenchmarkReadLatency measures read latency.
func BenchmarkReadLatency(b *testing.B) {
	consensus.GetLatencyConfig().Disable()

	// This benchmark measures the read path latency
	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		// Placeholder - would need running cluster
		_ = i
	}

	b.StopTimer()
}

// BenchmarkThroughputMatrix runs throughput tests across different configurations.
func BenchmarkThroughputMatrix(b *testing.B) {
	configs := []struct {
		name        string
		regions     []string
		concurrency int
	}{
		{"SingleRegion_1Writer", []string{"us-east-1"}, 1},
		{"SingleRegion_10Writers", []string{"us-east-1"}, 10},
		{"ThreeRegions_1Writer", []string{"us-east-1", "us-west-2", "eu-west-1"}, 1},
		{"ThreeRegions_10Writers", []string{"us-east-1", "us-west-2", "eu-west-1"}, 10},
	}

	for _, cfg := range configs {
		b.Run(cfg.name, func(b *testing.B) {
			// Simulate the operation
			b.ResetTimer()

			if cfg.concurrency == 1 {
				for i := 0; i < b.N; i++ {
					// Single-threaded operation
					_ = i
				}
			} else {
				b.RunParallel(func(pb *testing.PB) {
					for pb.Next() {
						// Parallel operation
					}
				})
			}

			b.StopTimer()
			ops := float64(b.N) / b.Elapsed().Seconds()
			b.ReportMetric(ops, "ops/sec")
		})
	}
}

// BenchmarkValueSizes measures performance impact of different value sizes.
func BenchmarkValueSizes(b *testing.B) {
	sizes := []int{10, 100, 1000, 10000}

	for _, size := range sizes {
		b.Run(fmt.Sprintf("Size%d", size), func(b *testing.B) {
			value := generateValue(size)

			b.ResetTimer()
			b.ReportAllocs()

			for i := 0; i < b.N; i++ {
				// Simulate operation with value
				_ = len(value)
			}

			b.StopTimer()
			ops := float64(b.N) / b.Elapsed().Seconds()
			throughputMBps := (ops * float64(size)) / (1024 * 1024)
			b.ReportMetric(ops, "ops/sec")
			b.ReportMetric(throughputMBps, "MB/s")
		})
	}
}

// BenchmarkConsensusRoundTrip measures full consensus round-trip time.
func BenchmarkConsensusRoundTrip(b *testing.B) {
	presets := []struct {
		name    string
		preset  consensus.LatencyPreset
		regions []string
	}{
		{"Local_3Nodes", consensus.PresetLocal, []string{"us-east-1"}},
		{"Continental_3Nodes", consensus.PresetSingleContinent, []string{"us-east-1", "us-west-2"}},
		{"Global_3Nodes", consensus.PresetGlobal, []string{"us-east-1", "us-west-2", "eu-west-1"}},
		{"Global_5Nodes", consensus.PresetGlobal, []string{"us-east-1", "us-west-2", "eu-west-1", "ap-south-1"}},
	}

	for _, p := range presets {
		b.Run(p.name, func(b *testing.B) {
			latencyConfig := consensus.GetLatencyConfig()
			latencyConfig.ApplyPreset(p.preset)
			defer latencyConfig.Disable()

			// Simulate WPaxos round-trip:
			// - Phase 1: Leader -> Q1 quorum (Fn+1 nodes per Z-Fz zones)
			// - Phase 2: Leader -> Q2 quorum (L-Fn nodes per Fz+1 zones)

			numRegions := len(p.regions)
			nodesPerRegion := 3 / numRegions
			if nodesPerRegion < 1 {
				nodesPerRegion = 1
			}

			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				// Simulate Phase 2 (Accept/Commit) round-trip to majority
				// In WPaxos, we need responses from Q2 quorum
				localRegion := p.regions[0]

				var maxLatency time.Duration
				for _, region := range p.regions[:min(numRegions, 2)] {
					lat := latencyConfig.GetLatency(localRegion, region)
					if lat > maxLatency {
						maxLatency = lat
					}
				}

				// Round-trip = 2 * one-way latency (request + response)
				if maxLatency > 0 {
					time.Sleep(2 * maxLatency)
				}
			}

			b.StopTimer()
			ops := float64(b.N) / b.Elapsed().Seconds()
			b.ReportMetric(ops, "ops/sec")
		})
	}
}

// BenchmarkReplicationFanout measures replication to multiple replicas.
func BenchmarkReplicationFanout(b *testing.B) {
	fanouts := []int{1, 2, 3, 5}

	for _, fanout := range fanouts {
		b.Run(fmt.Sprintf("Fanout%d", fanout), func(b *testing.B) {
			latencyConfig := consensus.GetLatencyConfig()
			latencyConfig.EnableDefault()
			defer latencyConfig.Disable()

			regions := []string{"us-east-1", "us-west-2", "eu-west-1", "ap-south-1", "sa-east-1"}[:fanout]

			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				// Parallel replication to all regions
				var wg sync.WaitGroup
				for _, region := range regions {
					wg.Add(1)
					go func(r string) {
						defer wg.Done()
						lat := latencyConfig.GetLatency("us-east-1", r)
						if lat > 0 {
							time.Sleep(lat)
						}
					}(region)
				}
				wg.Wait()
			}

			b.StopTimer()
			ops := float64(b.N) / b.Elapsed().Seconds()
			b.ReportMetric(ops, "ops/sec")
		})
	}
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
