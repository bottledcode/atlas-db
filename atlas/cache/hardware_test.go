package cache

import (
	"fmt"
	"runtime"
	"testing"
)

func TestDetectHardware(t *testing.T) {
	hw := DetectHardware()

	// Verify basic sanity
	if hw.NumCPU <= 0 {
		t.Errorf("NumCPU should be positive, got %d", hw.NumCPU)
	}

	if hw.NumCPU != runtime.NumCPU() {
		t.Errorf("NumCPU mismatch: got %d, expected %d", hw.NumCPU, runtime.NumCPU())
	}

	if hw.TotalMemory == 0 {
		t.Error("TotalMemory should be non-zero")
	}

	if hw.CacheSize == 0 {
		t.Error("CacheSize should be non-zero")
	}

	// Cache size should be reasonable (between 256MB and 16GB)
	const minCacheSize = 256 * 1024 * 1024
	const maxCacheSize = 16 * 1024 * 1024 * 1024

	if hw.CacheSize < minCacheSize {
		t.Errorf("CacheSize too small: %d (min: %d)", hw.CacheSize, minCacheSize)
	}

	if hw.CacheSize > maxCacheSize {
		t.Errorf("CacheSize too large: %d (max: %d)", hw.CacheSize, maxCacheSize)
	}

	t.Logf("Detected hardware: CPUs=%d, TotalMemory=%s, CacheSize=%s",
		hw.NumCPU, FormatMemory(hw.TotalMemory), FormatMemory(hw.CacheSize))
}

func TestComputeShardConfig(t *testing.T) {
	tests := []struct {
		name          string
		cacheSize     uint64
		minShards     int
		maxShards     int
		minSlots      int
		maxSlots      int
	}{
		{
			name:      "256MB cache",
			cacheSize: 256 * 1024 * 1024,
			minShards: 16,
			maxShards: 256,
			minSlots:  256,
			maxSlots:  65536,
		},
		{
			name:      "1GB cache",
			cacheSize: 1024 * 1024 * 1024,
			minShards: 16,
			maxShards: 256,
			minSlots:  256,
			maxSlots:  65536,
		},
		{
			name:      "4GB cache",
			cacheSize: 4 * 1024 * 1024 * 1024,
			minShards: 16,
			maxShards: 256,
			minSlots:  256,
			maxSlots:  65536,
		},
		{
			name:      "16GB cache",
			cacheSize: 16 * 1024 * 1024 * 1024,
			minShards: 16,
			maxShards: 256,
			minSlots:  256,
			maxSlots:  65536,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			numShards, slotsPerShard := ComputeShardConfig(tt.cacheSize)

			// Verify power of 2
			if numShards&(numShards-1) != 0 {
				t.Errorf("numShards is not power of 2: %d", numShards)
			}

			if slotsPerShard&(slotsPerShard-1) != 0 {
				t.Errorf("slotsPerShard is not power of 2: %d", slotsPerShard)
			}

			// Verify bounds
			if numShards < tt.minShards || numShards > tt.maxShards {
				t.Errorf("numShards out of bounds: %d (expected %d-%d)",
					numShards, tt.minShards, tt.maxShards)
			}

			if slotsPerShard < tt.minSlots || slotsPerShard > tt.maxSlots {
				t.Errorf("slotsPerShard out of bounds: %d (expected %d-%d)",
					slotsPerShard, tt.minSlots, tt.maxSlots)
			}

			totalSlots := numShards * slotsPerShard
			t.Logf("Cache size: %s → shards=%d, slots/shard=%d, total_slots=%d",
				FormatMemory(tt.cacheSize), numShards, slotsPerShard, totalSlots)
		})
	}
}

func TestConfigFromMemorySize(t *testing.T) {
	tests := []uint64{
		256 * 1024 * 1024,      // 256MB
		512 * 1024 * 1024,      // 512MB
		1024 * 1024 * 1024,     // 1GB
		2 * 1024 * 1024 * 1024, // 2GB
		4 * 1024 * 1024 * 1024, // 4GB
	}

	for _, size := range tests {
		t.Run(FormatMemory(size), func(t *testing.T) {
			cfg := ConfigFromMemorySize(size)

			// Verify config is valid
			if cfg.NumShards <= 0 {
				t.Errorf("Invalid NumShards: %d", cfg.NumShards)
			}

			if cfg.SlotsPerShard <= 0 {
				t.Errorf("Invalid SlotsPerShard: %d", cfg.SlotsPerShard)
			}

			// Verify power of 2
			if cfg.NumShards&(cfg.NumShards-1) != 0 {
				t.Errorf("NumShards not power of 2: %d", cfg.NumShards)
			}

			if cfg.SlotsPerShard&(cfg.SlotsPerShard-1) != 0 {
				t.Errorf("SlotsPerShard not power of 2: %d", cfg.SlotsPerShard)
			}

			// Estimate memory usage
			estimated := cfg.EstimateMemoryUsage()
			t.Logf("Target: %s → Config: shards=%d, slots=%d → Estimated: %s",
				FormatMemory(size), cfg.NumShards, cfg.SlotsPerShard, FormatMemory(estimated))

			// Estimated should be reasonably close to target
			// Allow for overhead, so check it's within 3x
			if estimated > size*3 {
				t.Errorf("Estimated memory too high: %s (target: %s)",
					FormatMemory(estimated), FormatMemory(size))
			}
		})
	}
}

func TestConfigFromHardware(t *testing.T) {
	cfg := ConfigFromHardware()

	// Verify config is valid
	if cfg.NumShards <= 0 {
		t.Errorf("Invalid NumShards: %d", cfg.NumShards)
	}

	if cfg.SlotsPerShard <= 0 {
		t.Errorf("Invalid SlotsPerShard: %d", cfg.SlotsPerShard)
	}

	// Verify power of 2
	if cfg.NumShards&(cfg.NumShards-1) != 0 {
		t.Errorf("NumShards not power of 2: %d", cfg.NumShards)
	}

	if cfg.SlotsPerShard&(cfg.SlotsPerShard-1) != 0 {
		t.Errorf("SlotsPerShard not power of 2: %d", cfg.SlotsPerShard)
	}

	hw := DetectHardware()
	estimated := cfg.EstimateMemoryUsage()

	t.Logf("Hardware: CPUs=%d, Memory=%s, CacheSize=%s",
		hw.NumCPU, FormatMemory(hw.TotalMemory), FormatMemory(hw.CacheSize))
	t.Logf("Config: shards=%d, slots=%d, estimated=%s",
		cfg.NumShards, cfg.SlotsPerShard, FormatMemory(estimated))
}

func TestNextPowerOf2(t *testing.T) {
	tests := []struct {
		input    int
		expected int
	}{
		{0, 1},
		{1, 1},
		{2, 2},
		{3, 4},
		{4, 4},
		{5, 8},
		{7, 8},
		{8, 8},
		{15, 16},
		{16, 16},
		{17, 32},
		{100, 128},
		{256, 256},
		{1000, 1024},
		{1024, 1024},
		{1025, 2048},
	}

	for _, tt := range tests {
		t.Run(fmt.Sprintf("%d", tt.input), func(t *testing.T) {
			result := nextPowerOf2(tt.input)
			if result != tt.expected {
				t.Errorf("nextPowerOf2(%d) = %d, expected %d",
					tt.input, result, tt.expected)
			}

			// Verify it's actually a power of 2
			if result > 0 && result&(result-1) != 0 {
				t.Errorf("Result is not power of 2: %d", result)
			}
		})
	}
}

func TestFormatMemory(t *testing.T) {
	tests := []struct {
		bytes    uint64
		expected string
	}{
		{0, "0 B"},
		{100, "100 B"},
		{1023, "1023 B"},
		{1024, "1.0 KB"},
		{1536, "1.5 KB"},
		{1024 * 1024, "1.0 MB"},
		{1024 * 1024 * 1024, "1.0 GB"},
		{1536 * 1024 * 1024, "1.5 GB"},
		{4 * 1024 * 1024 * 1024, "4.0 GB"},
	}

	for _, tt := range tests {
		t.Run(tt.expected, func(t *testing.T) {
			result := FormatMemory(tt.bytes)
			if result != tt.expected {
				t.Errorf("FormatMemory(%d) = %q, expected %q",
					tt.bytes, result, tt.expected)
			}
		})
	}
}

func TestEstimateMemoryUsage(t *testing.T) {
	configs := []Config{
		{NumShards: 16, SlotsPerShard: 256},
		{NumShards: 64, SlotsPerShard: 1024},
		{NumShards: 128, SlotsPerShard: 4096},
		{NumShards: 128, SlotsPerShard: 52 * 1024},
	}

	for _, cfg := range configs {
		t.Run(fmt.Sprintf("shards=%d,slots=%d", cfg.NumShards, cfg.SlotsPerShard), func(t *testing.T) {
			estimated := cfg.EstimateMemoryUsage()

			if estimated == 0 {
				t.Error("Estimated memory should not be zero")
			}

			totalSlots := cfg.NumShards * cfg.SlotsPerShard
			t.Logf("Config: %d shards × %d slots = %d total slots → %s estimated",
				cfg.NumShards, cfg.SlotsPerShard, totalSlots, FormatMemory(estimated))
		})
	}
}

func BenchmarkDetectHardware(b *testing.B) {
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_ = DetectHardware()
	}
}

func BenchmarkComputeShardConfig(b *testing.B) {
	const cacheSize = 1024 * 1024 * 1024 // 1GB

	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_, _ = ComputeShardConfig(cacheSize)
	}
}

func BenchmarkConfigFromHardware(b *testing.B) {
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_ = ConfigFromHardware()
	}
}
