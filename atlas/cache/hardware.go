package cache

import (
	"fmt"
	"runtime"
)

// HardwareConfig contains detected hardware specifications
type HardwareConfig struct {
	NumCPU       int    // Number of logical CPUs
	TotalMemory  uint64 // Total system memory in bytes
	CacheSize    uint64 // Recommended cache size in bytes
	NumShards    int    // Recommended number of shards
	SlotsPerShard int   // Recommended slots per shard
}

// DetectHardware detects system hardware and returns recommended configuration
func DetectHardware() HardwareConfig {
	numCPU := runtime.NumCPU()

	// Get total memory (best effort, fallback to conservative estimate)
	totalMemory := getTotalMemory()

	// Default to 10% of total memory for cache, with bounds
	cacheSize := uint64(float64(totalMemory) * 0.10)

	// Apply bounds: min 256MB, max 16GB
	const minCacheSize = 256 * 1024 * 1024      // 256 MB
	const maxCacheSize = 16 * 1024 * 1024 * 1024 // 16 GB

	if cacheSize < minCacheSize {
		cacheSize = minCacheSize
	}
	if cacheSize > maxCacheSize {
		cacheSize = maxCacheSize
	}

	return HardwareConfig{
		NumCPU:      numCPU,
		TotalMemory: totalMemory,
		CacheSize:   cacheSize,
	}
}

// ComputeShardConfig calculates optimal shard configuration for a target cache size
func ComputeShardConfig(cacheSize uint64) (numShards, slotsPerShard int) {
	// Assumptions from design doc:
	// - Record overhead: ~200 bytes average
	// - Node overhead: 96 bytes
	// - Slot overhead: 8 bytes (pointer)
	// - Load factor: 1.0-1.25 target
	// - Target: 50% overhead (1.5x multiplier)

	const bytesPerRecord = 200
	const bytesPerNode = 96
	const bytesPerSlot = 8
	const overheadMultiplier = 1.5
	const targetLoadFactor = 1.0

	// Estimate number of records that fit in cache
	// cacheSize = records * bytesPerRecord + records * bytesPerNode + slots * bytesPerSlot
	// cacheSize = records * (bytesPerRecord + bytesPerNode) + (records / loadFactor) * bytesPerSlot
	estimatedRecords := int(float64(cacheSize) / (float64(bytesPerRecord+bytesPerNode) + float64(bytesPerSlot)/targetLoadFactor))

	// Calculate slots needed (with load factor)
	totalSlots := int(float64(estimatedRecords) / targetLoadFactor)

	// Determine optimal shard count based on CPU count
	numCPU := runtime.NumCPU()

	// Heuristic: 4-8 shards per core for good parallelism
	// Use power of 2 for bit-masking efficiency
	numShards = nextPowerOf2(numCPU * 4)

	// Clamp to reasonable range: 16-256 shards
	if numShards < 16 {
		numShards = 16
	}
	if numShards > 256 {
		numShards = 256
	}

	// Calculate slots per shard (must be power of 2)
	slotsPerShard = totalSlots / numShards
	slotsPerShard = nextPowerOf2(slotsPerShard)

	// Ensure minimum slot count per shard: 256
	if slotsPerShard < 256 {
		slotsPerShard = 256
	}

	// Ensure reasonable maximum: 64K slots per shard
	if slotsPerShard > 65536 {
		slotsPerShard = 65536
	}

	return numShards, slotsPerShard
}

// ConfigFromHardware creates a CloxCache config from detected hardware
func ConfigFromHardware() Config {
	hw := DetectHardware()
	numShards, slotsPerShard := ComputeShardConfig(hw.CacheSize)

	return Config{
		NumShards:     numShards,
		SlotsPerShard: slotsPerShard,
	}
}

// ConfigFromMemorySize creates a CloxCache config for a specific memory target
func ConfigFromMemorySize(targetBytes uint64) Config {
	numShards, slotsPerShard := ComputeShardConfig(targetBytes)

	return Config{
		NumShards:     numShards,
		SlotsPerShard: slotsPerShard,
	}
}

// nextPowerOf2 returns the next power of 2 >= n
func nextPowerOf2(n int) int {
	if n <= 0 {
		return 1
	}

	// Check if already power of 2
	if n&(n-1) == 0 {
		return n
	}

	// Find next power of 2
	power := 1
	for power < n {
		power <<= 1
	}

	return power
}

// EstimateMemoryUsage estimates total memory usage for a given configuration
func (c Config) EstimateMemoryUsage() uint64 {
	const bytesPerNode = 96
	const bytesPerSlot = 8
	const shardOverhead = 64 // approximate overhead per shard struct

	totalSlots := uint64(c.NumShards * c.SlotsPerShard)

	// Slot array memory
	slotArrayMemory := totalSlots * bytesPerSlot

	// Estimate nodes (assume load factor 1.25)
	estimatedNodes := uint64(float64(totalSlots) * 1.25)
	nodeMemory := estimatedNodes * bytesPerNode

	// Shard overhead
	shardMemory := uint64(c.NumShards) * shardOverhead

	return slotArrayMemory + nodeMemory + shardMemory
}

// FormatMemory formats bytes as human-readable string
func FormatMemory(bytes uint64) string {
	const unit = 1024
	if bytes < unit {
		return fmt.Sprintf("%d B", bytes)
	}

	div, exp := uint64(unit), 0
	for n := bytes / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}

	units := []string{"KB", "MB", "GB", "TB"}
	return fmt.Sprintf("%.1f %s", float64(bytes)/float64(div), units[exp])
}

// getTotalMemory attempts to get total system memory
// Falls back to conservative estimate if detection fails
func getTotalMemory() uint64 {
	// Try to get memory stats from runtime
	var m runtime.MemStats
	runtime.ReadMemStats(&m)

	// On some systems, Sys gives a reasonable upper bound
	// But it's not total system memory, it's Go's memory from OS
	// So we use a heuristic: assume we can use 50% of physical memory

	// Conservative fallback: assume 8GB system if we can't determine
	const fallbackMemory = 8 * 1024 * 1024 * 1024

	// If Sys is reasonable (> 100MB), use it as a hint
	if m.Sys > 100*1024*1024 {
		// Estimate total system memory as 10x current Go heap
		// This is a rough heuristic
		estimated := m.Sys * 10
		if estimated < fallbackMemory {
			return estimated
		}
	}

	return fallbackMemory
}
