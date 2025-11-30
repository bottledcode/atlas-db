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
	"os"
	"sync"
	"time"
)

// LatencyConfig defines inter-region latency for testing purposes.
// This is used by integration tests to simulate realistic network conditions.
type LatencyConfig struct {
	mu sync.RWMutex
	// latencyMatrix maps "fromRegion:toRegion" to one-way latency
	latencyMatrix map[string]time.Duration
	// jitterPercent adds random variance (0-100)
	jitterPercent int
	// enabled controls whether latency injection is active
	enabled bool
}

// DefaultLatencyMatrix provides realistic inter-region latencies based on
// real-world cloud provider measurements (AWS/GCP approximate values).
var DefaultLatencyMatrix = map[string]time.Duration{
	// Same region (intra-AZ)
	"us-east-1:us-east-1":   1 * time.Millisecond,
	"us-west-2:us-west-2":   1 * time.Millisecond,
	"eu-west-1:eu-west-1":   1 * time.Millisecond,
	"ap-south-1:ap-south-1": 1 * time.Millisecond,

	// US East <-> US West (~60-70ms)
	"us-east-1:us-west-2": 65 * time.Millisecond,
	"us-west-2:us-east-1": 65 * time.Millisecond,

	// US <-> EU (~80-100ms)
	"us-east-1:eu-west-1": 85 * time.Millisecond,
	"eu-west-1:us-east-1": 85 * time.Millisecond,
	"us-west-2:eu-west-1": 140 * time.Millisecond,
	"eu-west-1:us-west-2": 140 * time.Millisecond,

	// US <-> Asia Pacific (~150-200ms)
	"us-east-1:ap-south-1": 180 * time.Millisecond,
	"ap-south-1:us-east-1": 180 * time.Millisecond,
	"us-west-2:ap-south-1": 200 * time.Millisecond,
	"ap-south-1:us-west-2": 200 * time.Millisecond,

	// EU <-> Asia Pacific (~120-150ms)
	"eu-west-1:ap-south-1": 130 * time.Millisecond,
	"ap-south-1:eu-west-1": 130 * time.Millisecond,
}

var globalLatencyConfig = &LatencyConfig{
	latencyMatrix: make(map[string]time.Duration),
	jitterPercent: 10,
	enabled:       false,
}

func init() {
	// Check environment variable for latency injection preset
	// ATLAS_LATENCY_PRESET can be: local, single-continent, global, high-latency
	if preset := os.Getenv("ATLAS_LATENCY_PRESET"); preset != "" {
		switch LatencyPreset(preset) {
		case PresetLocal:
			globalLatencyConfig.ApplyPreset(PresetLocal)
		case PresetSingleContinent:
			globalLatencyConfig.ApplyPreset(PresetSingleContinent)
		case PresetGlobal:
			globalLatencyConfig.ApplyPreset(PresetGlobal)
		case PresetHighLatency:
			globalLatencyConfig.ApplyPreset(PresetHighLatency)
		}
	}
}

// GetLatencyConfig returns the global latency configuration.
func GetLatencyConfig() *LatencyConfig {
	return globalLatencyConfig
}

// Enable activates latency injection with the given matrix.
func (c *LatencyConfig) Enable(matrix map[string]time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.latencyMatrix = matrix
	c.enabled = true
}

// EnableDefault activates latency injection with default realistic values.
func (c *LatencyConfig) EnableDefault() {
	c.Enable(DefaultLatencyMatrix)
}

// Disable turns off latency injection.
func (c *LatencyConfig) Disable() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.enabled = false
}

// IsEnabled returns whether latency injection is active.
func (c *LatencyConfig) IsEnabled() bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.enabled
}

// SetJitter sets the jitter percentage (0-100).
func (c *LatencyConfig) SetJitter(percent int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if percent < 0 {
		percent = 0
	}
	if percent > 100 {
		percent = 100
	}
	c.jitterPercent = percent
}

// GetLatency returns the one-way latency between two regions.
func (c *LatencyConfig) GetLatency(fromRegion, toRegion string) time.Duration {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if !c.enabled {
		return 0
	}

	key := fromRegion + ":" + toRegion
	if latency, ok := c.latencyMatrix[key]; ok {
		return latency
	}

	// Default fallback for unknown region pairs
	if fromRegion == toRegion {
		return 1 * time.Millisecond
	}
	return 50 * time.Millisecond
}

// SetLatency sets custom latency between two regions.
func (c *LatencyConfig) SetLatency(fromRegion, toRegion string, latency time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	key := fromRegion + ":" + toRegion
	c.latencyMatrix[key] = latency
}

// LatencyPreset defines common testing scenarios.
type LatencyPreset string

const (
	// PresetLocal simulates local/same-datacenter deployment (1-2ms)
	PresetLocal LatencyPreset = "local"
	// PresetSingleContinent simulates nodes within one continent (~20-70ms)
	PresetSingleContinent LatencyPreset = "single-continent"
	// PresetGlobal simulates worldwide deployment (~50-200ms)
	PresetGlobal LatencyPreset = "global"
	// PresetHighLatency simulates extreme conditions (~200-500ms)
	PresetHighLatency LatencyPreset = "high-latency"
)

// ApplyPreset applies a predefined latency configuration.
func (c *LatencyConfig) ApplyPreset(preset LatencyPreset) {
	switch preset {
	case PresetLocal:
		c.Enable(map[string]time.Duration{
			"us-east-1:us-east-1": 1 * time.Millisecond,
			"us-east-1:us-east-2": 2 * time.Millisecond,
			"us-east-2:us-east-1": 2 * time.Millisecond,
			"us-west-2:us-west-2": 1 * time.Millisecond,
			"eu-west-1:eu-west-1": 1 * time.Millisecond,
		})
	case PresetSingleContinent:
		c.Enable(map[string]time.Duration{
			"us-east-1:us-east-1": 1 * time.Millisecond,
			"us-west-2:us-west-2": 1 * time.Millisecond,
			"us-east-1:us-west-2": 65 * time.Millisecond,
			"us-west-2:us-east-1": 65 * time.Millisecond,
		})
	case PresetGlobal:
		c.EnableDefault()
	case PresetHighLatency:
		c.Enable(map[string]time.Duration{
			"us-east-1:us-east-1":  5 * time.Millisecond,
			"us-east-1:eu-west-1":  200 * time.Millisecond,
			"eu-west-1:us-east-1":  200 * time.Millisecond,
			"us-east-1:ap-south-1": 350 * time.Millisecond,
			"ap-south-1:us-east-1": 350 * time.Millisecond,
			"eu-west-1:ap-south-1": 280 * time.Millisecond,
			"ap-south-1:eu-west-1": 280 * time.Millisecond,
		})
	}
}
