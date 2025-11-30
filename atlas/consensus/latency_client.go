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
	"context"
	"math/rand/v2"
	"sync/atomic"
	"time"

	"github.com/bottledcode/atlas-db/atlas/options"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/emptypb"
)

// LatencyStats tracks latency injection statistics for benchmarking.
type LatencyStats struct {
	TotalCalls     atomic.Int64
	TotalLatencyNs atomic.Int64
	MinLatencyNs   atomic.Int64
	MaxLatencyNs   atomic.Int64
}

var globalLatencyStats = &LatencyStats{}

func init() {
	globalLatencyStats.MinLatencyNs.Store(int64(time.Hour)) // Start high
}

// GetLatencyStats returns the global latency statistics.
func GetLatencyStats() *LatencyStats {
	return globalLatencyStats
}

// Reset clears all latency statistics.
func (s *LatencyStats) Reset() {
	s.TotalCalls.Store(0)
	s.TotalLatencyNs.Store(0)
	s.MinLatencyNs.Store(int64(time.Hour))
	s.MaxLatencyNs.Store(0)
}

// AverageLatency returns the average injected latency.
func (s *LatencyStats) AverageLatency() time.Duration {
	calls := s.TotalCalls.Load()
	if calls == 0 {
		return 0
	}
	return time.Duration(s.TotalLatencyNs.Load() / calls)
}

// latencyClient wraps a ConsensusClient and injects configurable latency.
type latencyClient struct {
	inner        ConsensusClient
	targetRegion string
	config       *LatencyConfig
}

// WrapWithLatency wraps a ConsensusClient with latency injection.
func WrapWithLatency(client ConsensusClient, targetRegion string) ConsensusClient {
	config := GetLatencyConfig()
	if !config.IsEnabled() {
		return client
	}
	return &latencyClient{
		inner:        client,
		targetRegion: targetRegion,
		config:       config,
	}
}

func (c *latencyClient) injectLatency() {
	localRegion := options.CurrentOptions.Region
	latency := c.config.GetLatency(localRegion, c.targetRegion)

	if latency <= 0 {
		return
	}

	// Add jitter
	c.config.mu.RLock()
	jitterPercent := c.config.jitterPercent
	c.config.mu.RUnlock()

	if jitterPercent > 0 {
		jitterRange := float64(latency) * float64(jitterPercent) / 100.0
		jitter := time.Duration((rand.Float64()*2 - 1) * jitterRange)
		latency += jitter
		if latency < 0 {
			latency = 0
		}
	}

	// Record stats
	latencyNs := int64(latency)
	globalLatencyStats.TotalCalls.Add(1)
	globalLatencyStats.TotalLatencyNs.Add(latencyNs)

	// Update min/max atomically
	for {
		old := globalLatencyStats.MinLatencyNs.Load()
		if latencyNs >= old || globalLatencyStats.MinLatencyNs.CompareAndSwap(old, latencyNs) {
			break
		}
	}
	for {
		old := globalLatencyStats.MaxLatencyNs.Load()
		if latencyNs <= old || globalLatencyStats.MaxLatencyNs.CompareAndSwap(old, latencyNs) {
			break
		}
	}

	time.Sleep(latency)
}

func (c *latencyClient) StealTableOwnership(ctx context.Context, in *StealTableOwnershipRequest, opts ...grpc.CallOption) (*StealTableOwnershipResponse, error) {
	c.injectLatency()
	return c.inner.StealTableOwnership(ctx, in, opts...)
}

func (c *latencyClient) WriteMigration(ctx context.Context, in *WriteMigrationRequest, opts ...grpc.CallOption) (*WriteMigrationResponse, error) {
	c.injectLatency()
	return c.inner.WriteMigration(ctx, in, opts...)
}

func (c *latencyClient) AcceptMigration(ctx context.Context, in *WriteMigrationRequest, opts ...grpc.CallOption) (*emptypb.Empty, error) {
	c.injectLatency()
	return c.inner.AcceptMigration(ctx, in, opts...)
}

func (c *latencyClient) Replicate(ctx context.Context, opts ...grpc.CallOption) (grpc.ClientStreamingClient[ReplicationRequest, ReplicationResponse], error) {
	c.injectLatency()
	return c.inner.Replicate(ctx, opts...)
}

func (c *latencyClient) DeReference(ctx context.Context, in *DereferenceRequest, opts ...grpc.CallOption) (grpc.ServerStreamingClient[DereferenceResponse], error) {
	c.injectLatency()
	return c.inner.DeReference(ctx, in, opts...)
}

func (c *latencyClient) Ping(ctx context.Context, in *PingRequest, opts ...grpc.CallOption) (*PingResponse, error) {
	c.injectLatency()
	return c.inner.Ping(ctx, in, opts...)
}

func (c *latencyClient) PrefixScan(ctx context.Context, in *PrefixScanRequest, opts ...grpc.CallOption) (*PrefixScanResponse, error) {
	c.injectLatency()
	return c.inner.PrefixScan(ctx, in, opts...)
}

func (c *latencyClient) RequestSlots(ctx context.Context, in *SlotRequest, opts ...grpc.CallOption) (grpc.ServerStreamingClient[RecordMutation], error) {
	c.injectLatency()
	return c.inner.RequestSlots(ctx, in, opts...)
}

func (c *latencyClient) Follow(ctx context.Context, in *SlotRequest, opts ...grpc.CallOption) (grpc.ServerStreamingClient[RecordMutation], error) {
	c.injectLatency()
	return c.inner.Follow(ctx, in, opts...)
}

func (c *latencyClient) ReadRecord(ctx context.Context, in *ReadRecordRequest, opts ...grpc.CallOption) (*ReadRecordResponse, error) {
	c.injectLatency()
	return c.inner.ReadRecord(ctx, in, opts...)
}
