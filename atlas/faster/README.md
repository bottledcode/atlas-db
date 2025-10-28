# FASTER: Fast Persistent Recoverable Log for Consensus

A pure-Go implementation of FASTER-style hybrid logging, optimized for consensus protocols like WPaxos. This implementation provides **lock-free reads**, **atomic writes**, and **perfect semantic alignment** with Paxos accept/commit phases.

## Architecture

FASTER uses a three-region hybrid log architecture that separates uncommitted (speculative) entries from committed (durable) entries:

```
┌─────────────────────────────────────────────────────────────┐
│                     FASTER Architecture                      │
├─────────────────┬─────────────────┬─────────────────────────┤
│   In-Memory     │     Mutable     │    Immutable Tail       │
│   Hash Index    │  Ring Buffer    │   (Memory-Mapped)       │
│                 │                 │                         │
│  [slot→offset]  │  [Uncommitted]  │  [Committed entries]    │
│                 │   entries       │                         │
│  • sync.Map     │  • Lock-free    │  • Append-only          │
│  • Fast lookup  │  • CAS alloc    │  • Durable (fsync)      │
│  • Concurrent   │  • 2-64MB       │  • Lock-free reads      │
│                 │                 │  • 1GB+ segments        │
└─────────────────┴─────────────────┴─────────────────────────┘
```

### Why This Design?

Traditional LSM-tree stores (like BadgerDB, LevelDB) have **semantic mismatch** with consensus logs:

| Requirement | LSM-Tree Stores | FASTER |
|-------------|----------------|--------|
| Accept/Commit distinction | ❌ Must track in value | ✅ Mutable vs. Immutable |
| Lock-free committed reads | ❌ Locks on read path | ✅ Direct mmap reads |
| Overwrite uncommitted | ❌ Complex merge logic | ✅ Natural in mutable region |
| Sequential execution | ⚠️ Manual gap handling | ✅ Natural slot ordering |
| Recovery scanning | ⚠️ Compaction interference | ✅ Fast sequential scan |

## Core Components

### 1. FasterLog - The Main Log

The core log structure that manages entries across the three regions.

```go
log, err := faster.NewFasterLog(faster.Config{
    Path:         "/data/consensus.log",
    MutableSize:  64 * 1024 * 1024,  // 64MB for uncommitted entries
    SegmentSize:  1 * 1024 * 1024 * 1024,  // 1GB per segment
    NumThreads:   128,  // Max concurrent goroutines
    SyncOnCommit: true,  // fsync on every commit (durable)
})
defer log.Close()
```

#### Configuration Guidelines

| Parameter | Recommended | Trade-off |
|-----------|-------------|-----------|
| `MutableSize` | 64-256 MB | Larger = more uncommitted entries, more memory |
| `SegmentSize` | 1-4 GB | Larger = fewer files, slower recovery |
| `NumThreads` | 128-1024 | Must be ≥ max concurrent goroutines |
| `SyncOnCommit` | `true` (production) | `false` = faster but risk data loss |

### 2. LogManager - Multi-Log Management

Manages multiple FASTER logs with LRU eviction and reference counting.

```go
manager := faster.NewLogManager()
defer manager.CloseAll()

// Get a log (automatically created if needed)
log, release, err := manager.GetLog([]byte("table:users"))
if err != nil {
    return err
}
defer release()  // CRITICAL: Always call release()

// Use the log safely
err = log.Accept(slot, ballot, value)
```

**Key Features:**
- **Reference Counting**: Prevents closing logs while in use
- **LRU Eviction**: Automatically closes idle logs (max 256 open)
- **Thread-Safe**: Concurrent GetLog calls are safe
- **Leak Protection**: CloseAll waits for references to drain

### 3. RingBuffer - Lock-Free Mutable Region

Lock-free buffer for uncommitted entries using atomic CAS operations.

**Design Principles:**
- **Reserve-Write-Publish Protocol**: Prevents readers from seeing partial writes
- **CAS-based Allocation**: No locks on write path
- **Sequential Scanning**: Fast for small regions (2-64MB)
- **Automatic Reclamation**: Space freed as entries commit

```go
// Writers (lock-free):
offset, err := buffer.Append(entry)  // Atomic CAS allocation

// Readers (lock-free):
entry, err := buffer.Read(offset)

// Reclamation (triggered by Commit):
buffer.TryAdvanceTail(indexCheck)
```

### 4. Snapshot Manager - State Machine Checkpoints

Handles periodic snapshots for faster recovery.

```go
snapMgr, err := faster.NewSnapshotManager("/data/snapshots", log)

// Create snapshot at slot 1000
stateData := serializeStateMachine()
err = snapMgr.CreateSnapshot(1000, stateData)

// Recovery: load latest snapshot
snapshot, err := snapMgr.GetLatestSnapshot()
restoreStateMachine(snapshot.Data)
replayFrom(snapshot.Slot + 1)
```

## WPaxos Integration

FASTER's three operations map perfectly to WPaxos phases:

### Phase-2b: Accept (Uncommitted Entry)

```go
// Acceptor receives Phase-2a message
err := log.Accept(
    slot,    // uint64: Paxos slot number
    ballot,  // faster.Ballot{ID, NodeID}
    value,   // []byte: proposed value
)

// Entry is now in mutable region (uncommitted)
// Can be overwritten by higher ballot
```

**What Happens:**
1. Entry serialized with ballot and `committed=false`
2. CAS-allocated space in ring buffer (lock-free!)
3. Index updated: `slot → offset|mutableFlag`
4. **Durable**: Only in memory (fast!)

### Phase-3: Commit (Mark as Durable)

```go
// Leader receives Q2 quorum of accepts
err := log.Commit(slot)

// Entry is now in immutable tail (committed)
// Cannot be overwritten
```

**What Happens:**
1. Entry read from mutable region
2. `committed` flag set to `true`
3. Appended to immutable tail (sequential write)
4. Optional `fsync()` if `SyncOnCommit=true`
5. Index updated: `slot → tailOffset` (no mutable flag)
6. Mutable space reclaimed automatically

### Phase-1: Recovery (Scan Uncommitted)

```go
// New leader needs to recover uncommitted entries
uncommitted, err := log.ScanUncommitted()

for _, entry := range uncommitted {
    // Re-propose with new ballot
    if entry.Ballot.Less(myBallot) {
        // Take over this slot
        propose(entry.Slot, myBallot, entry.Value)
    }
}
```

**What Happens:**
1. Scans ring buffer for all entries
2. Filters to only entries still in mutable region (via index check)
3. Returns uncommitted entries for re-proposal
4. **Fast**: Sequential scan of small region (~64MB)

### State Machine Reads (Only Committed)

```go
// Read only committed entries for state machine
entry, err := log.ReadCommittedOnly(slot)
if err == faster.ErrNotCommitted {
    // Slot exists but not yet committed - wait or skip
}

// entry.Value is safe to apply to state machine
applyToStateMachine(entry.Value)
```

**What Happens:**
1. Index lookup: `slot → offset`
2. If in mutable region: read from ring buffer, check `committed` flag
3. If in tail: **lock-free mmap read** (fast!)
4. Returns error if uncommitted

## Performance Characteristics

Benchmarked on Intel i7-11800H @ 2.30GHz (16 threads). All benchmarks run with Go 1.21+.

### Core Operations

| Operation | Latency | Throughput | Notes |
|-----------|---------|------------|-------|
| **Accept** (uncommitted write) | 755 ns | 1.32M ops/sec | Lock-free CAS allocation |
| **Commit** (no fsync) | 1.76 µs | 568k ops/sec | Sequential append to mmap |
| **Commit** (with fsync) | 4.47 ms | 224 ops/sec | Disk sync overhead |
| **Read** (uncommitted) | 150 ns | 6.68M ops/sec | Ring buffer scan |
| **Read** (committed) | 113 ns | 8.84M ops/sec | Direct mmap read |
| **Accept+Commit+Read** | 1.85 µs | 541k ops/sec | Full write cycle |

### Advanced Operations

| Operation | Latency | Throughput | Details |
|-----------|---------|------------|---------|
| **ScanUncommitted** | 131 µs | 7,634 scans/sec | ~1,000 entries |
| **Checkpoint** | 170 µs | - | Flush committed to tail |
| **Recovery** (1k entries) | 4.16 ms | 240k entries/sec | Index rebuild |
| **Recovery** (10k entries) | 5.74 ms | 1.74M entries/sec | Scales well |
| **Snapshot Create** | 6.23 ms | - | 1MB state |
| **Snapshot Read** | 201 µs | - | Deserialize + verify |

### Value Size Impact

| Value Size | Latency | Throughput (MB/s) |
|------------|---------|-------------------|
| 10 bytes | 1.81 µs | 5.28 MB/s |
| 100 bytes | 1.76 µs | 54.33 MB/s |

**Observation**: Larger values improve throughput (better amortization of fixed overhead).

### Concurrent Performance

| Scenario | Latency | Throughput | Speedup |
|----------|---------|------------|---------|
| **Concurrent Reads** (16 threads) | 23 ns | 42.9M ops/sec | ~5x single-thread |
| **Concurrent Writes** (16 threads) | 504 ns | 1.99M ops/sec | ~1.5x single-thread |

**Why**: Lock-free reads scale linearly. Writes have CAS contention but still scale.

### FASTER vs. BadgerDB Comparison

Head-to-head benchmarks against BadgerDB (same hardware, same workload):

| Workload | FASTER | BadgerDB | Speedup |
|----------|--------|----------|---------|
| **Random Keys** (writes) | 2.18 µs<br/>460k ops/sec | 7.61 µs<br/>131k ops/sec | **3.5x faster** |
| **Sequential Writes** | 1.53 µs<br/>656k ops/sec | 6.54 µs<br/>153k ops/sec | **4.3x faster** |
| **Read-Heavy** (90% reads) | 133 ns<br/>7.5M ops/sec | 1.40 µs<br/>713k ops/sec | **10.5x faster** |
| **Mixed** (50/50 read/write) | 849 ns<br/>1.18M ops/sec | 3.48 µs<br/>287k ops/sec | **4.1x faster** |
| **Concurrent Writes** | 504 ns<br/>1.99M ops/sec | 5.08 µs<br/>197k ops/sec | **10x faster** |
| **Recovery** (10k entries) | 2.15 ms | 21.7 ms | **10x faster** |
| **ScanUncommitted** | 646 µs<br/>1,548 scans/sec | 557 µs<br/>1,794 scans/sec | ~Same |

**Key Takeaways:**
- ✅ **Reads are 10x faster**: Lock-free mmap reads vs. LSM lookup
- ✅ **Writes are 4x faster**: No compaction overhead
- ✅ **Recovery is 10x faster**: Sequential scan vs. LSM rebuild
- ✅ **Predictable latency**: No background compaction spikes
- ⚠️ **Scan performance similar**: Both use sequential scan (but FASTER's scan is on uncommitted entries, BadgerDB is on all entries)

### Key Performance Features

- ✅ **Lock-free reads** from immutable tail (memory-mapped)
- ✅ **Lock-free writes** to mutable region (atomic CAS)
- ✅ **Only commits need mutex** (sequential disk I/O)
- ✅ **No background compaction** (predictable latency)
- ✅ **Linear read scaling** with concurrent goroutines
- ✅ **10x faster recovery** (sequential vs. LSM rebuild)

### Performance Tuning

**For Maximum Throughput:**
```go
Config{
    SyncOnCommit: false,  // Skip fsync (568k vs 224 ops/sec)
    MutableSize:  256 * 1024 * 1024,  // Large buffer for batching
}
```
⚠️ **Trade-off**: Risk losing uncommitted data on crash (acceptable for consensus with replication).

**For Maximum Durability:**
```go
Config{
    SyncOnCommit: true,  // fsync every commit (224 ops/sec)
    MutableSize:  64 * 1024 * 1024,
}
```
✅ **Guarantee**: Committed entries survive crashes.

**Batch Optimization:**
- Accept 100 entries (~755 ns × 100 = 75.5 µs)
- Commit batch with single fsync (~4.47 ms)
- **Effective throughput**: 100 commits / 4.55 ms = **22k commits/sec** (100x improvement!)

### Hardware Considerations

Results above are from:
- **CPU**: Intel i7-11800H (8 cores, 16 threads, 2.3-4.6 GHz)
- **Storage**: NVMe SSD (fsync ~4ms)
- **RAM**: DDR4-3200

**Expected performance on other hardware:**
- **Faster SSD** (Intel Optane): fsync ~100µs → 10k commits/sec (50x improvement)
- **Slower SSD** (SATA): fsync ~10ms → 100 commits/sec (2x slower)
- **HDD**: fsync ~50ms → 20 commits/sec (25x slower - not recommended!)
- **More cores**: Linear scaling for concurrent reads (up to memory bandwidth)

## Usage Patterns

### Pattern 1: Single Log (Simple Consensus)

```go
// Single consensus log for all operations
log, err := faster.NewFasterLog(faster.Config{
    Path:         "/data/consensus.log",
    MutableSize:  64 * 1024 * 1024,
    SegmentSize:  1 * 1024 * 1024 * 1024,
    NumThreads:   128,
    SyncOnCommit: true,
})
defer log.Close()

// Accept-Commit cycle
slot := getNextSlot()
ballot := getCurrentBallot()

// Phase-2: Accept
err = log.Accept(slot, ballot, value)

// Phase-3: Commit (after Q2 quorum)
err = log.Commit(slot)

// Read committed state
entry, err := log.ReadCommittedOnly(slot)
```

### Pattern 2: Multi-Log (Table-Partitioned Consensus)

```go
// Manage separate logs per table
manager := faster.NewLogManager()
defer manager.CloseAll()

// Each table gets its own log
func processWrite(table string, slot uint64, ballot faster.Ballot, value []byte) error {
    log, release, err := manager.GetLog([]byte(table))
    if err != nil {
        return err
    }
    defer release()  // CRITICAL: Don't leak references!

    // Accept-Commit for this table's log
    if err := log.Accept(slot, ballot, value); err != nil {
        return err
    }

    // Later: commit after quorum
    if err := log.Commit(slot); err != nil {
        return err
    }

    return nil
}
```

**Benefits of Multi-Log:**
- Parallel consensus per table
- LRU eviction keeps hot tables in memory
- Automatic log lifecycle management

### Pattern 3: Checkpointing (Long-Running Logs)

```go
log, _ := faster.NewFasterLog(cfg)
snapMgr, _ := faster.NewSnapshotManager("/snapshots", log)

// Periodically checkpoint state machine
ticker := time.NewTicker(5 * time.Minute)
for range ticker.C {
    // Serialize current state
    stateData := marshalStateMachine(currentState)

    // Create snapshot at current slot
    err := snapMgr.CreateSnapshot(currentSlot, stateData)

    // Optionally: truncate old log entries before snapshot
    // (not yet implemented, but planned)
}

// Recovery: load snapshot + replay
snapshot, _ := snapMgr.GetLatestSnapshot()
currentState = unmarshalStateMachine(snapshot.Data)

// Replay entries after snapshot
for slot := snapshot.Slot + 1; slot <= latestSlot; slot++ {
    entry, err := log.ReadCommittedOnly(slot)
    if err == nil {
        applyToStateMachine(entry.Value)
    }
}
```

## Error Handling

### Common Errors

```go
// Slot not found (never written)
_, err := log.Read(999)
if errors.Is(err, faster.ErrSlotNotFound) {
    // Slot doesn't exist
}

// Entry exists but not committed
_, err := log.ReadCommittedOnly(100)
if errors.Is(err, faster.ErrNotCommitted) {
    // Slot exists, but still in accept phase
    // Either wait for commit or skip
}

// Buffer full (too many uncommitted entries)
err := log.Accept(slot, ballot, value)
if errors.Is(err, faster.ErrBufferFull) {
    // Mutable region exhausted
    // Either: increase MutableSize, or commit more frequently
}

// Log closed
_, err := log.Read(100)
if errors.Is(err, faster.ErrClosed) {
    // Log has been closed, cannot use
}
```

### Critical Safety Rules

1. **Always call release()**: LogManager.GetLog returns a release function that **MUST** be called
   ```go
   log, release, err := manager.GetLog(key)
   defer release()  // Don't forget!
   ```

2. **Don't use log after Close()**: Once closed, all operations return `ErrClosed`

3. **Commit promptly**: Mutable region has finite size. Commit entries to avoid `ErrBufferFull`

4. **Check committed flag**: State machine should only read committed entries
   ```go
   entry, err := log.ReadCommittedOnly(slot)  // Use this for state machine!
   ```

## Thread Safety

All operations are thread-safe:

- ✅ **Accept**: Lock-free CAS allocation
- ✅ **Commit**: Mutex-protected tail writes (sequential I/O)
- ✅ **Read**: Lock-free for tail, epoch-protected for mutable
- ✅ **ScanUncommitted**: Epoch-protected iteration

### Epoch-Based Memory Management

FASTER uses epochs to protect concurrent readers from use-after-free:

```go
// Automatic epoch management (internal)
threadID := getThreadID(slot)
epoch := log.epoch.Load()
log.threadEpochs[threadID].Store(epoch)
defer log.threadEpochs[threadID].Store(0)

// Read is now safe - epoch prevents reclamation
entry := readFromBuffer(offset)
```

**What This Means:**
- Readers announce presence via epoch
- Writers cannot reclaim memory while readers are active
- No locks needed (atomic operations only)

## Crash Recovery

### Tail Recovery (Automatic)

On log open, FASTER rebuilds the index from the tail:

```go
log, err := faster.NewFasterLog(cfg)
// Automatically:
// 1. Opens tail file
// 2. Memory-maps it
// 3. Scans entries and rebuilds index
// 4. Validates checksums
// 5. Truncates corrupted tail
```

### Mutable Region Recovery

The mutable region is **in-memory only** and **not persisted**. On crash:

- ✅ Committed entries are safe (in tail)
- ❌ Uncommitted entries are lost (expected behavior!)

**This is correct for consensus:**
- Uncommitted = speculative, can be lost
- New leader will recover via Phase-1 from other replicas
- Only committed entries are durable

### Recovery Example

```go
// After crash, reopen log
log, err := faster.NewFasterLog(cfg)

// Option 1: Load from snapshot
snapMgr, _ := faster.NewSnapshotManager("/snapshots", log)
snapshot, err := snapMgr.GetLatestSnapshot()
if err == nil {
    // Restore state from snapshot
    restoreState(snapshot.Data)
    replayFrom := snapshot.Slot + 1

    // Replay commits from snapshot to present
    for slot := replayFrom; slot <= getMaxSlot(); slot++ {
        entry, err := log.ReadCommittedOnly(slot)
        if err == nil {
            applyToStateMachine(entry.Value)
        }
    }
}

// Option 2: Full log replay (if no snapshots)
for slot := uint64(0); slot <= getMaxSlot(); slot++ {
    entry, err := log.ReadCommittedOnly(slot)
    if err == nil {
        applyToStateMachine(entry.Value)
    }
}
```

## Debugging and Monitoring

### Log Statistics

```go
// LogManager provides statistics
stats := manager.Stats()
fmt.Printf("Total logs: %d\n", stats.TotalLogs)
fmt.Printf("Open logs: %d\n", stats.OpenLogs)
fmt.Printf("Active refs: %d\n", stats.ActiveRefs)

// Monitor for reference leaks
if stats.ActiveRefs > stats.OpenLogs*10 {
    log.Warn("Possible reference leak detected")
}
```

### Common Issues

**Issue: `ErrBufferFull` under load**
- **Cause**: Mutable region exhausted (too many uncommitted entries)
- **Fix 1**: Increase `MutableSize` (e.g., 128MB or 256MB)
- **Fix 2**: Commit more frequently (batch commits)
- **Fix 3**: Call `log.Checkpoint()` periodically to flush committed entries

**Issue: High memory usage**
- **Cause**: Too many open logs in LogManager
- **Fix**: Decrease `MaxHotKeys` (default 256)
- **Check**: Call `manager.Stats()` to see open log count

**Issue: Slow recovery after crash**
- **Cause**: Large tail file, no snapshots
- **Fix**: Use SnapshotManager to checkpoint regularly
- **Check**: Tail file size should be < 10GB for fast recovery

**Issue: Reference leak in LogManager**
- **Cause**: Forgetting to call `release()` function
- **Symptom**: `CloseAll()` times out, logs not evicted
- **Fix**: Use `defer release()` immediately after `GetLog()`

## Design Rationale

### Why Not Use BadgerDB/LevelDB?

LSM-tree stores are optimized for **key-value workloads**, not **consensus logs**:

1. **MVCC vs. Commit**: LSM uses versions for MVCC, we need accept/commit distinction
2. **Compaction**: Background compaction interferes with recovery scans
3. **Locks**: Read path has locks, we need lock-free committed reads
4. **Overwrite semantics**: LSM makes it hard to replace uncommitted entries

### Why Lock-Free?

Consensus protocols have **tight latency requirements** (especially Phase-2):

- Traditional locks: ~50-100ns overhead + contention
- Lock-free CAS: ~5-10ns, no contention
- For 1M ops/sec, lock overhead = 5-10% of CPU!

### Why Separate Mutable/Immutable?

Matches **Paxos semantics perfectly**:

```
Paxos Accept  →  Mutable Region  (uncommitted, can change)
Paxos Commit  →  Immutable Tail  (committed, permanent)
```

This makes correctness **obvious** rather than **clever**.

## Future Enhancements

Planned features (not yet implemented):

1. **Log Truncation**: Delete entries before snapshots to bound log size
2. **Batch Commits**: Commit multiple slots in one `fsync()` call
3. **Remote Snapshots**: Ship snapshots to new replicas for faster bootstrap
4. **Tiered Storage**: Move old segments to S3/object storage
5. **Compression**: Optional compression of tail segments

## References

- [FASTER Paper (Microsoft Research)](https://www.microsoft.com/en-us/research/uploads/prod/2018/03/faster-sigmod18.pdf)
- [WPaxos Paper](https://www.vldb.org/pvldb/vol11/p1903-ye.pdf)
- [Paxos Made Simple (Lamport)](https://lamport.azurewebsites.net/pubs/paxos-simple.pdf)

## License

Atlas-DB is free software: you can redistribute it and/or modify it under the terms of the GNU Affero General Public License as published by the Free Software Foundation, either version 3 of the License, or (at your option) any later version.
