# Consensus

Atlas works through distributed consensus based upon the [WPaxos algorithm](https://arxiv.org/abs/1703.08905). This
algorithm is a variant of the [Paxos algorithm](https://en.wikipedia.org/wiki/Paxos_(computer_science)) designed to
operate efficiently over wide-area networks (WANs).

---

## Overview

Atlas is an edge database deployed in a distributed manner, designed to work across a wide-area network. The system
operates at the granularity of individual tables, with a single node owning each table’s schema and data. Ownership can
transfer dynamically to optimize locality, load balance, or recover from faults.

---

## Ownership

### Ownership Transfer

A single node owns each table in Atlas.
Ownership is essential for maintaining consistency, as all Writes must be
directed to the table’s owner. A table’s ownership can be transferred to another node when:

- **Load balancing**: The current owner is overloaded.
- **Locality optimization**: Moving ownership closer to frequent accessors reduces latency.
- **Fault recovery**: The current owner becomes unreachable.

#### Ownership Transfer Process

1. A new owner proposes a transfer with a valid reason.
2. Cluster nodes vote on the proposal based on the current state.
3. If the majority agrees, ownership is transferred to the new owner.
4. Future writes are redirected to the new owner.

---

## Replication

Atlas supports table-level replication, with all nodes replicating table schemas, even if the data itself is not
replicated.
A node can register an "interest" in a table, triggering data replication for that table.

### Replication Types

1. **Global Replication**:
    - Data is replicated to all regions in the cluster.
    - Updates are batched based on time (default: 30 seconds) or count (default: 100 updates).
    - Replication is performed using flexible quorums.

2. **Regional Replication**:
    - Each region has a unique copy of the data stored durably.
    - Enables region-specific caches for performance.

3. **Local Replication**:
    - Data is not replicated beyond a single node.
    - Useful for development, testing, or node-level caches.

Once configured, a table’s replication type cannot be changed. However, data can be copied into a new table with a
different replication type.

---

## Quorum Selection and Management

### Quorum Definitions

1. **Phase-1 Quorum (Q1)**:
    - Used for leader election or table stealing.
    - Spans multiple regions and ensures intersection with Q2.

   **Formula**:

```
Cardinality(Q1) = (fn + 1) x (Z - fz)
```

Where:

- `fn`: Nodes allowed to fail in each region.
- `fz`: Regions allowed to fail.
- `Z`: Total number of regions.

2. **Phase-2 Quorum (Q2)**:

- Used for committing Writes during normal operation.
- Optimized for latency and fault tolerance.

**Formula**:

```
Cardinality(Q2) = (l - fn) x (fz + 1)
```

Where:

- `l`: Total nodes in a region.

### Quorum Selection Process

1. **Current Owner Anchoring**:

- Always include the current owner in Q1 to ensure consistency and avoid "lost" writes.
- Always include the new owner in both Q1 and Q2 to ensure future data consistency.

2. **RTT-Based Region Selection**:

- Select regions dynamically based on recent RTT measurements for low-latency quorum formation.
- Fallback to deterministic rules (e.g., node IDs) in cases of conflict.

3. **Fallback for Owner Unavailability**:

- If the owner is unavailable:
    - Temporarily set `fz = 0` and `fn = 0`, requiring all nodes to form Q1.
    - Recover state and elect a new owner.

---

### Example

#### Scenario

- **Regions**:
- Region A: 2 nodes.
- Region B: 5 nodes (leader).
- Region C: 3 nodes.
- **Parameters**: `fz = 1`, `fn = 1`.

#### Q1 Selection

1. Include the leader (Region B).
2. Select:

```
(fn + 1) * (Z - fz) = (1 + 1) * (3 - 1) = 4 nodes
```

- 2 nodes from Region B (leader + another).
- 2 nodes from Region C (RTT-based).

#### Q2 Selection

1. Include the leader (Region B).
2. Select:

```
(l - fn) * (fz + 1) = (5 - 1) * (1 + 1) = 5 nodes
```

- 4 nodes from Region B.
- 1 node from Region A (closest RTT).

#### Validation

- **Intersection**: The leader is in both Q1 and Q2.
- **Fault Tolerance**:

```
Fmin = min(Cardinality(Q1), Cardinality(Q2)) - 1 Fmin = 4
Fmax = N - Cardinality(Q1) - Cardinality(Q2) + (fz + 1) * (fn + 1) Fmax = 10 - 4 - 5 + (1 + 1) * (1 + 1) = 5
```

---

### Handling Failures

#### Owner Down

1. Detect owner unavailability.
2. Increase quorum size (`fz = 0, fn = 0`) to recover state.
3. Elect a new owner from the recovered quorum.
4. Revert to normal operation once the new owner is established.

#### Quorum Adjustments

- Dynamically adjust quorums as nodes or regions join or leave.
- Ensure intersection between old and new quorums during transitions.

---

## Summary

Atlas ensures fault tolerance and performance through flexible quorum selection and ownership anchoring:

1. Anchors quorums to the current and future owner.
2. Optimize quorums for latency while guaranteeing intersection.
3. Adjust dynamically during failures to recover state.
4. Validate configurations to avoid data loss.

These strategies enable Atlas to remain resilient and performant in wide-area, distributed environments.
