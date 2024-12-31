# Migration Protocol

Paxos, and by extension, Atlas, is not tolerant of byzantine failures.
This means that if a node is behaving incorrectly or maliciously, it can cause the entire system to fail.
This document serves as a living document for the migration protocol, based upon S-Paxos, W-Paxos, and F-Paxos.

## Overview

Atlas desires to distribute tables of the system across multiple nodes.
This is done to increase the availability of the system
and in such a way to also decrease the latency of reads across the planet.
It does this by using a Paxos-based protocol to ensure that all nodes agree on the state of the system
and ensuring that latency is minimized by keeping replica nodes close to the client.

## Quorums

To ensure that the system is consistent, we must ensure that a majority of nodes agree on the state of the system.
This is done through a "quorum" of nodes.
Every table in the system has its own quorum configuration.

### Single node cluster

For a single node cluster, all tables behave as if they are local tables.
However, if a second node is added, non-local tables will immediately begin replication.
The type of replication depends on the table’s replication level and whether the new node is in the same region as the
original.

### Multi-node cluster (global tables)

For a multi-node cluster, the user can specify how many regions may fault and the number of replicas per region.
It is not recommended to have more than four replicas per region,
but the number of regions that may fault can be arbitrary.
**Note that this is not RAFT-based consensus,
the number of nodes per region can be any number greater than 1**, there will always be a quorum.
Quorums and replication is dynamic and self-healing.
If a node fails, the system will automatically reconfigure itself to ensurer that a quorum is always available.

### Quorum selection

When a node attempts to "steal" ownership of a table -- including creating new tables -- it must first acquire a quorum.
This is akin to Phase 1 of Paxos.

Quorums in Phase one are spread across all regions.
It is only necessary to select at least one node in each region and the current owner,
though sometimes more will be selected depending upon quorum selection.
The selected quorum is sent along during phase 1. Each node is responsible
for ensuring it intersects with a previous phase 1. If it does not,
it must reject it—unless all previous nodes are down.

If a node determines that the stealer is missing data, the node must respond with the missing data.
The client is responsible for learning about the missing data, applying it to its state, and then retrying the steal.
During heavy writes this may result in the inability to steal.
Thus, a client also sends a "reason" for the steal.
Certain reasons have priority over Writes and will be allowed to steal, as well as sending back any missing data.

Each node in the quorum sends back to the stealer a "promise"
to not accept any other steal requests and to not accept any Writes until the steal is complete.
The stealer then notifies other servers of the steal and becomes the facto owner, or not.
If a stealer dies, another node will pick up the steal with a reason: "owner died" and the process will continue.

Once an owner is selected, it gains the right to accept Writes.
If a Write is performed,
the owner captures the session changeset and sends it to nodes from the original quorum in the current region.
It does **not** send it to other regions.
This is akin to Phase 2 of W-Paxos.
The owner must wait for a majority of nodes in the region to acknowledge the Write, 
and at least one of these nodes must be from the original quorum.
The node from the original quorum is responsible for sending the changeset to the other regions.

If a node dies from the original quorum, a new stealing round must begin.
The node that wants to become the owner of the table and "steal" it must first select a quorum of nodes.
This quorum **must** intersect with the previous stolen quorum.
This will likely mean a majority of nodes in the previous region + a node from every other region.

### Quorum size

For Q1 (Phase 1), nodes are selected from a pool of (total regions)—(tolerated region failures).
For Q2 (writes), the quorum is selected from a pool of (tolerated region failures) + 1.


## Analogs

- Node ids are globally unique, unlike in wpaxos where they are a tuple.
- Ballots are table versions
- Slots are versions in the migrations table
- Instances: ballot + command + committed = []slots

- o = table object or migration
- b = ballots[o] = table version
- n = node id
- s = slots[o] = migration
- v = <slots[o], self> = migration + node id = command

## Phases

**Steal: phase 1**

**client: 1a**
1. check if `o` is in `own`
   1. if `yes` goto phase 2
2. send {sender, table object, incremented table version} to Q1

**node: 1b**
1. if `table version` < `local table version`: reject
2. store `table entry` in `tables`
3. if `table` in `own`: remove from `own`
4. send {server node, table entry, table version, uncommitted migrations} to client

**client: 2a**
1. collect messages from Q1
2. for each `uncommitted migration` + `current migration`:
   1. append to log
   2. send {sender, table object, table version, migration version, migration} to Q2

**node: 2b**

1. if `table version` <= `local table version`: reject
2. store `table entry` in `tables`
3. append to log
4. send {server node, table entry, table version, migration version} to client

**client: 3**
1. collect messages from Q2
2. if Q2 rejects, abort
3. if Q2 accepts, commits, and distributes to replicas
