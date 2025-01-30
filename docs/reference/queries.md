# Query Execution and Serializability in AtlasDb

## Introduction

AtlasDb is a distributed database designed to ensure serializable query execution while balancing performance and
availability across multiple regions. It provides strong consistency guarantees, optimizes query routing, and supports
efficient transaction management. This document outlines how AtlasDb executes queries and enforces serializability.

## Query Execution Model

### Query Planning and Execution

When a client submits a query, AtlasDb parses and analyzes it to determine the optimal execution strategy. The query
planner optimizes execution based on factors such as data locality, consistency requirements, and replication
constraints. Write queries are only allowed within a `BEGIN IMMEDIATE` transaction to guarantee atomicity and isolation.
Read queries, however, can be executed within or outside a transaction, depending on the client’s requirements.

### Query Routing and Execution

Write transactions typically execute on the local node unless specific conditions require otherwise. The system first
determines the leader of the table being modified. If the leader resides in the local region, the writer temporarily
borrows the table for the duration of the transaction. If the leader is remote, a table leadership transfer (stealing)
may be initiated for long-term ownership.

A leader may deny a steal attempt if it determines that the local region has a higher write load, in which case the
writer continues borrowing the table. To maintain consistency, tables, views, and triggers that are frequently updated
should be grouped to ensure that updates propagate correctly across replicas.

## Borrowing Process

Borrowing allows a node to execute a write transaction without assuming long-term ownership of the table. The process
follows these steps:

1. The leader initiates an immediate transaction and synchronizes the writer with the latest table state.

2. The writer executes the transaction while maintaining consistency with the leader.

3. The writer sends the migration changeset to the leader to ensure state consistency before commit.

4. The leader applies the migration and verifies the changes.

5. Upon commit, the leader finalizes the migration using a two-phase commit protocol to ensure atomicity and durability.

6. If a rollback occurs, the leader propagates the rollback using the same two-phase mechanism.

7. In the event of a leader failure, the writer assumes leadership of the table. If another node is elected leader, it
captures the pending migration on commit.

Borrowing allows transaction execution with minimal leadership disruption while maintaining strong consistency
guarantees.

### Borrowing and SQLite WAL Mode

AtlasDb leverages SQLite’s Write-Ahead Logging (WAL) mode, which enhances Borrowing by enabling the following
optimizations:

1. **Full Database Locking for Writes**: SQLite locks the database during `BEGIN IMMEDIATE` transactions, ensuring that no
conflicting writes occur. This eliminates the need to coordinate multiple write transactions.

2. **Concurrent Read Access**: While writes are blocked, WAL mode enables concurrent reads, ensuring that query execution
performance remains high.

3. **Eliminating Leader Election Overhead**: Borrowing avoids the costly leader election process (Phase 1 of Paxos), allowing
transactions to execute immediately.

4. **Failure Recovery**: If the leader fails mid-transaction, the writer can seamlessly assume leadership and ensure
transaction durability.

5. **Optimized Multi-Table Transactions**: If a leader already governs multiple tables involved in a transaction, only a single
leader lock is required, reducing synchronization overhead.

By integrating Borrowing with SQLite’s WAL mode, AtlasDb minimizes transaction latency while maintaining strong
consistency and availability guarantees.

## Definitions

**Borrowing**: A temporary execution mechanism where a node processes a write transaction without permanently assuming table
leadership, ensuring synchronized execution with the leader.

**Stealing**: The process of transferring table leadership to a new node, allowing it to manage writes long-term while
ensuring strong consistency.

**Leader**: The node responsible for executing and committing writes to a particular table, ensuring serializability across
transactions.

**Writer**: A node executing write operations within a region but not necessarily the leader. Writers help distribute the
load while maintaining synchronization with the leader.

**Replication Constraints**: Rules defining how data is propagated across nodes to guarantee consistency for read and write
operations.

By combining Borrowing, leader-based execution, and SQLite WAL mode, AtlasDb ensures high-performance, serializable
query execution in a distributed environment while minimizing disruption to ongoing transactions.
