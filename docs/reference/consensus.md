# Consensus

Atlas works through distributed consensus based upon the [W-Paxos algorithm](https://arxiv.org/abs/1703.08905).
This algorithm is a variant of the [Paxos algorithm](https://en.wikipedia.org/wiki/Paxos_(computer_science)) that is
designed to be performed over a wide area network.

## Overview

Atlas is designed to work as an edge database, deployed in a distributed manner, and able to work in a wide area
network.
The granularity of Atlas is at the table level.

## Ownership

A single node owns each table in Atlas.
This node is responsible for the table’s schema and data.
Another node can "steal" the table from the owner by providing a reason for the steal.
If the cluster agrees that this is the best decision for the table, the table ownership is transferred to the new owner.

## Replication

Any Atlas node can replicate any table. In fact, all Atlas nodes _always_ replicate all table schemas, even if the data
is not replicated.
A node may register an 'interest' in a table, which will cause the node to replicate the data for that table.

### Availability

There are three types of replication in Atlas:

1. global
2. regional
3. local

Global replication is when the data is replicated to all regions of the cluster.
This is in the form of a "bounded"
replication in which updates are batched based on time (30 seconds by default) or updates (100 by default).
These updates are then accepted by all replicas for that table in the cluster (using a flexible quorum approach).

Regional replication ensures that every region has a unique copy of the data, and is stored durably in each region.
This allows for region-specific caches and is the most similar to other distributed databases that rely on the RAFT
algorithm.

Local replication disables replication and acts like a single-node database.
This is useful for development and testing, as well as for node-level caches.

A table is created with a default replication, and it **cannot be changed** once configured.
However, tables can be copied to new tables with different replication settings.

Atlas attempts to have at-most 4 replicas per region per table.

### Consistency

Atlas uses a flexible quorum approach to ensure consistency.
Via a `pragma` statement, the user can specify what kind of consistency they want for following queries.

The consistency levels are:

1. strong
2. bounded
3. linearizable
3. eventual

Strong consistency queries at least two replicas in the current region and takes the most recent value,
while bounded ensures that the data is no older than the specified bounds.
Linearizable queries the owner of the table directly, while eventual queries any nearby replica.

All writes _must_ go through the owner of the table.
In fact, any attempted write to a non-owner replica will migrate the current query to the owner.

## Transactions and Session Migration

Atlas supports transactions implicitly.
It does this through "session migration" where a session started on one node can be migrated to another node
transparently.
This is done in certain cases to support joins where one table exists on one node and another table exists on another
node.
In the worst case, one of the nodes will have to copy over the data and become a new replica.
This should only happen once (for example, a new application version is deployed)
as applications tend to continuously run the same query patterns.
