# Consensus

Atlas relies on a wide-area variant of WPaxos to coordinate ownership and replication. Consensus code lives in
`atlas/consensus` and is exposed through the `Consensus` gRPC service that the Caddy module registers for each node.
Every node maintains two Badger databases: one for application data and one for cluster metadata. Metadata stores
representations of nodes, tables, ownership, migrations, and ACL state.

## Architecture overview

- **Bootstrap & Caddy integration**: the `atlas/caddy` module initialises the local Badger stores, joins or seeds the
  cluster through the bootstrap service, starts the gRPC server, and exposes the Unix socket protocol.
- **Repositories**: `NodeRepositoryKV`, `TableRepositoryKV`, and `MigrationRepositoryKV` encapsulate metadata access.
  They track nodes by ID and region, table configuration (including allowed or restricted regions), and the migration
  log.
- **Connection manager**: `NodeConnectionManager` maintains pooled gRPC clients with health checks and RTT sampling used
  for quorum hints and `NODE PING`.
- **Quorum manager**: `defaultQuorumManager` caches known nodes per region and computes quorums per table on demand.

## Consensus service surface

The gRPC service defined in `consensus.proto` powers the control plane:

- `JoinCluster` – registers or refreshes a node. Node metadata is validated and stored, and the quorum manager is
  updated so that the node can participate in future quorums.
- `Ping` – lightweight reachability probe written by `NODE PING` and health monitoring.
- `StealTableOwnership` – Phase 1 of WPaxos. A requester proposes a higher ballot for a table. The server checks
  existing ownership, compares versions and node IDs, and, when successful, returns missing migrations that the new
  owner must apply.
- `WriteMigration` / `AcceptMigration` – Phase 2 commit helpers that apply mutations to storage. Mutations are described
  as `KVChange` operations (`SET`, `DEL`, `ACL_SET`, `ACL_DEL`, and `ADD_NODE`).
- `GossipMigration` – propagates committed migrations to random peers to reduce staleness.
- `PrefixScan` – broadcasts prefix scans across the cluster and merges results from all nodes.

## Quorum selection

Two quorums are maintained per table:

- **Q1** (phase 1) protects leadership changes such as ownership steals.
- **Q2** (phase 2) covers replication commits.

Runtime options `Fn` (nodes tolerated per zone) and `Fz` (zones tolerated) come from `atlas/options`. The quorum manager
filters nodes by table policy, prunes unhealthy endpoints using the connection manager, then chooses regions:

1. Calculate eligible regions for Q1 and Q2 with the configured failure budgets.
2. Prefer farthest regions for Q1 to maximise availability and closest regions for Q2 to minimise latency.
3. If quorum constraints cannot be met the manager reduces `Fn`/`Fz` progressively; failure to find a quorum raises an
   error surfaced to callers such as `QUORUM INFO` or `KEY` operations.

## Table ownership & groups

Tables are versioned entities stored in the metadata repository. Each table records an owner node and optional
allowed/restricted regions. When a table represents a **group**, `StealTableOwnership` ensures every member of the group
moves together so that grouped tables stay colocated.

Ownership transitions:

1. Requester calls `StealTableOwnership` with an incremented version.
2. The current owner (or replicas) validate the ballot, returning missing migrations if the new owner wins.
3. The requester applies migrations, writes them via `WriteMigration`, and gossip propagates the changes.

## Migrations and ACL propagation

Migrations describe deterministic updates applied to either the metadata store (`atlas.*` keys) or the data store.
`applyMigration` handles the following operations:

- `SET` – write bytes to the key/value store.
- `DEL` – delete a key.
- `ACL_SET` / `ACL_DEL` – manage access control lists in metadata, removing the ACL when the owner list becomes empty.
- `ADD_NODE` – add or update a node entry and feed it into the quorum manager.

Committed migrations are recorded with monotonically increasing `(table, table_version, migration_version, node_id)`
keys so that nodes can reconcile divergent histories and resume gossip safely.

## Prefix scanning and discovery

`PrefixScan` requests originate from the socket `SCAN` command. The quorum manager selects a majority quorum, broadcasts
`PrefixScanRequest` to every member, and merges the returned key lists. Errors surfaced by any node yield `ERROR WARN`
to clients, while partial success is logged for operators.

## Failure handling

- `Fn`/`Fz` can be tuned at runtime through `atlas/options` to reflect deployment shape (regions and zones).
- When quorum selection fails the manager reduces the failure budgets until it either finds a valid configuration or
  reports `unable to form a quorum`.
- Gossip maintains eventual consistency: migrations that arrive out-of-order are queued until their predecessors appear,
  preventing gaps in the log.

This consensus layer underpins every command that touches replicated state (`KEY`, `ACL`, `SCAN`, `NODE`, and
`QUORUM INFO`) and is the authoritative source of truth for cluster membership.
