# Migrations

AtlasDb is built on sqlite as its primary data store and uses sessions to keep track of general changes to the database.
One short-coming of sessions is that they do not keep track of schema changes, so we must keep track of them ourselves.
The following outlines the general process for changing the schema.

## Preparing the migration

When a migration command is received, we must first accept it.
We begin a session and write the command to the `__migrations` table.
This table is used to track all possible migrations that have been (or were attempted) to be applied to the database.

After the session is captured, we submit (a paxos Prepare step) the migration to all replicas in the system.
A simple majority of replicas must accept this migration before any further action can be taken.

### Errors during preparation

Errors may occur during this phase. Here are the most common ones and how they are handled.

#### Failure to write to `__migrations` table

If we fail to write, we will retry a couple of times. If we still fail, we will abort the migration.

#### Failure to submit to replicas

If we fail to submit to a replica, we will retry a couple of times. If we still fail, we will abort the migration.

#### Failure to get a majority of replicas to accept

If we fail to get a majority of replicas to accept, we will abort the migration.

#### Conflicting migrations

If we find that a migration is already in progress, we will abort the migration and the existing migration will be applied.

## Applying the migration

Once we have a promise that replicas will accept the migration, we ask them to accept it.
This causes all replicas to write the migration to their own `__migrations` table and apply the migration.

Finally, we commit the migration to the database and close the session.

## Replicas

All nodes in AtlasDb are considered schema replicas.
However, not every node contains data for those tables.
This allows any node to quickly become a data replica by simply copying the data from another node.
For example, if a node is consistently querying a table that it does not have data for,
it can request that it become a data replica.
