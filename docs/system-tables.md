# System Tables

AtlasDb has a few system tables that can be queried for debugging.
Writing to these tables is not recommended and may cause severe and unrecoverable issues.

## `__migrations`

This table is used to track all possible migrations that have been (or were attempted) to be applied to the database.

## `__nodes`

This table is used to track all nodes in the system.

## `__regions`

This table is used to track all regions in the system.

## `__table_migrations`

This table is used to track all possible migrations that have been (or were attempted) to be applied to a specific table.

It is used to reconstruct a table’s schema at any point in time.

## `__table_nodes`

This table is used to track all nodes that contain data for a specific table.

## `__tables`

This table is used to track all user tables in the system and their current mode.
