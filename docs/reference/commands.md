# Commands

At the moment, the following commands/statements are supported:

### CREATE [LOCAL|REGIONAL|GLOBAL] TABLE ...

Creates a new table with the specified replication level. The default is `GLOBAL`.

### PRAGMA ATLAS_CONSISTENCY = ...

The `pragma` statement is used to set the consistency level for the following queries. The consistency levels are:

1. `strong`
2. `bounded`
3. `linearizable`
4. `eventual`

Example:

```sql
pragma
atlas_consistency
= strong;
```

Note that any write to the database automatically switches the consistency level to `linearizable`.

### PRAGMA ATLAS_NODE = ID

This outputs the current node’s ID or migrates the session to the specified node. This is useful for debugging and
understanding the current state.

Note that session migration may change the current node upon running a query.

### PRAGMA ATLAS_SESSION_MIGRATION = ON|OFF

This enables or disables session migration. By default, session migration is enabled.

### PRAGMA ATLAS_TABLES

This outputs the tables that the current node owns.
