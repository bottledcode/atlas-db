# Query Hints

Query hints provide additional information to AtlasDb and can be used to optimize queries.

## Syntax

Query hints are specified in square brackets, are case-insensitive, in a comment, and are always the second term in any
query.

## Select Hints

### Eventually Consistent (EC)

You may perform a query with this hint to indicate that you don’t care if the data is stale or not up to date:

```sql
select /* [EC] */ *
from users;
```

### Session Consistent (SESSION)

This is the default mode of AtlasDb. In this mode, you will always "read-after-write" but may not see other writes.

```sql
select /* [SESSION] */ *
from users;
```

### Strongly Consistent (SC)

In this mode, you will always see the latest committed data. This is the slowest mode of AtlasDb.

```sql
select /* [SC] */ *
from users;
```

## Insert Hints

There are currently no insertion hints

## Special Commands

AtlasDb provides several special commands that can be used when performing table creations:

### Create Table

#### Local Tables

Local tables are tables that are **not** replicated; however, the schema is replicated.
This allows you to use the database as a sort-of cache where the data is local to that node.

```sql
create
local table user (id int primary key, name text);
```

#### Regional Tables

Regional tables are tables that are replicated across multiple nodes within a region.
Every region will have its own copy of the data.

```sql
create
regional table user (id int primary key, name text);
```

#### Global Tables

Global tables are tables that are replicated across all regions. This is the default behavior of AtlasDb.

```sql
create table user
(
    id   int primary key,
    name text
);
```

### Joins

You may join between any table in AtlasDb. This is done by "stealing" the data from another node and becoming the owner
of that data.
This can cause severe latency if care is not taken.
In most cases, AtlasDb will attempt to run the query on a node that owns as much data as possible.
However, this could be on the other side of the planet!

### Joins with local/regional tables to global tables

Joining a local/regional table to a regional or global table can cause severe latency.
It is not recommended!
This is
because AtlasDb **must** ensure
that the current node is a data replica for the tables being joined as well as "stealing" ownership of those tables.
This **will fail** if the joined tables cannot be replicated to the current node or region.

### Alter Table

AtlasDb provides support for "region locking" a global table.
This allows you to keep data in specific regions or forcibly migrate data out of a region.

#### Region Locking

Syntax:

> ALTER TABLE <table_name> LOCK REGION <region_name>, <region_name>, ...;

```sql
alter table user lock region us-west-1, us-west-2; 
```

Upon running the above command, all data replicas outside of those regions will be deleted.
All queries attempting to access those tables from outside that region will fail.

> NOTE: There is no protection against data loss! Be careful when using this command.

#### Region Unlocking

Syntax:

> ALTER TABLE <table_name> UNLOCK REGION [ALL]|<region_name>, <region_name>, ...;

```sql
alter table user unlock region all;
```

Upon running the above command, data will immediately begin replicating to all regions.

```sql
alter table user unlock region us-west-1;
```

This would remove the region lock on `us-west-1` and begin deleting the replicas in that region.
Only other locked regions will have data.

#### Region Migration

Syntax:

> ALTER TABLE <table_name> MIGRATE REGION <region_name>;

```sql
alter table user migrate region us-west-1;
```

Upon running the above command, a data replica in `us-west-1` will become the owner of the data for this table.
AtlasDb may decide to immediately move the table to another region if it is more optimal.
This is typically done for analysis by copying a table and then migrating it to a more local region. 
