/*
 * This file is part of Atlas-DB.
 *
 * Atlas-DB is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of
 * the License, or (at your option) any later version.
 *
 * Atlas-DB is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with Atlas-DB. If not, see <https://www.gnu.org/licenses/>.
 *
 */

/* Nodes allow us to represent active/inactive nodes throughout the cluster */
create table nodes
(
    /* A globally unique identifier for this node */
    id         integer not null primary key,

    /* The address of the node */
    address    text    not null,

    /* The port of the node */
    port       int     not null,

    /* The region of the node */
    region     text    not null,

    /* The state of the node */
    active     int     not null,

    /* The time the node was created */
    created_at timestamp default CURRENT_TIMESTAMP,

    /* The latency of the node */
    rtt        int     not null
);

/* Tables represent tracked and replicated tables in the cluster; the lack of foreign keys is important!
   During cluster bootstrap, we do not yet have any data anywhere, so we need to be able to bootstrap the cluster.
   Additionally, a node may receive a table with an owner that it has not yet received information about! */
create table tables
(
    /* The fully qualified name of the table */
    name               text not null primary key,

    /* The replication level of the table (immutable) */
    replication_level  text check (replication_level in ('local', 'regional', 'global')),

    /* The node that owns the table */
    owner_node_id      integer,

    /* The time the table was created */
    created_at         timestamp     default CURRENT_TIMESTAMP,

    /* The version of the table (ballot number) */
    version            int  not null default 0,

    /* The allowed regions for the table -- mutually exclusive with restricted regions
       This column defines the regions that are allowed to read/write to the table.
       Only regions defined here will be replicated to/from.
       The table won't even be tracked in other regions.
    */
    allowed_regions    text not null default '',

    /* The restricted regions for the table -- mutally exclusive with allowed regions.
       This column defines the regions that are not allowed to read/write to the table.
       The table will be tracked in all regions except the ones defined here.
    */
    restricted_regions text not null default ''
);
/* Insert the nodes table as a tracked table */
insert into tables
values ('atlas.nodes', 'global', null, current_timestamp, 0, '', '');

/* Migrations are defined as rows on this table.
   They are ultimately numbered by the version and batch_part columns.
   A batch is a set of migrations that belong to a single version, thus a version may have multiple batches.
   A version may be given to a node via consensus (gossip == 0) or via gossip (gossip == 1).
*/
create table migrations
(
    /* The table that the migration is for */
    table_id      text not null
        constraint migrations_tables_id_fk
            references tables,

    /* The version of the migration */
    version       int  not null,

    /* The version of the table that the migration is for */
    table_version int  not null,

    /* The part number of the batch that the migration belongs to */
    batch_part    int  not null,

    /* The node (leader) that applied the migration */
    by_node_id    int  not null,

    /* The command to run on the user table */
    command       text          default null,

    /* The data to apply to the user table */
    data          blob          default null,

    /* Whether the migration was committed */
    committed     int  not null default 0,

    /* Whether the migration was received via gossip */
    gossip        int  not null default 0,

    primary key (table_id, table_version, version, batch_part, by_node_id)
);

/* Now we add the ability to have table groups in the cluster
   Groups self-reference the tables table such that a group is tracked just like a table
*/
alter table tables
    add column group_id text default null;
alter table tables
    add column is_group int default 0;
create index idx_tables_group_id on tables (group_id);

/* In the following migration, we add support for triggers and views */
alter table tables
    add column table_type text default 'table' check ( table_type in ('table', 'trigger', 'view', 'group') );
update tables
set table_type = 'group'
where is_group = 1;
alter table tables
    drop column is_group;

/*
 * This file is part of Atlas-DB.
 *
 * Atlas-DB is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of
 * the License, or (at your option) any later version.
 *
 * Atlas-DB is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with Atlas-DB. If not, see <https://www.gnu.org/licenses/>.
 *
 */

/* In the following migration, we add support for sharded tables.
   These tables are treated like table groups but are owned individually instead of collectively.
   They should use `group_id` to reference the table they actually belong to -- shards may not be part of a group.
   A sharded table (the parent table)
   can be part of a group so long as all the group participants are sharded by the same principles
   and is reflected in the group's `shard_principles` column.
*/
alter table tables
    add column table_type_new text default 'table' check ( table_type_new in ('table', 'trigger', 'view', 'group', 'sharded') );
-- noinspection SqlWithoutWhere
update tables
set table_type_new = table_type;
alter table tables
    drop column table_type;
alter table tables
    rename column table_type_new to table_type;
alter table tables
    add column shard_principles text default '' not null;
