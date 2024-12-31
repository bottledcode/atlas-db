create table regions
(
    name text not null primary key
);
create table nodes
(
    id         integer not null primary key,
    address    text    not null,
    port       int     not null,
    region     text    not null
        constraint nodes_regions_id_fk references regions,
    active     int     not null,
    created_at timestamp default CURRENT_TIMESTAMP,
    rtt        int     not null
);
create table tables
(
    table_name         text not null primary key,
    replication_level  text check (replication_level in ('local', 'regional', 'global')),
    owner_node_id      integer
        constraint tables_nodes_id_fk
            references nodes,
    created_at         timestamp     default CURRENT_TIMESTAMP,
    version            int  not null default 0,
    allowed_regions    text not null default '',
    restricted_regions text not null default ''
);
create table migrations
(
    table_id     text    not null
        constraint migrations_tables_id_fk
            references tables,
    version      integer not null,
    batch_part   integer not null,
    by_node_id   integer not null
        constraint migrations_nodes_id_fk
            references nodes,
    command      text default null, -- command to run on the user table
    data         blob default null, -- data to apply to the user table
    self_command text default null, -- command to run on the migration table
    primary key (table_id, version, batch_part, by_node_id)
);
create table own
(
    table_id   integer not null primary key
        constraint own_tables_id_fk
            references tables,
    table_name text    not null -- denormalized for easy access
);
create unique index own_table_name_uindex
    on own (table_name);
