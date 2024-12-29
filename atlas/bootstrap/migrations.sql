create table tables
(
    id            integer not null primary key autoincrement,
    table_name    text    not null,
    is_region_replicated integer not null,
    is_global_replicated integer not null
);
create table regions
(
    id   integer not null primary key autoincrement,
    name text    not null
);
create index regions_name_uindex
    on regions (name);
create table nodes
(
    id        integer not null primary key autoincrement,
    address   text    not null,
    port      int     not null,
    region_id int     not null
        constraint nodes_regions_id_fk references regions
);
create table table_nodes
(
    id       integer not null primary key autoincrement,
    is_owner INTEGER not null,
    table_id integer
        constraint table_nodes_tables_id_fk
            references tables,
    node_id  integer
        constraint table_nodes_nodes_id_fk
            references nodes
);
create table table_migrations
(
    ballot     integer not null,
    table_id   integer not null
        constraint table_migrations_tables_id_fk
            references tables,
    migrations BLOB    not null,
    constraint table_migrations_pk
        primary key (ballot, table_id)
);
create index table_migrations_table_id_index
    on table_migrations (table_id);
