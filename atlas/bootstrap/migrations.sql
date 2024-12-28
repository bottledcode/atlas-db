create table tables
(
    id         integer not null primary key autoincrement,
    table_name text    not null,
    mode       text    not null default 'global'
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
    id integer not null primary key autoincrement,
    table_id integer not null
        constraint table_migrations_tables_id_fk references tables,
    command text,
    patch blob,
    snapshot blob
);
