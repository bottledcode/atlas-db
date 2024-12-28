create table atlas.tables
(
    id         integer not null primary key autoincrement,
    table_name text    not null,
    mode       text    not null default 'global'
);
create table atlas.regions
(
    id   integer not null primary key autoincrement,
    name text    not null
);
create table atlas.nodes
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
