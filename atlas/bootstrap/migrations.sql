create table regions
(
    id   integer not null primary key autoincrement,
    name text    not null unique
);
create index regions_name_uindex
    on regions (name);
create table nodes
(
    id          integer not null primary key autoincrement,
    address     text    not null,
    port        int     not null,
    region_id   int     not null
        constraint nodes_regions_id_fk references regions,
    active      int     not null,
    create_at   timestamp default CURRENT_TIMESTAMP,
    last_active timestamp default CURRENT_TIMESTAMP
);
create table tables
(
    id                integer not null primary key autoincrement,
    table_name        text    not null unique,
    replication_level text check (replication_level in ('local', 'regional', 'global')),
    owner_node_id     integer -- for global replication
        constraint tables_nodes_id_fk
            references nodes,
    created_at        timestamp        default CURRENT_TIMESTAMP,
    version           int     not null default 0
);
create table leadership
(
    table_id     integer not null
        constraint leadership_tables_id_fk
            references tables,
    node_id      integer not null
        constraint leadership_nodes_id_fk
            references nodes,
    region_id    integer not null
        constraint leadership_regions_id_fk
            references regions,
    last_updated timestamp default CURRENT_TIMESTAMP,
    primary key (table_id, node_id, region_id)
);
create table schema_migrations
(
    table_id   integer not null,
    version    integer not null,
    command    text    not null,
    applied_at timestamp default CURRENT_TIMESTAMP,
    primary key (table_id, version)
);
create table data_migrations
(
    table_id integer not null,
    version  integer not null,
    data     blob    not null,
    primary key (table_id, version)
);
