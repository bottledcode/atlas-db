create table nodes
(
    id         integer not null primary key,
    address    text    not null,
    port       int     not null,
    region     text    not null,
    active     int     not null,
    created_at timestamp default CURRENT_TIMESTAMP,
    rtt        int     not null
);
create table tables
(
    name               text not null primary key,
    replication_level  text check (replication_level in ('local', 'regional', 'global')),
    owner_node_id      integer
        constraint tables_nodes_id_fk
            references nodes,
    created_at         timestamp     default CURRENT_TIMESTAMP,
    version            int  not null default 0,
    allowed_regions    text not null default '',
    restricted_regions text not null default ''
);
insert into tables
values ('atlas.nodes', 'global', null, current_timestamp, 0, '', '');
create table migrations
(
    table_id   text    not null
        constraint migrations_tables_id_fk
            references tables,
    version    int not null,
    batch_part int not null,
    by_node_id int not null,
    command    text             default null, -- command to run on the user table
    data       blob             default null, -- data to apply to the user table
    committed  int     not null default 0,
    primary key (table_id, version, batch_part, by_node_id)
);
create table own
(
    table_id text not null
        constraint own_tables_id_fk
            references tables
);
