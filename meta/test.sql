attach database 'd:\atlas.meta' as atlas;

create table atlas.migrations
(
    id       integer       not null primary key,
    command  TEXT          not null,
    executed INT default 0 not null
);
create table atlas.tables
(
    id          integer not null primary key,
    table_name  text    not null,
    is_local    int     not null default 0,
    is_regional int     not null default 0
);
create table atlas.regions
(
    id   integer not null primary key,
    name text    not null
);
create table atlas.nodes
(
    id        integer not null primary key,
    address   text    not null,
    port      int     not null,
    region_id int     not null
        constraint nodes_regions_id_fk references regions
);
create table table_nodes
(
    id       integer not null
        constraint table_nodes_pk
            primary key,
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
    id           integer not null primary key,
    table_id     integer not null
        constraint table_migrations_tables_id_fk references tables,
    migration_id integer not null
        constraint table_migrations_migrations_id_fk references migrations
);
create index table_migrations_table_id_index on table_migrations (table_id);

-- drop all tables
drop table atlas.migrations;
drop table atlas.tables;
drop table atlas.regions;
drop table atlas.nodes;
drop table table_nodes;
drop table table_migrations;

insert into regions
values (1, 'eu');
insert into nodes
values (1, 'localhost', 1234, 1);
insert into tables
values (1, 'users', 0, 0);
insert into table_nodes
values (1, 1, 1, 1);
insert into nodes
values (2, 'otherhost', 1234, 1);
insert into table_nodes
values (2, 0, 1, 2);

-- see all table information
select *
from tables
         inner join table_nodes tn on tables.id = tn.table_id
         inner join nodes n on tn.node_id = n.id
where table_name = 'users';
