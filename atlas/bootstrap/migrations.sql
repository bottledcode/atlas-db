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
    owner_node_id      integer,
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
