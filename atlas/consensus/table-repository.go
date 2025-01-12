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

package consensus

import (
	"context"
	"errors"
	"github.com/bottledcode/atlas-db/atlas"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
	"strings"
	"zombiezen.com/go/sqlite"
)

type TableRepository interface {
	GetTable(name string) (*Table, error)
	UpdateTable(table *Table) error
	InsertTable(table *Table) error
	GetGroup(name string) (*TableGroup, error)
	UpdateGroup(group *TableGroup) error
	InsertGroup(group *TableGroup) error
}

func GetDefaultTableRepository(ctx context.Context, conn *sqlite.Conn) TableRepository {
	return &tableRepository{
		ctx:  ctx,
		conn: conn,
	}
}

type tableRepository struct {
	ctx  context.Context
	conn *sqlite.Conn
}

func (r *tableRepository) GetGroup(name string) (*TableGroup, error) {
	details, err := r.GetTable(name)
	if err != nil {
		return nil, err
	}
	if details == nil {
		return nil, nil
	}
	if !details.GetIsGroupMeta() {
		return nil, errors.New("not a group")
	}

	group := &TableGroup{
		Details: details,
	}

	results, err := atlas.ExecuteSQL(r.ctx, `
select * from tables where group_id = :group_id
`, r.conn, false, atlas.Param{
		Name:  "group_id",
		Value: name,
	})
	if err != nil {
		return nil, err
	}
	if results.Empty() {
		return group, nil
	}

	for _, result := range results.Rows {
		table := r.extractTableFromRow(&result)
		if table.GetName() == name {
			// this might be an error, but we can just skip it now.
			continue
		}
		group.Tables = append(group.Tables, table)
	}

	return group, nil
}

func (r *tableRepository) UpdateGroup(group *TableGroup) error {
	return r.UpdateTable(group.Details)
}

func (r *tableRepository) InsertGroup(group *TableGroup) error {
	return r.InsertTable(group.Details)
}

type tableField string

const (
	TableName              tableField = "name"
	TableVersion           tableField = "version"
	TableReplicationLevel  tableField = "replication_level"
	TableAllowedRegions    tableField = "allowed_regions"
	TableRestrictedRegions tableField = "restricted_regions"
	TableOwnerNodeId       tableField = "owner_node_id"
	TableCreatedAt         tableField = "created_at"
	TableIsGroupMeta       tableField = "is_group"
	TableGroupName         tableField = "group_id"
)

func (r *tableRepository) getTableParameters(table *Table, names ...tableField) []atlas.Param {
	params := make([]atlas.Param, len(names))

	for i, name := range names {
		switch name {
		case TableName:
			params[i] = atlas.Param{
				Name:  "name",
				Value: table.Name,
			}
		case TableVersion:
			params[i] = atlas.Param{
				Name:  "version",
				Value: table.Version,
			}
		case TableReplicationLevel:
			params[i] = atlas.Param{
				Name:  "replication_level",
				Value: table.ReplicationLevel.String(),
			}
		case TableAllowedRegions:
			params[i] = atlas.Param{
				Name:  "allowed_regions",
				Value: strings.Join(table.AllowedRegions, ","),
			}
		case TableRestrictedRegions:
			params[i] = atlas.Param{
				Name:  "restricted_regions",
				Value: strings.Join(table.RestrictedRegions, ","),
			}
		case TableOwnerNodeId:
			if table.Owner != nil {
				params[i] = atlas.Param{
					Name:  "owner_node_id",
					Value: table.Owner.Id,
				}
			} else {
				params[i] = atlas.Param{
					Name:  "owner_node_id",
					Value: nil,
				}
			}
		case TableCreatedAt:
			params[i] = atlas.Param{
				Name:  "created_at",
				Value: table.CreatedAt.AsTime(),
			}
		case TableIsGroupMeta:
			params[i] = atlas.Param{
				Name:  "is_group",
				Value: table.GetIsGroupMeta(),
			}
		case TableGroupName:
			params[i] = atlas.Param{
				Name:  "group_id",
				Value: table.GetGroup(),
			}
		default:
			panic("unknown table field")
		}
	}

	return params
}

func (r *tableRepository) UpdateTable(table *Table) error {
	_, err := atlas.ExecuteSQL(
		r.ctx,
		`
update tables
set version            = :version,
    replication_level  = :replication_level,
    allowed_regions    = :allowed_regions,
    restricted_regions = :restricted_regions,
    owner_node_id      = :owner_node_id,
    group_id           = :group_id,
    is_group           = :is_group
where name = :name`,
		r.conn,
		false,
		r.getTableParameters(
			table,
			TableReplicationLevel,
			TableAllowedRegions,
			TableRestrictedRegions,
			TableOwnerNodeId,
			TableName,
			TableVersion,
			TableGroupName,
			TableIsGroupMeta,
		)...,
	)
	return err
}

func (r *tableRepository) InsertTable(table *Table) error {
	_, err := atlas.ExecuteSQL(
		r.ctx,
		`
insert into tables (name, owner_node_id, version, restricted_regions, allowed_regions, replication_level, created_at, is_group, group_id)
values (:name, :owner_node_id, :version, :restricted_regions, :allowed_regions, :replication_level, :created_at, :is_group, :group_id)`,
		r.conn,
		false,
		r.getTableParameters(
			table,
			TableName,
			TableOwnerNodeId,
			TableVersion,
			TableRestrictedRegions,
			TableAllowedRegions,
			TableReplicationLevel,
			TableCreatedAt,
			TableGroupName,
			TableIsGroupMeta,
		)...,
	)
	return err
}

func getCommaFields(s string) []string {
	return strings.FieldsFunc(s, func(r rune) bool {
		return r == ','
	})
}

func (r *tableRepository) GetTable(name string) (*Table, error) {
	var table *Table
	results, err := atlas.ExecuteSQL(r.ctx, `
select name,
       owner_node_id,
       version,
       replication_level,
       allowed_regions,
       restricted_regions,
       t.created_at                             as created_at,
       case when n.id is null then 0 else 1 end as node_exists,
       n.id                                     as node_id,
       n.created_at                             as node_created_at,
       n.address                                as node_address,
       n.port                                   as node_port,
       n.region                                 as node_region,
       n.active                                 as node_active,
       n.rtt                                    as node_rtt,
       group_id,
       is_group
from tables t
         left join nodes n on t.owner_node_id = n.id
where name = :name 
`, r.conn, false, atlas.Param{
		Name:  "name",
		Value: name,
	})
	if err != nil {
		return nil, err
	}

	if results.Empty() {
		return nil, nil
	}

	table = r.extractTableFromRow(results.GetIndex(0))

	return table, nil
}

func (r *tableRepository) extractTableFromRow(result *atlas.Row) *Table {
	replicationLevel := ReplicationLevel_value[result.GetColumn("replication_level").GetString()]
	groupName := ""
	if !result.GetColumn("group_id").IsNull() {
		groupName = result.GetColumn("group_id").GetString()
	}

	table := &Table{
		Name:              result.GetColumn("name").GetString(),
		ReplicationLevel:  ReplicationLevel(replicationLevel),
		Owner:             nil,
		CreatedAt:         timestamppb.New(result.GetColumn("created_at").GetTime()),
		Version:           result.GetColumn("version").GetInt(),
		AllowedRegions:    getCommaFields(result.GetColumn("allowed_regions").GetString()),
		RestrictedRegions: getCommaFields(result.GetColumn("restricted_regions").GetString()),
		IsGroupMeta:       result.GetColumn("is_group").GetBool(),
		Group:             groupName,
	}

	if result.GetColumn("node_exists").GetBool() {
		table.Owner = &Node{
			Id:      result.GetColumn("node_id").GetInt(),
			Address: result.GetColumn("node_address").GetString(),
			Port:    result.GetColumn("node_port").GetInt(),
			Region:  &Region{Name: result.GetColumn("node_region").GetString()},
			Active:  result.GetColumn("node_active").GetBool(),
			Rtt:     durationpb.New(result.GetColumn("node_rtt").GetDuration()),
		}
	}
	return table
}
