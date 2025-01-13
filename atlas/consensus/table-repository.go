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
	// GetTable returns a table by name.
	GetTable(name string) (*Table, error)
	// UpdateTable updates a table.
	UpdateTable(table *Table) error
	// InsertTable inserts a table.
	InsertTable(table *Table) error
	// GetGroup returns a group by name.
	GetGroup(name string) (*TableGroup, error)
	// UpdateGroup updates a group.
	UpdateGroup(group *TableGroup) error
	// InsertGroup inserts a group.
	InsertGroup(group *TableGroup) error
	// GetShard returns a shard of a table, given the principal.
	GetShard(table *Table, principals []*Principal) (*Shard, error)
	// UpdateShard updates a shard metadata.
	UpdateShard(table *Shard) error
	// InsertShard inserts a shard metadata.
	// Ensure principals are set and the shard meta-name will be updated before inserting.
	InsertShard(table *Shard) error
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

func (r *tableRepository) hashPrincipals(principals []*Principal, order []string) (string, error) {
	p := make(map[string]string, len(principals))
	for _, principal := range principals {
		p[principal.GetName()] = principal.GetValue()
	}

	var hashStr strings.Builder

	for _, o := range order {
		if v, ok := p[o]; ok {
			hashStr.WriteString(v)
		} else {
			return "", errors.New("missing principal")
		}
	}

	// hash the string using a simple Larson's hash (very fast for small strings and low collision rate)
	var hash uint32
	str := hashStr.String()
	for _, b := range str {
		hash = hash*101 + uint32(b)
	}

	// return the hash number as a string
	chars := []rune{'Z', 'A', 'C', '2', 'B', '3', 'E', 'F', '4', 'G', 'H', '5', 'T', 'K', '6', '7', 'P', '8', 'R', 'S', '9', 'W', 'X', 'Y'}
	hashStr.Reset()
	for {
		hashStr.WriteRune(chars[hash%24])
		hash /= 24
		if hash == 0 {
			break
		}
	}
	return hashStr.String(), nil
}

func (r *tableRepository) GetShard(table *Table, principals []*Principal) (*Shard, error) {
	hash, err := r.hashPrincipals(principals, table.GetShardPrincipals())
	if err != nil {
		return nil, err
	}
	// retrieve the shard
	shard, err := r.GetTable(table.GetName() + "_" + hash)
	if err != nil {
		return nil, err
	}
	if shard == nil {
		return nil, nil
	}
	return &Shard{
		Table:      table,
		Shard:      shard,
		Principals: principals,
	}, nil
}

func (r *tableRepository) UpdateShard(table *Shard) error {
	return r.UpdateTable(table.GetShard())
}

func (r *tableRepository) InsertShard(table *Shard) error {
	hash, err := r.hashPrincipals(table.GetPrincipals(), table.GetTable().GetShardPrincipals())
	if err != nil {
		return err
	}
	table.GetShard().Name = table.GetTable().GetName() + "_" + hash

	return r.InsertTable(table.GetShard())
}

func (r *tableRepository) GetGroup(name string) (*TableGroup, error) {
	details, err := r.GetTable(name)
	if err != nil {
		return nil, err
	}
	if details == nil {
		return nil, nil
	}
	if details.GetType() != TableType_group {
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
	TableGroupName         tableField = "group_id"
	TableTypeName          tableField = "table_type"
	TableShardPrincipals   tableField = "shard_principals"
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
		case TableTypeName:
			params[i] = atlas.Param{
				Name:  "table_type",
				Value: table.GetType().String(),
			}
		case TableGroupName:
			params[i] = atlas.Param{
				Name:  "group_id",
				Value: table.GetGroup(),
			}
		case TableShardPrincipals:
			params[i] = atlas.Param{
				Name:  "shard_principals",
				Value: strings.Join(table.ShardPrincipals, ","),
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
    table_type         = :table_type,
    shard_principals   = :shard_principals
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
			TableTypeName,
			TableShardPrincipals,
		)...,
	)
	return err
}

func (r *tableRepository) InsertTable(table *Table) error {
	_, err := atlas.ExecuteSQL(
		r.ctx,
		`
insert into tables (name, owner_node_id, version, restricted_regions, allowed_regions, replication_level, created_at, table_type, group_id, shard_principals)
values (:name, :owner_node_id, :version, :restricted_regions, :allowed_regions, :replication_level, :created_at, :table_type, :group_id, :shard_principals)`,
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
			TableTypeName,
			TableShardPrincipals,
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
       table_type,
       shard_principals
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
		Type:              TableType(TableType_value[result.GetColumn("table_type").GetString()]),
		Group:             groupName,
		ShardPrincipals:   strings.Split(result.GetColumn("shard_principals").GetString(), ","),
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
