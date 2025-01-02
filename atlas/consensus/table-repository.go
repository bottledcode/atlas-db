package consensus

import (
	"context"
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

type tableField string

const (
	TableName              tableField = "name"
	TableVersion           tableField = "version"
	TableReplicationLevel  tableField = "replication_level"
	TableAllowedRegions    tableField = "allowed_regions"
	TableRestrictedRegions tableField = "restricted_regions"
	TableOwnerNodeId       tableField = "owner_node_id"
	TableCreatedAt         tableField = "created_at"
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
    version            = :version
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
		)...,
	)
	return err
}

func (r *tableRepository) InsertTable(table *Table) error {
	_, err := atlas.ExecuteSQL(
		r.ctx,
		`
insert into tables (name, owner_node_id, version, restricted_regions, allowed_regions, replication_level, created_at)
values (:name, :owner_node_id, :version, :restricted_regions, :allowed_regions, :replication_level, :created_at)`,
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
       n.rtt                                    as node_rtt
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

	replicationLevel := ReplicationLevel_value[results.GetIndex(0).GetColumn("replication_level").GetString()]

	result := results.GetIndex(0)
	table = &Table{
		Name:              result.GetColumn("name").GetString(),
		ReplicationLevel:  ReplicationLevel(replicationLevel),
		Owner:             nil,
		CreatedAt:         timestamppb.New(result.GetColumn("created_at").GetTime()),
		Version:           result.GetColumn("version").GetInt(),
		AllowedRegions:    getCommaFields(result.GetColumn("allowed_regions").GetString()),
		RestrictedRegions: getCommaFields(result.GetColumn("restricted_regions").GetString()),
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

	return table, nil
}
