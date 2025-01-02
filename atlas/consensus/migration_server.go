package consensus

import (
	"errors"
	"github.com/bottledcode/atlas-db/atlas"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
	"strings"
)

func (t *Table) FromSqlRow(row *atlas.Row) error {
	switch row.GetColumn("replication_level").GetString() {
	case "global":
		t.ReplicationLevel = ReplicationLevel_global
	case "regional":
		t.ReplicationLevel = ReplicationLevel_regional
	case "local":
		t.ReplicationLevel = ReplicationLevel_local
	}

	t.Name = row.GetColumn("name").GetString()
	t.Owner.Id = row.GetColumn("owner_node_id").GetInt()
	t.CreatedAt = timestamppb.New(row.GetColumn("created_at").GetTime())
	t.Version = row.GetColumn("version").GetInt()

	t.AllowedRegions = strings.FieldsFunc(row.GetColumn("allowed_regions").GetString(), func(r rune) bool {
		return r == ','
	})
	t.RestrictedRegions = strings.FieldsFunc(row.GetColumn("restricted_regions").GetString(), func(r rune) bool {
		return r == ','
	})

	return nil
}

func (n *Node) FromSqlRow(row *atlas.Row) {
	n.Id = row.GetColumn("id").GetInt()
	n.Address = row.GetColumn("address").GetString()
	n.Port = row.GetColumn("port").GetInt()
	n.Region = &Region{Name: row.GetColumn("region").GetString()}
	n.Active = row.GetColumn("active").GetBool()
	n.Rtt = durationpb.New(row.GetColumn("rtt").GetDuration())
}

var ErrCannotChangeReplicationLevel = errors.New("cannot change replication level of a table")
var ErrTablePolicyViolation = errors.New("table policy violation")
