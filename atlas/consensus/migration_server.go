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
	"errors"
	"strings"

	"github.com/bottledcode/atlas-db/atlas"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
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
