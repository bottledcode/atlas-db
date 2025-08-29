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
	"fmt"
	"strconv"

	"github.com/bottledcode/atlas-db/atlas"
	"google.golang.org/protobuf/types/known/durationpb"
	"zombiezen.com/go/sqlite"
)

type NodeRepository interface {
	GetNodeById(id int64) (*Node, error)
	GetNodeByAddress(address string, port uint) (*Node, error)
	GetNodesByRegion(region string) ([]*Node, error)
	GetRegions() ([]*Region, error)
	Iterate(fn func(*Node) error) error
	TotalCount() (int64, error)
	GetRandomNodes(num int64, excluding ...int64) ([]*Node, error)
}

func GetDefaultNodeRepository(ctx context.Context, conn *sqlite.Conn) NodeRepository {
	return &nodeRepository{
		ctx:  ctx,
		conn: conn,
	}
}

type nodeRepository struct {
	ctx  context.Context
	conn *sqlite.Conn
}

func (n *nodeRepository) GetRandomNodes(num int64, excluding ...int64) ([]*Node, error) {
	query := "select id, address, region, port, active, rtt from nodes where active = 1"
	if len(excluding) > 0 {
		query += " and id not in ("
		for i, _ := range excluding {
			if i > 0 {
				query += ", "
			}
			query += ":id" + strconv.Itoa(i)
		}
		query += ")"
	}
	query += " order by random() limit :num"

	params := make([]atlas.Param, len(excluding)+1)
	for i, id := range excluding {
		params[i] = atlas.Param{
			Name:  "id" + fmt.Sprint(i),
			Value: id,
		}
	}
	params[len(excluding)] = atlas.Param{
		Name:  "num",
		Value: num,
	}

	results, err := atlas.ExecuteSQL(n.ctx, query, n.conn, false, params...)
	if err != nil {
		return nil, err
	}

	nodes := make([]*Node, len(results.Rows))
	for i, row := range results.Rows {
		nodes[i] = n.convertRowToNode(&row)
	}

	return nodes, nil
}

func (n *nodeRepository) Iterate(fn func(*Node) error) error {
	// todo: use optimal stepper
	results, err := atlas.ExecuteSQL(n.ctx, "select id, address, region, port, active, rtt from nodes where active = 1 order by id", n.conn, false)
	if err != nil {
		return err
	}
	for _, row := range results.Rows {
		if err := fn(n.convertRowToNode(&row)); err != nil {
			return err
		}
	}
	return nil
}

func (n *nodeRepository) TotalCount() (int64, error) {
	results, err := atlas.ExecuteSQL(n.ctx, "select count(*) as count from nodes where active = 1", n.conn, false)
	if err != nil {
		return 0, err
	}
	return results.GetIndex(0).GetColumn("count").GetInt(), nil
}

func (n *nodeRepository) GetRegions() ([]*Region, error) {
	results, err := atlas.ExecuteSQL(n.ctx, "select distinct region from nodes where active = 1", n.conn, false)
	if err != nil {
		return nil, err
	}

	regions := make([]*Region, len(results.Rows))
	for i, row := range results.Rows {
		regions[i] = &Region{
			Name: row.GetColumn("region").GetString(),
		}
	}

	return regions, nil
}

func (n *nodeRepository) GetNodesByRegion(region string) ([]*Node, error) {
	results, err := atlas.ExecuteSQL(n.ctx, "select id, address, region, port, active, rtt from nodes where region = :region and active = 1 order by id", n.conn, false, atlas.Param{
		Name:  "region",
		Value: region,
	})
	if err != nil {
		return nil, err
	}

	nodes := make([]*Node, len(results.Rows))
	for i, row := range results.Rows {
		nodes[i] = n.convertRowToNode(&row)
	}

	return nodes, nil
}

func (n *nodeRepository) convertRowToNode(row *atlas.Row) *Node {
	return &Node{
		Id:      row.GetColumn("id").GetInt(),
		Address: row.GetColumn("address").GetString(),
		Region: &Region{
			Name: row.GetColumn("region").GetString(),
		},
		Port:   row.GetColumn("port").GetInt(),
		Active: row.GetColumn("active").GetBool(),
		Rtt:    durationpb.New(row.GetColumn("rtt").GetDuration()),
	}
}

func (n *nodeRepository) GetNodeById(id int64) (*Node, error) {
	results, err := atlas.ExecuteSQL(n.ctx, "select id, address, region, port, active, rtt from nodes where id = :id", n.conn, false, atlas.Param{
		Name:  "id",
		Value: id,
	})
	if err != nil {
		return nil, err
	}

	if results.Empty() {
		return nil, nil
	}

	return n.convertRowToNode(results.GetIndex(0)), nil
}

func (n *nodeRepository) GetNodeByAddress(address string, port uint) (*Node, error) {
	results, err := atlas.ExecuteSQL(n.ctx, "select id, address, port, region, active, rtt from nodes where address = :address and port = :port", n.conn, false, atlas.Param{
		Name:  "address",
		Value: address,
	}, atlas.Param{
		Name:  "port",
		Value: port,
	})
	if err != nil {
		return nil, err
	}

	if results.Empty() {
		return nil, nil
	}

	if results.NonSingle() {
		return nil, fmt.Errorf("expected single node, got %d", len(results.Rows))
	}

	return n.convertRowToNode(results.GetIndex(0)), nil
}
