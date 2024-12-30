package consensus

import (
	"context"
	"github.com/bottledcode/atlas-db/atlas"
	"github.com/go-faker/faker/v4"
	"github.com/stretchr/testify/assert"
	"google.golang.org/protobuf/types/known/timestamppb"
	"math/rand"
	"strings"
	"testing"
	"time"
	"zombiezen.com/go/sqlite"
)

func TestCreateRegion(t *testing.T, r *Region, conn *sqlite.Conn) {
	if r.GetRegionId() == 0 {
		r.RegionId = rand.Int63()
	}

	if r.GetRegionName() == "" {
		r.RegionName = faker.Word()
	}

	_, err := atlas.ExecuteSQL(
		context.Background(),
		"insert into regions values (:id, :name)",
		conn,
		false,
		atlas.Param{
			Name:  "id",
			Value: r.GetRegionId(),
		}, atlas.Param{
			Name:  "name",
			Value: r.GetRegionName(),
		},
	)
	assert.NoError(t, err)
}

func TestCreateNode(t *testing.T, n *Node, conn *sqlite.Conn) {
	if n.GetNodeId() == 0 {
		n.NodeId = rand.Int63()
	}

	regionId := int64(0)

	if n.GetNodeRegion() == "" {
		r := &Region{}
		TestCreateRegion(t, r, conn)
		n.NodeRegion = r.GetRegionName()
	}

	regionId, err := atlas.GetRegionIdFromName(context.Background(), conn, n.GetNodeRegion())
	assert.NoError(t, err)

	if n.GetNodePort() == 0 {
		n.NodePort = rand.Int63n(65535)
	}

	if n.GetNodeAddress() == "" {
		n.NodeAddress = faker.IPv4()
	}

	_, err = atlas.ExecuteSQL(
		context.Background(),
		"insert into nodes values (:id, :address, :port, :region_id, :active, current_timestamp, current_timestamp)",
		conn,
		false,
		atlas.Param{
			Name:  "id",
			Value: n.GetNodeId(),
		}, atlas.Param{
			Name:  "address",
			Value: n.GetNodeAddress(),
		}, atlas.Param{
			Name:  "port",
			Value: n.GetNodePort(),
		}, atlas.Param{
			Name:  "region_id",
			Value: regionId,
		}, atlas.Param{
			Name:  "active",
			Value: 1,
		})
	assert.NoError(t, err)
}

func TestCreateTableEntry(t *testing.T, table *Table, conn *sqlite.Conn) {
	if table.GetId() == 0 {
		table.Id = rand.Int63()
	}

	if table.GetName() == "" {
		table.Name = faker.Word()
	}

	assert.NotZero(t, table.GetVersion())

	if table.GetCreatedAt() == nil {
		tt, err := time.Parse(time.DateTime, faker.Timestamp())
		assert.NoError(t, err)
		table.CreatedAt = timestamppb.New(tt)
	}

	_, err := atlas.ExecuteSQL(
		context.Background(),
		"insert into tables (id, table_name, replication_level, owner_node_id, version, allowed_regions) VALUES (:id, :table_name, :replication_level, :owner_node_id, :version, :allowed_regions)",
		conn,
		false,
		atlas.Param{
			Name:  "id",
			Value: table.GetId(),
		}, atlas.Param{
			Name:  "table_name",
			Value: table.GetName(),
		}, atlas.Param{
			Name:  "replication_level",
			Value: table.GetReplicationLevel().String(),
		}, atlas.Param{
			Name:  "owner_node_id",
			Value: table.GetGlobalOwner().GetNodeId(),
		}, atlas.Param{
			Name:  "version",
			Value: table.GetVersion(),
		}, atlas.Param{
			Name:  "allowed_regions",
			Value: strings.Join(table.GetAllowedRegions(), ","),
		},
	)
	assert.NoError(t, err)
}
