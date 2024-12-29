package consensus

import (
	"context"
	"errors"
	"fmt"
	"github.com/bottledcode/atlas-db/atlas"
	"go.uber.org/zap"
)

type Server struct {
	UnimplementedConsensusServer
}

var ErrInvalidTopologyChange = errors.New("invalid topology change")

const PlaceholderName = "--PLACEHOLDER--"

func (s *Server) ProposeTopologyChange(ctx context.Context, request *ProposeTopologyChangeRequest) (*PromiseTopologyChange, error) {
	switch m := request.GetChange().(type) {
	case *ProposeTopologyChangeRequest_NodeChange:
		return s.nodeProposal(ctx, m.NodeChange)
	case *ProposeTopologyChangeRequest_RegionChange:
		return s.regionProposal(ctx, m.RegionChange)
	}
	return nil, ErrInvalidTopologyChange
}

func (s *Server) regionProposal(ctx context.Context, region *ProposeRegionTopologyChange) (*PromiseTopologyChange, error) {
	switch region.GetKind() {
	case TopologyChange_ADD:
		return s.regionAddProposal(ctx, region.GetRegion())
	// note: cannot delete regions
	default:
		return nil, ErrInvalidTopologyChange
	}
}

func (s *Server) regionAddProposal(ctx context.Context, region *Region) (*PromiseTopologyChange, error) {
	conn, err := atlas.MigrationsPool.Take(ctx)
	if err != nil {
		return nil, err
	}
	defer atlas.MigrationsPool.Put(conn)

	_, err = atlas.ExecuteSQL(ctx, "BEGIN IMMEDIATE", conn, false)
	if err != nil {
		return nil, err
	}
	defer func() {
		if err != nil {
			atlas.ExecuteSQL(ctx, "ROLLBACK", conn, false)
		}
	}()

	_, err = atlas.ExecuteSQL(ctx, "insert into regions values (:id, :name)", conn, false, atlas.Param{
		Name:  "id",
		Value: region.GetRegionId(),
	}, atlas.Param{
		Name:  "name",
		Value: PlaceholderName,
	})
	if err != nil {
		var results *atlas.Rows
		results, err = atlas.ExecuteSQL(ctx, "select id, name from regions where id = :id", conn, false, atlas.Param{
			Name:  "id",
			Value: region.GetRegionId(),
		})
		if err != nil {
			return nil, err
		}
		first := results.GetIndex(0)
		actualRegion := &Region{
			RegionId:   first.GetColumn("id").GetInt(),
			RegionName: first.GetColumn("name").GetString(),
		}

		if actualRegion.GetRegionName() == PlaceholderName {
			return nil, ErrInvalidTopologyChange
		}

		return &PromiseTopologyChange{
			Promise:  false,
			Response: &PromiseTopologyChange_Region{Region: actualRegion},
		}, nil
	}
	if conn.LastInsertRowID() != region.GetRegionId() {
		return nil, ErrInvalidTopologyChange
	}

	_, err = atlas.ExecuteSQL(ctx, "COMMIT", conn, false)
	if err != nil {
		return nil, err
	}

	return &PromiseTopologyChange{
		Promise:  true,
		Response: &PromiseTopologyChange_Region{Region: region},
	}, nil
}

func (s *Server) nodeProposal(ctx context.Context, node *ProposeNodeTopologyChange) (*PromiseTopologyChange, error) {
	switch node.GetKind() {
	case TopologyChange_ADD:
		return s.nodeAddProposal(ctx, node.GetNode())
	case TopologyChange_REMOVE:
		return s.nodeRemoveProposal(ctx, node.GetNode())
	default:
		return nil, ErrInvalidTopologyChange
	}
}

// nodeRemoveProposal is a helper function to handle the REMOVE node topology change
func (s *Server) nodeRemoveProposal(ctx context.Context, node *Node) (*PromiseTopologyChange, error) {
	conn, err := atlas.MigrationsPool.Take(ctx)
	if err != nil {
		return nil, err
	}
	defer atlas.MigrationsPool.Put(conn)

	_, err = atlas.ExecuteSQL(ctx, "BEGIN IMMEDIATE", conn, false)
	if err != nil {
		return nil, err
	}
	defer func() {
		atlas.ExecuteSQL(ctx, "ROLLBACK", conn, false)
	}()

	regionId, err := atlas.GetRegionIdFromName(ctx, conn, node.GetNodeRegion())
	if err != nil {
		return nil, err
	}
	if regionId == 0 {
		return nil, fmt.Errorf("region %s not found: %w", node.GetNodeRegion(), ErrInvalidTopologyChange)
	}

	results, err := atlas.ExecuteSQL(ctx, "select id, address, region_id, port from nodes where id = :id", conn, false, atlas.Param{
		Name:  "id",
		Value: node.GetNodeId(),
	})
	if err != nil {
		return nil, err
	}
	if len(results.Rows) == 0 {
		return &PromiseTopologyChange{
			Promise:  false,
			Response: nil,
		}, nil
	}
	original := results.GetIndex(0)

	// check that the node is an expected change
	if original.GetColumn("address").GetString() == PlaceholderName {
		return nil, ErrInvalidTopologyChange
	}

	former := &Node{
		NodeId:      original.GetColumn("id").GetInt(),
		NodeAddress: original.GetColumn("address").GetString(),
		NodeRegion:  node.GetNodeRegion(),
		NodePort:    original.GetColumn("port").GetInt(),
	}

	if former.GetNodeAddress() == node.GetNodeAddress() && former.GetNodePort() == node.GetNodePort() {
		return &PromiseTopologyChange{
			Promise:  true,
			Response: &PromiseTopologyChange_Node{Node: former},
		}, nil
	}

	return &PromiseTopologyChange{
		Promise:  false,
		Response: &PromiseTopologyChange_Node{Node: former},
	}, nil
}

// nodeAddProposal is a helper function to handle the ADD node topology change
func (s *Server) nodeAddProposal(ctx context.Context, node *Node) (*PromiseTopologyChange, error) {
	conn, err := atlas.MigrationsPool.Take(ctx)
	if err != nil {
		return nil, err
	}
	defer atlas.MigrationsPool.Put(conn)

	_, err = atlas.ExecuteSQL(ctx, "BEGIN IMMEDIATE", conn, false)
	if err != nil {
		return nil, err
	}
	defer func() {
		if err != nil {
			atlas.ExecuteSQL(ctx, "ROLLBACK", conn, false)
		}
	}()

	regionId, err := atlas.GetRegionIdFromName(ctx, conn, node.GetNodeRegion())
	if err != nil {
		return nil, err
	}
	if regionId == 0 {
		return nil, fmt.Errorf("region %s not found: %w", node.GetNodeRegion(), ErrInvalidTopologyChange)
	}

	_, err = atlas.ExecuteSQL(ctx, "insert into nodes values (:id, :name, 0, :region)", conn, false, atlas.Param{
		Name:  "id",
		Value: node.GetNodeId(),
	}, atlas.Param{
		Name:  "region",
		Value: regionId,
	}, atlas.Param{
		Name:  "name",
		Value: PlaceholderName,
	})
	if err != nil {
		var results *atlas.Rows
		results, err = atlas.ExecuteSQL(ctx, "select id, address, region_id, port from nodes where id = :id", conn, false, atlas.Param{
			Name:  "id",
			Value: node.GetNodeId(),
		})
		if err != nil {
			return nil, err
		}
		first := results.GetIndex(0)
		regionName, err := atlas.GetRegionNameFromId(ctx, conn, first.GetColumn("region_id").GetInt())
		if err != nil {
			return nil, err
		}
		actualNode := &Node{
			NodeId:      first.GetColumn("id").GetInt(),
			NodeAddress: first.GetColumn("address").GetString(),
			NodeRegion:  regionName,
			NodePort:    first.GetColumn("port").GetInt(),
		}

		if actualNode.NodeAddress == PlaceholderName {
			return nil, ErrInvalidTopologyChange
		}

		return &PromiseTopologyChange{
			Promise:  false,
			Response: &PromiseTopologyChange_Node{Node: actualNode},
		}, nil
	}

	nodeId := conn.LastInsertRowID()
	if nodeId == 0 {
		return nil, ErrInvalidTopologyChange
	}

	_, err = atlas.ExecuteSQL(ctx, "COMMIT", conn, false)
	if err != nil {
		return nil, err
	}

	return &PromiseTopologyChange{
		Promise: true,
		Response: &PromiseTopologyChange_Node{
			Node: node,
		},
	}, nil
}

// AcceptTopologyChange is a gRPC handler to accept a proposed topology change
func (s *Server) AcceptTopologyChange(ctx context.Context, accept *AcceptTopologyChangeRequest) (*AcceptedTopologyChange, error) {
	conn, err := atlas.MigrationsPool.Take(ctx)
	if err != nil {
		return nil, err
	}
	defer atlas.MigrationsPool.Put(conn)

	_, err = atlas.ExecuteSQL(ctx, "BEGIN IMMEDIATE", conn, false)
	if err != nil {
		return nil, err
	}
	defer func() {
		if err != nil {
			atlas.ExecuteSQL(ctx, "ROLLBACK", conn, false)
		}
	}()

	resp := &AcceptedTopologyChange{}

	switch change := accept.GetChange().(type) {
	case *AcceptTopologyChangeRequest_Node:
		atlas.Logger.Info("☄️ A node has joined the cluster", zap.Int64("node_id", change.Node.GetNodeId()))
		_, err = atlas.ExecuteSQL(ctx, "update nodes set address = :address, port = :port where id = :id", conn, false, atlas.Param{
			Name:  "address",
			Value: change.Node.GetNodeAddress(),
		}, atlas.Param{
			Name:  "port",
			Value: change.Node.GetNodePort(),
		}, atlas.Param{
			Name:  "id",
			Value: change.Node.GetNodeId(),
		})
		if err != nil {
			return nil, err
		}
		_, err = atlas.ExecuteSQL(ctx, "COMMIT", conn, false)
		resp.Response = &AcceptedTopologyChange_Node{Node: change.Node}
	case *AcceptTopologyChangeRequest_Region:
		_, err = atlas.ExecuteSQL(ctx, "update regions set name = :name where id = :id", conn, false, atlas.Param{
			Name:  "name",
			Value: change.Region.GetRegionName(),
		})
		if err != nil {
			return nil, err
		}
		_, err = atlas.ExecuteSQL(ctx, "COMMIT", conn, false)
		resp.Response = &AcceptedTopologyChange_Region{Region: change.Region}
	}
	return resp, err
}
