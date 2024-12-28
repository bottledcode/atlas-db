package consensus

import (
	"context"
	"errors"
	"fmt"
	"github.com/bottledcode/atlas-db/atlas"
)

type Server struct {
	UnimplementedConsensusServer
}

var ErrInvalidTopologyChange = errors.New("invalid topology change")

func (s *Server) ProposeTopologyChange(ctx context.Context, request *ProposeTopologyChangeRequest) (*PromiseTopologyChange, error) {
	switch m := request.GetChange().(type) {
	case *ProposeTopologyChangeRequest_NodeChange:
		return s.nodeProposal(ctx, m.NodeChange)
	case *ProposeTopologyChangeRequest_RegionChange:
		panic("implement me")
	}
	return nil, ErrInvalidTopologyChange
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
	if original.GetColumn("address").GetString() == "placeholder" {
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

	_, err = atlas.ExecuteSQL(ctx, "insert into nodes values (:id, 'placeholder', 1234, :region)", conn, false, atlas.Param{
		Name:  "id",
		Value: node.GetNodeId(),
	}, atlas.Param{
		Name:  "region",
		Value: regionId,
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

		if actualNode.NodeAddress == "placeholder" {
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

func (s *Server) AcceptTopologyChange(ctx context.Context, node *Node) (*Node, error) {
	//TODO implement me
	panic("implement me")
}
