package commands

import (
	"bytes"
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/bottledcode/atlas-db/atlas/consensus"
	"github.com/bottledcode/atlas-db/atlas/kv"
	"github.com/bottledcode/atlas-db/atlas/options"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type NodeCommand struct{ CommandString }

func (c *NodeCommand) GetNext() (Command, error) {
	if next, ok := c.SelectNormalizedCommand(1); ok {
		switch next {
		case "LIST":
			return &NodeListCommand{*c}, nil
		case "INFO":
			return &NodeInfoCommand{*c}, nil
		case "PING":
			return &NodePingCommand{*c}, nil
		}
	}
	return EmptyCommandString, nil
}

// NodeListCommand lists active nodes known to the repository
type NodeListCommand struct{ NodeCommand }

func (n *NodeListCommand) GetNext() (Command, error) { return n, nil }
func (n *NodeListCommand) Execute(ctx context.Context) ([]byte, error) {
	if err := n.CheckExactLen(2); err != nil { // NODE LIST
		return nil, err
	}
	pool := kv.GetPool()
	if pool == nil {
		return nil, fmt.Errorf("kv pool not initialized")
	}
	store := pool.MetaStore()
	if store == nil {
		return nil, fmt.Errorf("meta store not available")
	}

	repo := consensus.NewNodeRepository(ctx, store)
	var lines []string
	err := repo.Iterate(func(node *consensus.Node) error {
		lines = append(lines, formatNodeSummary(node))
		return nil
	})
	if err != nil {
		return nil, err
	}
	return []byte(strings.Join(lines, "\n")), nil
}

// NodeInfoCommand shows details for a node
type NodeInfoCommand struct{ NodeCommand }

func (n *NodeInfoCommand) GetNext() (Command, error) { return n, nil }
func (n *NodeInfoCommand) Execute(ctx context.Context) ([]byte, error) {
	if err := n.CheckExactLen(3); err != nil { // NODE INFO <id>
		return nil, err
	}
	idStr, _ := n.SelectNormalizedCommand(2)
	id, err := strconv.ParseInt(idStr, 10, 64)
	if err != nil {
		return nil, fmt.Errorf("invalid node id: %s", idStr)
	}
	pool := kv.GetPool()
	if pool == nil {
		return nil, fmt.Errorf("kv pool not initialized")
	}
	store := pool.MetaStore()
	if store == nil {
		return nil, fmt.Errorf("meta store not available")
	}
	repo := consensus.NewNodeRepository(ctx, store)
	node, err := repo.GetNodeById(id)
	if err != nil {
		return nil, err
	}
	if node == nil {
		return nil, fmt.Errorf("node %d not found", id)
	}
	return []byte(formatNodeDetail(node)), nil
}

// NodePingCommand pings a node and reports RTT
type NodePingCommand struct{ NodeCommand }

func (n *NodePingCommand) GetNext() (Command, error) { return n, nil }
func (n *NodePingCommand) Execute(ctx context.Context) ([]byte, error) {
	if err := n.CheckExactLen(3); err != nil { // NODE PING <id>
		return nil, err
	}
	idStr, _ := n.SelectNormalizedCommand(2)
	id, err := strconv.ParseInt(idStr, 10, 64)
	if err != nil {
		return nil, fmt.Errorf("invalid node id: %s", idStr)
	}

	mgr := consensus.GetNodeConnectionManager(ctx)
	if mgr == nil {
		return nil, fmt.Errorf("node connection manager unavailable")
	}

	// We want to capture RTT; ExecuteOnNode passes only error. We'll calculate RTT inside and encode as response side-effect.
	var resp []byte
	op := func(client consensus.ConsensusClient) error {
		start := time.Now()
		_, err := client.Ping(ctx, &consensus.PingRequest{SenderNodeId: options.CurrentOptions.ServerId, Timestamp: timestamppb.Now()})
		rtt := time.Since(start)
		if err != nil {
			return err
		}
		buf := bytes.NewBuffer(nil)
		fmt.Fprintf(buf, "PONG node=%d rtt_ms=%d", id, rtt.Milliseconds())
		resp = buf.Bytes()
		return nil
	}

	if err := mgr.ExecuteOnNode(id, op); err != nil {
		return nil, err
	}
	if resp == nil {
		// This should not happen, but guard anyway
		return nil, fmt.Errorf("failed to ping node %d", id)
	}
	return resp, nil
}

func formatNodeSummary(n *consensus.Node) string {
	status := "INACTIVE"
	if n.GetActive() {
		status = "ACTIVE"
	}
	rtt := n.GetRtt()
	rttMs := int64(0)
	if rtt != nil {
		rttMs = rtt.AsDuration().Milliseconds()
	}
	return fmt.Sprintf("NODE id=%d region=%s status=%s rtt_ms=%d addr=%s port=%d",
		n.GetId(), n.GetRegion().GetName(), status, rttMs, n.GetAddress(), n.GetPort())
}

func formatNodeDetail(n *consensus.Node) string {
	// For now, include the same summary on one line. Future: multi-line with more fields.
	// Ensure RTT is present even if missing in storage
	if n.GetRtt() == nil {
		n.Rtt = durationpb.New(0)
	}
	return formatNodeSummary(n)
}
