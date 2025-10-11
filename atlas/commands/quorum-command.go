package commands

import (
	"context"
	"fmt"
	"strings"

	"github.com/bottledcode/atlas-db/atlas/consensus"
)

type QuorumCommand struct{ CommandString }

func (c *QuorumCommand) GetNext() (Command, error) {
	if p, ok := c.SelectNormalizedCommand(1); ok && p == "INFO" {
		return &QuorumInfoCommand{*c}, nil
	}
	return EmptyCommandString, fmt.Errorf("unknown QUORUM subcommand")
}

type QuorumInfoCommand struct{ QuorumCommand }

func (q *QuorumInfoCommand) GetNext() (Command, error) { return q, nil }

func (q *QuorumInfoCommand) Execute(ctx context.Context) ([]byte, error) {
	if err := q.CheckExactLen(3); err != nil { // QUORUM INFO <table>
		return nil, err
	}
	table, _ := q.SelectNormalizedCommand(2)

	q1, q2, err := consensus.DescribeQuorum(ctx, consensus.KeyName(table))
	if err != nil {
		return nil, err
	}

	var b strings.Builder
	// Q1
	fmt.Fprintf(&b, "Q1 size=%d\n", len(q1))
	for _, n := range q1 {
		fmt.Fprintf(&b, "id=%d region=%s addr=%s port=%d\n", n.GetId(), n.GetRegion().GetName(), n.GetAddress(), n.GetPort())
	}
	// Q2
	fmt.Fprintf(&b, "Q2 size=%d\n", len(q2))
	for _, n := range q2 {
		fmt.Fprintf(&b, "id=%d region=%s addr=%s port=%d\n", n.GetId(), n.GetRegion().GetName(), n.GetAddress(), n.GetPort())
	}

	return []byte(b.String()), nil
}
