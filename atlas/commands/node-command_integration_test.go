//go:build integration

package commands

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/bottledcode/atlas-db/atlas/consensus"
	"github.com/bottledcode/atlas-db/atlas/kv"
	"github.com/bottledcode/atlas-db/atlas/options"
	"go.uber.org/zap/zaptest"
	"google.golang.org/protobuf/types/known/durationpb"
)

func setupTempPool(t *testing.T) func() {
	t.Helper()
	dir := t.TempDir()
	data := filepath.Join(dir, "data")
	meta := filepath.Join(dir, "meta")
	if err := os.MkdirAll(data, 0o755); err != nil {
		t.Fatalf("mkdir data: %v", err)
	}
	if err := os.MkdirAll(meta, 0o755); err != nil {
		t.Fatalf("mkdir meta: %v", err)
	}
	// Initialize global pool for commands to find
	if err := kv.CreatePool(data, meta); err != nil {
		t.Fatalf("create pool: %v", err)
	}
	return func() { _ = kv.DrainPool() }
}

func TestNode_List_And_Info(t *testing.T) {
	cleanup := setupTempPool(t)
	defer cleanup()
	ctx := context.Background()
	options.Logger = zaptest.NewLogger(t)

	// Insert a couple of nodes into meta via repository
	repo := consensus.NewNodeRepository(ctx, kv.GetPool().MetaStore())
	n1 := &consensus.Node{Id: 1, Address: "127.0.0.1", Port: 1111, Region: &consensus.Region{Name: "us-east-1"}, Active: true, Rtt: durationpb.New(0)}
	n2 := &consensus.Node{Id: 2, Address: "127.0.0.2", Port: 2222, Region: &consensus.Region{Name: "us-west-2"}, Active: true, Rtt: durationpb.New(0)}
	if err := repo.AddNode(n1); err != nil {
		t.Fatalf("add node1: %v", err)
	}
	if err := repo.AddNode(n2); err != nil {
		t.Fatalf("add node2: %v", err)
	}

	// NODE LIST
	listCmd := CommandFromString("NODE LIST")
	next, err := listCmd.GetNext()
	if err != nil {
		t.Fatalf("get next: %v", err)
	}
	nlc, ok := next.(*NodeListCommand)
	if !ok {
		t.Fatalf("expected *NodeListCommand, got %T", next)
	}
	out, err := nlc.Execute(ctx)
	if err != nil {
		t.Fatalf("list execute: %v", err)
	}
	s := string(out)
	if !strings.Contains(s, "id=1") || !strings.Contains(s, "id=2") {
		t.Fatalf("expected both nodes in output, got: %s", s)
	}

	// NODE INFO 1
	infoCmd := CommandFromString("NODE INFO 1")
	next, err = infoCmd.GetNext()
	if err != nil {
		t.Fatalf("get next: %v", err)
	}
	nic, ok := next.(*NodeInfoCommand)
	if !ok {
		t.Fatalf("expected *NodeInfoCommand, got %T", next)
	}
	out, err = nic.Execute(ctx)
	if err != nil {
		t.Fatalf("info execute: %v", err)
	}
	if !strings.Contains(string(out), "id=1") {
		t.Fatalf("expected node id=1 in info output, got: %s", string(out))
	}
}

func TestQuorum_Info_Execution(t *testing.T) {
	cleanup := setupTempPool(t)
	defer cleanup()
	ctx := context.Background()
	options.Logger = zaptest.NewLogger(t)

	// Create nodes across regions and add them to both repo and quorum manager
	repo := consensus.NewNodeRepository(ctx, kv.GetPool().MetaStore())
	qm := consensus.GetDefaultQuorumManager(ctx)
	nodes := []*consensus.Node{
		{Id: 1, Address: "127.0.0.1", Port: 1111, Region: &consensus.Region{Name: "us-east-1"}, Active: true, Rtt: durationpb.New(0)},
		{Id: 2, Address: "127.0.0.2", Port: 1112, Region: &consensus.Region{Name: "us-east-1"}, Active: true, Rtt: durationpb.New(0)},
		{Id: 3, Address: "127.0.0.3", Port: 2111, Region: &consensus.Region{Name: "us-west-2"}, Active: true, Rtt: durationpb.New(0)},
		{Id: 4, Address: "127.0.0.4", Port: 2112, Region: &consensus.Region{Name: "us-west-2"}, Active: true, Rtt: durationpb.New(0)},
		{Id: 5, Address: "127.0.0.5", Port: 3111, Region: &consensus.Region{Name: "eu-west-1"}, Active: true, Rtt: durationpb.New(0)},
	}
	for _, n := range nodes {
		if err := repo.AddNode(n); err != nil {
			t.Fatalf("add node %d: %v", n.Id, err)
		}
		_ = qm.AddNode(ctx, n)
	}

	// Execute QUORUM INFO
	cmd := CommandFromString("QUORUM INFO mytable")
	next, err := cmd.GetNext()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	qic, ok := next.(*QuorumInfoCommand)
	if !ok {
		t.Fatalf("expected *QuorumInfoCommand, got %T", next)
	}
	out, err := qic.Execute(ctx)
	if err != nil {
		t.Fatalf("execute quorum info: %v", err)
	}
	s := string(out)
	if !strings.Contains(s, "Q1 size=") || !strings.Contains(s, "Q2 size=") {
		t.Fatalf("expected Q1/Q2 in output, got: %s", s)
	}
}
