package commands

import (
	"testing"
)

func TestNode_Parse_List(t *testing.T) {
	cmd := CommandFromString("NODE LIST")
	next, err := cmd.GetNext()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if _, ok := next.(*NodeListCommand); !ok {
		t.Fatalf("expected *NodeListCommand, got %T", next)
	}
}

func TestNode_Parse_Info(t *testing.T) {
	cmd := CommandFromString("NODE INFO 42")
	next, err := cmd.GetNext()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if _, ok := next.(*NodeInfoCommand); !ok {
		t.Fatalf("expected *NodeInfoCommand, got %T", next)
	}
}

func TestNode_Ping_Parses(t *testing.T) {
	cmd := CommandFromString("NODE PING 1")
	next, err := cmd.GetNext()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if _, ok := next.(*NodePingCommand); !ok {
		t.Fatalf("expected *NodePingCommand, got %T", next)
	}
}

func TestQuorum_Info_Parses(t *testing.T) {
	cmd := CommandFromString("QUORUM INFO mytable")
	next, err := cmd.GetNext()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if _, ok := next.(*QuorumInfoCommand); !ok {
		t.Fatalf("expected *QuorumInfoCommand, got %T", next)
	}
}
