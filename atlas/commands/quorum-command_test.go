package commands

import "testing"

func TestQuorum_Parse_Info_And_Unknown(t *testing.T) {
	// INFO parses to QuorumInfoCommand
	cs := CommandFromString("QUORUM INFO mytable")
	cmd, err := cs.GetNext()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if _, ok := cmd.(*QuorumInfoCommand); !ok {
		t.Fatalf("expected *QuorumInfoCommand, got %T", cmd)
	}

	// Unknown subcommand returns an error
	cs2 := CommandFromString("QUORUM WHAT mytable")
	_, err = cs2.GetNext()
	if err == nil {
		t.Fatalf("expected error for unknown QUORUM subcommand")
	}
}
