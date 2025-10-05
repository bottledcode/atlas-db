package commands

import (
	"context"
	"testing"
	"time"
)

func TestKeyPut_Parse_SimpleValue(t *testing.T) {
	cmd := CommandFromString("KEY PUT my.key worm")
	next, err := cmd.GetNext()
	if err != nil {
		t.Fatalf("unexpected error getting next: %v", err)
	}
	kpc, ok := next.(*KeyPutCommand)
	if !ok {
		t.Fatalf("expected *KeyPutCommand, got %T", next)
	}
	parsed, err := kpc.Parse()
	if err != nil {
		t.Fatalf("unexpected parse error: %v", err)
	}
	if string(parsed.Value) != "worm" {
		t.Fatalf("expected value 'worm', got %q", string(parsed.Value))
	}
	if parsed.WormTTL != nil || parsed.WormExp != nil {
		t.Fatalf("did not expect WORM attributes to be set for bare 'worm' value")
	}
}

func TestKeyPut_Parse_WormTTL_ThenValue(t *testing.T) {
	cmd := CommandFromString("KEY PUT my.key WORM TTL 1h hello world")
	next, err := cmd.GetNext()
	if err != nil {
		t.Fatalf("unexpected error getting next: %v", err)
	}
	kpc, ok := next.(*KeyPutCommand)
	if !ok {
		t.Fatalf("expected *KeyPutCommand, got %T", next)
	}
	parsed, err := kpc.Parse()
	if err != nil {
		t.Fatalf("unexpected parse error: %v", err)
	}
	if parsed.WormTTL == nil {
		t.Fatalf("expected WORM TTL to be set")
	}
	if d := *parsed.WormTTL; d != time.Hour {
		t.Fatalf("expected TTL 1h, got %v", d)
	}
	if string(parsed.Value) != "hello world" {
		t.Fatalf("expected value 'hello world', got %q", string(parsed.Value))
	}
}

func TestKeyPut_Parse_Principal_ThenValue(t *testing.T) {
	cmd := CommandFromString("KEY PUT my.key PRINCIPAL bob 1234")
	next, err := cmd.GetNext()
	if err != nil {
		t.Fatalf("unexpected error getting next: %v", err)
	}
	kpc, ok := next.(*KeyPutCommand)
	if !ok {
		t.Fatalf("expected *KeyPutCommand, got %T", next)
	}
	parsed, err := kpc.Parse()
	if err != nil {
		t.Fatalf("unexpected parse error: %v", err)
	}
	if parsed.Principal != "bob" {
		t.Fatalf("expected principal 'bob', got %q", parsed.Principal)
	}
	if string(parsed.Value) != "1234" {
		t.Fatalf("expected value '1234', got %q", string(parsed.Value))
	}
}

func TestKeyGet_FromKey_Mapping(t *testing.T) {
	cmd := CommandFromString("KEY GET table.row")
	next, err := cmd.GetNext()
	if err != nil {
		t.Fatalf("unexpected error getting next: %v", err)
	}
	kgc, ok := next.(*KeyGetCommand)
	if !ok {
		t.Fatalf("expected *KeyGetCommand, got %T", next)
	}
	// Normalized() uppercases tokens, so SelectNormalizedCommand(2) yields "TABLE.ROW"
	key, _ := kgc.SelectNormalizedCommand(2)
	builder := kgc.FromKey(key)
	if got := builder.String(); got != "t:TABLE:r:ROW" {
		t.Fatalf("unexpected key mapping, got %q", got)
	}
}

func TestKeyGet_FromKey_Mapping_MultiPart(t *testing.T) {
	cmd := CommandFromString("KEY GET table.row.attr.more")
	next, err := cmd.GetNext()
	if err != nil {
		t.Fatalf("unexpected error getting next: %v", err)
	}
	kgc, ok := next.(*KeyGetCommand)
	if !ok {
		t.Fatalf("expected *KeyGetCommand, got %T", next)
	}
	key, _ := kgc.SelectNormalizedCommand(2)
	builder := kgc.FromKey(key)
	if got := builder.String(); got != "t:TABLE:r:ROW:ATTR.MORE" {
		t.Fatalf("unexpected key mapping, got %q", got)
	}
}

func TestKeyDel_FromKey_Mapping(t *testing.T) {
	cmd := CommandFromString("KEY DEL table.row.attr.more")
	next, err := cmd.GetNext()
	if err != nil {
		t.Fatalf("unexpected error getting next: %v", err)
	}
	kd, ok := next.(*KeyDelCommand)
	if !ok {
		t.Fatalf("expected *KeyDelCommand, got %T", next)
	}
	key, _ := kd.SelectNormalizedCommand(2)
	builder := kd.FromKey(key)
	if got := builder.String(); got != "t:TABLE:r:ROW:ATTR.MORE" {
		t.Fatalf("unexpected key mapping, got %q", got)
	}
}

func TestScan_NotImplemented(t *testing.T) {
	cmd := CommandFromString("SCAN prefix")
	next, err := cmd.GetNext()
	if err != nil {
		t.Fatalf("unexpected error getting next: %v", err)
	}
	sc, ok := next.(*ScanCommand)
	if !ok {
		t.Fatalf("expected *ScanCommand, got %T", next)
	}
	if _, err := sc.Execute(context.Background()); err == nil {
		t.Fatalf("expected not implemented error for SCAN")
	}
}

func TestCount_NotImplemented(t *testing.T) {
	cmd := CommandFromString("COUNT prefix")
	next, err := cmd.GetNext()
	if err != nil {
		t.Fatalf("unexpected error getting next: %v", err)
	}
	cc, ok := next.(*CountCommand)
	if !ok {
		t.Fatalf("expected *CountCommand, got %T", next)
	}
	if _, err := cc.Execute(context.Background()); err == nil {
		t.Fatalf("expected not implemented error for COUNT")
	}
}

func TestSample_NotImplemented(t *testing.T) {
	cmd := CommandFromString("SAMPLE prefix N 5")
	next, err := cmd.GetNext()
	if err != nil {
		t.Fatalf("unexpected error getting next: %v", err)
	}
	sc, ok := next.(*SampleCommand)
	if !ok {
		t.Fatalf("expected *SampleCommand, got %T", next)
	}
	if _, err := sc.Execute(context.Background()); err == nil {
		t.Fatalf("expected not implemented error for SAMPLE")
	}
}

// Basic test to ensure the fix compiles and command parsing still works
func TestKeyGet_ParseCommandStructure(t *testing.T) {
	cmd := CommandFromString("KEY GET test.key")
	next, err := cmd.GetNext()
	if err != nil {
		t.Fatalf("unexpected error getting next: %v", err)
	}
	kgc, ok := next.(*KeyGetCommand)
	if !ok {
		t.Fatalf("expected *KeyGetCommand, got %T", next)
	}

	// Ensure command structure is intact after our changes
	if err := kgc.CheckMinLen(3); err != nil {
		t.Fatalf("command should have at least 3 tokens: %v", err)
	}

	key, _ := kgc.SelectNormalizedCommand(2)
	if key != "TEST.KEY" {
		t.Fatalf("expected normalized key 'TEST.KEY', got %q", key)
	}
}
