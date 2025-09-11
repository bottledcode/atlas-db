package commands

import "testing"

func TestSelectNormalizedCommand_NegativeIndex(t *testing.T) {
	cs := CommandFromString("KEY GET user.profile.name")
	// Normalized parts: ["KEY", "GET", "USER.PROFILE.NAME"]
	last, ok := cs.SelectNormalizedCommand(-1)
	if !ok || last != "USER.PROFILE.NAME" {
		t.Fatalf("expected last token USER.PROFILE.NAME, got %q ok=%v", last, ok)
	}
	prev, ok := cs.SelectNormalizedCommand(-2)
	if !ok || prev != "GET" {
		t.Fatalf("expected token GET, got %q ok=%v", prev, ok)
	}
}

func TestRemoveAfter_TooLarge_ReturnsEmpty(t *testing.T) {
	cs := CommandFromString("SELECT * FROM table")
	out := cs.RemoveAfter(10)
	if out == nil {
		t.Fatalf("expected non-nil command")
	}
	if out.Raw() != "" {
		t.Fatalf("expected empty Raw, got %q", out.Raw())
	}
}
