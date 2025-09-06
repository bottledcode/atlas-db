package socket

import (
	"errors"
	"strings"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestFormatCommandError_PermissionDenied_WithPrincipal(t *testing.T) {
	err := status.Error(codes.PermissionDenied, "write access denied")
	code, msg := formatCommandError(err, "alice")
	if code != Warning {
		t.Fatalf("expected Warning, got %v", code)
	}
	if msg == "" || !strings.HasPrefix(msg, "permission denied") {
		t.Fatalf("unexpected message: %q", msg)
	}
	if want := "alice"; msg != "permission denied for principal '"+want+"': write access denied" {
		t.Fatalf("expected principal in message, got %q", msg)
	}
}

func TestFormatCommandError_GeneralError(t *testing.T) {
	err := errors.New("something bad")
	code, msg := formatCommandError(err, "")
	if code != Warning {
		t.Fatalf("expected Warning, got %v", code)
	}
	if msg != "something bad" {
		t.Fatalf("unexpected message: %q", msg)
	}
}
