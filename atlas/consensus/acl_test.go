package consensus

import (
	"context"
	"testing"

	"google.golang.org/grpc/metadata"
)

func TestEncodeDecodeOwner(t *testing.T) {
	owner := "alice"
	enc := encodeOwner(owner)
	dec, ok := decodeOwner(enc)
	if !ok {
		t.Fatalf("decodeOwner returned !ok")
	}
	if dec != owner {
		t.Fatalf("expected %q, got %q", owner, dec)
	}
}

func TestGetPrincipalFromContext(t *testing.T) {
	// Incoming metadata
	mdIn := metadata.Pairs("Atlas-Principal", "carol")
	ctx := metadata.NewIncomingContext(context.Background(), mdIn)
	if p := getPrincipalFromContext(ctx); p != "carol" {
		t.Fatalf("expected carol from incoming ctx, got %q", p)
	}

	// Outgoing metadata
	mdOut := metadata.Pairs("Atlas-Principal", "dave")
	ctx2 := metadata.NewOutgoingContext(context.Background(), mdOut)
	if p := getPrincipalFromContext(ctx2); p != "dave" {
		t.Fatalf("expected dave from outgoing ctx, got %q", p)
	}
}
