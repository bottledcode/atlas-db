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

func TestEncodeDecodeOwnerOversized(t *testing.T) {
	// Create an owner string larger than uint16 max (65535)
	oversizedOwner := make([]byte, 70000)
	for i := range oversizedOwner {
		oversizedOwner[i] = byte('a' + (i % 26))
	}
	owner := string(oversizedOwner)

	enc := encodeOwner(owner)
	dec, ok := decodeOwner(enc)
	if !ok {
		t.Fatalf("decodeOwner returned !ok")
	}

	// Should be truncated to 65535 bytes
	expectedTruncated := owner[:0xFFFF]
	if dec != expectedTruncated {
		t.Fatalf("expected truncated owner of length %d, got length %d", len(expectedTruncated), len(dec))
	}

	// Verify the encoded length matches the payload
	if len(enc) != 2+0xFFFF {
		t.Fatalf("expected encoded length %d, got %d", 2+0xFFFF, len(enc))
	}
}

func TestGetPrincipalFromContext(t *testing.T) {
	// Incoming metadata
	mdIn := metadata.Pairs(atlasPrincipalKey, "carol")
	ctx := metadata.NewIncomingContext(context.Background(), mdIn)
	if p := getPrincipalFromContext(ctx); p != "carol" {
		t.Fatalf("expected carol from incoming ctx, got %q", p)
	}

	// Outgoing metadata
	mdOut := metadata.Pairs(atlasPrincipalKey, "dave")
	ctx2 := metadata.NewOutgoingContext(context.Background(), mdOut)
	if p := getPrincipalFromContext(ctx2); p != "dave" {
		t.Fatalf("expected dave from outgoing ctx, got %q", p)
	}
}
