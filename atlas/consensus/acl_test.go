package consensus

import (
	"context"
	"testing"
	"time"

	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/types/known/timestamppb"
)

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

func TestEncodeDecodeACLData(t *testing.T) {
	principals := []string{"alice", "bob", "charlie"}
	encoded, err := encodeACLData(principals)
	if err != nil {
		t.Fatalf("encodeACLData failed: %v", err)
	}

	decoded, err := decodeACLData(encoded)
	if err != nil {
		t.Fatalf("decodeACLData failed: %v", err)
	}

	if len(decoded.Principals) != len(principals) {
		t.Fatalf("expected %d principals, got %d", len(principals), len(decoded.Principals))
	}

	for i, expected := range principals {
		if decoded.Principals[i] != expected {
			t.Fatalf("expected principal %q at index %d, got %q", expected, i, decoded.Principals[i])
		}
	}

	if decoded.CreatedAt == nil || decoded.UpdatedAt == nil {
		t.Fatalf("timestamps should not be nil")
	}
}

func TestGrantRevokeACLData(t *testing.T) {
	// Start with empty ACL
	aclData := &ACLData{
		Principals: []string{},
		CreatedAt:  timestamppb.New(time.Now()),
		UpdatedAt:  timestamppb.New(time.Now()),
	}

	// Grant access to alice
	updatedACL := grantPrincipal(aclData, "alice")
	if !hasPrincipal(updatedACL, "alice") {
		t.Fatalf("alice should have access after grant")
	}
	if len(updatedACL.Principals) != 1 {
		t.Fatalf("expected 1 principal, got %d", len(updatedACL.Principals))
	}

	// Grant access to bob
	updatedACL = grantPrincipal(updatedACL, "bob")
	if !hasPrincipal(updatedACL, "alice") || !hasPrincipal(updatedACL, "bob") {
		t.Fatalf("both alice and bob should have access")
	}
	if len(updatedACL.Principals) != 2 {
		t.Fatalf("expected 2 principals, got %d", len(updatedACL.Principals))
	}

	// Granting same principal should not duplicate
	updatedACL = grantPrincipal(updatedACL, "alice")
	if len(updatedACL.Principals) != 2 {
		t.Fatalf("granting existing principal should not duplicate, got %d principals", len(updatedACL.Principals))
	}

	// Revoke alice
	updatedACL = revokePrincipal(updatedACL, "alice")
	if hasPrincipal(updatedACL, "alice") {
		t.Fatalf("alice should not have access after revoke")
	}
	if !hasPrincipal(updatedACL, "bob") {
		t.Fatalf("bob should still have access")
	}
	if len(updatedACL.Principals) != 1 {
		t.Fatalf("expected 1 principal after revoke, got %d", len(updatedACL.Principals))
	}

	// Revoke bob
	updatedACL = revokePrincipal(updatedACL, "bob")
	if len(updatedACL.Principals) != 0 {
		t.Fatalf("expected 0 principals after revoking all, got %d", len(updatedACL.Principals))
	}
}

func TestCheckACLAccess(t *testing.T) {
	// Test nil ACL (public access)
	if !checkACLAccess(nil, "anyone") {
		t.Fatalf("nil ACL should allow access")
	}

	// Test empty ACL (public access)
	emptyACL := &ACLData{Principals: []string{}}
	if !checkACLAccess(emptyACL, "anyone") {
		t.Fatalf("empty ACL should allow access")
	}

	// Test ACL with principals
	aclData := &ACLData{Principals: []string{"alice", "bob"}}
	if !checkACLAccess(aclData, "alice") {
		t.Fatalf("alice should have access")
	}
	if !checkACLAccess(aclData, "bob") {
		t.Fatalf("bob should have access")
	}
	if checkACLAccess(aclData, "charlie") {
		t.Fatalf("charlie should not have access")
	}
	if checkACLAccess(aclData, "") {
		t.Fatalf("empty principal should not have access when ACL is restricted")
	}
}

func TestCreateACLKey(t *testing.T) {
	key := createACLKey("users", "user:123")
	expected := "meta:acl:users:user:123"
	if key != expected {
		t.Fatalf("expected %q, got %q", expected, key)
	}
}
