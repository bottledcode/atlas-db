package consensus

import (
	"context"
	"slices"
	"time"

	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// gRPC metadata key for the session principal; must be lowercase.
const atlasPrincipalKey = "atlas-principal"

// encodeACLData encodes ACLData protobuf message to bytes.
func encodeACLData(principals []string) ([]byte, error) {
	now := timestamppb.New(time.Now())
	aclData := &ACLData{
		Principals: principals,
		CreatedAt:  now,
		UpdatedAt:  now,
	}
	return proto.Marshal(aclData)
}

// decodeACLData decodes bytes to ACLData protobuf message.
func decodeACLData(b []byte) (*ACLData, error) {
	var aclData ACLData
	err := proto.Unmarshal(b, &aclData)
	if err != nil {
		return nil, err
	}
	return &aclData, nil
}

// hasPrincipal checks if a principal exists in the ACL data.
func hasPrincipal(aclData *ACLData, principal string) bool {
	return slices.Contains(aclData.Principals, principal)
}

// grantPrincipal adds a principal to the ACL data if not already present.
func grantPrincipal(aclData *ACLData, principal string) *ACLData {
	if !hasPrincipal(aclData, principal) {
		return &ACLData{
			Principals: append(aclData.Principals, principal),
			CreatedAt:  aclData.CreatedAt,
			UpdatedAt:  timestamppb.New(time.Now()),
		}
	}
	return aclData
}

// revokePrincipal removes a principal from the ACL data.
func revokePrincipal(aclData *ACLData, principal string) *ACLData {
	result := make([]string, 0, len(aclData.Principals))
	for _, p := range aclData.Principals {
		if p != principal {
			result = append(result, p)
		}
	}
	return &ACLData{
		Principals: result,
		CreatedAt:  aclData.CreatedAt,
		UpdatedAt:  timestamppb.New(time.Now()),
	}
}

// getPrincipalFromContext extracts the principal identifier from outgoing/incoming metadata.
func getPrincipalFromContext(ctx context.Context) string {
	if md, ok := metadata.FromIncomingContext(ctx); ok {
		if vals := md.Get(atlasPrincipalKey); len(vals) > 0 {
			return vals[0]
		}
	}
	if md, ok := metadata.FromOutgoingContext(ctx); ok {
		if vals := md.Get(atlasPrincipalKey); len(vals) > 0 {
			return vals[0]
		}
	}
	return ""
}

// checkACLAccess verifies if a principal has access to a key based on ACL data.
func checkACLAccess(aclData *ACLData, principal string) bool {
	if aclData == nil || len(aclData.Principals) == 0 {
		return true // No ACL means public access
	}
	return hasPrincipal(aclData, principal)
}

// createACLKey creates an ACL metadata key for a given data key and table.
func createACLKey(table, key string) string {
	return "meta:acl:" + table + ":" + key
}

// GrantACLToKey grants access to a principal for a specific key by updating ACL metadata.
// This is designed to be used via WriteKey operations to ensure consensus.
func GrantACLToKey(ctx context.Context, metaStore interface {
	Get(context.Context, []byte) ([]byte, error)
	Put(context.Context, []byte, []byte) error
}, table, key, principal string) error {
	aclKey := createACLKey(table, key)
	aclVal, err := metaStore.Get(ctx, []byte(aclKey))

	var aclData *ACLData
	if err == nil {
		// ACL exists - decode it
		aclData, err = decodeACLData(aclVal)
		if err != nil {
			return err
		}
	} else {
		// ACL doesn't exist - create new one
		aclData = &ACLData{
			Principals: []string{},
			CreatedAt:  timestamppb.New(time.Now()),
			UpdatedAt:  timestamppb.New(time.Now()),
		}
	}

	// Grant access to principal
	updatedACL := grantPrincipal(aclData, principal)
	encodedACL, err := proto.Marshal(updatedACL)
	if err != nil {
		return err
	}

	return metaStore.Put(ctx, []byte(aclKey), encodedACL)
}

// RevokeACLFromKey revokes access from a principal for a specific key by updating ACL metadata.
// This is designed to be used via WriteKey operations to ensure consensus.
func RevokeACLFromKey(ctx context.Context, metaStore interface {
	Get(context.Context, []byte) ([]byte, error)
	Put(context.Context, []byte, []byte) error
	Delete(context.Context, []byte) error
}, table, key, principal string) error {
	aclKey := createACLKey(table, key)
	aclVal, err := metaStore.Get(ctx, []byte(aclKey))
	if err != nil {
		// ACL doesn't exist - nothing to revoke
		return nil
	}

	// Decode existing ACL
	aclData, err := decodeACLData(aclVal)
	if err != nil {
		return err
	}

	// Revoke access from principal
	updatedACL := revokePrincipal(aclData, principal)

	// If no principals left, delete the ACL entry entirely
	if len(updatedACL.Principals) == 0 {
		return metaStore.Delete(ctx, []byte(aclKey))
	}

	// Otherwise update the ACL
	encodedACL, err := proto.Marshal(updatedACL)
	if err != nil {
		return err
	}

	return metaStore.Put(ctx, []byte(aclKey), encodedACL)
}
