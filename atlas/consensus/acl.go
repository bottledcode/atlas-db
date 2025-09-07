package consensus

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/bottledcode/atlas-db/atlas/kv"
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

// DecodeACLData decodes bytes to ACLData protobuf message.
func DecodeACLData(b []byte) (*ACLData, error) {
	var aclData ACLData
	err := proto.Unmarshal(b, &aclData)
	if err != nil {
		return nil, err
	}
	return &aclData, nil
}

// HasPrincipal checks if a principal exists in the ACL data.
func HasPrincipal(aclData *ACLData, principal string) bool {
	return slices.Contains(aclData.Principals, principal)
}

// grantPrincipal adds a principal to the ACL data if not already present.
func grantPrincipal(aclData *ACLData, principal string) *ACLData {
	if !HasPrincipal(aclData, principal) {
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
	return HasPrincipal(aclData, principal)
}

// CreateACLKey creates an ACL metadata key for a given data key.
// Uses the same colon-separated format as the rest of Atlas-DB.
func CreateACLKey(key string) string {
	return "meta:acl:" + key
}

// CreateReadACLKey creates an ACL metadata key for READ permissions for a given data key.
func CreateReadACLKey(key string) string {
	return "meta:acl-r:" + key
}

// CreateWriteACLKey creates an ACL metadata key for WRITE permissions for a given data key.
func CreateWriteACLKey(key string) string {
	return "meta:acl-w:" + key
}

// internal helper to grant to a specific ACL key
func grantToACLKey(ctx context.Context, metaStore interface {
	Get(context.Context, []byte) ([]byte, error)
	Put(context.Context, []byte, []byte) error
}, aclKey, principal string) error {
	aclVal, err := metaStore.Get(ctx, []byte(aclKey))

	var aclData *ACLData
	if err == nil {
		// ACL exists - decode it
		aclData, err = DecodeACLData(aclVal)
		if err != nil {
			return err
		}
	} else if errors.Is(err, kv.ErrKeyNotFound) {
		// ACL doesn't exist - create new one
		aclData = &ACLData{
			Principals: []string{},
			CreatedAt:  timestamppb.New(time.Now()),
			UpdatedAt:  timestamppb.New(time.Now()),
		}
	} else {
		// Unexpected error; propagate
		return err
	}

	// Grant access to principal
	updatedACL := grantPrincipal(aclData, principal)
	encodedACL, err := proto.Marshal(updatedACL)
	if err != nil {
		return err
	}

	return metaStore.Put(ctx, []byte(aclKey), encodedACL)
}

// internal helper to revoke from a specific ACL key
func revokeFromACLKey(ctx context.Context, metaStore interface {
	Get(context.Context, []byte) ([]byte, error)
	Put(context.Context, []byte, []byte) error
	Delete(context.Context, []byte) error
}, aclKey, principal string) error {
	aclVal, err := metaStore.Get(ctx, []byte(aclKey))
	if err != nil {
		// Ignore only not-found errors; propagate others
		if errors.Is(err, kv.ErrKeyNotFound) {
			return nil
		}
		return err
	}

	// Decode existing ACL
	aclData, err := DecodeACLData(aclVal)
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

// GrantACLToKeyWithPermission grants a specific permission (READ, WRITE, OWNER)
// to a principal for the given key by updating the appropriate ACL metadata key.
// OWNER maps to the base ACL key and implies full access.
func GrantACLToKeyWithPermission(ctx context.Context, metaStore interface {
	Get(context.Context, []byte) ([]byte, error)
	Put(context.Context, []byte, []byte) error
}, key, principal, permission string) error {
	switch strings.ToUpper(permission) {
	case "READ":
		return grantToACLKey(ctx, metaStore, CreateReadACLKey(key), principal)
	case "WRITE":
		return grantToACLKey(ctx, metaStore, CreateWriteACLKey(key), principal)
	case "OWNER":
		return GrantACLToKey(ctx, metaStore, key, principal)
	default:
		return fmt.Errorf("unknown permission: %s", permission)
	}
}

// RevokeACLFromKeyWithPermission revokes a specific permission (READ, WRITE, OWNER)
// from a principal for the given key by updating the appropriate ACL metadata key.
// OWNER maps to the base ACL key and implies full access.
func RevokeACLFromKeyWithPermission(ctx context.Context, metaStore interface {
	Get(context.Context, []byte) ([]byte, error)
	Put(context.Context, []byte, []byte) error
	Delete(context.Context, []byte) error
}, key, principal, permission string) error {
	switch strings.ToUpper(permission) {
	case "READ":
		return revokeFromACLKey(ctx, metaStore, CreateReadACLKey(key), principal)
	case "WRITE":
		return revokeFromACLKey(ctx, metaStore, CreateWriteACLKey(key), principal)
	case "OWNER":
		return RevokeACLFromKey(ctx, metaStore, key, principal)
	default:
		return fmt.Errorf("unknown permission: %s", permission)
	}
}

// GrantACLToKey grants access to a principal for a specific key by updating ACL metadata.
// This is designed to be used via WriteKey operations to ensure consensus.
func GrantACLToKey(ctx context.Context, metaStore interface {
	Get(context.Context, []byte) ([]byte, error)
	Put(context.Context, []byte, []byte) error
}, key, principal string) error {
	aclKey := CreateACLKey(key)
	aclVal, err := metaStore.Get(ctx, []byte(aclKey))

	var aclData *ACLData
	if err == nil {
		// ACL exists - decode it
		aclData, err = DecodeACLData(aclVal)
		if err != nil {
			return err
		}
	} else if errors.Is(err, kv.ErrKeyNotFound) {
		// ACL doesn't exist - create new one
		aclData = &ACLData{
			Principals: []string{},
			CreatedAt:  timestamppb.New(time.Now()),
			UpdatedAt:  timestamppb.New(time.Now()),
		}
	} else {
		// Unexpected error; propagate
		return err
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
}, key, principal string) error {
	aclKey := CreateACLKey(key)
	aclVal, err := metaStore.Get(ctx, []byte(aclKey))
	if err != nil {
		// Ignore only not-found errors; propagate others
		if errors.Is(err, kv.ErrKeyNotFound) {
			return nil
		}
		return err
	}

	// Decode existing ACL
	aclData, err := DecodeACLData(aclVal)
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
