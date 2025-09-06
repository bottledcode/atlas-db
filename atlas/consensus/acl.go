package consensus

import (
	"context"
	"encoding/binary"

	"google.golang.org/grpc/metadata"
)

// aclKeyForDataKey maps a data key to its ACL metadata key.
func aclKeyForDataKey(dataKey string) []byte {
	return []byte("meta:acl:" + dataKey)
}

// encodeOwner encodes a single-string owner principal as length-prefixed bytes.
func encodeOwner(owner string) []byte {
	b := make([]byte, 2+len(owner))
	binary.BigEndian.PutUint16(b[:2], uint16(len(owner)))
	copy(b[2:], []byte(owner))
	return b
}

// decodeOwner decodes a single-string owner principal from length-prefixed bytes.
func decodeOwner(b []byte) (string, bool) {
	if len(b) < 2 {
		return "", false
	}
	l := int(binary.BigEndian.Uint16(b[:2]))
	if len(b) < 2+l {
		return "", false
	}
	return string(b[2 : 2+l]), true
}

// getPrincipalFromContext extracts the principal identifier from outgoing/incoming metadata.
func getPrincipalFromContext(ctx context.Context) string {
	if md, ok := metadata.FromIncomingContext(ctx); ok {
		if vals := md.Get("Atlas-Principal"); len(vals) > 0 {
			return vals[0]
		}
	}
	if md, ok := metadata.FromOutgoingContext(ctx); ok {
		if vals := md.Get("Atlas-Principal"); len(vals) > 0 {
			return vals[0]
		}
	}
	return ""
}
