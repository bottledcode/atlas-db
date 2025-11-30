/*
 * This file is part of Atlas-DB.
 *
 * Atlas-DB is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of
 * the License, or (at your option) any later version.
 *
 * Atlas-DB is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with Atlas-DB. If not, see <https://www.gnu.org/licenses/>.
 *
 */

package consensus

import (
	"context"
	"slices"

	"google.golang.org/grpc/metadata"
)

// gRPC metadata key for the session principal; must be lowercase.
//
//nolint:unused
const atlasPrincipalKey = "atlas-principal"

//nolint:unused
func isOwner(ctx context.Context, record *Record) bool {
	if record.Acl == nil || record.Acl.Owners == nil {
		return true
	}
	principal := &Principal{
		Name:  "user",
		Value: getPrincipalFromContext(ctx),
	}
	return slices.ContainsFunc(record.Acl.Owners, func(s *Principal) bool {
		return s.Name == principal.Name && s.Value == principal.Value
	})
}

//nolint:unused
func canWrite(ctx context.Context, record *Record) bool {
	if record.Acl == nil || record.Acl.Writers == nil {
		return isOwner(ctx, record)
	}
	principal := &Principal{
		Name:  "user",
		Value: getPrincipalFromContext(ctx),
	}
	return isOwner(ctx, record) || slices.ContainsFunc(record.Acl.Writers, func(p *Principal) bool {
		return p.Name == principal.Name && p.Value == principal.Value
	})
}

//nolint:unused
func canRead(ctx context.Context, record *Record) bool {
	if record.Acl == nil || record.Acl.Readers == nil {
		return isOwner(ctx, record)
	}
	principal := &Principal{
		Name:  "user",
		Value: getPrincipalFromContext(ctx),
	}
	return isOwner(ctx, record) || slices.ContainsFunc(record.Acl.Readers, func(p *Principal) bool {
		return p.Name == principal.Name && p.Value == principal.Value
	})
}

// getPrincipalFromContext extracts the principal identifier from outgoing/incoming metadata.
//
//nolint:unused
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
