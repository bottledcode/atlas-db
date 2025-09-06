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

package commands

import (
	"context"
	"strings"
	"testing"

	"github.com/bottledcode/atlas-db/atlas/consensus"
	"github.com/bottledcode/atlas-db/atlas/kv"
	"github.com/bottledcode/atlas-db/atlas/options"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func TestACLCommand_Parsing(t *testing.T) {
	tests := []struct {
		name        string
		command     string
		expectError bool
		expectType  any
	}{
		{
			name:        "ACL GRANT command",
			command:     "ACL GRANT users.123 alice PERMS READ",
			expectError: false,
			expectType:  &ACLGrantCommand{},
		},
		{
			name:        "ACL REVOKE command",
			command:     "ACL REVOKE users.456 bob PERMS WRITE",
			expectError: false,
			expectType:  &ACLRevokeCommand{},
		},
		{
			name:        "ACL with invalid subcommand",
			command:     "ACL LIST alice users",
			expectError: true,
		},
		{
			name:        "ACL with no subcommand",
			command:     "ACL",
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cs := CommandFromString(tt.command)
			cmd, err := cs.GetNext()

			if tt.expectError {
				assert.Error(t, err)
				return
			}

			require.NoError(t, err)
			assert.IsType(t, tt.expectType, cmd)
		})
	}
}

func TestACLGrantCommand_Parse_Args(t *testing.T) {
	tests := []struct {
		name        string
		command     string
		expectError bool
		errorMsg    string
	}{
		{
			name:        "Valid grant command",
			command:     "ACL GRANT users.123 alice PERMS READ",
			expectError: false,
		},
		{
			name:        "Missing arguments",
			command:     "ACL GRANT users.123 alice",
			expectError: true,
			errorMsg:    "expects 6 arguments",
		},
		{
			name:        "Missing PERMS keyword",
			command:     "ACL GRANT users.123 alice READ extra",
			expectError: true,
			errorMsg:    "ACL GRANT requires format",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cs := CommandFromString(tt.command)
			cmd, err := cs.GetNext()
			require.NoError(t, err)

			grantCmd, ok := cmd.(*ACLGrantCommand)
			require.True(t, ok)

			// Test parsing by attempting execution (will fail on KV pool, but parsing will work)
			_, err = grantCmd.Execute(context.Background())

			if tt.expectError {
				assert.Error(t, err)
				if tt.errorMsg != "" {
					assert.Contains(t, err.Error(), tt.errorMsg)
				}
			}
		})
	}
}

func TestACLRevokeCommand_Parse_Args(t *testing.T) {
	tests := []struct {
		name        string
		command     string
		expectError bool
		errorMsg    string
	}{
		{
			name:        "Valid revoke command",
			command:     "ACL REVOKE users.456 bob PERMS WRITE",
			expectError: false,
		},
		{
			name:        "Missing arguments",
			command:     "ACL REVOKE users.456 bob",
			expectError: true,
			errorMsg:    "expects 6 arguments",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cs := CommandFromString(tt.command)
			cmd, err := cs.GetNext()
			require.NoError(t, err)

			revokeCmd, ok := cmd.(*ACLRevokeCommand)
			require.True(t, ok)

			// Test parsing by attempting execution (will fail on KV pool, but parsing will work)
			_, err = revokeCmd.Execute(context.Background())

			if tt.expectError {
				assert.Error(t, err)
				if tt.errorMsg != "" {
					assert.Contains(t, err.Error(), tt.errorMsg)
				}
			}
		})
	}
}

func TestACLCommands_Integration(t *testing.T) {
	// Setup test environment
	cleanup := setupKVForACL(t)
	defer cleanup()

	ctx := context.Background()

	// Test ACL GRANT
	grantCmd := CommandFromString("ACL GRANT users.123 alice PERMS READ")
	cmd, err := grantCmd.GetNext()
	require.NoError(t, err)

	result, err := cmd.Execute(ctx)
	require.NoError(t, err)
	builtKey := string(kv.FromDottedKey("USERS.123").Build())
	assert.Contains(t, string(result), "ACL granted to alice for "+builtKey+" with permissions READ")

	// Verify READ ACL was set by checking the metadata store directly
	kvPool := kv.GetPool()
	metaStore := kvPool.MetaStore()
	aclKeyRead := consensus.CreateReadACLKey(builtKey)
	aclVal, err := metaStore.Get(ctx, []byte(aclKeyRead))
	require.NoError(t, err)

	aclData, err := consensus.DecodeACLData(aclVal)
	require.NoError(t, err)
	assert.True(t, consensus.HasPrincipal(aclData, "alice"))

	// Test ACL GRANT to add another principal (WRITE)
	grantCmd2 := CommandFromString("ACL GRANT users.123 bob PERMS WRITE")
	cmd2, err := grantCmd2.GetNext()
	require.NoError(t, err)

	result2, err := cmd2.Execute(ctx)
	require.NoError(t, err)
	assert.Contains(t, string(result2), "ACL granted to bob for "+builtKey+" with permissions WRITE")

	// Verify principals are placed under correct permission keys
	// READ should have alice
	aclVal2Read, err := metaStore.Get(ctx, []byte(aclKeyRead))
	require.NoError(t, err)
	aclData2Read, err := consensus.DecodeACLData(aclVal2Read)
	require.NoError(t, err)
	assert.True(t, consensus.HasPrincipal(aclData2Read, "alice"))
	assert.Len(t, aclData2Read.Principals, 1)
	// WRITE should have bob
	aclKeyWrite := consensus.CreateWriteACLKey(builtKey)
	aclVal2Write, err := metaStore.Get(ctx, []byte(aclKeyWrite))
	require.NoError(t, err)
	aclData2Write, err := consensus.DecodeACLData(aclVal2Write)
	require.NoError(t, err)
	assert.True(t, consensus.HasPrincipal(aclData2Write, "bob"))
	assert.Len(t, aclData2Write.Principals, 1)

	// Test ACL REVOKE
	revokeCmd := CommandFromString("ACL REVOKE users.123 alice PERMS READ")
	cmd3, err := revokeCmd.GetNext()
	require.NoError(t, err)

	result3, err := cmd3.Execute(ctx)
	require.NoError(t, err)
	assert.Contains(t, string(result3), "ACL revoked from alice for "+builtKey+" with permissions READ")

	// Verify alice was removed from READ but bob remains in WRITE
	_, err = metaStore.Get(ctx, []byte(aclKeyRead))
	assert.Error(t, err) // READ list should be gone
	aclVal3Write, err := metaStore.Get(ctx, []byte(aclKeyWrite))
	require.NoError(t, err)
	aclData3Write, err := consensus.DecodeACLData(aclVal3Write)
	require.NoError(t, err)
	assert.True(t, consensus.HasPrincipal(aclData3Write, "bob"))
	assert.Len(t, aclData3Write.Principals, 1)

	// Test ACL REVOKE last principal (should remove ACL entirely)
	revokeCmd2 := CommandFromString("ACL REVOKE users.123 bob PERMS WRITE")
	cmd4, err := revokeCmd2.GetNext()
	require.NoError(t, err)

	result4, err := cmd4.Execute(ctx)
	require.NoError(t, err)
	assert.Contains(t, string(result4), "ACL revoked from bob for "+builtKey+" with permissions WRITE")

	// Verify WRITE ACL was completely removed and READ already removed
	_, err = metaStore.Get(ctx, []byte(aclKeyWrite))
	assert.Error(t, err) // Should be kv.ErrKeyNotFound
	_, err = metaStore.Get(ctx, []byte(aclKeyRead))
	assert.Error(t, err) // Should be kv.ErrKeyNotFound
}

func TestACLCommands_CaseInsensitive(t *testing.T) {
	tests := []struct {
		name    string
		command string
	}{
		{
			name:    "Lowercase acl grant",
			command: "acl grant users.123 alice perms read",
		},
		{
			name:    "Mixed case acl revoke",
			command: "Acl Revoke users.456 Bob Perms Write",
		},
		{
			name:    "Uppercase ACL GRANT",
			command: "ACL GRANT USERS.789 CHARLIE PERMS OWNER",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cs := CommandFromString(tt.command)
			cmd, err := cs.GetNext()
			require.NoError(t, err)

			// Should parse successfully regardless of case
			if strings.Contains(strings.ToUpper(tt.command), "GRANT") {
				assert.IsType(t, &ACLGrantCommand{}, cmd)
			} else {
				assert.IsType(t, &ACLRevokeCommand{}, cmd)
			}
		})
	}
}

// setupKVForACL initializes the KV store for ACL testing
func setupKVForACL(t *testing.T) func() {
	// Initialize logger
	options.Logger = zap.NewNop()

	// Setup KV store (similar to server_acl_test.go pattern)
	data := t.TempDir()
	meta := t.TempDir()
	err := kv.CreatePool(data, meta)
	require.NoError(t, err)

	return func() {
		_ = kv.DrainPool()
	}
}
