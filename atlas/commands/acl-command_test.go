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

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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
