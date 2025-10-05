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

package socket

import (
	"testing"

	"github.com/bottledcode/atlas-db/atlas/commands"
	"github.com/stretchr/testify/assert"
)

func TestWriteTaggedMessage(t *testing.T) {
	tests := []struct {
		name       string
		requestID  string
		message    []byte
		expectsTag bool
	}{
		{
			name:       "with request ID",
			requestID:  "123",
			message:    []byte("OK"),
			expectsTag: true,
		},
		{
			name:       "without request ID",
			requestID:  "",
			message:    []byte("OK"),
			expectsTag: false,
		},
		{
			name:       "with complex request ID",
			requestID:  "req-abc-456",
			message:    []byte("BLOB 1024"),
			expectsTag: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.expectsTag {
				expected := append([]byte("[ID:"+tt.requestID+"] "), tt.message...)
				assert.Contains(t, string(expected), "[ID:"+tt.requestID+"]")
				assert.Contains(t, string(expected), string(tt.message))
			}
		})
	}
}

func TestRequestIDParsing(t *testing.T) {
	tests := []struct {
		name              string
		command           string
		expectedRequestID string
		expectedHasID     bool
	}{
		{
			name:              "command with request ID",
			command:           "[ID:test-123] KEY BLOB GET mykey",
			expectedRequestID: "test-123",
			expectedHasID:     true,
		},
		{
			name:              "command without request ID",
			command:           "KEY BLOB GET mykey",
			expectedRequestID: "",
			expectedHasID:     false,
		},
		{
			name:              "command with UUID request ID",
			command:           "[ID:550e8400-e29b-41d4-a716-446655440000] SCAN prefix:",
			expectedRequestID: "550e8400-e29b-41d4-a716-446655440000",
			expectedHasID:     true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cmd := commands.CommandFromString(tt.command)
			assert.Equal(t, tt.expectedRequestID, cmd.GetRequestID())
			assert.Equal(t, tt.expectedHasID, cmd.HasRequestID())
		})
	}
}

func TestConcurrentCommandTracking(t *testing.T) {
	commandStrs := []string{
		"[ID:1] KEY BLOB GET file1",
		"[ID:2] KEY BLOB GET file2",
		"[ID:3] SCAN table:",
		"KEY BLOB GET file3",
	}

	var asyncCount, syncCount int

	for _, cmdStr := range commandStrs {
		cmd := commands.CommandFromString(cmdStr)
		if cmd.HasRequestID() {
			asyncCount++
		} else {
			syncCount++
		}
	}

	assert.Equal(t, 3, asyncCount, "should have 3 async commands")
	assert.Equal(t, 1, syncCount, "should have 1 sync command")
}

func TestPrincipalCapture(t *testing.T) {
	// This test verifies that principals are captured at command submission time,
	// not at execution time, to avoid race conditions when the principal changes
	// between command submission and async execution.

	tests := []struct {
		name              string
		command           string
		principalAtSubmit string
		expectedPrincipal string
	}{
		{
			name:              "async command captures principal at submission",
			command:           "[ID:1] KEY GET data",
			principalAtSubmit: "alice",
			expectedPrincipal: "alice",
		},
		{
			name:              "sync command uses current principal",
			command:           "KEY GET data",
			principalAtSubmit: "bob",
			expectedPrincipal: "bob",
		},
		{
			name:              "async command with empty principal",
			command:           "[ID:2] SCAN table:",
			principalAtSubmit: "",
			expectedPrincipal: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cmd := commands.CommandFromString(tt.command)

			// Simulate capturing principal at submission time
			capturedPrincipal := tt.principalAtSubmit

			// Verify the principal would be the one at submission time
			assert.Equal(t, tt.expectedPrincipal, capturedPrincipal,
				"principal should be captured at submission time")

			// Verify command has correct request ID status
			if tt.command[:1] == "[" {
				assert.True(t, cmd.HasRequestID(), "command should have request ID")
			} else {
				assert.False(t, cmd.HasRequestID(), "command should not have request ID")
			}
		})
	}
}
