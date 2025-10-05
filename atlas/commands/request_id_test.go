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
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestCommandFromString_WithRequestID(t *testing.T) {
	tests := []struct {
		name              string
		input             string
		expectedRequestID string
		expectedCommand   string
		expectedNorm      string
	}{
		{
			name:              "simple request ID",
			input:             "[ID:123] KEY BLOB GET test",
			expectedRequestID: "123",
			expectedCommand:   "[ID:123] KEY BLOB GET test",
			expectedNorm:      "KEY BLOB GET TEST",
		},
		{
			name:              "alphanumeric request ID",
			input:             "[ID:req-abc-456] SCAN table:USERS:",
			expectedRequestID: "req-abc-456",
			expectedCommand:   "[ID:req-abc-456] SCAN table:USERS:",
			expectedNorm:      "SCAN TABLE:USERS:",
		},
		{
			name:              "UUID request ID",
			input:             "[ID:550e8400-e29b-41d4-a716-446655440000] COUNT table:SESSIONS:",
			expectedRequestID: "550e8400-e29b-41d4-a716-446655440000",
			expectedCommand:   "[ID:550e8400-e29b-41d4-a716-446655440000] COUNT table:SESSIONS:",
			expectedNorm:      "COUNT TABLE:SESSIONS:",
		},
		{
			name:              "no request ID",
			input:             "KEY BLOB GET test",
			expectedRequestID: "",
			expectedCommand:   "KEY BLOB GET test",
			expectedNorm:      "KEY BLOB GET TEST",
		},
		{
			name:              "request ID with extra spaces",
			input:             "[ID:abc]   KEY BLOB GET test",
			expectedRequestID: "abc",
			expectedCommand:   "[ID:abc]   KEY BLOB GET test",
			expectedNorm:      "KEY BLOB GET TEST",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cs := CommandFromString(tt.input)

			assert.Equal(t, tt.expectedRequestID, cs.GetRequestID(), "request ID mismatch")
			assert.Equal(t, tt.expectedCommand, cs.Raw(), "raw command mismatch")
			assert.Equal(t, tt.expectedNorm, cs.Normalized(), "normalized command mismatch")

			if tt.expectedRequestID != "" {
				assert.True(t, cs.HasRequestID(), "should have request ID")
			} else {
				assert.False(t, cs.HasRequestID(), "should not have request ID")
			}
		})
	}
}

func TestCommandFromString_InvalidRequestID(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		hasReqID bool
	}{
		{
			name:     "malformed - no closing bracket",
			input:    "[ID:123 KEY BLOB GET test",
			hasReqID: false,
		},
		{
			name:     "malformed - no ID prefix",
			input:    "[123] KEY BLOB GET test",
			hasReqID: false,
		},
		{
			name:     "malformed - empty ID",
			input:    "[ID:] KEY BLOB GET test",
			hasReqID: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cs := CommandFromString(tt.input)
			assert.Equal(t, tt.hasReqID, cs.HasRequestID(), "unexpected request ID presence")
		})
	}
}

func TestSetRequestID(t *testing.T) {
	cs := CommandFromString("KEY BLOB GET test")
	assert.False(t, cs.HasRequestID())
	assert.Equal(t, "", cs.GetRequestID())

	cs.SetRequestID("test-123")
	assert.True(t, cs.HasRequestID())
	assert.Equal(t, "test-123", cs.GetRequestID())

	cs.SetRequestID("")
	assert.False(t, cs.HasRequestID())
	assert.Equal(t, "", cs.GetRequestID())
}
