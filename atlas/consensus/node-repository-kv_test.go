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
 */

package consensus

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestBuildNodeRegionKey(t *testing.T) {
	tests := []struct {
		name     string
		region   string
		nodeID   int64
		expected string
	}{
		{
			name:     "Simple region and node",
			region:   "us-east-1",
			nodeID:   123,
			expected: "meta:index:node:region:us-east-1:123",
		},
		{
			name:     "Region with dashes",
			region:   "eu-west-2",
			nodeID:   456,
			expected: "meta:index:node:region:eu-west-2:456",
		},
		{
			name:     "Single character region",
			region:   "a",
			nodeID:   1,
			expected: "meta:index:node:region:a:1",
		},
		{
			name:     "Large node ID",
			region:   "global",
			nodeID:   999999,
			expected: "meta:index:node:region:global:999999",
		},
		{
			name:     "Zero node ID",
			region:   "local",
			nodeID:   0,
			expected: "meta:index:node:region:local:0",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := buildNodeRegionKey(tt.region, tt.nodeID)
			assert.Equal(t, tt.expected, string(result))
		})
	}
}

func TestParseRegionFromNodeRegionKey(t *testing.T) {
	tests := []struct {
		name           string
		key            string
		expectedRegion string
		expectedOK     bool
	}{
		{
			name:           "Valid key with standard region",
			key:            "meta:index:node:region:us-east-1:123",
			expectedRegion: "us-east-1",
			expectedOK:     true,
		},
		{
			name:           "Valid key with hyphenated region",
			key:            "meta:index:node:region:eu-west-2:456",
			expectedRegion: "eu-west-2",
			expectedOK:     true,
		},
		{
			name:           "Valid key with single char region",
			key:            "meta:index:node:region:a:1",
			expectedRegion: "a",
			expectedOK:     true,
		},
		{
			name:           "Valid key with numeric region",
			key:            "meta:index:node:region:123:456",
			expectedRegion: "123",
			expectedOK:     true,
		},
		{
			name:           "Valid key with large node ID",
			key:            "meta:index:node:region:global:999999",
			expectedRegion: "global",
			expectedOK:     true,
		},
		{
			name:           "Invalid key - too few parts",
			key:            "meta:index:node:region",
			expectedRegion: "",
			expectedOK:     false,
		},
		{
			name:           "Invalid key - empty region",
			key:            "meta:index:node:region::123",
			expectedRegion: "",
			expectedOK:     false,
		},
		{
			name:           "Invalid key - wrong format",
			key:            "different:format:key",
			expectedRegion: "",
			expectedOK:     false,
		},
		{
			name:           "Invalid key - completely wrong structure",
			key:            "not-a-key",
			expectedRegion: "",
			expectedOK:     false,
		},
		{
			name:           "Edge case - empty string",
			key:            "",
			expectedRegion: "",
			expectedOK:     false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			region, ok := parseRegionFromNodeRegionKey(tt.key)
			assert.Equal(t, tt.expectedOK, ok, "Expected ok=%v, got ok=%v", tt.expectedOK, ok)
			assert.Equal(t, tt.expectedRegion, region, "Expected region=%q, got region=%q", tt.expectedRegion, region)
		})
	}
}

func TestRoundTripKeyGenerationAndParsing(t *testing.T) {
	testCases := []struct {
		region string
		nodeID int64
	}{
		{"us-east-1", 123},
		{"eu-west-2", 456},
		{"global", 1},
		{"a", 999999},
		{"region-with-many-dashes", 0},
	}

	for _, tc := range testCases {
		t.Run(tc.region, func(t *testing.T) {
			// Generate key
			key := buildNodeRegionKey(tc.region, tc.nodeID)
			keyStr := string(key)

			// Parse it back
			parsedRegion, ok := parseRegionFromNodeRegionKey(keyStr)

			// Verify round-trip worked
			assert.True(t, ok, "Should be able to parse generated key")
			assert.Equal(t, tc.region, parsedRegion, "Region should round-trip correctly")
		})
	}
}
