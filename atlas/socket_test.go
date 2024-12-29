package atlas

import (
	"testing"
)

func TestRemaining(t *testing.T) {
	tests := []struct {
		command  string
		parts    []string
		from     int
		expected string
	}{
		{"COMMAND PART1 PART2 PART3", []string{"COMMAND", "PART1", "PART2", "PART3"}, 2, "PART2 PART3"},
		{"COMMAND PART1 PART2 PART3", []string{"COMMAND", "PART1", "PART2", "PART3"}, 3, "PART3"},
		{"COMMAND PART1 PART2 PART3", []string{"COMMAND", "PART1", "PART2", "PART3"}, 4, "PART3"},
		{"COMMAND PART1 PART2", []string{"COMMAND", "PART1", "PART2"}, 1, "PART1 PART2"},
	}

	for _, tt := range tests {
		t.Run(tt.command, func(t *testing.T) {
			result := remaining(tt.command, tt.parts, tt.from)
			if result != tt.expected {
				t.Errorf("remaining(%q, %v, %d) = %q; want %q", tt.command, tt.parts, tt.from, result, tt.expected)
			}
		})
	}
}
