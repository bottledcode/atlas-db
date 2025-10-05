package commands

import (
	"bytes"
	"context"
	"fmt"
	"strings"

	"github.com/bottledcode/atlas-db/atlas"
)

type ScanCommand struct{ CommandString }

func (s *ScanCommand) GetNext() (Command, error) { return s, nil }
func (s *ScanCommand) Execute(ctx context.Context) ([]byte, error) {
	// SCAN <prefix>
	if err := s.CheckMinLen(2); err != nil {
		return nil, err
	}

	// Use raw command to preserve case sensitivity for prefix matching
	prefix, ok := s.SelectNormalizedCommand(1)
	if !ok {
		return nil, fmt.Errorf("expected prefix")
	}
	parts := strings.Split(prefix, ".")
	tablePrefix := parts[0]
	rowPrefix := ""
	if len(parts) > 1 {
		rowPrefix = parts[1]
	}

	keys, err := atlas.PrefixScan(ctx, tablePrefix, rowPrefix)
	if err != nil {
		return nil, err
	}

	if len(keys) == 0 {
		return []byte("EMPTY"), nil
	}

	// Format: KEYS:<count>\n<key1>\n<key2>\n...
	var buf bytes.Buffer
	buf.WriteString(fmt.Sprintf("KEYS:%d\n", len(keys)))
	buf.WriteString(strings.Join(keys, "\n"))

	return buf.Bytes(), nil
}

type CountCommand struct{ CommandString }

func (c *CountCommand) GetNext() (Command, error) { return c, nil }
func (c *CountCommand) Execute(ctx context.Context) ([]byte, error) {
	// Placeholder until implemented
	return nil, fmt.Errorf("COUNT not implemented")
}

type SampleCommand struct{ CommandString }

func (s *SampleCommand) GetNext() (Command, error) { return s, nil }
func (s *SampleCommand) Execute(ctx context.Context) ([]byte, error) {
	// Placeholder until implemented
	return nil, fmt.Errorf("SAMPLE not implemented")
}
