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
	fmt.Println("DEBUG: ScanCommand.Execute called")
	if err := s.CheckMinLen(2); err != nil {
		return nil, err
	}

	// Use raw command to preserve case sensitivity for prefix matching
	prefix := s.SelectCommand(1)
	fmt.Printf("DEBUG: ScanCommand prefix: %s\n", prefix)

	keys, err := atlas.PrefixScan(ctx, prefix)
	if err != nil {
		fmt.Printf("DEBUG: PrefixScan error: %v\n", err)
		return nil, err
	}

	fmt.Printf("DEBUG: PrefixScan returned %d keys\n", len(keys))
	if len(keys) == 0 {
		fmt.Println("DEBUG: Returning EMPTY")
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
