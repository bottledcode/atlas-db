package commands

import (
	"bytes"
	"context"
	"fmt"

	"github.com/bottledcode/atlas-db/atlas"
	"github.com/bottledcode/atlas-db/atlas/consensus"
)

type ScanCommand struct{ CommandString }

func (s *ScanCommand) GetNext() (Command, error) { return s, nil }
func (s *ScanCommand) Execute(ctx context.Context) ([]byte, error) {
	// SCAN <prefix>
	if err := s.CheckMinLen(2); err != nil {
		return nil, err
	}

	prefix, ok := s.SelectNormalizedCommand(1)
	if !ok {
		return nil, fmt.Errorf("expected prefix")
	}

	keys, err := atlas.PrefixScan(ctx, consensus.KeyName(prefix))
	if err != nil {
		return nil, err
	}

	if len(keys) == 0 {
		return []byte("EMPTY"), nil
	}

	// Format: KEYS:<count>\n<key1>\n<key2>\n...
	var buf bytes.Buffer
	buf.WriteString(fmt.Sprintf("KEYS:%d\n", len(keys)))
	buf.Write(bytes.Join(keys, []byte("\n")))

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
