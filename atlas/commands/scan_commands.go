package commands

import (
	"context"
	"fmt"
)

type ScanCommand struct{ CommandString }

func (s *ScanCommand) GetNext() (Command, error) { return s, nil }
func (s *ScanCommand) Execute(ctx context.Context) ([]byte, error) {
	// Placeholder until implemented
	return nil, fmt.Errorf("SCAN not implemented")
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
