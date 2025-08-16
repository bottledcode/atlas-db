package config

import (
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
)

func TestDefaultConfig(t *testing.T) {
	cfg := DefaultConfig()

	if cfg.Node.ID == "" {
		t.Error("Expected default node ID to be set")
	}

	if cfg.Node.Address == "" {
		t.Error("Expected default address to be set")
	}

	if cfg.Node.RegionID <= 0 {
		t.Error("Expected default region ID to be positive")
	}

	if cfg.Storage.NumVersions <= 0 {
		t.Error("Expected default num versions to be positive")
	}

	if cfg.Transport.Port <= 0 {
		t.Error("Expected default port to be positive")
	}

	if cfg.Consensus.PrepareTimeout <= 0 {
		t.Error("Expected default prepare timeout to be positive")
	}

	err := cfg.Validate()
	if err != nil {
		t.Errorf("Default config should be valid: %v", err)
	}
}

func TestConfigValidation(t *testing.T) {
	tests := []struct {
		name        string
		modify      func(*Config)
		expectError bool
		errorMsg    string
	}{
		{
			name: "valid config",
			modify: func(c *Config) {
				// No modifications - should be valid
			},
			expectError: false,
		},
		{
			name: "empty node ID",
			modify: func(c *Config) {
				c.Node.ID = ""
			},
			expectError: true,
			errorMsg:    "node.id cannot be empty",
		},
		{
			name: "empty address",
			modify: func(c *Config) {
				c.Node.Address = ""
			},
			expectError: true,
			errorMsg:    "node.address cannot be empty",
		},
		{
			name: "empty data dir",
			modify: func(c *Config) {
				c.Node.DataDir = ""
			},
			expectError: true,
			errorMsg:    "node.data_dir cannot be empty",
		},
		{
			name: "zero region ID",
			modify: func(c *Config) {
				c.Node.RegionID = 0
			},
			expectError: true,
			errorMsg:    "node.region_id must be positive",
		},
		{
			name: "negative region ID",
			modify: func(c *Config) {
				c.Node.RegionID = -1
			},
			expectError: true,
			errorMsg:    "node.region_id must be positive",
		},
		{
			name: "zero port",
			modify: func(c *Config) {
				c.Transport.Port = 0
			},
			expectError: true,
			errorMsg:    "transport.port must be between 1 and 65535",
		},
		{
			name: "port too high",
			modify: func(c *Config) {
				c.Transport.Port = 70000
			},
			expectError: true,
			errorMsg:    "transport.port must be between 1 and 65535",
		},
		{
			name: "zero num versions",
			modify: func(c *Config) {
				c.Storage.NumVersions = 0
			},
			expectError: true,
			errorMsg:    "storage.num_versions must be positive",
		},
		{
			name: "invalid GC ratio - too low",
			modify: func(c *Config) {
				c.Storage.ValueLogGCRatio = 0
			},
			expectError: true,
			errorMsg:    "storage.value_log_gc_ratio must be between 0 and 1",
		},
		{
			name: "invalid GC ratio - too high",
			modify: func(c *Config) {
				c.Storage.ValueLogGCRatio = 1.5
			},
			expectError: true,
			errorMsg:    "storage.value_log_gc_ratio must be between 0 and 1",
		},
		{
			name: "invalid quorum threshold - too low",
			modify: func(c *Config) {
				c.Consensus.LocalQuorumThreshold = 0
			},
			expectError: true,
			errorMsg:    "consensus.local_quorum_threshold must be between 0 and 1",
		},
		{
			name: "invalid quorum threshold - too high",
			modify: func(c *Config) {
				c.Consensus.LocalQuorumThreshold = 1.5
			},
			expectError: true,
			errorMsg:    "consensus.local_quorum_threshold must be between 0 and 1",
		},
		{
			name: "invalid write strategy",
			modify: func(c *Config) {
				c.Region.WriteStrategy = "invalid"
			},
			expectError: true,
			errorMsg:    "region.write_strategy must be one of",
		},
		{
			name: "invalid read strategy",
			modify: func(c *Config) {
				c.Region.ReadStrategy = "invalid"
			},
			expectError: true,
			errorMsg:    "region.read_strategy must be one of",
		},
		{
			name: "invalid log level",
			modify: func(c *Config) {
				c.Logging.Level = "invalid"
			},
			expectError: true,
			errorMsg:    "logging.level must be one of",
		},
		{
			name: "invalid log format",
			modify: func(c *Config) {
				c.Logging.Format = "invalid"
			},
			expectError: true,
			errorMsg:    "logging.format must be one of",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := DefaultConfig()
			tt.modify(cfg)

			err := cfg.Validate()
			if tt.expectError {
				if err == nil {
					t.Error("Expected validation error")
				} else if tt.errorMsg != "" && !strings.Contains(err.Error(), tt.errorMsg) {
					t.Errorf("Expected error containing %q, got %q", tt.errorMsg, err.Error())
				}
			} else {
				if err != nil {
					t.Errorf("Expected no validation error, got: %v", err)
				}
			}
		})
	}
}

func TestLoadConfigFromFile(t *testing.T) {
	tmpDir := t.TempDir()
	configFile := filepath.Join(tmpDir, "test-config.yaml")

	configContent := `
node:
  id: "test-node"
  address: "localhost:9090"
  data_dir: "/tmp/atlas"
  region_id: 2

transport:
  port: 9090

region:
  write_strategy: "global"
  read_strategy: "consistent"

logging:
  level: "debug"
  format: "text"
`

	err := os.WriteFile(configFile, []byte(configContent), 0644)
	if err != nil {
		t.Fatalf("Failed to write config file: %v", err)
	}

	cfg, err := LoadConfig(configFile)
	if err != nil {
		t.Fatalf("LoadConfig failed: %v", err)
	}

	if cfg.Node.ID != "test-node" {
		t.Errorf("Expected node ID 'test-node', got %s", cfg.Node.ID)
	}

	if cfg.Node.Address != "localhost:9090" {
		t.Errorf("Expected address 'localhost:9090', got %s", cfg.Node.Address)
	}

	if cfg.Node.RegionID != 2 {
		t.Errorf("Expected region ID 2, got %d", cfg.Node.RegionID)
	}

	if cfg.Region.WriteStrategy != "global" {
		t.Errorf("Expected write strategy 'global', got %s", cfg.Region.WriteStrategy)
	}

	if cfg.Region.ReadStrategy != "consistent" {
		t.Errorf("Expected read strategy 'consistent', got %s", cfg.Region.ReadStrategy)
	}

	if cfg.Logging.Level != "debug" {
		t.Errorf("Expected log level 'debug', got %s", cfg.Logging.Level)
	}

	if cfg.Logging.Format != "text" {
		t.Errorf("Expected log format 'text', got %s", cfg.Logging.Format)
	}
}

func TestLoadConfigMissingFile(t *testing.T) {
	// Reset Viper state to clear any cached config
	ResetViper()

	// Clear any environment variables that might interfere
	originalEnvVars := make(map[string]string)
	envVars := []string{"ATLAS_NODE_ID", "ATLAS_REGION_ID", "ATLAS_LOG_LEVEL", "ATLAS_NODE_ADDRESS", "ATLAS_DATA_DIR", "ATLAS_PORT", "ATLAS_WRITE_STRATEGY", "ATLAS_READ_STRATEGY"}

	for _, envVar := range envVars {
		originalEnvVars[envVar] = os.Getenv(envVar)
		os.Unsetenv(envVar)
	}

	defer func() {
		for envVar, value := range originalEnvVars {
			if value != "" {
				os.Setenv(envVar, value)
			} else {
				os.Unsetenv(envVar)
			}
		}
		ResetViper() // Reset after test
	}()

	// Loading missing file should use defaults (config file not found is not an error)
	cfg, err := LoadConfig("")
	if err != nil {
		t.Fatalf("LoadConfig should not fail when no config specified: %v", err)
	}

	// Should have default values
	if cfg.Node.ID != "atlas-node-1" {
		t.Errorf("Expected default node ID, got %s", cfg.Node.ID)
	}
}

func TestLoadConfigInvalidYAML(t *testing.T) {
	tmpDir := t.TempDir()
	configFile := filepath.Join(tmpDir, "invalid.yaml")

	invalidContent := `
node:
  id: "test
  invalid yaml content
`

	err := os.WriteFile(configFile, []byte(invalidContent), 0644)
	if err != nil {
		t.Fatalf("Failed to write invalid config file: %v", err)
	}

	_, err = LoadConfig(configFile)
	if err == nil {
		t.Error("Expected LoadConfig to fail with invalid YAML")
	}
}

func TestLoadConfigInvalidValues(t *testing.T) {
	tmpDir := t.TempDir()
	configFile := filepath.Join(tmpDir, "invalid-values.yaml")

	invalidContent := `
node:
  id: ""
  address: "localhost:8080"
  region_id: -1
`

	err := os.WriteFile(configFile, []byte(invalidContent), 0644)
	if err != nil {
		t.Fatalf("Failed to write config file: %v", err)
	}

	_, err = LoadConfig(configFile)
	if err == nil {
		t.Error("Expected LoadConfig to fail with invalid values")
	}
}

func TestSaveConfig(t *testing.T) {
	tmpDir := t.TempDir()
	configFile := filepath.Join(tmpDir, "saved-config.yaml")

	cfg := DefaultConfig()
	cfg.Node.ID = "saved-node"
	cfg.Node.RegionID = 3

	err := cfg.SaveConfig(configFile)
	if err != nil {
		t.Fatalf("SaveConfig failed: %v", err)
	}

	// Load it back
	loadedCfg, err := LoadConfig(configFile)
	if err != nil {
		t.Fatalf("LoadConfig after save failed: %v", err)
	}

	if loadedCfg.Node.ID != "saved-node" {
		t.Errorf("Expected node ID 'saved-node', got %s", loadedCfg.Node.ID)
	}

	if loadedCfg.Node.RegionID != 3 {
		t.Errorf("Expected region ID 3, got %d", loadedCfg.Node.RegionID)
	}
}

func TestLoadClusterConfig(t *testing.T) {
	tmpDir := t.TempDir()
	clusterFile := filepath.Join(tmpDir, "cluster.yaml")

	clusterContent := `
nodes:
  - id: "node1"
    address: "localhost:8080"
    region_id: 1
  - id: "node2"
    address: "localhost:8081"
    region_id: 1
  - id: "node3"
    address: "localhost:8082"
    region_id: 2
`

	err := os.WriteFile(clusterFile, []byte(clusterContent), 0644)
	if err != nil {
		t.Fatalf("Failed to write cluster file: %v", err)
	}

	clusterCfg, err := LoadClusterConfig(clusterFile)
	if err != nil {
		t.Fatalf("LoadClusterConfig failed: %v", err)
	}

	if len(clusterCfg.Nodes) != 3 {
		t.Errorf("Expected 3 nodes, got %d", len(clusterCfg.Nodes))
	}

	expectedNodes := map[string]NodeInfo{
		"node1": {ID: "node1", Address: "localhost:8080", RegionID: 1},
		"node2": {ID: "node2", Address: "localhost:8081", RegionID: 1},
		"node3": {ID: "node3", Address: "localhost:8082", RegionID: 2},
	}

	for _, node := range clusterCfg.Nodes {
		expected, exists := expectedNodes[node.ID]
		if !exists {
			t.Errorf("Unexpected node: %s", node.ID)
			continue
		}

		if node.Address != expected.Address {
			t.Errorf("Node %s: expected address %s, got %s", node.ID, expected.Address, node.Address)
		}

		if node.RegionID != expected.RegionID {
			t.Errorf("Node %s: expected region %d, got %d", node.ID, expected.RegionID, node.RegionID)
		}
	}
}

func TestEnvironmentVariableOverrides(t *testing.T) {
	// Reset Viper state to clear any cached config
	ResetViper()

	// Clear any existing environment variables first
	envVars := []string{"ATLAS_NODE_ID", "ATLAS_REGION_ID", "ATLAS_LOG_LEVEL", "ATLAS_NODE_ADDRESS", "ATLAS_DATA_DIR", "ATLAS_PORT", "ATLAS_WRITE_STRATEGY", "ATLAS_READ_STRATEGY"}
	originalValues := make(map[string]string)

	for _, envVar := range envVars {
		originalValues[envVar] = os.Getenv(envVar)
		os.Unsetenv(envVar)
	}

	// Set environment variables
	os.Setenv("ATLAS_NODE_ID", "env-node")
	os.Setenv("ATLAS_REGION_ID", "5")
	os.Setenv("ATLAS_LOG_LEVEL", "error")

	defer func() {
		// Restore original values
		for envVar, value := range originalValues {
			if value != "" {
				os.Setenv(envVar, value)
			} else {
				os.Unsetenv(envVar)
			}
		}
		ResetViper() // Reset after test
	}()

	cfg, err := LoadConfig("")
	if err != nil {
		t.Fatalf("LoadConfig failed: %v", err)
	}

	if cfg.Node.ID != "env-node" {
		t.Errorf("Expected node ID from env 'env-node', got %s", cfg.Node.ID)
	}

	if cfg.Node.RegionID != 5 {
		t.Errorf("Expected region ID from env 5, got %d", cfg.Node.RegionID)
	}

	if cfg.Logging.Level != "error" {
		t.Errorf("Expected log level from env 'error', got %s", cfg.Logging.Level)
	}
}

func TestConfigDurations(t *testing.T) {
	cfg := DefaultConfig()

	// Check that durations are properly set
	if cfg.Storage.GCInterval <= 0 {
		t.Error("Expected positive GC interval")
	}

	if cfg.Transport.ConnectionTimeout <= 0 {
		t.Error("Expected positive connection timeout")
	}

	if cfg.Consensus.PrepareTimeout <= 0 {
		t.Error("Expected positive prepare timeout")
	}

	if cfg.Region.LatencyMonitorInterval <= 0 {
		t.Error("Expected positive latency monitor interval")
	}
}

func TestConfigSizes(t *testing.T) {
	cfg := DefaultConfig()

	// Check that sizes are reasonable
	if cfg.Storage.MaxTableSize <= 0 {
		t.Error("Expected positive max table size")
	}

	if cfg.Storage.LevelOneSize <= 0 {
		t.Error("Expected positive level one size")
	}

	if cfg.Storage.ValueLogFileSize <= 0 {
		t.Error("Expected positive value log file size")
	}

	if cfg.Transport.MaxRecvMsgSize <= 0 {
		t.Error("Expected positive max recv msg size")
	}

	if cfg.Transport.MaxSendMsgSize <= 0 {
		t.Error("Expected positive max send msg size")
	}
}

// Helper function to check if slice contains string
func stringInSlice(slice []string, item string) bool {
	return slices.Contains(slice, item)
}
