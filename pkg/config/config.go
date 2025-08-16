package config

import (
	"fmt"
	"slices"
	"time"

	"github.com/spf13/viper"
)

// Config represents the complete Atlas DB configuration
type Config struct {
	Node      NodeConfig      `mapstructure:"node"`
	Storage   StorageConfig   `mapstructure:"storage"`
	Transport TransportConfig `mapstructure:"transport"`
	Consensus ConsensusConfig `mapstructure:"consensus"`
	Region    RegionConfig    `mapstructure:"region"`
	Logging   LoggingConfig   `mapstructure:"logging"`
}

// NodeConfig contains node-specific configuration
type NodeConfig struct {
	ID       string `mapstructure:"id"`
	Address  string `mapstructure:"address"`
	DataDir  string `mapstructure:"data_dir"`
	RegionID int32  `mapstructure:"region_id"`
}

// StorageConfig contains BadgerDB storage configuration
type StorageConfig struct {
	SyncWrites       bool          `mapstructure:"sync_writes"`
	NumVersions      int           `mapstructure:"num_versions"`
	GCInterval       time.Duration `mapstructure:"gc_interval"`
	ValueLogGCRatio  float64       `mapstructure:"value_log_gc_ratio"`
	MaxTableSize     int64         `mapstructure:"max_table_size"`
	LevelOneSize     int64         `mapstructure:"level_one_size"`
	ValueLogFileSize int64         `mapstructure:"value_log_file_size"`
}

// TransportConfig contains gRPC transport configuration
type TransportConfig struct {
	Port                int           `mapstructure:"port"`
	MaxRecvMsgSize      int           `mapstructure:"max_recv_msg_size"`
	MaxSendMsgSize      int           `mapstructure:"max_send_msg_size"`
	ConnectionTimeout   time.Duration `mapstructure:"connection_timeout"`
	KeepAliveTime       time.Duration `mapstructure:"keep_alive_time"`
	KeepAliveTimeout    time.Duration `mapstructure:"keep_alive_timeout"`
	MaxConnectionIdle   time.Duration `mapstructure:"max_connection_idle"`
	MaxConnectionAge    time.Duration `mapstructure:"max_connection_age"`
	MaxConcurrentStream int           `mapstructure:"max_concurrent_stream"`
}

// ConsensusConfig contains w-paxos consensus configuration
type ConsensusConfig struct {
	PrepareTimeout       time.Duration `mapstructure:"prepare_timeout"`
	AcceptTimeout        time.Duration `mapstructure:"accept_timeout"`
	CommitTimeout        time.Duration `mapstructure:"commit_timeout"`
	HeartbeatInterval    time.Duration `mapstructure:"heartbeat_interval"`
	ElectionTimeout      time.Duration `mapstructure:"election_timeout"`
	MaxProposalRetries   int           `mapstructure:"max_proposal_retries"`
	FastPathEnabled      bool          `mapstructure:"fast_path_enabled"`
	LocalQuorumThreshold float64       `mapstructure:"local_quorum_threshold"`
}

// RegionConfig contains regional optimization configuration
type RegionConfig struct {
	LatencyMonitorInterval time.Duration `mapstructure:"latency_monitor_interval"`
	RegionSyncEnabled      bool          `mapstructure:"region_sync_enabled"`
	RegionSyncInterval     time.Duration `mapstructure:"region_sync_interval"`
	WriteStrategy          string        `mapstructure:"write_strategy"` // "local" or "global"
	ReadStrategy           string        `mapstructure:"read_strategy"`  // "local", "nearest", or "consistent"
	MaxRegionLatency       time.Duration `mapstructure:"max_region_latency"`
}

// LoggingConfig contains logging configuration
type LoggingConfig struct {
	Level  string `mapstructure:"level"`
	Format string `mapstructure:"format"` // "json" or "text"
	Output string `mapstructure:"output"` // "stdout", "stderr", or file path
}

// ClusterConfig represents cluster-wide configuration
type ClusterConfig struct {
	Nodes []NodeInfo `mapstructure:"nodes"`
}

// NodeInfo represents information about a cluster node
type NodeInfo struct {
	ID       string `mapstructure:"id"`
	Address  string `mapstructure:"address"`
	RegionID int32  `mapstructure:"region_id"`
}

// DefaultConfig returns a configuration with default values
func DefaultConfig() *Config {
	return &Config{
		Node: NodeConfig{
			ID:       "atlas-node-1",
			Address:  "localhost:8080",
			DataDir:  "./data",
			RegionID: 1,
		},
		Storage: StorageConfig{
			SyncWrites:       true,
			NumVersions:      10,
			GCInterval:       5 * time.Minute,
			ValueLogGCRatio:  0.5,
			MaxTableSize:     64 << 20,  // 64MB
			LevelOneSize:     256 << 20, // 256MB
			ValueLogFileSize: 1 << 30,   // 1GB
		},
		Transport: TransportConfig{
			Port:                8080,
			MaxRecvMsgSize:      4 << 20, // 4MB
			MaxSendMsgSize:      4 << 20, // 4MB
			ConnectionTimeout:   10 * time.Second,
			KeepAliveTime:       30 * time.Second,
			KeepAliveTimeout:    5 * time.Second,
			MaxConnectionIdle:   15 * time.Minute,
			MaxConnectionAge:    30 * time.Minute,
			MaxConcurrentStream: 100,
		},
		Consensus: ConsensusConfig{
			PrepareTimeout:       2 * time.Second,
			AcceptTimeout:        2 * time.Second,
			CommitTimeout:        1 * time.Second,
			HeartbeatInterval:    1 * time.Second,
			ElectionTimeout:      5 * time.Second,
			MaxProposalRetries:   3,
			FastPathEnabled:      true,
			LocalQuorumThreshold: 0.6,
		},
		Region: RegionConfig{
			LatencyMonitorInterval: 30 * time.Second,
			RegionSyncEnabled:      true,
			RegionSyncInterval:     10 * time.Second,
			WriteStrategy:          "local",
			ReadStrategy:           "local",
			MaxRegionLatency:       100 * time.Millisecond,
		},
		Logging: LoggingConfig{
			Level:  "info",
			Format: "json",
			Output: "stdout",
		},
	}
}

// ResetViper resets the global Viper instance (for testing)
func ResetViper() {
	viper.Reset()
}

// LoadConfig loads configuration from file and environment variables
func LoadConfig(configPath string) (*Config, error) {
	config := DefaultConfig()

	viper.SetConfigType("yaml")
	viper.SetConfigName("atlas")

	if configPath != "" {
		viper.SetConfigFile(configPath)
	} else {
		viper.AddConfigPath("./config")
		viper.AddConfigPath("/etc/atlas")
		viper.AddConfigPath("$HOME/.atlas")
	}

	// Environment variable prefix
	viper.SetEnvPrefix("ATLAS")
	viper.AutomaticEnv()

	// Set up environment variable mapping
	viper.BindEnv("node.id", "ATLAS_NODE_ID")
	viper.BindEnv("node.address", "ATLAS_NODE_ADDRESS")
	viper.BindEnv("node.data_dir", "ATLAS_DATA_DIR")
	viper.BindEnv("node.region_id", "ATLAS_REGION_ID")
	viper.BindEnv("transport.port", "ATLAS_PORT")
	viper.BindEnv("region.write_strategy", "ATLAS_WRITE_STRATEGY")
	viper.BindEnv("region.read_strategy", "ATLAS_READ_STRATEGY")
	viper.BindEnv("logging.level", "ATLAS_LOG_LEVEL")

	// Try to read config file
	if err := viper.ReadInConfig(); err != nil {
		if _, ok := err.(viper.ConfigFileNotFoundError); !ok {
			return nil, fmt.Errorf("error reading config file: %w", err)
		}
		// Config file not found is okay, we'll use defaults and env vars
	}

	// Unmarshal config
	if err := viper.Unmarshal(config); err != nil {
		return nil, fmt.Errorf("error unmarshaling config: %w", err)
	}

	// Validate configuration
	if err := config.Validate(); err != nil {
		return nil, fmt.Errorf("invalid configuration: %w", err)
	}

	return config, nil
}

// LoadClusterConfig loads cluster configuration
func LoadClusterConfig(configPath string) (*ClusterConfig, error) {
	viper := viper.New()
	viper.SetConfigType("yaml")
	viper.SetConfigFile(configPath)

	if err := viper.ReadInConfig(); err != nil {
		return nil, fmt.Errorf("error reading cluster config: %w", err)
	}

	var config ClusterConfig
	if err := viper.Unmarshal(&config); err != nil {
		return nil, fmt.Errorf("error unmarshaling cluster config: %w", err)
	}

	return &config, nil
}

// Validate validates the configuration
func (c *Config) Validate() error {
	if c.Node.ID == "" {
		return fmt.Errorf("node.id cannot be empty")
	}

	if c.Node.Address == "" {
		return fmt.Errorf("node.address cannot be empty")
	}

	if c.Node.DataDir == "" {
		return fmt.Errorf("node.data_dir cannot be empty")
	}

	if c.Node.RegionID <= 0 {
		return fmt.Errorf("node.region_id must be positive")
	}

	if c.Transport.Port <= 0 || c.Transport.Port > 65535 {
		return fmt.Errorf("transport.port must be between 1 and 65535")
	}

	if c.Storage.NumVersions <= 0 {
		return fmt.Errorf("storage.num_versions must be positive")
	}

	if c.Storage.ValueLogGCRatio <= 0 || c.Storage.ValueLogGCRatio >= 1 {
		return fmt.Errorf("storage.value_log_gc_ratio must be between 0 and 1")
	}

	if c.Consensus.LocalQuorumThreshold <= 0 || c.Consensus.LocalQuorumThreshold > 1 {
		return fmt.Errorf("consensus.local_quorum_threshold must be between 0 and 1")
	}

	validWriteStrategies := []string{"local", "global"}
	if !contains(validWriteStrategies, c.Region.WriteStrategy) {
		return fmt.Errorf("region.write_strategy must be one of: %v", validWriteStrategies)
	}

	validReadStrategies := []string{"local", "nearest", "consistent"}
	if !contains(validReadStrategies, c.Region.ReadStrategy) {
		return fmt.Errorf("region.read_strategy must be one of: %v", validReadStrategies)
	}

	validLogLevels := []string{"debug", "info", "warn", "error"}
	if !contains(validLogLevels, c.Logging.Level) {
		return fmt.Errorf("logging.level must be one of: %v", validLogLevels)
	}

	validLogFormats := []string{"json", "text"}
	if !contains(validLogFormats, c.Logging.Format) {
		return fmt.Errorf("logging.format must be one of: %v", validLogFormats)
	}

	return nil
}

// contains checks if a slice contains a string
func contains(slice []string, item string) bool {
	return slices.Contains(slice, item)
}

// SaveConfig saves the configuration to a file
func (c *Config) SaveConfig(path string) error {
	viper.SetConfigFile(path)
	viper.Set("node", c.Node)
	viper.Set("storage", c.Storage)
	viper.Set("transport", c.Transport)
	viper.Set("consensus", c.Consensus)
	viper.Set("region", c.Region)
	viper.Set("logging", c.Logging)

	return viper.WriteConfig()
}
