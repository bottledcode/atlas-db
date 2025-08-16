package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/spf13/cobra"

	"github.com/withinboredom/atlas-db-2/internal/server"
	"github.com/withinboredom/atlas-db-2/pkg/config"
)

var (
	configPath string
	nodeID     string
	address    string
	regionID   int32
	dataDir    string
)

func main() {
	if err := rootCmd.Execute(); err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
}

var rootCmd = &cobra.Command{
	Use:   "atlas-db",
	Short: "Atlas DB - A distributed database with regional optimization",
	Long: `Atlas DB is a distributed database that uses W-Paxos consensus
for global replication while optimizing writes for local regions.
It uses BadgerDB as the underlying key-value store and gRPC for
inter-node communication.`,
}

var startCmd = &cobra.Command{
	Use:   "start",
	Short: "Start the Atlas DB server",
	Long:  `Start the Atlas DB server with the specified configuration.`,
	RunE:  runStart,
}

var versionCmd = &cobra.Command{
	Use:   "version",
	Short: "Print version information",
	Run: func(cmd *cobra.Command, args []string) {
		fmt.Println("Atlas DB v0.1.0")
		fmt.Println("Built with Go")
	},
}

func init() {
	rootCmd.AddCommand(startCmd)
	rootCmd.AddCommand(versionCmd)

	// Global flags
	rootCmd.PersistentFlags().StringVarP(&configPath, "config", "c", "", "Config file path")

	// Start command flags
	startCmd.Flags().StringVar(&nodeID, "node-id", "", "Node ID (overrides config)")
	startCmd.Flags().StringVar(&address, "address", "", "Server address (overrides config)")
	startCmd.Flags().Int32Var(&regionID, "region-id", 0, "Region ID (overrides config)")
	startCmd.Flags().StringVar(&dataDir, "data-dir", "", "Data directory (overrides config)")
}

func runStart(cmd *cobra.Command, args []string) error {
	// Load configuration
	cfg, err := config.LoadConfig(configPath)
	if err != nil {
		return fmt.Errorf("failed to load config: %w", err)
	}

	// Override config with command line flags
	if nodeID != "" {
		cfg.Node.ID = nodeID
	}
	if address != "" {
		cfg.Node.Address = address
	}
	if regionID != 0 {
		cfg.Node.RegionID = regionID
	}
	if dataDir != "" {
		cfg.Node.DataDir = dataDir
	}

	// Create server
	srv, err := server.NewServer(cfg)
	if err != nil {
		return fmt.Errorf("failed to create server: %w", err)
	}

	// Set up signal handling
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	// Start server
	fmt.Printf("Starting Atlas DB server...\n")
	fmt.Printf("Node ID: %s\n", cfg.Node.ID)
	fmt.Printf("Address: %s\n", cfg.Node.Address)
	fmt.Printf("Region ID: %d\n", cfg.Node.RegionID)
	fmt.Printf("Data Directory: %s\n", cfg.Node.DataDir)

	if err := srv.Start(); err != nil {
		return fmt.Errorf("failed to start server: %w", err)
	}

	fmt.Printf("Atlas DB server started successfully\n")

	// Wait for shutdown signal
	select {
	case sig := <-sigChan:
		fmt.Printf("\nReceived signal: %v\n", sig)
		fmt.Printf("Shutting down Atlas DB server...\n")
	case <-ctx.Done():
		fmt.Printf("Context cancelled, shutting down...\n")
	}

	// Graceful shutdown
	if err := srv.Stop(); err != nil {
		fmt.Printf("Error during shutdown: %v\n", err)
		return err
	}

	fmt.Printf("Atlas DB server stopped\n")
	return nil
}