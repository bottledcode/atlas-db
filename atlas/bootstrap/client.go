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

package bootstrap

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync"

	"github.com/bottledcode/atlas-db/atlas/faster"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"

	"github.com/bottledcode/atlas-db/atlas/consensus"
	"github.com/bottledcode/atlas-db/atlas/kv"
	"github.com/bottledcode/atlas-db/atlas/options"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/metadata"
)

var configKey = []byte("atlas:cluster_config")

func JoinCluster(ctx context.Context) error {
	options.Logger.Info("Starting cluster join process")

	// Check if we already have KV stores initialized and contain node information
	kvPool := kv.GetPool()
	if kvPool != nil {
		// Check if we're already registered in the cluster
		existing, err := checkExistingNodeRegistration(ctx)
		if err == nil && existing != nil {
			options.Logger.Info("Node already registered in cluster", zap.Uint64("node_id", existing.GetId()))
			options.CurrentOptions.ServerId = existing.GetId()
			// Load all nodes into quorum manager
			return loadNodesIntoQuorumManager(ctx)
		}
	}

	// We need to bootstrap - get the cluster state first
	kvPool = kv.GetPool()
	if kvPool == nil {
		return fmt.Errorf("KV pool not available - bootstrap database first")
	}

	metaStore := kvPool.MetaStore()
	if metaStore == nil {
		return fmt.Errorf("metadata store not available")
	}

	// Find the node table to get the current owner (for cluster contact)
	//tableRepo := consensus.NewTableRepositoryKV(ctx, metaStore)
	//nodeTable, err := tableRepo.GetTable(consensus.NodeTable)
	//if err != nil {
	//	return fmt.Errorf("failed to get node table: %w", err)
	//}
	//if nodeTable == nil {
	//	return fmt.Errorf("no node table found - cannot join cluster")
	//}

	// Find the next available node ID
	//nodeRepo := consensus.NewNodeRepository(ctx, metaStore)
	//nextID, err := getNextNodeID(nodeRepo)
	//if err != nil {
	//	return fmt.Errorf("failed to get next node ID: %w", err)
	//}

	//options.Logger.Info("Requesting to join cluster",
	//	zap.Int64("next_node_id", nextID),
	//	zap.String("owner_address", nodeTable.GetOwner().GetAddress()))

	//options.CurrentOptions.ServerId = nextID

	// Contact the cluster to request membership
	//err = requestClusterMembership(ctx, nodeTable, nextID)
	//if err != nil {
	//	return fmt.Errorf("failed to request cluster membership: %w", err)
	//}

	// Load all nodes into quorum manager for future consensus operations
	//err = loadNodesIntoQuorumManager(ctx)
	//if err != nil {
	//	return fmt.Errorf("failed to load nodes into quorum manager: %w", err)
	//}

	//options.Logger.Info("Successfully joined cluster", zap.Int64("node_id", nextID))
	return nil
}

// checkExistingNodeRegistration checks if this node is already registered
func checkExistingNodeRegistration(ctx context.Context) (*consensus.Node, error) {
	kvPool := kv.GetPool()
	if kvPool == nil {
		return nil, fmt.Errorf("KV pool not available")
	}

	metaStore := kvPool.MetaStore()
	if metaStore == nil {
		return nil, fmt.Errorf("metadata store not available")
	}

	//nodeRepo := consensus.NewNodeRepository(ctx, metaStore)
	//return nodeRepo.GetNodeByAddress(
	//	options.CurrentOptions.AdvertiseAddress,
	//	uint(options.CurrentOptions.AdvertisePort),
	//)
	return nil, nil
}

// loadNodesIntoQuorumManager loads all known nodes into the quorum manager cache
func loadNodesIntoQuorumManager(ctx context.Context) error {
	kvPool := kv.GetPool()
	if kvPool == nil {
		return fmt.Errorf("KV pool not available")
	}

	metaStore := kvPool.MetaStore()
	if metaStore == nil {
		return fmt.Errorf("metadata store not available")
	}

	//nodeRepo := consensus.NewNodeRepository(ctx, metaStore)
	//qm := consensus.GetDefaultQuorumManager(ctx)

	//return nodeRepo.Iterate(false, func(node *consensus.Node, txn *kv.Transaction) error {
	//	err := qm.AddNode(ctx, node)
	//	if err != nil {
	//		return fmt.Errorf("failed to add node to quorum manager: %w", err)
	//	}
	//	options.Logger.Debug("Added node to quorum manager",
	//		zap.Int64("node_id", node.GetId()),
	//		zap.String("address", node.GetAddress()))
	//	return nil
	//})
	return nil
}

// BootstrapAndJoin performs a complete bootstrap: downloads cluster state and registers as new node
func BootstrapAndJoin(ctx context.Context, bootstrapURL string, dataPath string, metaPath string) error {
	options.Logger.Info("Starting complete bootstrap process",
		zap.String("bootstrap_url", bootstrapURL),
		zap.String("data_path", dataPath),
		zap.String("meta_path", metaPath))

	// Step 1: Download database state from bootstrap server
	options.Logger.Info("Phase 1: Downloading cluster state")
	err := DoBootstrap(ctx, bootstrapURL, dataPath, metaPath)
	if err != nil {
		return fmt.Errorf("failed to bootstrap database state: %w", err)
	}

	// Step 2: Join the cluster as a new node
	//options.Logger.Info("Phase 2: Registering as new node in cluster")
	//err = JoinCluster(ctx)
	//if err != nil {
	//	return fmt.Errorf("failed to join cluster: %w", err)
	//}

	options.Logger.Info("Bootstrap process completed successfully - node is now part of the cluster")
	return nil
}

// InitializeMaybe checks if the database is empty and initializes it if it is
func InitializeMaybe(ctx context.Context) error {
	pool := kv.GetPool()
	if pool == nil {
		return fmt.Errorf("KV pool not available")
	}
	//nodeRepo := consensus.NewNodeRepository(ctx, pool.MetaStore())

	//count, err := nodeRepo.TotalCount()
	//if err != nil {
	//	return err
	//}
	//qm := consensus.GetDefaultQuorumManager(ctx)

	//if count > int64(0) {
	//	options.Logger.Info("Atlas database is not empty; skipping initialization and continuing normal operations")

	//	self, err := nodeRepo.GetNodeByAddress(options.CurrentOptions.AdvertiseAddress, uint(options.CurrentOptions.AdvertisePort))
	//	if err != nil {
	//		return err
	//	}

	//	if self == nil {
	//		options.Logger.Fatal("Could not find the current node in the database, but a node currently exists; please connect to the cluster.")
	//	}

	//	options.CurrentOptions.ServerId = self.GetId()

	//	// add all known nodes to the internal cache
	//	err = nodeRepo.Iterate(false, func(node *consensus.Node, txn *kv.Transaction) error {
	//		return qm.AddNode(ctx, node)
	//	})
	//	if err != nil {
	//		return err
	//	}

	//	return nil
	//}

	qm := consensus.GetDefaultQuorumManager(ctx)

	if options.CurrentOptions.Region == "" {
		options.CurrentOptions.Region = "default"
		options.Logger.Warn("No region specified, using default region", zap.String("region", options.CurrentOptions.Region))
	}

	region := options.CurrentOptions.Region

	if options.CurrentOptions.AdvertisePort == 0 {
		options.CurrentOptions.AdvertisePort = 8080
		options.Logger.Warn("No port specified, using the default port", zap.Uint("port", options.CurrentOptions.AdvertisePort))
	}

	if options.CurrentOptions.AdvertiseAddress == "" {
		options.CurrentOptions.AdvertiseAddress = "localhost"
		options.Logger.Warn("No address specified, using the default address", zap.String("address", options.CurrentOptions.AdvertiseAddress))
	}

	// no nodes exist in the database, so we need to configure things here

	// define the new node:
	node := &consensus.Node{
		Id:      1,
		Address: options.CurrentOptions.AdvertiseAddress,
		Region:  &consensus.Region{Name: region},
		Port:    int64(options.CurrentOptions.AdvertisePort),
		Active:  true,
		Rtt:     durationpb.New(0),
	}

	// initialize the cluster configuration
	mgr := faster.NewLogManager()
	// todo: any gaps requires re-bootstrapping
	log, release, err := mgr.GetLog([]byte("atlas:cluster_config"))
	if err != nil {
		return fmt.Errorf("failed to get cluster config log: %w", err)
	}
	defer release()

	currentConfig := &consensus.ClusterConfig{
		ClusterVersion: 1,
		Nodes:          []*consensus.Node{},
		Fn:             options.CurrentOptions.GetFn(),
		Fz:             options.CurrentOptions.GetFz(),
	}
	slot := uint64(1)
	ballot := faster.Ballot{
		ID:     0,
		NodeID: options.CurrentOptions.ServerId,
	}
	err = log.IterateCommitted(func(entry *faster.LogEntry) error {
		// we need to bootstrap the existing cluster config if it exists, which overrides our configuration
		// todo: make this DRY
		record := &consensus.RecordMutation{}
		err := proto.Unmarshal(entry.Value, record)
		if err != nil {
			return fmt.Errorf("failed to unmarshal cluster config record: %w", err)
		}
		slot = entry.Slot
		if ballot.Less(entry.Ballot) {
			ballot.ID = entry.Ballot.ID
			ballot.NodeID = entry.Ballot.NodeID
		}

		switch payload := record.Message.(type) {
		case *consensus.RecordMutation_ValueAddress:
			err := proto.Unmarshal(payload.ValueAddress.Address, currentConfig)
			if err != nil {
				return fmt.Errorf("failed to unmarshal cluster config: %w", err)
			}
			// ensure we do not reference the memory of the log entry
			currentConfig = proto.Clone(currentConfig).(*consensus.ClusterConfig)
		case *consensus.RecordMutation_Config:
			currentConfig = proto.Clone(payload.Config).(*consensus.ClusterConfig)
		default:
			return fmt.Errorf("unexpected cluster config mutation type: %T", payload)
		}
		return nil
	}, faster.IterateOptions{
		MinSlot:            0,
		MaxSlot:            0,
		IncludeUncommitted: false,
		SkipErrors:         false,
	})
	if err != nil {
		return fmt.Errorf("failed to iterate committed log entries: %w", err)
	}

	if len(currentConfig.Nodes) == 0 {
		currentConfig.Nodes = append(currentConfig.Nodes, node)
		options.Logger.Info("Initialized new cluster configuration with first node", zap.Uint64("node_id", node.Id))
		// add to the log
		mutation := &consensus.RecordMutation{
			Slot: &consensus.Slot{
				Key:  configKey,
				Id:   slot + 1,
				Node: node.Id,
			},
			Ballot: &consensus.Ballot{
				Id:   ballot.ID,
				Node: node.Id,
			},
			Message: &consensus.RecordMutation_Config{
				Config: currentConfig,
			},
			Committed: true,
		}
		options.CurrentOptions.ServerId = node.Id
		data, err := proto.Marshal(mutation)
		if err != nil {
			return fmt.Errorf("failed to marshal cluster config mutation: %w", err)
		}
		ballot.ID += 1
		ballot.NodeID = node.Id
		err = log.Accept(slot+1, ballot, data)
		if err != nil {
			return fmt.Errorf("failed to accept cluster config mutation: %w", err)
		}
		err = log.Commit(slot + 1)
		if err != nil {
			return fmt.Errorf("failed to commit cluster config mutation: %w", err)
		}

		err = qm.AddNode(ctx, node)
		if err != nil {
			return fmt.Errorf("failed to add new node to quorum manager: %w", err)
		}
	} else {
		for _, n := range currentConfig.Nodes {
			// determine if this node is us, possibly with a different ID
			if options.CurrentOptions.ServerId == 0 && n.Address == options.CurrentOptions.AdvertiseAddress && n.Port == int64(options.CurrentOptions.AdvertisePort) {
				options.CurrentOptions.ServerId = n.Id
			}
			err = qm.AddNode(ctx, n)
			if err != nil {
				return fmt.Errorf("failed to add existing node to quorum manager: %w", err)
			}
		}
	}

	// the cluster will now begin in a learning mode to ensure it has the most updated cluster configuration and then
	// insert itself into the cluster.

	return nil
}

// DoBootstrap connects to a seed node, follows cluster config, proposes adding self,
// and waits until we see ourselves in the committed config.
func DoBootstrap(ctx context.Context, seedURL string, dataPath string, metaPath string) error {
	options.Logger.Info("Starting bootstrap process", zap.String("seed_url", seedURL))

	// Initialize the KV pool
	err := kv.CreatePool(dataPath, metaPath)
	if err != nil {
		return fmt.Errorf("failed to create KV pool: %w", err)
	}

	// Define ourselves (ID will be assigned later if needed)
	self := &consensus.Node{
		Address: options.CurrentOptions.AdvertiseAddress,
		Region:  &consensus.Region{Name: options.CurrentOptions.Region},
		Port:    int64(options.CurrentOptions.AdvertisePort),
		Active:  true,
		Rtt:     durationpb.New(0),
	}

	// Load existing config from local storage (snapshot + log entries)
	var currentConfig *consensus.ClusterConfig
	var currentSlot uint64
	var configMu sync.Mutex
	alreadyInCluster := false

	mgr := faster.NewLogManager()
	log, release, err := mgr.InitKey(configKey, func(snapshot *faster.Snapshot) error {
		// Load from snapshot
		cfg := &consensus.ClusterConfig{}
		if err := proto.Unmarshal(snapshot.Data, cfg); err != nil {
			return fmt.Errorf("failed to unmarshal config snapshot: %w", err)
		}
		currentConfig = cfg
		currentSlot = snapshot.Slot
		return nil
	}, func(entry *faster.LogEntry) error {
		// Replay log entry
		record := &consensus.RecordMutation{}
		if err := proto.Unmarshal(entry.Value, record); err != nil {
			return fmt.Errorf("failed to unmarshal config entry: %w", err)
		}
		if cfg, ok := record.Message.(*consensus.RecordMutation_Config); ok {
			currentConfig = proto.Clone(cfg.Config).(*consensus.ClusterConfig)
			currentSlot = entry.Slot
		}
		return nil
	})
	if err != nil {
		return fmt.Errorf("failed to initialize cluster config log: %w", err)
	}
	defer release()

	// Check if we're already in the locally stored config
	if currentConfig != nil {
		for _, node := range currentConfig.Nodes {
			if node.Address == self.Address && node.Port == self.Port {
				self.Id = node.Id
				options.CurrentOptions.ServerId = self.Id
				alreadyInCluster = true
				options.Logger.Info("Found ourselves in local config", zap.Uint64("node_id", self.Id))
				break
			}
		}
	}

	// If we're already in the cluster, just need to catch up - no need to propose
	if alreadyInCluster {
		options.Logger.Info("Already part of cluster, bootstrap complete")
		return nil
	}

	// Connect to seed node
	conn, err := dialNode(ctx, seedURL)
	if err != nil {
		return fmt.Errorf("failed to connect to seed node: %w", err)
	}
	defer conn.Close()

	client := consensus.NewConsensusClient(conn)

	// Start following cluster config from seed node
	stream, err := client.Follow(ctx, &consensus.SlotRequest{
		Key:       configKey,
		StartSlot: currentSlot,
		EndSlot:   0,
	})
	if err != nil {
		return fmt.Errorf("failed to start follow stream: %w", err)
	}

	// Channel to signal when we see ourselves in config
	sawSelf := make(chan struct{})
	errChan := make(chan error, 1)
	configReady := make(chan struct{})
	var configReadyOnce sync.Once

	// Follow goroutine
	go func() {
		for {
			resp, err := stream.Recv()
			if errors.Is(err, io.EOF) {
				errChan <- fmt.Errorf("follow stream closed unexpectedly")
				return
			}
			if err != nil {
				errChan <- fmt.Errorf("follow stream error: %w", err)
				return
			}

			switch c := resp.Message.(type) {
			case *consensus.RecordMutation_Config:
				// Apply to local log
				data, err := proto.Marshal(resp)
				if err != nil {
					options.Logger.Error("failed to marshal config", zap.Error(err))
					continue
				}

				ballot := faster.Ballot{
					ID:     resp.Ballot.Id,
					NodeID: resp.Ballot.Node,
				}

				if err := log.Accept(resp.Slot.Id, ballot, data); err != nil {
					options.Logger.Error("failed to accept config", zap.Error(err))
					continue
				}

				if err := log.Commit(resp.Slot.Id); err != nil {
					options.Logger.Error("failed to commit config", zap.Error(err))
					continue
				}

				// Update current config
				configMu.Lock()
				currentConfig = proto.Clone(c.Config).(*consensus.ClusterConfig)
				currentSlot = resp.Slot.Id
				configMu.Unlock()

				// Signal that we have initial config
				configReadyOnce.Do(func() {
					close(configReady)
				})

				// Check if we see ourselves
				for _, n := range c.Config.Nodes {
					if n.Id == self.Id && self.Id != 0 {
						close(sawSelf)
						return
					}
				}
			}
		}
	}()

	// Wait for initial config from remote (or use what we already have)
	if currentConfig == nil {
		select {
		case <-configReady:
			options.Logger.Info("Received initial cluster config from seed")
		case err := <-errChan:
			return err
		case <-ctx.Done():
			return ctx.Err()
		}
	} else {
		// We have local config but aren't in it yet - signal ready
		configReadyOnce.Do(func() {
			close(configReady)
		})
	}

	// Assign ourselves the next node ID
	configMu.Lock()
	maxID := uint64(0)
	for _, node := range currentConfig.Nodes {
		if node.Id > maxID {
			maxID = node.Id
		}
		// Double-check if we're in the remote config
		if node.Address == self.Address && node.Port == self.Port {
			self.Id = node.Id
			options.CurrentOptions.ServerId = self.Id
			configMu.Unlock()
			options.Logger.Info("Found ourselves in remote config", zap.Uint64("node_id", self.Id))
			return nil
		}
	}
	self.Id = maxID + 1
	options.CurrentOptions.ServerId = self.Id
	configSnapshot := proto.Clone(currentConfig).(*consensus.ClusterConfig)
	slotSnapshot := currentSlot
	configMu.Unlock()

	options.Logger.Info("Assigned new node ID", zap.Uint64("node_id", self.Id))

	// Propose adding ourselves
	options.Logger.Info("Proposing self to cluster")
	err = proposeAddNode(ctx, configSnapshot, self, slotSnapshot)
	if err != nil {
		return fmt.Errorf("failed to propose adding self: %w", err)
	}

	// Wait until we see ourselves in config
	select {
	case <-sawSelf:
		options.Logger.Info("Successfully joined cluster")
	case err := <-errChan:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}

	return nil
}

// proposeAddNode proposes adding a new node to the cluster config via broadcast quorum
func proposeAddNode(ctx context.Context, currentConfig *consensus.ClusterConfig, self *consensus.Node, currentSlot uint64) error {
	// Create new config with self added
	newConfig := proto.Clone(currentConfig).(*consensus.ClusterConfig)
	newConfig.ClusterVersion++
	newConfig.Nodes = append(newConfig.Nodes, self)

	// Build the mutation
	mutation := &consensus.RecordMutation{
		Slot: &consensus.Slot{
			Key:  configKey,
			Id:   currentSlot + 1,
			Node: self.Id,
		},
		Ballot: &consensus.Ballot{
			Id:   currentConfig.ClusterVersion + 1,
			Node: self.Id,
		},
		Message: &consensus.RecordMutation_Config{
			Config: newConfig,
		},
		Committed: false,
	}

	// Get quorum manager and add all existing nodes
	qm := consensus.GetDefaultQuorumManager(ctx)
	for _, node := range currentConfig.Nodes {
		if err := qm.AddNode(ctx, node); err != nil {
			options.Logger.Warn("failed to add node to quorum manager", zap.Error(err))
		}
	}

	quorum, err := qm.GetBroadcastQuorum(ctx, configKey)
	if err != nil {
		return fmt.Errorf("failed to get broadcast quorum: %w", err)
	}

	// Phase 2: Accept
	acceptResp, err := quorum.WriteMigration(ctx, &consensus.WriteMigrationRequest{
		Record: mutation,
	})
	if err != nil {
		return fmt.Errorf("write migration failed: %w", err)
	}
	if !acceptResp.Accepted {
		return fmt.Errorf("write migration not accepted")
	}

	// Phase 3: Commit
	mutation.Committed = true
	_, err = quorum.AcceptMigration(ctx, &consensus.WriteMigrationRequest{
		Record: mutation,
	})
	if err != nil {
		return fmt.Errorf("accept migration failed: %w", err)
	}

	return nil
}

// dialNode creates a gRPC connection to a node
func dialNode(ctx context.Context, addr string) (*grpc.ClientConn, error) {
	tlsConfig, err := options.GetTLSConfig("https://" + addr)
	if err != nil {
		return nil, fmt.Errorf("failed to create TLS config: %w", err)
	}

	creds := credentials.NewTLS(tlsConfig)

	return grpc.NewClient(addr,
		grpc.WithTransportCredentials(creds),
		grpc.WithUnaryInterceptor(func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
			ctx = metadata.AppendToOutgoingContext(ctx, "Authorization", "Bearer "+options.CurrentOptions.ApiKey)
			ctx = metadata.AppendToOutgoingContext(ctx, "Atlas-Service", "Bootstrap")
			return invoker(ctx, method, req, reply, cc, opts...)
		}),
		grpc.WithStreamInterceptor(func(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn, method string, streamer grpc.Streamer, opts ...grpc.CallOption) (grpc.ClientStream, error) {
			ctx = metadata.AppendToOutgoingContext(ctx, "Authorization", "Bearer "+options.CurrentOptions.ApiKey)
			ctx = metadata.AppendToOutgoingContext(ctx, "Atlas-Service", "Bootstrap")
			return streamer(ctx, desc, cc, method, opts...)
		}),
	)
}

// applySnapshotEntries applies KV entries to a store
func applySnapshotEntries(ctx context.Context, store kv.Store, entries []*KVEntry, storeType string) error {
	options.Logger.Info("Applying snapshot entries",
		zap.String("store_type", storeType),
		zap.Int("entry_count", len(entries)))

	// Use batch operations for better performance
	batch := store.NewBatch()
	defer batch.Reset()

	batchSize := 0
	const maxBatchSize = 1000

	for i, entry := range entries {
		err := batch.Set(entry.Key, entry.Value)
		if err != nil {
			return fmt.Errorf("failed to set key %s in batch: %w", string(entry.Key), err)
		}

		batchSize++

		// Flush batch periodically to avoid memory issues
		if batchSize >= maxBatchSize || i == len(entries)-1 {
			err = batch.Flush()
			if err != nil {
				return fmt.Errorf("failed to flush %s batch: %w", storeType, err)
			}

			batch.Reset()
			batchSize = 0

			options.Logger.Debug("Applied batch of entries",
				zap.String("store_type", storeType),
				zap.Int("entries_applied", i+1),
				zap.Int("total_entries", len(entries)))
		}
	}

	options.Logger.Info("Successfully applied all snapshot entries",
		zap.String("store_type", storeType),
		zap.Int("total_entries", len(entries)))

	return nil
}
