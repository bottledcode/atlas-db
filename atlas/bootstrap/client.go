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
	"bytes"
	"context"
	"crypto/tls"
	"fmt"
	"io"

	"google.golang.org/protobuf/proto"

	"github.com/bottledcode/atlas-db/atlas/consensus"
	"github.com/bottledcode/atlas-db/atlas/kv"
	"github.com/bottledcode/atlas-db/atlas/options"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func JoinCluster(ctx context.Context) error {
	options.Logger.Info("Starting cluster join process")

	// Check if we already have KV stores initialized and contain node information
	kvPool := kv.GetPool()
	if kvPool != nil {
		// Check if we're already registered in the cluster
		existing, err := checkExistingNodeRegistration(ctx)
		if err == nil && existing != nil {
			options.Logger.Info("Node already registered in cluster", zap.Int64("node_id", existing.GetId()))
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
	tableRepo := consensus.NewTableRepositoryKV(ctx, metaStore)
	nodeTable, err := tableRepo.GetTable(consensus.NodeTable)
	if err != nil {
		return fmt.Errorf("failed to get node table: %w", err)
	}
	if nodeTable == nil {
		return fmt.Errorf("no node table found - cannot join cluster")
	}

	// Find the next available node ID
	nodeRepo := consensus.NewNodeRepositoryKV(ctx, metaStore)
	nextID, err := getNextNodeID(nodeRepo)
	if err != nil {
		return fmt.Errorf("failed to get next node ID: %w", err)
	}

	options.Logger.Info("Requesting to join cluster",
		zap.Int64("next_node_id", nextID),
		zap.String("owner_address", nodeTable.GetOwner().GetAddress()))

	options.CurrentOptions.ServerId = nextID

	// Contact the cluster to request membership
	err = requestClusterMembership(ctx, nodeTable, nextID)
	if err != nil {
		return fmt.Errorf("failed to request cluster membership: %w", err)
	}

	// Load all nodes into quorum manager for future consensus operations
	err = loadNodesIntoQuorumManager(ctx)
	if err != nil {
		return fmt.Errorf("failed to load nodes into quorum manager: %w", err)
	}

	options.Logger.Info("Successfully joined cluster", zap.Int64("node_id", nextID))
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

	nodeRepo := consensus.NewNodeRepositoryKV(ctx, metaStore)
	return nodeRepo.GetNodeByAddress(
		options.CurrentOptions.AdvertiseAddress,
		uint(options.CurrentOptions.AdvertisePort),
	)
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

	nodeRepo := consensus.NewNodeRepositoryKV(ctx, metaStore)
	qm := consensus.GetDefaultQuorumManager(ctx)

	return nodeRepo.Iterate(func(node *consensus.Node) error {
		err := qm.AddNode(ctx, node)
		if err != nil {
			return fmt.Errorf("failed to add node to quorum manager: %w", err)
		}
		options.Logger.Debug("Added node to quorum manager",
			zap.Int64("node_id", node.GetId()),
			zap.String("address", node.GetAddress()))
		return nil
	})
}

// getNextNodeID finds the next available node ID
func getNextNodeID(nodeRepo consensus.NodeRepository) (int64, error) {
	maxID := int64(0)

	err := nodeRepo.Iterate(func(node *consensus.Node) error {
		if node.GetId() > maxID {
			maxID = node.GetId()
		}
		return nil
	})

	if err != nil {
		return 0, err
	}

	return maxID + 1, nil
}

// requestClusterMembership contacts the cluster to request membership
func requestClusterMembership(ctx context.Context, nodeTable *consensus.Table, nodeID int64) error {
	owner := nodeTable.GetOwner()
	url := fmt.Sprintf("%s:%d", owner.GetAddress(), owner.GetPort())

	options.Logger.Info("Contacting cluster owner for membership", zap.String("url", url))

	tlsConfig := &tls.Config{
		InsecureSkipVerify: true,
	}
	creds := credentials.NewTLS(tlsConfig)

	conn, err := grpc.NewClient(url, grpc.WithTransportCredentials(creds),
		grpc.WithUnaryInterceptor(func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
			ctx = metadata.AppendToOutgoingContext(ctx, "Authorization", "Bearer "+options.CurrentOptions.ApiKey)
			ctx = metadata.AppendToOutgoingContext(ctx, "Atlas-Service", "Consensus")
			return invoker(ctx, method, req, reply, cc, opts...)
		}))
	if err != nil {
		return fmt.Errorf("failed to connect to cluster owner: %w", err)
	}
	defer func() { _ = conn.Close() }()

	client := consensus.NewConsensusClient(conn)

	// Build our node registration request
	newNode := &consensus.Node{
		Id:      nodeID,
		Address: options.CurrentOptions.AdvertiseAddress,
		Port:    int64(options.CurrentOptions.AdvertisePort),
		Region:  &consensus.Region{Name: options.CurrentOptions.Region},
		Active:  true,
		Rtt:     durationpb.New(0),
	}

	result, err := client.JoinCluster(ctx, newNode)
	if err != nil {
		return fmt.Errorf("cluster join request failed: %w", err)
	}

	if !result.GetSuccess() {
		if result.GetTable() != nil {
			return fmt.Errorf("join request rejected - not the current owner (table version: %d)",
				result.GetTable().GetVersion())
		}
		return fmt.Errorf("join request rejected")
	}

	options.Logger.Info("Cluster membership request accepted", zap.Int64("assigned_node_id", result.GetNodeId()))

	// Now that we're successfully part of the cluster, add ourselves to our local repository
	// so that we can participate in consensus and KV operations
	kvPool := kv.GetPool()
	if kvPool != nil {
		metaStore := kvPool.MetaStore()
		if metaStore != nil {
			nodeRepo := consensus.NewNodeRepositoryKV(ctx, metaStore)
			if kvRepo, ok := nodeRepo.(*consensus.NodeRepositoryKV); ok {
				err = kvRepo.AddNode(newNode)
				if err != nil {
					options.Logger.Warn("Failed to add self to local node repository after successful join", zap.Error(err))
					// Don't fail the entire join process for this
				} else {
					options.Logger.Info("Successfully added self to local node repository")
				}
			}
		}
	}

	// Add ourselves to quorum manager and connection manager for immediate participation
	connectionManager := consensus.GetNodeConnectionManager(ctx)
	if connectionManager != nil {
		quorumManager := consensus.GetDefaultQuorumManager(ctx)
		if quorumManager != nil {
			// Add the current node to the quorum manager and connection manager
			err = quorumManager.AddNode(ctx, newNode)
			if err != nil {
				options.Logger.Warn("Failed to add self to quorum manager after successful join", zap.Error(err))
				// Don't fail the entire join process for this
			} else {
				options.Logger.Info("Successfully added self to quorum and connection manager")
			}
		}
	}

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
	options.Logger.Info("Phase 2: Registering as new node in cluster")
	err = JoinCluster(ctx)
	if err != nil {
		return fmt.Errorf("failed to join cluster: %w", err)
	}

	options.Logger.Info("Bootstrap process completed successfully - node is now part of the cluster")
	return nil
}

// InitializeMaybe checks if the database is empty and initializes it if it is
func InitializeMaybe(ctx context.Context) error {
	pool := kv.GetPool()
	if pool == nil {
		return fmt.Errorf("KV pool not available")
	}
	nodeRepo := consensus.NewNodeRepositoryKV(ctx, pool.MetaStore())

	count, err := nodeRepo.TotalCount()
	if err != nil {
		return err
	}
	qm := consensus.GetDefaultQuorumManager(ctx)

	if count > int64(0) {
		options.Logger.Info("Atlas database is not empty; skipping initialization and continuing normal operations")

		self, err := nodeRepo.GetNodeByAddress(options.CurrentOptions.AdvertiseAddress, uint(options.CurrentOptions.AdvertisePort))
		if err != nil {
			return err
		}

		if self == nil {
			options.Logger.Fatal("Could not find the current node in the database, but a node currently exists; please connect to the cluster.")
		}

		options.CurrentOptions.ServerId = self.GetId()

		// add all known nodes to the internal cache
		err = nodeRepo.Iterate(func(node *consensus.Node) error {
			return qm.AddNode(ctx, node)
		})
		if err != nil {
			return err
		}

		return nil
	}

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

	table := &consensus.Table{
		Name:              consensus.NodeTable,
		ReplicationLevel:  consensus.ReplicationLevel_global,
		Owner:             node,
		CreatedAt:         timestamppb.Now(),
		Version:           1,
		AllowedRegions:    []string{},
		RestrictedRegions: []string{},
	}

	// For the bootstrap node, directly insert into KV stores without consensus
	options.Logger.Info("Initializing first node directly into KV stores", zap.Int64("node_id", node.Id))

	// Initialize repositories for direct data insertion
	tableRepo := consensus.NewTableRepositoryKV(ctx, pool.MetaStore())
	nodeRepoKV := consensus.NewNodeRepositoryKV(ctx, pool.MetaStore())

	// Insert the node table directly (since we're the first node, we own it) -- however, it may already exist
	_ = tableRepo.InsertTable(table)

	// For the node insertion, we need to use the KV repository directly
	// since the interface doesn't include AddNode (it's handled via consensus in normal operations)
	err = nodeRepoKV.(*consensus.NodeRepositoryKV).AddNode(node)
	if err != nil {
		return fmt.Errorf("failed to insert bootstrap node: %w", err)
	}

	// Set our server ID
	options.CurrentOptions.ServerId = node.Id

	// Add the node to the quorum manager
	err = qm.AddNode(ctx, node)
	if err != nil {
		return fmt.Errorf("failed to add bootstrap node to quorum manager: %w", err)
	}

	options.Logger.Info("Successfully initialized first node", zap.Int64("node_id", node.Id))
	return nil
}

// DoBootstrap connects to the bootstrap server and receives the complete database state
func DoBootstrap(ctx context.Context, url string, dataPath string, metaPath string) error {
	options.Logger.Info("Connecting to bootstrap server for database state transfer", zap.String("url", url))

	tlsConfig := &tls.Config{
		InsecureSkipVerify: true,
	}

	creds := credentials.NewTLS(tlsConfig)

	conn, err := grpc.NewClient(url, grpc.WithTransportCredentials(creds), grpc.WithUnaryInterceptor(func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		ctx = metadata.AppendToOutgoingContext(ctx, "Authorization", "Bearer "+options.CurrentOptions.ApiKey)
		ctx = metadata.AppendToOutgoingContext(ctx, "Atlas-Service", "Bootstrap")
		return invoker(ctx, method, req, reply, cc, opts...)
	}), grpc.WithStreamInterceptor(func(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn, method string, streamer grpc.Streamer, opts ...grpc.CallOption) (grpc.ClientStream, error) {
		ctx = metadata.AppendToOutgoingContext(ctx, "Authorization", "Bearer "+options.CurrentOptions.ApiKey)
		ctx = metadata.AppendToOutgoingContext(ctx, "Atlas-Service", "Bootstrap")
		return streamer(ctx, desc, cc, method, opts...)
	}))
	if err != nil {
		return err
	}
	defer func() { _ = conn.Close() }()

	client := NewBootstrapClient(conn)
	resp, err := client.GetBootstrapData(ctx, &BootstrapRequest{
		Version: 1,
	})
	if err != nil {
		return err
	}

	// Collect all chunks into a buffer
	var completeData bytes.Buffer

	for {
		chunk, err := resp.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return fmt.Errorf("failed to receive bootstrap chunk: %w", err)
		}

		if chunk.GetIncompatibleVersion() != nil {
			return fmt.Errorf("incompatible version: needs version %d", chunk.GetIncompatibleVersion().NeedsVersion)
		}

		data := chunk.GetBootstrapData().GetData()
		if len(data) == 0 {
			// Empty chunk signals end of stream
			break
		}

		_, err = completeData.Write(data)
		if err != nil {
			return fmt.Errorf("failed to buffer bootstrap data: %w", err)
		}
	}

	// Parse the complete database snapshot
	var snapshot DatabaseSnapshot
	err = proto.Unmarshal(completeData.Bytes(), &snapshot)
	if err != nil {
		return fmt.Errorf("failed to unmarshal database snapshot: %w", err)
	}

	options.Logger.Info("Received database snapshot",
		zap.Int("meta_entries", len(snapshot.MetaEntries)),
		zap.Int("data_entries", len(snapshot.DataEntries)))

	// Initialize the KV pool with clean stores
	err = kv.CreatePool(dataPath, metaPath)
	if err != nil {
		return fmt.Errorf("failed to create KV pool: %w", err)
	}

	kvPool := kv.GetPool()
	if kvPool == nil {
		return fmt.Errorf("KV pool not initialized after creation")
	}

	// Apply metadata entries to metadata store
	if len(snapshot.MetaEntries) > 0 {
		metaStore := kvPool.MetaStore()
		if metaStore == nil {
			return fmt.Errorf("metadata store not available")
		}

		err = applySnapshotEntries(ctx, metaStore, snapshot.MetaEntries, "metadata")
		if err != nil {
			return fmt.Errorf("failed to apply metadata entries: %w", err)
		}
	}

	// Apply data entries to data store
	if len(snapshot.DataEntries) > 0 {
		dataStore := kvPool.DataStore()
		if dataStore == nil {
			return fmt.Errorf("data store not available")
		}

		err = applySnapshotEntries(ctx, dataStore, snapshot.DataEntries, "data")
		if err != nil {
			return fmt.Errorf("failed to apply data entries: %w", err)
		}
	}

	// Sync stores to ensure persistence
	err = kvPool.Sync()
	if err != nil {
		return fmt.Errorf("failed to sync KV stores: %w", err)
	}

	options.Logger.Info("Bootstrap completed successfully - database state transferred and applied")
	return nil
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
