package consensus

import (
	"context"
	"crypto/tls"
	"fmt"
	"github.com/bottledcode/atlas-db/atlas"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/metadata"
	"sync"
	"sync/atomic"
	"time"
)

func getNewClient(url string) (ConsensusClient, error, func()) {
	tlsConfig := &tls.Config{
		InsecureSkipVerify: true,
	}

	creds := credentials.NewTLS(tlsConfig)

	conn, err := grpc.NewClient(url, grpc.WithTransportCredentials(creds), grpc.WithUnaryInterceptor(func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		ctx = metadata.AppendToOutgoingContext(ctx, "Authorization", "Bearer "+atlas.CurrentOptions.ApiKey)
		ctx = metadata.AppendToOutgoingContext(ctx, "Atlas-Service", "Consensus")
		return invoker(ctx, method, req, reply, cc, opts...)
	}), grpc.WithStreamInterceptor(func(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn, method string, streamer grpc.Streamer, opts ...grpc.CallOption) (grpc.ClientStream, error) {
		ctx = metadata.AppendToOutgoingContext(ctx, "Authorization", "Bearer "+atlas.CurrentOptions.ApiKey)
		ctx = metadata.AppendToOutgoingContext(ctx, "Atlas-Service", "Consensus")
		return streamer(ctx, desc, cc, method, opts...)
	}))
	if err != nil {
		return nil, err, nil
	}

	client := NewConsensusClient(conn)
	return client, nil, func() {
		_ = conn.Close()
	}
}

var ErrNoMajority = fmt.Errorf("no majority")

// ProposeRegion proposes a new region to the cluster
func ProposeRegion(ctx context.Context, options *atlas.Options) error {
	conn, err := atlas.MigrationsPool.Take(ctx)
	if err != nil {
		atlas.Logger.Error("failed to take db connection", zap.Error(err))
		return err
	}
	defer atlas.MigrationsPool.Put(conn)

	// begin our transaction and setup a guard to rollback on error
	_, err = atlas.ExecuteSQL(ctx, "begin immediate", conn, false)
	if err != nil {
		atlas.Logger.Error("failed to begin transaction", zap.Error(err))
		return err
	}
	defer func() {
		if err != nil {
			_, err = atlas.ExecuteSQL(ctx, "rollback", conn, false)
			if err != nil {
				atlas.Logger.Error("failed to rollback transaction", zap.Error(err))
			}
		}
	}()

	// check if the region already exists
	regionId, _ := atlas.GetRegionIdFromName(ctx, conn, options.Region)
	if regionId != 0 {
		_, _ = atlas.ExecuteSQL(ctx, "rollback", conn, false)
		// this region already exists and does not need to be proposed
		return nil
	}

	// add the region into the local database, but do not commit yet
	_, err = atlas.ExecuteSQL(ctx, "insert into regions (name) values (:name)", conn, false, atlas.Param{
		Name:  "name",
		Value: options.Region,
	})
	if err != nil {
		return err
	}
	actualRegion := &Region{
		RegionName: options.Region,
		RegionId:   conn.LastInsertRowID(),
	}

	// get a list of all nodes in the cluster
	nodes, err := atlas.ExecuteSQL(ctx, "select id, address, port, region_id from nodes", conn, false)
	if err != nil {
		atlas.Logger.Error("failed to get nodes", zap.Error(err))
		return err
	}

	errChan := make(chan error)
	wg := sync.WaitGroup{}
	wg.Add(len(nodes.Rows))
	finisher := make(chan struct{})
	go func() {
		wg.Wait()
		close(finisher)
	}()
	promised := atomic.Uint64{}
	expected := atomic.Uint64{}

	clients := make([]ConsensusClient, len(nodes.Rows))
	closers := make([]func(), len(nodes.Rows))
	defer func() {
		for _, closer := range closers {
			if closer != nil {
				closer()
			}
		}
	}()

	ctx, done := context.WithTimeout(ctx, time.Second*30)
	defer done()

	// propose the region to all nodes in the cluster
	for i, node := range nodes.Rows {
		go func(node atlas.Row, i int) {
			defer wg.Done()
			if node.GetColumn("address").GetString() == PlaceholderName {
				return
			}
			if node.GetColumn("address").GetString() == options.AdvertiseAddress && node.GetColumn("port").GetInt() == int64(options.AdvertisePort) {
				// we don't need to propose the region to ourselves
				return
			}
			expected.Add(1)
			clients[i], err, closers[i] = getNewClient(node.GetColumn("address").GetString() + ":" + node.GetColumn("port").GetString())
			if err != nil {
				errChan <- err
				return
			}
			result, err := clients[i].ProposeTopologyChange(ctx, &ProposeTopologyChangeRequest{
				Change: &ProposeTopologyChangeRequest_RegionChange{
					RegionChange: &ProposeRegionTopologyChange{
						Region: actualRegion,
						Kind:   TopologyChange_ADD,
					},
				},
			})
			if err != nil {
				errChan <- err
				return
			}
			if result.GetPromise() {
				promised.Add(1)
			} else if result.GetRegion() != nil {
				// todo: adopt the region
			}
		}(node, i)
	}

	// wait for proposals to finish
	select {
	case err = <-errChan:
		atlas.Logger.Error("failed to propose region", zap.Error(err))
	case <-finisher:
	}
	if err != nil {
		return err
	}

	// topology changes must be accepted by the global majority
	if promised.Load() >= expected.Load()/2+1 {
		errChan = make(chan error)
		wg.Add(len(nodes.Rows))
		finisher = make(chan struct{})

		go func() {
			wg.Wait()
			close(finisher)
		}()

		// we can go ahead and commit our own changes and accept the region
		_, err = atlas.ExecuteSQL(ctx, "commit", conn, false)
		if err != nil {
			atlas.Logger.Error("failed to commit transaction", zap.Error(err))
			return err
		}

		// we have a majority and can accept the change
		for i, node := range nodes.Rows {
			go func(node atlas.Row, i int) {
				defer wg.Done()
				if clients[i] == nil {
					return
				}
				_, err = clients[i].AcceptTopologyChange(ctx, &AcceptTopologyChangeRequest{
					Change: &AcceptTopologyChangeRequest_Region{
						Region: actualRegion,
					},
				})
				if err != nil {
					errChan <- err
					return
				}
			}(node, i)
		}

		select {
		case err = <-errChan:
			atlas.Logger.Error("failed to accept region", zap.Error(err))
		case <-finisher:
		}

		if err != nil {
			return err
		}
	} else {
		// we don't have a majority, and there will later be a value accepted here
		_, err = atlas.ExecuteSQL(ctx, "update regions set name = :name where id = :id", conn, false, atlas.Param{
			Name:  "name",
			Value: PlaceholderName,
		}, atlas.Param{
			Name:  "id",
			Value: actualRegion.RegionId,
		})
		_, err = atlas.ExecuteSQL(ctx, "commit", conn, false)
		if err != nil {
			atlas.Logger.Error("failed to rollback transaction", zap.Error(err))
			return err
		}
		return ErrNoMajority
	}

	return nil
}

func ProposeNode(ctx context.Context, options *atlas.Options) error {
	conn, err := atlas.MigrationsPool.Take(ctx)
	if err != nil {
		atlas.Logger.Error("failed to take db connection", zap.Error(err))
		return err
	}
	defer atlas.MigrationsPool.Put(conn)

	// begin our transaction and setup a guard to rollback on error
	_, err = atlas.ExecuteSQL(ctx, "begin immediate", conn, false)
	if err != nil {
		atlas.Logger.Error("failed to begin transaction", zap.Error(err))
		return err
	}
	defer func() {
		if err != nil {
			_, err = atlas.ExecuteSQL(ctx, "rollback", conn, false)
			if err != nil {
				atlas.Logger.Error("failed to rollback transaction", zap.Error(err))
			}
		}
	}()

	regionId, _ := atlas.GetRegionIdFromName(ctx, conn, options.Region)
	if regionId == 0 {
		_, _ = atlas.ExecuteSQL(ctx, "rollback", conn, false)
		return fmt.Errorf("region %s does not exist", options.Region)
	}

	// check if the node already exists
	results, err := atlas.ExecuteSQL(ctx, "select id from nodes where address = :address and port = :port", conn, false, atlas.Param{
		Name:  "address",
		Value: options.AdvertiseAddress,
	}, atlas.Param{
		Name:  "port",
		Value: options.AdvertisePort,
	})
	if err != nil {
		atlas.Logger.Error("failed to check if node exists", zap.Error(err))
		return err
	}
	if len(results.Rows) > 0 {
		// this node already exists and does not need to be proposed
		// todo: it WILL need to be deleted and added again if the region changed
		_, _ = atlas.ExecuteSQL(ctx, "rollback", conn, false)
		return nil
	}

	// add the node into the local database, but do not commit yet
	_, err = atlas.ExecuteSQL(ctx, "insert into nodes (address, port, region_id) values (:address, :port, :region)", conn, false, atlas.Param{
		Name:  "address",
		Value: options.AdvertiseAddress,
	}, atlas.Param{
		Name:  "port",
		Value: options.AdvertisePort,
	}, atlas.Param{
		Name:  "region",
		Value: regionId,
	})
	if err != nil {
		return err
	}
	actualNode := &Node{
		NodeId:      conn.LastInsertRowID(),
		NodeAddress: options.AdvertiseAddress,
		NodeRegion:  options.Region,
		NodePort:    int64(options.AdvertisePort),
	}

	// get a list of all nodes in the cluster
	nodes, err := atlas.ExecuteSQL(ctx, "select id, address, port, region_id from nodes", conn, false)
	if err != nil {
		atlas.Logger.Error("failed to get nodes", zap.Error(err))
		return err
	}

	errChan := make(chan error)
	wg := sync.WaitGroup{}
	wg.Add(len(nodes.Rows))
	finisher := make(chan struct{})
	go func() {
		wg.Wait()
		close(finisher)
	}()
	promised := atomic.Uint64{}
	expected := atomic.Uint64{}

	clients := make([]ConsensusClient, len(nodes.Rows))
	closers := make([]func(), len(nodes.Rows))
	defer func() {
		for _, closer := range closers {
			if closer != nil {
				closer()
			}
		}
	}()

	ctx, done := context.WithTimeout(ctx, time.Second*30)
	defer done()

	// propose the node to all nodes in the cluster
	for i, node := range nodes.Rows {
		go func(node atlas.Row, i int) {
			defer wg.Done()
			if node.GetColumn("address").GetString() == PlaceholderName {
				return
			}
			if node.GetColumn("address").GetString() == options.AdvertiseAddress && node.GetColumn("port").GetInt() == int64(options.AdvertisePort) {
				// we don't need to propose the region to ourselves
				return
			}
			expected.Add(1)
			clients[i], err, closers[i] = getNewClient(node.GetColumn("address").GetString() + ":" + node.GetColumn("port").GetString())
			if err != nil {
				errChan <- err
				return
			}
			result, err := clients[i].ProposeTopologyChange(ctx, &ProposeTopologyChangeRequest{
				Change: &ProposeTopologyChangeRequest_NodeChange{
					NodeChange: &ProposeNodeTopologyChange{
						Kind: TopologyChange_ADD,
						Node: actualNode,
					},
				},
			})
			if err != nil {
				errChan <- err
				return
			}
			if result.GetPromise() {
				promised.Add(1)
			} else if result.GetRegion() != nil {
				// todo: adopt the node
			}
		}(node, i)
	}

	// wait for proposals to finish
	select {
	case err = <-errChan:
		atlas.Logger.Error("failed to propose node", zap.Error(err))
	case <-finisher:
	}
	if err != nil {
		return err
	}

	// topology changes must be accepted by the global majority
	if promised.Load() >= expected.Load()/2+1 {
		errChan = make(chan error)
		wg.Add(len(nodes.Rows))
		finisher = make(chan struct{})

		go func() {
			wg.Wait()
			close(finisher)
		}()

		// we can go ahead and commit our own changes and accept the node
		_, err = atlas.ExecuteSQL(ctx, "commit", conn, false)
		if err != nil {
			atlas.Logger.Error("failed to commit transaction", zap.Error(err))
			return err
		}

		// we have a majority and can accept the change
		for i, node := range nodes.Rows {
			go func(node atlas.Row, i int) {
				defer wg.Done()
				if clients[i] == nil {
					return
				}
				_, err = clients[i].AcceptTopologyChange(ctx, &AcceptTopologyChangeRequest{
					Change: &AcceptTopologyChangeRequest_Node{
						Node: actualNode,
					},
				})
				if err != nil {
					errChan <- err
					return
				}
			}(node, i)
		}

		select {
		case err = <-errChan:
			atlas.Logger.Error("failed to accept node", zap.Error(err))
		case <-finisher:
		}

		if err != nil {
			return err
		}

		options.ServerId = int(actualNode.NodeId)
	} else {
		// we don't have a majority, and there will later be a value accepted here
		_, err = atlas.ExecuteSQL(ctx, "update nodes set address = :name where id = :id", conn, false, atlas.Param{
			Name:  "name",
			Value: PlaceholderName,
		}, atlas.Param{
			Name:  "id",
			Value: actualNode.NodeId,
		})
		_, err = atlas.ExecuteSQL(ctx, "commit", conn, false)
		if err != nil {
			atlas.Logger.Error("failed to rollback transaction", zap.Error(err))
			return err
		}
		return ErrNoMajority
	}

	return nil
}
