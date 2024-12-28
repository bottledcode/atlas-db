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
)

func getNewClient(url string) (ConsensusClient, error) {
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
		return nil, err
	}
	defer conn.Close()

	client := NewConsensusClient(conn)
	return client, nil
}

var ErrNoMajority = fmt.Errorf("no majority")

// ProposeRegion proposes a new region to the cluster
func ProposeRegion(ctx context.Context, region string) error {
	conn, err := atlas.MigrationsPool.Take(ctx)
	if err != nil {
		atlas.Logger.Error("failed to take db connection", zap.Error(err))
		return err
	}
	defer atlas.MigrationsPool.Put(conn)

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

	_, err = atlas.ExecuteSQL(ctx, "insert into regions (name) values (:name)", conn, false, atlas.Param{
		Name:  "name",
		Value: region,
	})
	if err != nil {
		// region already exists, so we can just return
		return nil
	}
	actualRegion := &Region{
		RegionName: region,
		RegionId:   conn.LastInsertRowID(),
	}

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

	for i, node := range nodes.Rows {
		go func(node atlas.Row, i int) {
			defer wg.Done()
			if node.GetColumn("address").GetString() == PlaceholderName {
				return
			}
			expected.Add(1)
			clients[i], err = getNewClient(node.GetColumn("address").GetString() + ":" + node.GetColumn("port").GetString())
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

	select {
	case err = <-errChan:
		atlas.Logger.Error("failed to propose region", zap.Error(err))
	case <-finisher:
	}
	if err != nil {
		return err
	}

	errChan = make(chan error)
	wg.Add(len(nodes.Rows))
	finisher = make(chan struct{})

	go func() {
		wg.Wait()
		close(finisher)
	}()

	// topology changes must be accepted by the global majority
	if promised.Load() >= expected.Load()/2+1 {
		_, err = atlas.ExecuteSQL(ctx, "commit", conn, false)
		if err != nil {
			atlas.Logger.Error("failed to commit transaction", zap.Error(err))
			return err
		}

		// we have a majority and can accept the change
		for i, node := range nodes.Rows {
			go func(node atlas.Row, i int) {
				defer wg.Done()
				if node.GetColumn("address").GetString() == PlaceholderName {
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

	select {
	case err = <-errChan:
		atlas.Logger.Error("failed to accept region", zap.Error(err))
	case <-finisher:
	}

	if err != nil {
		return err
	}

	return nil
}
