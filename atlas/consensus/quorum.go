package consensus

import (
	"context"
	"errors"
	"github.com/bottledcode/atlas-db/atlas"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/emptypb"
	"sync"
	"sync/atomic"
	"zombiezen.com/go/sqlite"
)

type QuorumManager struct{}

type Quorum interface {
	ConsensusClient
}

type majorityQuorum struct {
	nodes []*QuorumNode
}

func (m *majorityQuorum) runQuorum(ctx context.Context, runCmd func(ctx context.Context, node *QuorumNode, i int) error) error {
	wg := sync.WaitGroup{}
	wg.Add(len(m.nodes))

	errs := make([]error, len(m.nodes))

	for i, _ := range m.nodes {
		go func(i int) {
			defer wg.Done()

			err := runCmd(ctx, m.nodes[i], i)
			errs[i] = err
		}(i)
	}
	wg.Wait()

	return errors.Join(errs...)
}

func (m *majorityQuorum) StealTableOwnership(ctx context.Context, in *StealTableOwnershipRequest, opts ...grpc.CallOption) (*StealTableOwnershipResponse, error) {
	// phase 1a
	results := make([]*StealTableOwnershipResponse, len(m.nodes))

	err := m.runQuorum(ctx, func(ctx context.Context, node *QuorumNode, i int) (err error) {
		results[i], err = node.client.StealTableOwnership(ctx, in)
		if err != nil {
			return err
		}

		return nil
	})

	if err != nil {
		return nil, err
	}

	// phase 1b

	successes := 0
	missingMigrations := make([]*Migration, 0)
	highestBallot := in.GetTable()
	for _, result := range results {
		if result != nil && result.Promised {
			successes++
			missingMigrations = append(missingMigrations, result.GetSuccess().MissingMigrations...)
		}
		// if there is a failure, it is due to a low ballot, so we need to increase the ballot and try again
		if result != nil && !result.Promised {
			if result.GetFailure().GetTable().GetVersion() >= highestBallot.GetVersion() {
				highestBallot = result.GetFailure().GetTable()
			}
		}
	}
	if successes <= len(m.nodes)/2 {
		// there is a higher ballot out there and it won
		return &StealTableOwnershipResponse{
			Promised: false,
			Response: &StealTableOwnershipResponse_Failure{
				Failure: &StealTableOwnershipFailure{
					Table: highestBallot,
				},
			},
		}, nil
	}

	// we have a majority, so we are the leader
	return &StealTableOwnershipResponse{
		Promised: true,
		Response: &StealTableOwnershipResponse_Success{
			Success: &StealTableOwnershipSuccess{
				Table:             in.Table,
				MissingMigrations: missingMigrations,
			},
		},
	}, nil
}

func (m *majorityQuorum) WriteMigration(ctx context.Context, in *WriteMigrationRequest, opts ...grpc.CallOption) (*WriteMigrationResponse, error) {
	// phase 2a
	successes := atomic.Int64{}
	mu := sync.Mutex{}
	var table *Table
	err := m.runQuorum(ctx, func(ctx context.Context, node *QuorumNode, i int) (err error) {
		result, err := node.client.WriteMigration(ctx, in)
		if err != nil {
			return err
		}

		if result.GetSuccess() {
			successes.Add(1)
		} else {
			mu.Lock()
			if in.GetTableVersion() < result.GetTable().GetVersion() {
				table = result.GetTable()
			}
			mu.Unlock()
		}

		return nil
	})

	if err != nil {
		return nil, err
	}

	// phase 2b
	if successes.Load() <= int64(len(m.nodes)/2) {
		return &WriteMigrationResponse{
			Success: false,
			Table:   table,
		}, nil
	}

	return &WriteMigrationResponse{
		Success: true,
	}, nil
}

func (m *majorityQuorum) AcceptMigration(ctx context.Context, in *WriteMigrationRequest, opts ...grpc.CallOption) (*emptypb.Empty, error) {
	// phase 3
	err := m.runQuorum(ctx, func(ctx context.Context, node *QuorumNode, i int) (err error) {
		_, err = node.client.AcceptMigration(ctx, in)
		if err != nil {
			return err
		}

		return nil
	})

	if err != nil {
		return nil, err
	}

	return &emptypb.Empty{}, nil
}

func (m *majorityQuorum) LearnMigration(ctx context.Context, in *LearnMigrationRequest, opts ...grpc.CallOption) (Consensus_LearnMigrationClient, error) {
	//TODO implement me
	panic("implement me")
}

func (m *majorityQuorum) JoinCluster(ctx context.Context, in *Node, opts ...grpc.CallOption) (*JoinClusterResponse, error) {
	mu := sync.Mutex{}
	var table *Table
	err := m.runQuorum(ctx, func(ctx context.Context, node *QuorumNode, i int) (err error) {
		resp, err := node.client.JoinCluster(ctx, in)
		if err != nil {
			return err
		}
		if resp.Success {
			return nil
		}

		mu.Lock()
		if table == nil || resp.Table.GetVersion() > table.GetVersion() {
			table = resp.Table
		}
		mu.Unlock()
		return nil
	})

	if err != nil {
		return nil, err
	}

	if table != nil {
		return &JoinClusterResponse{
			Success: false,
			Table:   table,
		}, nil
	}

	return &JoinClusterResponse{
		Success: true,
	}, nil
}

type QuorumNode struct {
	address string
	port    uint
	closer  func()
	client  ConsensusClient
}

// GetStealQuorum returns the quorum for stealing a table. It uses a grid-based approach to determine the best solution.
func (q *QuorumManager) GetStealQuorum(ctx context.Context, table string) (Quorum, error) {
	conn, err := atlas.MigrationsPool.Take(ctx)
	if err != nil {
		return nil, err
	}
	defer func() {
		if err != nil {
			_, _ = atlas.ExecuteSQL(ctx, "ROLLBACK", conn, false)
		}
		atlas.MigrationsPool.Put(conn)
	}()

	// count the number of regions that have a node
	results, err := atlas.ExecuteSQL(ctx, `select count(distinct region) from nodes where active = 1`, conn, false)
	if err != nil {
		return nil, err
	}
	regionCount := results.GetIndex(0).GetColumn("c").GetInt()

	if regionCount == 1 {
		// this is a single region cluster, so we just need to get a simple majority
		results, err = atlas.ExecuteSQL(ctx, `select address, port from nodes where active = 1`, conn, false)
		if err != nil {
			return nil, err
		}

		nodes := make([]*QuorumNode, len(results.Rows))
		for i, row := range results.Rows {
			var client ConsensusClient
			var closer func()
			client, err, closer = getNewClient(row.GetColumn("address").GetString() + ":" + row.GetColumn("port").GetString())

			nodes[i] = &QuorumNode{
				address: row.GetColumn("address").GetString(),
				port:    uint(row.GetColumn("port").GetInt()),
				closer:  closer,
				client:  client,
			}
		}

		return &majorityQuorum{
			nodes: nodes,
		}, nil
	}

	// this is a multi-region cluster...
	// todo: implement the rest of the function
	return nil, nil
}

func (q *QuorumManager) GetMigrationQuorum(ctx context.Context, table string, conn *sqlite.Conn) (Quorum, error) {
	// count the number of regions that have a node
	results, err := atlas.ExecuteSQL(ctx, `select count(distinct region) as c from nodes where active = 1`, conn, false)
	if err != nil {
		return nil, err
	}
	regionCount := results.GetIndex(0).GetColumn("c").GetInt()

	if regionCount == 1 {
		// this is a single region cluster, so we just need to get a simple majority
		results, err = atlas.ExecuteSQL(ctx, `select address, port from nodes where active = 1 and id != :self`, conn, false, atlas.Param{
			Name:  "self",
			Value: atlas.CurrentOptions.ServerId,
		})
		if err != nil {
			return nil, err
		}

		nodes := make([]*QuorumNode, len(results.Rows))
		for i, row := range results.Rows {
			var client ConsensusClient
			var closer func()
			client, err, closer = getNewClient(row.GetColumn("address").GetString() + ":" + row.GetColumn("port").GetString())
			if err != nil {
				return nil, err
			}

			nodes[i] = &QuorumNode{
				address: row.GetColumn("address").GetString(),
				port:    uint(row.GetColumn("port").GetInt()),
				closer:  closer,
				client:  client,
			}
		}

		// this is a fledgling cluster, so we just need ourselves
		if len(nodes) == 0 {
			return &selfQuorum{
				server: &Server{},
			}, nil
		}

		return &majorityQuorum{
			nodes: nodes,
		}, nil
	}

	// this is a multi-region cluster...
	// todo: implement the rest of the function
	return nil, nil
}
