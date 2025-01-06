package consensus

import (
	"context"
	"errors"
	"github.com/bottledcode/atlas-db/atlas"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/emptypb"
	"sync"
)

type majorityQuorum struct {
	q1 []*QuorumNode
	q2 []*QuorumNode
}

func (m *majorityQuorum) StealTableOwnership(ctx context.Context, in *StealTableOwnershipRequest, opts ...grpc.CallOption) (*StealTableOwnershipResponse, error) {
	// phase 1a
	results := make([]*StealTableOwnershipResponse, len(m.q1))
	errs := make([]error, len(m.q1))

	wg := sync.WaitGroup{}
	wg.Add(len(m.q1))

	for i, node := range m.q1 {
		go func() {
			results[i], errs[i] = node.StealTableOwnership(ctx, in)
			wg.Done()
		}()
	}

	wg.Wait()

	err := joinErrs(errs...)
	if err != nil {
		return nil, err
	}

	// phase 1b
	missingMigrations := make([]*Migration, 0)
	highestBallot := in.GetTable()
	for _, result := range results {
		if result != nil && result.Promised {
			missingMigrations = append(missingMigrations, result.GetSuccess().MissingMigrations...)
		}
		// if there is a failure, it is due to a low ballot, so we need to increase the ballot and try again
		if result != nil && !result.Promised {
			if result.GetFailure().GetTable().GetVersion() >= highestBallot.GetVersion() {
				highestBallot = result.GetFailure().GetTable()
			}
		}
	}
	if highestBallot != in.GetTable() {
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

func joinErrs(e ...error) error {
	errs := make([]error, 0)
	for _, err := range e {
		if err != nil {
			errs = append(errs, err)
		}
	}
	if len(errs) == 0 {
		return nil
	}

	return errors.Join(errs...)
}

func (m *majorityQuorum) WriteMigration(ctx context.Context, in *WriteMigrationRequest, opts ...grpc.CallOption) (*WriteMigrationResponse, error) {
	// phase 2a
	results := make([]*WriteMigrationResponse, len(m.q2))
	errs := make([]error, len(m.q2))
	wg := sync.WaitGroup{}
	wg.Add(len(m.q2))
	for i, node := range m.q2 {
		go func() {
			results[i], errs[i] = node.WriteMigration(ctx, in)
			wg.Done()
		}()
	}

	wg.Wait()

	err := joinErrs(errs...)
	if err != nil {
		return nil, err
	}

	// phase 2b
	for _, result := range results {
		if result != nil && !result.GetSuccess() {
			return result, nil
		}
	}

	atlas.Ownership.Add(in.TableId, in.TableVersion)

	return &WriteMigrationResponse{
		Success: true,
	}, nil
}

func (m *majorityQuorum) AcceptMigration(ctx context.Context, in *WriteMigrationRequest, opts ...grpc.CallOption) (*emptypb.Empty, error) {
	// phase 3
	errs := make([]error, len(m.q2))

	wg := sync.WaitGroup{}
	wg.Add(len(m.q2))

	for i, node := range m.q2 {
		go func() {
			_, errs[i] = node.AcceptMigration(ctx, in)
			wg.Done()
		}()
	}

	wg.Wait()

	err := joinErrs(errs...)
	if err != nil {
		return nil, err
	}

	atlas.Ownership.Commit(in.TableId, in.GetMigration().GetVersion())

	return &emptypb.Empty{}, nil
}

func (m *majorityQuorum) LearnMigration(ctx context.Context, in *LearnMigrationRequest, opts ...grpc.CallOption) (Consensus_LearnMigrationClient, error) {
	//TODO implement me
	panic("implement me")
}

func (m *majorityQuorum) JoinCluster(ctx context.Context, in *Node, opts ...grpc.CallOption) (*JoinClusterResponse, error) {
	return nil, errors.New("no quorum needed to join a cluster")
}
