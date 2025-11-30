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

package atlas

import (
	"context"

	"github.com/bottledcode/atlas-db/atlas/cas"
	"github.com/bottledcode/atlas-db/atlas/consensus"
)

func WriteKey(ctx context.Context, key []byte, value []byte) error {
	// 1. Store value in CAS (content-addressable storage)
	qm := consensus.GetDefaultQuorumManager(ctx)
	store := cas.NewStore(qm)

	address, err := store.Put(ctx, key, value)
	if err != nil {
		return err
	}

	// 2. Write mutation through consensus (handles Phase 1/2/3 internally)
	return consensus.WriteMutation(ctx, key, &consensus.RecordMutation{
		Message: &consensus.RecordMutation_ValueAddress{
			ValueAddress: &consensus.DataReference{
				Address: address,
			},
		},
	})
}

func AddOwner(ctx context.Context, key []byte, owner string) error {
	return consensus.WriteMutation(ctx, key, &consensus.RecordMutation{
		Message: &consensus.RecordMutation_AddPrincipal{
			AddPrincipal: &consensus.AddPrincipal{
				Role:      consensus.AclRole_OWNER,
				Principal: &consensus.Principal{Name: "user", Value: owner},
			},
		},
	})
}

func RevokeOwner(ctx context.Context, key []byte, owner string) error {
	return consensus.WriteMutation(ctx, key, &consensus.RecordMutation{
		Message: &consensus.RecordMutation_RemovePrincipal{
			RemovePrincipal: &consensus.RemovePrincipal{
				Role:      consensus.AclRole_OWNER,
				Principal: &consensus.Principal{Name: "user", Value: owner},
			},
		},
	})
}

func AddWriter(ctx context.Context, key []byte, writer string) error {
	return consensus.WriteMutation(ctx, key, &consensus.RecordMutation{
		Message: &consensus.RecordMutation_AddPrincipal{
			AddPrincipal: &consensus.AddPrincipal{
				Role:      consensus.AclRole_WRITER,
				Principal: &consensus.Principal{Name: "user", Value: writer},
			},
		},
	})
}

func RevokeWriter(ctx context.Context, key []byte, writer string) error {
	return consensus.WriteMutation(ctx, key, &consensus.RecordMutation{
		Message: &consensus.RecordMutation_RemovePrincipal{
			RemovePrincipal: &consensus.RemovePrincipal{
				Role:      consensus.AclRole_WRITER,
				Principal: &consensus.Principal{Name: "user", Value: writer},
			},
		},
	})
}

func AddReader(ctx context.Context, key []byte, reader string) error {
	return consensus.WriteMutation(ctx, key, &consensus.RecordMutation{
		Message: &consensus.RecordMutation_AddPrincipal{
			AddPrincipal: &consensus.AddPrincipal{
				Role:      consensus.AclRole_READER,
				Principal: &consensus.Principal{Name: "user", Value: reader},
			},
		},
	})
}

func RevokeReader(ctx context.Context, key []byte, reader string) error {
	return consensus.WriteMutation(ctx, key, &consensus.RecordMutation{
		Message: &consensus.RecordMutation_RemovePrincipal{
			RemovePrincipal: &consensus.RemovePrincipal{
				Role:      consensus.AclRole_READER,
				Principal: &consensus.Principal{Name: "user", Value: reader},
			},
		},
	})
}

func GetKey(ctx context.Context, key []byte) ([]byte, error) {
	// 1. Read the Record from consensus (contains DataReference/CAS address)
	record, err := consensus.ReadKey(ctx, key)
	if err != nil {
		return nil, err
	}

	// Check if record has data
	if record == nil || record.Data == nil {
		return nil, nil // Key exists but has no value (deleted?)
	}

	// 2. Dereference the CAS address to get the actual value
	qm := consensus.GetDefaultQuorumManager(ctx)
	store := cas.NewStore(qm)

	value, err := store.Get(ctx, record.Data.Address)
	if err != nil {
		return nil, err
	}

	return value, nil
}

// PrefixScan performs a distributed prefix scan across all nodes in the cluster.
// It returns all keys matching the prefix that are owned by any node.
func PrefixScan(ctx context.Context, keyPrefix []byte) ([]string, error) {
	// Get broadcast quorum to query all nodes
	qm := consensus.GetDefaultQuorumManager(ctx)
	bq, err := qm.GetBroadcastQuorum(ctx, keyPrefix)
	if err != nil {
		return nil, err
	}

	// Perform prefix scan across all nodes
	resp, err := bq.PrefixScan(ctx, &consensus.PrefixScanRequest{
		Prefix: keyPrefix,
	})
	if err != nil {
		return nil, err
	}

	// Convert [][]byte to []string
	keys := make([]string, len(resp.Keys))
	for i, k := range resp.Keys {
		keys[i] = string(k)
	}

	return keys, nil
}
