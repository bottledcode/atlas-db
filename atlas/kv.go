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
	"fmt"
	"time"

	"github.com/bottledcode/atlas-db/atlas/consensus"
	"google.golang.org/protobuf/types/known/durationpb"
)

func WriteKey(ctx context.Context, key consensus.KeyName, value []byte) error {
	op := &consensus.KVChange{
		Operation: &consensus.KVChange_Set{
			Set: &consensus.SetChange{
				Key: key,
				Data: &consensus.Record{
					Data: &consensus.Record_Value{
						Value: &consensus.RawData{
							Data: value,
						},
					},
					AccessControl: nil,
				},
			},
		},
	}

	return sendWrite(ctx, key, op)
}

func sendWrite(ctx context.Context, key consensus.KeyName, change *consensus.KVChange) error {
	qm := consensus.GetDefaultQuorumManager(ctx)

	q, err := qm.GetQuorum(ctx, key)
	if err != nil {
		return err
	}

	resp, err := q.WriteKey(ctx, &consensus.WriteKeyRequest{
		Sender: nil,
		Table:  key,
		Value:  change,
	})
	if err != nil {
		return err
	}
	if resp.Success {
		return nil
	}
	return fmt.Errorf("write failed: %s", resp.Error)
}

func AddOwner(ctx context.Context, key consensus.KeyName, owner string) error {
	op := &consensus.KVChange{
		Operation: &consensus.KVChange_Acl{
			Acl: &consensus.AclChange{
				Key: key,
				Change: &consensus.AclChange_Addition{
					Addition: &consensus.ACL{
						Owners: &consensus.ACLData{
							Principals: []string{owner},
						},
					},
				},
			},
		},
	}
	return sendWrite(ctx, key, op)
}

func RevokeOwner(ctx context.Context, key consensus.KeyName, owner string) error {
	op := &consensus.KVChange{
		Operation: &consensus.KVChange_Acl{
			Acl: &consensus.AclChange{
				Key: key,
				Change: &consensus.AclChange_Deletion{
					Deletion: &consensus.ACL{
						Owners: &consensus.ACLData{
							Principals: []string{owner},
						},
					},
				},
			},
		},
	}
	return sendWrite(ctx, key, op)
}

func AddWriter(ctx context.Context, key consensus.KeyName, writer string) error {
	op := &consensus.KVChange{
		Operation: &consensus.KVChange_Acl{
			Acl: &consensus.AclChange{
				Key: key,
				Change: &consensus.AclChange_Addition{
					Addition: &consensus.ACL{
						Writers: &consensus.ACLData{
							Principals: []string{writer},
						},
					},
				},
			},
		},
	}
	return sendWrite(ctx, key, op)
}

func RevokeWriter(ctx context.Context, key consensus.KeyName, writer string) error {
	op := &consensus.KVChange{
		Operation: &consensus.KVChange_Acl{
			Acl: &consensus.AclChange{
				Key: key,
				Change: &consensus.AclChange_Deletion{
					Deletion: &consensus.ACL{
						Writers: &consensus.ACLData{
							Principals: []string{writer},
						},
					},
				},
			},
		},
	}

	return sendWrite(ctx, key, op)
}

func AddReader(ctx context.Context, key consensus.KeyName, reader string) error {
	op := &consensus.KVChange{
		Operation: &consensus.KVChange_Acl{
			Acl: &consensus.AclChange{
				Key: key,
				Change: &consensus.AclChange_Addition{
					Addition: &consensus.ACL{
						Readers: &consensus.ACLData{
							Principals: []string{reader},
						},
					},
				},
			},
		},
	}

	return sendWrite(ctx, key, op)
}

func RevokeReader(ctx context.Context, key consensus.KeyName, reader string) error {
	op := &consensus.KVChange{
		Operation: &consensus.KVChange_Acl{
			Acl: &consensus.AclChange{
				Key: key,
				Change: &consensus.AclChange_Deletion{
					Deletion: &consensus.ACL{
						Readers: &consensus.ACLData{
							Principals: []string{reader},
						},
					},
				},
			},
		},
	}

	return sendWrite(ctx, key, op)
}

func GetKey(ctx context.Context, key consensus.KeyName) ([]byte, error) {
	qm := consensus.GetDefaultQuorumManager(ctx)

	q, err := qm.GetQuorum(ctx, key)
	if err != nil {
		return nil, err
	}
	resp, err := q.ReadKey(ctx, &consensus.ReadKeyRequest{
		Sender: nil,
		Key:    key,
	})
	if err != nil {
		return nil, err
	}
	if resp.Success {
		return resp.Value, nil
	}
	return nil, nil
}

// DeleteKey performs a distributed delete of the provided key using the
// same migration-based consensus path used for writes.
func DeleteKey(ctx context.Context, key consensus.KeyName) error {
	qm := consensus.GetDefaultQuorumManager(ctx)

	q, err := qm.GetQuorum(ctx, key)
	if err != nil {
		return err
	}

	// Reuse WriteKeyRequest shape for quorum-level delete operation
	resp, err := q.DeleteKey(ctx, &consensus.WriteKeyRequest{
		Sender: nil,
		Table:  key,
		Value: &consensus.KVChange{
			Operation: &consensus.KVChange_Del{
				Del: &consensus.DelChange{
					Key: key,
				},
			},
		},
	})
	if err != nil {
		return err
	}
	if resp.Success {
		return nil
	}
	return fmt.Errorf("delete failed: %s", resp.Error)
}

// PrefixScan performs a distributed prefix scan across all nodes in the cluster.
// It returns all keys matching the prefix that are owned by any node.
func PrefixScan(ctx context.Context, prefix consensus.KeyName) ([][]byte, error) {
	// PrefixScan doesn't use table-based quorums since it scans across all keys/tables
	// Instead, we need to directly call the majority quorum's PrefixScan which broadcasts to all nodes
	// For now, use any table to get the quorum (it will use the majority quorum implementation)
	qm := consensus.GetDefaultQuorumManager(ctx)
	broadcast, err := qm.GetBroadcastQuorum(ctx)
	if err != nil {
		return nil, err
	}

	resp, err := broadcast.PrefixScan(ctx, &consensus.PrefixScanRequest{
		Sender: nil,
		Prefix: prefix,
	})
	if resp != nil && resp.Success {
		return resp.GetKeys(), err
	}
	return nil, err
}

type SubscribeOptions struct {
	RetryAttempts  int
	RetryAfterBase time.Duration
	Auth           string
}

func Subscribe(ctx context.Context, prefix consensus.KeyName, callbackUrl string, opts SubscribeOptions) error {
	if opts.RetryAttempts == 0 {
		opts.RetryAttempts = 3
	}
	if opts.RetryAfterBase == 0 {
		opts.RetryAfterBase = 100 * time.Millisecond
	}

	op := &consensus.KVChange{
		Operation: &consensus.KVChange_Sub{
			Sub: &consensus.Subscribe{
				Url:    callbackUrl,
				Prefix: prefix,
				Options: &consensus.SubscribeOptions{
					Batch:          true,
					RetryAttempts:  int32(opts.RetryAttempts),
					RetryAfterBase: durationpb.New(opts.RetryAfterBase),
					Auth:           opts.Auth,
				},
			},
		},
	}

	return sendWrite(ctx, prefix, op)
}
