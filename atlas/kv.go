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

	"github.com/bottledcode/atlas-db/atlas/consensus"
	"github.com/bottledcode/atlas-db/atlas/kv"
)

func WriteKey(ctx context.Context, builder *kv.KeyBuilder, value []byte) error {
	op := &consensus.KVChange{
		Operation: &consensus.KVChange_Set{
			Set: &consensus.SetChange{
				Key: builder.Build(),
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

	return sendWrite(ctx, builder, op)
}

func sendWrite(ctx context.Context, builder *kv.KeyBuilder, change *consensus.KVChange) error {
	qm := consensus.GetDefaultQuorumManager(ctx)

	key := builder.Build()
	keyString := string(key)
	tableName, ok := builder.TableName()
	if !ok || tableName == "" {
		if t, _, valid := kv.ParseTableRowKey(key); valid {
			tableName = t
		} else {
			tableName = keyString
		}
	}

	q, err := qm.GetQuorum(ctx, tableName)
	if err != nil {
		return err
	}

	resp, err := q.WriteKey(ctx, &consensus.WriteKeyRequest{
		Sender: nil,
		Table:  tableName,
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

func AddOwner(ctx context.Context, builder *kv.KeyBuilder, owner string) error {
	op := &consensus.KVChange{
		Operation: &consensus.KVChange_Acl{
			Acl: &consensus.AclChange{
				Key: builder.Build(),
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
	return sendWrite(ctx, builder, op)
}

func RevokeOwner(ctx context.Context, builder *kv.KeyBuilder, owner string) error {
	op := &consensus.KVChange{
		Operation: &consensus.KVChange_Acl{
			Acl: &consensus.AclChange{
				Key: builder.Build(),
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
	return sendWrite(ctx, builder, op)
}

func AddWriter(ctx context.Context, builder *kv.KeyBuilder, writer string) error {
	op := &consensus.KVChange{
		Operation: &consensus.KVChange_Acl{
			Acl: &consensus.AclChange{
				Key: builder.Build(),
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
	return sendWrite(ctx, builder, op)
}

func RevokeWriter(ctx context.Context, builder *kv.KeyBuilder, writer string) error {
	op := &consensus.KVChange{
		Operation: &consensus.KVChange_Acl{
			Acl: &consensus.AclChange{
				Key: builder.Build(),
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

	return sendWrite(ctx, builder, op)
}

func AddReader(ctx context.Context, builder *kv.KeyBuilder, reader string) error {
	op := &consensus.KVChange{
		Operation: &consensus.KVChange_Acl{
			Acl: &consensus.AclChange{
				Key: builder.Build(),
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

	return sendWrite(ctx, builder, op)
}

func RevokeReader(ctx context.Context, builder *kv.KeyBuilder, reader string) error {
	op := &consensus.KVChange{
		Operation: &consensus.KVChange_Acl{
			Acl: &consensus.AclChange{
				Key: builder.Build(),
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

	return sendWrite(ctx, builder, op)
}

func GetKey(ctx context.Context, builder *kv.KeyBuilder) ([]byte, error) {
	qm := consensus.GetDefaultQuorumManager(ctx)

	key := builder.Build()
	keyString := string(key)
	tableName, ok := builder.TableName()
	if !ok || tableName == "" {
		if t, _, valid := kv.ParseTableRowKey(key); valid {
			tableName = t
		} else {
			tableName = keyString
		}
	}

	q, err := qm.GetQuorum(ctx, tableName)
	if err != nil {
		return nil, err
	}
	resp, err := q.ReadKey(ctx, &consensus.ReadKeyRequest{
		Sender: nil,
		Key:    keyString,
		Table:  tableName,
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
func DeleteKey(ctx context.Context, builder *kv.KeyBuilder) error {
	qm := consensus.GetDefaultQuorumManager(ctx)

	key := builder.Build()
	keyString := string(key)
	tableName, ok := builder.TableName()
	if !ok || tableName == "" {
		if t, _, valid := kv.ParseTableRowKey(key); valid {
			tableName = t
		} else {
			tableName = keyString
		}
	}

	q, err := qm.GetQuorum(ctx, tableName)
	if err != nil {
		return err
	}

	// Reuse WriteKeyRequest shape for quorum-level delete operation
	resp, err := q.DeleteKey(ctx, &consensus.WriteKeyRequest{
		Sender: nil,
		Table:  tableName,
		Value: &consensus.KVChange{
			Operation: &consensus.KVChange_Del{
				Del: &consensus.DelChange{
					Key: builder.Build(),
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
func PrefixScan(ctx context.Context, tablePrefix, rowPrefix string) ([]string, error) {
	// PrefixScan doesn't use table-based quorums since it scans across all keys/tables
	// Instead, we need to directly call the majority quorum's PrefixScan which broadcasts to all nodes
	// For now, use any table to get the quorum (it will use the majority quorum implementation)
	qm := consensus.GetDefaultQuorumManager(ctx)

	// Use a non-empty table name to get a valid quorum object
	// The majority quorum's PrefixScan will broadcast to all nodes regardless of table
	q, err := qm.GetQuorum(ctx, "atlas.nodes")
	if err != nil {
		return nil, err
	}

	if tablePrefix == "" && rowPrefix != "" {
		return nil, fmt.Errorf("row prefix specified without table prefix")
	}

	resp, err := q.PrefixScan(ctx, &consensus.PrefixScanRequest{
		Sender:      nil,
		TablePrefix: tablePrefix,
		RowPrefix:   rowPrefix,
	})
	if err != nil {
		return nil, err
	}

	if resp.Success {
		return resp.Keys, nil
	}
	return nil, fmt.Errorf("prefix scan failed: %s", resp.Error)
}
