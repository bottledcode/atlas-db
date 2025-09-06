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
	qm := consensus.GetDefaultQuorumManager(ctx)

	key := builder.Build()
	keyString := string(key)

	q, err := qm.GetQuorum(ctx, keyString)
	if err != nil {
		return err
	}
	resp, err := q.WriteKey(ctx, &consensus.WriteKeyRequest{
		Sender: nil,
		Key:    keyString,
		Table:  keyString,
		Value:  value,
	})
	if err != nil {
		return err
	}
	if resp.Success {
		return nil
	}
	return fmt.Errorf("write failed: %s", resp.Error)
}

func GetKey(ctx context.Context, builder *kv.KeyBuilder) ([]byte, error) {
	qm := consensus.GetDefaultQuorumManager(ctx)

	key := builder.Build()
	keyString := string(key)

	q, err := qm.GetQuorum(ctx, keyString)
	if err != nil {
		return nil, err
	}
	resp, err := q.ReadKey(ctx, &consensus.ReadKeyRequest{
		Sender: nil,
		Key:    keyString,
		Table:  keyString,
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

	q, err := qm.GetQuorum(ctx, keyString)
	if err != nil {
		return err
	}

	// Reuse WriteKeyRequest shape for quorum-level delete operation
	resp, err := q.DeleteKey(ctx, &consensus.WriteKeyRequest{
		Sender: nil,
		Key:    keyString,
		Table:  keyString,
	})
	if err != nil {
		return err
	}
	if resp.Success {
		return nil
	}
	return fmt.Errorf("delete failed: %s", resp.Error)
}
