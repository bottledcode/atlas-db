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

package consensus

import (
	"context"
	"testing"

	"github.com/bottledcode/atlas-db/atlas/kv"
	"github.com/bottledcode/atlas-db/atlas/options"
	"go.uber.org/zap"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// helper to set up a temporary KV pool and basic table/node
func setupKVForACL(t *testing.T) (cleanup func()) {
	t.Helper()
	data := t.TempDir()
	meta := t.TempDir()
	if err := kv.CreatePool(data, meta); err != nil {
		t.Fatalf("CreatePool: %v", err)
	}

	// Initialize test logger and current node options
	options.Logger = zap.NewNop()
	options.CurrentOptions.ServerId = 1001
	options.CurrentOptions.Region = "local"
	options.CurrentOptions.AdvertiseAddress = "127.0.0.1"
	options.CurrentOptions.AdvertisePort = 9000

	// Insert current node and a table owned by it
	pool := kv.GetPool()
	nr := NewNodeRepository(context.Background(), pool.MetaStore())
	node := &Node{Id: options.CurrentOptions.ServerId, Address: options.CurrentOptions.AdvertiseAddress, Port: int64(options.CurrentOptions.AdvertisePort), Region: &Region{Name: options.CurrentOptions.Region}, Active: true, Rtt: durationpb.New(0)}
	if err := nr.AddNode(node); err != nil {
		t.Fatalf("AddNode: %v", err)
	}

	tr := NewTableRepositoryKV(context.Background(), pool.MetaStore())
	table := &Table{
		Name:              KeyName("user.table"),
		ReplicationLevel:  ReplicationLevel_global,
		Owner:             node,
		CreatedAt:         timestamppb.Now(),
		Version:           1,
		AllowedRegions:    []string{},
		RestrictedRegions: []string{},
		Group:             "",
		Type:              TableType_table,
		ShardPrincipals:   []string{},
	}
	if err := tr.InsertTable(table); err != nil {
		t.Fatalf("InsertTable: %v", err)
	}

	return func() {
		_ = kv.DrainPool()
	}
}
