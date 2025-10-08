package consensus

import (
	"context"
	"testing"

	"github.com/bottledcode/atlas-db/atlas/kv"
	"github.com/bottledcode/atlas-db/atlas/options"
	"go.uber.org/zap"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/proto"
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
		Name:              "user.table",
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

func TestReadKey_ACL_PublicAndOwner(t *testing.T) {
	cleanup := setupKVForACL(t)
	defer cleanup()

	s := &Server{}
	pool := kv.GetPool()

	key := "table:USER:row:ROW"
	table := "user.table"
	// Seed data store with value as a Record protobuf, no ACL => public read
	record := &Record{
		Data: &Record_Value{
			Value: &RawData{Data: []byte("v1")},
		},
	}
	recordBytes, err := proto.Marshal(record)
	if err != nil {
		t.Fatalf("marshal record: %v", err)
	}
	if err := pool.DataStore().Put(context.Background(), []byte(key), recordBytes); err != nil {
		t.Fatalf("seed Put: %v", err)
	}

	// Public read without principal
	if resp, err := s.ReadKey(context.Background(), &ReadKeyRequest{Key: key, Table: table}); err != nil || !resp.GetSuccess() {
		t.Fatalf("ReadKey public failed: resp=%v err=%v", resp, err)
	}

	// Update the record to include ACL for alice as owner
	recordWithACL := &Record{
		Data: &Record_Value{
			Value: &RawData{Data: []byte("v1")},
		},
		AccessControl: &ACL{
			Owners: &ACLData{
				Principals: []string{"alice"},
				CreatedAt:  timestamppb.Now(),
				UpdatedAt:  timestamppb.Now(),
			},
		},
	}
	recordWithACLBytes, err := proto.Marshal(recordWithACL)
	if err != nil {
		t.Fatalf("marshal record with ACL: %v", err)
	}
	if err := pool.DataStore().Put(context.Background(), []byte(key), recordWithACLBytes); err != nil {
		t.Fatalf("update record with ACL: %v", err)
	}

	// Read without principal should be denied
	if resp, _ := s.ReadKey(context.Background(), &ReadKeyRequest{Key: key, Table: table}); resp.GetSuccess() {
		t.Fatalf("expected access denied without principal")
	}

	// Read with correct principal
	ctxAlice := metadata.NewIncomingContext(context.Background(), metadata.Pairs(atlasPrincipalKey, "alice"))
	if resp, err := s.ReadKey(ctxAlice, &ReadKeyRequest{Key: key, Table: table}); err != nil || !resp.GetSuccess() {
		t.Fatalf("ReadKey with owner failed: resp=%v err=%v", resp, err)
	}
}
