package bootstrap

import (
	"bytes"
	"context"
	"crypto/tls"
	"fmt"
	"github.com/bottledcode/atlas-db/atlas"
	"github.com/bottledcode/atlas-db/atlas/consensus"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
	"io"
	"os"
	"zombiezen.com/go/sqlite"
)

func JoinCluster(ctx context.Context) error {
	tlsConfig := &tls.Config{
		InsecureSkipVerify: true,
	}

	creds := credentials.NewTLS(tlsConfig)

	conn, err := atlas.MigrationsPool.Take(ctx)
	if err != nil {
		return err
	}
	defer atlas.MigrationsPool.Put(conn)
	_, err = atlas.ExecuteSQL(ctx, "BEGIN", conn, false)
	if err != nil {
		return err
	}
	defer func() {
		if err != nil {
			_, _ = atlas.ExecuteSQL(ctx, "ROLLBACK", conn, false)
		}
	}()

	// check if we are already in the cluster
	results, err := atlas.ExecuteSQL(ctx, "select id from nodes where address = :address and port = :port", conn, false, atlas.Param{
		Name:  "address",
		Value: atlas.CurrentOptions.AdvertiseAddress,
	}, atlas.Param{
		Name:  "port",
		Value: atlas.CurrentOptions.AdvertisePort,
	})
	if err != nil {
		return err
	}

	if len(results.Rows) > 0 {
		atlas.CurrentOptions.ServerId = results.GetIndex(0).GetColumn("id").GetInt()
		return nil
	}

	// find the current owner of the nodes table
	results, err = atlas.ExecuteSQL(ctx, "select owner_node_id from tables where name = 'atlas.nodes'", conn, false)
	if err != nil {
		return err
	}
	if len(results.Rows) == 0 {
		// todo: this may happen on a new cluster that is forming
		return fmt.Errorf("no owner for table atlas.nodes")
	}

	owner, err := atlas.ExecuteSQL(ctx, "select address, port from nodes where id = :id", conn, false, atlas.Param{
		Name:  "id",
		Value: results.GetIndex(0).GetColumn("owner_node_id").GetInt(),
	})
	if err != nil {
		return err
	}

	nextIds, err := atlas.ExecuteSQL(ctx, "select max(id) + 1 as id from nodes", conn, false)
	if err != nil {
		return err
	}

	_, err = atlas.ExecuteSQL(ctx, "ROLLBACK", conn, false)
	if err != nil {
		return err
	}

	url := owner.GetIndex(0).GetColumn("address").GetString() + ":" + owner.GetIndex(0).GetColumn("port").GetString()

	c, err := grpc.NewClient(url, grpc.WithTransportCredentials(creds), grpc.WithUnaryInterceptor(func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		ctx = metadata.AppendToOutgoingContext(ctx, "Authorization", "Bearer "+atlas.CurrentOptions.ApiKey)
		ctx = metadata.AppendToOutgoingContext(ctx, "Atlas-Service", "Bootstrap")
		return invoker(ctx, method, req, reply, cc, opts...)
	}), grpc.WithStreamInterceptor(func(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn, method string, streamer grpc.Streamer, opts ...grpc.CallOption) (grpc.ClientStream, error) {
		ctx = metadata.AppendToOutgoingContext(ctx, "Authorization", "Bearer "+atlas.CurrentOptions.ApiKey)
		ctx = metadata.AppendToOutgoingContext(ctx, "Atlas-Service", "Bootstrap")
		return streamer(ctx, desc, cc, method, opts...)
	}))
	if err != nil {
		return err
	}
	defer c.Close()

	client := consensus.NewConsensusClient(c)
	result, err := client.JoinCluster(ctx, &consensus.Node{
		Id:      nextIds.GetIndex(0).GetColumn("id").GetInt(),
		Address: atlas.CurrentOptions.AdvertiseAddress,
		Port:    int64(atlas.CurrentOptions.AdvertisePort),
		Region:  &consensus.Region{Name: atlas.CurrentOptions.Region},
		Active:  true,
		Rtt:     durationpb.New(0),
	})

	if err != nil {
		return err
	}

	if !result.GetSuccess() {
		return fmt.Errorf("could not join a cluster")
	}

	atlas.CurrentOptions.ServerId = result.NodeId

	return nil
}

// InitializeMaybe checks if the database is empty and initializes it if it is
func InitializeMaybe(ctx context.Context) error {
	conn, err := atlas.MigrationsPool.Take(ctx)
	if err != nil {
		return err
	}
	defer atlas.MigrationsPool.Put(conn)
	_, err = atlas.ExecuteSQL(ctx, "BEGIN", conn, false)
	if err != nil {
		return err
	}
	defer func() {
		if err != nil {
			_, _ = atlas.ExecuteSQL(ctx, "ROLLBACK", conn, false)
		}
	}()

	// are we dealing with an empty database?
	results, err := atlas.ExecuteSQL(ctx, "select count(*) as c from nodes", conn, false)
	if err != nil {
		return err
	}
	if results.GetIndex(0).GetColumn("c").GetInt() > 0 {
		atlas.Logger.Info("Atlas database is not empty; skipping initialization and continuing normal operations")

		// get our current node id
		results, err = atlas.ExecuteSQL(ctx, "select id from nodes where address = :address and port = :port", conn, false, atlas.Param{
			Name:  "address",
			Value: atlas.CurrentOptions.AdvertiseAddress,
		}, atlas.Param{
			Name:  "port",
			Value: atlas.CurrentOptions.AdvertisePort,
		})

		// this will crash...
		atlas.CurrentOptions.ServerId = results.GetIndex(0).GetColumn("id").GetInt()

		return nil
	}

	if atlas.CurrentOptions.Region == "" {
		atlas.CurrentOptions.Region = "default"
		atlas.Logger.Warn("No region specified, using default region", zap.String("region", atlas.CurrentOptions.Region))
	}

	region := atlas.CurrentOptions.Region

	if atlas.CurrentOptions.AdvertisePort == 0 {
		atlas.CurrentOptions.AdvertisePort = 8080
		atlas.Logger.Warn("No port specified, using the default port", zap.Uint("port", atlas.CurrentOptions.AdvertisePort))
	}

	if atlas.CurrentOptions.AdvertiseAddress == "" {
		atlas.CurrentOptions.AdvertiseAddress = "localhost"
		atlas.Logger.Warn("No address specified, using the default address", zap.String("address", atlas.CurrentOptions.AdvertiseAddress))
	}

	// no nodes exist in the database, so we need to configure things here
	server := consensus.Server{}

	// define the new node:
	node := &consensus.Node{
		Id:      1,
		Address: atlas.CurrentOptions.AdvertiseAddress,
		Region:  &consensus.Region{Name: region},
		Port:    int64(atlas.CurrentOptions.AdvertisePort),
		Active:  true,
		Rtt:     durationpb.New(0),
	}

	table := &consensus.Table{
		Name:              consensus.NodeTable,
		ReplicationLevel:  consensus.ReplicationLevel_global,
		Owner:             node,
		CreatedAt:         timestamppb.Now(),
		Version:           1,
		AllowedRegions:    []string{},
		RestrictedRegions: []string{},
	}

	var steal *consensus.StealTableOwnershipResponse
	steal, err = server.StealTableOwnership(ctx, &consensus.StealTableOwnershipRequest{
		Sender: node,
		Reason: consensus.StealReason_queryReason,
		Table:  table,
	})
	if err != nil {
		return err
	}

	if !steal.GetPromised() {
		return fmt.Errorf("could not steal table ownership")
	}

	_, err = atlas.ExecuteSQL(ctx, "ROLLBACK", conn, false)
	if err != nil {
		return err
	}

	var sess *sqlite.Session
	sess, err = conn.CreateSession("")
	if err != nil {
		return err
	}
	err = sess.Attach("nodes")
	if err != nil {
		return err
	}

	_, err = atlas.ExecuteSQL(ctx, "BEGIN IMMEDIATE", conn, false)
	if err != nil {
		return err
	}

	nextVersion := int64(1)

	for _, missing := range steal.GetSuccess().GetMissingMigrations() {
		nextVersion = missing.GetVersion() + 1
		switch missing.GetMigration().(type) {
		case *consensus.Migration_Data:
			for _, data := range missing.GetData().GetSession() {
				reader := bytes.NewReader(data)
				err = conn.ApplyChangeset(reader, nil, func(conflictType sqlite.ConflictType, iterator *sqlite.ChangesetIterator) sqlite.ConflictAction {
					return sqlite.ChangesetReplace
				})
				if err != nil {
					return err
				}
			}
		case *consensus.Migration_Schema:
			for _, command := range missing.GetSchema().GetCommands() {
				var stmt *sqlite.Stmt
				stmt, _, err = conn.PrepareTransient(command)
				if err != nil {
					return err
				}
				_, err = stmt.Step()
				if err != nil {
					return err
				}
				err = stmt.Finalize()
				if err != nil {
					return err
				}
			}
		}
	}

	_, err = atlas.ExecuteSQL(ctx, `
insert into nodes (id, address, port, region, active, created_at, rtt)
values (:id, :address, :port, :region, 1, current_timestamp, 0) on conflict do UPDATE
SET address = :address, port = :port, region = :region, active = 1
`, conn, false, atlas.Param{
		Name:  "id",
		Value: node.Id,
	}, atlas.Param{
		Name:  "address",
		Value: node.Address,
	}, atlas.Param{
		Name:  "port",
		Value: node.Port,
	}, atlas.Param{
		Name:  "region",
		Value: region,
	})
	if err != nil {
		return err
	}

	var sessData bytes.Buffer
	err = sess.WritePatchset(&sessData)

	_, err = atlas.ExecuteSQL(ctx, "rollback", conn, false)
	if err != nil {
		return err
	}
	sess.Delete()

	// we are not outside the transaction and we can commit the migration

	// now we exclusively own the table in our single node cluster...
	migration := &consensus.WriteMigrationRequest{
		TableId:      consensus.NodeTable,
		TableVersion: 1,
		Sender:       node,
		Migration: &consensus.Migration{
			TableId: consensus.NodeTable,
			Version: nextVersion,
			Migration: &consensus.Migration_Data{
				Data: &consensus.DataMigration{
					Session: [][]byte{sessData.Bytes()},
				},
			},
		},
	}

	// err intentionally shadowed here to prevent a rollback outside the transaction
	writeMigrationResponse, err := server.WriteMigration(ctx, migration)
	if err != nil {
		return err
	}

	if !writeMigrationResponse.GetSuccess() {
		return fmt.Errorf("could not write migration")
	}

	_, err = server.AcceptMigration(ctx, migration)
	if err != nil {
		return err
	}

	atlas.CurrentOptions.ServerId = node.Id

	return err
}

// DoBootstrap connects to the bootstrap server and writes the data to the meta file
func DoBootstrap(ctx context.Context, url string, metaFilename string) error {

	atlas.Logger.Info("Connecting to bootstrap server", zap.String("url", url))

	tlsConfig := &tls.Config{
		InsecureSkipVerify: true,
	}

	creds := credentials.NewTLS(tlsConfig)

	conn, err := grpc.NewClient(url, grpc.WithTransportCredentials(creds), grpc.WithUnaryInterceptor(func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		ctx = metadata.AppendToOutgoingContext(ctx, "Authorization", "Bearer "+atlas.CurrentOptions.ApiKey)
		ctx = metadata.AppendToOutgoingContext(ctx, "Atlas-Service", "Bootstrap")
		return invoker(ctx, method, req, reply, cc, opts...)
	}), grpc.WithStreamInterceptor(func(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn, method string, streamer grpc.Streamer, opts ...grpc.CallOption) (grpc.ClientStream, error) {
		ctx = metadata.AppendToOutgoingContext(ctx, "Authorization", "Bearer "+atlas.CurrentOptions.ApiKey)
		ctx = metadata.AppendToOutgoingContext(ctx, "Atlas-Service", "Bootstrap")
		return streamer(ctx, desc, cc, method, opts...)
	}))
	if err != nil {
		return err
	}
	defer conn.Close()

	client := NewBootstrapClient(conn)
	resp, err := client.GetBootstrapData(ctx, &BootstrapRequest{
		Version: 1,
	})
	if err != nil {
		return err
	}

	// delete the wal and shm files
	os.Remove(metaFilename + "-wal")
	os.Remove(metaFilename + "-shm")

	// write the data to the meta file
	f, err := os.Create(metaFilename)
	if err != nil {
		return err
	}
	defer f.Close()

	for {
		chunk, err := resp.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}

		if chunk.GetIncompatibleVersion() != nil {
			return fmt.Errorf("incompatible version: needs version %d", chunk.GetIncompatibleVersion().NeedsVersion)
		}

		data := chunk.GetBootstrapData().GetData()
		if len(data) == 0 {
			break
		}
	writeRest:
		n, err := f.Write(data)
		if err != nil {
			return err
		}
		if n != len(data) {
			data = data[n:]
			goto writeRest
		}
	}

	// we are now ready to connect to the database
	atlas.CreatePool(atlas.CurrentOptions)
	m, err := atlas.MigrationsPool.Take(ctx)
	if err != nil {
		return err
	}
	defer atlas.MigrationsPool.Put(m)

	return nil
}
