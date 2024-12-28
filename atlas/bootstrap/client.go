package bootstrap

import (
	"context"
	"crypto/tls"
	"fmt"
	"github.com/bottledcode/atlas-db/atlas"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/metadata"
	"io"
	"os"
)

// InitializeMaybe checks if the database is empty and initializes it if it is
func InitializeMaybe() error {
	ctx := context.Background()

	conn, err := atlas.MigrationsPool.Take(ctx)
	if err != nil {
		return err
	}
	defer atlas.MigrationsPool.Put(conn)
	_, err = atlas.ExecuteSQL(ctx, "BEGIN IMMEDIATE", conn, false)
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
	if results.GetIndex(0).GetColumn("c").GetInt() == 0 {
		// see if there is a region
		regionId, err := atlas.GetOrAddRegion(ctx, conn, atlas.CurrentOptions.Region)
		if err != nil {
			return err
		}

		if atlas.CurrentOptions.Region == "" {
			atlas.Logger.Warn("No region specified, using default region", zap.String("region", atlas.CurrentOptions.Region))
			atlas.CurrentOptions.Region = "default"
		}

		if atlas.CurrentOptions.AdvertisePort == 0 {
			atlas.Logger.Warn("No port specified, using default port", zap.Uint("port", atlas.CurrentOptions.AdvertisePort))
			atlas.CurrentOptions.AdvertisePort = 8080
		}

		if atlas.CurrentOptions.AdvertiseAddress == "" {
			atlas.Logger.Warn("No address specified, using default address", zap.String("address", atlas.CurrentOptions.AdvertiseAddress))
			atlas.CurrentOptions.AdvertiseAddress = "localhost"
		}

		// No nodes currently exist, and we didn't bootstrap. So, start writing!
		_, err = atlas.ExecuteSQL(ctx, "insert into nodes (address, port, region_id) values (:address, :port, :region)", conn, false, atlas.Param{
			Name:  "address",
			Value: atlas.CurrentOptions.AdvertiseAddress,
		}, atlas.Param{
			Name:  "port",
			Value: atlas.CurrentOptions.AdvertisePort,
		}, atlas.Param{
			Name:  "region",
			Value: regionId,
		})
	}

	_, err = atlas.ExecuteSQL(ctx, "COMMIT", conn, false)
	return err
}

// DoBootstrap connects to the bootstrap server and writes the data to the meta file
func DoBootstrap(url string, metaFilename string) error {

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
	resp, err := client.GetBootstrapData(context.Background(), &BootstrapRequest{
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

		// todo: proper way of doing this
		if _, err := f.Write(chunk.GetBootstrapData().Data); err != nil {
			return err
		}
	}

	return nil
}
