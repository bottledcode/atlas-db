package bootstrap

import (
	"context"
	"crypto/tls"
	"github.com/bottledcode/atlas-db/atlas"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/metadata"
	"os"
)

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

	err = os.WriteFile(metaFilename, resp.GetBootstrapData().GetData(), 0644)
	if err != nil {
		return err
	}
	// delete the wal and shm files
	os.Remove(metaFilename + "-wal")
	os.Remove(metaFilename + "-shm")

	return nil
}
