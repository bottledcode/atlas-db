package consensus

import (
	"context"
	"crypto/tls"
	"github.com/bottledcode/atlas-db/atlas"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/metadata"
)

func getNewClient(url string) (ConsensusClient, error, func()) {
	tlsConfig := &tls.Config{
		InsecureSkipVerify: true,
	}

	creds := credentials.NewTLS(tlsConfig)

	conn, err := grpc.NewClient(url, grpc.WithTransportCredentials(creds), grpc.WithUnaryInterceptor(func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		ctx = metadata.AppendToOutgoingContext(ctx, "Authorization", "Bearer "+atlas.CurrentOptions.ApiKey)
		ctx = metadata.AppendToOutgoingContext(ctx, "Atlas-Service", "Consensus")
		return invoker(ctx, method, req, reply, cc, opts...)
	}), grpc.WithStreamInterceptor(func(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn, method string, streamer grpc.Streamer, opts ...grpc.CallOption) (grpc.ClientStream, error) {
		ctx = metadata.AppendToOutgoingContext(ctx, "Authorization", "Bearer "+atlas.CurrentOptions.ApiKey)
		ctx = metadata.AppendToOutgoingContext(ctx, "Atlas-Service", "Consensus")
		return streamer(ctx, desc, cc, method, opts...)
	}))
	if err != nil {
		return nil, err, nil
	}

	client := NewConsensusClient(conn)
	return client, nil, func() {
		_ = conn.Close()
	}
}
