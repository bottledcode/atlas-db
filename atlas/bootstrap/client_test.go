package bootstrap_test

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"github.com/bottledcode/atlas-db/atlas"
	"go.uber.org/zap"
	"io"
	"math/big"
	"os"
	"testing"
	"time"

	"github.com/bottledcode/atlas-db/atlas/bootstrap"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
)

type mockBootstrapServer struct {
	bootstrap.UnimplementedBootstrapServer
}

func generateSelfSignedCert() tls.Certificate {
	priv, _ := rsa.GenerateKey(rand.Reader, 2048)
	template := x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject: pkix.Name{
			Organization: []string{"Test Org"},
		},
		NotBefore:             time.Now(),
		NotAfter:              time.Now().Add(365 * 24 * time.Hour),
		KeyUsage:              x509.KeyUsageKeyEncipherment | x509.KeyUsageDigitalSignature,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		BasicConstraintsValid: true,
	}

	derBytes, _ := x509.CreateCertificate(rand.Reader, &template, &template, &priv.PublicKey, priv)
	cert := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: derBytes})
	key := pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(priv)})

	tlsCert, _ := tls.X509KeyPair(cert, key)
	return tlsCert
}

func (m *mockBootstrapServer) GetBootstrapData(req *bootstrap.BootstrapRequest, stream bootstrap.Bootstrap_GetBootstrapDataServer) error {
	if req.GetVersion() != 1 {
		return stream.Send(&bootstrap.BootstrapResponse{
			Response: &bootstrap.BootstrapResponse_IncompatibleVersion{
				IncompatibleVersion: &bootstrap.IncompatibleVersion{
					NeedsVersion: 1,
				},
			},
		})
	}

	data := []byte("test data")
	for i := 0; i < 3; i++ {
		if err := stream.Send(&bootstrap.BootstrapResponse{
			Response: &bootstrap.BootstrapResponse_BootstrapData{
				BootstrapData: &bootstrap.BootstrapData{
					Data: data,
				},
			},
		}); err != nil {
			return err
		}
	}
	return nil
}

func startMockServer(t *testing.T) (string, func()) {
	if atlas.Logger == nil {
		atlas.Logger, _ = zap.NewDevelopment()
	}

	lis, err := tls.Listen("tcp", "localhost:0", &tls.Config{
		Certificates:       []tls.Certificate{generateSelfSignedCert()},
		InsecureSkipVerify: true,
		NextProtos:         []string{"h2", "http/1.1"},
	})
	require.NoError(t, err)

	s := grpc.NewServer()
	bootstrap.RegisterBootstrapServer(s, &mockBootstrapServer{})

	errchan := make(chan error)
	go func() {
		if err := s.Serve(lis); err != nil {
			errchan <- err
		}
	}()

	select {
	case err := <-errchan:
		require.NoError(t, err)
	case <-time.After(100 * time.Millisecond):
		// server started successfully
	}

	return lis.Addr().String(), func() {
		s.Stop()
		lis.Close()
	}
}

func TestDoBootstrap(t *testing.T) {
	serverAddr, cleanup := startMockServer(t)
	defer cleanup()

	metaFilename := "test_meta.db"
	defer os.Remove(metaFilename)

	err := bootstrap.DoBootstrap(serverAddr, metaFilename)
	require.NoError(t, err)

	file, err := os.Open(metaFilename)
	require.NoError(t, err)
	defer file.Close()

	data, err := io.ReadAll(file)
	require.NoError(t, err)
	require.Equal(t, []byte("test datatest datatest data"), data)
}

func getTempDb(t *testing.T) (string, func()) {
	f, err := os.CreateTemp("", "initialize-maybe*")
	require.NoError(t, err)
	f.Close()
	return f.Name(), func() {
		os.Remove(f.Name())
		os.Remove(f.Name() + "-wal")
		os.Remove(f.Name() + "-shm")
	}
}

func TestInitializeMaybe(t *testing.T) {
	f, cleanup := getTempDb(t)
	defer cleanup()
	m, cleanup2 := getTempDb(t)
	defer cleanup2()
	atlas.CreatePool(&atlas.Options{
		DbFilename:   f,
		MetaFilename: m,
	})
	ctx := context.Background()

	// Mock the database state
	conn, err := atlas.MigrationsPool.Take(ctx)
	require.NoError(t, err)
	defer atlas.MigrationsPool.Put(conn)

	_, err = atlas.ExecuteSQL(ctx, "DELETE FROM nodes", conn, false)
	require.NoError(t, err)

	// Test with an empty database
	err = bootstrap.InitializeMaybe()
	require.NoError(t, err)

	results, err := atlas.ExecuteSQL(ctx, "SELECT count(*) as c FROM nodes", conn, false)
	require.NoError(t, err)
	require.Equal(t, int64(1), results.GetIndex(0).GetColumn("c").GetInt())

	// Test with a non-empty database
	err = bootstrap.InitializeMaybe()
	require.NoError(t, err)

	results, err = atlas.ExecuteSQL(ctx, "SELECT count(*) FROM nodes", conn, false)
	require.NoError(t, err)
	require.Equal(t, int64(1), results.GetIndex(0).GetColumn("count(*)").GetInt())
}
