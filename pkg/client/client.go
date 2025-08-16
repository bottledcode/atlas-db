package client

import (
	"context"
	"fmt"
	"io"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/withinboredom/atlas-db-2/proto/atlas"
)

// Client represents the Atlas DB client interface
type Client interface {
	Get(ctx context.Context, key string, opts ...GetOption) (*GetResult, error)
	Put(ctx context.Context, key string, value []byte, opts ...PutOption) (*PutResult, error)
	Delete(ctx context.Context, key string, opts ...DeleteOption) error
	Scan(ctx context.Context, opts ...ScanOption) (*ScanResult, error)
	Batch(ctx context.Context, ops ...BatchOp) (*BatchResult, error)
	Close() error
}

type client struct {
	conn   *grpc.ClientConn
	grpc   atlas.AtlasDBClient
	mu     sync.RWMutex
	closed bool
}

// GetResult represents the result of a Get operation
type GetResult struct {
	Value   []byte
	Found   bool
	Version int64
}

// PutResult represents the result of a Put operation
type PutResult struct {
	Version int64
}

// ScanResult represents the result of a Scan operation
type ScanResult struct {
	Items []*ScanItem
}

// ScanItem represents a single item in scan results
type ScanItem struct {
	Key     string
	Value   []byte
	Version int64
}

// BatchResult represents the result of a Batch operation
type BatchResult struct {
	Results []*BatchOpResult
}

// BatchOpResult represents the result of a single batch operation
type BatchOpResult struct {
	Success bool
	Value   []byte
	Version int64
	Error   string
}

// Connect creates a new Atlas DB client connection
func Connect(address string, opts ...ConnectOption) (Client, error) {
	config := &connectConfig{
		timeout: 10 * time.Second,
	}

	for _, opt := range opts {
		opt(config)
	}

	ctx, cancel := context.WithTimeout(context.Background(), config.timeout)
	defer cancel()

	conn, err := grpc.DialContext(ctx, address, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, fmt.Errorf("failed to connect to Atlas DB at %s: %w", address, err)
	}

	return &client{
		conn: conn,
		grpc: atlas.NewAtlasDBClient(conn),
	}, nil
}

func (c *client) Get(ctx context.Context, key string, opts ...GetOption) (*GetResult, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if c.closed {
		return nil, fmt.Errorf("client is closed")
	}

	config := &getConfig{}
	for _, opt := range opts {
		opt(config)
	}

	req := &atlas.GetRequest{
		Key:            key,
		ConsistentRead: config.consistentRead,
	}

	resp, err := c.grpc.Get(ctx, req)
	if err != nil {
		return nil, fmt.Errorf("get operation failed: %w", err)
	}

	return &GetResult{
		Value:   resp.Value,
		Found:   resp.Found,
		Version: resp.Version,
	}, nil
}

func (c *client) Put(ctx context.Context, key string, value []byte, opts ...PutOption) (*PutResult, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if c.closed {
		return nil, fmt.Errorf("client is closed")
	}

	config := &putConfig{}
	for _, opt := range opts {
		opt(config)
	}

	req := &atlas.PutRequest{
		Key:             key,
		Value:           value,
		ExpectedVersion: config.expectedVersion,
	}

	resp, err := c.grpc.Put(ctx, req)
	if err != nil {
		return nil, fmt.Errorf("put operation failed: %w", err)
	}

	if !resp.Success {
		return nil, fmt.Errorf("put operation failed: %s", resp.Error)
	}

	return &PutResult{
		Version: resp.Version,
	}, nil
}

func (c *client) Delete(ctx context.Context, key string, opts ...DeleteOption) error {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if c.closed {
		return fmt.Errorf("client is closed")
	}

	config := &deleteConfig{}
	for _, opt := range opts {
		opt(config)
	}

	req := &atlas.DeleteRequest{
		Key:             key,
		ExpectedVersion: config.expectedVersion,
	}

	resp, err := c.grpc.Delete(ctx, req)
	if err != nil {
		return fmt.Errorf("delete operation failed: %w", err)
	}

	if !resp.Success {
		return fmt.Errorf("delete operation failed: %s", resp.Error)
	}

	return nil
}

func (c *client) Scan(ctx context.Context, opts ...ScanOption) (*ScanResult, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if c.closed {
		return nil, fmt.Errorf("client is closed")
	}

	config := &scanConfig{
		limit: 100, // default limit
	}
	for _, opt := range opts {
		opt(config)
	}

	req := &atlas.ScanRequest{
		StartKey: config.startKey,
		EndKey:   config.endKey,
		Limit:    int32(config.limit),
		Reverse:  config.reverse,
	}

	stream, err := c.grpc.Scan(ctx, req)
	if err != nil {
		return nil, fmt.Errorf("scan operation failed: %w", err)
	}

	var items []*ScanItem
	for {
		resp, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("scan stream error: %w", err)
		}

		items = append(items, &ScanItem{
			Key:     resp.Key,
			Value:   resp.Value,
			Version: resp.Version,
		})
	}

	return &ScanResult{Items: items}, nil
}

func (c *client) Batch(ctx context.Context, ops ...BatchOp) (*BatchResult, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if c.closed {
		return nil, fmt.Errorf("client is closed")
	}

	operations := make([]*atlas.Operation, len(ops))
	for i, op := range ops {
		operations[i] = op.toProto()
	}

	req := &atlas.BatchRequest{
		Operations: operations,
	}

	resp, err := c.grpc.Batch(ctx, req)
	if err != nil {
		return nil, fmt.Errorf("batch operation failed: %w", err)
	}

	results := make([]*BatchOpResult, len(resp.Results))
	for i, result := range resp.Results {
		results[i] = &BatchOpResult{
			Success: result.Success,
			Value:   result.Value,
			Version: result.Version,
			Error:   result.Error,
		}
	}

	return &BatchResult{Results: results}, nil
}

func (c *client) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.closed {
		return nil
	}

	c.closed = true
	return c.conn.Close()
}

// Options and configuration

type connectConfig struct {
	timeout time.Duration
}

type ConnectOption func(*connectConfig)

func WithTimeout(timeout time.Duration) ConnectOption {
	return func(c *connectConfig) {
		c.timeout = timeout
	}
}

type getConfig struct {
	consistentRead bool
}

type GetOption func(*getConfig)

func WithConsistentRead() GetOption {
	return func(c *getConfig) {
		c.consistentRead = true
	}
}

type putConfig struct {
	expectedVersion int64
}

type PutOption func(*putConfig)

func WithExpectedVersion(version int64) PutOption {
	return func(c *putConfig) {
		c.expectedVersion = version
	}
}

type deleteConfig struct {
	expectedVersion int64
}

type DeleteOption func(*deleteConfig)

func WithExpectedVersionForDelete(version int64) DeleteOption {
	return func(c *deleteConfig) {
		c.expectedVersion = version
	}
}

type scanConfig struct {
	startKey string
	endKey   string
	limit    int
	reverse  bool
}

type ScanOption func(*scanConfig)

func WithStartKey(key string) ScanOption {
	return func(c *scanConfig) {
		c.startKey = key
	}
}

func WithEndKey(key string) ScanOption {
	return func(c *scanConfig) {
		c.endKey = key
	}
}

func WithLimit(limit int) ScanOption {
	return func(c *scanConfig) {
		c.limit = limit
	}
}

func WithReverse() ScanOption {
	return func(c *scanConfig) {
		c.reverse = true
	}
}

// Batch operations

type BatchOp interface {
	toProto() *atlas.Operation
}

type GetOp struct {
	Key string
}

func (g GetOp) toProto() *atlas.Operation {
	return &atlas.Operation{
		Type: atlas.Operation_GET,
		Key:  g.Key,
	}
}

type PutOp struct {
	Key             string
	Value           []byte
	ExpectedVersion int64
}

func (p PutOp) toProto() *atlas.Operation {
	return &atlas.Operation{
		Type:            atlas.Operation_PUT,
		Key:             p.Key,
		Value:           p.Value,
		ExpectedVersion: p.ExpectedVersion,
	}
}

type DeleteOp struct {
	Key             string
	ExpectedVersion int64
}

func (d DeleteOp) toProto() *atlas.Operation {
	return &atlas.Operation{
		Type:            atlas.Operation_DELETE,
		Key:             d.Key,
		ExpectedVersion: d.ExpectedVersion,
	}
}

// Convenience constructors for batch operations

func Get(key string) BatchOp {
	return GetOp{Key: key}
}

func Put(key string, value []byte) BatchOp {
	return PutOp{Key: key, Value: value}
}

func PutWithVersion(key string, value []byte, version int64) BatchOp {
	return PutOp{Key: key, Value: value, ExpectedVersion: version}
}

func Delete(key string) BatchOp {
	return DeleteOp{Key: key}
}

func DeleteWithVersion(key string, version int64) BatchOp {
	return DeleteOp{Key: key, ExpectedVersion: version}
}