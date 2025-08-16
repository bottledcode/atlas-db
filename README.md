# Atlas DB

Atlas DB is a distributed database written in Go that provides global replication using the W-Paxos consensus algorithm while optimizing writes for local regions. It uses BadgerDB as the underlying key-value store and gRPC for inter-node communication.

## Features

- **W-Paxos Consensus**: Implements the Wide-area Paxos algorithm for global consistency with regional optimization
- **Regional Write Optimization**: Prioritizes local region writes for low latency while maintaining global consistency
- **BadgerDB Storage**: Fast, embedded key-value storage with ACID guarantees
- **gRPC Communication**: High-performance inter-node communication
- **Go Library Interface**: Use as a library in Go applications or as a standalone server
- **Configurable**: Extensive configuration options for different deployment scenarios

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                        Atlas DB                            │
├─────────────────────────────────────────────────────────────┤
│  Client API (Go Library)                                   │
├─────────────────────────────────────────────────────────────┤
│  gRPC Transport Layer                                       │
├─────────────────────────────────────────────────────────────┤
│  Regional Write Optimizer                                   │
├─────────────────────────────────────────────────────────────┤
│  W-Paxos Consensus Engine                                   │
├─────────────────────────────────────────────────────────────┤
│  BadgerDB Storage Engine                                    │
└─────────────────────────────────────────────────────────────┘
```

## Quick Start

### Prerequisites

- Go 1.21 or later
- Protocol Buffers compiler (`protoc`)

### Installation

1. Clone the repository:
```bash
git clone https://github.com/bottledcode/atlas-db.git
cd atlas-db-2
```

2. Install dependencies:
```bash
go mod download
```

3. Generate protocol buffer files:
```bash
./scripts/generate-proto.sh
```

4. Build the server:
```bash
go build -o atlas-db ./cmd/atlas-db
```

### Running the Server

1. Create a configuration file (see `config/atlas.yaml` for example):
```yaml
node:
  id: "atlas-node-1"
  address: "localhost:8080"
  data_dir: "./data"
  region_id: 1

# ... other configuration options
```

2. Start the server:
```bash
./atlas-db start --config config/atlas.yaml
```

Or with command line options:
```bash
./atlas-db start --node-id node1 --address localhost:8080 --region-id 1 --data-dir ./data
```

### Using the Go Client Library

```go
package main

import (
    "context"
    "log"
    
    "github.com/bottledcode/atlas-db/pkg/client"
)

func main() {
    // Connect to Atlas DB
    db, err := client.Connect("localhost:8080")
    if err != nil {
        log.Fatal(err)
    }
    defer db.Close()
    
    ctx := context.Background()
    
    // Put a value
    result, err := db.Put(ctx, "key1", []byte("value1"))
    if err != nil {
        log.Fatal(err)
    }
    
    // Get a value
    value, err := db.Get(ctx, "key1")
    if err != nil {
        log.Fatal(err)
    }
    
    if value.Found {
        log.Printf("Found: %s", string(value.Value))
    }
}
```

## Configuration

Atlas DB supports configuration via YAML files, environment variables, and command line flags.

### Configuration File

See `config/atlas.yaml` for a complete example with all available options.

### Environment Variables

All configuration options can be set via environment variables with the `ATLAS_` prefix:

- `ATLAS_NODE_ID`: Node identifier
- `ATLAS_NODE_ADDRESS`: Server bind address
- `ATLAS_REGION_ID`: Region identifier
- `ATLAS_DATA_DIR`: Data directory path
- `ATLAS_LOG_LEVEL`: Logging level (debug, info, warn, error)

### Command Line Flags

```bash
./atlas-db start --help
```

## Project Structure

```
atlas-db-2/
├── cmd/atlas-db/           # Main server executable
├── pkg/
│   ├── client/             # Go client library
│   ├── config/             # Configuration management
│   ├── consensus/          # W-Paxos implementation
│   ├── storage/            # BadgerDB wrapper
│   └── transport/          # gRPC communication
├── internal/
│   ├── server/             # Server implementation
│   └── region/             # Regional optimization
├── proto/                  # Protocol buffer definitions
├── config/                 # Example configurations
├── examples/               # Usage examples
└── scripts/                # Build scripts
```

## API Operations

### Database Operations

- `Get(key)`: Retrieve a value by key
- `Put(key, value)`: Store a key-value pair
- `Delete(key)`: Remove a key
- `Scan(startKey, endKey)`: Range scan
- `Batch(operations)`: Execute multiple operations atomically

### Options

- **Consistent Reads**: Force reads to go through consensus
- **Conditional Updates**: Updates with expected version checking
- **Regional Strategies**: Configure read/write behavior per region

## W-Paxos Algorithm

Atlas DB implements the W-Paxos (Wide-area Paxos) consensus algorithm, which provides:

1. **Fast Path**: Local region majority for low latency
2. **Slow Path**: Global majority for consistency
3. **Automatic Fallback**: Graceful degradation when fast path fails

### Phases

1. **Prepare Phase**: Proposer contacts acceptors for promises
2. **Accept Phase**: Proposer sends accept requests with values
3. **Commit Phase**: Notify all nodes of committed values

## Regional Optimization

The regional optimizer provides multiple strategies:

### Write Strategies

- `local`: Prefer local region writes (eventually consistent)
- `global`: Use consensus for all writes (strongly consistent)

### Read Strategies

- `local`: Read from local region only
- `nearest`: Read from nearest available region
- `consistent`: Read through consensus for strong consistency

## Monitoring and Operations

### Statistics

```go
stats, err := server.GetStats()
```

### Health Checks

The server provides health check endpoints via gRPC.

### Garbage Collection

Automatic BadgerDB garbage collection runs periodically based on configuration.

## Development

### Building from Source

```bash
# Install dependencies
go mod download

# Generate protocol buffers
./scripts/generate-proto.sh

# Run tests
go test ./...

# Build
go build ./cmd/atlas-db
```

### Running Tests

```bash
go test -v ./...
```

### Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests
5. Submit a pull request

## Examples

See the `examples/` directory for:

- Basic client usage
- Batch operations
- Configuration examples
- Multi-node cluster setup

## Performance

Atlas DB is designed for:

- **Low Latency**: Local region writes complete in microseconds
- **High Availability**: Continues operating with region failures
- **Scalability**: Horizontal scaling across regions
- **Consistency**: Configurable consistency levels

## Support

- Issues: GitHub Issues
- Documentation: See docs/ directory
- Examples: See examples/ directory