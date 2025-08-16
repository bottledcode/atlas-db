#!/bin/bash

# Atlas DB build script

set -e

echo "Building Atlas DB..."

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Function to print colored output
print_status() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Check if Go is installed
if ! command -v go &> /dev/null; then
    print_error "Go is not installed. Please install Go 1.21 or later."
    exit 1
fi

# Check Go version
GO_VERSION=$(go version | awk '{print $3}' | sed 's/go//')
REQUIRED_VERSION="1.21"

if ! printf '%s\n%s\n' "$REQUIRED_VERSION" "$GO_VERSION" | sort -V -C; then
    print_error "Go version $GO_VERSION is too old. Please upgrade to Go $REQUIRED_VERSION or later."
    exit 1
fi

print_status "Go version $GO_VERSION is compatible"

# Check if protoc is installed
if ! command -v protoc &> /dev/null; then
    print_warning "Protocol Buffers compiler (protoc) is not installed."
    print_warning "Please install protoc to generate protocol buffer files."
    print_warning "On Ubuntu/Debian: sudo apt-get install protobuf-compiler"
    print_warning "On macOS: brew install protobuf"
else
    print_status "Protocol Buffers compiler found"
fi

# Create build directory
mkdir -p build

# Install Go dependencies
print_status "Installing Go dependencies..."
go mod download

# Generate protocol buffer files if protoc is available
if command -v protoc &> /dev/null; then
    print_status "Generating protocol buffer files..."
    chmod +x scripts/generate-proto.sh
    ./scripts/generate-proto.sh
else
    print_warning "Skipping protocol buffer generation (protoc not found)"
fi

# Build the main binary
print_status "Building Atlas DB server..."
go build -ldflags="-s -w" -o build/atlas-db ./cmd/atlas-db

if [ $? -eq 0 ]; then
    print_status "Build completed successfully!"
    print_status "Binary location: build/atlas-db"
    
    # Make the binary executable
    chmod +x build/atlas-db
    
    echo ""
    echo "To run Atlas DB:"
    echo "  ./build/atlas-db start --config config/atlas.yaml"
    echo ""
    echo "For help:"
    echo "  ./build/atlas-db --help"
else
    print_error "Build failed!"
    exit 1
fi

# Build examples if requested
if [ "$1" = "--with-examples" ]; then
    print_status "Building examples..."
    go build -o build/client-example ./examples/client_example.go
    print_status "Example client built: build/client-example"
fi

print_status "Build process completed!"