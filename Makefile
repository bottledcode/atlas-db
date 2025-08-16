# Atlas DB Makefile

.PHONY: all build test clean proto deps install run

# Build variables
BINARY_NAME=atlas-db
BUILD_DIR=build
MAIN_PATH=./cmd/atlas-db

# Go parameters
GOCMD=go
GOBUILD=$(GOCMD) build
GOCLEAN=$(GOCMD) clean
GOTEST=$(GOCMD) test
GOGET=$(GOCMD) get
GOMOD=$(GOCMD) mod

# Default target
all: deps proto build

# Install dependencies
deps:
	$(GOMOD) download
	$(GOMOD) tidy

# Generate protocol buffer files
proto:
	@echo "Generating protocol buffer files..."
	@chmod +x scripts/generate-proto.sh
	@./scripts/generate-proto.sh

# Build the binary
build: proto
	@echo "Building $(BINARY_NAME)..."
	@mkdir -p $(BUILD_DIR)
	$(GOBUILD) -o $(BUILD_DIR)/$(BINARY_NAME) $(MAIN_PATH)

# Build for multiple platforms
build-all: proto
	@echo "Building for multiple platforms..."
	@mkdir -p $(BUILD_DIR)
	GOOS=linux GOARCH=amd64 $(GOBUILD) -o $(BUILD_DIR)/$(BINARY_NAME)-linux-amd64 $(MAIN_PATH)
	GOOS=darwin GOARCH=amd64 $(GOBUILD) -o $(BUILD_DIR)/$(BINARY_NAME)-darwin-amd64 $(MAIN_PATH)
	GOOS=windows GOARCH=amd64 $(GOBUILD) -o $(BUILD_DIR)/$(BINARY_NAME)-windows-amd64.exe $(MAIN_PATH)

# Run tests
test:
	@echo "Running test suite..."
	@chmod +x scripts/run-tests.sh
	@./scripts/run-tests.sh

# Run tests with race detection
test-race:
	@echo "Running tests with race detection..."
	@chmod +x scripts/run-tests.sh
	@./scripts/run-tests.sh --race

# Run tests with coverage
test-coverage:
	@echo "Running tests with coverage analysis..."
	@chmod +x scripts/run-tests.sh
	@./scripts/run-tests.sh --coverage

# Run benchmarks
test-bench:
	@echo "Running benchmarks..."
	@chmod +x scripts/run-tests.sh
	@./scripts/run-tests.sh --bench

# Run complete test suite
test-all:
	@echo "Running complete test suite..."
	@chmod +x scripts/run-tests.sh
	@./scripts/run-tests.sh --all

# Quick unit tests only
test-unit:
	$(GOTEST) -v ./pkg/... ./internal/...

# Install the binary to GOPATH/bin
install: build
	$(GOCMD) install $(MAIN_PATH)

# Run the application with default config
run: build
	$(BUILD_DIR)/$(BINARY_NAME) start --config config/atlas.yaml

# Run with custom parameters
run-dev: build
	$(BUILD_DIR)/$(BINARY_NAME) start --node-id dev-node --address localhost:8080 --region-id 1 --data-dir ./dev-data

# Clean build artifacts
clean:
	$(GOCLEAN)
	rm -rf $(BUILD_DIR)
	rm -f coverage.out coverage.html

# Format code
fmt:
	$(GOCMD) fmt ./...

# Lint code (requires golangci-lint)
lint:
	golangci-lint run

# Security scan (requires gosec)
security:
	gosec ./...

# Generate mocks (requires gomock)
mocks:
	@echo "Generating mocks..."
	# Add mock generation commands here when needed

# Docker build
docker-build:
	docker build -t atlas-db:latest .

# Docker run
docker-run:
	docker run -p 8080:8080 -v $(PWD)/config:/config atlas-db:latest

# Help
help:
	@echo "Available targets:"
	@echo "  all          - Install deps, generate proto, and build"
	@echo "  deps         - Install Go dependencies"
	@echo "  proto        - Generate protocol buffer files"
	@echo "  build        - Build the binary"
	@echo "  build-all    - Build for multiple platforms"
	@echo "  test         - Run tests"
	@echo "  test-race    - Run tests with race detection"
	@echo "  test-coverage- Run tests with coverage"
	@echo "  install      - Install binary to GOPATH/bin"
	@echo "  run          - Run with default config"
	@echo "  run-dev      - Run with development config"
	@echo "  clean        - Clean build artifacts"
	@echo "  fmt          - Format code"
	@echo "  lint         - Lint code"
	@echo "  security     - Run security scan"
	@echo "  docker-build - Build Docker image"
	@echo "  docker-run   - Run Docker container"
	@echo "  help         - Show this help message"