GOOS ?= linux
GOARCH ?= amd64
GO_FILES = $(shell find . -name '*.go' -not -path "./vendor/*")

caddy: atlas/caddy/caddy
	cp atlas/caddy/caddy caddy

atlas/caddy/caddy: $(GO_FILES) atlas/bootstrap/bootstrap.pb.go atlas/consensus/consensus.pb.go
	@echo "Building Caddy"
	@cd atlas/caddy && go build

atlas/bootstrap/bootstrap.pb.go: atlas/bootstrap/bootstrap.proto
	@echo "Generating bootstrap protobuf files"
	@cd atlas && protoc --go_out=. --go-grpc_out=. bootstrap/bootstrap.proto

atlas/consensus/consensus.pb.go: atlas/consensus/consensus.proto
	@echo "Generating consensus protobuf files"
	@cd atlas && protoc --go_out=. --go-grpc_out=. consensus/consensus.proto

.PHONY: test
test:
	@go test -v -race ./...

.PHONY: clean
clean:
	@rm -f caddy
	@rm -f atlas/caddy/caddy
	@rm -f atlas/bootstrap/bootstrap.pb.go
	@rm -f atlas/consensus/consensus.pb.go
	@rm -f atlas/consensus/consensus_grpc.pb.go
	@rm -f atlas/bootstrap/bootstrap_grpc.pb.go
