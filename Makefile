GOOS ?= linux
GOARCH ?= amd64
GO_FILES = $(shell find . -name '*.go' -not -path "./vendor/*")
export PATH = $(shell pwd)/tools/bin:$(shell echo $$PATH)

caddy: atlas/caddy/caddy
	cp atlas/caddy/caddy caddy

atlasdb: caddy tools/bin/upx
	go run golang.org/x/tools/gopls/internal/analysis/modernize/cmd/modernize@latest -fix -test ./...
	go fmt ./...
	golangci-lint run ./...
	cp caddy atlasdb

.PHONY: small
small: atlasdb
	upx atlasdb

atlas/caddy/caddy: $(GO_FILES) atlas/bootstrap/bootstrap.pb.go atlas/consensus/consensus.pb.go
	@echo "Building Caddy"
	@cd atlas/caddy && go build

atlas/bootstrap/bootstrap.pb.go: atlas/bootstrap/bootstrap.proto tools/bin/protoc tools/bin/protoc-gen-go-grpc
	@echo "Generating bootstrap protobuf files"
	@cd atlas && protoc --go_out=. --go-grpc_out=. bootstrap/bootstrap.proto

atlas/consensus/consensus.pb.go: atlas/consensus/consensus.proto tools/bin/protoc tools/bin/protoc-gen-go-grpc
	@echo "Generating consensus protobuf files"
	@cd atlas && protoc --go_out=. --go-grpc_out=. consensus/consensus.proto

tools/bin/protoc-gen-go:
	@echo "Installing protoc"
	@mkdir -p tools
	go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
	cp $(shell go env GOPATH)/bin/protoc-gen-go tools/bin/protoc-gen-go

tools/bin/protoc-gen-go-grpc: tools/bin/protoc-gen-go
	@echo "Installing protoc"
	@mkdir -p tools
	go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest
	cp $(shell go env GOPATH)/bin/protoc-gen-go-grpc tools/bin/protoc-gen-go-grpc

tools/bin/protoc:
	./scripts/install-protoc.sh

tools/bin/upx:
	@mkdir -p tools/bin
	@wget https://github.com/upx/upx/releases/download/v5.0.2/upx-5.0.2-amd64_linux.tar.xz -O upx.tar.xz
	@tar -xvf upx.tar.xz && mv upx-*/upx tools/bin/upx && rm -rf upx.tar.xz && rm -rf upx-*

.PHONY: release
release: caddy tools/bin/upx
	@upx caddy

.PHONY: test test-go test-e2e-acl test-integration
test: test-go test-e2e-acl test-integration

test-go:
	@go test -v -race ./...

# End-to-end ACL test using the local caddy binary and socket REPL
test-e2e-acl: caddy
	@bash ./test_acl_e2e.sh

# Integration tests using the Go test harness
test-integration: caddy
	@go test -v -timeout 2m -tags=integration ./integration-tests/...

.PHONY: clean
clean:
	@rm -f caddy
	@rm -f atlas/caddy/caddy
	@rm -f atlas/bootstrap/bootstrap.pb.go
	@rm -f atlas/consensus/consensus.pb.go
	@rm -f atlas/consensus/consensus_grpc.pb.go
	@rm -f atlas/bootstrap/bootstrap_grpc.pb.go
	@rm -rf tools
