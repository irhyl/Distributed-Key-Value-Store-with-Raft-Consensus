.PHONY: all proto build test clean chaos

BINARY_SERVER := raftkv-server
BINARY_CLI    := raftkv-cli
PROTO_DIR     := proto
PROTO_FILE    := $(PROTO_DIR)/raftkv.proto

all: proto build

# Regenerate Go files from the proto definition.
# Requires: protoc, protoc-gen-go, protoc-gen-go-grpc
#   go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
#   go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest
proto:
	protoc \
		--go_out=. \
		--go_opt=paths=source_relative \
		--go-grpc_out=. \
		--go-grpc_opt=paths=source_relative \
		$(PROTO_FILE)

# Download module dependencies
deps:
	go mod tidy

# Build both binaries
build: deps
	go build -o $(BINARY_SERVER) .
	go build -o $(BINARY_CLI)    ./client/

# Run all package tests
test: deps
	go test ./wal/...     -timeout 60s -v -count=1
	go test ./storage/... -timeout 60s -v -count=1
	go test ./raft/...    -timeout 60s -v -count=1
	go test ./server/...  -timeout 60s -v -count=1

# Run tests with the race detector enabled
test-race: deps
	go test -race ./wal/...     -timeout 60s -count=1
	go test -race ./storage/... -timeout 60s -count=1
	go test -race ./raft/...    -timeout 60s -count=1
	go test -race ./server/...  -timeout 60s -count=1

# Run benchmarks across all packages
bench:
	go test ./wal/...     -bench=. -benchmem -run=^$
	go test ./storage/... -bench=. -benchmem -run=^$

# Run the chaos test (requires binaries to be built first)
chaos: build
	python3 chaos/chaos.py \
		--binary ./$(BINARY_SERVER) \
		--cli    ./$(BINARY_CLI) \
		--rounds 3 \
		--workers 4 \
		--writes 50

# Start a local 3-node cluster
cluster:
	@chmod +x scripts/run_cluster.sh
	./scripts/run_cluster.sh

# Stop the local cluster
cluster-stop:
	./scripts/run_cluster.sh stop

clean:
	rm -f $(BINARY_SERVER) $(BINARY_CLI)
	rm -f $(PROTO_DIR)/*.pb.go
