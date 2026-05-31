#!/usr/bin/env bash
# build.sh — builds all binaries and runs the full test suite.
set -euo pipefail

echo "==> Tidying modules..."
go mod tidy

echo "==> Running tests..."
go test ./wal/...     -timeout 30s -v
go test ./storage/... -timeout 30s -v
go test ./raft/...    -timeout 30s -v
go test ./server/...  -timeout 30s -v

echo ""
echo "==> Building server binary..."
go build -o raftkv-server ./

echo "==> Building CLI binary..."
go build -o raftkv-cli ./client/

echo ""
echo "✅ All tests passed. Binaries built:"
echo "   ./raftkv-server"
echo "   ./raftkv-cli"
echo ""
echo "Start a local cluster:"
echo "   ./scripts/run_cluster.sh"
echo ""
echo "Run chaos tests:"
echo "   python3 chaos/chaos.py --binary ./raftkv-server --cli ./raftkv-cli"
