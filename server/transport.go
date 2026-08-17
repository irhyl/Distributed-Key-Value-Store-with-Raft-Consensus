// transport.go - gRPC implementation of raft.Transport.
// This replaces memtransport.go for production; nodes communicate over the network.

package server

import (
	"context"
	"fmt"
	"sync"
	"time"

	pb "github.com/raftkv/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

const rpcTimeout = 100 * time.Millisecond

// GRPCTransport implements raft.Transport using gRPC connections.
// It maintains a connection pool to peer nodes, reconnecting on failure.
type GRPCTransport struct {
	mu      sync.Mutex
	peers   map[string]string                 // peerID → address
	conns   map[string]*grpc.ClientConn       // peerID → active connection
	clients map[string]pb.RaftServiceClient   // peerID → gRPC client stub
}

// NewGRPCTransport creates a transport with the given peer map.
func NewGRPCTransport(peers map[string]string) *GRPCTransport {
	return &GRPCTransport{
		peers:   peers,
		conns:   make(map[string]*grpc.ClientConn),
		clients: make(map[string]pb.RaftServiceClient),
	}
}

// RequestVote sends a RequestVote RPC to the given peer.
func (t *GRPCTransport) RequestVote(peerID string, req *pb.RequestVoteRequest) (*pb.RequestVoteResponse, error) {
	client, err := t.getClient(peerID)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(context.Background(), rpcTimeout)
	defer cancel()
	return client.RequestVote(ctx, req)
}

// AppendEntries sends an AppendEntries RPC to the given peer.
func (t *GRPCTransport) AppendEntries(peerID string, req *pb.AppendEntriesRequest) (*pb.AppendEntriesResponse, error) {
	client, err := t.getClient(peerID)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(context.Background(), rpcTimeout)
	defer cancel()
	return client.AppendEntries(ctx, req)
}

// InstallSnapshot sends an InstallSnapshot RPC to the given peer.
func (t *GRPCTransport) InstallSnapshot(peerID string, req *pb.InstallSnapshotRequest) (*pb.InstallSnapshotResponse, error) {
	client, err := t.getClient(peerID)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(context.Background(), rpcTimeout)
	defer cancel()
	return client.InstallSnapshot(ctx, req)
}

// Close shuts down all peer connections.
func (t *GRPCTransport) Close() {
	t.mu.Lock()
	defer t.mu.Unlock()
	for _, conn := range t.conns {
		conn.Close()
	}
}

// getClient returns a cached gRPC client for peerID, creating one if needed.
func (t *GRPCTransport) getClient(peerID string) (pb.RaftServiceClient, error) {
	t.mu.Lock()
	defer t.mu.Unlock()

	if client, ok := t.clients[peerID]; ok {
		return client, nil
	}

	addr, ok := t.peers[peerID]
	if !ok {
		return nil, fmt.Errorf("transport: unknown peer %q", peerID)
	}

	// Connect without TLS; production should use mutual TLS.
	dialCtx, dialCancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer dialCancel()
	// DialContext+WithBlock is deprecated in favor of NewClient, but NewClient
	// connects lazily on first RPC - we need dial itself to fail within this
	// short timeout so an unreachable peer is detected quickly, not on the
	// next heartbeat's RPC call.
	conn, err := grpc.DialContext(dialCtx, addr, //nolint:staticcheck
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithBlock(), //nolint:staticcheck
	)
	if err != nil {
		return nil, fmt.Errorf("transport: dial %s (%s): %w", peerID, addr, err)
	}

	client := pb.NewRaftServiceClient(conn)
	t.conns[peerID] = conn
	t.clients[peerID] = client
	return client, nil
}
