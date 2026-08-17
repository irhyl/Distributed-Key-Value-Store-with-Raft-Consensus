// kvserver.go - the gRPC server that handles two kinds of traffic:
//   1. Raft RPCs from peer nodes (RequestVote, AppendEntries, InstallSnapshot)
//   2. KV RPCs from clients (Get, Put, Delete)

package server

import (
	"context"
	"fmt"
	"log"
	"net"

	"github.com/raftkv/raft"
	pb "github.com/raftkv/proto"
	"github.com/raftkv/storage"
	"google.golang.org/grpc"
)

// KVServer is the top-level server object. It owns:
//   - a Raft node (consensus)
//   - an LSM storage engine (durability)
//   - a StateMachine (applies committed entries to storage)
//   - a gRPC server (handles RPCs from clients and peers)
type KVServer struct {
	pb.UnimplementedRaftServiceServer
	pb.UnimplementedKVServiceServer

	cfg       Config
	node      *raft.Node
	engine    *storage.Engine
	sm        *StateMachine
	grpcSrv   *grpc.Server
	transport *GRPCTransport
	walState  *walState
	stopCh    chan struct{}
}

// Config holds the full configuration for a KVServer node.
type Config struct {
	NodeID      string            // this node's identifier
	ListenAddr  string            // address to bind the gRPC server on
	Peers       map[string]string // peerID → gRPC address (other nodes)
	DataDir     string            // directory for WAL and SSTable files
	MetricsAddr string            // if set, serve Prometheus metrics at http://MetricsAddr/metrics
}

// leaderHintAddr translates the Raft leader ID returned by node.LeaderID()
// into a dialable gRPC address for the client to redirect to.
// Returns "" if the leader is unknown.
func (s *KVServer) leaderHintAddr() string {
	id := s.node.LeaderID()
	if id == "" {
		return ""
	}
	if id == s.cfg.NodeID {
		return s.cfg.ListenAddr
	}
	return s.cfg.Peers[id] // "" if id not in peers map
}

// NewKVServer creates and wires up a fully configured KVServer.
// Call Start() to begin accepting connections.
func NewKVServer(cfg Config) (*KVServer, error) {
	// 1. Open storage engine
	eng, err := storage.Open(cfg.DataDir + "/storage")
	if err != nil {
		return nil, fmt.Errorf("open storage: %w", err)
	}

	// 2. Open WAL-backed persistent state for Raft
	walState, err := newWALState(cfg.DataDir + "/wal")
	if err != nil {
		return nil, fmt.Errorf("open wal: %w", err)
	}

	// 3. Create gRPC transport to peers
	transport := NewGRPCTransport(cfg.Peers)

	// 4. Create the Raft node
	raftCfg := raft.Config{
		ID:      cfg.NodeID,
		Peers:   cfg.Peers,
		DataDir: cfg.DataDir,
	}
	node := raft.NewNode(raftCfg, transport, walState)

	// 5. Create state machine
	sm := NewStateMachine(node, eng)

	// 6. Build the gRPC server
	grpcSrv := grpc.NewServer()
	srv := &KVServer{
		cfg:       cfg,
		node:      node,
		engine:    eng,
		sm:        sm,
		grpcSrv:   grpcSrv,
		transport: transport,
		walState:  walState,
		stopCh:    make(chan struct{}),
	}
	pb.RegisterRaftServiceServer(grpcSrv, srv)
	pb.RegisterKVServiceServer(grpcSrv, srv)
	srv.wireMetrics()

	return srv, nil
}

// Start begins the Raft protocol and starts serving gRPC requests.
func (s *KVServer) Start(listenAddr string) error {
	lis, err := net.Listen("tcp", listenAddr)
	if err != nil {
		return fmt.Errorf("listen %s: %w", listenAddr, err)
	}

	if err := s.node.Start(); err != nil {
		return fmt.Errorf("raft start: %w", err)
	}

	ServeMetrics(s.cfg.MetricsAddr)
	startReplicationLagUpdater(s.node, s.stopCh)

	log.Printf("[%s] listening on %s", s.node.LeaderID(), listenAddr)
	go func() {
		if err := s.grpcSrv.Serve(lis); err != nil {
			log.Printf("grpc serve error: %v", err)
		}
	}()
	return nil
}

// Stop gracefully shuts down the server.
func (s *KVServer) Stop() {
	close(s.stopCh)
	s.grpcSrv.GracefulStop()
	s.sm.Stop()
	s.node.Stop()
	s.transport.Close()
	s.engine.Close()
}

// ── Raft RPC handlers ─────────────────────────────────────────────────────────
// These are called by peer nodes during consensus.

func (s *KVServer) RequestVote(_ context.Context, req *pb.RequestVoteRequest) (*pb.RequestVoteResponse, error) {
	return s.node.HandleRequestVote(req), nil
}

func (s *KVServer) AppendEntries(_ context.Context, req *pb.AppendEntriesRequest) (*pb.AppendEntriesResponse, error) {
	return s.node.HandleAppendEntries(req), nil
}

func (s *KVServer) InstallSnapshot(_ context.Context, req *pb.InstallSnapshotRequest) (*pb.InstallSnapshotResponse, error) {
	return s.node.HandleInstallSnapshot(req), nil
}

// ── KV RPC handlers ───────────────────────────────────────────────────────────
// These are called by clients (CLI, applications).

// Get returns the value for a key.
// Only the leader can serve linearizable reads.
// If this node is not the leader, it returns a hint so the client can redirect.
func (s *KVServer) Get(_ context.Context, req *pb.GetRequest) (*pb.GetResponse, error) {
	val, found, err := s.sm.ReadValue(req.Key)
	if err == ErrNotLeader {
		return &pb.GetResponse{LeaderHint: s.leaderHintAddr()}, nil
	}
	if err != nil {
		return nil, err
	}
	return &pb.GetResponse{
		Key:   req.Key,
		Value: val,
		Found: found,
	}, nil
}

// Put stores a key-value pair.
// Proposes to Raft and blocks until committed across a majority.
func (s *KVServer) Put(_ context.Context, req *pb.PutRequest) (*pb.PutResponse, error) {
	cmd := Command{Op: "put", Key: req.Key, Value: req.Value}
	err := s.sm.ProposeWrite(cmd)
	if err == ErrNotLeader {
		return &pb.PutResponse{LeaderHint: s.leaderHintAddr()}, nil
	}
	if err != nil {
		return nil, err
	}
	return &pb.PutResponse{Success: true}, nil
}

// Delete removes a key.
func (s *KVServer) Delete(_ context.Context, req *pb.DeleteRequest) (*pb.DeleteResponse, error) {
	cmd := Command{Op: "delete", Key: req.Key}
	err := s.sm.ProposeWrite(cmd)
	if err == ErrNotLeader {
		return &pb.DeleteResponse{LeaderHint: s.leaderHintAddr()}, nil
	}
	if err != nil {
		return nil, err
	}
	return &pb.DeleteResponse{Success: true}, nil
}

// Watch streams every write applied on this node from the point of
// subscription onward - change data capture. Unlike Get/Put/Delete, this
// works on any node (not leader-only): a follower's applied stream lags the
// leader's by at most one replication round trip, which is an acceptable
// tradeoff for a change feed and means Watch doesn't need leader redirection.
func (s *KVServer) Watch(req *pb.WatchRequest, stream pb.KVService_WatchServer) error {
	events, unsubscribe := s.sm.Subscribe(req.KeyPrefix)
	defer unsubscribe()

	for {
		select {
		case event, ok := <-events:
			if !ok {
				return fmt.Errorf("watch: subscriber fell too far behind and was disconnected")
			}
			if err := stream.Send(event); err != nil {
				return err
			}
		case <-stream.Context().Done():
			return stream.Context().Err()
		}
	}
}
