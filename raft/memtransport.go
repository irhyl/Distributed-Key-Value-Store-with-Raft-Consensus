// memtransport.go — a synchronous in-memory Transport for unit tests.
// Nodes are wired together via a shared registry; RPCs are direct function calls.

package raft

import (
	"fmt"
	"sync"

	pb "github.com/raftkv/proto"
)

// Network simulates a cluster of nodes with controllable connectivity.
// Use Disconnect/Reconnect to simulate node failures and partitions.
type Network struct {
	mu       sync.Mutex
	nodes    map[string]*Node
	isolated map[string]bool // nodes that cannot send or receive messages
}

// NewNetwork creates an empty simulated network.
func NewNetwork() *Network {
	return &Network{
		nodes:    make(map[string]*Node),
		isolated: make(map[string]bool),
	}
}

// Add registers a node with the network.
func (net *Network) Add(id string, node *Node) {
	net.mu.Lock()
	defer net.mu.Unlock()
	net.nodes[id] = node
}

// Disconnect simulates a node crash or partition (all messages dropped).
func (net *Network) Disconnect(id string) {
	net.mu.Lock()
	defer net.mu.Unlock()
	net.isolated[id] = true
}

// Reconnect brings a node back online.
func (net *Network) Reconnect(id string) {
	net.mu.Lock()
	defer net.mu.Unlock()
	delete(net.isolated, id)
}

// TransportFor returns the Transport that node `from` should use.
func (net *Network) TransportFor(from string) Transport {
	return &memTransport{net: net, from: from}
}

// memTransport implements Transport for a specific node.
type memTransport struct {
	net  *Network
	from string
}

func (t *memTransport) RequestVote(peerID string, req *pb.RequestVoteRequest) (*pb.RequestVoteResponse, error) {
	t.net.mu.Lock()
	isolated := t.net.isolated[t.from] || t.net.isolated[peerID]
	peer := t.net.nodes[peerID]
	t.net.mu.Unlock()

	if isolated || peer == nil {
		return nil, fmt.Errorf("network: %s unreachable", peerID)
	}
	return peer.HandleRequestVote(req), nil
}

func (t *memTransport) AppendEntries(peerID string, req *pb.AppendEntriesRequest) (*pb.AppendEntriesResponse, error) {
	t.net.mu.Lock()
	isolated := t.net.isolated[t.from] || t.net.isolated[peerID]
	peer := t.net.nodes[peerID]
	t.net.mu.Unlock()

	if isolated || peer == nil {
		return nil, fmt.Errorf("network: %s unreachable", peerID)
	}
	return peer.HandleAppendEntries(req), nil
}

func (t *memTransport) InstallSnapshot(peerID string, req *pb.InstallSnapshotRequest) (*pb.InstallSnapshotResponse, error) {
	t.net.mu.Lock()
	isolated := t.net.isolated[t.from] || t.net.isolated[peerID]
	peer := t.net.nodes[peerID]
	t.net.mu.Unlock()

	if isolated || peer == nil {
		return nil, fmt.Errorf("network: %s unreachable", peerID)
	}
	return peer.HandleInstallSnapshot(req), nil
}
