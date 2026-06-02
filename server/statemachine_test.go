package server

import (
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/raftkv/raft"
	"github.com/raftkv/storage"
)

// makeTestSM creates a single-node cluster with a real WAL and storage engine.
// The node becomes leader automatically (quorum = 1 with no peers).
func makeTestSM(t *testing.T) (*StateMachine, func()) {
	t.Helper()

	dir, err := os.MkdirTemp("", "server-test-*")
	if err != nil {
		t.Fatal(err)
	}

	eng, err := storage.Open(dir + "/storage")
	if err != nil {
		os.RemoveAll(dir)
		t.Fatal(err)
	}

	ws, err := newWALState(dir + "/wal")
	if err != nil {
		eng.Close()
		os.RemoveAll(dir)
		t.Fatal(err)
	}

	net := raft.NewNetwork()
	cfg := raft.Config{ID: "node1", Peers: map[string]string{}}
	node := raft.NewNode(cfg, net.TransportFor("node1"), ws)
	net.Add("node1", node)

	if err := node.Start(); err != nil {
		eng.Close()
		os.RemoveAll(dir)
		t.Fatal(err)
	}

	// Single-node cluster: quorum = 1, leader election is immediate.
	deadline := time.Now().Add(2 * time.Second)
	for !node.IsLeader() {
		if time.Now().After(deadline) {
			t.Fatal("node did not become leader within 2s")
		}
		time.Sleep(10 * time.Millisecond)
	}

	sm := NewStateMachine(node, eng)

	cleanup := func() {
		sm.Stop()
		node.Stop()
		eng.Close()
		os.RemoveAll(dir)
	}
	return sm, cleanup
}

func cmd(op, key string, val []byte, clientID string, seq uint64) Command {
	return Command{Op: op, Key: key, Value: val, ClientID: clientID, SeqNum: seq}
}

// TestPutAndGet verifies the full round-trip: propose → commit → apply → read.
func TestPutAndGet(t *testing.T) {
	sm, cleanup := makeTestSM(t)
	defer cleanup()

	if err := sm.ProposeWrite(cmd("put", "foo", []byte("bar"), "c1", 1)); err != nil {
		t.Fatalf("put: %v", err)
	}

	val, found, err := sm.ReadValue("foo")
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if !found {
		t.Fatal("key not found after put")
	}
	if string(val) != "bar" {
		t.Fatalf("got %q want %q", val, "bar")
	}
}

// TestDeduplication verifies that retrying the same (ClientID, SeqNum) does not
// apply the command twice (at-most-once semantics).
func TestDeduplication(t *testing.T) {
	sm, cleanup := makeTestSM(t)
	defer cleanup()

	if err := sm.ProposeWrite(cmd("put", "counter", []byte("1"), "c1", 1)); err != nil {
		t.Fatalf("first put: %v", err)
	}

	// Retry with same seqNum — must not overwrite
	if err := sm.ProposeWrite(cmd("put", "counter", []byte("2"), "c1", 1)); err != nil {
		t.Fatalf("retry put: %v", err)
	}

	val, found, _ := sm.ReadValue("counter")
	if !found {
		t.Fatal("key not found")
	}
	if string(val) != "1" {
		t.Fatalf("dedup failed: got %q want %q", val, "1")
	}
}

// TestNonLeaderRejectsWrites verifies that a non-leader node returns ErrNotLeader.
func TestNonLeaderRejectsWrites(t *testing.T) {
	dir, _ := os.MkdirTemp("", "server-follower-*")
	defer os.RemoveAll(dir)

	eng, _ := storage.Open(dir + "/storage")
	defer eng.Close()
	ws, _ := newWALState(dir + "/wal")

	net := raft.NewNetwork()
	// Two-node config so quorum = 2 and this node can never win alone
	cfg := raft.Config{ID: "node1", Peers: map[string]string{"node2": "node2"}}
	node := raft.NewNode(cfg, net.TransportFor("node1"), ws)
	net.Add("node1", node)
	net.Disconnect("node1") // isolated — will never reach quorum

	if err := node.Start(); err != nil {
		t.Fatal(err)
	}
	defer node.Stop()

	sm := NewStateMachine(node, eng)
	defer sm.Stop()

	// Let election timer fire — node becomes Candidate but cannot win
	time.Sleep(400 * time.Millisecond)

	err := sm.ProposeWrite(cmd("put", "k", []byte("v"), "c1", 1))
	if err != ErrNotLeader {
		t.Fatalf("expected ErrNotLeader, got %v", err)
	}
}

// TestMultipleWritesOrdered verifies 20 sequential writes are applied in order
// and all values are readable.
func TestMultipleWritesOrdered(t *testing.T) {
	sm, cleanup := makeTestSM(t)
	defer cleanup()

	const n = 20
	for i := 1; i <= n; i++ {
		key := fmt.Sprintf("key%02d", i)
		val := fmt.Sprintf("val%02d", i)
		if err := sm.ProposeWrite(cmd("put", key, []byte(val), "c1", uint64(i))); err != nil {
			t.Fatalf("put %d: %v", i, err)
		}
	}

	if sm.LastApplied() != uint64(n) {
		t.Fatalf("lastApplied=%d want %d", sm.LastApplied(), n)
	}

	for i := 1; i <= n; i++ {
		key := fmt.Sprintf("key%02d", i)
		want := fmt.Sprintf("val%02d", i)
		val, found, _ := sm.ReadValue(key)
		if !found {
			t.Fatalf("key %s not found", key)
		}
		if string(val) != want {
			t.Fatalf("key %s: got %q want %q", key, val, want)
		}
	}
}

// TestDeleteRemovesKey verifies that a DELETE makes the key unreadable.
func TestDeleteRemovesKey(t *testing.T) {
	sm, cleanup := makeTestSM(t)
	defer cleanup()

	sm.ProposeWrite(cmd("put", "gone", []byte("here"), "c1", 1))
	sm.ProposeWrite(cmd("delete", "gone", nil, "c1", 2))

	_, found, err := sm.ReadValue("gone")
	if err != nil {
		t.Fatalf("read after delete: %v", err)
	}
	if found {
		t.Fatal("key still visible after delete")
	}
}

// TestCommitNotificationUnblocksPropose verifies that ProposeWrite returns only
// after the entry is committed and applied, not before.
func TestCommitNotificationUnblocksPropose(t *testing.T) {
	sm, cleanup := makeTestSM(t)
	defer cleanup()

	done := make(chan error, 1)
	go func() {
		done <- sm.ProposeWrite(cmd("put", "sync", []byte("yes"), "c1", 1))
	}()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("propose: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("ProposeWrite did not unblock within 3s")
	}

	val, found, _ := sm.ReadValue("sync")
	if !found || string(val) != "yes" {
		t.Fatalf("value not visible after propose returned: found=%v val=%q", found, val)
	}
}
