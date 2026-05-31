package server

import (
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/raftkv/raft"
	"github.com/raftkv/storage"
)

// makeTestNode creates a single-node Raft cluster that immediately becomes
// leader (quorum = 1 with no peers) and wires it to a real storage engine.
func makeTestNode(t *testing.T) (*StateMachine, func()) {
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

	walSt, err := newWALState(dir + "/wal")
	if err != nil {
		eng.Close()
		os.RemoveAll(dir)
		t.Fatal(err)
	}

	net := raft.NewNetwork()
	cfg := raft.Config{ID: "node1", Peers: map[string]string{}}
	node := raft.NewNode(cfg, net.TransportFor("node1"), walSt)
	net.Add("node1", node)

	if err := node.Start(); err != nil {
		eng.Close()
		os.RemoveAll(dir)
		t.Fatal(err)
	}

	// A single-node cluster has quorum = 1, so it elects itself immediately.
	deadline := time.Now().Add(2 * time.Second)
	for !node.IsLeader() && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if !node.IsLeader() {
		node.Stop()
		eng.Close()
		os.RemoveAll(dir)
		t.Fatal("node1 did not become leader within 2s")
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

// TestPutAndGet verifies the full round-trip: propose → commit → apply → read.
func TestPutAndGet(t *testing.T) {
	sm, cleanup := makeTestNode(t)
	defer cleanup()

	cmd := Command{Op: "put", Key: "foo", Value: []byte("bar")}
	if err := sm.ProposeWrite(cmd); err != nil {
		t.Fatalf("ProposeWrite: %v", err)
	}

	val, found := sm.engine.Get("foo")
	if !found {
		t.Fatal("key not found after put")
	}
	if string(val) != "bar" {
		t.Fatalf("got %q, want %q", val, "bar")
	}
}

// TestDeduplication verifies that retrying a write with the same (ClientID,
// SeqNum) applies the command exactly once and still returns success.
func TestDeduplication(t *testing.T) {
	sm, cleanup := makeTestNode(t)
	defer cleanup()

	cmd := Command{
		Op:       "put",
		Key:      "counter",
		Value:    []byte("1"),
		ClientID: "client-A",
		SeqNum:   1,
	}

	// First write
	if err := sm.ProposeWrite(cmd); err != nil {
		t.Fatalf("first ProposeWrite: %v", err)
	}

	// Simulate retry with identical (ClientID, SeqNum)
	cmd.Value = []byte("99") // different value, same seq — must not overwrite
	if err := sm.ProposeWrite(cmd); err != nil {
		t.Fatalf("retry ProposeWrite: %v", err)
	}

	// Storage must still hold the first write
	val, found := sm.engine.Get("counter")
	if !found {
		t.Fatal("key not found")
	}
	if string(val) != "1" {
		t.Fatalf("dedup failed: got %q, want %q", val, "1")
	}
}

// TestNonLeaderRejectsWrites verifies ErrNotLeader is returned when the node
// is not the leader. We test this by stopping the node so it steps down.
func TestNonLeaderRejectsWrites(t *testing.T) {
	sm, cleanup := makeTestNode(t)
	defer cleanup()

	// Stop the Raft node so it steps down from leadership
	sm.node.Stop()

	// Give the node a moment to process the stop
	time.Sleep(50 * time.Millisecond)

	cmd := Command{Op: "put", Key: "x", Value: []byte("y")}
	err := sm.ProposeWrite(cmd)
	if err != ErrNotLeader && err != ErrStopped {
		t.Fatalf("expected ErrNotLeader or ErrStopped, got %v", err)
	}
}

// TestMultipleWritesOrdered verifies that N sequential writes all commit in
// order and are all readable afterwards.
func TestMultipleWritesOrdered(t *testing.T) {
	sm, cleanup := makeTestNode(t)
	defer cleanup()

	const n = 20
	for i := 0; i < n; i++ {
		cmd := Command{
			Op:    "put",
			Key:   fmt.Sprintf("key%02d", i),
			Value: []byte(fmt.Sprintf("val%02d", i)),
		}
		if err := sm.ProposeWrite(cmd); err != nil {
			t.Fatalf("write %d: %v", i, err)
		}
	}

	if sm.LastApplied() != n {
		t.Fatalf("lastApplied = %d, want %d", sm.LastApplied(), n)
	}

	for i := 0; i < n; i++ {
		key := fmt.Sprintf("key%02d", i)
		want := fmt.Sprintf("val%02d", i)
		val, found := sm.engine.Get(key)
		if !found {
			t.Fatalf("key %s not found", key)
		}
		if string(val) != want {
			t.Fatalf("key %s: got %q, want %q", key, val, want)
		}
	}
}

// TestDeleteRemovesKey verifies that a delete command makes the key unreadable.
func TestDeleteRemovesKey(t *testing.T) {
	sm, cleanup := makeTestNode(t)
	defer cleanup()

	put := Command{Op: "put", Key: "temp", Value: []byte("exists")}
	if err := sm.ProposeWrite(put); err != nil {
		t.Fatalf("put: %v", err)
	}

	del := Command{Op: "delete", Key: "temp"}
	if err := sm.ProposeWrite(del); err != nil {
		t.Fatalf("delete: %v", err)
	}

	_, found := sm.engine.Get("temp")
	if found {
		t.Fatal("key still present after delete")
	}
}

// TestReadValueRequiresLeader verifies that ReadValue returns ErrNotLeader
// when the underlying node is not the leader.
func TestReadValueRequiresLeader(t *testing.T) {
	sm, cleanup := makeTestNode(t)
	defer cleanup()

	// Write a key while we are leader
	put := Command{Op: "put", Key: "ro", Value: []byte("v")}
	if err := sm.ProposeWrite(put); err != nil {
		t.Fatalf("put: %v", err)
	}

	// Stop the node so leadership is lost
	sm.node.Stop()
	time.Sleep(50 * time.Millisecond)

	_, _, err := sm.ReadValue("ro")
	if err != ErrNotLeader {
		t.Fatalf("expected ErrNotLeader, got %v", err)
	}
}
