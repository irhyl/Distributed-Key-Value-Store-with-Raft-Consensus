package server

import (
	"fmt"
	"os"
	"testing"
	"time"

	pb "github.com/raftkv/proto"
	"github.com/raftkv/raft"
	"github.com/raftkv/storage"
)

// makeTestSM creates a single-node cluster with a real WAL and storage engine.
// The node becomes leader automatically (quorum = 1 with no peers).
func makeTestSM(t *testing.T) (*StateMachine, func()) {
	t.Helper()
	sm, _, _, cleanup := makeTestSMWithOpts(t)
	return sm, cleanup
}

// makeTestSMWithOpts is makeTestSM but also returns the underlying engine
// and data directory (needed by tests that inspect engine state directly or
// reopen the same directory to test restart behavior), and forwards opts to
// NewStateMachine (needed by tests exercising WithSnapshotInterval).
func makeTestSMWithOpts(t *testing.T, opts ...Option) (sm *StateMachine, eng *storage.Engine, dir string, cleanup func()) {
	t.Helper()

	dir, err := os.MkdirTemp("", "server-test-*")
	if err != nil {
		t.Fatal(err)
	}

	eng, err = storage.Open(dir + "/storage")
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

	sm = NewStateMachine(node, eng, opts...)

	cleanup = func() {
		sm.Stop()
		node.Stop()
		eng.Close()
		os.RemoveAll(dir)
	}
	return sm, eng, dir, cleanup
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

	// Retry with same seqNum - must not overwrite
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
	net.Disconnect("node1") // isolated - will never reach quorum

	if err := node.Start(); err != nil {
		t.Fatal(err)
	}
	defer node.Stop()

	sm := NewStateMachine(node, eng)
	defer sm.Stop()

	// Let election timer fire - node becomes Candidate but cannot win
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

// TestSnapshotTriggersAtInterval verifies that WithSnapshotInterval causes
// the state machine to compact the Raft log automatically as entries are
// applied, at multiples of the configured interval.
func TestSnapshotTriggersAtInterval(t *testing.T) {
	sm, _, _, cleanup := makeTestSMWithOpts(t, WithSnapshotInterval(5))
	defer cleanup()

	for i := 1; i <= 12; i++ {
		key := fmt.Sprintf("key%02d", i)
		if err := sm.ProposeWrite(cmd("put", key, []byte("v"), "c1", uint64(i))); err != nil {
			t.Fatalf("put %d: %v", i, err)
		}
	}

	// maybeSnapshot runs synchronously in the apply loop right after each
	// entry that lands on an interval boundary, so by the time
	// ProposeWrite(12) has returned, the snapshot at index 10 (the highest
	// multiple of 5 <= 12) should already be recorded.
	if got := sm.node.SnapshotIndex(); got != 10 {
		t.Fatalf("SnapshotIndex: got %d, want 10", got)
	}
}

// TestSnapshotSurvivesRestart verifies that a snapshot taken before a
// restart is picked up on reopen - proving the restart fast-path (seeding
// commitIndex from the snapshot instead of always starting at 0) actually
// engages, and that data survives the compaction + restart combination.
func TestSnapshotSurvivesRestart(t *testing.T) {
	// cleanup is intentionally unused: this test manually stops/closes the
	// first sm/node/eng partway through to reopen the same directory, so
	// the normal cleanup closure (which would double-Stop them) doesn't apply.
	sm, eng, dir, _ := makeTestSMWithOpts(t, WithSnapshotInterval(5))

	for i := 1; i <= 8; i++ {
		key := fmt.Sprintf("key%02d", i)
		val := fmt.Sprintf("val%02d", i)
		if err := sm.ProposeWrite(cmd("put", key, []byte(val), "c1", uint64(i))); err != nil {
			t.Fatalf("put %d: %v", i, err)
		}
	}
	if got := sm.node.SnapshotIndex(); got != 5 {
		t.Fatalf("before restart: SnapshotIndex got %d, want 5", got)
	}

	sm.Stop()
	sm.node.Stop()
	eng.Close()

	// Reopen against the same directory - mirrors kvserver.go's startup
	// sequence, not calling makeTestSMWithOpts again (which would create a
	// fresh temp dir).
	eng2, err := storage.Open(dir + "/storage")
	if err != nil {
		os.RemoveAll(dir)
		t.Fatalf("reopen engine: %v", err)
	}
	ws2, err := newWALState(dir + "/wal")
	if err != nil {
		eng2.Close()
		os.RemoveAll(dir)
		t.Fatalf("reopen wal state: %v", err)
	}
	net2 := raft.NewNetwork()
	cfg := raft.Config{ID: "node1", Peers: map[string]string{}}
	node2 := raft.NewNode(cfg, net2.TransportFor("node1"), ws2)
	net2.Add("node1", node2)
	if err := node2.Start(); err != nil {
		eng2.Close()
		os.RemoveAll(dir)
		t.Fatalf("restart node: %v", err)
	}
	defer func() {
		node2.Stop()
		eng2.Close()
		os.RemoveAll(dir)
	}()

	// The snapshot metadata must have been loaded on Start(), proving the
	// fast-path engaged rather than starting fresh at lastIncludedIndex=0.
	if got := node2.SnapshotIndex(); got != 5 {
		t.Fatalf("after restart: SnapshotIndex got %d, want 5 (fast-path did not engage)", got)
	}

	// All 8 original writes must still be readable - 1-5 via the snapshot
	// (engine.Open already replayed its own SSTables independent of Raft),
	// 6-8 via normal WAL replay of the entries after the snapshot boundary.
	for i := 1; i <= 8; i++ {
		key := fmt.Sprintf("key%02d", i)
		want := fmt.Sprintf("val%02d", i)
		val, found := eng2.Get(key)
		if !found || string(val) != want {
			t.Fatalf("after restart: get %s = %q %v, want %q", key, val, found, want)
		}
	}
}

// TestWatchStreamsAppliedWrites verifies that a Subscribe()d change-data-
// capture consumer receives every applied write, in commit order, with the
// right op/key/value.
func TestWatchStreamsAppliedWrites(t *testing.T) {
	sm, cleanup := makeTestSM(t)
	defer cleanup()

	events, unsubscribe := sm.Subscribe("")
	defer unsubscribe()

	if err := sm.ProposeWrite(cmd("put", "a", []byte("1"), "c1", 1)); err != nil {
		t.Fatalf("put a: %v", err)
	}
	if err := sm.ProposeWrite(cmd("put", "b", []byte("2"), "c1", 2)); err != nil {
		t.Fatalf("put b: %v", err)
	}
	if err := sm.ProposeWrite(cmd("delete", "a", nil, "c1", 3)); err != nil {
		t.Fatalf("delete a: %v", err)
	}

	want := []struct {
		op  pb.OpType
		key string
		val string
	}{
		{pb.OpType_OP_PUT, "a", "1"},
		{pb.OpType_OP_PUT, "b", "2"},
		{pb.OpType_OP_DELETE, "a", ""},
	}

	for i, w := range want {
		select {
		case event := <-events:
			if event.Op != w.op || event.Key != w.key || string(event.Value) != w.val {
				t.Fatalf("event %d: got op=%v key=%q val=%q, want op=%v key=%q val=%q",
					i, event.Op, event.Key, event.Value, w.op, w.key, w.val)
			}
		case <-time.After(2 * time.Second):
			t.Fatalf("event %d: timeout waiting for change event", i)
		}
	}
}

// TestWatchFiltersByKeyPrefix verifies that a Watch subscriber with a
// key_prefix only receives events for matching keys.
func TestWatchFiltersByKeyPrefix(t *testing.T) {
	sm, cleanup := makeTestSM(t)
	defer cleanup()

	events, unsubscribe := sm.Subscribe("user:")
	defer unsubscribe()

	if err := sm.ProposeWrite(cmd("put", "user:1", []byte("alice"), "c1", 1)); err != nil {
		t.Fatalf("put user:1: %v", err)
	}
	if err := sm.ProposeWrite(cmd("put", "order:1", []byte("widget"), "c1", 2)); err != nil {
		t.Fatalf("put order:1: %v", err)
	}
	if err := sm.ProposeWrite(cmd("put", "user:2", []byte("bob"), "c1", 3)); err != nil {
		t.Fatalf("put user:2: %v", err)
	}

	for _, wantKey := range []string{"user:1", "user:2"} {
		select {
		case event := <-events:
			if event.Key != wantKey {
				t.Fatalf("got key %q, want %q", event.Key, wantKey)
			}
		case <-time.After(2 * time.Second):
			t.Fatalf("timeout waiting for %q", wantKey)
		}
	}

	select {
	case event := <-events:
		t.Fatalf("unexpected event for non-matching prefix: %+v", event)
	case <-time.After(200 * time.Millisecond):
		// Correctly filtered out - order:1 never arrives.
	}
}

// TestWatchDropsSlowSubscriber verifies that a subscriber which never
// drains its channel is disconnected once it exceeds its buffer, rather
// than being allowed to block the apply loop (and therefore every write in
// the cluster) waiting for it to catch up.
func TestWatchDropsSlowSubscriber(t *testing.T) {
	sm, cleanup := makeTestSM(t)
	defer cleanup()

	events, unsubscribe := sm.Subscribe("")
	defer unsubscribe()

	for i := 0; i < watchSubscriberBuffer+10; i++ {
		key := fmt.Sprintf("k%d", i)
		if err := sm.ProposeWrite(cmd("put", key, []byte("v"), "c1", uint64(i+1))); err != nil {
			t.Fatalf("put %d: %v", i, err)
		}
	}

	deadline := time.After(2 * time.Second)
	for {
		select {
		case _, ok := <-events:
			if !ok {
				return // dropped, as expected
			}
		case <-deadline:
			t.Fatal("subscriber was not dropped after exceeding its buffer")
		}
	}
}

// BenchmarkProposeWrite measures the full local write path this codebase
// didn't previously have a benchmark for: ProposeWrite → Raft log append +
// WAL persist → (single-node quorum, so commit is immediate) → apply loop →
// LSM engine write → client unblocked. This does NOT include real network
// round-trips to followers - makeTestSM's cluster has one node, so
// consensus overhead here is real (Propose, log append, the ApplyCh
// round-trip through the apply-loop goroutine) but there's no peer RTT to
// wait for. A multi-node deployment adds whatever the network adds on top
// of this baseline.
func BenchmarkProposeWrite(b *testing.B) {
	dir, err := os.MkdirTemp("", "server-bench-*")
	if err != nil {
		b.Fatal(err)
	}
	defer os.RemoveAll(dir)

	eng, err := storage.Open(dir + "/storage")
	if err != nil {
		b.Fatal(err)
	}
	defer eng.Close()

	ws, err := newWALState(dir + "/wal")
	if err != nil {
		b.Fatal(err)
	}

	net := raft.NewNetwork()
	node := raft.NewNode(raft.Config{ID: "node1", Peers: map[string]string{}}, net.TransportFor("node1"), ws)
	net.Add("node1", node)
	if err := node.Start(); err != nil {
		b.Fatal(err)
	}
	defer node.Stop()

	deadline := time.Now().Add(2 * time.Second)
	for !node.IsLeader() {
		if time.Now().After(deadline) {
			b.Fatal("node did not become leader within 2s")
		}
		time.Sleep(10 * time.Millisecond)
	}

	sm := NewStateMachine(node, eng)
	defer sm.Stop()

	val := make([]byte, 64) // representative small-value write
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("bench-%d", i)
		if err := sm.ProposeWrite(cmd("put", key, val, "bench-client", uint64(i+1))); err != nil {
			b.Fatalf("propose %d: %v", i, err)
		}
	}
}

// restartBenchEntries is how many entries get written before each simulated
// crash+restart in BenchmarkRestart{With,Without}Snapshot.
const restartBenchEntries = 2000

// BenchmarkRestartWithoutSnapshot measures cold-start time - Start() plus
// waiting for the apply loop to catch back up - after restartBenchEntries
// entries with no snapshotting: full WAL replay, the only path available
// before this project had snapshotting at all.
func BenchmarkRestartWithoutSnapshot(b *testing.B) {
	benchmarkRestart(b, 0) // snapshotInterval=0 disables snapshotting
}

// BenchmarkRestartWithSnapshot measures the same restart with snapshotting
// enabled every 200 entries, so restart only replays the tail after the
// last snapshot instead of the full log.
func BenchmarkRestartWithSnapshot(b *testing.B) {
	benchmarkRestart(b, 200)
}

func benchmarkRestart(b *testing.B, snapshotInterval uint64) {
	for i := 0; i < b.N; i++ {
		b.StopTimer()

		dir, err := os.MkdirTemp("", "restart-bench-*")
		if err != nil {
			b.Fatal(err)
		}

		eng, err := storage.Open(dir + "/storage")
		if err != nil {
			b.Fatal(err)
		}
		ws, err := newWALState(dir + "/wal")
		if err != nil {
			b.Fatal(err)
		}
		net := raft.NewNetwork()
		node := raft.NewNode(raft.Config{ID: "node1", Peers: map[string]string{}}, net.TransportFor("node1"), ws)
		net.Add("node1", node)
		if err := node.Start(); err != nil {
			b.Fatal(err)
		}
		waitForLeaderB(b, node)

		var opts []Option
		if snapshotInterval > 0 {
			opts = append(opts, WithSnapshotInterval(snapshotInterval))
		}
		sm := NewStateMachine(node, eng, opts...)
		for j := 0; j < restartBenchEntries; j++ {
			key := fmt.Sprintf("k%d", j)
			if err := sm.ProposeWrite(cmd("put", key, []byte("v"), "seed-client", uint64(j+1))); err != nil {
				b.Fatalf("seed propose %d: %v", j, err)
			}
		}
		sm.Stop()
		node.Stop()
		eng.Close()

		// ── Timed region: the actual restart cost ──
		// node2.Start() (WAL/snapshot loading) is timed. waitForLeaderB
		// deliberately is NOT: it's dominated by this node's own randomized
		// 150-300ms election timeout, which has nothing to do with whether
		// snapshotting sped up loading - it's an artifact of this being a
		// single-node benchmark cluster. A restarted follower in a real
		// multi-node deployment doesn't wait on its own election timer at
		// all; it just resumes receiving heartbeats from whichever leader
		// is already there. Leaving that noise in the timed region was
		// tried first and completely swamped the actual signal (a few ms
		// either way, next to a ~200ms roll of the dice).
		b.StartTimer()

		eng2, err := storage.Open(dir + "/storage")
		if err != nil {
			b.Fatal(err)
		}
		net2 := raft.NewNetwork()
		ws2, err := newWALState(dir + "/wal")
		if err != nil {
			b.Fatal(err)
		}
		node2 := raft.NewNode(raft.Config{ID: "node1", Peers: map[string]string{}}, net2.TransportFor("node1"), ws2)
		net2.Add("node1", node2)
		if err := node2.Start(); err != nil {
			b.Fatal(err)
		}

		b.StopTimer()
		waitForLeaderB(b, node2)
		b.StartTimer()

		sm2 := NewStateMachine(node2, eng2)

		// The current-term-commit safety rule (see docs/design-decisions.md)
		// means the pre-restart entries don't get re-marked committed on
		// their own: a freshly-elected leader only ever commits entries
		// from its own (new, higher) term, and every one of those 2000
		// entries was written in an earlier term. They become committed as
		// a side effect of this first new-term write - which is exactly the
		// mechanism a real restarted node relies on to resume serving
		// writes at all, so it's a fair thing to include in "restart time."
		// It's also where the actual snapshot-vs-replay difference shows
		// up: ProposeWrite doesn't return until its entry is committed AND
		// applied, and the (single-threaded, strictly-ordered) apply loop
		// must finish applying everything before it first - all 2001
		// entries without a snapshot, or just this 1 with one, since
		// commitIndex was already seeded to 2000.
		if err := sm2.ProposeWrite(cmd("put", "post-restart", []byte("v"), "seed-client", restartBenchEntries+1)); err != nil {
			b.Fatalf("post-restart propose: %v", err)
		}

		catchupDeadline := time.Now().Add(10 * time.Second)
		for sm2.LastApplied() < restartBenchEntries+1 {
			if time.Now().After(catchupDeadline) {
				b.Fatalf("restart did not catch up within 10s (lastApplied=%d, want %d)",
					sm2.LastApplied(), restartBenchEntries+1)
			}
			time.Sleep(time.Millisecond)
		}

		b.StopTimer()

		sm2.Stop()
		node2.Stop()
		eng2.Close()
		os.RemoveAll(dir)
	}
}

func waitForLeaderB(b *testing.B, node *raft.Node) {
	b.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for !node.IsLeader() {
		if time.Now().After(deadline) {
			b.Fatal("node did not become leader within 2s")
		}
		time.Sleep(5 * time.Millisecond)
	}
}
