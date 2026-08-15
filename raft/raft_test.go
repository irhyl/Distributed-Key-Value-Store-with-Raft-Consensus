package raft

import (
	"fmt"
	"testing"
	"time"
)

// ── Cluster helpers ────────────────────────────────────────────────────────────

// makeCluster creates N nodes wired together on an in-memory network.
func makeCluster(t *testing.T, n int) (*Network, []*Node) {
	t.Helper()

	ids := make([]string, n)
	for i := range ids {
		ids[i] = fmt.Sprintf("node%d", i+1)
	}

	net := NewNetwork()
	nodes := make([]*Node, n)

	for i, id := range ids {
		peers := make(map[string]string)
		for j, pid := range ids {
			if i != j {
				peers[pid] = pid // address = id in test (transport resolves by id)
			}
		}
		cfg := Config{ID: id, Peers: peers}
		node := NewNode(cfg, net.TransportFor(id), newMemState())
		nodes[i] = node
		net.Add(id, node)
	}

	for _, node := range nodes {
		if err := node.Start(); err != nil {
			t.Fatalf("start: %v", err)
		}
	}

	t.Cleanup(func() {
		for _, node := range nodes {
			node.Stop()
		}
	})

	return net, nodes
}

// waitForLeader polls until exactly one leader emerges and stays leader
// across a follow-up check. Returns the leader node. Fails the test if no
// stable leader within 3s.
//
// A single poll seeing one leader isn't enough: on cold start it's common
// for two nodes' election timers to fire close together, so a node can be
// the sole leader for one instant and get deposed moments later by a
// concurrent, higher-term election that was already in flight. A caller
// that immediately Propose()s against that transient leader would have its
// entry silently lost when leadership actually settles elsewhere — this
// showed up as an intermittent "did not commit within Ns" test flake.
// Re-checking after one heartbeat interval filters out that transient case.
func waitForLeader(t *testing.T, nodes []*Node) *Node {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		var leaders []*Node
		for _, n := range nodes {
			if n.IsLeader() {
				leaders = append(leaders, n)
			}
		}
		if len(leaders) == 1 {
			candidate := leaders[0]
			time.Sleep(heartbeatInterval * 2)
			if candidate.IsLeader() {
				return candidate
			}
			continue
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("no stable single leader emerged within 3s")
	return nil
}

// waitForCommit waits until a node has committed an entry with the given index.
func waitForCommit(t *testing.T, node *Node, targetIndex uint64, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		node.mu.Lock()
		ci := node.commitIndex
		node.mu.Unlock()
		if ci >= targetIndex {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("node %s did not commit index %d within %v", node.cfg.ID, targetIndex, timeout)
}

// ── Tests ──────────────────────────────────────────────────────────────────────

// TestNodeSeedsFromSnapshotOnStart verifies that a node whose PersistentState
// already has a saved snapshot picks up lastIncludedIndex/Term and seeds
// commitIndex from it on Start(), instead of always starting fresh at 0 —
// the whole point of a snapshot is to skip replaying history it covers.
func TestNodeSeedsFromSnapshotOnStart(t *testing.T) {
	state := newMemState()
	if err := state.SaveSnapshot([]byte("fake-snapshot-data"), 42, 5); err != nil {
		t.Fatalf("seed snapshot: %v", err)
	}
	if err := state.AppendEntries([]*LogEntry{
		{Index: 43, Term: 6},
		{Index: 44, Term: 6},
	}); err != nil {
		t.Fatalf("seed entries: %v", err)
	}

	net := NewNetwork()
	node := NewNode(Config{ID: "node1", Peers: map[string]string{}}, net.TransportFor("node1"), state)
	net.Add("node1", node)
	if err := node.Start(); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer node.Stop()

	node.mu.Lock()
	defer node.mu.Unlock()

	if node.lastIncludedIndex != 42 || node.lastIncludedTerm != 5 {
		t.Fatalf("lastIncludedIndex/Term: got %d/%d, want 42/5",
			node.lastIncludedIndex, node.lastIncludedTerm)
	}
	if node.commitIndex != 42 {
		t.Fatalf("commitIndex: got %d, want 42 (seeded from snapshot)", node.commitIndex)
	}
	if got := node.lastLogIndex(); got != 44 {
		t.Fatalf("lastLogIndex: got %d, want 44", got)
	}
	if got := node.lastLogTerm(); got != 6 {
		t.Fatalf("lastLogTerm: got %d, want 6", got)
	}
	pos, ok := node.logPos(42)
	if !ok || node.log[pos].Term != 5 {
		t.Fatalf("logPos(42): pos=%d ok=%v term=%d, want a valid position with term=5",
			pos, ok, node.log[pos].Term)
	}
}

// TestElectionBasic verifies that a 3-node cluster elects exactly one leader.
func TestElectionBasic(t *testing.T) {
	_, nodes := makeCluster(t, 3)
	leader := waitForLeader(t, nodes)

	term, isLeader := leader.GetState()
	if !isLeader {
		t.Fatal("waitForLeader returned a non-leader")
	}
	if term == 0 {
		t.Fatal("leader term should be > 0")
	}

	// Confirm all other nodes are followers in the same term
	for _, n := range nodes {
		if n == leader {
			continue
		}
		nTerm, nLeader := n.GetState()
		if nLeader {
			t.Fatalf("two leaders detected: %s and %s", leader.cfg.ID, n.cfg.ID)
		}
		if nTerm != term {
			t.Fatalf("follower %s in term %d, leader in term %d", n.cfg.ID, nTerm, term)
		}
	}
}

// TestElectionFiveNodes verifies election works with 5 nodes (quorum=3).
func TestElectionFiveNodes(t *testing.T) {
	_, nodes := makeCluster(t, 5)
	waitForLeader(t, nodes)
}

// TestReelectionAfterLeaderFailure verifies that if the leader is killed,
// the remaining nodes elect a new leader.
func TestReelectionAfterLeaderFailure(t *testing.T) {
	net, nodes := makeCluster(t, 3)
	leader := waitForLeader(t, nodes)
	oldTerm, _ := leader.GetState()

	// Kill the leader (Stop is idempotent so cleanup calling it again is fine)
	leaderID := leader.cfg.ID
	net.Disconnect(leaderID)
	leader.Stop()
	// Drain any pending goroutines from old leader to prevent timer races
	time.Sleep(50 * time.Millisecond)

	// Find remaining nodes
	var remaining []*Node
	for _, n := range nodes {
		if n.cfg.ID != leaderID {
			remaining = append(remaining, n)
		}
	}

	// A new leader should emerge
	newLeader := waitForLeader(t, remaining)
	newTerm, _ := newLeader.GetState()

	if newTerm <= oldTerm {
		t.Fatalf("new term %d should be > old term %d", newTerm, oldTerm)
	}

	t.Logf("Leader failed (term %d), new leader elected in term %d", oldTerm, newTerm)
}

// TestLogReplication verifies that a command proposed by the leader
// is committed on all followers.
func TestLogReplication(t *testing.T) {
	_, nodes := makeCluster(t, 3)
	leader := waitForLeader(t, nodes)

	// Propose a command
	idx, term, ok := leader.Propose([]byte("SET x=1"))
	if !ok {
		t.Fatal("propose failed")
	}
	if idx != 1 || term == 0 {
		t.Fatalf("propose: got idx=%d term=%d", idx, term)
	}

	// Wait for all nodes to commit it. Generous timeout: on occasional slow
	// cluster convergence (e.g. under system load) a 3-node cluster can go
	// through a few extra election rounds before settling, each costing up
	// to one full electionTimeoutMax; this budget covers that without
	// masking a genuine hang (see waitForLeader's doc comment).
	for _, n := range nodes {
		waitForCommit(t, n, 1, 5*time.Second)
	}

	// Verify all nodes have the same log
	for _, n := range nodes {
		n.mu.Lock()
		logLen := len(n.log)
		ci := n.commitIndex
		n.mu.Unlock()

		if logLen != 2 { // dummy + 1 entry
			t.Fatalf("node %s: log length %d, want 2", n.cfg.ID, logLen)
		}
		if ci != 1 {
			t.Fatalf("node %s: commitIndex %d, want 1", n.cfg.ID, ci)
		}
	}
}

// TestReplicationWithFollowerFailure verifies that a cluster of 3 can
// still commit entries when one follower is down (quorum = 2).
func TestReplicationWithFollowerFailure(t *testing.T) {
	net, nodes := makeCluster(t, 3)
	leader := waitForLeader(t, nodes)

	// Disconnect one follower
	var follower *Node
	for _, n := range nodes {
		if n != leader {
			follower = n
			break
		}
	}
	net.Disconnect(follower.cfg.ID)

	// Propose should still succeed with 2/3 nodes
	idx, _, ok := leader.Propose([]byte("SET y=2"))
	if !ok {
		t.Fatal("propose failed with one follower down")
	}

	// Leader and the connected follower should commit
	var connected []*Node
	for _, n := range nodes {
		if n.cfg.ID != follower.cfg.ID {
			connected = append(connected, n)
		}
	}
	for _, n := range connected {
		waitForCommit(t, n, idx, 5*time.Second)
	}

	// Reconnect the failed follower — it should catch up
	net.Reconnect(follower.cfg.ID)

	// Trigger replication by proposing another entry
	leader.Propose([]byte("SET z=3"))

	// Eventually all 3 should be in sync
	for _, n := range nodes {
		waitForCommit(t, n, idx, 3*time.Second)
	}
}

// TestNoCommitWithMinorityPartition verifies that a leader partitioned from
// the majority cannot commit new entries (no quorum).
func TestNoCommitWithMinorityPartition(t *testing.T) {
	net, nodes := makeCluster(t, 5)
	leader := waitForLeader(t, nodes)

	// Disconnect ALL followers from the leader.
	// Leader is now completely isolated: it can receive no AppendEntries ACKs.
	// Quorum needs 3/5; isolated leader only counts itself = 1. Cannot commit.
	for _, n := range nodes {
		if n != leader {
			net.Disconnect(n.cfg.ID)
		}
	}
	// Also disconnect the leader itself from sending (so its RPCs get dropped too)
	net.Disconnect(leader.cfg.ID)

	// Propose — ok=true is expected because the leader doesn't know it's isolated yet
	idx, _, ok := leader.Propose([]byte("SET isolated=true"))
	if !ok {
		// Leader may have already stepped down due to missed heartbeat ACKs — fine
		t.Log("leader already stepped down before propose")
		return
	}

	// Give plenty of time — the entry must NOT be committed (no quorum)
	time.Sleep(400 * time.Millisecond)

	leader.mu.Lock()
	ci := leader.commitIndex
	leader.mu.Unlock()

	if ci >= idx {
		t.Fatalf("leader committed entry %d while fully isolated — safety violation!", idx)
	}

	t.Logf("Correctly refused to commit entry %d while isolated (commitIndex=%d)", idx, ci)
}

// TestCommitNotification verifies that committed entries flow through CommitCh.
func TestCommitNotification(t *testing.T) {
	_, nodes := makeCluster(t, 3)
	leader := waitForLeader(t, nodes)

	commands := []string{"cmd1", "cmd2", "cmd3"}
	for _, cmd := range commands {
		_, _, ok := leader.Propose([]byte(cmd))
		if !ok {
			t.Fatalf("propose %q failed", cmd)
		}
	}

	// Drain CommitCh from leader — should receive all 3 commits
	received := make(map[string]bool)
	timeout := time.After(3 * time.Second)
	for len(received) < len(commands) {
		select {
		case notify := <-leader.CommitCh:
			received[string(notify.Entry.Data)] = true
		case <-timeout:
			t.Fatalf("timeout: only received %d/%d commits. got: %v", len(received), len(commands), received)
		}
	}
}
