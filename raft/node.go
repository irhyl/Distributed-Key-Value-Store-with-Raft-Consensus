// Package raft implements the Raft consensus algorithm.
//
// Key properties guaranteed:
//   - Election safety: at most one leader per term
//   - Log matching: if two logs have the same (index, term), all preceding entries are identical
//   - Leader completeness: if a log entry is committed, all future leaders have it
//   - State machine safety: if a server applies an entry at index N, no other server
//     applies a different entry at index N
//
// This file: the core Node — state machine, RPC handlers, election timer, log replication.

package raft

import (
	"fmt"
	"log"
	"math/rand"
	"sync"
	"time"

	pb "github.com/raftkv/proto"
)

// Role represents the three states a Raft node can be in.
type Role int

const (
	RoleFollower  Role = iota
	RoleCandidate Role = iota
	RoleLeader    Role = iota
)

func (r Role) String() string {
	return [...]string{"Follower", "Candidate", "Leader"}[r]
}

const (
	// Election timeout: if a follower doesn't hear from a leader in this window,
	// it starts a new election. Randomized to prevent split votes.
	electionTimeoutMin = 150 * time.Millisecond
	electionTimeoutMax = 300 * time.Millisecond

	// Leader sends heartbeats at this interval to prevent followers from timing out.
	// Must be << electionTimeoutMin.
	heartbeatInterval = 50 * time.Millisecond
)

// LogEntry wraps the proto type with a convenience constructor.
type LogEntry = pb.LogEntry

// CommitNotify is sent on the commitCh whenever entries are newly committed.
type CommitNotify struct {
	Entry *LogEntry
}

// Config holds the static configuration for a Raft node.
type Config struct {
	ID      string            // this node's ID (e.g. "node1")
	Peers   map[string]string // map of nodeID → gRPC address for all OTHER nodes
	DataDir string            // where to store WAL and snapshots
}

// Node is a single Raft participant.
// It communicates with peers via the Transport interface and
// notifies the application layer of committed entries via CommitCh.
type Node struct {
	mu  sync.Mutex
	cfg Config

	// ── Persistent state (must survive crash — stored in WAL) ──
	currentTerm uint64     // latest term this node has seen
	votedFor    string     // candidateID we voted for in currentTerm (or "")
	log         []*LogEntry // the replicated log (index 0 = dummy entry)

	// ── Volatile state (reconstructed after crash) ──
	role        Role
	leaderID    string // current leader's ID; updated on every valid AppendEntries
	commitIndex uint64 // highest log index known to be committed
	lastApplied uint64 // highest log index applied to state machine

	// ── Leader-only volatile state (reinitialized after each election) ──
	nextIndex  map[string]uint64 // for each peer: next log index to send
	matchIndex map[string]uint64 // for each peer: highest index known replicated

	// ── Infrastructure ──
	transport  Transport       // how to send RPCs to peers
	storage    PersistentState // saves currentTerm, votedFor, log to disk

	// ── Timers ──
	electionTimer  *time.Timer
	heartbeatTimer *time.Timer

	// ── Channels ──
	CommitCh chan CommitNotify // application reads committed entries from here
	stopCh   chan struct{}
	wg       sync.WaitGroup
}

// Transport is how a Raft node sends RPCs to its peers.
// The gRPC implementation lives in server/transport.go.
// This interface makes the Raft core testable without real network I/O.
type Transport interface {
	RequestVote(peerID string, req *pb.RequestVoteRequest) (*pb.RequestVoteResponse, error)
	AppendEntries(peerID string, req *pb.AppendEntriesRequest) (*pb.AppendEntriesResponse, error)
	InstallSnapshot(peerID string, req *pb.InstallSnapshotRequest) (*pb.InstallSnapshotResponse, error)
}

// PersistentState abstracts durable storage for Raft's hard state.
type PersistentState interface {
	SaveHardState(term uint64, votedFor string) error
	LoadHardState() (term uint64, votedFor string, err error)
	AppendEntries(entries []*LogEntry) error
	LoadEntries() ([]*LogEntry, error)
	TruncateSuffix(keepIndex uint64) error
}

// NewNode creates a new Raft node. Call Start() to begin participating.
func NewNode(cfg Config, transport Transport, storage PersistentState) *Node {
	n := &Node{
		cfg:       cfg,
		role:      RoleFollower,
		transport: transport,
		storage:   storage,
		CommitCh:  make(chan CommitNotify, 4096),
		stopCh:    make(chan struct{}),
		nextIndex:  make(map[string]uint64),
		matchIndex: make(map[string]uint64),
	}

	// Dummy entry at index 0 so that log indexing is 1-based.
	// This simplifies the "prevLogIndex = 0" case in AppendEntries.
	n.log = []*LogEntry{{Index: 0, Term: 0}}

	return n
}

// Start loads persisted state and begins the Raft protocol.
func (n *Node) Start() error {
	// Recover persisted hard state
	term, votedFor, err := n.storage.LoadHardState()
	if err != nil {
		return fmt.Errorf("raft: load hard state: %w", err)
	}
	n.currentTerm = term
	n.votedFor = votedFor

	// Recover log
	entries, err := n.storage.LoadEntries()
	if err != nil {
		return fmt.Errorf("raft: load log: %w", err)
	}
	n.log = append(n.log, entries...)

	n.resetElectionTimer()

	n.wg.Add(1)
	go n.run()

	log.Printf("[%s] started: term=%d log_len=%d", n.cfg.ID, n.currentTerm, len(n.log)-1)
	return nil
}

// Stop shuts down the node. Safe to call multiple times.
func (n *Node) Stop() {
	select {
	case <-n.stopCh:
		return // already stopped
	default:
		close(n.stopCh)
	}
	if n.electionTimer != nil {
		n.electionTimer.Stop()
	}
	if n.heartbeatTimer != nil {
		n.heartbeatTimer.Stop()
	}
	n.wg.Wait()
}

// Propose submits a new command to the cluster.
// Returns (logIndex, term, isLeader).
// If this node is not the leader, the caller should redirect to the leader.
func (n *Node) Propose(data []byte) (uint64, uint64, bool) {
	n.mu.Lock()
	defer n.mu.Unlock()

	if n.role != RoleLeader {
		return 0, 0, false
	}

	entry := &LogEntry{
		Index: n.lastLogIndex() + 1,
		Term:  n.currentTerm,
		Data:  data,
	}

	n.log = append(n.log, entry)
	if err := n.storage.AppendEntries([]*LogEntry{entry}); err != nil {
		log.Printf("[%s] ERROR: persist proposed entry %d: %v", n.cfg.ID, entry.Index, err)
	}

	// Immediately replicate to all peers (don't wait for the heartbeat tick)
	// Self is always counted implicitly in maybeAdvanceCommit (the +1 base count).
	for peerID := range n.cfg.Peers {
		go n.replicateToPeer(peerID)
	}

	// Check commit immediately: in a single-node cluster quorum is already
	// satisfied, so the entry commits right here without waiting for peers.
	n.maybeAdvanceCommit()

	return entry.Index, entry.Term, true
}

// LeaderID returns the current leader's ID, or "" if unknown.
// Leaders return their own ID; followers return the last leader they heard from.
func (n *Node) LeaderID() string {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.leaderID
}

// IsLeader reports whether this node believes itself to be the current leader.
func (n *Node) IsLeader() bool {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.role == RoleLeader
}

// GetState returns (currentTerm, isLeader) — useful for tests.
func (n *Node) GetState() (uint64, bool) {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.currentTerm, n.role == RoleLeader
}

// ── Main event loop ────────────────────────────────────────────────────────────

// run is the main goroutine. It processes timer events only;
// RPC handlers can run concurrently (they lock mu themselves).
func (n *Node) run() {
	defer n.wg.Done()
	for {
		select {
		case <-n.stopCh:
			return
		case <-n.electionTimerC():
			n.mu.Lock()
			if n.role != RoleLeader {
				n.startElection()
			}
			n.mu.Unlock()
		case <-n.heartbeatTimerC():
			n.mu.Lock()
			if n.role == RoleLeader {
				n.sendHeartbeats()
				n.resetHeartbeatTimer()
			}
			n.mu.Unlock()
		}
	}
}

// ── Elections ─────────────────────────────────────────────────────────────────

// startElection transitions to Candidate and sends RequestVote to all peers.
// MUST be called with n.mu held.
func (n *Node) startElection() {
	n.currentTerm++
	n.role = RoleCandidate
	n.votedFor = n.cfg.ID // vote for ourselves
	if err := n.storage.SaveHardState(n.currentTerm, n.votedFor); err != nil {
		log.Printf("[%s] ERROR: save hard state: %v", n.cfg.ID, err)
	}
	n.resetElectionTimer()

	term := n.currentTerm
	lastIdx := n.lastLogIndex()
	lastTerm := n.lastLogTerm()

	log.Printf("[%s] starting election: term=%d", n.cfg.ID, term)

	// Count our own vote
	votes := 1
	needed := n.quorum()

	// Single-node cluster: our own vote is sufficient — become leader immediately.
	if votes >= needed {
		n.becomeLeader()
		return
	}

	// Ask all peers for votes concurrently (only peers, not self)
	var voteMu sync.Mutex
	for peerID := range n.cfg.Peers {
		go func(peer string) {
			req := &pb.RequestVoteRequest{
				Term:         term,
				CandidateId:  n.cfg.ID,
				LastLogIndex: lastIdx,
				LastLogTerm:  lastTerm,
			}
			resp, err := n.transport.RequestVote(peer, req)
			if err != nil {
				return
			}

			n.mu.Lock()
			defer n.mu.Unlock()

			// If we've moved to a newer term, abandon this election
			if resp.Term > n.currentTerm {
				n.becomeFollower(resp.Term)
				return
			}

			if resp.VoteGranted && n.role == RoleCandidate && n.currentTerm == term {
				voteMu.Lock()
				votes++
				won := votes >= needed
				voteMu.Unlock()

				if won {
					n.becomeLeader()
				}
			}
		}(peerID)
	}
}

// becomeLeader transitions this node to Leader.
// MUST be called with n.mu held.
func (n *Node) becomeLeader() {
	n.role = RoleLeader
	n.leaderID = n.cfg.ID
	log.Printf("[%s] became leader: term=%d", n.cfg.ID, n.currentTerm)

	// Reinitialize per-peer tracking
	nextIdx := n.lastLogIndex() + 1
	for peerID := range n.cfg.Peers {
		n.nextIndex[peerID] = nextIdx
		n.matchIndex[peerID] = 0
	}

	// Send immediate heartbeats so followers don't start a new election
	n.sendHeartbeats()
	n.resetHeartbeatTimer()
	n.electionTimer.Stop()
}

// becomeFollower steps down to Follower, updating term if needed.
// MUST be called with n.mu held.
func (n *Node) becomeFollower(term uint64) {
	n.role = RoleFollower
	n.leaderID = "" // unknown until we get an AppendEntries
	if term > n.currentTerm {
		n.currentTerm = term
		n.votedFor = ""
		if err := n.storage.SaveHardState(n.currentTerm, n.votedFor); err != nil {
			log.Printf("[%s] ERROR: save hard state on term update: %v", n.cfg.ID, err)
		}
	}
	n.resetElectionTimer()
}

// ── RPC Handlers ──────────────────────────────────────────────────────────────

// HandleRequestVote processes an incoming RequestVote RPC.
// Called by the gRPC server on receipt of a vote request from a candidate.
func (n *Node) HandleRequestVote(req *pb.RequestVoteRequest) *pb.RequestVoteResponse {
	n.mu.Lock()
	defer n.mu.Unlock()

	resp := &pb.RequestVoteResponse{Term: n.currentTerm}

	// Rule 1: Reject if candidate's term is stale
	if req.Term < n.currentTerm {
		return resp // VoteGranted = false
	}

	// If we see a higher term, immediately convert to follower
	if req.Term > n.currentTerm {
		n.becomeFollower(req.Term)
		resp.Term = n.currentTerm
	}

	// Rule 2: Only vote if we haven't voted yet (or voted for this candidate already)
	alreadyVoted := n.votedFor != "" && n.votedFor != req.CandidateId
	if alreadyVoted {
		return resp
	}

	// Rule 3: Only vote if candidate's log is at least as up-to-date as ours.
	// "Up-to-date" = higher last term, or same last term with >= last index.
	// This prevents a stale candidate from becoming leader and losing committed entries.
	if !n.candidateLogUpToDate(req.LastLogIndex, req.LastLogTerm) {
		return resp
	}

	// Grant the vote
	n.votedFor = req.CandidateId
	if err := n.storage.SaveHardState(n.currentTerm, n.votedFor); err != nil {
		log.Printf("[%s] ERROR: save vote for %s: %v", n.cfg.ID, req.CandidateId, err)
	}
	n.resetElectionTimer() // reset so we don't start our own election immediately
	resp.VoteGranted = true
	return resp
}

// HandleAppendEntries processes an incoming AppendEntries RPC.
// This is the workhorse: it handles both heartbeats (empty entries) and
// actual log replication, and advances commitIndex when the leader says to.
func (n *Node) HandleAppendEntries(req *pb.AppendEntriesRequest) *pb.AppendEntriesResponse {
	n.mu.Lock()
	defer n.mu.Unlock()

	resp := &pb.AppendEntriesResponse{Term: n.currentTerm}

	// Reject stale leaders
	if req.Term < n.currentTerm {
		return resp // success = false
	}

	// Valid leader — reset election timer and update term if needed
	if req.Term > n.currentTerm {
		n.becomeFollower(req.Term)
	} else {
		n.role = RoleFollower
		n.resetElectionTimer()
	}
	n.leaderID = req.LeaderId // track for client redirect hints
	resp.Term = n.currentTerm

	// Log consistency check:
	// The leader tells us what it thinks our last entry is (prevLogIndex, prevLogTerm).
	// If we don't have that entry (or it has a different term), our logs have diverged.
	if req.PrevLogIndex > 0 {
		if req.PrevLogIndex > n.lastLogIndex() {
			// We're missing entries — tell leader where our log ends
			resp.ConflictIndex = n.lastLogIndex() + 1
			resp.ConflictTerm = 0
			return resp
		}
		if n.log[req.PrevLogIndex].Term != req.PrevLogTerm {
			// Term mismatch — find the first index of the conflicting term for fast rollback
			conflictTerm := n.log[req.PrevLogIndex].Term
			resp.ConflictTerm = conflictTerm
			resp.ConflictIndex = req.PrevLogIndex
			for resp.ConflictIndex > 1 && n.log[resp.ConflictIndex-1].Term == conflictTerm {
				resp.ConflictIndex--
			}
			return resp
		}
	}

	// Append any new entries, truncating conflicting entries first
	for i, entry := range req.Entries {
		idx := req.PrevLogIndex + uint64(i) + 1
		if idx < uint64(len(n.log)) {
			if n.log[idx].Term != entry.Term {
				// Conflict: truncate our log here and append from leader
				n.log = n.log[:idx]
				if err := n.storage.TruncateSuffix(idx - 1); err != nil {
					log.Printf("[%s] ERROR: truncate log at %d: %v", n.cfg.ID, idx-1, err)
				}
			} else {
				continue // entry already in our log, skip
			}
		}
		n.log = append(n.log, entry)
	}

	// Persist new entries
	if len(req.Entries) > 0 {
		if err := n.storage.AppendEntries(req.Entries); err != nil {
			log.Printf("[%s] ERROR: persist %d entries from leader: %v", n.cfg.ID, len(req.Entries), err)
		}
	}

	// Advance commitIndex if leader's commit is ahead of ours
	if req.LeaderCommit > n.commitIndex {
		newCommit := req.LeaderCommit
		if last := n.lastLogIndex(); last < newCommit {
			newCommit = last
		}
		n.advanceCommitTo(newCommit)
	}

	resp.Success = true
	resp.MatchIndex = n.lastLogIndex()
	return resp
}

// HandleInstallSnapshot processes a snapshot sent by the leader.
// Called when a follower is so far behind that sending individual log entries
// would be more expensive than sending the full state.
func (n *Node) HandleInstallSnapshot(req *pb.InstallSnapshotRequest) *pb.InstallSnapshotResponse {
	n.mu.Lock()
	defer n.mu.Unlock()

	resp := &pb.InstallSnapshotResponse{Term: n.currentTerm}

	if req.Term < n.currentTerm {
		return resp
	}
	if req.Term > n.currentTerm {
		n.becomeFollower(req.Term)
	}

	// In a full implementation: write req.Data to disk, then apply the snapshot
	// to the state machine. For now we just update our term tracking.
	// The snapshot application logic lives in server/statemachine.go.
	resp.Term = n.currentTerm
	return resp
}

// ── Log replication ───────────────────────────────────────────────────────────

// sendHeartbeats sends AppendEntries to all peers.
// With no new entries, this is a pure heartbeat (prevents election timeout).
// MUST be called with n.mu held.
func (n *Node) sendHeartbeats() {
	for peerID := range n.cfg.Peers {
		go n.replicateToPeer(peerID)
	}
}

// replicateToPeer sends the appropriate AppendEntries to a single peer.
// It handles both heartbeats (when peer is caught up) and actual replication
// (when the peer is behind). If the peer is very far behind, it sends a snapshot.
func (n *Node) replicateToPeer(peerID string) {
	n.mu.Lock()
	if n.role != RoleLeader {
		n.mu.Unlock()
		return
	}

	nextIdx := n.nextIndex[peerID]
	prevLogIndex := nextIdx - 1

	// Collect entries to send (from nextIdx to end of log)
	var entries []*LogEntry
	if nextIdx <= n.lastLogIndex() {
		entries = make([]*LogEntry, len(n.log[nextIdx:]))
		copy(entries, n.log[nextIdx:])
	}

	var prevLogTerm uint64
	if prevLogIndex > 0 && prevLogIndex < uint64(len(n.log)) {
		prevLogTerm = n.log[prevLogIndex].Term
	}

	req := &pb.AppendEntriesRequest{
		Term:         n.currentTerm,
		LeaderId:     n.cfg.ID,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  prevLogTerm,
		Entries:      entries,
		LeaderCommit: n.commitIndex,
	}
	n.mu.Unlock()

	resp, err := n.transport.AppendEntries(peerID, req)
	if err != nil {
		return // peer is unreachable — will retry on next heartbeat
	}

	n.mu.Lock()
	defer n.mu.Unlock()

	if resp.Term > n.currentTerm {
		n.becomeFollower(resp.Term)
		return
	}

	if n.role != RoleLeader || n.currentTerm != req.Term {
		return // we're no longer leader, discard this response
	}

	if resp.Success {
		// Update our tracking for this peer
		newMatch := req.PrevLogIndex + uint64(len(entries))
		if newMatch > n.matchIndex[peerID] {
			n.matchIndex[peerID] = newMatch
			n.nextIndex[peerID] = newMatch + 1
		}

		// Check if we can advance commitIndex
		// Raft rule: a leader can only commit entries from its current term
		n.maybeAdvanceCommit()
	} else {
		// Log mismatch — back up nextIndex using the conflict hint
		if resp.ConflictTerm > 0 {
			// Find the last entry in our log with ConflictTerm
			newNext := resp.ConflictIndex
			for i := n.lastLogIndex(); i >= 1; i-- {
				if n.log[i].Term == resp.ConflictTerm {
					newNext = i + 1
					break
				}
			}
			n.nextIndex[peerID] = newNext
		} else {
			n.nextIndex[peerID] = resp.ConflictIndex
		}
		if n.nextIndex[peerID] < 1 {
			n.nextIndex[peerID] = 1
		}

		// Immediately retry with the corrected nextIndex
		go n.replicateToPeer(peerID)
	}
}

// maybeAdvanceCommit checks whether a new log entry can be committed.
// An entry is committed once a majority of nodes have it in their log.
// MUST be called with n.mu held.
func (n *Node) maybeAdvanceCommit() {
	for idx := n.lastLogIndex(); idx > n.commitIndex; idx-- {
		// Only commit entries from the current term
		// (Raft's rule to prevent the "leader completeness" violation)
		if n.log[idx].Term != n.currentTerm {
			continue
		}

		// Count replications: self (1) + each peer that has acknowledged this index
		count := 1 // leader always has the entry it just appended
		for peerID, match := range n.matchIndex {
			if peerID == n.cfg.ID {
				continue // don't double-count self
			}
			if match >= idx {
				count++
			}
		}

		if count >= n.quorum() {
			n.advanceCommitTo(idx)
			break
		}
	}
}

// advanceCommitTo advances commitIndex to newCommit and notifies the application.
// Entries are collected under the lock, then sent without holding it so that
// RPC handlers are not blocked while the state machine drains the channel.
// MUST be called with n.mu held.
func (n *Node) advanceCommitTo(newCommit uint64) {
	var toNotify []*LogEntry
	for i := n.commitIndex + 1; i <= newCommit; i++ {
		if i < uint64(len(n.log)) {
			toNotify = append(toNotify, n.log[i])
		}
	}
	n.commitIndex = newCommit

	if len(toNotify) == 0 {
		return
	}

	// Release the lock while sending so RPC handlers can still acquire it.
	// Entries are already committed; releasing here is safe.
	n.mu.Unlock()
	for _, entry := range toNotify {
		select {
		case n.CommitCh <- CommitNotify{Entry: entry}:
		case <-n.stopCh:
			n.mu.Lock()
			return
		}
	}
	n.mu.Lock()
}

// ── Helpers ───────────────────────────────────────────────────────────────────

func (n *Node) lastLogIndex() uint64 {
	return uint64(len(n.log) - 1)
}

func (n *Node) lastLogTerm() uint64 {
	if len(n.log) == 0 {
		return 0
	}
	return n.log[len(n.log)-1].Term
}

func (n *Node) quorum() int {
	// Total nodes = this node + all peers
	total := 1 + len(n.cfg.Peers)
	return total/2 + 1
}

// candidateLogUpToDate returns true if the candidate's log is at least as
// up-to-date as ours. This is the "election restriction" from Section 5.4.1.
func (n *Node) candidateLogUpToDate(candidateLastIdx, candidateLastTerm uint64) bool {
	myLastTerm := n.lastLogTerm()
	myLastIdx := n.lastLogIndex()

	if candidateLastTerm != myLastTerm {
		return candidateLastTerm > myLastTerm
	}
	return candidateLastIdx >= myLastIdx
}

// ── Timer management ──────────────────────────────────────────────────────────

func (n *Node) resetElectionTimer() {
	d := electionTimeoutMin + time.Duration(rand.Int63n(int64(electionTimeoutMax-electionTimeoutMin)))
	if n.electionTimer == nil {
		n.electionTimer = time.NewTimer(d)
	} else {
		n.electionTimer.Reset(d)
	}
}

func (n *Node) resetHeartbeatTimer() {
	if n.heartbeatTimer == nil {
		n.heartbeatTimer = time.NewTimer(heartbeatInterval)
	} else {
		n.heartbeatTimer.Reset(heartbeatInterval)
	}
}

func (n *Node) electionTimerC() <-chan time.Time {
	if n.electionTimer == nil {
		return nil
	}
	return n.electionTimer.C
}

func (n *Node) heartbeatTimerC() <-chan time.Time {
	if n.heartbeatTimer == nil {
		return nil
	}
	return n.heartbeatTimer.C
}
