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

	// snapshotChunkSize bounds a single InstallSnapshot RPC's payload so a
	// large snapshot doesn't block a peer connection with one huge message.
	snapshotChunkSize = 1 << 20 // 1MB
)

// LogEntry wraps the proto type with a convenience constructor.
type LogEntry = pb.LogEntry

// ApplyMsg is sent on ApplyCh whenever there's something new for the
// application layer to apply: either a newly committed log entry, or an
// installed snapshot that should replace all prior state. Shape modeled on
// the standard MIT 6.5840 Raft lab ApplyMsg.
type ApplyMsg struct {
	CommandValid bool
	Entry        *LogEntry

	SnapshotValid bool
	Snapshot      []byte
	SnapshotIndex uint64
	SnapshotTerm  uint64
}

// snapshotRecvState tracks reassembly of a chunked InstallSnapshot transfer.
type snapshotRecvState struct {
	lastIncludedIndex uint64
	lastIncludedTerm  uint64
	data              []byte
}

// Config holds the static configuration for a Raft node.
type Config struct {
	ID      string            // this node's ID (e.g. "node1")
	Peers   map[string]string // map of nodeID → gRPC address for all OTHER nodes
	DataDir string            // where to store WAL and snapshots
}

// Node is a single Raft participant.
// It communicates with peers via the Transport interface and notifies the
// application layer of committed entries and installed snapshots via ApplyCh.
type Node struct {
	mu  sync.Mutex
	cfg Config

	// ── Persistent state (must survive crash — stored in WAL) ──
	currentTerm uint64      // latest term this node has seen
	votedFor    string      // candidateID we voted for in currentTerm (or "")
	log         []*LogEntry // the replicated log tail still held in memory

	// lastIncludedIndex/Term describe the most recent snapshot: everything up
	// to and including this index has been compacted out of log/storage.
	// log[0] is always a sentinel entry {Index: lastIncludedIndex, Term:
	// lastIncludedTerm} — generalizing the pre-snapshot convention where
	// log[0] was the fixed {0,0} dummy entry. Use logPos() to translate a
	// Raft log index into a position in log; never index log[] directly with
	// a raw Raft index.
	lastIncludedIndex uint64
	lastIncludedTerm  uint64

	// ── Volatile state (reconstructed after crash) ──
	role        Role
	leaderID    string // current leader's ID; updated on every valid AppendEntries
	commitIndex uint64 // highest log index known to be committed

	// recvSnapshot accumulates chunks of an in-progress InstallSnapshot
	// transfer from the leader. nil except mid-transfer.
	recvSnapshot *snapshotRecvState

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
	ApplyCh chan ApplyMsg // application reads committed entries / snapshots from here
	stopCh  chan struct{}
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

	// SaveSnapshot durably persists snapshot data along with the Raft index
	// and term it covers. Must be atomic with respect to crashes: a reader
	// must never observe a partially-written snapshot.
	SaveSnapshot(data []byte, lastIncludedIndex, lastIncludedTerm uint64) error
	// LoadSnapshot returns the most recently saved snapshot, or a nil data
	// slice with lastIncludedIndex 0 if none has ever been saved.
	LoadSnapshot() (data []byte, lastIncludedIndex, lastIncludedTerm uint64, err error)
	// TruncatePrefix removes all log entries with index <= discardIndex,
	// called after a snapshot covering them has been durably saved.
	TruncatePrefix(discardIndex uint64) error
}

// NewNode creates a new Raft node. Call Start() to begin participating.
func NewNode(cfg Config, transport Transport, storage PersistentState) *Node {
	n := &Node{
		cfg:       cfg,
		role:      RoleFollower,
		transport: transport,
		storage:   storage,
		ApplyCh:   make(chan ApplyMsg, 4096),
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
	// Recover snapshot metadata first, if any. This seeds where our
	// in-memory log begins so we don't need entries the snapshot already
	// covers, and lets commitIndex start from the snapshot's index instead
	// of 0 — replaying already-committed history on every restart is exactly
	// what snapshotting exists to avoid. The snapshot bytes themselves are
	// NOT reapplied here: this node's own storage.Engine already reflects
	// this state (or newer) from its own on-disk SSTables. The bytes only
	// matter when a peer needs them via InstallSnapshot.
	_, lastIncludedIndex, lastIncludedTerm, err := n.storage.LoadSnapshot()
	if err != nil {
		return fmt.Errorf("raft: load snapshot: %w", err)
	}
	if lastIncludedIndex > 0 {
		n.lastIncludedIndex = lastIncludedIndex
		n.lastIncludedTerm = lastIncludedTerm
		n.log = []*LogEntry{{Index: lastIncludedIndex, Term: lastIncludedTerm}}
		n.commitIndex = lastIncludedIndex
	}

	// Recover persisted hard state
	term, votedFor, err := n.storage.LoadHardState()
	if err != nil {
		return fmt.Errorf("raft: load hard state: %w", err)
	}
	n.currentTerm = term
	n.votedFor = votedFor

	// Recover log entries after the snapshot baseline (if any)
	entries, err := n.storage.LoadEntries()
	if err != nil {
		return fmt.Errorf("raft: load log: %w", err)
	}
	n.log = append(n.log, entries...)

	n.resetElectionTimer()

	// Capture recovered state for logging before starting n.run(): once the
	// background goroutine is running, n.currentTerm/n.log may be mutated
	// (e.g. by a near-instant election) without n.mu held here, which is a
	// data race under the race detector even though it's log-line-only.
	startTerm, startLastIndex := n.currentTerm, n.lastLogIndex()

	n.wg.Add(1)
	go n.run()

	log.Printf("[%s] started: term=%d last_log_index=%d", n.cfg.ID, startTerm, startLastIndex)
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
		// electionTimer/heartbeatTimer are mutated by RPC handlers and
		// election goroutines under n.mu (see startElection, becomeLeader,
		// becomeFollower, HandleAppendEntries). Snapshot the channel values
		// under the same lock before selecting on them, rather than reading
		// the fields directly in the select — the latter races with those
		// writers even though the field values themselves change rarely.
		n.mu.Lock()
		electionC := n.electionTimerC()
		heartbeatC := n.heartbeatTimerC()
		n.mu.Unlock()

		select {
		case <-n.stopCh:
			return
		case <-electionC:
			n.mu.Lock()
			if n.role != RoleLeader {
				n.startElection()
			}
			n.mu.Unlock()
		case <-heartbeatC:
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
		if pos, ok := n.logPos(req.PrevLogIndex); ok {
			if n.log[pos].Term != req.PrevLogTerm {
				// Term mismatch — find the first index of the conflicting term for fast rollback
				conflictTerm := n.log[pos].Term
				resp.ConflictTerm = conflictTerm
				resp.ConflictIndex = req.PrevLogIndex
				for resp.ConflictIndex > n.lastIncludedIndex+1 {
					p, ok := n.logPos(resp.ConflictIndex - 1)
					if !ok || n.log[p].Term != conflictTerm {
						break
					}
					resp.ConflictIndex--
				}
				return resp
			}
		}
		// else: PrevLogIndex is at or before our snapshot baseline. Everything
		// up to lastIncludedIndex is committed by construction (a snapshot can
		// only cover applied entries), so the consistency check trivially
		// passes — fall through to appending entries.
	}

	// Append any new entries, truncating conflicting entries first
	for i, entry := range req.Entries {
		idx := req.PrevLogIndex + uint64(i) + 1
		if idx <= n.lastIncludedIndex {
			continue // already compacted into our snapshot — nothing to do
		}
		if pos, ok := n.logPos(idx); ok {
			if n.log[pos].Term != entry.Term {
				// Conflict: truncate our log here and append from leader
				n.log = n.log[:pos]
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
		return resp // Success = false
	}
	if req.Term > n.currentTerm {
		n.becomeFollower(req.Term)
	} else {
		// A snapshot from a valid leader is also proof of life, same as a
		// heartbeat — reset our election timer so we don't start a spurious
		// election while a (possibly large) transfer is in progress.
		n.role = RoleFollower
		n.resetElectionTimer()
	}
	n.leaderID = req.LeaderId
	resp.Term = n.currentTerm

	// Reject a snapshot that's no newer than what we already have. Without
	// this, a retried or reordered RPC (e.g. after a leader change) could
	// regress a follower that has already caught up normally via
	// AppendEntries. Success=true here because nothing is actually wrong —
	// we're just already ahead of or at this snapshot.
	if req.LastIncludedIndex <= n.lastIncludedIndex {
		resp.Success = true
		return resp
	}

	if req.Offset == 0 {
		// Start (or restart) a transfer — reset the reassembly buffer. A
		// leader restarting a failed transfer always begins at offset 0.
		n.recvSnapshot = &snapshotRecvState{
			lastIncludedIndex: req.LastIncludedIndex,
			lastIncludedTerm:  req.LastIncludedTerm,
		}
	}
	if n.recvSnapshot == nil ||
		n.recvSnapshot.lastIncludedIndex != req.LastIncludedIndex ||
		uint64(len(n.recvSnapshot.data)) != req.Offset {
		// Chunk doesn't fit where we expect (no transfer in progress, wrong
		// snapshot, or offset mismatch) — reject rather than mis-concatenate.
		// The leader will notice via Success=false and restart from offset 0.
		return resp
	}

	n.recvSnapshot.data = append(n.recvSnapshot.data, req.Data...)
	resp.Success = true

	if !req.Done {
		return resp
	}

	// Final chunk: install it.
	data := n.recvSnapshot.data
	lastIncludedIndex := n.recvSnapshot.lastIncludedIndex
	lastIncludedTerm := n.recvSnapshot.lastIncludedTerm
	n.recvSnapshot = nil

	if err := n.storage.SaveSnapshot(data, lastIncludedIndex, lastIncludedTerm); err != nil {
		log.Printf("[%s] ERROR: save installed snapshot: %v", n.cfg.ID, err)
		resp.Success = false
		return resp
	}
	if err := n.storage.TruncatePrefix(lastIncludedIndex); err != nil {
		log.Printf("[%s] ERROR: truncate prefix after installing snapshot: %v", n.cfg.ID, err)
	}

	// Conservative simplification vs. the Raft paper's "retain matching
	// suffix" optimization (§7): discard the entire in-memory log rather
	// than keeping any entries we already have that happen to match the
	// leader's. Simpler and always correct, at the cost of possibly
	// re-replicating a few entries we didn't strictly need to.
	n.log = []*LogEntry{{Index: lastIncludedIndex, Term: lastIncludedTerm}}
	n.lastIncludedIndex = lastIncludedIndex
	n.lastIncludedTerm = lastIncludedTerm
	if lastIncludedIndex > n.commitIndex {
		n.commitIndex = lastIncludedIndex
	}

	// Notify the application layer under the same lock advanceCommitTo uses,
	// so all ApplyCh producers share one total order.
	select {
	case n.ApplyCh <- ApplyMsg{
		SnapshotValid: true,
		Snapshot:      data,
		SnapshotIndex: lastIncludedIndex,
		SnapshotTerm:  lastIncludedTerm,
	}:
	case <-n.stopCh:
	}

	return resp
}

// CompactLog persists a snapshot covering entries up to and including
// lastIncludedIndex, then discards them from the in-memory log and durable
// log storage. Called by the application layer once it has produced a
// snapshot of its own state as of that index — Node has no opinion on when
// to snapshot, only how to record that one happened.
func (n *Node) CompactLog(data []byte, lastIncludedIndex, lastIncludedTerm uint64) error {
	n.mu.Lock()
	defer n.mu.Unlock()

	if lastIncludedIndex <= n.lastIncludedIndex {
		return nil // stale — we've already compacted at least this far
	}
	if lastIncludedIndex > n.commitIndex {
		return fmt.Errorf("raft: cannot compact past commitIndex (%d > %d)", lastIncludedIndex, n.commitIndex)
	}

	if err := n.storage.SaveSnapshot(data, lastIncludedIndex, lastIncludedTerm); err != nil {
		return fmt.Errorf("raft: save snapshot: %w", err)
	}
	if err := n.storage.TruncatePrefix(lastIncludedIndex); err != nil {
		return fmt.Errorf("raft: truncate prefix: %w", err)
	}

	if pos, ok := n.logPos(lastIncludedIndex); ok {
		// n.log[pos] is the real entry at lastIncludedIndex; its Index/Term
		// already match what a fresh sentinel would hold, so it becomes the
		// new log[0] simply by slicing — no need to construct a new one.
		n.log = n.log[pos:]
	} else {
		n.log = []*LogEntry{{Index: lastIncludedIndex, Term: lastIncludedTerm}}
	}
	n.lastIncludedIndex = lastIncludedIndex
	n.lastIncludedTerm = lastIncludedTerm

	return nil
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
	if nextIdx <= n.lastIncludedIndex {
		// The peer needs entries we've already compacted away — send a
		// snapshot instead of AppendEntries.
		n.mu.Unlock()
		n.sendSnapshotToPeer(peerID)
		return
	}

	prevLogIndex := nextIdx - 1

	// Collect entries to send (from nextIdx to end of log)
	var entries []*LogEntry
	if nextIdx <= n.lastLogIndex() {
		if pos, ok := n.logPos(nextIdx); ok {
			entries = make([]*LogEntry, len(n.log[pos:]))
			copy(entries, n.log[pos:])
		}
	}

	var prevLogTerm uint64
	if prevLogIndex > 0 {
		if pos, ok := n.logPos(prevLogIndex); ok {
			prevLogTerm = n.log[pos].Term
		}
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
			for i := n.lastLogIndex(); i > n.lastIncludedIndex; i-- {
				if pos, ok := n.logPos(i); ok && n.log[pos].Term == resp.ConflictTerm {
					newNext = i + 1
					break
				}
			}
			n.nextIndex[peerID] = newNext
		} else {
			n.nextIndex[peerID] = resp.ConflictIndex
		}
		if n.nextIndex[peerID] < n.lastIncludedIndex+1 {
			n.nextIndex[peerID] = n.lastIncludedIndex + 1
		}

		// Immediately retry with the corrected nextIndex
		go n.replicateToPeer(peerID)
	}
}

// sendSnapshotToPeer sends our current snapshot to a peer whose nextIndex
// has fallen at or behind lastIncludedIndex, in fixed-size chunks via
// InstallSnapshot. Called from replicateToPeer instead of the normal
// AppendEntries path in that case.
func (n *Node) sendSnapshotToPeer(peerID string) {
	n.mu.Lock()
	if n.role != RoleLeader {
		n.mu.Unlock()
		return
	}
	term := n.currentTerm
	lastIncludedIndex := n.lastIncludedIndex
	lastIncludedTerm := n.lastIncludedTerm
	n.mu.Unlock()

	data, savedIndex, _, err := n.storage.LoadSnapshot()
	if err != nil {
		log.Printf("[%s] ERROR: load snapshot to send to %s: %v", n.cfg.ID, peerID, err)
		return
	}
	if savedIndex != lastIncludedIndex {
		// Storage's saved snapshot doesn't match what we read under the lock
		// above (a newer CompactLog raced in) — bail and let the next
		// heartbeat retry with up-to-date values.
		return
	}

	offset := 0
	for {
		end := offset + snapshotChunkSize
		if end > len(data) {
			end = len(data)
		}
		done := end >= len(data)

		req := &pb.InstallSnapshotRequest{
			Term:              term,
			LeaderId:          n.cfg.ID,
			LastIncludedIndex: lastIncludedIndex,
			LastIncludedTerm:  lastIncludedTerm,
			Offset:            uint64(offset),
			Data:              data[offset:end],
			Done:              done,
		}
		resp, err := n.transport.InstallSnapshot(peerID, req)
		if err != nil {
			return // peer unreachable — retry on next heartbeat
		}

		n.mu.Lock()
		if resp.Term > n.currentTerm {
			n.becomeFollower(resp.Term)
			n.mu.Unlock()
			return
		}
		if n.role != RoleLeader || n.currentTerm != term {
			n.mu.Unlock()
			return // no longer leader for the term this transfer started in
		}
		if !resp.Success {
			n.mu.Unlock()
			return // rejected — retry from offset 0 on the next heartbeat
		}
		if done {
			if lastIncludedIndex > n.matchIndex[peerID] {
				n.matchIndex[peerID] = lastIncludedIndex
				n.nextIndex[peerID] = lastIncludedIndex + 1
			}
			n.mu.Unlock()
			return
		}
		n.mu.Unlock()

		offset = end
	}
}

// maybeAdvanceCommit checks whether a new log entry can be committed.
// An entry is committed once a majority of nodes have it in their log.
// MUST be called with n.mu held.
func (n *Node) maybeAdvanceCommit() {
	for idx := n.lastLogIndex(); idx > n.commitIndex; idx-- {
		// Only commit entries from the current term
		// (Raft's rule to prevent the "leader completeness" violation)
		pos, ok := n.logPos(idx)
		if !ok || n.log[pos].Term != n.currentTerm {
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
// MUST be called with n.mu held; it is kept held for the entire function,
// including while sending on ApplyCh (see note below).
func (n *Node) advanceCommitTo(newCommit uint64) {
	var toNotify []*LogEntry
	for i := n.commitIndex + 1; i <= newCommit; i++ {
		if pos, ok := n.logPos(i); ok {
			toNotify = append(toNotify, n.log[pos])
		}
	}
	n.commitIndex = newCommit

	// Send while still holding n.mu. This function can be called concurrently
	// from different goroutines (e.g. two peers' AppendEntries responses each
	// advancing commit via maybeAdvanceCommit). If the lock were released
	// before sending, two such calls could interleave their ApplyCh sends
	// out of index order; the consumer drops any entry whose index is <= its
	// own lastApplied, so a lower-index entry arriving after a higher-index
	// one would be silently lost, not just reordered — this previously caused
	// a reproducible hang in server tests. ApplyCh is generously buffered, so
	// holding the lock here only blocks other RPC handlers in the pathological
	// case where the application has fallen far behind. HandleInstallSnapshot
	// sends its SnapshotValid message the same way, under the same lock, so
	// all producers share one total order.
	for _, entry := range toNotify {
		select {
		case n.ApplyCh <- ApplyMsg{CommandValid: true, Entry: entry}:
		case <-n.stopCh:
			return
		}
	}
}

// ── Helpers ───────────────────────────────────────────────────────────────────

// logPos translates a Raft log index into a position in n.log, accounting
// for entries already compacted away into a snapshot. Returns ok=false if
// index is before lastIncludedIndex (compacted away, no longer in memory —
// see maybeSendSnapshot/HandleInstallSnapshot for how that case is handled)
// or beyond what we currently have. Every read of n.log by Raft index
// (never by raw slice position) MUST go through this.
func (n *Node) logPos(index uint64) (int, bool) {
	if index < n.lastIncludedIndex {
		return 0, false
	}
	pos := int(index - n.lastIncludedIndex)
	if pos >= len(n.log) {
		return 0, false
	}
	return pos, true
}

func (n *Node) lastLogIndex() uint64 {
	return n.lastIncludedIndex + uint64(len(n.log)-1)
}

func (n *Node) lastLogTerm() uint64 {
	if len(n.log) == 0 {
		return n.lastIncludedTerm
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
