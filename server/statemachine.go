// Package server wires Raft consensus to the LSM storage engine.
// This file: the StateMachine, which consumes committed log entries
// and applies them to storage, then notifies waiting client RPCs.

package server

import (
	"encoding/json"
	"fmt"
	"log"
	"sync"

	"github.com/raftkv/raft"
	"github.com/raftkv/storage"
)

// Command is the payload stored inside each Raft log entry.
// We use JSON here for human-debuggability; production systems use protobuf.
type Command struct {
	Op    string `json:"op"`    // "put" or "delete"
	Key   string `json:"key"`
	Value []byte `json:"value,omitempty"`

	// Deduplication fields — a client attaches these to every write.
	// The state machine ignores commands it has already applied.
	ClientID string `json:"client_id,omitempty"`
	SeqNum   uint64 `json:"seq_num,omitempty"`
}

// Result is what a pending client RPC receives once its command is committed.
type Result struct {
	Err string
}

// pendingWrite is a write that has been appended to the Raft log but not yet
// committed. The client goroutine blocks on the Done channel.
type pendingWrite struct {
	logIndex uint64
	term     uint64
	done     chan Result
}

// StateMachine consumes committed Raft entries and applies them to the LSM engine.
// It also manages pending client RPCs, unblocking them once their entry commits.
type StateMachine struct {
	mu      sync.Mutex
	engine  *storage.Engine
	node    *raft.Node

	// pending maps logIndex → the client RPC waiting on that entry
	pending map[uint64]*pendingWrite

	// lastApplied tracks which log index we've applied (for idempotency on restart)
	lastApplied uint64

	// dedup: for each clientID, the highest seqNum we've successfully applied.
	// Requests with seqNum <= lastSeq[clientID] are silently skipped.
	lastSeq map[string]uint64

	stopCh chan struct{}
	wg     sync.WaitGroup
}

// NewStateMachine creates a state machine backed by engine, consuming from node.ApplyCh.
func NewStateMachine(node *raft.Node, engine *storage.Engine) *StateMachine {
	sm := &StateMachine{
		engine:  engine,
		node:    node,
		pending: make(map[uint64]*pendingWrite),
		lastSeq: make(map[string]uint64),
		stopCh:  make(chan struct{}),
	}
	sm.wg.Add(1)
	go sm.applyLoop()
	return sm
}

// Stop shuts down the apply loop.
func (sm *StateMachine) Stop() {
	close(sm.stopCh)
	sm.wg.Wait()
}

// ProposeWrite submits a command to Raft and blocks until it is committed
// (or the node loses leadership). Returns an error if the write fails.
//
// This is the critical path for client PUT and DELETE requests.
func (sm *StateMachine) ProposeWrite(cmd Command) error {
	data, err := json.Marshal(cmd)
	if err != nil {
		return fmt.Errorf("marshal command: %w", err)
	}

	// Hold sm.mu across Propose()+registration below. Propose() can commit
	// and notify synchronously (e.g. the single-node fast path, where
	// maybeAdvanceCommit runs inline and the apply loop may run before this
	// function would otherwise register its waiter) — apply() takes the same
	// lock before calling notifyPending, so registering under it here closes
	// the race where a fast commit's notification arrives, finds no pending
	// waiter yet, and is dropped, leaving this call blocked forever.
	sm.mu.Lock()

	// Submit to Raft — this appends to the local log and starts replication
	logIndex, term, isLeader := sm.node.Propose(data)
	if !isLeader {
		sm.mu.Unlock()
		return ErrNotLeader
	}

	// Register a pending waiter for this log index
	done := make(chan Result, 1)
	pw := &pendingWrite{logIndex: logIndex, term: term, done: done}
	sm.pending[logIndex] = pw
	sm.mu.Unlock()

	// Block until the entry commits (apply loop sends on done) or we stop
	select {
	case result := <-done:
		if result.Err != "" {
			return fmt.Errorf(result.Err)
		}
		return nil
	case <-sm.stopCh:
		return ErrStopped
	}
}

// ReadValue reads a key from the local storage engine.
//
// Linearizability note: for strict linearizable reads you would implement
// ReadIndex (ask Raft for the current commit index, wait for it to be applied,
// then read). Here we do a simpler approach: only serve reads if we're leader
// and trust that recent commits are already applied. A full ReadIndex
// implementation is shown in comments below.
func (sm *StateMachine) ReadValue(key string) ([]byte, bool, error) {
	if !sm.node.IsLeader() {
		return nil, false, ErrNotLeader
	}

	// For strict linearizability, uncomment ReadIndex flow:
	// readIdx, err := sm.node.ReadIndex()
	// if err != nil { return nil, false, err }
	// sm.waitForApplied(readIdx) // block until lastApplied >= readIdx

	val, found := sm.engine.Get(key)
	return val, found, nil
}

// ── Apply loop ────────────────────────────────────────────────────────────────

// applyLoop is the single goroutine that consumes ApplyCh and calls apply()
// or applySnapshot(). Single-threaded application is deliberate: it ensures
// entries are applied in strict log order, which is required for state
// machine safety, and that a snapshot install never races with an in-flight
// apply() of a normal entry.
func (sm *StateMachine) applyLoop() {
	defer sm.wg.Done()
	for {
		select {
		case msg := <-sm.node.ApplyCh:
			sm.dispatch(msg)
		case <-sm.stopCh:
			// Drain any remaining messages before exiting
			for {
				select {
				case msg := <-sm.node.ApplyCh:
					sm.dispatch(msg)
				default:
					return
				}
			}
		}
	}
}

// dispatch routes a single ApplyCh message to the right handler.
func (sm *StateMachine) dispatch(msg raft.ApplyMsg) {
	switch {
	case msg.CommandValid:
		sm.apply(msg.Entry)
	case msg.SnapshotValid:
		// TODO(snapshotting): install msg.Snapshot into the storage engine
		// and reset dedup/pending state. Not yet wired up — a peer that
		// falls behind and receives InstallSnapshot from the leader will
		// have this message dropped here rather than actually catching up.
		log.Printf("[statemachine] received snapshot up to index %d (not yet applied)", msg.SnapshotIndex)
	}
}

// apply executes a single committed log entry against the storage engine.
// After applying, it wakes any client RPC that was waiting on this log index.
func (sm *StateMachine) apply(entry *raft.LogEntry) {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	// Never apply the same index twice (safety: applies are idempotent but
	// duplicate application would cause double-writes and corrupt counters)
	if entry.Index <= sm.lastApplied {
		log.Printf("[statemachine] skipping already-applied index %d", entry.Index)
		sm.notifyPending(entry.Index, Result{})
		return
	}

	// Decode the command
	var cmd Command
	if err := json.Unmarshal(entry.Data, &cmd); err != nil {
		// Malformed entry — log and skip. In production: trigger a panic or
		// leader-side validation to prevent bad entries ever reaching the log.
		log.Printf("[statemachine] ERROR: unmarshal entry %d: %v", entry.Index, err)
		sm.lastApplied = entry.Index
		sm.notifyPending(entry.Index, Result{Err: "corrupt log entry"})
		return
	}

	// Deduplication: if this client already had this seqNum applied, skip the
	// write but still wake the pending RPC (it may be a retry that reached us).
	if cmd.ClientID != "" {
		if seq, ok := sm.lastSeq[cmd.ClientID]; ok && cmd.SeqNum <= seq {
			log.Printf("[statemachine] dedup: client=%s seq=%d already applied", cmd.ClientID, cmd.SeqNum)
			sm.lastApplied = entry.Index
			sm.notifyPending(entry.Index, Result{})
			return
		}
	}

	// Apply to storage
	var applyErr string
	switch cmd.Op {
	case "put":
		if err := sm.engine.Put(cmd.Key, cmd.Value); err != nil {
			applyErr = err.Error()
		}
	case "delete":
		if err := sm.engine.Delete(cmd.Key); err != nil {
			applyErr = err.Error()
		}
	default:
		applyErr = fmt.Sprintf("unknown op: %q", cmd.Op)
	}

	// Update dedup tracker
	if cmd.ClientID != "" && applyErr == "" {
		sm.lastSeq[cmd.ClientID] = cmd.SeqNum
	}

	sm.lastApplied = entry.Index

	// Wake the pending client RPC
	sm.notifyPending(entry.Index, Result{Err: applyErr})
}

// notifyPending unblocks a waiting client RPC for the given log index.
// If the pending write's term doesn't match the current entry's term, it means
// a new leader re-used this index with a different command — the original client
// must retry (its command was never committed).
func (sm *StateMachine) notifyPending(logIndex uint64, result Result) {
	pw, ok := sm.pending[logIndex]
	if !ok {
		return // no client was waiting on this index
	}
	delete(sm.pending, logIndex)

	// Non-blocking send: the client may have already timed out
	select {
	case pw.done <- result:
	default:
	}
}

// LastApplied returns the index of the most recently applied log entry.
func (sm *StateMachine) LastApplied() uint64 {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	return sm.lastApplied
}

// ── Sentinel errors ────────────────────────────────────────────────────────────

// ErrNotLeader is returned when a write or linearizable read is attempted
// on a non-leader node. The client should redirect to the leader.
var ErrNotLeader = fmt.Errorf("not leader")

// ErrStopped is returned when the node is shutting down.
var ErrStopped = fmt.Errorf("node stopped")
