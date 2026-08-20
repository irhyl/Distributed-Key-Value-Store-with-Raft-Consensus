# Server, State Machine, and Transport

## Overview

The `server/` package is the glue layer. It takes three independent components - the Raft node, the WAL, and the LSM engine - and wires them into a running gRPC server that accepts client requests and participates in consensus.

```
server/
├── kvserver.go      Top-level server: wires everything, exposes gRPC handlers
├── statemachine.go  Apply loop: turns committed Raft entries into storage writes
├── walstate.go      Bridges raft.PersistentState → our WAL implementation
├── transport.go     Bridges raft.Transport → gRPC connections to peer nodes
├── metrics.go       Prometheus counters/histograms, /metrics endpoint
├── watch.go         change-data-capture fan-out for the Watch RPC
├── statemachine_test.go
└── walstate_test.go
```

---

## KVServer

[server/kvserver.go](../server/kvserver.go)

`KVServer` owns all components for one node in the cluster:

```go
type KVServer struct {
    cfg       Config         // NodeID, ListenAddr, Peers, DataDir, MetricsAddr
    node      *raft.Node     // consensus
    engine    *storage.Engine // durability
    sm        *StateMachine  // applies committed entries
    grpcSrv   *grpc.Server   // network
    transport *GRPCTransport // peer connections
    walState  *walState      // kept around so metrics.go can attach WAL hooks
    stopCh    chan struct{}  // signals background goroutines (e.g. the
                              // replication-lag metrics sampler) to stop
}
```

### Wiring order in NewKVServer

The components must be created in dependency order:

```
1. storage.Open(dataDir/storage)       → engine
2. newWALState(dataDir/wal)            → walState (PersistentState impl)
3. NewGRPCTransport(cfg.Peers)         → transport (Transport impl)
4. raft.NewNode(cfg, transport, walState) → node
5. NewStateMachine(node, engine)       → sm
6. grpc.NewServer()                    → grpcSrv
7. register RaftService + KVService on grpcSrv
8. wireMetrics()                       → attach OnBecomeLeader/OnFsync/OnCompaction hooks
```

`Start()` additionally begins serving `/metrics` (if `MetricsAddr` is set) and starts the replication-lag sampler.

### gRPC handlers

**RPC handlers for peers (RaftService)**

These are thin pass-throughs to the Raft node:

```go
func (s *KVServer) RequestVote(_ context.Context, req) → s.node.HandleRequestVote(req)
func (s *KVServer) AppendEntries(_ context.Context, req) → s.node.HandleAppendEntries(req)
func (s *KVServer) InstallSnapshot(_ context.Context, req) → s.node.HandleInstallSnapshot(req)
```

**RPC handlers for clients (KVService)**

Get, Put, and Delete all follow the same pattern:
1. Delegate to the StateMachine
2. If `ErrNotLeader`, return `LeaderHint` (the dial address of the current leader)
3. If success, return the result

`Watch` is different: it works on any node (not leader-only) and streams `ChangeEvent`s from `StateMachine.Subscribe` until the client disconnects or falls too far behind. See the "Change data capture" section in the main README.

### Leader hint

```go
func (s *KVServer) leaderHintAddr() string {
    id := s.node.LeaderID()   // returns node ID like "node1"
    if id == s.cfg.NodeID {
        return s.cfg.ListenAddr  // we are the leader
    }
    return s.cfg.Peers[id]       // look up the address for that peer ID
}
```

`LeaderID()` returns a node ID, not an address. The `leaderHintAddr` method translates it to a dialable gRPC address so the client can redirect.

---

## StateMachine

[server/statemachine.go](../server/statemachine.go)

The StateMachine is the bridge between Raft's `ApplyCh` and the LSM engine. It also tracks in-flight client writes and unblocks them when their entries commit, fans out applied writes to `Watch` subscribers, and (if `WithSnapshotInterval` is set) periodically snapshots the engine and compacts the Raft log.

### Command format

```go
type Command struct {
    Op       string `json:"op"`       // "put" or "delete"
    Key      string `json:"key"`
    Value    []byte `json:"value,omitempty"`
    ClientID string `json:"client_id,omitempty"`
    SeqNum   uint64 `json:"seq_num,omitempty"`
}
```

Commands are serialised as JSON before being passed to `Raft.Propose`. JSON is used here for human-debuggability; a production system would use protobuf for performance.

### ProposeWrite: blocking write

```go
func (sm *StateMachine) ProposeWrite(cmd Command) error {
    data, _ := json.Marshal(cmd)

    // sm.mu is held across Propose() and registering the pending waiter,
    // not just around the map write. See below for why.
    sm.mu.Lock()
    logIndex, term, isLeader := sm.node.Propose(data)
    if !isLeader {
        sm.mu.Unlock()
        return ErrNotLeader
    }
    done := make(chan Result, 1)
    sm.pending[logIndex] = &pendingWrite{logIndex, term, done}
    sm.mu.Unlock()

    // Block until commit or shutdown
    select {
    case result := <-done:
        return result.Err
    case <-sm.stopCh:
        return ErrStopped
    }
}
```

The client goroutine blocks on `done` until the applyLoop processes the entry and sends a result. This gives natural backpressure: if Raft is slow, the client waits rather than queuing unboundedly.

**Why `sm.mu` spans both calls:** on a single-node cluster, `Propose` can commit and notify synchronously, in the same call, before this function gets back around to registering `done` in `sm.pending`. If that race wins, the notification finds nothing to deliver to and is dropped, and this call hangs forever waiting for a result that already happened. `apply()` also takes `sm.mu` before calling `notifyPending`, so holding it here across both steps means the apply loop can't get ahead of registration.

### applyLoop: single-goroutine apply

```go
func (sm *StateMachine) applyLoop() {
    for {
        select {
        case msg := <-sm.node.ApplyCh:
            sm.dispatch(msg)
        case <-sm.stopCh:
            // drain remaining
            return
        }
    }
}

func (sm *StateMachine) dispatch(msg raft.ApplyMsg) {
    switch {
    case msg.CommandValid:
        sm.apply(msg.Entry)
        if sm.snapshotInterval > 0 && msg.Entry.Index%sm.snapshotInterval == 0 {
            sm.maybeSnapshot(msg.Entry.Index, msg.Entry.Term)
        }
    case msg.SnapshotValid:
        sm.applySnapshot(msg)
    }
}
```

The apply loop is deliberately single-threaded. A single goroutine consuming `ApplyCh` guarantees that entries are applied in strict log order, and that a snapshot install (`applySnapshot`) never runs concurrently with a normal `apply()`. If parallel application were used (for throughput), per-key ordering would need to be tracked explicitly - significantly more complex.

This is the same design as etcd. The snapshot trigger runs after `apply()` returns, not inside it, so it doesn't hold `sm.mu` during the engine snapshot I/O - but it's still on this same goroutine, so it can't overlap with applying the next entry.

### apply: deduplication and storage write

```go
func (sm *StateMachine) apply(entry *raft.LogEntry) {
    // 1. Skip if already applied (idempotency on restart)
    if entry.Index <= sm.lastApplied { ... }

    // 2. Unmarshal Command
    var cmd Command
    json.Unmarshal(entry.Data, &cmd)

    // 3. Deduplication: skip if (ClientID, SeqNum) already applied
    if cmd.ClientID != "" {
        if seq, ok := sm.lastSeq[cmd.ClientID]; ok && cmd.SeqNum <= seq {
            sm.notifyPending(entry.Index, Result{})
            return
        }
    }

    // 4. Apply to storage engine
    switch cmd.Op {
    case "put":    sm.engine.Put(cmd.Key, cmd.Value)
    case "delete": sm.engine.Delete(cmd.Key)
    }

    // 5. Publish to Watch subscribers (only on a real, successful apply -
    //    not for the skipped-duplicate path above)
    sm.broadcaster.publish(&pb.ChangeEvent{...})

    // 6. Update dedup tracker, but only after a successful apply
    if cmd.ClientID != "" {
        sm.lastSeq[cmd.ClientID] = cmd.SeqNum
    }

    // 7. Advance lastApplied
    sm.lastApplied = entry.Index

    // 8. Wake any client waiting on this log index
    sm.notifyPending(entry.Index, Result{})
}
```

### Deduplication: at-most-once semantics

The problem: a client sends `PUT foo=bar`, the leader commits it and responds, but the network drops the response. The client retries. Without deduplication, `foo` is written twice - harmless for a simple PUT, but catastrophic for a balance decrement.

The solution: each client attaches a monotonically increasing `SeqNum` to every write. The state machine tracks the highest applied `SeqNum` per `ClientID`. If `SeqNum <= lastSeq[ClientID]`, the write is a retry and is silently skipped. The client still gets an OK response (the entry committed; it's just not re-applied).

This gives **at-most-once semantics**: a command is applied at most once per (ClientID, SeqNum) pair, even if the client retries indefinitely.

### notifyPending: unblocking the RPC goroutine

```go
func (sm *StateMachine) notifyPending(logIndex uint64, result Result) {
    pw, ok := sm.pending[logIndex]
    if !ok {
        return // no client was waiting (e.g., replayed on restart)
    }
    delete(sm.pending, logIndex)
    select {
    case pw.done <- result:
    default: // client timed out and stopped listening; discard
    }
}
```

The send is non-blocking: if the client goroutine already gave up (timed out), the result is dropped. The state was still applied - the client just doesn't know yet. On retry, the deduplication logic will recognise the SeqNum and return OK without re-applying.

---

## WALState

[server/walstate.go](../server/walstate.go)

`walState` implements the `raft.PersistentState` interface using our WAL and a JSON metadata file.

```go
type PersistentState interface {
    SaveHardState(term uint64, votedFor string) error
    LoadHardState() (term uint64, votedFor string, err error)
    AppendEntries(entries []*LogEntry) error
    LoadEntries() ([]*LogEntry, error)
    TruncateSuffix(keepIndex uint64) error
    SaveSnapshot(data []byte, lastIncludedIndex, lastIncludedTerm uint64) error
    LoadSnapshot() (data []byte, lastIncludedIndex, lastIncludedTerm uint64, err error)
    LoadSnapshotMeta() (lastIncludedIndex, lastIncludedTerm uint64, err error)
    TruncatePrefix(discardIndex uint64) error
}
```

| Method | Implementation |
|--------|----------------|
| `SaveHardState` | Write JSON to temp file, atomically rename to `hardstate.json` |
| `LoadHardState` | Read and parse `hardstate.json`; return zeros if not found |
| `AppendEntries` | Delegate to `wal.AppendBatch` |
| `LoadEntries` | Delegate to `wal.ReadAll` |
| `TruncateSuffix` | Delegate to `wal.TruncateSuffix` |
| `SaveSnapshot` | Pack (index, term, data) into one buffer, temp file + atomic rename to `snapshot.bin` |
| `LoadSnapshot` | Read the full `snapshot.bin`, split header from data |
| `LoadSnapshotMeta` | Read only the 16-byte header, not the data - used on every `Start()` so restart cost doesn't scale with snapshot size |
| `TruncatePrefix` | Delegate to `wal.TruncatePrefix` |

---

## GRPCTransport

[server/transport.go](../server/transport.go)

`GRPCTransport` implements `raft.Transport` using real gRPC connections to peer nodes.

```go
type Transport interface {
    RequestVote(peerID string, req *pb.RequestVoteRequest) (*pb.RequestVoteResponse, error)
    AppendEntries(peerID string, req *pb.AppendEntriesRequest) (*pb.AppendEntriesResponse, error)
    InstallSnapshot(peerID string, req *pb.InstallSnapshotRequest) (*pb.InstallSnapshotResponse, error)
}
```

Connections are cached in a map: first call to a peer creates the connection; subsequent calls reuse it.

```go
func (t *GRPCTransport) getClient(peerID string) (pb.RaftServiceClient, error) {
    if client, ok := t.clients[peerID]; ok {
        return client, nil  // cached
    }
    addr := t.peers[peerID]
    conn, _ := grpc.DialContext(ctx, addr,
        grpc.WithTransportCredentials(insecure.NewCredentials()),
        grpc.WithBlock(),
    )
    t.clients[peerID] = pb.NewRaftServiceClient(conn)
    return t.clients[peerID], nil
}
```

Each RPC has a 100ms timeout (`rpcTimeout`). This is deliberately short: if a peer is unreachable, the caller should return quickly and let Raft handle the failure (back off, retry on next heartbeat). Blocking for 30 seconds would delay leader detection.

### Testing with MemTransport

In unit tests, `raft.Transport` is implemented by `raft.memTransport` ([raft/memtransport.go](../raft/memtransport.go)), which makes direct function calls to the target node's handler methods rather than going over the network. This makes tests fast, deterministic, and controllable:

```go
net := raft.NewNetwork()
net.Disconnect("node2")  // simulate node2 crash
net.Reconnect("node2")   // bring it back
```

---

## Test coverage

12 tests in [server/statemachine_test.go](../server/statemachine_test.go) plus 1 in [server/walstate_test.go](../server/walstate_test.go):

| Test | What it proves |
|------|----------------|
| `TestPutAndGet` | Full round-trip: propose → Raft commit → apply → LSM → readable |
| `TestDeduplication` | Retry with same SeqNum does not re-apply; first value survives |
| `TestNonLeaderRejectsWrites` | Isolated node returns `ErrNotLeader` without blocking |
| `TestMultipleWritesOrdered` | 20 writes apply in order; `lastApplied == 20` |
| `TestDeleteRemovesKey` | DELETE makes key unreadable even though tombstone is in storage |
| `TestCommitNotificationUnblocksPropose` | ProposeWrite returns only after commit, not before |
| `TestSnapshotTriggersAtInterval` | `WithSnapshotInterval` compacts the log at the right boundary |
| `TestSnapshotSurvivesRestart` | Snapshot + restart seeds the fast-path, data intact |
| `TestWALStateSnapshotRoundTrip` | `walState.SaveSnapshot`/`LoadSnapshot` survive a reopen |
| `TestWatchStreamsAppliedWrites` | Watch subscriber sees every write, in commit order |
| `TestWatchFiltersByKeyPrefix` | `key_prefix` excludes non-matching keys |
| `TestWatchDropsSlowSubscriber` | A subscriber that never drains is disconnected, not buffered forever |
