# Architecture

## Overview

raftkv is a distributed key-value store that guarantees linearizable reads and at-least-once-durable writes across a cluster of nodes. It is built from three independent layers, a consensus layer (Raft), a durability layer (WAL), and a storage layer (LSM-tree) - that are wired together through a thin application server.

Every node in the cluster is identical. Any node can receive client requests, but only the leader can commit writes. The leader is elected automatically and re-elected whenever the current leader fails.

---

## Layer map

```
┌─────────────────────────────────────────────────────────────┐
│  Client (raftkv-cli)                                        │
│  Issues Get / Put / Delete over gRPC.                       │
│  Follows LeaderHint on rejection; retries automatically.    │
└────────────────────────────┬────────────────────────────────┘
                             │ gRPC  KVService
                             ▼
┌─────────────────────────────────────────────────────────────┐
│  KVServer  (server/kvserver.go)                             │
│  Single process per node. Registers two gRPC services:      │
│    · KVService   - client-facing reads and writes           │
│    · RaftService - peer-to-peer consensus RPCs              │
│                                                             │
│  ┌───────────────────────────────────────────────────────┐  │
│  │  Raft Node  (raft/node.go)                            │  │
│  │                                                       │  │
│  │  Follower ──election timeout──► Candidate             │  │
│  │  Candidate ──majority vote───► Leader                 │  │
│  │  Candidate/Leader ──higher term──► Follower           │  │
│  │                                                       │  │
│  │  Leader:  AppendEntries to all peers                  │  │
│  │           Heartbeat every 50 ms                       │  │
│  │           Commit when majority ACKs                   │  │
│  │           Log compaction via InstallSnapshot          │  │
│  │                              │ ApplyCh                │  │
│  └──────────────────────────────┼────────────────────────┘  │
│                                 ▼                           │
│  ┌───────────────────────────────────────────────────────┐  │
│  │  StateMachine  (server/statemachine.go)               │  │
│  │                                                       │  │
│  │  Single goroutine drains ApplyCh.                     │  │
│  │  Deserialises Command, checks (ClientID, SeqNum),     │  │
│  │  calls engine.Put / engine.Delete,                    │  │
│  │  wakes the blocked client RPC goroutine,               │  │
│  │  snapshots the engine every N applied entries.        │  │
│  └───────────────────────────────────────────────────────┘  │
│                                 │                           │
│                                 ▼                           │
│  ┌───────────────────────────────────────────────────────┐  │
│  │  LSM Engine  (storage/)                               │  │
│  │                                                       │  │
│  │  Writes:  active Memtable → flush → SSTable           │  │
│  │  Reads:   Memtable → immutable Memtable → SSTables    │  │
│  │  Background: flush worker, compaction worker          │  │
│  └───────────────────────────────────────────────────────┘  │
│                                                             │
│  ┌───────────────────────────────────────────────────────┐  │
│  │  WAL  (wal/)                                          │  │
│  │                                                       │  │
│  │  Raft log entries are written here before being       │  │
│  │  applied to the state machine.  Segment files of      │  │
│  │  8 MB each.  CRC32 checksums detect partial writes.   │  │
│  └───────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────┘
         │  gRPC  RaftService
         ▼
   peer nodes  (same structure)
```

---

## Data flow for a write

```
1.  Client calls Put("foo", "bar") on any node.

2.  If the node is not the leader, it returns a LeaderHint address
    and the client retries on that address.

3.  Leader receives Put RPC.
    KVServer.Put → StateMachine.ProposeWrite → Raft.Propose

4.  Raft.Propose:
      a. Appends LogEntry{index, term, data} to the in-memory log.
      b. Persists the entry to the WAL (fsync).
      c. Launches replicateToPeer goroutines for every peer.
      d. If single-node cluster, commits immediately (quorum = 1).

5.  Each replicateToPeer goroutine sends AppendEntries RPC.
    The follower:
      a. Checks term and log consistency.
      b. Appends new entries to its own WAL.
      c. Returns success + matchIndex.

6.  When the leader sees matchIndex ≥ entry.Index on a majority of
    peers (including itself), it advances commitIndex and sends
    ApplyMsg{CommandValid: true} on ApplyCh.

7.  StateMachine.applyLoop receives the ApplyMsg:
      a. Deserialises the Command.
      b. Checks deduplication (ClientID + SeqNum).
      c. Calls engine.Put(key, value).
      d. Sends Result on the pending write's done channel.
      e. Publishes the change to any Watch subscribers.
      f. Every N applied entries, snapshots the engine and asks Raft
         to compact its log (WithSnapshotInterval).

8.  ProposeWrite unblocks. KVServer.Put returns PutResponse{Success: true}.

9.  Client prints "OK".
```

---

## Data flow for a read

```
1.  Client calls Get("foo") on any node.

2.  If the node is not the leader, it returns LeaderHint.

3.  Leader: StateMachine.ReadValue("foo")
      → engine.Get("foo")
      → checks active Memtable, immutable Memtable, SSTables newest-first
      → returns (value, found)

4.  KVServer.Get returns GetResponse{Value, Found}.
```

Reads are served directly from the LSM engine without going through the Raft log. This makes reads fast but means the leader may transiently serve stale data if it has just lost leadership without knowing it yet. Full linearizability would require a ReadIndex round-trip (commented in `statemachine.go`).

---

## Data flow for crash recovery

```
1.  Node crashes (power loss, process kill, OS panic).

2.  On restart, KVServer.NewKVServer:
      a. Opens LSM engine. Loads all SSTable files from disk.
      b. Opens WAL-backed persistent state.
      c. Starts Raft node, which:
         - Reads snapshot metadata (lastIncludedIndex, lastIncludedTerm),
           if a snapshot was ever taken. This seeds commitIndex so
           already-snapshotted history is not replayed.
         - Loads hard state (currentTerm, votedFor).
         - Rebuilds the in-memory Raft log from the entries still in
           the WAL (only the entries after the last snapshot, if any).
      d. Raft re-joins as a follower.

3.  The leader sends AppendEntries to catch the restarted node up on
    anything committed while it was down. If the node is far enough
    behind that the leader has already compacted past what it has,
    the leader sends a snapshot via InstallSnapshot instead.
```

This works because the WAL is written before the LSM engine is updated. If the node crashed mid-apply, the entry is in the WAL and will be re-applied. The apply is idempotent for PUT/DELETE. See [design-decisions.md](design-decisions.md) for how snapshotting bounds how much of the WAL ever needs replaying.

---

## gRPC service separation

Two services are registered on the same gRPC server:

| Service | Callers | RPCs |
|---------|---------|------|
| `RaftService` | peer nodes | RequestVote, AppendEntries, InstallSnapshot |
| `KVService` | clients (CLI, apps) | Get, Put, Delete, Watch |

Keeping them on the same port simplifies deployment (one firewall rule, one port forward) while keeping the handler code separate. Consensus traffic is not affected by client load: if a client floods the server with Get requests, heartbeats still go through.

---

## Threading model

Each node runs:

| Goroutine | Count | Responsibility |
|-----------|-------|----------------|
| `node.run` | 1 | Election timer, heartbeat timer |
| `replicateToPeer` | N per replicate call | Send AppendEntries to one peer |
| `statemachine.applyLoop` | 1 | Drain ApplyCh, apply entries to storage |
| `storage.flushWorker` | 1 | Flush immutable Memtable to SSTable |
| `storage.compactionWorker` | 1 | Merge SSTables in background |
| gRPC server goroutines | one per RPC | Handle incoming RPCs concurrently |
| metrics HTTP server | 1, optional | Serves `/metrics` when `--metrics-listen` is set |

The Raft node itself is protected by a single mutex (`n.mu`). All state reads and writes go through that lock. RPC handlers acquire it, the timer goroutine acquires it, and `Propose` acquires it. `advanceCommitTo` sends on ApplyCh while still holding the lock: an earlier version released it first, but that let two concurrent callers (for example two peers acknowledging around the same time) interleave their sends out of order, and the consumer silently drops any entry that arrives at or below its own last-applied index. Holding the lock through the send closes that gap; ApplyCh is buffered generously enough that this does not become a bottleneck.

---

## Directory structure

```
raftkv/
├── proto/               gRPC message types and service definitions
│   ├── raftkv.proto
│   ├── raftkv.pb.go     generated - do not edit
│   ├── raftkv_grpc.pb.go generated - do not edit
│   └── generate.go      go:generate directive for protoc
│
├── wal/                 Write-ahead log (Layer 2)
│   ├── wal.go
│   └── wal_test.go
│
├── storage/             LSM-tree storage engine (Layer 3)
│   ├── memtable.go
│   ├── sstable.go
│   ├── engine.go
│   └── engine_test.go
│
├── raft/                Raft consensus algorithm (Layer 4)
│   ├── node.go
│   ├── memstate.go      in-memory PersistentState for tests
│   ├── memtransport.go  in-memory Transport for tests
│   └── raft_test.go
│
├── server/              Application layer: gRPC server + state machine (Layers 5-6)
│   ├── kvserver.go
│   ├── statemachine.go
│   ├── walstate.go
│   ├── transport.go
│   ├── metrics.go       Prometheus counters/histograms, /metrics endpoint
│   ├── watch.go         change-data-capture fan-out for the Watch RPC
│   └── statemachine_test.go
│
├── client/              CLI binary (Layer 6)
│   └── main.go
│
├── chaos/               Chaos testing harness (Layer 7)
│   └── chaos.py
│
├── scripts/
│   ├── build.sh
│   └── run_cluster.sh
│
├── docs/                This documentation
│
├── main.go              Server binary entry point
├── go.mod
├── go.sum
├── Makefile
├── Dockerfile
└── docker-compose.yml
```
