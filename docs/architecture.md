# Architecture

## Overview

raftkv is a distributed key-value store that guarantees linearizable reads and at-least-once-durable writes across a cluster of nodes. It is built from three independent layers, a consensus layer (Raft), a durability layer (WAL), and a storage layer (LSM-tree) — that are wired together through a thin application server.

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
│    · KVService   — client-facing reads and writes           │
│    · RaftService — peer-to-peer consensus RPCs              │
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
│  │                              │ CommitCh               │  │
│  └──────────────────────────────┼────────────────────────┘  │
│                                 ▼                           │
│  ┌───────────────────────────────────────────────────────┐  │
│  │  StateMachine  (server/statemachine.go)               │  │
│  │                                                       │  │
│  │  Single goroutine drains CommitCh.                    │  │
│  │  Deserialises Command, checks (ClientID, SeqNum),     │  │
│  │  calls engine.Put / engine.Delete,                    │  │
│  │  wakes the blocked client RPC goroutine.              │  │
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
    CommitNotify on CommitCh.

7.  StateMachine.applyLoop receives the CommitNotify:
      a. Deserialises the Command.
      b. Checks deduplication (ClientID + SeqNum).
      c. Calls engine.Put(key, value).
      d. Sends Result on the pending write's done channel.

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
      a. Opens WAL. Reads all segments, verifies CRC32 checksums,
         stops at first corrupted record (partial write at tail).
      b. Rebuilds the in-memory Raft log from WAL entries.
      c. Loads hard state (currentTerm, votedFor) from hardstate.json.
      d. Opens LSM engine. Loads all SSTable files from disk.
      e. Starts Raft node. Raft re-joins as a follower.

3.  The leader sends AppendEntries to catch the restarted node up.
    Any entries that were in the WAL but not in the LSM engine
    (written after the last SSTable flush) are re-applied.
```

This works because the WAL is written before the LSM engine is updated. If the node crashed mid-apply, the entry is in the WAL and will be re-applied. The apply is idempotent for PUT/DELETE.

---

## gRPC service separation

Two services are registered on the same gRPC server:

| Service | Callers | RPCs |
|---------|---------|------|
| `RaftService` | peer nodes | RequestVote, AppendEntries, InstallSnapshot |
| `KVService` | clients (CLI, apps) | Get, Put, Delete |

Keeping them on the same port simplifies deployment (one firewall rule, one port forward) while keeping the handler code separate. Consensus traffic is not affected by client load: if a client floods the server with Get requests, heartbeats still go through.

---

## Threading model

Each node runs:

| Goroutine | Count | Responsibility |
|-----------|-------|----------------|
| `node.run` | 1 | Election timer, heartbeat timer |
| `replicateToPeer` | N per replicate call | Send AppendEntries to one peer |
| `statemachine.applyLoop` | 1 | Drain CommitCh, apply entries to storage |
| `storage.flushWorker` | 1 | Flush immutable Memtable to SSTable |
| `storage.compactionWorker` | 1 | Merge SSTables in background |
| gRPC server goroutines | one per RPC | Handle incoming RPCs concurrently |

The Raft node itself is protected by a single mutex (`n.mu`). All state reads and writes go through that lock. RPC handlers acquire it, the timer goroutine acquires it, and `Propose` acquires it. `advanceCommitTo` temporarily releases the lock while blocking on the CommitCh send to avoid deadlocking the RPC handlers.

---

## Directory structure

```
raftkv/
├── proto/               gRPC message types and service definitions
│   ├── raftkv.proto
│   ├── raftkv.pb.go     generated — do not edit
│   ├── raftkv_grpc.pb.go generated — do not edit
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
├── server/              Application layer: gRPC server + state machine (Layers 5–6)
│   ├── kvserver.go
│   ├── statemachine.go
│   ├── walstate.go
│   ├── transport.go
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
