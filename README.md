# Distributed Key-Value Store with RAFT Consensus

A distributed key-value store built from scratch in Go, implementing the same primitives that power **etcd**, **CockroachDB**, and **TiKV**: Raft consensus, a write-ahead log, and an LSM-tree storage engine.

Fault-tolerant across any minority of node failures. A 3-node cluster survives one dead node and continues accepting reads and writes without operator intervention.

---

## How a write works

```text
client PUT foo=bar
        │
        ▼
raftkv-cli ── gRPC──►  KVServer (leader)
                              │
                        1. Append to WAL    ← crash-safe before anything else
                              │
                        2. Propose to Raft  ← broadcast to followers
                              │
                        3. Majority ACK     ← quorum (2 of 3) confirms
                              │
                        4. Commit + apply   ← StateMachine writes to LSM
                              │
                        5. Return OK        ← client gets confirmation
```

If the leader crashes after step 3, a new leader is elected and the entry is still committed it was already on a majority. If it crashes before step 3, the entry is lost and the client retries.

---

## Architecture

```text
┌─────────────────────────────────────────────────┐
│  KVServer                                       │
│                                                 │
│  ┌────────────────────────────────────────────┐ │
│  │  Raft Node                                 │ │
│  │  · Leader election  (randomised timeouts)  │ │
│  │  · Log replication  (AppendEntries)        │ │
│  │  · Term-based commit safety                │ │
│  │  · Log compaction   (InstallSnapshot)      │ │
│  │                           │ ApplyCh        │ │
│  └───────────────────────────┼────────────────┘ │
│                              ▼                  │
│  ┌────────────────────────────────────────────┐ │
│  │  StateMachine                              │ │
│  │  · Single-goroutine apply loop             │ │
│  │  · Client dedup  (ClientID + SeqNum)       │ │
│  │  · Wakes blocked client RPCs on commit     │ │
│  │  · Periodic engine snapshot + compaction   │ │
│  └────────────────────────────────────────────┘ │
│                              │                  │
│                              ▼                  │
│  ┌────────────────────────────────────────────┐ │
│  │  LSM Storage Engine                        │ │
│  │  · Memtable  — sorted in-memory writes     │ │
│  │  · SSTables  — immutable on-disk files     │ │
│  │  · Bloom filters  — skip disk for misses   │ │
│  │  · Background compaction + tombstone GC    │ │
│  └────────────────────────────────────────────┘ │
│                                                 │
│  ┌────────────────────────────────────────────┐ │
│  │  WAL                                       │ │
│  │  · [len][crc32][data] record format        │ │
│  │  · 8MB segment files, rolls automatically  │ │
│  │  · Single fsync per batch                  │ │
│  └────────────────────────────────────────────┘ │
└─────────────────────────────────────────────────┘
         │  RaftService gRPC
         ▼
   peer nodes (same structure)
```

Two separate gRPC services on the same port: `RaftService` (peer-to-peer consensus traffic) and `KVService` (client-facing reads and writes). Keeping them separate means a client flood can't starve heartbeats.

---

## Components

| Package | File | What it does |
| ------- | ---- | ------------ |
| `proto/` | `raftkv.proto` | All gRPC message types: Raft RPCs, KV ops, leader hints |
| `wal/` | `wal.go` | Write-ahead log: CRC32 records, segment rollover, batch fsync |
| `storage/` | `memtable.go` | Sorted in-memory buffer; O(log n) insert via binary search |
| `storage/` | `sstable.go` | Immutable on-disk sorted file with bloom filter |
| `storage/` | `engine.go` | LSM coordinator: flush, compaction, crash recovery |
| `raft/` | `node.go` | Raft state machine: elections, replication, commit |
| `raft/` | `memstate.go` | In-memory `PersistentState` for unit tests |
| `raft/` | `memtransport.go` | In-memory `Transport` with controllable partitions for tests |
| `server/` | `kvserver.go` | gRPC server wiring Raft + storage together |
| `server/` | `statemachine.go` | Apply loop, deduplication, pending write tracking |
| `server/` | `walstate.go` | Bridges `raft.PersistentState` → WAL |
| `server/` | `transport.go` | gRPC implementation of `raft.Transport` |
| `client/` | `main.go` | CLI: get/put/delete with transparent leader redirection |
| `chaos/` | `chaos.py` | Concurrent writes + random kills + consistency verification |

---

## Documentation

Detailed write-ups for every layer are in [`docs/`](docs/):

| File | Covers |
| ---- | ------ |
| [architecture.md](docs/architecture.md) | System overview, write/read/recovery data flows, threading model |
| [raft.md](docs/raft.md) | Elections, log replication, fast rollback, Figure 8 current-term rule |
| [wal.md](docs/wal.md) | Record format, segment files, fsync strategy, crash recovery |
| [storage.md](docs/storage.md) | Memtable, SSTable layout, bloom filter, compaction algorithm |
| [server.md](docs/server.md) | KVServer wiring, StateMachine apply loop, deduplication |
| [client.md](docs/client.md) | CLI usage, leader redirection, retry logic |
| [chaos-testing.md](docs/chaos-testing.md) | What the harness proves, failure scenarios, interpreting violations |
| [design-decisions.md](docs/design-decisions.md) | 13 annotated decisions with alternatives and trade-offs |

---

## Quick start

### Prerequisites

- Go 1.22+
- `protoc` + plugins (only needed if you modify the `.proto` file)

```bash
# Install protoc plugins (one-time)
go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest

# Regenerate proto stubs — only needed after editing raftkv.proto
make proto
```

### Build

```bash
make build        # runs go mod tidy, then compiles both binaries
make test         # runs all 34 tests across 4 packages
make              # proto + tidy + test + build in one shot
```

Produces `./raftkv-server` and `./raftkv-cli`.

### Run a local 3-node cluster

```bash
./scripts/run_cluster.sh

# In another terminal:
PEERS="node1=localhost:7001,node2=localhost:7002,node3=localhost:7003"

./raftkv-cli --peers $PEERS put greeting "hello from raft"
./raftkv-cli --peers $PEERS get greeting
# → hello from raft

# Simulate a node failure (cluster stays up — quorum is 2/3)
kill $(cat /tmp/raftkv/node1.pid)
./raftkv-cli --peers $PEERS get greeting        # still works
./raftkv-cli --peers $PEERS put resilient "yes" # still commits

# Restart the dead node — it catches up via log replication
./raftkv-server \
  --id=node1 \
  --listen=localhost:7001 \
  --peers=node2=localhost:7002,node3=localhost:7003 \
  --data-dir=/tmp/raftkv/node1 &

./scripts/run_cluster.sh stop
```

### Run with Docker Compose

```bash
docker compose up -d

# Run CLI inside the node1 container (uses Docker internal hostnames)
docker compose exec node1 raftkv-cli \
  --peers node1=node1:7001,node2=node2:7001,node3=node3:7001 \
  put hello world

docker compose exec node1 raftkv-cli \
  --peers node1=node1:7001,node2=node2:7001,node3=node3:7001 \
  get hello
# → world

docker compose down -v
```

### Run chaos tests

```bash
# Build binaries first, then:
python3 chaos/chaos.py \
  --binary ./raftkv-server \
  --cli    ./raftkv-cli \
  --rounds 3 \
  --workers 4 \
  --writes 50
```

The chaos harness starts a 3-node cluster, hammers it with concurrent writes across 4 goroutines, and randomly kills and restarts nodes throughout. After all writes complete, it reads back every confirmed key and checks for missing or corrupted values. The test passes only if every write that returned `OK` is still readable with the correct value.

---

## Test coverage

```text
wal/         (4 tests)
  TestWALWriteAndRead              — 100 entries survive close and reopen
  TestWALTruncate                  — suffix removal leaves correct entries
  TestWALTruncatePrefix            — prefix removal after compaction; lastIndex stays correct
  TestWALBatchAppend               — 200-entry batch, single fsync
  BenchmarkAppend                  — single-entry throughput (~50k–100k ops/s on SSD)

storage/     (9 tests)
  TestMemtableBasic                — put, get, delete, overwrite
  TestMemtableSortOrder            — snapshot is always sorted
  TestSSTableWriteRead             — write entries, read back including tombstones
  TestBloomFilter                  — no false negatives, ~1% false positive rate
  TestEngineBasic                  — end-to-end read and write
  TestEngineRestart                — data survives engine close and reopen
  TestEngineCompaction             — 50k entries, compaction runs, spot checks pass
  TestEngineSnapshotRoundTrip      — snapshot across memtable+SSTables, load into a fresh engine
  BenchmarkEnginePut/Get           — throughput under realistic load

raft/        (12 tests)
  TestNodeSeedsFromSnapshotOnStart — Start() seeds commitIndex/log from a saved snapshot
  TestElectionBasic                — exactly one leader elected in a 3-node cluster
  TestElectionFiveNodes            — one leader in a 5-node cluster
  TestReelectionAfterLeaderFailure — new leader's term is strictly higher
  TestLogReplication               — all nodes converge to same log and commitIndex
  TestReplicationWithFollowerFailure — cluster commits with 2 of 3 nodes alive
  TestNoCommitWithMinorityPartition  — isolated leader never advances commitIndex
  TestCommitNotification           — ApplyCh delivers every entry exactly once, in order
  TestCompactLogTruncatesPrefix    — CompactLog persists a snapshot and discards its prefix
  TestInstallSnapshotRejectsStale  — a snapshot no newer than what's held is a no-op
  TestInstallSnapshotChunkedTransferApplies — multi-chunk reassembly + correct ApplyMsg delivery
  TestInstallSnapshotRejectsOffsetMismatch  — a chunk that doesn't pick up where the last left off is rejected

server/      (9 tests)
  TestPutAndGet                    — full round-trip: propose → commit → apply → read
  TestDeduplication                — retry with same SeqNum does not re-apply
  TestNonLeaderRejectsWrites       — isolated node returns ErrNotLeader immediately
  TestMultipleWritesOrdered        — 20 writes apply in order; lastApplied == 20
  TestDeleteRemovesKey             — DELETE makes key unreadable via tombstone
  TestCommitNotificationUnblocksPropose — ProposeWrite returns only after commit
  TestSnapshotTriggersAtInterval   — WithSnapshotInterval compacts the log at the right boundary
  TestSnapshotSurvivesRestart      — snapshot + restart seeds the fast-path, data intact
  TestWALStateSnapshotRoundTrip    — walState.SaveSnapshot/LoadSnapshot survive a reopen
```

---

## Consistency guarantees

**Writes** — `Put` returns `OK` only after the entry is committed on a majority of nodes. An `OK` response is a durability guarantee: the value survives any single node failure, including the leader crashing immediately after responding.

**Reads** — `Get` is served only by the current leader. Reads reflect all writes that committed before them. (Full linearizability requires a ReadIndex round-trip to confirm leadership; the code has this path stubbed and commented in `statemachine.go`.)

**Deduplication** — Every write carries a `(ClientID, SeqNum)`. The state machine skips commands it has already applied, so a client can safely retry a timed-out write without double-applying it.

**Leader redirection** — Non-leader nodes return a `LeaderHint` address on every rejected request. The CLI follows this hint automatically, so callers never need to know which node is currently the leader.

---

## Key design decisions

### LSM tree over B-tree

All writes land in an in-memory memtable first — always a sequential append. B-trees require random I/O for in-place page updates. For a write-heavy distributed store, LSM wins on throughput at the cost of read amplification (multiple SSTables to scan) and write amplification (compaction rewrites data several times). RocksDB, Cassandra, and LevelDB all make the same trade-off.

### WAL before memtable

The memtable is volatile. Crash before flushing to SSTable and those writes are gone. The WAL is written first — every write hits disk before the client gets `OK`. On restart, replay the WAL to reconstruct the memtable exactly. Crash recovery is just replay.

The on-disk record format is `[4-byte length][4-byte CRC32][payload]`. The checksum catches partial writes (e.g., power loss after 3 of 15 bytes). On detecting a corrupted tail record during replay, we stop — everything before it is clean.

### Batch fsync

`AppendBatch` writes all entries then calls `Sync()` once. If Raft is replicating 500 entries/second and you fsync each one separately, that's 500 disk flushes/second. A batch of 50 entries with one fsync is 10× cheaper. This is the same technique Kafka uses to achieve high write throughput with durability.

### Randomised election timeouts

Every follower picks a random election timeout between 150ms and 300ms. When the leader goes silent, the first follower to time out starts an election and usually wins before others even wake up. Without randomisation, all followers time out simultaneously, all vote for themselves, nobody reaches majority, and the cluster loops forever on split votes.

### Only commit current-term entries

A new leader may have entries from old terms in its log that were replicated to a majority but never committed. Committing them by replication count alone is unsafe: a subsequent leader elected from a node that missed those entries could overwrite them, and two nodes would apply different commands at the same index. The fix is one line in `maybeAdvanceCommit`: skip any entry whose term is not the current leader's term. Old entries get committed implicitly as a side effect when the new leader appends and commits its first entry.

This is Raft's leader completeness rule (Section 5.4 of the paper) and the most commonly misimplemented detail.

---

## What's not implemented

- **Membership changes** — The cluster topology is fixed at startup. Dynamic add/remove requires a two-phase joint consensus protocol (Raft Section 6).
- **TLS** — Peer connections use plaintext gRPC. Production deployments should use mutual TLS between nodes.
- **Leveled compaction** — The storage engine uses a simple "merge all SSTables" strategy. RocksDB-style leveled compaction would reduce write amplification significantly for large datasets.
- **PreVote** — A partitioned node's election timer keeps firing with nobody to reset it, so its term climbs unboundedly for as long as it's disconnected. On reconnection that inflated term forces the legitimate leader through a series of disruptive step-downs before terms converge, even though the rejoining node's stale log means it can never actually win. Not a safety issue — just a liveness cost on reconnection. See [design-decisions.md](docs/design-decisions.md#no-prevote).
