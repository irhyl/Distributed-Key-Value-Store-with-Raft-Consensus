# Design Decisions

Every non-trivial implementation choice involves a trade-off. This document explains the key decisions made in raftkv, what alternatives were considered, and why the chosen approach was preferred.

---

## LSM tree over B-tree for storage

**Decision:** Use a Log-Structured Merge tree (memtable + SSTables + compaction) instead of a B-tree.

**Trade-off:**

| | LSM tree | B-tree |
|--|---------|--------|
| Write pattern | Sequential (always appends) | Random (in-place page updates) |
| Write throughput | High — 50k–500k ops/sec on SSD | Moderate — bounded by random I/O |
| Read latency | Higher — must check multiple levels | Lower — single tree traversal |
| Space amplification | Higher — duplicates exist until compaction | Lower — data exists once |
| Write amplification | Higher — data rewritten during compaction | Lower — written once |
| Implementation complexity | Higher | Lower (for a simple implementation) |

**Why LSM?**

This is a write-heavy system. Every Raft commit results in a write, and a distributed KV store's primary bottleneck is write throughput. LSM trees are designed to maximise sequential write throughput by never doing random I/O on the write path.

B-trees require reading a page, modifying it in place, and writing it back. For a page that isn't cached, that's two random I/Os per write. On a spinning disk this is devastating (100–200 IOPS). On an SSD it's acceptable but still the bottleneck.

LSM trees convert every write to a sequential append — memtable insert followed eventually by a sequential SSTable write. The compaction I/O is background work that can be rate-limited to avoid impacting foreground reads.

RocksDB (Meta), Cassandra (Apache), LevelDB (Google), InfluxDB all made this same choice.

---

## WAL before state machine application

**Decision:** Write to the WAL before applying any entry to the LSM engine. The WAL is the source of truth on restart; the LSM engine is rebuilt from it.

**Why not write to both simultaneously?**

Two-phase write (WAL + storage engine atomically) is the right mental model, but "atomically" is impossible across two separate files without a higher-level coordinator. The WAL serves as that coordinator.

The invariant is: **the WAL is always ahead of or equal to the state machine**. On restart:
- Entries in the WAL but not yet applied: re-apply them (idempotent for PUT/DELETE)
- Entries in neither: never committed; the client retried or got an error

If the order were reversed (apply to storage first, then WAL), a crash between the two would leave the storage engine ahead of the WAL. On restart, we'd have no record of what was applied and would lose those writes from the Raft perspective.

---

## CRC32 checksums in WAL records

**Decision:** Every WAL record includes a CRC32 checksum over the payload.

**Alternative:** Trust the filesystem.

Filesystems and storage hardware can corrupt data in ways that don't surface as read errors. Bit rot, bad sectors that get remapped silently, controller bugs — all can produce data that looks valid to the OS but isn't. The CRC32 catches these.

More practically, the checksum solves the partial-write problem cleanly. If the machine loses power after writing 3 of 15 bytes of a record, the header might indicate a 15-byte payload but the CRC will fail (or the read will return an unexpected EOF). Either way, the reader stops and treats the prior records as authoritative.

CRC32 is deliberately chosen over SHA-256 or similar because:
- **Speed**: CRC32 is ~10 GB/s on modern CPUs with hardware acceleration; SHA-256 is ~500 MB/s
- **Purpose**: we need corruption detection, not tamper detection; CRC32 is sufficient
- **Size**: 4 bytes per record versus 32 for SHA-256

---

## Single-fsync batch writes

**Decision:** `AppendBatch` writes all entries to the OS buffer and calls `Sync()` once at the end, rather than after each entry.

**The numbers:**

A typical NVMe SSD can sustain:
- Sequential write throughput: 3–5 GB/s
- fsync (flush write cache): 50,000–200,000 ops/sec

If Raft is replicating 10,000 entries/sec and you fsync each one, you use your entire fsync budget on a mid-range drive. With batch fsync, 10,000 entries/sec becomes 1 fsync per batch — a fraction of the budget.

**Safety:**

Batching fsyncs does not sacrifice durability. The guarantee changes from "each entry is durable before the next is accepted" to "all entries in the batch are durable before any are acknowledged." Since the leader doesn't commit until a majority acknowledges, and followers don't acknowledge until after their own batch fsync, the committed entries are always durable on a majority.

This is the same technique used by Apache Kafka (where it's the primary throughput lever) and PostgreSQL (group commit).

---

## Segment files instead of a single WAL file

**Decision:** Roll over to a new segment file every 8 MB instead of appending to one monolithic file forever.

**Alternatives considered:**
1. Single growing file
2. Fixed-size segments (chosen)
3. Time-based segments (e.g., new file every hour)

**Why not a single file?**

Log compaction (once snapshots are implemented) requires deleting old WAL entries. Deleting from the middle of a file means rewriting everything after the deletion point. For a 10 GB WAL, that's 10 GB of I/O to delete the first 1 GB. Segment files make deletion O(1): delete the segment file and it's gone.

**Why fixed-size over time-based?**

Fixed-size gives predictable worst-case recovery time. A 8 MB segment takes roughly the same time to replay regardless of when it was written. Time-based segments can grow arbitrarily large under high write load.

The 8 MB threshold is small by production standards (etcd uses 64 MB) but it means the test suite exercises segment rollover without needing millions of test entries.

---

## Randomised election timeouts

**Decision:** Each node independently picks a random election timeout in `[150ms, 300ms]`.

**Why random?**

Consider a deterministic timeout of 200ms. All three nodes start simultaneously. At t=200ms, all three transition to Candidate simultaneously. Each votes for itself. None gets a majority. All reset their timers. At t=400ms, it happens again. This loop continues indefinitely — the cluster never elects a leader.

Randomisation breaks the symmetry. With high probability, one node's timer fires before the others. It starts an election, sends RequestVote to the other two, and collects their votes before their timers fire. Election completes in one round.

**Choosing the range:**

- Lower bound must be >> round-trip time to avoid a candidate timing out before its own votes come back
- Upper bound must be << the application's tolerance for downtime
- The ratio upper/lower determines split-vote probability — wider range = lower probability

150–300ms is the same range used by the original Raft paper. At typical LAN latencies (< 5ms), one election round takes ~10ms, well within the 150ms lower bound.

---

## Dummy log entry at index 0

**Decision:** Pre-populate the log with a sentinel entry `{Index: 0, Term: 0}` at position 0.

**Why?**

The Raft paper uses 1-based log indexing. The `prevLogIndex` in `AppendEntries` is "the index of the entry immediately before the new ones." For the very first entry (index 1), `prevLogIndex = 0`. Without a sentinel, accessing `log[0]` to check `prevLogTerm` would require a special case.

With the sentinel, `log[0]` always exists and has term 0. The consistency check `log[prevLogIndex].Term == prevLogTerm` works uniformly for all entries including the first.

This is a common implementation pattern in Raft implementations (TiKV, etcd both use it).

---

## matchIndex counts peers only, not self

**Decision:** `matchIndex` tracks replication progress on peers only. The leader counts itself as `+1` in the quorum check directly.

**The bug this avoids:**

An earlier version initialised `matchIndex[self.ID] = 0` and updated it alongside peers. `maybeAdvanceCommit` then counted `matchIndex[self.ID] >= idx` — but the leader's `matchIndex` for itself was only updated after receiving a response from itself, which never happened (the leader doesn't send RPCs to itself).

Result: the leader always counted 0 peers for itself and required `quorum` votes from peers alone, effectively raising the quorum requirement by 1 for all entries. In a 3-node cluster, the leader needed 2 peer acknowledgements instead of 1, making writes require all 3 nodes (no fault tolerance).

**The fix:**

```go
count := 1  // leader always has the entry (the +1)
for peerID, match := range matchIndex {
    if match >= idx {
        count++
    }
}
```

`matchIndex` is only populated for peers. The leader's own presence is always the implicit `+1`.

---

## Single-goroutine apply loop

**Decision:** A single goroutine drains `CommitCh` and applies entries to the storage engine sequentially.

**Alternative:** Parallel application for throughput.

**Why single-threaded?**

Raft's state machine safety requirement is: if server A applies entry X at index N, no server B applies a different entry Y at index N. This requires entries to be applied in strict log order.

A single goroutine trivially satisfies this. With parallel application, you'd need per-key ordering or a global ordering constraint, reintroducing serialisation at a different level.

The single-goroutine design is simple, obviously correct, and sufficient for this use case. etcd uses the same pattern. For a storage engine that supports batched writes (like RocksDB), you'd batch a window of committed entries and write them in one system call — still sequentially ordered but with fewer round trips.

---

## ProposeWrite blocks until commit

**Decision:** `ProposeWrite` blocks the calling goroutine until the entry is committed and applied.

**Alternative:** Return immediately after proposing, let the client poll for completion.

**Why block?**

Blocking gives natural backpressure. If the Raft cluster is slow (under load, recovering from a failure, mid-election), `ProposeWrite` blocks. The gRPC server goroutine blocks. New client RPCs wait for a new goroutine. Eventually the thread pool fills and the OS starts rejecting new connections. This is the right behaviour: the system slows down proportionally to its actual capacity rather than queuing unboundedly and eventually running out of memory.

Polling-based designs require clients to manage retry timeouts, exponential backoff, and completion callbacks — significantly more complex. For a synchronous CLI and a simple library API, blocking is the right trade-off.

**What about the term check?**

`proposeWrite` stores the `term` alongside the `logIndex` in the pending map. If the entry at `logIndex` is applied with a different term (which happens when a new leader re-uses the index), the client knows its entry was overwritten and must retry. This prevents the client from accepting a false OK.

---

## Atomic rename for hard state

**Decision:** Write `hardstate.json` by writing to `hardstate.tmp` first, then renaming.

**Why?**

`os.WriteFile` truncates the destination file and writes bytes. If the machine loses power after truncation but before all bytes are written, the file contains partial data — say, the first 10 bytes of a 40-byte JSON object. The JSON is invalid. `LoadHardState` fails. The node can't start.

`os.Rename` is atomic on POSIX filesystems (guaranteed by POSIX) and on NTFS (Windows, guaranteed by the file system driver). Either the rename happens or it doesn't; there is no intermediate state where `hardstate.json` is partially updated.

This is a standard pattern used by databases, distributed systems, and package managers everywhere. Git uses it for ref updates. SQLite uses it for journal files.

---

## gRPC as the transport protocol

**Decision:** Use gRPC (Protocol Buffers over HTTP/2) for both peer-to-peer Raft RPCs and client-facing KV operations.

**Alternatives:** Raw TCP + custom framing, HTTP/1.1 + JSON, Thrift, Cap'n Proto.

**Why gRPC?**

1. **Type safety**: `.proto` definitions generate client and server stubs. The compiler catches mismatches before runtime.
2. **Bidirectional streaming**: not used here, but available for future snapshot streaming (sending large snapshots as chunked streams).
3. **Multiplexing**: HTTP/2 multiplexes multiple RPCs over one TCP connection, avoiding head-of-line blocking and the overhead of separate connections per RPC.
4. **Ecosystem**: health checks, load balancing, interceptors, and distributed tracing all have gRPC-native solutions.
5. **Binary efficiency**: protobuf encoding is ~5–10x more compact than JSON.

The `.proto` file also serves as the API contract — a single file that documents every message and service the system exposes, versioned with the code.

---

## LeaderHint as an address, not a node ID

**Decision:** The `LeaderHint` field in responses contains a dialable gRPC address (`host:port`), not a logical node ID (`node1`).

**Why not send the node ID?**

The client only knows addresses — that's what it dials. If the hint is `"node1"` and the client's peer map is `{"node1": "localhost:7001"}`, the client must do a lookup. But the client doesn't have the full peer map in every context (e.g., the chaos harness constructs CLI subprocesses that only receive addresses).

By translating the node ID to an address on the server side (using `cfg.Peers[leaderID]`), the client can redirect without any knowledge of the cluster topology. It just dials the hint address directly.

---

## Log snapshotting

**Decision:** the state machine snapshots the LSM engine and compacts the Raft log every `snapshotInterval` applied entries (`server.WithSnapshotInterval`, off by default). A lagging follower whose `nextIndex` falls at or before the leader's compaction point receives the snapshot via chunked `InstallSnapshot` RPCs instead of individual log entries.

**The problem this solves:**

Without snapshots, startup time and per-follower catch-up time both grow linearly with total log length — a node that's fallen behind (or is restarting after processing millions of writes) must replay every entry from the beginning. Snapshotting bounds both: a node only ever needs to replay entries after the most recent snapshot, and a follower that's fallen far enough behind gets a single state transfer instead of a long tail of individual `AppendEntries` calls.

**How it fits together (bottom to top):**

- `storage.Engine.Snapshot()`/`LoadSnapshot()` — engine-level, Raft-agnostic. `Snapshot()` merges the active memtable, the immutable memtable (if a flush is mid-flight), and all on-disk SSTables using the same newest-wins/tombstone-dropping rules `Get()` and compaction already use, rather than forcing a flush first and trusting it landed — the latter has a race where a concurrent background flush could leave very recent writes out of the snapshot.
- `wal.WAL.TruncatePrefix` / `raft.PersistentState.{Save,Load}Snapshot` — durable storage for the snapshot bytes plus the `(lastIncludedIndex, lastIncludedTerm)` it covers, and a way to discard the WAL entries it makes redundant. `walState` packs the index/term into the same file as the snapshot bytes, rather than a separate metadata file, so one atomic temp-file+rename can't leave them pointing at different snapshots after a crash mid-write.
- `raft.Node` — owns compaction bookkeeping (`lastIncludedIndex`/`lastIncludedTerm`, and `logPos()` to translate a Raft index into the correct position in the now-shorter in-memory log) and the wire protocol (`CompactLog` to record a locally-taken snapshot, `HandleInstallSnapshot` to receive one, `replicateToPeer` switching to chunked sends when a peer's `nextIndex` has fallen behind the compaction point). Node has no opinion on *when* to snapshot — only on how to record that one happened.
- `server.StateMachine` — decides *when*: every Nth applied entry, synchronously in the same apply-loop goroutine (never concurrently with applying the next entry, which is what keeps a snapshot's contents consistent with the index it's recorded under), and applies snapshots it receives from a leader.

**Design choices worth calling out:**

- *Discard-entire-log-on-install, not retain-matching-suffix.* The Raft paper (§7) notes a follower receiving `InstallSnapshot` can keep any suffix of its own log that happens to match the leader's, rather than discarding everything. This implementation always discards and starts fresh from the snapshot's sentinel entry — simpler, always correct, at the cost of occasionally re-replicating a few entries the follower already had.
- *Dedup table reset on snapshot install.* `StateMachine`'s per-client sequence-number map has no representation in the snapshot format, so installing one wipes it. A client whose write was folded into the snapshot could in principle retry after this and be reapplied. Real systems like etcd embed the session table in the snapshot itself to avoid this; left out here to keep the snapshot format simple — documented, not silently accepted.
- *Interaction with the current-term-commit rule.* A snapshot can only ever cover applied (and therefore committed) entries, so a peer's `matchIndex` jumping to `lastIncludedIndex` after a successful install can never retroactively "commit" anything through `maybeAdvanceCommit` — those indices aren't even addressable in the compacted log anymore. The one thing that had to change was every raw `n.log[idx]` access in the replication/commit path, which now goes through `logPos()` instead — verified as a behavior-preserving refactor on its own, before any snapshot-producing code was added (see `raft/node.go`'s `logPos` doc comment).

**A gap found while building this:** testing the leader→follower catch-up path surfaced a real, pre-existing liveness issue — this implementation has no PreVote phase, so a disconnected node's election term drifts unboundedly and is disruptive on reconnection. Not a snapshotting bug, and not a safety issue, but real enough to be worth its own section — see "No PreVote" below.

---

## No PreVote

**Decision:** a follower's election timer fires unconditionally when it hasn't heard from a leader, immediately incrementing `currentTerm` and becoming a candidate.

**The cost:**

A node that's partitioned from the rest of the cluster — no leader reachable, no peers reachable — still has its election timer running. With nobody to reset it, that timer fires every 150-300ms indefinitely, and `currentTerm` climbs by one each time even though the node can never actually win (it never receives a single vote). The longer it stays partitioned, the further its term drifts from the rest of the cluster's.

This becomes a real liveness problem on reconnection, not just a wasted-CPU one: the rejoining node's inflated term is higher than the current legitimate leader's, so the leader's heartbeats — carrying its own, lower, genuinely-in-use term — get rejected outright by `HandleAppendEntries`'s stale-term check. The rejoining node's election timer is never reset by a real leader as a result, so it tries again, forcing the leader to adopt the higher term and step down for a fresh election — one the rejoining node still can't win, since `candidateLogUpToDate` correctly rejects its stale log. This repeats, closing the term gap by roughly one per cycle, until the cluster is disrupted enough times to catch up. Confirmed directly while testing snapshot catch-up (see `TestInstallSnapshotChunkedTransferApplies` in `raft/raft_test.go`): a follower disconnected for even a few seconds destabilized a fully-connected 2-node remainder for 10+ seconds after reconnecting.

**What PreVote would fix:**

Before actually incrementing its term, a candidate first runs a non-binding "pre-vote" round: would peers vote for me if I were to start a real election? A partitioned node's pre-vote requests never reach anyone, so it never learns it *could* win — and since it never learns that, it never bumps its real term while alone. `currentTerm` only advances once an actual election starts, which only happens after a majority has already signaled they'd support it. Reconnecting is then a non-event: the node's term is still close to the cluster's, so no disruptive step-down cascade follows.

**Why it's not implemented:**

It's an additive protocol change (a new RPC or a flag on the existing one, plus a candidate-side pre-check) that's out of scope for the snapshotting work that surfaced it. Nothing here is unsafe without it — the term-convergence cycle above always terminates, and no Raft safety property depends on PreVote — it's purely a liveness/availability improvement for the reconnection case.
