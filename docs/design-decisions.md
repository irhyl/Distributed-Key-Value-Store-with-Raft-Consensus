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

## No snapshots (yet)

**Decision:** `HandleInstallSnapshot` is a no-op stub. Recovery is always done by full WAL replay.

**The cost:**

Startup time grows linearly with log length. A cluster that has processed 1 million writes must replay all 1 million WAL entries before the node can serve requests. Each entry is a protobuf unmarshal + LSM write — on a fast machine, ~1 million/sec. So 1 million entries takes ~1 second. Acceptable for moderate logs, but it compounds: after 10 million entries, restart takes ~10 seconds.

**What snapshots would fix:**

A snapshot is a point-in-time serialisation of the entire LSM state plus the `lastApplied` index. On restart, instead of replaying entries 1 through N, the node loads the snapshot (covering 1 through S) and replays only entries S+1 through N. If snapshots are taken every 100k entries, restart never replays more than 100k entries regardless of total log size.

**Why it's not implemented:**

Snapshots are the most complex part of Raft. The leader must send snapshots to lagging followers (via `InstallSnapshot`), followers must apply snapshots to their state machines atomically, and both sides must handle chunked transfers for large snapshots. The correctness requirements are subtle enough that the Raft paper devotes a full section to them. Implementing it correctly without tests would be risky.

The system is correct and complete without snapshots — it just has a startup time proportional to log length. Adding snapshots is a well-defined extension that doesn't require changing any existing code.
