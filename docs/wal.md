# Write-Ahead Log (WAL)

## What the WAL is for

The WAL is a crash-safe journal. Before any Raft log entry is applied to the state machine, it is written here. If the process crashes mid-operation, the WAL survives and the node can replay it on restart to reconstruct its exact pre-crash state.

Without a WAL, a crash between "entry committed" and "entry applied to LSM" would mean the entry is in the Raft majority but not in this node's storage, the node would serve stale reads after restarting.

---

## On-disk format

Each record in the WAL has a fixed-size header followed by a variable-length payload:

```
┌──────────────────────────────────────────────────────────┐
│  HEADER (8 bytes)                                        │
│    bytes 0-3: payload length   (uint32, little-endian)   │
│    bytes 4-7: CRC32 checksum   (uint32, little-endian)   │
├──────────────────────────────────────────────────────────┤
│  PAYLOAD (N bytes)                                       │
│    protobuf-serialised LogEntry                          │
└──────────────────────────────────────────────────────────┘
```

The checksum is computed over the payload bytes only, not the header. On read, the checksum is recomputed and compared. A mismatch means the record was partially written, the machine likely lost power mid-write. The reader stops at that point and treats everything before it as the authoritative log.

### Why this format?

Four bytes of length let the reader know exactly how many bytes to read next without scanning for a delimiter. Four bytes of CRC32 detect corruption without needing to understand the protobuf encoding. Together they handle every crash scenario:

| Crash point | What the reader sees | Result |
|-------------|----------------------|--------|
| After header write, before payload | Header OK, payload read fails (EOF/short read) | Stop; treat prior records as final |
| Mid-payload | Header OK, CRC mismatch | Stop; treat prior records as final |
| After full record, before fsync | Segment file unchanged at prior length | Record not visible; safe to re-propose |
| After fsync | Full record readable | Record replayed on restart |

---

## Segment files

The WAL is split into multiple segment files rather than a single growing file. Each segment is capped at 8 MB. When a new entry would exceed that limit, the current segment is synced and closed, and a new one is created.

Segment files are named by the index of their first entry:

```
0000000000000001.wal   ← entries 1-800 (say)
0000000000000801.wal   ← entries 801-1500
0000000000001501.wal   ← entries 1501-current
```

Benefits of segmentation:

- **Log compaction**: once a snapshot is taken up to index N, all segments whose highest index is ≤ N can be deleted in a single `os.Remove` call. No need to rewrite a multi-GB file.
- **Bounded recovery time**: replaying 8 MB is fast. A single monolithic file could grow unbounded.
- **Concurrent reads during writes**: older segments are immutable once rolled; they can be read concurrently with new writes to the active segment.

---

## fsync strategy

Durability requires that data reaches persistent storage (actual disk platters or flash cells), not just the OS page cache. The OS may hold writes in memory for several seconds before flushing them. A crash in that window would lose data even though the write "succeeded" from the application's perspective.

raftkv uses two distinct strategies:

### Single-entry Append

```go
func (w *WAL) Append(entry *pb.LogEntry) error {
    // ... write record ...
    w.writer.Flush()
    w.current.Sync()  // fsync
    w.lastIndex = entry.Index
    return nil
}
```

Every single-entry append fsyncs immediately. The client RPC does not return until the entry is on disk. This is the safe but slow path - typically used when a single proposal is in flight.

### Batch AppendBatch

```go
func (w *WAL) AppendBatch(entries []*pb.LogEntry) error {
    for _, entry := range entries {
        // ... write each record (no fsync) ...
    }
    w.writer.Flush()
    w.current.Sync()  // single fsync for the whole batch
    return nil
}
```

When Raft replicates a batch of entries (e.g., catching up a lagging follower), all entries are written with a single fsync at the end. This is the production hot path. On a modern SSD, one fsync per batch versus one fsync per entry is the difference between 50k writes/sec and 500 writes/sec.

The trade-off: if the machine loses power mid-batch, only the records before the fsync are durable. But that is safe by design - only fsynced entries are acknowledged to the leader, and the leader only commits once a majority have acknowledged.

---

## Truncation

When a follower discovers its log conflicts with the leader's, it must roll back. The leader sends the correct entries in the next `AppendEntries`, but first the conflicting tail of the follower's log must be removed.

```go
func (w *WAL) TruncateSuffix(keepIndex uint64) error {
    // 1. Close current segment (important on Windows: cannot delete open files)
    // 2. Read all entries from all segments
    // 3. Delete all segment files
    // 4. Create a new segment starting at index 1
    // 5. Rewrite only entries with index <= keepIndex
    // 6. fsync
}
```

This is a full rewrite - read everything, delete everything, write the kept entries fresh. It's expensive but correct and simple. Truncation is rare (it only happens when a follower's log diverged, which requires a leader change during active replication).

### Windows portability note

On POSIX systems, `os.Remove` on an open file succeeds - the file is unlinked from the directory but remains accessible via the open file descriptor until it's closed. On Windows, `os.Remove` on an open file returns an error. `TruncateSuffix` therefore closes and nils `w.current` before calling `os.Remove`, making it correct on both platforms.

### TruncatePrefix

The mirror-image operation: instead of dropping a conflicting tail, `TruncatePrefix(discardIndex)` drops everything up to and including `discardIndex`, called after a Raft snapshot has durably captured that range. Same rewrite-from-scratch approach and the same Windows close-before-remove handling, just keeping the entries above the cut point instead of below it. One subtlety: if nothing is left to keep (the whole log got discarded), `lastIndex` is set to `discardIndex`, not reset to 0 - the log logically continues from there, so the next `Append` picks up at `discardIndex+1`.

---

## Hard state

Raft requires two pieces of non-log state to survive crashes: `currentTerm` and `votedFor`. These are stored separately from the WAL in a tiny JSON file at `{dataDir}/wal/hardstate.json`.

Writes use the atomic rename pattern:

```
1. Marshal JSON to bytes
2. Write bytes to hardstate.tmp
3. os.Rename(hardstate.tmp, hardstate.json)   ← atomic on POSIX and NTFS
```

If the process crashes between step 2 and step 3, `hardstate.tmp` exists but `hardstate.json` is unchanged - the old values are intact. If it crashes during step 2, `hardstate.tmp` is partial or absent, but again `hardstate.json` is intact.

This is important because a corrupted `votedFor` could allow a node to vote twice in the same term, which violates election safety and could result in two leaders in the same term.

---

## Recovery sequence

On node startup:

```
1. Open all *.wal files, sort by name (= sort by first index)
2. For each segment, call readSegment():
     a. Read header (8 bytes)
     b. Read payload (length bytes)
     c. Verify CRC32; if mismatch, stop reading this segment
     d. Unmarshal LogEntry from payload
     e. Append to entries slice
3. Return the complete entry slice to the Raft node
4. Raft node appends recovered entries to its in-memory log
5. Open hardstate.json; load (currentTerm, votedFor)
6. Node starts as Follower; joins the cluster
```

---

## Implementation reference

| Symbol | Location | Description |
|--------|----------|-------------|
| `WAL` struct | [wal/wal.go:42](../wal/wal.go) | Main type; holds dir, current segment, buffered writer, size, lastIndex, OnFsync hook |
| `Open` | [wal/wal.go:59](../wal/wal.go) | Open or create a WAL; recovers lastIndex on existing WAL |
| `Append` | [wal/wal.go:103](../wal/wal.go) | Write + fsync one entry |
| `AppendBatch` | [wal/wal.go:139](../wal/wal.go) | Write N entries + single fsync |
| `ReadAll` | [wal/wal.go:173](../wal/wal.go) | Replay full WAL; used on startup |
| `TruncateSuffix` | [wal/wal.go:197](../wal/wal.go) | Remove entries with index > keepIndex |
| `TruncatePrefix` | [wal/wal.go:263](../wal/wal.go) | Remove entries with index <= discardIndex, after a snapshot |
| `syncTimed` | [wal/wal.go:351](../wal/wal.go) | Internal: Sync() plus reporting duration via OnFsync |
| `writeRecord` | [wal/wal.go:362](../wal/wal.go) | Internal: write one `[len][crc][data]` record |
| `createSegment` | [wal/wal.go:382](../wal/wal.go) | Open a new segment file with `O_EXCL` |
| `rollSegment` | [wal/wal.go:399](../wal/wal.go) | Flush + sync current, then createSegment |
| `readSegment` | [wal/wal.go:423](../wal/wal.go) | Read all valid records from one segment file |
