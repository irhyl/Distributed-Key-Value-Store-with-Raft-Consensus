# LSM-Tree Storage Engine

## The core insight

Random writes to disk are slow - even on SSDs, random I/O costs orders of magnitude more than sequential I/O. The key idea behind an LSM (Log-Structured Merge) tree is: **never do random writes**. Instead:

1. Accept all writes into an in-memory buffer (Memtable)
2. When the buffer fills, write it to disk as a single sorted sequential file (SSTable)
3. Periodically merge multiple SSTable files in the background (compaction)

Reads pay a cost - multiple places must be checked - but writes are always sequential, which is fast.

This is the same architecture used by LevelDB, RocksDB, Cassandra, HBase, and InfluxDB.

---

## Component overview

```
Write path:
  Put(k, v) ──► Memtable (sorted, in-memory)
                    │  when size ≥ 4MB
                    ▼
               immutable Memtable
                    │  background flush
                    ▼
               SSTable file (sorted, immutable, on-disk)
                    │  when count ≥ 8 files
                    ▼
               Compaction → single merged SSTable

Read path:
  Get(k) ──► active Memtable
                    │  miss
                    ▼
             immutable Memtable (if one is being flushed)
                    │  miss
                    ▼
             SSTable[0] (newest) → bloom check → scan
             SSTable[1]
             ...
             SSTable[N] (oldest)
```

---

## Memtable

The Memtable is a sorted, in-memory buffer for recent writes. It is the only place where data is mutable - all other structures are immutable once written.

### Data structure

```go
type Memtable struct {
    entries []memEntry  // sorted by key, ascending
    size    int         // approximate byte count
    seq     uint64      // monotonic sequence counter
}

type memEntry struct {
    key   string
    value []byte
    kind  entryKind  // kindPut or kindDelete
    seq   uint64
}
```

Entries are stored in a slice sorted by key. Binary search (`sort.Search`) locates the insertion point in O(log n). When a key is updated, the old entry is replaced in-place - the slice stays sorted and the size stays bounded.

### Why a sorted slice instead of a hash map?

An unsorted hash map would give O(1) point lookups but would require a full sort at flush time. A sorted slice gives O(log n) everything - insert, lookup, range scan - and is already sorted when it's time to flush. No sorting step at flush time means the flush I/O is minimal and predictable.

### Tombstones

A `Delete` operation does **not** remove the key from the Memtable. Instead it inserts a tombstone (`kind = kindDelete`). The reason: an older SSTable file might still have the key. If we simply removed the key from the Memtable, a subsequent read would find the old value in an SSTable and incorrectly return it.

Tombstones propagate down through compaction until the entry is in the oldest SSTable, at which point no older value can exist and the tombstone can be safely dropped.

### Flush threshold

When `size >= 4MB`, the Memtable is flushed to an SSTable. The threshold is configurable but 4MB is a common default (RocksDB defaults to 64MB; we use smaller for easier testing).

On flush:
1. The active Memtable becomes `immutable` (read-only, still visible to reads)
2. A new empty Memtable becomes `active`
3. The background `flushWorker` goroutine writes the immutable Memtable to a new SSTable file

---

## SSTable

An SSTable (Sorted String Table) is an immutable, sorted file on disk. Once written it is never modified. Reads use bloom filters to avoid unnecessary disk I/O, and linear scan to find entries.

### On-disk layout

```
┌──────────────────────────────────────────────────────────────┐
│  DATA BLOCK                                                  │
│  For each entry (sorted by key):                             │
│    4 bytes: key length                                       │
│    N bytes: key                                              │
│    4 bytes: value length                                     │
│    M bytes: value                                            │
│    1 byte:  kind (0 = put, 1 = delete/tombstone)             │
│  ...                                                         │
├──────────────────────────────────────────────────────────────┤
│  BLOOM FILTER BLOCK  (1024 bytes)                            │
│  8192-bit bit array; 7 hash functions                        │
├──────────────────────────────────────────────────────────────┤
│  FOOTER  (24 bytes, fixed - always at file end)              │
│    8 bytes: bloom_offset  (byte offset of bloom block)       │
│    8 bytes: bloom_size    (byte length of bloom block)       │
│    4 bytes: entry_count                                      │
│    4 bytes: magic number  (0xdeadbeef)                       │
└──────────────────────────────────────────────────────────────┘
```

The fixed-size footer is always at the end of the file. A reader jumps directly to `fileSize - 24` to read it, extracting the bloom filter location and entry count without scanning the data block. This is the same trick used by LevelDB, SQLite, and Parquet.

### Bloom filter

A bloom filter is a space-efficient probabilistic data structure that can answer "is this key definitely NOT in this file?" in O(1) time.

Properties:
- **Zero false negatives**: if a key is in the file, the bloom filter always says "maybe yes"
- **~1% false positive rate**: occasionally says "maybe yes" for keys not in the file
- **Fixed memory cost**: 1 KB regardless of how many entries are in the SSTable

The implementation uses 7 hash functions over a 1024-byte (8192-bit) bit array. For each key, all 7 hash positions must be set to 1 for the filter to say "maybe yes". A single 0 bit means "definitely not present".

When a key is missing:
- Without bloom filter: must scan the entire data block to confirm absence
- With bloom filter: check 7 bits (O(1)), skip the file 99% of the time

For a cluster serving mostly reads of a large dataset spread across many SSTables, bloom filters are the primary read performance lever.

### Read path for a single SSTable

```
1. Open footer: read last 24 bytes, extract bloom offset, verify magic
2. Read bloom filter block
3. Check bloom filter for the key
   → if definitely absent: return (nil, notFound) without reading data block
4. Scan data block from start
   → compare each entry's key to the target
   → return first match (or notFound if end of file)
```

Step 4 is a linear scan. Production LSMs (LevelDB, RocksDB) use a separate index block with one entry per data block to enable binary search. That optimisation reduces read amplification from O(n) to O(log n) per SSTable but adds implementation complexity. For this project's scale, linear scan is sufficient.

---

## Compaction

Without compaction, SSTables accumulate indefinitely. Every read must check more files, old tombstones are never cleaned up, and disk usage grows unbounded.

Compaction merges multiple SSTables into one. The result is smaller, has no duplicate entries, and has no tombstones for keys that no longer exist anywhere else.

### Trigger

Compaction runs when the SSTable count reaches 8 (the `compactionThreshold`). It also runs on a 30-second timer as a catch-all.

### Algorithm

```
1. Read all entries from all SSTables (oldest first, so newer values appear last)
2. MergeEntries: multi-way merge, keeping only the newest value per key
3. During merge, drop tombstones for keys that don't appear in any older SSTable
   (i.e., all entries with kind=delete that have no corresponding put below them)
4. Write merged entries to a new SSTable file
5. Atomically swap: replace the old SSTable slice with the single new one
6. Delete old SSTable files
```

The merge produces a file that is the same as reading all the old SSTables at once - but stored on disk and queryable with one bloom filter check instead of N.

### Write amplification

Each byte written to the cluster is written multiple times:
1. To the WAL (once, on the leader)
2. To the Memtable (in memory)
3. To an SSTable (at flush time)
4. To a merged SSTable (at compaction time)
5. Possibly again in later compaction rounds

This is the fundamental trade-off of LSM trees. B-trees don't have write amplification beyond the initial write, but they require random I/O for in-place page updates. For write-heavy workloads on spinning disks or high-throughput SSDs, LSM wins. For read-heavy workloads, B-tree wins.

---

## Engine lifecycle

### Opening

```go
func Open(dir string) (*Engine, error) {
    // 1. Create directory if it doesn't exist
    // 2. Load existing SSTable files from disk (sorted newest-first by filename)
    // 3. Set the SSTable sequence counter past the highest existing file
    // 4. Create empty active Memtable
    // 5. Start flushWorker goroutine
    // 6. Start compactionWorker goroutine
}
```

### Closing

```go
func (e *Engine) Close() error {
    // 1. Mark engine as closed (atomic CAS)
    // 2. Flush the active Memtable to SSTable synchronously
    // 3. Close the closeCh channel (signals workers to stop)
    // 4. Wait for all background goroutines to finish
}
```

Close flushes the active Memtable first so no in-memory data is lost on a graceful shutdown. Crash recovery (from WAL replay) handles the ungraceful case.

---

## Concurrency

The engine uses a single `sync.RWMutex`:

- **Writes** (`Put`, `Delete`): acquire write lock, insert into Memtable, release
- **Reads** (`Get`): acquire read lock, take snapshot of Memtable and SSTable pointers, release, then read without the lock
- **Flush**: acquire write lock to swap active/immutable Memtables; release; write SSTable file without lock; re-acquire to update sstables slice
- **Compaction**: acquire read lock to copy SSTable slice; release; do all I/O without lock; re-acquire to atomically swap in new SSTable

The lock is never held during disk I/O. All slow operations (SSTable writes, reads, merges) happen outside the lock. The lock only protects in-memory pointer swaps, which are nanosecond-scale operations.

---

## Implementation reference

| Symbol | File | Description |
|--------|------|-------------|
| `Memtable` | [storage/memtable.go](../storage/memtable.go) | Sorted in-memory buffer; binary search insert/lookup |
| `memEntry` | [storage/memtable.go](../storage/memtable.go) | A single record with key, value, kind, seq |
| `SSTableWriter` | [storage/sstable.go](../storage/sstable.go) | Writes a sorted entry slice to a new SSTable file |
| `SSTableReader` | [storage/sstable.go](../storage/sstable.go) | Reads from an SSTable; holds bloom filter in memory |
| `MergeEntries` | [storage/sstable.go](../storage/sstable.go) | Multi-way merge for compaction |
| `Engine` | [storage/engine.go](../storage/engine.go) | Coordinates all layers; public API |
| `Engine.flushWorker` | [storage/engine.go](../storage/engine.go) | Background Memtable → SSTable flush |
| `Engine.compactionWorker` | [storage/engine.go](../storage/engine.go) | Background SSTable merge |
| `Engine.loadSSTables` | [storage/engine.go](../storage/engine.go) | Recovery: load existing SSTable files on startup |
| `Engine.Snapshot` | [storage/snapshot.go](../storage/snapshot.go) | Point-in-time dump of all live keys, merging memtable(s) + SSTables |
| `Engine.LoadSnapshot` | [storage/snapshot.go](../storage/snapshot.go) | Replace all on-disk and in-memory state with a snapshot's contents |
