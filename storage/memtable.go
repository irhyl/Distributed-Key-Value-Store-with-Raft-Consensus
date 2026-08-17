// Package storage implements an LSM-tree storage engine.
//
// Data flow:
//   Writes → Memtable (in-memory) → SSTable files (on-disk, immutable)
//   Reads  → Memtable first, then SSTables newest-to-oldest
//
// This file: the Memtable, a sorted in-memory buffer.
// When it exceeds maxMemtableSize, it's flushed to an SSTable.

package storage

import (
	"sort"
	"sync"
)

const (
	maxMemtableSize = 4 * 1024 * 1024 // 4MB - flush to SSTable when exceeded
)

// entryKind distinguishes live values from deletions.
// Deletions are stored as "tombstones" - we can't just remove the key,
// because an older SSTable might still have the value. The tombstone
// propagates through compaction until it reaches the oldest level.
type entryKind uint8

const (
	kindPut    entryKind = 0
	kindDelete entryKind = 1
)

// memEntry is a single record inside the memtable.
type memEntry struct {
	key   string
	value []byte
	kind  entryKind
	seq   uint64 // sequence number - higher = newer (for MVCC-style reads)
}

// Memtable is a sorted, in-memory write buffer.
// Writes are O(log n) via binary search on the sorted key slice.
// Safe for concurrent use.
type Memtable struct {
	mu      sync.RWMutex
	entries []memEntry // sorted by (key ASC, seq DESC)
	size    int        // approximate byte size
	seq     uint64     // monotonically increasing sequence counter
}

// NewMemtable creates an empty memtable.
func NewMemtable() *Memtable {
	return &Memtable{}
}

// Put inserts or updates a key.
func (m *Memtable) Put(key string, value []byte) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.seq++
	m.insert(memEntry{key: key, value: value, kind: kindPut, seq: m.seq})
	m.size += len(key) + len(value) + 16
}

// Delete marks a key as deleted (tombstone).
func (m *Memtable) Delete(key string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.seq++
	m.insert(memEntry{key: key, value: nil, kind: kindDelete, seq: m.seq})
	m.size += len(key) + 16
}

// Get returns the most recent value for key.
// Returns (value, true) if found and alive, (nil, false) if not found or deleted.
func (m *Memtable) Get(key string) ([]byte, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	// Find the leftmost entry with this key
	idx := sort.Search(len(m.entries), func(i int) bool {
		return m.entries[i].key >= key
	})

	if idx >= len(m.entries) || m.entries[idx].key != key {
		return nil, false // key not in memtable
	}

	e := m.entries[idx]
	if e.kind == kindDelete {
		return nil, false // tombstone - key was deleted
	}
	return e.value, true
}

// ShouldFlush returns true when the memtable has exceeded its size budget.
func (m *Memtable) ShouldFlush() bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.size >= maxMemtableSize
}

// Size returns the approximate byte size of the memtable.
func (m *Memtable) Size() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.size
}

// Snapshot returns a sorted copy of all entries for flushing to SSTable.
// The copy is safe to read without holding the lock.
func (m *Memtable) Snapshot() []memEntry {
	m.mu.RLock()
	defer m.mu.RUnlock()
	snap := make([]memEntry, len(m.entries))
	copy(snap, m.entries)
	return snap
}

// insert adds an entry, maintaining sort order by key.
// If the key already exists, the new entry (higher seq) goes first.
func (m *Memtable) insert(e memEntry) {
	idx := sort.Search(len(m.entries), func(i int) bool {
		return m.entries[i].key >= e.key
	})

	// If an entry for this key already exists, replace it.
	// (In a full MVCC implementation you'd keep all versions; we keep only latest.)
	if idx < len(m.entries) && m.entries[idx].key == e.key {
		m.entries[idx] = e
		return
	}

	// Insert at idx, shifting everything right
	m.entries = append(m.entries, memEntry{})
	copy(m.entries[idx+1:], m.entries[idx:])
	m.entries[idx] = e
}
