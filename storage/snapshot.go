// snapshot.go - engine-wide point-in-time snapshot capture and restore.
//
// This is what makes Raft log compaction possible: instead of replaying
// every WAL entry since the beginning of time, a lagging node can be
// brought up to date with a single blob representing "all live keys as of
// index N", then replay only the (small) tail of entries after that.
//
// The storage package stays Raft-agnostic here - Snapshot/LoadSnapshot deal
// only in key/value bytes. raft.Node owns the index/term metadata that goes
// with a snapshot; server.StateMachine is what ties the two together.

package storage

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"log"
	"os"
)

// Snapshot returns a serialized, point-in-time view of every live key in
// the engine: the active memtable, the immutable memtable (if a flush is
// mid-flight), and all on-disk SSTables, merged with the same newest-wins
// and tombstone-dropping rules as a normal read - so it's correct
// regardless of what the background flush/compaction workers are doing
// concurrently, without needing to synchronize with them.
func (e *Engine) Snapshot() ([]byte, error) {
	e.mu.RLock()
	active := e.active
	immutable := e.immutable
	sstables := e.sstables // newest first
	e.mu.RUnlock()

	// MergeEntries picks the newest entry for a key by taking the input with
	// the highest slice index, so inputs must be ordered oldest → newest:
	// SSTables (oldest first), then immutable, then active last.
	var inputs [][]memEntry
	for i := len(sstables) - 1; i >= 0; i-- {
		entries, err := sstables[i].IterateAll()
		if err != nil {
			return nil, fmt.Errorf("storage: snapshot read %s: %w", sstables[i].Path(), err)
		}
		inputs = append(inputs, entries)
	}
	if immutable != nil {
		inputs = append(inputs, immutable.Snapshot())
	}
	inputs = append(inputs, active.Snapshot())

	// MergeEntries already drops tombstones, so every entry here is live.
	merged := MergeEntries(inputs)
	return encodeSnapshot(merged)
}

// LoadSnapshot replaces the engine's entire on-disk and in-memory state
// with the contents of a snapshot produced by Snapshot(). Used when a
// follower receives InstallSnapshot from the leader because it's too far
// behind for normal log replication to catch it up.
func (e *Engine) LoadSnapshot(data []byte) error {
	entries, err := decodeSnapshot(data)
	if err != nil {
		return fmt.Errorf("storage: decode snapshot: %w", err)
	}

	fresh := NewMemtable()
	for _, entry := range entries {
		fresh.Put(entry.key, entry.value)
	}

	e.mu.Lock()
	oldSSTables := e.sstables
	e.sstables = nil
	e.active = fresh
	e.immutable = nil
	e.mu.Unlock()

	// Old SSTable files are no longer reachable from e.sstables; remove them.
	// A concurrent compaction cycle that already captured a reference to one
	// of these may see a transient read error - it logs and skips that
	// cycle, which is safe and self-correcting on the next trigger.
	for _, sst := range oldSSTables {
		if err := os.Remove(sst.Path()); err != nil && !os.IsNotExist(err) {
			log.Printf("storage: remove stale sstable %s: %v\n", sst.Path(), err)
		}
	}

	// Make the snapshot durable immediately: a crash right after LoadSnapshot
	// returns must not lose it and fall back to an empty engine.
	e.mu.Lock()
	if e.active.Size() > 0 {
		e.immutable = e.active
		e.active = NewMemtable()
	}
	e.mu.Unlock()
	e.doFlush()

	return nil
}

// ── Snapshot wire format ──────────────────────────────────────────────────
//
// A flat stream of [keyLen uint32][key][valLen uint32][val] records, EOF
// terminated. All entries are live puts (Snapshot() only ever produces
// tombstone-free input), so no kind byte is needed.

func encodeSnapshot(entries []memEntry) ([]byte, error) {
	var buf bytes.Buffer
	w := bufio.NewWriter(&buf)

	for _, e := range entries {
		if err := binary.Write(w, binary.LittleEndian, uint32(len(e.key))); err != nil {
			return nil, err
		}
		if _, err := w.WriteString(e.key); err != nil {
			return nil, err
		}
		if err := binary.Write(w, binary.LittleEndian, uint32(len(e.value))); err != nil {
			return nil, err
		}
		if _, err := w.Write(e.value); err != nil {
			return nil, err
		}
	}
	if err := w.Flush(); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func decodeSnapshot(data []byte) ([]memEntry, error) {
	r := bufio.NewReader(bytes.NewReader(data))
	var entries []memEntry

	for {
		var keyLen uint32
		if err := binary.Read(r, binary.LittleEndian, &keyLen); err != nil {
			if err == io.EOF {
				break
			}
			return nil, fmt.Errorf("read key length: %w", err)
		}
		keyBytes := make([]byte, keyLen)
		if _, err := io.ReadFull(r, keyBytes); err != nil {
			return nil, fmt.Errorf("read key: %w", err)
		}

		var valLen uint32
		if err := binary.Read(r, binary.LittleEndian, &valLen); err != nil {
			return nil, fmt.Errorf("read value length: %w", err)
		}
		var valBytes []byte
		if valLen > 0 {
			valBytes = make([]byte, valLen)
			if _, err := io.ReadFull(r, valBytes); err != nil {
				return nil, fmt.Errorf("read value: %w", err)
			}
		}

		entries = append(entries, memEntry{key: string(keyBytes), value: valBytes, kind: kindPut})
	}

	return entries, nil
}
