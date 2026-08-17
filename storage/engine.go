// Engine is the top-level LSM storage engine.
//
// Lifecycle of a write:
//   1. Put(key, value) → written to active memtable (in-memory, fast)
//   2. When memtable exceeds 4MB → flushed to a new SSTable file on disk
//   3. When SSTable count exceeds compactionThreshold → background compaction
//      merges SSTables, drops tombstones, produces a smaller set of files
//
// Lifecycle of a read:
//   1. Check active memtable (newest data)
//   2. Check immutable memtable if one exists (being flushed right now)
//   3. Check SSTables newest-to-oldest (first hit wins)
//
// This is the same architecture as LevelDB, RocksDB, and Cassandra's storage.

package storage

import (
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

const (
	compactionThreshold = 8  // trigger compaction when we have this many SSTables
	maxSSTables         = 16 // hard limit before writes block
)

// Engine is the main storage interface. Safe for concurrent use.
type Engine struct {
	mu        sync.RWMutex
	dir       string
	active    *Memtable   // current write target
	immutable *Memtable   // being flushed to disk (read-only)
	sstables  []*SSTableReader // newest first

	flushCh   chan struct{} // signal background flusher
	compactCh chan struct{} // signal background compactor
	closeCh   chan struct{}
	wg        sync.WaitGroup

	sstableSeq uint64 // atomic counter for SSTable filenames
	closed     int32  // atomic bool

	// OnCompaction, if set, is called after every compaction run with how
	// long it took. nil-safe, unset by default - keeps this package free of
	// any dependency on a specific metrics backend.
	OnCompaction func(time.Duration)
}

// Open opens (or creates) an LSM engine at dir.
func Open(dir string) (*Engine, error) {
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, fmt.Errorf("storage: mkdir: %w", err)
	}

	e := &Engine{
		dir:       dir,
		active:    NewMemtable(),
		flushCh:   make(chan struct{}, 1),
		compactCh: make(chan struct{}, 1),
		closeCh:   make(chan struct{}),
	}

	// Reload existing SSTable files
	if err := e.loadSSTables(); err != nil {
		return nil, err
	}

	// Start background workers
	e.wg.Add(2)
	go e.flushWorker()
	go e.compactionWorker()

	return e, nil
}

// Put stores a key-value pair.
func (e *Engine) Put(key string, value []byte) error {
	if atomic.LoadInt32(&e.closed) == 1 {
		return fmt.Errorf("storage: engine is closed")
	}

	e.mu.Lock()
	e.active.Put(key, value)
	shouldFlush := e.active.ShouldFlush()
	e.mu.Unlock()

	if shouldFlush {
		// Signal background flusher (non-blocking)
		select {
		case e.flushCh <- struct{}{}:
		default:
		}
	}

	return nil
}

// Delete marks a key as deleted.
func (e *Engine) Delete(key string) error {
	if atomic.LoadInt32(&e.closed) == 1 {
		return fmt.Errorf("storage: engine is closed")
	}

	e.mu.Lock()
	e.active.Delete(key)
	e.mu.Unlock()
	return nil
}

// Get retrieves a value by key.
// Returns (value, true) if found, (nil, false) if not found or deleted.
func (e *Engine) Get(key string) ([]byte, bool) {
	e.mu.RLock()
	active := e.active
	immutable := e.immutable
	sstables := e.sstables
	e.mu.RUnlock()

	// Layer 1: active memtable
	if val, ok := active.Get(key); ok {
		return val, true
	}

	// Layer 2: immutable memtable (currently being flushed)
	if immutable != nil {
		if val, ok := immutable.Get(key); ok {
			return val, true
		}
	}

	// Layer 3: SSTables, newest first
	for _, sst := range sstables {
		val, kind, found := sst.Get(key)
		if found {
			if kind == kindDelete {
				return nil, false // tombstone - key was deleted
			}
			return val, true
		}
	}

	return nil, false
}

// Close flushes all pending writes and shuts down background workers.
func (e *Engine) Close() error {
	if !atomic.CompareAndSwapInt32(&e.closed, 0, 1) {
		return nil // already closed
	}

	// Flush active memtable before closing
	e.mu.Lock()
	if e.active.Size() > 0 {
		e.immutable = e.active
		e.active = NewMemtable()
	}
	e.mu.Unlock()

	if e.immutable != nil {
		e.doFlush()
	}

	close(e.closeCh)
	e.wg.Wait()
	return nil
}

// ── Background workers ────────────────────────────────────────────────────────

// flushWorker waits for flush signals and writes the immutable memtable to disk.
func (e *Engine) flushWorker() {
	defer e.wg.Done()
	for {
		select {
		case <-e.flushCh:
			e.rotateAndFlush()
		case <-e.closeCh:
			return
		}
	}
}

// rotateAndFlush moves the active memtable to immutable and flushes it.
func (e *Engine) rotateAndFlush() {
	e.mu.Lock()
	if e.immutable != nil {
		// Previous flush still in progress - skip this cycle
		e.mu.Unlock()
		return
	}
	e.immutable = e.active
	e.active = NewMemtable()
	e.mu.Unlock()

	e.doFlush()

	// Signal compactor if we have too many SSTables
	e.mu.RLock()
	count := len(e.sstables)
	e.mu.RUnlock()
	if count >= compactionThreshold {
		select {
		case e.compactCh <- struct{}{}:
		default:
		}
	}
}

// doFlush writes the immutable memtable to a new SSTable file.
func (e *Engine) doFlush() {
	e.mu.RLock()
	imm := e.immutable
	e.mu.RUnlock()

	if imm == nil || imm.Size() == 0 {
		return
	}

	seq := atomic.AddUint64(&e.sstableSeq, 1)
	path := filepath.Join(e.dir, fmt.Sprintf("%016d.sst", seq))

	entries := imm.Snapshot()
	writer := NewSSTableWriter(path)
	if err := writer.Write(entries); err != nil {
		log.Printf("storage: flush error: %v\n", err)
		return
	}

	reader, err := OpenSSTable(path)
	if err != nil {
		log.Printf("storage: open new sst error: %v\n", err)
		return
	}

	// Prepend to sstables list (newest first)
	e.mu.Lock()
	e.sstables = append([]*SSTableReader{reader}, e.sstables...)
	e.immutable = nil
	e.mu.Unlock()
}

// compactionWorker merges SSTables in the background.
func (e *Engine) compactionWorker() {
	defer e.wg.Done()
	ticker := time.NewTicker(30 * time.Second) // also run periodically
	defer ticker.Stop()
	for {
		select {
		case <-e.compactCh:
			e.doCompaction()
		case <-ticker.C:
			e.mu.RLock()
			count := len(e.sstables)
			e.mu.RUnlock()
			if count >= compactionThreshold {
				e.doCompaction()
			}
		case <-e.closeCh:
			return
		}
	}
}

// doCompaction merges all current SSTables into one.
// Production LSMs use leveled compaction (L0→L1→L2…); we use a simpler
// "merge everything" strategy that's easier to understand but does more I/O.
func (e *Engine) doCompaction() {
	e.mu.RLock()
	toCompact := make([]*SSTableReader, len(e.sstables))
	copy(toCompact, e.sstables)
	e.mu.RUnlock()

	if len(toCompact) < 2 {
		return
	}

	start := time.Now()
	if e.OnCompaction != nil {
		defer func() { e.OnCompaction(time.Since(start)) }()
	}

	// Read all entries from all SSTables (oldest first, so newer overwrites older)
	// We reverse the slice because sstables is newest-first
	var inputs [][]memEntry
	for i := len(toCompact) - 1; i >= 0; i-- {
		entries, err := toCompact[i].IterateAll()
		if err != nil {
			log.Printf("storage: compaction read error: %v\n", err)
			return
		}
		inputs = append(inputs, entries)
	}

	// Merge all entries, resolve conflicts, drop tombstones
	merged := MergeEntries(inputs)

	// Write merged result as a new SSTable
	seq := atomic.AddUint64(&e.sstableSeq, 1)
	newPath := filepath.Join(e.dir, fmt.Sprintf("%016d.sst", seq))
	writer := NewSSTableWriter(newPath)
	if err := writer.Write(merged); err != nil {
		log.Printf("storage: compaction write error: %v\n", err)
		return
	}

	newReader, err := OpenSSTable(newPath)
	if err != nil {
		log.Printf("storage: compaction open error: %v\n", err)
		return
	}

	// Atomically swap in the new SSTable, remove the old ones
	e.mu.Lock()
	oldPaths := make([]string, len(toCompact))
	for i, sst := range toCompact {
		oldPaths[i] = sst.Path()
	}
	e.sstables = []*SSTableReader{newReader}
	e.mu.Unlock()

	// Delete old SSTable files (after the lock so readers can finish)
	for _, p := range oldPaths {
		os.Remove(p)
	}

	log.Printf("storage: compacted %d SSTables → 1 (%d entries)\n",
		len(toCompact), len(merged))
}

// ── Recovery ─────────────────────────────────────────────────────────────────

// loadSSTables scans the data directory and loads all existing SSTable files.
// Called during Open() to recover state after a restart.
func (e *Engine) loadSSTables() error {
	pattern := filepath.Join(e.dir, "*.sst")
	paths, err := filepath.Glob(pattern)
	if err != nil {
		return err
	}

	// Sort paths - they're named by sequence number so lexicographic = chronological
	sort.Sort(sort.Reverse(sort.StringSlice(paths))) // newest first

	e.sstables = make([]*SSTableReader, 0, len(paths))
	for _, p := range paths {
		r, err := OpenSSTable(p)
		if err != nil {
			log.Printf("storage: skipping corrupt SSTable %s: %v\n", p, err)
			continue
		}
		e.sstables = append(e.sstables, r)
	}

	// Set sequence counter past the highest existing SSTable
	if len(paths) > 0 {
		// Extract seq from last (highest) path name
		var seq uint64
		if _, err := fmt.Sscanf(filepath.Base(paths[0]), "%d.sst", &seq); err != nil {
			return fmt.Errorf("storage: parse sstable sequence from %s: %w", paths[0], err)
		}
		atomic.StoreUint64(&e.sstableSeq, seq)
	}

	return nil
}
