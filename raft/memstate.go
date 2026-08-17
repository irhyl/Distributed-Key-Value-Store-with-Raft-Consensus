// memstate.go - an in-memory PersistentState implementation for tests.
// Production code uses wal.WAL instead.

package raft

import "sync"

type memState struct {
	mu      sync.Mutex
	term    uint64
	voted   string
	entries []*LogEntry

	snapshot          []byte
	lastIncludedIndex uint64
	lastIncludedTerm  uint64
}

func newMemState() *memState { return &memState{} }

func (m *memState) SaveHardState(term uint64, votedFor string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.term = term
	m.voted = votedFor
	return nil
}

func (m *memState) LoadHardState() (uint64, string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.term, m.voted, nil
}

func (m *memState) AppendEntries(entries []*LogEntry) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.entries = append(m.entries, entries...)
	return nil
}

func (m *memState) LoadEntries() ([]*LogEntry, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]*LogEntry, len(m.entries))
	copy(out, m.entries)
	return out, nil
}

func (m *memState) TruncateSuffix(keepIndex uint64) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	var keep []*LogEntry
	for _, e := range m.entries {
		if e.Index <= keepIndex {
			keep = append(keep, e)
		}
	}
	m.entries = keep
	return nil
}

func (m *memState) SaveSnapshot(data []byte, lastIncludedIndex, lastIncludedTerm uint64) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.snapshot = append([]byte(nil), data...)
	m.lastIncludedIndex = lastIncludedIndex
	m.lastIncludedTerm = lastIncludedTerm
	return nil
}

func (m *memState) LoadSnapshot() ([]byte, uint64, uint64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.snapshot, m.lastIncludedIndex, m.lastIncludedTerm, nil
}

func (m *memState) LoadSnapshotMeta() (uint64, uint64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.lastIncludedIndex, m.lastIncludedTerm, nil
}

func (m *memState) TruncatePrefix(discardIndex uint64) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	var keep []*LogEntry
	for _, e := range m.entries {
		if e.Index > discardIndex {
			keep = append(keep, e)
		}
	}
	m.entries = keep
	return nil
}
