// walstate.go — implements raft.PersistentState using our WAL + a small metadata file.
// This is the bridge between Layer 2 (WAL) and Layer 4 (Raft).
//
// Raft requires two things to survive crashes:
//   1. Hard state: (currentTerm, votedFor) — who we voted for, what term we're in
//   2. Log entries: the replicated command log
//
// We store hard state as a tiny JSON file (it's just two fields, rarely written).
// Log entries go through the WAL we built in Layer 2.

package server

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"

	pb "github.com/raftkv/proto"
	"github.com/raftkv/wal"
)

// hardState is the small metadata file persisted alongside the WAL.
type hardState struct {
	Term     uint64 `json:"term"`
	VotedFor string `json:"voted_for"`
}

// walState implements raft.PersistentState.
type walState struct {
	dir string
	w   *wal.WAL
}

// newWALState opens (or creates) the WAL at dir and returns a walState.
func newWALState(dir string) (*walState, error) {
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, fmt.Errorf("walstate: mkdir: %w", err)
	}
	w, err := wal.Open(dir)
	if err != nil {
		return nil, fmt.Errorf("walstate: open wal: %w", err)
	}
	return &walState{dir: dir, w: w}, nil
}

// SaveHardState persists currentTerm and votedFor atomically.
// We write to a temp file then rename — rename is atomic on POSIX filesystems,
// so we never end up with a half-written hard state file.
func (ws *walState) SaveHardState(term uint64, votedFor string) error {
	hs := hardState{Term: term, VotedFor: votedFor}
	data, err := json.Marshal(hs)
	if err != nil {
		return err
	}

	tmpPath := filepath.Join(ws.dir, "hardstate.tmp")
	finalPath := filepath.Join(ws.dir, "hardstate.json")

	if err := os.WriteFile(tmpPath, data, 0644); err != nil {
		return fmt.Errorf("walstate: write hardstate tmp: %w", err)
	}
	return os.Rename(tmpPath, finalPath) // atomic
}

// LoadHardState reads the persisted term and votedFor.
// Returns zero values if no hard state file exists (fresh start).
func (ws *walState) LoadHardState() (uint64, string, error) {
	path := filepath.Join(ws.dir, "hardstate.json")
	data, err := os.ReadFile(path)
	if os.IsNotExist(err) {
		return 0, "", nil // fresh node
	}
	if err != nil {
		return 0, "", fmt.Errorf("walstate: read hardstate: %w", err)
	}

	var hs hardState
	if err := json.Unmarshal(data, &hs); err != nil {
		return 0, "", fmt.Errorf("walstate: unmarshal hardstate: %w", err)
	}
	return hs.Term, hs.VotedFor, nil
}

// AppendEntries writes new log entries to the WAL.
func (ws *walState) AppendEntries(entries []*pb.LogEntry) error {
	return ws.w.AppendBatch(entries)
}

// LoadEntries replays the WAL to reconstruct the full log.
// Called once during node startup.
func (ws *walState) LoadEntries() ([]*pb.LogEntry, error) {
	return ws.w.ReadAll()
}

// TruncateSuffix removes all log entries with index > keepIndex.
// Called when a follower must roll back conflicting entries.
func (ws *walState) TruncateSuffix(keepIndex uint64) error {
	return ws.w.TruncateSuffix(keepIndex)
}

// SaveSnapshot persists snapshot data plus the Raft index/term it covers,
// atomically (temp file + rename, same pattern as SaveHardState). The index
// and term are packed into the same file as the data itself rather than a
// separate metadata file, so a crash between two atomic renames can't leave
// them pointing at different snapshots.
func (ws *walState) SaveSnapshot(data []byte, lastIncludedIndex, lastIncludedTerm uint64) error {
	buf := make([]byte, 16+len(data))
	binary.LittleEndian.PutUint64(buf[0:8], lastIncludedIndex)
	binary.LittleEndian.PutUint64(buf[8:16], lastIncludedTerm)
	copy(buf[16:], data)

	tmpPath := filepath.Join(ws.dir, "snapshot.tmp")
	finalPath := filepath.Join(ws.dir, "snapshot.bin")

	if err := os.WriteFile(tmpPath, buf, 0644); err != nil {
		return fmt.Errorf("walstate: write snapshot tmp: %w", err)
	}
	return os.Rename(tmpPath, finalPath) // atomic
}

// LoadSnapshot reads the persisted snapshot, if any.
// Returns a nil data slice and lastIncludedIndex 0 if no snapshot has ever
// been saved (fresh node, or one that has never compacted its log).
func (ws *walState) LoadSnapshot() ([]byte, uint64, uint64, error) {
	path := filepath.Join(ws.dir, "snapshot.bin")
	buf, err := os.ReadFile(path)
	if os.IsNotExist(err) {
		return nil, 0, 0, nil
	}
	if err != nil {
		return nil, 0, 0, fmt.Errorf("walstate: read snapshot: %w", err)
	}
	if len(buf) < 16 {
		return nil, 0, 0, fmt.Errorf("walstate: snapshot file truncated (%d bytes)", len(buf))
	}
	lastIncludedIndex := binary.LittleEndian.Uint64(buf[0:8])
	lastIncludedTerm := binary.LittleEndian.Uint64(buf[8:16])
	data := buf[16:]
	return data, lastIncludedIndex, lastIncludedTerm, nil
}

// TruncatePrefix removes all log entries with index <= discardIndex.
// Called after a snapshot covering them has been durably saved.
func (ws *walState) TruncatePrefix(discardIndex uint64) error {
	return ws.w.TruncatePrefix(discardIndex)
}
