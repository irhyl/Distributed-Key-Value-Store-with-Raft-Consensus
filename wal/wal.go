// Package wal implements a Write-Ahead Log for crash-safe persistence.
//
// How it works:
//   Before any state change is applied, it's first written here.
//   If the process crashes mid-operation, on restart we replay
//   the WAL to recover to a consistent state.
//
// On-disk format for each record:
//   [4 bytes: payload length] [4 bytes: CRC32 checksum] [N bytes: payload]
//
// The checksum lets us detect truncated or corrupted writes —
// a common failure mode when a machine loses power mid-write.

package wal

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sync"

	"google.golang.org/protobuf/proto"
	pb "github.com/raftkv/proto"
)

const (
	// Each record header: 4 bytes length + 4 bytes CRC32
	headerSize = 8

	// How large a single WAL segment file can grow before we roll over.
	// In production (etcd uses 64MB). We use 8MB for easier testing.
	maxSegmentSize = 8 * 1024 * 1024
)

// WAL is a write-ahead log that persists Raft log entries to disk.
// It's safe for concurrent use — a single writer, multiple readers pattern.
type WAL struct {
	mu      sync.Mutex
	dir     string       // directory holding segment files
	current *os.File     // currently active segment file
	writer  *bufio.Writer
	size    int64        // bytes written to current segment
	lastIndex uint64     // highest log index written to WAL
}

// Open opens or creates a WAL in the given directory.
// On first open, a new segment file is created.
// On subsequent opens (after crash), we resume writing to the last segment.
func Open(dir string) (*WAL, error) {
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, fmt.Errorf("wal: mkdir %s: %w", dir, err)
	}

	w := &WAL{dir: dir}

	// Find existing segment files (named 0000000000000001.wal, etc.)
	segments, err := filepath.Glob(filepath.Join(dir, "*.wal"))
	if err != nil {
		return nil, err
	}

	if len(segments) == 0 {
		// Fresh start — create the first segment
		if err := w.createSegment(1); err != nil {
			return nil, err
		}
	} else {
		// Re-open the last segment for appending.
		// In a real implementation you'd verify the last segment isn't
		// corrupted and truncate any partial tail record.
		last := segments[len(segments)-1]
		f, err := os.OpenFile(last, os.O_RDWR|os.O_APPEND, 0644)
		if err != nil {
			return nil, fmt.Errorf("wal: open segment %s: %w", last, err)
		}
		info, _ := f.Stat()
		w.current = f
		w.writer = bufio.NewWriter(f)
		w.size = info.Size()

		// Scan through to find lastIndex
		if err := w.scanLastIndex(); err != nil {
			return nil, err
		}
	}

	return w, nil
}

// Append writes a Raft log entry to the WAL.
// This MUST be called before the entry is applied to the state machine.
// The write is fsynced so it survives a crash.
func (w *WAL) Append(entry *pb.LogEntry) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	// Serialize the protobuf entry
	data, err := proto.Marshal(entry)
	if err != nil {
		return fmt.Errorf("wal: marshal entry: %w", err)
	}

	// Roll over to a new segment if we'd exceed the size limit
	if w.size+int64(headerSize+len(data)) > maxSegmentSize {
		if err := w.rollSegment(entry.Index); err != nil {
			return err
		}
	}

	if err := w.writeRecord(data); err != nil {
		return err
	}

	// fsync so this entry survives a crash before the caller applies it
	if err := w.writer.Flush(); err != nil {
		return fmt.Errorf("wal: flush: %w", err)
	}
	if err := w.current.Sync(); err != nil {
		return fmt.Errorf("wal: sync: %w", err)
	}

	w.lastIndex = entry.Index
	return nil
}

// AppendBatch writes multiple entries atomically (single fsync).
// This is a critical optimization — Raft can replicate many entries at once,
// and fsyncing each one separately would be prohibitively slow.
func (w *WAL) AppendBatch(entries []*pb.LogEntry) error {
	if len(entries) == 0 {
		return nil
	}

	w.mu.Lock()
	defer w.mu.Unlock()

	for _, entry := range entries {
		data, err := proto.Marshal(entry)
		if err != nil {
			return fmt.Errorf("wal: marshal entry %d: %w", entry.Index, err)
		}
		// Roll segment if this entry would push us over the size limit
		if w.size+int64(headerSize+len(data)) > maxSegmentSize {
			if err := w.rollSegment(entry.Index); err != nil {
				return err
			}
		}
		if err := w.writeRecord(data); err != nil {
			return err
		}
		w.lastIndex = entry.Index
	}

	// Single fsync for the whole batch — huge throughput improvement
	if err := w.writer.Flush(); err != nil {
		return fmt.Errorf("wal: flush batch: %w", err)
	}
	return w.current.Sync()
}

// ReadAll replays the entire WAL from the beginning.
// Called during startup to reconstruct in-memory state.
func (w *WAL) ReadAll() ([]*pb.LogEntry, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	segments, err := filepath.Glob(filepath.Join(w.dir, "*.wal"))
	if err != nil {
		return nil, err
	}

	var entries []*pb.LogEntry
	for _, seg := range segments {
		segEntries, err := readSegment(seg)
		if err != nil {
			return nil, fmt.Errorf("wal: read segment %s: %w", seg, err)
		}
		entries = append(entries, segEntries...)
	}

	return entries, nil
}

// TruncateSuffix removes all entries with index > keepIndex.
// This is called when a Raft follower discovers its log conflicts
// with the leader's log and needs to roll back.
func (w *WAL) TruncateSuffix(keepIndex uint64) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	// Flush and close the current segment before reading back or deleting.
	// On Windows an open file cannot be deleted; closing first ensures Remove succeeds.
	if w.writer != nil {
		w.writer.Flush()
	}
	if w.current != nil {
		w.current.Close()
		w.current = nil
		w.writer = nil
	}

	// Read all entries, rewrite only those we want to keep
	segments, _ := filepath.Glob(filepath.Join(w.dir, "*.wal"))
	var keep []*pb.LogEntry
	for _, seg := range segments {
		entries, err := readSegment(seg)
		if err != nil {
			return err
		}
		for _, e := range entries {
			if e.Index <= keepIndex {
				keep = append(keep, e)
			}
		}
	}

	// Remove all segment files and rewrite from scratch
	for _, seg := range segments {
		if err := os.Remove(seg); err != nil {
			return fmt.Errorf("wal: remove segment %s: %w", seg, err)
		}
	}

	w.current = nil
	w.size = 0
	w.lastIndex = 0

	if err := w.createSegment(1); err != nil {
		return err
	}

	for _, e := range keep {
		data, err := proto.Marshal(e)
		if err != nil {
			return fmt.Errorf("wal: truncate marshal entry %d: %w", e.Index, err)
		}
		if err := w.writeRecord(data); err != nil {
			return fmt.Errorf("wal: truncate rewrite entry %d: %w", e.Index, err)
		}
		w.lastIndex = e.Index
	}

	w.writer.Flush()
	return w.current.Sync()
}

// LastIndex returns the index of the most recent entry in the WAL.
func (w *WAL) LastIndex() uint64 {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.lastIndex
}

// Close flushes and closes the WAL.
func (w *WAL) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.writer != nil {
		w.writer.Flush()
	}
	if w.current != nil {
		return w.current.Close()
	}
	return nil
}

// ── Internal helpers ────────────────────────────────────────────────────────

// writeRecord writes a single [length][crc32][data] record.
// Does NOT fsync — call Sync() or let AppendBatch do it.
func (w *WAL) writeRecord(data []byte) error {
	checksum := crc32.ChecksumIEEE(data)

	var header [headerSize]byte
	binary.LittleEndian.PutUint32(header[0:4], uint32(len(data)))
	binary.LittleEndian.PutUint32(header[4:8], checksum)

	if _, err := w.writer.Write(header[:]); err != nil {
		return fmt.Errorf("wal: write header: %w", err)
	}
	if _, err := w.writer.Write(data); err != nil {
		return fmt.Errorf("wal: write data: %w", err)
	}

	w.size += int64(headerSize + len(data))
	return nil
}

// createSegment opens a new segment file.
// Segment files are named by their first log index: 0000000000000001.wal
func (w *WAL) createSegment(firstIndex uint64) error {
	name := filepath.Join(w.dir, fmt.Sprintf("%016d.wal", firstIndex))
	f, err := os.OpenFile(name, os.O_CREATE|os.O_WRONLY|os.O_EXCL, 0644)
	if err != nil {
		return fmt.Errorf("wal: create segment: %w", err)
	}
	if w.current != nil {
		w.writer.Flush()
		w.current.Close()
	}
	w.current = f
	w.writer = bufio.NewWriter(f)
	w.size = 0
	return nil
}

// rollSegment closes the current segment and starts a new one.
func (w *WAL) rollSegment(nextIndex uint64) error {
	w.writer.Flush()
	w.current.Sync()
	return w.createSegment(nextIndex)
}

// scanLastIndex reads through the current segment to find the highest index.
func (w *WAL) scanLastIndex() error {
	entries, err := readSegment(w.current.Name())
	if err != nil {
		return err
	}
	if len(entries) > 0 {
		w.lastIndex = entries[len(entries)-1].Index
	}
	return nil
}

// readSegment reads all valid records from a segment file.
// It stops at the first corrupted or truncated record (partial writes from crashes).
func readSegment(path string) ([]*pb.LogEntry, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	reader := bufio.NewReader(f)
	var entries []*pb.LogEntry

	for {
		// Read header
		var header [headerSize]byte
		if _, err := io.ReadFull(reader, header[:]); err != nil {
			if err == io.EOF || err == io.ErrUnexpectedEOF {
				break // clean end or partial write at tail — stop here
			}
			return nil, fmt.Errorf("read header: %w", err)
		}

		length := binary.LittleEndian.Uint32(header[0:4])
		expectedCRC := binary.LittleEndian.Uint32(header[4:8])

		// Read payload
		data := make([]byte, length)
		if _, err := io.ReadFull(reader, data); err != nil {
			// Partial write — this record is corrupt, stop here
			break
		}

		// Verify checksum
		actualCRC := crc32.ChecksumIEEE(data)
		if actualCRC != expectedCRC {
			// Corrupted record — stop; don't return an error,
			// because a corruption at the tail is a known crash scenario
			break
		}

		// Deserialize
		entry := &pb.LogEntry{}
		if err := proto.Unmarshal(data, entry); err != nil {
			return nil, fmt.Errorf("unmarshal entry: %w", err)
		}

		entries = append(entries, entry)
	}

	return entries, nil
}
