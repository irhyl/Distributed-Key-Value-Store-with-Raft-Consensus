package wal

import (
	"os"
	"testing"

	pb "github.com/raftkv/proto"
)

func makeEntry(index, term uint64, key, val string) *pb.LogEntry {
	return &pb.LogEntry{
		Index: index,
		Term:  term,
		Data:  []byte(key + "=" + val),
	}
}

func TestWALWriteAndRead(t *testing.T) {
	dir, _ := os.MkdirTemp("", "wal-test-*")
	defer os.RemoveAll(dir)

	w, err := Open(dir)
	if err != nil {
		t.Fatalf("open: %v", err)
	}

	// Write 100 entries
	for i := 1; i <= 100; i++ {
		e := makeEntry(uint64(i), 1, "key", "value")
		if err := w.Append(e); err != nil {
			t.Fatalf("append %d: %v", i, err)
		}
	}

	if w.LastIndex() != 100 {
		t.Fatalf("lastIndex: got %d want 100", w.LastIndex())
	}
	w.Close()

	// Reopen and verify replay
	w2, err := Open(dir)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer w2.Close()

	entries, err := w2.ReadAll()
	if err != nil {
		t.Fatalf("readall: %v", err)
	}
	if len(entries) != 100 {
		t.Fatalf("got %d entries, want 100", len(entries))
	}
	for i, e := range entries {
		if e.Index != uint64(i+1) {
			t.Fatalf("entry %d: got index %d", i, e.Index)
		}
	}
}

func TestWALTruncate(t *testing.T) {
	dir, _ := os.MkdirTemp("", "wal-truncate-*")
	defer os.RemoveAll(dir)

	w, _ := Open(dir)
	for i := 1; i <= 50; i++ {
		w.Append(makeEntry(uint64(i), 1, "k", "v"))
	}

	if err := w.TruncateSuffix(30); err != nil {
		t.Fatalf("truncate: %v", err)
	}

	entries, _ := w.ReadAll()
	if len(entries) != 30 {
		t.Fatalf("after truncate: got %d entries, want 30", len(entries))
	}
	if entries[len(entries)-1].Index != 30 {
		t.Fatalf("last entry index: got %d want 30", entries[len(entries)-1].Index)
	}
	w.Close()
}

func TestWALBatchAppend(t *testing.T) {
	dir, _ := os.MkdirTemp("", "wal-batch-*")
	defer os.RemoveAll(dir)

	w, _ := Open(dir)
	defer w.Close()

	batch := make([]*pb.LogEntry, 200)
	for i := range batch {
		batch[i] = makeEntry(uint64(i+1), 1, "k", "v")
	}

	if err := w.AppendBatch(batch); err != nil {
		t.Fatalf("batch append: %v", err)
	}

	if w.LastIndex() != 200 {
		t.Fatalf("lastIndex after batch: got %d want 200", w.LastIndex())
	}
}

// BenchmarkAppend measures single-entry append throughput.
// On an SSD you should see ~50k-100k ops/sec with fsync.
func BenchmarkAppend(b *testing.B) {
	dir, _ := os.MkdirTemp("", "wal-bench-*")
	defer os.RemoveAll(dir)
	w, _ := Open(dir)
	defer w.Close()

	entry := makeEntry(1, 1, "benchkey", "benchvalue")
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		entry.Index = uint64(i + 1)
		w.Append(entry)
	}
}
