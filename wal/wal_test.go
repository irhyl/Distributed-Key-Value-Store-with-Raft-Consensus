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

func TestWALTruncatePrefix(t *testing.T) {
	dir, _ := os.MkdirTemp("", "wal-truncate-prefix-*")
	defer os.RemoveAll(dir)

	w, _ := Open(dir)
	for i := 1; i <= 50; i++ {
		w.Append(makeEntry(uint64(i), 1, "k", "v"))
	}

	if err := w.TruncatePrefix(30); err != nil {
		t.Fatalf("truncate prefix: %v", err)
	}

	entries, _ := w.ReadAll()
	if len(entries) != 20 {
		t.Fatalf("after truncate prefix: got %d entries, want 20", len(entries))
	}
	if entries[0].Index != 31 {
		t.Fatalf("first entry index: got %d want 31", entries[0].Index)
	}
	if entries[len(entries)-1].Index != 50 {
		t.Fatalf("last entry index: got %d want 50", entries[len(entries)-1].Index)
	}
	if w.LastIndex() != 50 {
		t.Fatalf("lastIndex: got %d want 50", w.LastIndex())
	}

	// New entries continue the sequence from where the prefix was cut, not from 1.
	if err := w.Append(makeEntry(51, 1, "k", "v")); err != nil {
		t.Fatalf("append after truncate: %v", err)
	}
	w.Close()

	// Discarding everything (prefix covers the whole log) must still leave
	// lastIndex reflecting the log's logical continuation point, not 0.
	w2, _ := Open(dir)
	defer w2.Close()
	if err := w2.TruncatePrefix(51); err != nil {
		t.Fatalf("truncate prefix (all): %v", err)
	}
	entries, _ = w2.ReadAll()
	if len(entries) != 0 {
		t.Fatalf("after full truncate: got %d entries, want 0", len(entries))
	}
	if w2.LastIndex() != 51 {
		t.Fatalf("lastIndex after full truncate: got %d want 51", w2.LastIndex())
	}
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

// BenchmarkAppend measures single-entry append throughput (one fsync per
// entry). Measured at ~6.9k ops/sec on an NVMe SSD (13th Gen Intel Core
// i5-13500H) - see the README's benchmarks table for the full picture,
// including how much batching (AppendBatch) buys over this.
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

// BenchmarkAppendBatch measures throughput when appending in batches of 50
// (one fsync per batch, not per entry) - quantifies the "Batch fsync"
// design decision in the README rather than just asserting it.
func BenchmarkAppendBatch(b *testing.B) {
	dir, _ := os.MkdirTemp("", "wal-bench-batch-*")
	defer os.RemoveAll(dir)
	w, _ := Open(dir)
	defer w.Close()

	const batchSize = 50
	batch := make([]*pb.LogEntry, batchSize)
	for i := range batch {
		batch[i] = makeEntry(0, 1, "benchkey", "benchvalue")
	}

	b.ResetTimer()
	idx := uint64(1)
	for i := 0; i < b.N; i++ {
		for j := range batch {
			batch[j].Index = idx
			idx++
		}
		if err := w.AppendBatch(batch); err != nil {
			b.Fatal(err)
		}
	}
	b.ReportMetric(float64(batchSize), "entries/batch")
}
