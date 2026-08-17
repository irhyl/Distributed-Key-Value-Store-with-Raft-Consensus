package storage

import (
	"fmt"
	"os"
	"testing"
	"time"
)

func TestMemtableBasic(t *testing.T) {
	m := NewMemtable()

	m.Put("apple", []byte("red"))
	m.Put("banana", []byte("yellow"))
	m.Put("cherry", []byte("red"))

	val, ok := m.Get("banana")
	if !ok || string(val) != "yellow" {
		t.Fatalf("get banana: got %q %v", val, ok)
	}

	// Overwrite
	m.Put("banana", []byte("green"))
	val, ok = m.Get("banana")
	if !ok || string(val) != "green" {
		t.Fatalf("overwrite banana: got %q", val)
	}

	// Delete
	m.Delete("apple")
	_, ok = m.Get("apple")
	if ok {
		t.Fatal("deleted apple should not be found")
	}

	// Missing key
	_, ok = m.Get("mango")
	if ok {
		t.Fatal("mango should not exist")
	}
}

func TestMemtableSortOrder(t *testing.T) {
	m := NewMemtable()
	keys := []string{"zebra", "apple", "mango", "cherry", "banana"}
	for _, k := range keys {
		m.Put(k, []byte(k))
	}

	snap := m.Snapshot()
	for i := 1; i < len(snap); i++ {
		if snap[i].key <= snap[i-1].key {
			t.Fatalf("snapshot not sorted: %q before %q", snap[i-1].key, snap[i].key)
		}
	}
}

func TestSSTableWriteRead(t *testing.T) {
	dir, _ := os.MkdirTemp("", "sst-*")
	defer os.RemoveAll(dir)

	path := dir + "/test.sst"

	// Write a few entries
	entries := []memEntry{
		{key: "aardvark", value: []byte("small"), kind: kindPut},
		{key: "bear", value: []byte("large"), kind: kindPut},
		{key: "cat", value: nil, kind: kindDelete},
		{key: "dog", value: []byte("loyal"), kind: kindPut},
	}

	w := NewSSTableWriter(path)
	if err := w.Write(entries); err != nil {
		t.Fatalf("write: %v", err)
	}

	r, err := OpenSSTable(path)
	if err != nil {
		t.Fatalf("open: %v", err)
	}

	// Read existing key
	val, kind, found := r.Get("bear")
	if !found || kind != kindPut || string(val) != "large" {
		t.Fatalf("get bear: found=%v kind=%v val=%q", found, kind, val)
	}

	// Read tombstone
	_, kind, found = r.Get("cat")
	if !found || kind != kindDelete {
		t.Fatalf("get cat tombstone: found=%v kind=%v", found, kind)
	}

	// Read absent key
	_, _, found = r.Get("elephant")
	if found {
		t.Fatal("elephant should not be found")
	}
}

func TestBloomFilter(t *testing.T) {
	b := newBloom()

	keys := []string{"foo", "bar", "baz", "qux", "hello"}
	for _, k := range keys {
		b.add(k)
	}

	// All added keys must be found
	for _, k := range keys {
		if !b.mightContain(k) {
			t.Fatalf("bloom: false negative for key %q", k)
		}
	}

	// Check that most non-added keys return false
	// (some false positives are expected)
	falsePositives := 0
	testKeys := []string{"xyz", "abc", "nothere", "missing", "absent",
		"nope", "no", "never", "nein", "nyet"}
	for _, k := range testKeys {
		if b.mightContain(k) {
			falsePositives++
		}
	}
	if falsePositives > 3 { // allow up to 30% FPR for this tiny test
		t.Fatalf("too many false positives: %d/10", falsePositives)
	}
}

func TestEngineBasic(t *testing.T) {
	dir, _ := os.MkdirTemp("", "engine-*")
	defer os.RemoveAll(dir)

	eng, err := Open(dir)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer eng.Close()

	// Write and read back
	eng.Put("name", []byte("raftkv"))
	eng.Put("version", []byte("1.0"))

	val, ok := eng.Get("name")
	if !ok || string(val) != "raftkv" {
		t.Fatalf("get name: %q %v", val, ok)
	}

	// Overwrite
	eng.Put("name", []byte("distrakv"))
	val, ok = eng.Get("name")
	if !ok || string(val) != "distrakv" {
		t.Fatalf("overwrite name: %q", val)
	}

	// Delete
	eng.Delete("version")
	_, ok = eng.Get("version")
	if ok {
		t.Fatal("deleted version should not be found")
	}
}

func TestEngineRestart(t *testing.T) {
	dir, _ := os.MkdirTemp("", "engine-restart-*")
	defer os.RemoveAll(dir)

	// Write data and force flush to disk
	eng, _ := Open(dir)
	for i := 0; i < 10; i++ {
		eng.Put(fmt.Sprintf("key%02d", i), []byte(fmt.Sprintf("val%02d", i)))
	}
	// Force flush by closing (Close() flushes the active memtable)
	eng.Close()

	// Reopen and verify data survives restart
	eng2, err := Open(dir)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer eng2.Close()

	for i := 0; i < 10; i++ {
		key := fmt.Sprintf("key%02d", i)
		expected := fmt.Sprintf("val%02d", i)
		val, ok := eng2.Get(key)
		if !ok || string(val) != expected {
			t.Fatalf("after restart: get %s = %q %v, want %q", key, val, ok, expected)
		}
	}
}

func TestEngineCompaction(t *testing.T) {
	dir, _ := os.MkdirTemp("", "engine-compact-*")
	defer os.RemoveAll(dir)

	eng, _ := Open(dir)
	defer eng.Close()

	// Write enough data to trigger multiple flushes
	// Each memtable is 4MB; we write many small entries to fill it
	for i := 0; i < 50000; i++ {
		eng.Put(fmt.Sprintf("compkey-%08d", i), []byte(fmt.Sprintf("value-%08d", i)))
	}

	// Wait for background flush/compaction
	time.Sleep(500 * time.Millisecond)
	select {
	case eng.compactCh <- struct{}{}:
	default:
	}
	time.Sleep(500 * time.Millisecond)

	// After compaction, all data should still be readable
	for i := 0; i < 100; i++ { // spot check 100
		key := fmt.Sprintf("compkey-%08d", i*500)
		expected := fmt.Sprintf("value-%08d", i*500)
		val, ok := eng.Get(key)
		if !ok || string(val) != expected {
			t.Fatalf("post-compaction get %s = %q %v, want %q", key, val, ok, expected)
		}
	}
}

func TestEngineSnapshotRoundTrip(t *testing.T) {
	srcDir, _ := os.MkdirTemp("", "engine-snap-src-*")
	defer os.RemoveAll(srcDir)

	src, _ := Open(srcDir)
	defer src.Close()

	// Enough entries to force at least one background flush to an SSTable,
	// so the snapshot must merge disk-resident data, not just the memtable.
	for i := 0; i < 200; i++ {
		src.Put(fmt.Sprintf("snapkey-%03d", i), []byte(fmt.Sprintf("val-%03d", i)))
	}
	time.Sleep(100 * time.Millisecond) // let a background flush happen if triggered

	// A few more writes stay in the active memtable only.
	src.Put("fresh", []byte("still-in-memtable"))

	// Overwrite and delete should both be reflected correctly in the snapshot.
	src.Put("snapkey-000", []byte("overwritten"))
	src.Delete("snapkey-001")

	snap, err := src.Snapshot()
	if err != nil {
		t.Fatalf("snapshot: %v", err)
	}
	if len(snap) == 0 {
		t.Fatal("snapshot: empty output")
	}

	dstDir, _ := os.MkdirTemp("", "engine-snap-dst-*")
	defer os.RemoveAll(dstDir)

	dst, err := Open(dstDir)
	if err != nil {
		t.Fatalf("open dst: %v", err)
	}
	defer dst.Close()

	if err := dst.LoadSnapshot(snap); err != nil {
		t.Fatalf("load snapshot: %v", err)
	}

	for i := 0; i < 200; i++ {
		key := fmt.Sprintf("snapkey-%03d", i)
		switch i {
		case 0:
			val, ok := dst.Get(key)
			if !ok || string(val) != "overwritten" {
				t.Fatalf("get %s = %q %v, want %q", key, val, ok, "overwritten")
			}
		case 1:
			if _, ok := dst.Get(key); ok {
				t.Fatalf("get %s: want deleted, found a value", key)
			}
		default:
			expected := fmt.Sprintf("val-%03d", i)
			val, ok := dst.Get(key)
			if !ok || string(val) != expected {
				t.Fatalf("get %s = %q %v, want %q", key, val, ok, expected)
			}
		}
	}

	val, ok := dst.Get("fresh")
	if !ok || string(val) != "still-in-memtable" {
		t.Fatalf("get fresh = %q %v, want %q", val, ok, "still-in-memtable")
	}

	// The loaded snapshot must be durable - it should survive a restart of
	// the destination engine without replaying anything from src.
	dst.Close()
	dst2, err := Open(dstDir)
	if err != nil {
		t.Fatalf("reopen dst: %v", err)
	}
	defer dst2.Close()

	val, ok = dst2.Get("snapkey-002")
	if !ok || string(val) != "val-002" {
		t.Fatalf("after restart: get snapkey-002 = %q %v", val, ok)
	}
}

func BenchmarkEnginePut(b *testing.B) {
	dir, _ := os.MkdirTemp("", "engine-bench-*")
	defer os.RemoveAll(dir)
	eng, _ := Open(dir)
	defer eng.Close()

	val := make([]byte, 64)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		eng.Put(fmt.Sprintf("bench-%d", i), val)
	}
}

func BenchmarkEngineGet(b *testing.B) {
	dir, _ := os.MkdirTemp("", "engine-bench-get-*")
	defer os.RemoveAll(dir)
	eng, _ := Open(dir)
	defer eng.Close()

	// Pre-populate
	for i := 0; i < 10000; i++ {
		eng.Put(fmt.Sprintf("bench-%d", i), []byte("value"))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		eng.Get(fmt.Sprintf("bench-%d", i%10000))
	}
}
