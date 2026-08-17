// SSTable (Sorted String Table) - an immutable, sorted on-disk file.
//
// On-disk layout:
//
//   ┌─────────────────────────────────────────────────────────┐
//   │  DATA BLOCK                                              │
//   │  [key_len:4][key:N][val_len:4][val:M][kind:1] × entries │
//   ├─────────────────────────────────────────────────────────┤
//   │  BLOOM FILTER BLOCK                                      │
//   │  [bitset:bloomSize bytes]                                │
//   ├─────────────────────────────────────────────────────────┤
//   │  FOOTER (fixed 24 bytes)                                 │
//   │  [bloom_offset:8][bloom_size:8][entry_count:4][magic:4]  │
//   └─────────────────────────────────────────────────────────┘
//
// Bloom filter: a probabilistic set membership test.
// Before doing an expensive disk read, we check "is this key possibly in
// this SSTable?" The bloom filter answers in O(1) with no false negatives.
// False positives are possible (we'd do an unnecessary disk read) but rare.
// This is the same technique used by RocksDB, Cassandra, and BigTable.

package storage

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"hash/fnv"
	"io"
	"os"
	"sort"
)

const (
	sstMagic    = uint32(0xDEADB0B5)
	bloomSize   = 1024 // bytes = 8192 bits; good for ~5000 keys at 1% FPR
	bloomHashes = 7    // number of hash functions - optimal for our bloom size
)

// SSTableWriter builds a new SSTable file from a sorted list of entries.
type SSTableWriter struct {
	path string
}

// NewSSTableWriter creates a writer that will write to path.
func NewSSTableWriter(path string) *SSTableWriter {
	return &SSTableWriter{path: path}
}

// Write flushes a sorted snapshot of memtable entries to disk.
// entries MUST be sorted by key (Memtable.Snapshot() guarantees this).
func (w *SSTableWriter) Write(entries []memEntry) error {
	f, err := os.Create(w.path)
	if err != nil {
		return fmt.Errorf("sstable create %s: %w", w.path, err)
	}
	defer f.Close()

	buf := bufio.NewWriter(f)
	bloom := newBloom()

	// ── Data block ─────────────────────────────────────────────────────────
	for _, e := range entries {
		bloom.add(e.key)

		// key length + key bytes
		if err := binary.Write(buf, binary.LittleEndian, uint32(len(e.key))); err != nil {
			return err
		}
		if _, err := buf.WriteString(e.key); err != nil {
			return err
		}

		// value length + value bytes (0 length for tombstones)
		if err := binary.Write(buf, binary.LittleEndian, uint32(len(e.value))); err != nil {
			return err
		}
		if len(e.value) > 0 {
			if _, err := buf.Write(e.value); err != nil {
				return err
			}
		}

		// kind byte (put=0, delete=1)
		if err := buf.WriteByte(byte(e.kind)); err != nil {
			return err
		}
	}

	// Track actual bloom offset by flushing and checking file position
	if err := buf.Flush(); err != nil {
		return fmt.Errorf("sstable flush data block: %w", err)
	}
	bloomOffset, err := f.Seek(0, io.SeekCurrent)
	if err != nil {
		return fmt.Errorf("sstable seek: %w", err)
	}

	// ── Bloom filter block ─────────────────────────────────────────────────
	if _, err := f.Write(bloom.bits[:]); err != nil {
		return fmt.Errorf("sstable write bloom block: %w", err)
	}

	// ── Footer ────────────────────────────────────────────────────────────
	var footer [24]byte
	binary.LittleEndian.PutUint64(footer[0:8], uint64(bloomOffset))
	binary.LittleEndian.PutUint64(footer[8:16], uint64(bloomSize))
	binary.LittleEndian.PutUint32(footer[16:20], uint32(len(entries)))
	binary.LittleEndian.PutUint32(footer[20:24], sstMagic)
	if _, err := f.Write(footer[:]); err != nil {
		return fmt.Errorf("sstable write footer: %w", err)
	}

	return f.Sync()
}

// SSTableReader reads an SSTable file.
// It loads the bloom filter into memory; data blocks are read on demand.
type SSTableReader struct {
	path       string
	bloom      *bloomFilter
	entryCount uint32
}

// OpenSSTable opens an SSTable for reading and loads its bloom filter.
func OpenSSTable(path string) (*SSTableReader, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	// Read footer
	info, _ := f.Stat()
	if info.Size() < 24 {
		return nil, fmt.Errorf("sstable %s: file too small", path)
	}

	if _, err := f.Seek(-24, io.SeekEnd); err != nil {
		return nil, fmt.Errorf("sstable %s: seek footer: %w", path, err)
	}
	var footer [24]byte
	if _, err := io.ReadFull(f, footer[:]); err != nil {
		return nil, err
	}

	magic := binary.LittleEndian.Uint32(footer[20:24])
	if magic != sstMagic {
		return nil, fmt.Errorf("sstable %s: bad magic %x", path, magic)
	}

	bloomOffset := binary.LittleEndian.Uint64(footer[0:8])
	entryCount := binary.LittleEndian.Uint32(footer[16:20])

	// Load bloom filter
	bloom := &bloomFilter{}
	if _, err := f.Seek(int64(bloomOffset), io.SeekStart); err != nil {
		return nil, fmt.Errorf("sstable %s: seek bloom block: %w", path, err)
	}
	if _, err := io.ReadFull(f, bloom.bits[:]); err != nil {
		return nil, fmt.Errorf("sstable %s: read bloom block: %w", path, err)
	}

	return &SSTableReader{
		path:       path,
		bloom:      bloom,
		entryCount: entryCount,
	}, nil
}

// Get looks up a key in this SSTable.
// Returns (value, kindPut, true) if found, or (nil, *, false) if not.
//
// Performance path:
//  1. Check bloom filter - O(1), avoids disk read if key definitely absent
//  2. Scan data block - O(n) linear scan
//     (production: binary search on a separately stored index block)
func (r *SSTableReader) Get(key string) ([]byte, entryKind, bool) {
	// Bloom filter check - fast path
	if !r.bloom.mightContain(key) {
		return nil, 0, false // definitely not in this file
	}

	f, err := os.Open(r.path)
	if err != nil {
		return nil, 0, false
	}
	defer f.Close()

	reader := bufio.NewReader(f)

	for {
		// Read key length
		var keyLen uint32
		if err := binary.Read(reader, binary.LittleEndian, &keyLen); err != nil {
			break // EOF or bloom false positive
		}

		// Read key
		keyBytes := make([]byte, keyLen)
		if _, err := io.ReadFull(reader, keyBytes); err != nil {
			break
		}
		k := string(keyBytes)

		// Read value
		var valLen uint32
		if err := binary.Read(reader, binary.LittleEndian, &valLen); err != nil {
			break // EOF or bloom false positive
		}
		valBytes := make([]byte, valLen)
		if valLen > 0 {
			if _, err := io.ReadFull(reader, valBytes); err != nil {
				break
			}
		}

		// Read kind
		kindByte, err := reader.ReadByte()
		if err != nil {
			break
		}
		kind := entryKind(kindByte)

		if k == key {
			return valBytes, kind, true
		}

		// Since keys are sorted, if we've passed our target key, stop
		if k > key {
			break
		}
	}

	return nil, 0, false // bloom false positive - key not actually here
}

// IterateAll scans every entry in the SSTable in key order.
// Used by compaction to merge multiple SSTables.
func (r *SSTableReader) IterateAll() ([]memEntry, error) {
	f, err := os.Open(r.path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	// Read footer to find where data block ends (at bloom offset)
	if _, err := f.Seek(-24, io.SeekEnd); err != nil {
		return nil, fmt.Errorf("sstable %s: seek footer: %w", r.path, err)
	}
	var footer [24]byte
	if _, err := io.ReadFull(f, footer[:]); err != nil {
		return nil, fmt.Errorf("sstable %s: read footer: %w", r.path, err)
	}
	bloomOffset := binary.LittleEndian.Uint64(footer[0:8])

	if _, err := f.Seek(0, io.SeekStart); err != nil {
		return nil, fmt.Errorf("sstable %s: seek start: %w", r.path, err)
	}
	reader := bufio.NewReader(f)
	var entries []memEntry
	var pos int64

	for pos < int64(bloomOffset) {
		var keyLen uint32
		if err := binary.Read(reader, binary.LittleEndian, &keyLen); err != nil {
			break
		}
		pos += 4

		keyBytes := make([]byte, keyLen)
		if _, err := io.ReadFull(reader, keyBytes); err != nil {
			break
		}
		pos += int64(keyLen)

		var valLen uint32
		if err := binary.Read(reader, binary.LittleEndian, &valLen); err != nil {
			break
		}
		pos += 4

		valBytes := make([]byte, valLen)
		if valLen > 0 {
			if _, err := io.ReadFull(reader, valBytes); err != nil {
				break
			}
			pos += int64(valLen)
		}

		kindByte, err := reader.ReadByte()
		if err != nil {
			break
		}
		pos += 1

		entries = append(entries, memEntry{
			key:   string(keyBytes),
			value: valBytes,
			kind:  entryKind(kindByte),
		})
	}

	return entries, nil
}

// Path returns the file path of this SSTable.
func (r *SSTableReader) Path() string { return r.path }

// EntryCount returns the number of entries in this SSTable.
func (r *SSTableReader) EntryCount() uint32 { return r.entryCount }

// ── Bloom filter ────────────────────────────────────────────────────────────
//
// A bloom filter is a bit array. To add a key, we hash it K times,
// each hash sets one bit. To check membership, we hash K times and check
// all K bits are set. If any bit is 0, the key is definitely absent.
// If all K bits are 1, the key is *probably* present (might be a false positive).
//
// False positive rate ≈ (1 - e^(-kn/m))^k  where k=hashes, n=keys, m=bits
// With our parameters: ~1% FPR at 5000 keys.

type bloomFilter struct {
	bits [bloomSize]byte
}

func newBloom() *bloomFilter { return &bloomFilter{} }

func (b *bloomFilter) add(key string) {
	for i := 0; i < bloomHashes; i++ {
		b.bits[b.hash(key, uint32(i)) % bloomSize] |= 1
	}
}

func (b *bloomFilter) mightContain(key string) bool {
	for i := 0; i < bloomHashes; i++ {
		if b.bits[b.hash(key, uint32(i)) % bloomSize] == 0 {
			return false
		}
	}
	return true
}

// hash produces a position in [0, bloomSize) using FNV with a seed.
// We simulate K independent hash functions by mixing in the seed.
func (b *bloomFilter) hash(key string, seed uint32) uint32 {
	h := fnv.New32a()
	h.Write([]byte{byte(seed >> 24), byte(seed >> 16), byte(seed >> 8), byte(seed)})
	h.Write([]byte(key))
	return h.Sum32()
}

// ── Merge helper (used by compaction) ───────────────────────────────────────

// MergeEntries merges multiple sorted entry slices into one sorted slice.
// Later entries (higher index in the inputs slice) take precedence for the
// same key - so pass newer SSTables last.
//
// This is a k-way merge, like the merge step of merge sort.
func MergeEntries(inputs [][]memEntry) []memEntry {
	// Track current position in each input
	positions := make([]int, len(inputs))

	var result []memEntry

	for {
		// Find the smallest key across all inputs
		minKey := ""
		minInput := -1

		for i, pos := range positions {
			if pos >= len(inputs[i]) {
				continue
			}
			key := inputs[i][pos].key
			if minInput == -1 || key < minKey {
				minKey = key
				minInput = i
			}
		}

		if minInput == -1 {
			break // all inputs exhausted
		}

		// Among all inputs that have this key, take the newest (highest input index)
		newestEntry := inputs[minInput][positions[minInput]]
		for i := minInput + 1; i < len(inputs); i++ {
			if positions[i] < len(inputs[i]) && inputs[i][positions[i]].key == minKey {
				newestEntry = inputs[i][positions[i]]
			}
		}

		// Advance all inputs that had this key
		for i := range inputs {
			if positions[i] < len(inputs[i]) && inputs[i][positions[i]].key == minKey {
				positions[i]++
			}
		}

		// Drop tombstones at the bottom level (no older data can exist below)
		// In a real LSM you'd only drop at the last level; we always drop here for simplicity
		if newestEntry.kind == kindDelete {
			continue
		}

		result = append(result, newestEntry)
	}

	// Ensure result is sorted (it should be from the merge, but sort for safety)
	sort.Slice(result, func(i, j int) bool {
		return result[i].key < result[j].key
	})

	return result
}
