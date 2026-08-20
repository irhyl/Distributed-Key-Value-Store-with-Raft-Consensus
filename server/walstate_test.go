package server

import (
	"bytes"
	"os"
	"testing"
)

func TestWALStateSnapshotRoundTrip(t *testing.T) {
	dir, _ := os.MkdirTemp("", "walstate-snap-*")
	defer os.RemoveAll(dir)

	ws, err := newWALState(dir)
	if err != nil {
		t.Fatalf("newWALState: %v", err)
	}

	// Fresh node: no snapshot saved yet.
	data, idx, term, err := ws.LoadSnapshot()
	if err != nil {
		t.Fatalf("load snapshot (fresh): %v", err)
	}
	if data != nil || idx != 0 || term != 0 {
		t.Fatalf("fresh node: got data=%v idx=%d term=%d, want nil/0/0", data, idx, term)
	}

	payload := []byte("pretend this is an engine snapshot blob")
	if err := ws.SaveSnapshot(payload, 42, 7); err != nil {
		t.Fatalf("save snapshot: %v", err)
	}

	data, idx, term, err = ws.LoadSnapshot()
	if err != nil {
		t.Fatalf("load snapshot: %v", err)
	}
	if !bytes.Equal(data, payload) {
		t.Fatalf("data: got %q want %q", data, payload)
	}
	if idx != 42 || term != 7 {
		t.Fatalf("got idx=%d term=%d, want idx=42 term=7", idx, term)
	}

	// Saving a newer snapshot must fully replace the old one, not merge it.
	payload2 := []byte("a later, larger snapshot")
	if err := ws.SaveSnapshot(payload2, 100, 9); err != nil {
		t.Fatalf("save snapshot 2: %v", err)
	}
	data, idx, term, err = ws.LoadSnapshot()
	if err != nil {
		t.Fatalf("load snapshot 2: %v", err)
	}
	if !bytes.Equal(data, payload2) || idx != 100 || term != 9 {
		t.Fatalf("after overwrite: got data=%q idx=%d term=%d", data, idx, term)
	}

	// The snapshot must persist across a fresh walState opened on the same dir.
	ws2, err := newWALState(dir)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	data, idx, term, err = ws2.LoadSnapshot()
	if err != nil {
		t.Fatalf("load snapshot after reopen: %v", err)
	}
	if !bytes.Equal(data, payload2) || idx != 100 || term != 9 {
		t.Fatalf("after reopen: got data=%q idx=%d term=%d", data, idx, term)
	}
}
