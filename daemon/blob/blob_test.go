package blob

import (
	"bytes"
	"strings"
	"testing"
)

func newTestStore(t *testing.T) *Store {
	t.Helper()
	s, err := New(t.TempDir())
	if err != nil {
		t.Fatalf("blob.New: %v", err)
	}
	return s
}

func TestPut_Small_Inline(t *testing.T) {
	s := newTestStore(t)
	data := []byte("hello p2p-a2a")
	a, err := s.Put(data, "hello.txt", "text/plain")
	if err != nil {
		t.Fatalf("Put: %v", err)
	}
	if !strings.HasPrefix(a.Cid, "sha256:") {
		t.Errorf("CID missing sha256 prefix: %q", a.Cid)
	}
	if len(a.Inline) == 0 {
		t.Error("expected inline bytes for small file")
	}
	if !bytes.Equal(a.Inline, data) {
		t.Error("inline data mismatch")
	}
	if a.Uri != "" {
		t.Errorf("expected empty Uri for small file, got %q", a.Uri)
	}
	if a.Size != int64(len(data)) {
		t.Errorf("size mismatch: got %d", a.Size)
	}
}

func TestPut_Large_OnDisk(t *testing.T) {
	s := newTestStore(t)
	// just over the inline threshold
	data := bytes.Repeat([]byte("x"), InlineThreshold+1)
	a, err := s.Put(data, "big.bin", "application/octet-stream")
	if err != nil {
		t.Fatalf("Put: %v", err)
	}
	if len(a.Inline) != 0 {
		t.Error("expected no inline bytes for large file")
	}
	if !strings.HasPrefix(a.Uri, "blob://sha256:") {
		t.Errorf("unexpected Uri: %q", a.Uri)
	}
}

func TestGet_RoundTrip(t *testing.T) {
	s := newTestStore(t)
	data := bytes.Repeat([]byte("round-trip"), 10000) // >64KB
	a, err := s.Put(data, "big.txt", "text/plain")
	if err != nil {
		t.Fatalf("Put: %v", err)
	}

	got, err := s.Get(a.Cid)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if !bytes.Equal(got, data) {
		t.Error("Get data mismatch")
	}
}

func TestGet_NotFound(t *testing.T) {
	s := newTestStore(t)
	_, err := s.Get("sha256:doesnotexist")
	if err != ErrNotFound {
		t.Errorf("expected ErrNotFound, got %v", err)
	}
}

func TestPut_Idempotent(t *testing.T) {
	s := newTestStore(t)
	data := bytes.Repeat([]byte("idempotent"), 10000)
	a1, _ := s.Put(data, "f.bin", "")
	a2, _ := s.Put(data, "f.bin", "")
	if a1.Cid != a2.Cid {
		t.Error("CID differs on second Put of same content")
	}
}

func TestSave_CIDVerification(t *testing.T) {
	s := newTestStore(t)
	data := []byte("verified content")
	cid := computeCID(data)

	if err := s.Save(cid, data); err != nil {
		t.Fatalf("Save: %v", err)
	}

	// tampered data should fail
	if err := s.Save(cid, []byte("tampered")); err == nil {
		t.Error("expected CID mismatch error for tampered data")
	}
}

func TestHas(t *testing.T) {
	s := newTestStore(t)
	data := bytes.Repeat([]byte("z"), InlineThreshold+1)
	a, _ := s.Put(data, "f", "")

	if !s.Has(a.Cid) {
		t.Error("Has returned false for stored blob")
	}
	if s.Has("sha256:nonexistent") {
		t.Error("Has returned true for missing blob")
	}
}
