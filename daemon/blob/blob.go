// Package blob implements a local content-addressed blob store.
//
// Files are stored flat under <dataDir>/blobs/<cid> where cid is the
// hex-encoded SHA-256 hash prefixed with "sha256:" (matching the Artifact.Cid
// field convention used throughout the protocol).
//
// Small files (≤ inlineThreshold) are returned inline in the Artifact.
// Large files are stored on disk; callers receive the CID and must arrange
// peer-to-peer transfer via the /a2a/blob/1.0.0 protocol.
package blob

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	pb "github.com/sahilpohare/p2p-a2a/gen/a2a/v1"
)

const (
	InlineThreshold = 64 * 1024 // 64 KB — inline below this size
)

// ErrNotFound is returned when the requested CID is not in the local store.
var ErrNotFound = errors.New("blob not found")

// Store is a local content-addressed file store.
type Store struct {
	dir string // e.g. ~/.p2p-a2a/blobs
}

// New opens (or creates) a blob store rooted at dir.
func New(dir string) (*Store, error) {
	if err := os.MkdirAll(dir, 0700); err != nil {
		return nil, fmt.Errorf("create blob dir: %w", err)
	}
	return &Store{dir: dir}, nil
}

// Put stores data and returns an Artifact describing it.
// Data ≤ InlineThreshold is embedded in Artifact.Inline.
// Larger data is written to disk; Artifact.Uri is set to the local path.
func (s *Store) Put(data []byte, name, mimeType string) (*pb.Artifact, error) {
	cid := computeCID(data)

	artifact := &pb.Artifact{
		Cid:      cid,
		Name:     name,
		MimeType: mimeType,
		Size:     int64(len(data)),
	}

	if len(data) <= InlineThreshold {
		artifact.Inline = data
		return artifact, nil
	}

	// Write to disk (idempotent — same CID always same content)
	path := s.path(cid)
	if _, err := os.Stat(path); errors.Is(err, os.ErrNotExist) {
		if err := os.WriteFile(path, data, 0600); err != nil {
			return nil, fmt.Errorf("write blob %s: %w", cid, err)
		}
	}
	artifact.Uri = "blob://" + cid
	return artifact, nil
}

// Get returns the raw bytes for a CID.
// Returns ErrNotFound if the CID is not in the local store.
func (s *Store) Get(cid string) ([]byte, error) {
	data, err := os.ReadFile(s.path(cid))
	if errors.Is(err, os.ErrNotExist) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("read blob %s: %w", cid, err)
	}
	return data, nil
}

// Has reports whether cid exists in the local store.
func (s *Store) Has(cid string) bool {
	_, err := os.Stat(s.path(cid))
	return err == nil
}

// Save writes externally received bytes into the store (e.g. after a fetch).
// Returns an error if the data doesn't match the expected CID.
func (s *Store) Save(cid string, data []byte) error {
	got := computeCID(data)
	if got != cid {
		return fmt.Errorf("CID mismatch: expected %s, got %s", cid, got)
	}
	path := s.path(cid)
	if _, err := os.Stat(path); err == nil {
		return nil // already have it
	}
	if err := os.WriteFile(path, data, 0600); err != nil {
		return fmt.Errorf("save blob %s: %w", cid, err)
	}
	return nil
}

// ─── internal ─────────────────────────────────────────────────────────────────

func (s *Store) path(cid string) string {
	// strip "sha256:" prefix for filename
	name := cid
	if len(cid) > 7 && cid[:7] == "sha256:" {
		name = cid[7:]
	}
	return filepath.Join(s.dir, name)
}

// computeCID returns "sha256:<hex>" for the given data.
func computeCID(data []byte) string {
	h := sha256.Sum256(data)
	return "sha256:" + hex.EncodeToString(h[:])
}
