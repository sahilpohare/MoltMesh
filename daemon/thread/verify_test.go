package thread

import (
	"context"
	"errors"
	"testing"

	pb "github.com/sahilpohare/p2p-a2a/gen/a2a/v1"
)

// buildRaftChain returns a valid, self-consistent chain of n blocks using
// raftBlockHash, matching what RaftBackend.commitBlock actually produces.
func buildRaftChain(threadID string, proposerDID string, n int) []*pb.ThreadBlock {
	blocks := make([]*pb.ThreadBlock, n)
	var parentHash string
	for i := 0; i < n; i++ {
		b := &pb.ThreadBlock{
			ThreadId:    threadID,
			Height:      int64(i + 1),
			Round:       int32(i),
			ParentHash:  parentHash,
			Entries:     []*pb.ThreadEntry{{AuthorDid: proposerDID, Payload: []byte("entry"), Kind: "message"}},
			ProposerDid: proposerDID,
			CommittedAt: int64(i),
		}
		b.BlockHash = raftBlockHash(b)
		blocks[i] = b
		parentHash = b.BlockHash
	}
	return blocks
}

func TestVerifyChain_Valid(t *testing.T) {
	blocks := buildRaftChain("t1", "did:key:zProposer", 5)
	if err := VerifyChain(BackendRaft, blocks); err != nil {
		t.Fatalf("expected valid chain, got error: %v", err)
	}
}

func TestVerifyChain_Empty(t *testing.T) {
	if err := VerifyChain(BackendRaft, nil); err == nil {
		t.Fatal("expected error for empty chain")
	}
}

func TestVerifyChain_WrongHeight(t *testing.T) {
	blocks := buildRaftChain("t1", "did:key:zProposer", 3)
	blocks[1].Height = 5 // corrupt: should be 2
	if err := VerifyChain(BackendRaft, blocks); err == nil {
		t.Fatal("expected error for non-contiguous height")
	}
}

func TestVerifyChain_BrokenParentLink(t *testing.T) {
	blocks := buildRaftChain("t1", "did:key:zProposer", 3)
	blocks[2].ParentHash = "not-the-real-parent-hash"
	if err := VerifyChain(BackendRaft, blocks); err == nil {
		t.Fatal("expected error for broken parent_hash link")
	}
}

func TestVerifyChain_TamperedHash(t *testing.T) {
	blocks := buildRaftChain("t1", "did:key:zProposer", 3)
	blocks[0].BlockHash = "0000000000000000000000000000000000000000000000000000000000000"
	if err := VerifyChain(BackendRaft, blocks); err == nil {
		t.Fatal("expected error for tampered block_hash")
	}
}

func TestVerifyChain_TamperedProposer(t *testing.T) {
	// Raft's hash covers ProposerDid, so swapping it after the fact must be
	// caught even though Entries themselves aren't covered (the documented
	// gap in VerifyChain's doc comment).
	blocks := buildRaftChain("t1", "did:key:zProposer", 3)
	blocks[1].ProposerDid = "did:key:zAttacker"
	if err := VerifyChain(BackendRaft, blocks); err == nil {
		t.Fatal("expected error for tampered proposer_did (covered by raftBlockHash)")
	}
}

// ─── VerifyChainFromHead ──────────────────────────────────────────────────────

// chainFixture wires map-backed resolveHead/resolveHeight/fetchBlock stubs
// over an in-memory chain, standing in for the DHT + Bitswap in production.
type chainFixture struct {
	threadID  string
	blocks    []*pb.ThreadBlock // 1-indexed by height via blocks[height-1]
	byCid     map[string]*pb.ThreadBlock
	heightCid map[int64]string
}

func newChainFixture(threadID string, blocks []*pb.ThreadBlock) *chainFixture {
	f := &chainFixture{
		threadID:  threadID,
		blocks:    blocks,
		byCid:     map[string]*pb.ThreadBlock{},
		heightCid: map[int64]string{},
	}
	for _, b := range blocks {
		// A fake but unique-per-block "CID" — VerifyChainFromHead only
		// needs it to round-trip through fetchBlock, not to be a real CID.
		fakeCid := "fake-cid-" + b.BlockHash
		f.byCid[fakeCid] = b
		f.heightCid[b.Height] = fakeCid
	}
	return f
}

func (f *chainFixture) head() *ThreadHead {
	last := f.blocks[len(f.blocks)-1]
	return f.headAt(last.Height)
}

func (f *chainFixture) headAt(height int64) *ThreadHead {
	c := f.heightCid[height]
	b := f.byCid[c]
	return &ThreadHead{ThreadID: f.threadID, Height: height, BlockCid: c, BlockHash: b.BlockHash}
}

func (f *chainFixture) resolveHead(_ context.Context, threadID string) (*ThreadHead, error) {
	if threadID != f.threadID {
		return nil, errors.New("no head for thread")
	}
	return f.head(), nil
}

func (f *chainFixture) resolveHeight(_ context.Context, threadID string, height int64) (*ThreadHead, error) {
	if threadID != f.threadID {
		return nil, errors.New("no such thread")
	}
	if _, ok := f.heightCid[height]; !ok {
		return nil, errors.New("no such height")
	}
	return f.headAt(height), nil
}

func (f *chainFixture) fetchBlock(_ context.Context, cidStr string) (*pb.ThreadBlock, error) {
	b, ok := f.byCid[cidStr]
	if !ok {
		return nil, errors.New("block not found")
	}
	return b, nil
}

// {P}: a valid 4-block raft chain, published into a chainFixture (standing
//
//	in for DHT height-records + Bitswap blocks).
//
// {C}: VerifyChainFromHead walks from the head back to genesis.
// {Q}: returns all 4 blocks, oldest-first, no error — proving a fetcher with
//
//	nothing but a thread ID can reconstruct and verify the full history,
//	the core "stays on the network" property.
func TestVerifyChainFromHead_Valid(t *testing.T) {
	blocks := buildRaftChain("t1", "did:key:zProposer", 4)
	f := newChainFixture("t1", blocks)

	got, err := VerifyChainFromHead(context.Background(), BackendRaft, "t1", f.resolveHead, f.resolveHeight, f.fetchBlock)
	if err != nil {
		t.Fatalf("VerifyChainFromHead: %v", err)
	}
	if len(got) != 4 {
		t.Fatalf("expected 4 blocks, got %d", len(got))
	}
	for i, b := range got {
		if b.Height != int64(i+1) {
			t.Errorf("block %d: expected height %d, got %d", i, i+1, b.Height)
		}
	}
}

// {P}: a valid chain, but the fixture's per-height record at height 2 is
//
//	swapped to point at a different block's CID/hash (simulating a hostile
//	or corrupted DHT record for one hop).
//
// {C}: VerifyChainFromHead walks from the head.
// {Q}: the walk fails at the tampered hop instead of silently accepting a
//
//	forged link — this is the property that makes "don't trust whoever
//	served the DHT record" real rather than aspirational.
func TestVerifyChainFromHead_TamperedHeightRecord(t *testing.T) {
	blocks := buildRaftChain("t1", "did:key:zProposer", 4)
	f := newChainFixture("t1", blocks)

	// Point height 2's record at height 4's block instead.
	f.heightCid[2] = f.heightCid[4]

	_, err := VerifyChainFromHead(context.Background(), BackendRaft, "t1", f.resolveHead, f.resolveHeight, f.fetchBlock)
	if err == nil {
		t.Fatal("expected error for tampered height record")
	}
}

// {P}: a valid chain where the served block's own BlockHash field has been
//
//	corrupted (a peer serving a mutated block for one CID lookup).
//
// {C}: VerifyChainFromHead walks from the head.
// {Q}: the recomputed hash mismatch is caught.
func TestVerifyChainFromHead_TamperedBlock(t *testing.T) {
	blocks := buildRaftChain("t1", "did:key:zProposer", 3)
	f := newChainFixture("t1", blocks)
	blocks[1].Entries[0].Payload = []byte("tampered") // mutate after hashing

	_, err := VerifyChainFromHead(context.Background(), BackendRaft, "t1", f.resolveHead, f.resolveHeight, f.fetchBlock)
	if err == nil {
		t.Fatal("expected entry tampering to invalidate the Raft block hash")
	}
}

func TestVerifyChainFromHead_ResolveHeadError(t *testing.T) {
	blocks := buildRaftChain("t1", "did:key:zProposer", 2)
	f := newChainFixture("t1", blocks)

	_, err := VerifyChainFromHead(context.Background(), BackendRaft, "unknown-thread", f.resolveHead, f.resolveHeight, f.fetchBlock)
	if err == nil {
		t.Fatal("expected error resolving head for unknown thread")
	}
}
