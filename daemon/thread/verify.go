package thread

import (
	"context"
	"fmt"

	pb "github.com/sahilpohare/p2p-a2a/gen/a2a/v1"
)

// VerifyChain independently recomputes hashes and checks parent_hash
// linkage for a sequence of committed blocks, without trusting whoever
// supplied them (a DHT record, a Bitswap peer, or the local store). This is
// the "no independent verification path" gap named in
// docs/PROPOSAL-goakt-thread-actor.md's Context section: existing code only
// recomputes hashes/checks sigs at commit time, inside the consensus
// backend itself — never on read, and never for a fetcher with no
// consensus role at all.
//
// blocks must be ordered oldest-first (ascending height) and, per the
// project's genesis convention, start at height 1 with an empty
// ParentHash. Every block after the first must chain to the previous
// block's BlockHash via ParentHash.
//
// Both backends include entry bytes in their block hash, so verification
// detects payload substitution as well as sequence or parent-link tampering.
func VerifyChain(kind BackendKind, blocks []*pb.ThreadBlock) error {
	if len(blocks) == 0 {
		return fmt.Errorf("verify chain: no blocks")
	}

	hashFn := blockHashFn(kind)

	var parentHash string
	for i, b := range blocks {
		wantHeight := int64(i + 1)
		if b.Height != wantHeight {
			return fmt.Errorf("verify chain: block %d has height %d, expected %d (blocks must be contiguous from height 1)", i, b.Height, wantHeight)
		}
		if b.ParentHash != parentHash {
			return fmt.Errorf("verify chain: block at height %d has parent_hash %q, expected %q (chain broken)", b.Height, b.ParentHash, parentHash)
		}
		got := hashFn(b)
		if got != b.BlockHash {
			return fmt.Errorf("verify chain: block at height %d has block_hash %q, recomputed %q (hash mismatch — tampered or corrupted)", b.Height, b.BlockHash, got)
		}
		parentHash = b.BlockHash
	}
	return nil
}

// blockHashFn returns the hash function matching how a thread's backend
// computes BlockHash at commit time (see raft.go's raftBlockHash,
// tendermint.go's computeBlockHash).
func blockHashFn(kind BackendKind) func(*pb.ThreadBlock) string {
	if kind == BackendTendermint {
		return computeBlockHash
	}
	return raftBlockHash
}

// VerifyChainFromHead walks a thread's chain backward from its published
// head (see Publisher.PublishBlock/ResolveHead) all the way to genesis
// (height 1), fetching each block from Bitswap and each hop's CID from the
// DHT's per-height records (ResolveHeight) — the mechanism a peer with zero
// prior involvement in the thread (never a replica, or every replica has
// since gone offline) uses to independently reconstruct and verify the full
// history starting from nothing but a thread ID. Returns the verified
// blocks oldest-first (same ordering VerifyChain expects), so a caller can
// pass the result straight to VerifyChain for a second, from-scratch
// recomputation pass, or just trust this function's own per-hop checks.
//
// resolveHead and resolveHeight are called once each way down the chain —
// pass ResolveHead/ResolveHeight bound to a live *dht.IpfsDHT in production;
// tests can pass map-backed stubs instead of standing up a DHT. fetchBlock is
// called once per hop to retrieve the block itself — pass FetchBlock bound
// to a live *bitswap.Bitswap in production, or a stub in tests, for the same
// reason.
func VerifyChainFromHead(
	ctx context.Context,
	kind BackendKind,
	threadID string,
	resolveHead func(ctx context.Context, threadID string) (*ThreadHead, error),
	resolveHeight func(ctx context.Context, threadID string, height int64) (*ThreadHead, error),
	fetchBlock func(ctx context.Context, cidStr string) (*pb.ThreadBlock, error),
) ([]*pb.ThreadBlock, error) {
	head, err := resolveHead(ctx, threadID)
	if err != nil {
		return nil, fmt.Errorf("resolve head: %w", err)
	}

	blocks := make([]*pb.ThreadBlock, head.Height)
	wantHash := head.BlockHash
	for h := head.Height; h >= 1; h-- {
		var ptr *ThreadHead
		if h == head.Height {
			ptr = head
		} else {
			ptr, err = resolveHeight(ctx, threadID, h)
			if err != nil {
				return nil, fmt.Errorf("resolve height %d: %w", h, err)
			}
		}
		if ptr.BlockHash != wantHash {
			return nil, fmt.Errorf("verify chain: height %d pointer's block_hash %q does not match expected %q from child's parent_hash", h, ptr.BlockHash, wantHash)
		}

		block, err := fetchBlock(ctx, ptr.BlockCid)
		if err != nil {
			return nil, fmt.Errorf("fetch height %d: %w", h, err)
		}
		if block.ThreadId != threadID {
			return nil, fmt.Errorf("verify chain: height %d block thread_id %q does not match %q", h, block.ThreadId, threadID)
		}
		if block.Height != h {
			return nil, fmt.Errorf("verify chain: height %d record names a block claiming height %d", h, block.Height)
		}
		got := blockHashFn(kind)(block)
		if got != block.BlockHash {
			return nil, fmt.Errorf("verify chain: height %d block_hash %q does not match recomputed %q", h, block.BlockHash, got)
		}
		if got != wantHash {
			return nil, fmt.Errorf("verify chain: height %d block_hash %q does not match the hash its child named as parent_hash %q", h, got, wantHash)
		}

		blocks[h-1] = block
		wantHash = block.ParentHash
	}
	if wantHash != "" {
		return nil, fmt.Errorf("verify chain: genesis block (height 1) has non-empty parent_hash %q", wantHash)
	}
	return blocks, nil
}
