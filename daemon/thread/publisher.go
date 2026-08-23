// Publisher makes committed thread blocks durable and discoverable on the
// network independent of any live consensus participant — the "stays on the
// network" gap named in docs/PROPOSAL-goakt-thread-actor.md's Context
// section: "it doesn't survive or stay discoverable after the parties
// involved go offline." Two mechanisms, both content-addressed so a fetcher
// never has to trust whoever served the data:
//
//  1. Every committed block is pushed to the Bitswap blockstore under its
//     own CIDv1, so any peer that knows the CID can fetch it — the same
//     mechanism SendFile/FetchFile already use for artifacts.
//  2. A DHT record at /a2a/threads/<thread_id> points at the latest block's
//     CID, so a peer that does NOT already know a block CID (was never a
//     replica, or every replica has since gone offline) can still discover
//     where the chain currently ends and walk backward via VerifyChain.
package thread

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/ipfs/boxo/bitswap"
	"github.com/ipfs/boxo/blockstore"
	"github.com/ipfs/go-cid"
	dht "github.com/libp2p/go-libp2p-kad-dht"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"

	appactors "github.com/sahilpohare/p2p-a2a/daemon/actors"
	pb "github.com/sahilpohare/p2p-a2a/gen/a2a/v1"
	"github.com/sahilpohare/p2p-a2a/pkg/p2putil"
)

// ThreadHead is the JSON value published at /a2a/threads/<thread_id>. It
// intentionally carries no signature: authenticity comes from content
// addressing (BlockCid names an immutable, self-describing block; a fetcher
// verifies the chain itself via VerifyChain rather than trusting the DHT
// record or whichever peer answered the lookup). The record is only a
// pointer — "here is where to start looking" — not a claim to be trusted on
// its own.
type ThreadHead struct {
	ThreadID  string     `json:"thread_id"`
	Height    int64      `json:"height"`
	BlockCid  string     `json:"block_cid"`
	BlockHash string     `json:"block_hash"`
	UpdatedAt int64      `json:"updated_at"`
	Thread    *pb.Thread `json:"thread,omitempty"`
}

// threadHeadDHTKey returns the DHT key for a thread's head pointer, matching
// the /a2a/threads/ namespace registered on the DHT validator in
// daemon/node/node.go.
func threadHeadDHTKey(threadID string) string {
	return "/threads/" + threadID
}

// threadHeightDHTKey returns the DHT key that maps one specific committed
// height of a thread to that block's CID. Published alongside the head
// pointer for every committed block (not just the latest), so a fetcher
// that only has the head pointer can walk the whole chain backward by
// height — height N's key resolves to a CID, whose fetched block names
// height N-1 via ParentHash, and threadHeightDHTKey(id, N-1) resolves that
// height's CID in turn — without needing a ParentCid field on ThreadBlock
// itself (a wire-format change out of scope here) or trusting any single
// peer's answer, since each hop's CID is independently re-derivable by
// anyone who has the DHT record.
func threadHeightDHTKey(threadID string, height int64) string {
	return fmt.Sprintf("/threads/%s/%d", threadID, height)
}

// Publisher pushes committed blocks to Bitswap and updates each thread's DHT
// head pointer. A nil *Publisher is valid and every method is then a no-op —
// durability publishing is opt-in infrastructure, not a hard dependency for
// running consensus (mirrors registry/*Registry's nil-safety pattern used
// elsewhere in this codebase).
type Publisher struct {
	dht        *dht.IpfsDHT
	blockstore blockstore.Blockstore
	bitswap    *bitswap.Bitswap
	log        *zap.Logger
	exec       *appactors.Executor
}

func (p *Publisher) EnableActor(ctx context.Context, h *appactors.Hierarchy) error {
	return appactors.EnableSerialActor(ctx, h, "thread-durability", func(e *appactors.Executor) { p.exec = e }, nil)
}

// NewPublisher creates a Publisher. Any of d, bs, bsw may be nil to disable
// the corresponding half of publishing (e.g. tests that only care about
// consensus, not durability).
func NewPublisher(d *dht.IpfsDHT, bs blockstore.Blockstore, bsw *bitswap.Bitswap, log *zap.Logger) *Publisher {
	return &Publisher{dht: d, blockstore: bs, bitswap: bsw, log: log}
}

// PublishBlock stores a committed block to Bitswap and updates the thread's
// DHT head pointer. Best-effort: failures are logged, not returned, because
// publishing must never block or fail block commitment itself — commitBlock
// already persisted the block to SQLite (the durable source of truth for
// this node) before any publisher runs; publishing only extends reach to
// peers who aren't already replicas.
func (p *Publisher) PublishBlock(ctx context.Context, thread *pb.Thread, block *pb.ThreadBlock) {
	if p == nil {
		return
	}
	appactors.DispatchCast(p.exec, func() { p.publishBlock(ctx, thread, block) })
}

func (p *Publisher) publishBlock(ctx context.Context, thread *pb.Thread, block *pb.ThreadBlock) {

	data, err := proto.Marshal(block)
	if err != nil {
		p.log.Warn("publisher: marshal block", zap.Error(err))
		return
	}
	blk, err := p2putil.CIDv1Block(data)
	if err != nil {
		p.log.Warn("publisher: build block", zap.Error(err))
		return
	}

	if p.blockstore != nil {
		if err := p.blockstore.Put(ctx, blk); err != nil {
			p.log.Warn("publisher: store block", zap.String("thread", block.ThreadId), zap.Error(err))
			return
		}
		if p.bitswap != nil {
			if err := p.bitswap.NotifyNewBlocks(ctx, blk); err != nil {
				p.log.Warn("publisher: notify bitswap", zap.String("thread", block.ThreadId), zap.Error(err))
			}
		}
	}

	if p.dht == nil {
		return
	}
	head := ThreadHead{
		ThreadID:  block.ThreadId,
		Height:    block.Height,
		BlockCid:  blk.Cid().String(),
		BlockHash: block.BlockHash,
		UpdatedAt: time.Now().UnixMilli(),
		Thread:    proto.Clone(thread).(*pb.Thread),
	}
	headData, err := json.Marshal(head)
	if err != nil {
		p.log.Warn("publisher: marshal thread head", zap.Error(err))
		return
	}
	putCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	// Per-height record first: a chain walker needs height N's CID before
	// it can be sure the head pointer (which a concurrent, higher commit
	// could already have overwritten) still describes a reachable chain.
	// Order here doesn't provide atomicity across the two keys — DHT PUTs
	// are independent — but publishing height before head means a walker
	// that resolves the head first always finds this height's record
	// already there, never a race where the head points ahead of what's
	// been published.
	if err := p.dht.PutValue(putCtx, threadHeightDHTKey(block.ThreadId, block.Height), headData); err != nil {
		p.log.Warn("publisher: put thread height", zap.String("thread", block.ThreadId), zap.Int64("height", block.Height), zap.Error(err))
	}
	if err := p.dht.PutValue(putCtx, threadHeadDHTKey(block.ThreadId), headData); err != nil {
		p.log.Warn("publisher: put thread head", zap.String("thread", block.ThreadId), zap.Error(err))
	}
	// A provider record complements the signed/validated head pointers: it
	// lets a fresh recovery daemon find multiple peers that have persisted the
	// ciphertext. Advertise only after the block is in the local blockstore.
	if err := AdvertiseArchiveProvider(putCtx, p.dht, block.ThreadId); err != nil {
		p.log.Warn("publisher: advertise archive provider", zap.String("thread", block.ThreadId), zap.Error(err))
	}
}

// ResolveHead fetches a thread's current head pointer from the DHT. Returns
// an error if no record is found or the record fails ThreadHeadValidator's
// checks (see pkg/a2avalidator).
func ResolveHead(ctx context.Context, d *dht.IpfsDHT, threadID string) (*ThreadHead, error) {
	data, err := d.GetValue(ctx, threadHeadDHTKey(threadID))
	if err != nil {
		return nil, fmt.Errorf("dht get thread head %q: %w", threadID, err)
	}
	var head ThreadHead
	if err := json.Unmarshal(data, &head); err != nil {
		return nil, fmt.Errorf("unmarshal thread head: %w", err)
	}
	if head.ThreadID != threadID {
		return nil, fmt.Errorf("thread head ID %q does not match lookup %q", head.ThreadID, threadID)
	}
	if _, err := cid.Decode(head.BlockCid); err != nil {
		return nil, fmt.Errorf("invalid thread head CID: %w", err)
	}
	return &head, nil
}

// ResolveHeight fetches the pointer for one specific committed height of a
// thread from the DHT — the per-hop lookup VerifyChainFromHead uses to walk
// backward from the head.
func ResolveHeight(ctx context.Context, d *dht.IpfsDHT, threadID string, height int64) (*ThreadHead, error) {
	data, err := d.GetValue(ctx, threadHeightDHTKey(threadID, height))
	if err != nil {
		return nil, fmt.Errorf("dht get thread height %q/%d: %w", threadID, height, err)
	}
	var head ThreadHead
	if err := json.Unmarshal(data, &head); err != nil {
		return nil, fmt.Errorf("unmarshal thread height record: %w", err)
	}
	if head.ThreadID != threadID || head.Height != height {
		return nil, fmt.Errorf("thread height record does not match lookup %q/%d", threadID, height)
	}
	if _, err := cid.Decode(head.BlockCid); err != nil {
		return nil, fmt.Errorf("invalid thread height CID: %w", err)
	}
	return &head, nil
}

// FetchBlock fetches a single block by CID via Bitswap and unmarshals it.
// Returns the raw block alongside the decoded proto so callers that need
// content-addressing guarantees (the returned blk.Cid() is what was
// actually fetched) can cross-check it against what they asked for.
func FetchBlock(ctx context.Context, bsw *bitswap.Bitswap, cidStr string) (*pb.ThreadBlock, error) {
	c, err := cid.Decode(cidStr)
	if err != nil {
		return nil, fmt.Errorf("invalid block cid %q: %w", cidStr, err)
	}
	blk, err := bsw.GetBlock(ctx, c)
	if err != nil {
		return nil, fmt.Errorf("fetch block %s: %w", cidStr, err)
	}
	var block pb.ThreadBlock
	if err := proto.Unmarshal(blk.RawData(), &block); err != nil {
		return nil, fmt.Errorf("unmarshal block %s: %w", cidStr, err)
	}
	return &block, nil
}
