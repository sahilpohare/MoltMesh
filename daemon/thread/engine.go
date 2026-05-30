package thread

import (
	"context"
	"sync"

	"go.uber.org/zap"

	pb "github.com/sahilpohare/p2p-a2a/gen/a2a/v1"
	"github.com/sahilpohare/p2p-a2a/daemon/identity"
)

// CommitCallback is called (outside any lock) when a block commits.
type CommitCallback func(block *pb.ThreadBlock)

// Engine wraps a Backend and adds subscriber fan-out and a commit callback.
// It is the public handle used by Manager, GossipBridge, and the gRPC server.
type Engine struct {
	backend  Backend
	thread   *pb.Thread
	id       *identity.Identity
	log      *zap.Logger
	onCommit CommitCallback

	mu          sync.Mutex
	subscribers []chan *pb.ThreadEntryWithPos
}

// NewEngine creates an Engine using the selected backend kind.
// onCommit may be nil.
func NewEngine(
	thread *pb.Thread,
	id *identity.Identity,
	store *Store,
	log *zap.Logger,
	kind BackendKind,
	onCommit CommitCallback,
) (*Engine, error) {
	e := &Engine{
		thread:   thread,
		id:       id,
		log:      log,
		onCommit: onCommit,
	}

	var b Backend
	var err error
	switch kind {
	case BackendRaft:
		b, err = newRaftBackend(thread, id, store, log, e.handleCommit)
	default: // BackendTendermint
		b, err = newTendermintBackend(thread, id, store, log, e.handleCommit)
	}
	if err != nil {
		return nil, err
	}
	e.backend = b
	return e, nil
}

// Deliver feeds an inbound ConsensusMsg into the backend.
func (e *Engine) Deliver(msg *pb.ConsensusMsg) { e.backend.Deliver(msg) }

// Subscribe returns a channel that receives committed entries in order.
func (e *Engine) Subscribe() <-chan *pb.ThreadEntryWithPos {
	ch := make(chan *pb.ThreadEntryWithPos, 256)
	e.mu.Lock()
	e.subscribers = append(e.subscribers, ch)
	e.mu.Unlock()
	return ch
}

// Unsubscribe closes and removes a subscriber channel.
func (e *Engine) Unsubscribe(ch <-chan *pb.ThreadEntryWithPos) {
	e.mu.Lock()
	defer e.mu.Unlock()
	for i, s := range e.subscribers {
		if s == ch {
			e.subscribers = append(e.subscribers[:i], e.subscribers[i+1:]...)
			close(s)
			return
		}
	}
}

// Run starts the backend consensus loop. Blocks until ctx is cancelled.
func (e *Engine) Run(ctx context.Context, broadcast func(*pb.ConsensusMsg)) {
	e.backend.Run(ctx, broadcast)
}

// handleCommit fans out committed block entries to subscribers then calls onCommit.
func (e *Engine) handleCommit(block *pb.ThreadBlock) {
	e.mu.Lock()
	for i, entry := range block.Entries {
		ep := &pb.ThreadEntryWithPos{
			Height:    block.Height,
			Index:     int32(i),
			Entry:     entry,
			BlockHash: block.BlockHash,
		}
		for _, sub := range e.subscribers {
			select {
			case sub <- ep:
			default:
				e.log.Warn("subscriber channel full, dropping entry",
					zap.Int64("height", block.Height),
					zap.Int("index", i),
					zap.String("thread", block.ThreadId),
				)
			}
		}
	}
	e.mu.Unlock()

	if e.onCommit != nil {
		e.onCommit(block)
	}
}
