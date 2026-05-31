// Package thread: Manager owns all per-thread Engines and GossipBridges.
package thread

import (
	"context"
	"fmt"
	"sync"
	"time"

	pubsub "github.com/libp2p/go-libp2p-pubsub"
	"github.com/google/uuid"
	"go.uber.org/zap"

	pb "github.com/sahilpohare/p2p-a2a/gen/a2a/v1"
	"github.com/sahilpohare/p2p-a2a/daemon/identity"
)

// Manager owns all per-thread Engines and GossipSub bridges.
type Manager struct {
	store *Store
	id    *identity.Identity
	ps    *pubsub.PubSub
	log   *zap.Logger
	ctx   context.Context // lifecycle context — engines live as long as this

	mu      sync.Mutex
	engines map[string]*Engine
	bridges map[string]*GossipBridge
	cancels map[string]context.CancelFunc
}

// NewManager creates a Manager. ctx is the daemon lifecycle context — engines
// run until it is cancelled. Call Start for each thread you want active.
func NewManager(
	ctx context.Context,
	store *Store,
	id *identity.Identity,
	ps *pubsub.PubSub,
	log *zap.Logger,
) *Manager {
	return &Manager{
		ctx:     ctx,
		store:   store,
		id:      id,
		ps:      ps,
		log:     log,
		engines: make(map[string]*Engine),
		bridges: make(map[string]*GossipBridge),
		cancels: make(map[string]context.CancelFunc),
	}
}

// CreateThread persists a new thread and starts its consensus engine.
func (m *Manager) CreateThread(_ context.Context, req *pb.CreateThreadRequest) (*pb.Thread, error) {
	replicas := req.ReplicaDids
	// ensure creator is in the validator set
	found := false
	for _, did := range replicas {
		if did == m.id.DID {
			found = true
			break
		}
	}
	if !found {
		replicas = append([]string{m.id.DID}, replicas...)
	}

	f := req.F
	n := int32(3*f + 1)
	if f == 0 {
		n = 1 // single-validator mode
	}
	if int32(len(replicas)) < n {
		return nil, fmt.Errorf("need at least %d replicas for f=%d, got %d (hint: for single-validator use f=0)", n, f, len(replicas))
	}

	epochMs := req.EpochMs
	if epochMs == 0 {
		epochMs = defaultEpochMs
	}

	meta := req.Metadata
	if meta == nil {
		meta = map[string]string{}
	}
	if _, ok := meta["backend"]; !ok {
		meta["backend"] = string(BackendRaft) // default
	}

	thread := &pb.Thread{
		Id:          uuid.New().String(),
		CreatorDid:  m.id.DID,
		ReplicaDids: replicas,
		N:           n,
		F:           f,
		EpochMs:     epochMs,
		CreatedAt:   time.Now().UnixMilli(),
		Metadata:    meta,
	}

	if err := m.store.SaveThread(thread); err != nil {
		return nil, fmt.Errorf("save thread: %w", err)
	}

	if err := m.Start(thread); err != nil {
		return nil, fmt.Errorf("start thread engine: %w", err)
	}

	return thread, nil
}

// InviteReceived is called when a THREAD_INVITE message arrives from a peer.
// It saves the thread and starts its engine if not already running.
func (m *Manager) InviteReceived(thread *pb.Thread) error {
	if err := m.store.SaveThread(thread); err != nil {
		return fmt.Errorf("save thread: %w", err)
	}
	return m.Start(thread)
}

// Start starts the consensus engine for an existing thread.
// Safe to call multiple times — if already started, returns nil.
func (m *Manager) Start(thread *pb.Thread) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if _, ok := m.engines[thread.Id]; ok {
		return nil // already running
	}

	kind := BackendKind(thread.Metadata["backend"])
	if kind == "" {
		kind = BackendRaft // default to Raft
	}

	eng, err := NewEngine(thread, m.id, m.store, m.log, kind, nil)
	if err != nil {
		return fmt.Errorf("new engine: %w", err)
	}

	bridge, err := NewGossipBridge(m.ps, eng, thread.Id, m.id, m.log)
	if err != nil {
		return fmt.Errorf("new gossip bridge: %w", err)
	}

	tctx, cancel := context.WithCancel(m.ctx) // use lifecycle context, not request context
	m.engines[thread.Id] = eng
	m.bridges[thread.Id] = bridge
	m.cancels[thread.Id] = cancel

	go bridge.Run(tctx)

	m.log.Info("thread: engine started", zap.String("thread", thread.Id))
	return nil
}

// Stop halts the engine for a thread.
func (m *Manager) Stop(threadID string) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if cancel, ok := m.cancels[threadID]; ok {
		cancel()
		delete(m.engines, threadID)
		delete(m.bridges, threadID)
		delete(m.cancels, threadID)
	}
}

// Engine returns the running Engine for a thread, or nil.
func (m *Manager) Engine(threadID string) *Engine {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.engines[threadID]
}

// AppendEntry enqueues an entry for the next block proposal on this thread.
func (m *Manager) AppendEntry(threadID string, entry *pb.ThreadEntry) error {
	return m.store.EnqueueEntry(threadID, entry)
}

// GetThread loads a thread from the store.
func (m *Manager) GetThread(threadID string) (*pb.Thread, error) {
	return m.store.GetThread(threadID)
}

// GetEntries returns committed entries since sinceHeight.
func (m *Manager) GetEntries(threadID string, sinceHeight int64, limit int) ([]*pb.ThreadEntryWithPos, error) {
	blocks, err := m.store.GetBlocksSince(threadID, sinceHeight, limit)
	if err != nil {
		return nil, err
	}
	var out []*pb.ThreadEntryWithPos
	for _, b := range blocks {
		for i, entry := range b.Entries {
			out = append(out, &pb.ThreadEntryWithPos{
				Height:    b.Height,
				Index:     int32(i),
				Entry:     entry,
				BlockHash: b.BlockHash,
			})
		}
	}
	return out, nil
}
