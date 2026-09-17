// Package thread: Manager owns all per-thread Engines and GossipBridges.
package thread

import (
	"context"
	"encoding/base64"
	"fmt"
	"sync"
	"time"

	"github.com/google/uuid"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"

	"github.com/sahilpohare/p2p-a2a/daemon/identity"
	pb "github.com/sahilpohare/p2p-a2a/gen/a2a/v1"
	"github.com/sahilpohare/p2p-a2a/pkg/threadcrypto"
)

// Manager owns all per-thread Engines and GossipSub bridges. It embeds
// *Store directly so every store operation Manager doesn't add logic on top
// of (membership, invites, key envelopes, GetThread, ImportHistory,
// CommittedHead, ...) is satisfied by Go's method promotion instead of a
// hand-written forwarding method per operation — the same embedding
// ActorManager uses in actor_manager.go, so both implement
// rpc.ThreadManager/deliver.ThreadInviter off one shared set of promoted
// methods.
type Manager struct {
	*Store
	store *Store // same pointer as the embedded *Store; kept so internal call sites can keep saying m.store.X
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
		Store:   store,
		store:   store,
		ctx:     ctx,
		id:      id,
		ps:      ps,
		log:     log,
		engines: make(map[string]*Engine),
		bridges: make(map[string]*GossipBridge),
		cancels: make(map[string]context.CancelFunc),
	}
}

// maxTolerableFailures inverts the validator-set sizing rule: given a replica
// count, how many failures can that many nodes actually survive under the
// chosen backend. Raft needs 2f+1 voters, Tendermint 3f+1, so the answers are
// (n-1)/2 and (n-1)/3 respectively.
func maxTolerableFailures(backend BackendKind, replicas int) int {
	if replicas < 1 {
		return 0
	}
	if backend == BackendTendermint {
		return (replicas - 1) / 3
	}
	return (replicas - 1) / 2
}

// NewThreadFromRequest validates a CreateThreadRequest and builds the
// *pb.Thread to persist — the pure, side-effect-free part of thread
// creation shared by Manager.CreateThread (Engine/GossipBridge path) and
// ActorManager.CreateThread (GoAkt actor path, see actor_manager.go), so the
// replica-set/quorum/metadata rules live in exactly one place.
func NewThreadFromRequest(selfDID string, req *pb.CreateThreadRequest) (*pb.Thread, error) {
	return newThreadFromRequest(selfDID, req, "", 0)
}

// NewThreadFromSignedRequest reconstructs the exact SDK-signed descriptor.
// The daemon verifies this before any durable mutation; it never substitutes
// its own identity for an attached agent's thread creator identity.
func NewThreadFromSignedRequest(req *pb.CreateThreadRequest) (*pb.Thread, error) {
	if req == nil || req.ThreadId == "" || req.CreatorDid == "" || len(req.CreatorSignature) == 0 || req.CreatedAt <= 0 {
		return nil, fmt.Errorf("signed thread creation requires thread_id, creator_did, creator_signature, and created_at")
	}
	th, err := newThreadFromRequest(req.CreatorDid, req, req.ThreadId, req.CreatedAt)
	if err != nil {
		return nil, err
	}
	th.Metadata[descriptorSignatureKey] = base64.StdEncoding.EncodeToString(req.CreatorSignature)
	if err := VerifyDescriptor(th); err != nil {
		return nil, fmt.Errorf("verify SDK thread descriptor: %w", err)
	}
	return th, nil
}

func newThreadFromRequest(selfDID string, req *pb.CreateThreadRequest, requestedID string, createdAt int64) (*pb.Thread, error) {
	if req == nil {
		return nil, fmt.Errorf("thread creation request is required")
	}
	replicas := req.ReplicaDids
	// ensure creator is in the validator set
	found := false
	for _, did := range replicas {
		if did == selfDID {
			found = true
			break
		}
	}
	if !found {
		replicas = append([]string{selfDID}, replicas...)
	}

	meta := req.Metadata
	if meta == nil {
		meta = map[string]string{}
	}
	if _, ok := meta["backend"]; !ok {
		meta["backend"] = string(BackendRaft) // default
	}

	// The required validator-set size depends on the failure model the chosen
	// backend defends against, so it has to be computed after the backend is
	// known.
	//
	// Tendermint is Byzantine-fault-tolerant and needs n = 3f+1: its quorum is
	// 2f+1 (see TendermintBackend.quorum) and the safety argument requires
	// that any two quorums intersect in at least one honest validator, which
	// only holds when the validator set is 3f+1.
	//
	// Raft is crash-fault-tolerant. It needs only n = 2f+1, because a majority
	// of 2f+1 is f+1 and any two majorities intersect. Requiring 3f+1 for Raft
	// bought no extra fault tolerance: with f=1, four voters still tolerate
	// exactly one failure, the same as three, while demanding an extra node
	// and producing an even-sized voter set, which raises the quorum to 3 and
	// makes split votes more likely. thread.N is the voter-set size (see
	// newRaftBackend, which takes the first N replicas as voters and treats
	// the remainder as non-voting observers), so lowering it here narrows the
	// quorum rather than weakening it.
	f := req.F
	var n int32
	switch BackendKind(meta["backend"]) {
	case BackendTendermint:
		n = 3*f + 1
	default: // BackendRaft
		n = 2*f + 1
	}
	if f == 0 {
		n = 1 // single-validator mode
	}
	if int32(len(replicas)) < n {
		// Reject rather than silently lowering f. f is a promise about how
		// many failures the thread survives, and quietly downgrading it would
		// leave the caller believing in fault tolerance they do not have.
		// Report the largest f this replica count can actually support so the
		// caller can choose deliberately.
		//
		// No consensus algorithm can do better here. Tolerating a failure
		// requires a surviving majority to distinguish "peer crashed" from
		// "peer unreachable"; with two nodes each side of a partition sees one
		// of two and cannot tell, so allowing both to proceed would permit
		// split-brain. Two nodes therefore cannot tolerate one failure under
		// any algorithm, only under a weaker guarantee.
		return nil, fmt.Errorf(
			"backend %q with f=%d needs at least %d replicas, got %d: %d replicas supports at most f=%d (use f=0 for a single voter plus %d read-only observers)",
			meta["backend"], f, n, len(replicas),
			len(replicas), maxTolerableFailures(BackendKind(meta["backend"]), len(replicas)),
			len(replicas)-1)
	}

	epochMs := req.EpochMs
	if epochMs == 0 {
		epochMs = defaultEpochMs
	}

	if requestedID == "" {
		requestedID = uuid.New().String()
	}
	if createdAt == 0 {
		createdAt = time.Now().UnixMilli()
	}
	return &pb.Thread{
		Id:          requestedID,
		CreatorDid:  selfDID,
		ReplicaDids: replicas,
		N:           n,
		F:           f,
		EpochMs:     epochMs,
		CreatedAt:   createdAt,
		Metadata:    meta,
	}, nil
}

// CreateThread persists a new thread and starts its consensus engine.
func (m *Manager) CreateThread(_ context.Context, req *pb.CreateThreadRequest) (*pb.Thread, error) {
	thread, err := buildAndPersistThread(m.store, m.id, req)
	if err != nil {
		return nil, err
	}
	if err := m.Start(thread); err != nil {
		return nil, fmt.Errorf("start thread engine: %w", err)
	}
	return thread, nil
}

func (m *Manager) CreateThreadWithRecovery(ctx context.Context, req *pb.CreateThreadRequest) (*pb.Thread, *pb.ThreadRecoveryHandle, error) {
	return createThreadWithRecovery(ctx, req, m.store, m.CreateThread)
}

// StartAll loads all persisted threads and starts their engines.
// Call once on daemon startup after the Manager is created.
func (m *Manager) StartAll() error {
	threads, err := m.store.ListThreads()
	if err != nil {
		return fmt.Errorf("list threads: %w", err)
	}
	for _, t := range threads {
		if err := m.Start(t); err != nil {
			m.log.Warn("thread: start on boot", zap.String("thread", t.Id), zap.Error(err))
		}
	}
	return nil
}

// InviteReceived is called when a THREAD_INVITE message arrives from a peer.
// It saves the thread and starts its engine if not already running.
// selfDID returns this daemon's own DID, or empty when no identity is set.
func (m *Manager) selfDID() string {
	if m.id == nil {
		return ""
	}
	return m.id.DID
}

func (m *Manager) InviteReceived(thread *pb.Thread) error {
	if err := m.store.SaveThread(thread); err != nil {
		return fmt.Errorf("save thread: %w", err)
	}
	if err := recordSelfMembership(m.store, thread, m.selfDID()); err != nil {
		return fmt.Errorf("record membership: %w", err)
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

// ThreadSubscription is satisfied by *Engine; it is the interface rpc.Server
// depends on instead of the concrete *Engine type, so both the
// Manager/Raft path and the GoAkt actor path (ThreadActor, see actor.go)
// can implement the same thread-manager surface. Defined here (not just
// implicitly satisfied) so Engine returning it directly can express "no
// engine" as a true nil interface — a concrete (*Engine)(nil) boxed into an
// interface is not itself nil, which would break the "not a validator for
// this thread" check in rpc.Server.SubscribeThread.
type ThreadSubscription interface {
	Subscribe() <-chan *pb.ThreadEntryWithPos
	Unsubscribe(ch <-chan *pb.ThreadEntryWithPos)
}

// Engine returns the running Engine for a thread, or a true nil interface
// if none is running (not a validator for this thread, or thread unknown).
func (m *Manager) Engine(threadID string) ThreadSubscription {
	m.mu.Lock()
	defer m.mu.Unlock()
	eng, ok := m.engines[threadID]
	if !ok {
		return nil
	}
	return eng
}

// AppendEntry enqueues an entry for the next block proposal on this thread.
func (m *Manager) AppendEntry(threadID string, entry *pb.ThreadEntry) error {
	return m.store.EnqueueEntry(threadID, entry)
}

// ProposeVoterChange forwards a verified membership transition to the active
// Raft engine. Callers must first prove observer catch-up and wait for the
// committed ConfChangeV2 before updating the durable member role.
func (m *Manager) ProposeVoterChange(ctx context.Context, threadID, did string, add bool) error {
	m.mu.Lock()
	engine := m.engines[threadID]
	m.mu.Unlock()
	if engine == nil {
		return fmt.Errorf("thread engine %q is not active", threadID)
	}
	return engine.ProposeVoterChange(ctx, did, add)
}

// GetEntries returns committed entries since sinceHeight.
func (m *Manager) GetEntries(threadID string, sinceHeight int64, limit int) ([]*pb.ThreadEntryWithPos, error) {
	blocks, err := m.store.GetBlocksSince(threadID, sinceHeight, limit)
	if err != nil {
		return nil, err
	}
	return entriesFromBlocks(blocks), nil
}

// ─── shared by Manager and ActorManager (see actor_manager.go) ───────────────
//
// AuthorizeRecoveryCapability, ListMembers, MembershipEpoch, RemoveMember,
// RemoveMemberWithEpoch, PromoteMember, PromoteMemberWithEpoch, SaveInvite,
// AcceptInvite, AcceptInviteWithEpoch, SaveKeyEnvelope, KeyEnvelopes,
// SaveRecoveryKeyEnvelope, RecoveryKeyEnvelopes, GetThread, ImportHistory,
// and CommittedHead all reach rpc.ThreadManager/deliver.ThreadInviter
// callers via *Store's own methods, promoted through the embedded *Store on
// both Manager and ActorManager — neither type adds logic on top of them, so
// there is nothing to hand-write here.

// entriesFromBlocks flattens committed blocks into the per-entry view
// GetEntries returns — shared by Manager and ActorManager, which otherwise
// fetch blocks through different active-consensus paths but flatten them
// identically.
func entriesFromBlocks(blocks []*pb.ThreadBlock) []*pb.ThreadEntryWithPos {
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
	return out
}

// buildAndPersistThread runs the descriptor-building, signing, and
// persistence steps shared by Manager.CreateThread (Engine/GossipBridge
// path) and ActorManager.CreateThread (GoAkt actor path) — the two diverge
// only in how the thread is activated afterward.
func buildAndPersistThread(store *Store, id *identity.Identity, req *pb.CreateThreadRequest) (*pb.Thread, error) {
	var (
		thread *pb.Thread
		err    error
	)
	if req != nil && req.CreatorDid != "" {
		thread, err = NewThreadFromSignedRequest(req)
	} else {
		thread, err = NewThreadFromRequest(id.DID, req)
	}
	if err != nil {
		return nil, err
	}
	if req == nil || req.CreatorDid == "" {
		if err := SignDescriptor(thread, id); err != nil {
			return nil, err
		}
	}
	if err := store.SaveThread(thread); err != nil {
		return nil, fmt.Errorf("save thread: %w", err)
	}
	if err := store.SaveMember(thread.Id, &pb.ThreadMember{Did: thread.CreatorDid, Role: pb.ThreadMemberRole_THREAD_MEMBER_ROLE_ADMIN, JoinedEpoch: 1}); err != nil {
		return nil, fmt.Errorf("save creator membership: %w", err)
	}
	return thread, nil
}

// createThreadWithRecovery wraps createThread (either Manager's or
// ActorManager's) with the recovery-secret/commitment bookkeeping shared by
// both CreateThreadWithRecovery implementations.
func createThreadWithRecovery(
	ctx context.Context,
	req *pb.CreateThreadRequest,
	store *Store,
	createThread func(context.Context, *pb.CreateThreadRequest) (*pb.Thread, error),
) (*pb.Thread, *pb.ThreadRecoveryHandle, error) {
	if req == nil {
		req = &pb.CreateThreadRequest{}
	}
	request := proto.Clone(req).(*pb.CreateThreadRequest)
	secret := request.RecoverySecret
	if len(secret) == 0 {
		h, err := threadcrypto.NewRecoveryHandle("pending")
		if err != nil {
			return nil, nil, err
		}
		secret = h.Secret
		request.RecoverySecret = secret
	}
	commitment, err := threadcrypto.RecoveryCommitment(secret)
	if err != nil {
		return nil, nil, err
	}
	if request.CreatorDid != "" {
		// A client-signed descriptor must already commit to the supplied secret;
		// mutating its metadata here would invalidate that signature.
		if request.Metadata[threadcrypto.RecoveryCommitmentMetadataKey] != commitment {
			return nil, nil, fmt.Errorf("signed recovery thread must commit to its recovery secret")
		}
	} else {
		if request.Metadata == nil {
			request.Metadata = map[string]string{}
		}
		request.Metadata[threadcrypto.RecoveryCommitmentMetadataKey] = commitment
	}
	th, err := createThread(ctx, request)
	if err != nil {
		return nil, nil, err
	}
	if err := store.SaveRecoveryCapability(th.Id, secret); err != nil {
		return nil, nil, err
	}
	return th, &pb.ThreadRecoveryHandle{ThreadId: th.Id, RecoverySecret: secret, Version: 1}, nil
}
