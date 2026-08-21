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
	var (
		thread *pb.Thread
		err    error
	)
	if req != nil && req.CreatorDid != "" {
		thread, err = NewThreadFromSignedRequest(req)
	} else {
		thread, err = NewThreadFromRequest(m.id.DID, req)
	}
	if err != nil {
		return nil, err
	}
	if req == nil || req.CreatorDid == "" {
		if err := SignDescriptor(thread, m.id); err != nil {
			return nil, err
		}
	}

	if err := m.store.SaveThread(thread); err != nil {
		return nil, fmt.Errorf("save thread: %w", err)
	}
	if err := m.store.SaveMember(thread.Id, &pb.ThreadMember{Did: thread.CreatorDid, Role: pb.ThreadMemberRole_THREAD_MEMBER_ROLE_ADMIN, JoinedEpoch: 1}); err != nil {
		return nil, fmt.Errorf("save creator membership: %w", err)
	}

	if err := m.Start(thread); err != nil {
		return nil, fmt.Errorf("start thread engine: %w", err)
	}

	return thread, nil
}

func (m *Manager) AuthorizeRecoveryCapability(threadID string, secret []byte) (bool, error) {
	return m.store.AuthorizeRecoveryCapability(threadID, secret)
}
func (m *Manager) ListMembers(threadID string) ([]*pb.ThreadMember, error) {
	return m.store.ListMembers(threadID)
}
func (m *Manager) MembershipEpoch(threadID string) (uint64, error) {
	return m.store.MembershipEpoch(threadID)
}
func (m *Manager) RemoveMember(threadID, did string) error {
	return m.store.RemoveMember(threadID, did)
}
func (m *Manager) RemoveMemberWithEpoch(threadID, did string) (uint64, error) {
	return m.store.RemoveMemberWithEpoch(threadID, did)
}
func (m *Manager) PromoteMember(threadID, did string) (*pb.ThreadMember, error) {
	return m.store.PromoteMember(threadID, did)
}
func (m *Manager) PromoteMemberWithEpoch(threadID, did string) (*pb.ThreadMember, uint64, error) {
	return m.store.PromoteMemberWithEpoch(threadID, did)
}
func (m *Manager) SaveInvite(threadID, invitee string, role pb.ThreadMemberRole, nonce []byte, expiresAt int64) error {
	return m.store.SaveInvite(threadID, invitee, role, nonce, expiresAt)
}
func (m *Manager) AcceptInvite(threadID, invitee string, nonce []byte) (*pb.ThreadMember, error) {
	return m.store.AcceptInvite(threadID, invitee, nonce)
}
func (m *Manager) AcceptInviteWithEpoch(threadID, invitee string, nonce []byte) (*pb.ThreadMember, uint64, error) {
	return m.store.AcceptInviteWithEpoch(threadID, invitee, nonce)
}
func (m *Manager) SaveKeyEnvelope(envelope *pb.ThreadKeyEnvelope) error {
	return m.store.SaveKeyEnvelope(envelope)
}
func (m *Manager) KeyEnvelopes(threadID string, epoch uint64, recipient string) ([]*pb.ThreadKeyEnvelope, error) {
	return m.store.KeyEnvelopes(threadID, epoch, recipient)
}
func (m *Manager) SaveRecoveryKeyEnvelope(envelope *pb.ThreadKeyEnvelope) error {
	return m.store.SaveRecoveryKeyEnvelope(envelope)
}
func (m *Manager) RecoveryKeyEnvelopes(threadID string) ([]*pb.ThreadKeyEnvelope, error) {
	return m.store.RecoveryKeyEnvelopes(threadID)
}

func (m *Manager) CreateThreadWithRecovery(ctx context.Context, req *pb.CreateThreadRequest) (*pb.Thread, *pb.ThreadRecoveryHandle, error) {
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
	th, err := m.CreateThread(ctx, request)
	if err != nil {
		return nil, nil, err
	}
	if err := m.store.SaveRecoveryCapability(th.Id, secret); err != nil {
		return nil, nil, err
	}
	return th, &pb.ThreadRecoveryHandle{ThreadId: th.Id, RecoverySecret: secret, Version: 1}, nil
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

// GetThread loads a thread from the store.
func (m *Manager) GetThread(threadID string) (*pb.Thread, error) {
	return m.store.GetThread(threadID)
}

func (m *Manager) ImportHistory(th *pb.Thread, blocks []*pb.ThreadBlock) error {
	return m.store.ImportHistory(th, blocks)
}
func (m *Manager) CommittedHead(threadID string) (int64, string, error) {
	return m.store.CommittedHead(threadID)
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
