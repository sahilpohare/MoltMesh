package thread

import (
	"context"
	"fmt"
	"time"

	pubsub "github.com/libp2p/go-libp2p-pubsub"
	"github.com/tochemey/goakt/v4/actor"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"

	"github.com/sahilpohare/p2p-a2a/daemon/actors"
	"github.com/sahilpohare/p2p-a2a/daemon/identity"
	pb "github.com/sahilpohare/p2p-a2a/gen/a2a/v1"
	"github.com/sahilpohare/p2p-a2a/pkg/threadcrypto"
)

// ActorManager is the GoAkt-actor-path counterpart to Manager: it
// implements the same surface (rpc.ThreadManager, deliver.ThreadInviter)
// backed by ThreadSupervisor/ThreadActor instead of Engine/GossipBridge and
// RaftBackend.Run. It is the "wired in behind the existing Manager
// interface" adapter from docs/PROPOSAL-goakt-thread-actor.md's sequencing
// step 2.
type ActorManager struct {
	sup   *ThreadSupervisor
	store *Store
	id    *identity.Identity
	log   *zap.Logger
}

func (m *ActorManager) UseHierarchy(h *actors.Hierarchy) error { return m.sup.UseHierarchy(h) }

func (m *ActorManager) SetPassivationAfter(after time.Duration) {
	if after > 0 {
		m.sup.passivateAfter = after
	}
}

// NewActorManager creates an ActorManager. ctx is the daemon lifecycle
// context, matching NewManager's ctx parameter — ThreadActors and their
// GossipSub bridges run until it is cancelled. ps may be nil to disable
// GossipSub fan-out (single-node-only deployments/tests). durability may be
// nil to disable Bitswap/DHT durability publishing (see publisher.go).
func NewActorManager(
	ctx context.Context,
	system *actors.System,
	store *Store,
	id *identity.Identity,
	ps *pubsub.PubSub,
	durability *Publisher,
	log *zap.Logger,
) *ActorManager {
	return &ActorManager{
		sup:   NewThreadSupervisor(ctx, system, store, id, ps, durability, log),
		store: store,
		id:    id,
		log:   log,
	}
}

// CreateThread persists a new thread and spawns its ThreadActor.
func (m *ActorManager) CreateThread(ctx context.Context, req *pb.CreateThreadRequest) (*pb.Thread, error) {
	var (
		th  *pb.Thread
		err error
	)
	if req != nil && req.CreatorDid != "" {
		th, err = NewThreadFromSignedRequest(req)
	} else {
		th, err = NewThreadFromRequest(m.id.DID, req)
	}
	if err != nil {
		return nil, err
	}
	if req == nil || req.CreatorDid == "" {
		if err := SignDescriptor(th, m.id); err != nil {
			return nil, err
		}
	}
	if err := m.store.SaveThread(th); err != nil {
		return nil, fmt.Errorf("save thread: %w", err)
	}
	if err := m.store.SaveMember(th.Id, &pb.ThreadMember{Did: th.CreatorDid, Role: pb.ThreadMemberRole_THREAD_MEMBER_ROLE_ADMIN, JoinedEpoch: 1}); err != nil {
		return nil, fmt.Errorf("save creator membership: %w", err)
	}
	if _, err := m.sup.Spawn(ctx, th); err != nil {
		return nil, fmt.Errorf("spawn thread actor: %w", err)
	}
	return th, nil
}

// StartAll intentionally does not materialize persisted threads. With hundreds
// of thousands of durable threads, startup must be O(1) in thread count;
// actors are activated on append, subscription, invite, or inbound activity.
func (m *ActorManager) StartAll(ctx context.Context) error {
	return nil
}

// InviteReceived is called when a THREAD_INVITE message arrives from a
// peer. It saves the thread and spawns its ThreadActor if not already
// running. Satisfies deliver.ThreadInviter.
func (m *ActorManager) InviteReceived(th *pb.Thread) error {
	if err := m.store.SaveThread(th); err != nil {
		return fmt.Errorf("save thread: %w", err)
	}
	_, err := m.sup.Spawn(context.Background(), th)
	return err
}

// GetThread loads a thread from the store.
func (m *ActorManager) GetThread(threadID string) (*pb.Thread, error) {
	return m.store.GetThread(threadID)
}

func (m *ActorManager) AuthorizeRecoveryCapability(threadID string, secret []byte) (bool, error) {
	return m.store.AuthorizeRecoveryCapability(threadID, secret)
}
func (m *ActorManager) ListMembers(threadID string) ([]*pb.ThreadMember, error) {
	return m.store.ListMembers(threadID)
}
func (m *ActorManager) MembershipEpoch(threadID string) (uint64, error) {
	return m.store.MembershipEpoch(threadID)
}
func (m *ActorManager) RemoveMember(threadID, did string) error {
	return m.store.RemoveMember(threadID, did)
}
func (m *ActorManager) RemoveMemberWithEpoch(threadID, did string) (uint64, error) {
	return m.store.RemoveMemberWithEpoch(threadID, did)
}
func (m *ActorManager) PromoteMember(threadID, did string) (*pb.ThreadMember, error) {
	return m.store.PromoteMember(threadID, did)
}
func (m *ActorManager) PromoteMemberWithEpoch(threadID, did string) (*pb.ThreadMember, uint64, error) {
	return m.store.PromoteMemberWithEpoch(threadID, did)
}
func (m *ActorManager) SaveInvite(threadID, invitee string, role pb.ThreadMemberRole, nonce []byte, expiresAt int64) error {
	return m.store.SaveInvite(threadID, invitee, role, nonce, expiresAt)
}
func (m *ActorManager) AcceptInvite(threadID, invitee string, nonce []byte) (*pb.ThreadMember, error) {
	return m.store.AcceptInvite(threadID, invitee, nonce)
}
func (m *ActorManager) AcceptInviteWithEpoch(threadID, invitee string, nonce []byte) (*pb.ThreadMember, uint64, error) {
	return m.store.AcceptInviteWithEpoch(threadID, invitee, nonce)
}
func (m *ActorManager) SaveKeyEnvelope(envelope *pb.ThreadKeyEnvelope) error {
	return m.store.SaveKeyEnvelope(envelope)
}
func (m *ActorManager) KeyEnvelopes(threadID string, epoch uint64, recipient string) ([]*pb.ThreadKeyEnvelope, error) {
	return m.store.KeyEnvelopes(threadID, epoch, recipient)
}
func (m *ActorManager) SaveRecoveryKeyEnvelope(envelope *pb.ThreadKeyEnvelope) error {
	return m.store.SaveRecoveryKeyEnvelope(envelope)
}
func (m *ActorManager) RecoveryKeyEnvelopes(threadID string) ([]*pb.ThreadKeyEnvelope, error) {
	return m.store.RecoveryKeyEnvelopes(threadID)
}

func (m *ActorManager) CreateThreadWithRecovery(ctx context.Context, req *pb.CreateThreadRequest) (*pb.Thread, *pb.ThreadRecoveryHandle, error) {
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

// AppendEntry enqueues an entry for the next block proposal on this thread.
func (m *ActorManager) AppendEntry(threadID string, entry *pb.ThreadEntry) error {
	if err := m.store.EnqueueEntry(threadID, entry); err != nil {
		return err
	}
	pid, err := m.activate(context.Background(), threadID)
	if err != nil {
		return fmt.Errorf("activate thread %s (entry remains durable): %w", threadID, err)
	}
	return m.sup.system.ActorSystem().NoSender().Tell(context.Background(), pid, ActivateMsg{})
}

// ProposeVoterChange starts a joint-consensus transition inside the owning
// actor, then waits outside Receive for its committed ConfChangeV2 result.
func (m *ActorManager) ProposeVoterChange(ctx context.Context, threadID, did string, add bool) error {
	pid, err := m.activate(ctx, threadID)
	if err != nil {
		return err
	}
	response, err := pid.Ask(ctx, pid, ProposeVoterChangeMsg{DID: did, Add: add}, 5*time.Second)
	if err != nil {
		return err
	}
	reply, ok := response.(*VoterChangeReply)
	if !ok || reply == nil {
		return fmt.Errorf("invalid voter change response")
	}
	if reply.Err != nil {
		return reply.Err
	}
	select {
	case err := <-reply.Done:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

// GetEntries returns committed entries since sinceHeight.
func (m *ActorManager) GetEntries(threadID string, sinceHeight int64, limit int) ([]*pb.ThreadEntryWithPos, error) {
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

// ImportHistory installs verified read-only history. It does not activate a
// consensus actor when this DID is not a replica.
func (m *ActorManager) ImportHistory(th *pb.Thread, blocks []*pb.ThreadBlock) error {
	return m.store.ImportHistory(th, blocks)
}
func (m *ActorManager) CommittedHead(threadID string) (int64, string, error) {
	return m.store.CommittedHead(threadID)
}

// Engine returns the running ThreadActor for a thread as a
// ThreadSubscription, or a true nil interface if none is running. Named
// Engine (not e.g. ThreadActorFor) to match Manager's method name — both
// satisfy the same rpc.ThreadManager interface.
func (m *ActorManager) Engine(threadID string) ThreadSubscription {
	pid, err := m.activate(context.Background(), threadID)
	if err != nil {
		m.log.Warn("thread: activate subscription", zap.String("thread", threadID), zap.Error(err))
		return nil
	}
	ta, ok := pid.Actor().(*ThreadActor)
	if !ok {
		return nil
	}
	return ta
}

func (m *ActorManager) activate(ctx context.Context, threadID string) (*actor.PID, error) {
	if pid := m.sup.PID(threadID); pid != nil {
		return pid, nil
	}
	th, err := m.store.GetThread(threadID)
	if err != nil {
		return nil, err
	}
	return m.sup.Spawn(ctx, th)
}
