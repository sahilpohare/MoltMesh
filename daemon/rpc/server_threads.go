package rpc

import (
	"context"
	"errors"
	"fmt"
	"time"

	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"

	"github.com/google/uuid"
	appactors "github.com/sahilpohare/p2p-a2a/daemon/actors"
	"github.com/sahilpohare/p2p-a2a/daemon/identity"
	"github.com/sahilpohare/p2p-a2a/daemon/thread"
	pb "github.com/sahilpohare/p2p-a2a/gen/a2a/v1"
	"github.com/sahilpohare/p2p-a2a/pkg/threadcrypto"
)

func (s *Server) CreateThread(ctx context.Context, req *pb.CreateThreadRequest) (*pb.Thread, error) {
	if s.threads == nil {
		return nil, fmt.Errorf("thread manager not available")
	}
	owner, err := s.scopedOwner(ctx)
	if err != nil {
		return nil, err
	}
	if owner != "" {
		if req == nil || req.CreatorDid != owner {
			return nil, fmt.Errorf("signed thread creator must match authenticated agent")
		}
	} else if req != nil && req.CreatorDid != "" {
		return nil, fmt.Errorf("signed thread creation requires an agent session")
	}
	thread, err := s.threads.CreateThread(ctx, req)
	if err != nil {
		return nil, err
	}

	if err := s.enqueueThreadWake(thread); err != nil {
		s.log.Warn("enqueue initial thread wake", zap.Error(err))
	}
	return thread, nil
}

func (s *Server) GetThread(_ context.Context, req *pb.ThreadID) (*pb.Thread, error) {
	if s.threads == nil {
		return nil, fmt.Errorf("thread manager not available")
	}
	return s.threads.GetThread(req.Id)
}

// AddThreadReplica commits a late participant as a non-voting observer. This
// expands replication/read access without pretending that an unsafe in-place
// Raft voter-set rewrite is joint consensus.
func (s *Server) AddThreadReplica(ctx context.Context, req *pb.ThreadReplicaRequest) (*pb.Thread, error) {
	if req == nil || req.ThreadId == "" || req.ReplicaDid == "" {
		return nil, fmt.Errorf("thread_id and replica_did are required")
	}
	th, err := s.threads.GetThread(req.ThreadId)
	if err != nil {
		return nil, err
	}
	owner, err := s.scopedOwner(ctx)
	if err != nil {
		return nil, err
	}
	creator := s.id.DID
	if owner != "" {
		creator = owner
	}
	if th.CreatorDid != creator {
		return nil, fmt.Errorf("only the thread creator may add an observer")
	}
	for _, did := range th.ReplicaDids {
		if did == req.ReplicaDid {
			return th, nil
		}
	}
	entry := &pb.ThreadEntry{AuthorDid: creator, Payload: []byte(req.ReplicaDid), Kind: "membership:add-observer", SubmittedAt: time.Now().UnixMilli()}
	if err := s.threads.AppendEntry(th.Id, entry); err != nil {
		return nil, err
	}
	// The log entry alone does not populate thread_members, which ListMembers
	// reads, so record the observer locally too. Otherwise the creator cannot
	// later promote the very replica it just added.
	if saver, ok := s.threads.(threadMemberSaver); ok {
		epoch := uint64(1)
		if r, ok := s.threads.(threadMembershipEpochReader); ok {
			if e, err := r.MembershipEpoch(th.Id); err == nil && e > 0 {
				epoch = e
			}
		}
		if err := saver.SaveMember(th.Id, &pb.ThreadMember{
			Did:         req.ReplicaDid,
			Role:        pb.ThreadMemberRole_THREAD_MEMBER_ROLE_OBSERVER,
			JoinedEpoch: epoch,
		}); err != nil {
			return nil, fmt.Errorf("record observer membership: %w", err)
		}
	}
	ctx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	deadline := time.NewTicker(50 * time.Millisecond)
	defer deadline.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-deadline.C:
			updated, getErr := s.threads.GetThread(th.Id)
			if getErr != nil {
				return nil, getErr
			}
			for _, did := range updated.ReplicaDids {
				if did == req.ReplicaDid {
					if err := s.enqueueThreadWake(updated); err != nil {
						return nil, err
					}
					return updated, nil
				}
			}
		}
	}
}

type threadHistoryImporter interface {
	ImportHistory(*pb.Thread, []*pb.ThreadBlock) error
}

type threadRecoveryAuthorizer interface {
	AuthorizeRecoveryCapability(threadID string, secret []byte) (bool, error)
}

type threadRecoveryCreator interface {
	CreateThreadWithRecovery(context.Context, *pb.CreateThreadRequest) (*pb.Thread, *pb.ThreadRecoveryHandle, error)
}

type threadMemberLister interface {
	ListMembers(string) ([]*pb.ThreadMember, error)
}

type threadMemberSaver interface {
	SaveMember(string, *pb.ThreadMember) error
}

type threadMembershipEpochReader interface {
	MembershipEpoch(string) (uint64, error)
}

type threadKeyEnvelopeStore interface {
	SaveKeyEnvelope(*pb.ThreadKeyEnvelope) error
	KeyEnvelopes(string, uint64, string) ([]*pb.ThreadKeyEnvelope, error)
}

type threadRecoveryKeyEnvelopeStore interface {
	SaveRecoveryKeyEnvelope(*pb.ThreadKeyEnvelope) error
	RecoveryKeyEnvelopes(string) ([]*pb.ThreadKeyEnvelope, error)
}

type threadMemberRemover interface{ RemoveMember(string, string) error }

type threadMemberPromoter interface {
	PromoteMember(string, string) (*pb.ThreadMember, error)
}

type threadInviteSaver interface {
	SaveInvite(string, string, pb.ThreadMemberRole, []byte, int64) error
}

type threadInviteAccepter interface {
	AcceptInvite(string, string, []byte) (*pb.ThreadMember, error)
}

type threadInviteAccepterWithEpoch interface {
	AcceptInviteWithEpoch(string, string, []byte) (*pb.ThreadMember, uint64, error)
}

type threadMemberRemoverWithEpoch interface {
	RemoveMemberWithEpoch(string, string) (uint64, error)
}

type threadMemberPromoterWithEpoch interface {
	PromoteMemberWithEpoch(string, string) (*pb.ThreadMember, uint64, error)
}

type threadVoterChanger interface {
	ProposeVoterChange(context.Context, string, string, bool) error
}

type threadCommittedHeadReader interface {
	CommittedHead(string) (int64, string, error)
}

func (s *Server) InviteThreadMember(ctx context.Context, req *pb.InviteThreadMemberRequest) (*pb.ThreadMembershipChange, error) {
	if req == nil || req.ThreadId == "" || req.InviteeDid == "" || len(req.Nonce) == 0 {
		return nil, fmt.Errorf("invalid thread invite")
	}
	if req.Role != pb.ThreadMemberRole_THREAD_MEMBER_ROLE_OBSERVER {
		return nil, fmt.Errorf("new members must start as observers")
	}
	th, err := s.threads.GetThread(req.ThreadId)
	if err != nil {
		return nil, err
	}
	owner, err := s.scopedOwner(ctx)
	if err != nil {
		return nil, err
	}
	if owner != "" && owner != th.CreatorDid {
		return nil, fmt.Errorf("only thread creator may invite members")
	}
	saver, ok := s.threads.(threadInviteSaver)
	if !ok {
		return nil, fmt.Errorf("thread manager cannot save invites")
	}
	inviter := th.CreatorDid
	if owner != "" {
		inviter = owner
	}
	invite := &pb.ThreadInvitation{ThreadId: req.ThreadId, InviterDid: inviter, InviteeDid: req.InviteeDid, Role: req.Role, ExpiresAtUnixMs: req.ExpiresAtUnixMs, Nonce: req.Nonce}
	unsigned, err := proto.MarshalOptions{Deterministic: true}.Marshal(invite)
	if err != nil {
		return nil, err
	}
	if owner != "" {
		ok, err := identity.VerifyDID(owner, unsigned, req.Signature)
		if err != nil || !ok {
			return nil, fmt.Errorf("invalid invite signature")
		}
		invite.Signature = req.Signature
	} else {
		invite.Signature = s.id.Sign(unsigned)
	}
	if err := saver.SaveInvite(req.ThreadId, req.InviteeDid, req.Role, req.Nonce, req.ExpiresAtUnixMs); err != nil {
		return nil, err
	}
	wire, err := proto.Marshal(invite)
	if err != nil {
		return nil, err
	}
	return &pb.ThreadMembershipChange{ThreadId: req.ThreadId, Member: &pb.ThreadMember{Did: req.InviteeDid, Role: req.Role}, SignedInvitation: wire}, nil
}

func (s *Server) AcceptThreadInvite(ctx context.Context, req *pb.AcceptThreadInviteRequest) (*pb.Thread, error) {
	if req == nil || req.ThreadId == "" {
		return nil, fmt.Errorf("thread id required")
	}
	var invite pb.ThreadInvitation
	if err := proto.Unmarshal(req.Invite, &invite); err != nil {
		return nil, err
	}
	if invite.ThreadId != req.ThreadId || invite.ExpiresAtUnixMs <= time.Now().UnixMilli() {
		return nil, fmt.Errorf("invalid or expired invite")
	}
	sig := invite.Signature
	invite.Signature = nil
	unsigned, err := proto.MarshalOptions{Deterministic: true}.Marshal(&invite)
	if err != nil {
		return nil, err
	}
	ok, err := identity.VerifyDID(invite.InviterDid, unsigned, sig)
	if err != nil || !ok {
		return nil, fmt.Errorf("invalid invite signature")
	}
	invitee, err := s.agentDID(ctx)
	if err != nil {
		return nil, err
	}
	if invitee != invite.InviteeDid {
		return nil, fmt.Errorf("invitee does not match session")
	}
	accepter, ok := s.threads.(threadInviteAccepter)
	if !ok {
		return nil, fmt.Errorf("thread manager cannot accept invites")
	}
	if epochAccepter, ok := s.threads.(threadInviteAccepterWithEpoch); ok {
		if _, _, err = epochAccepter.AcceptInviteWithEpoch(req.ThreadId, invitee, invite.Nonce); err != nil {
			return nil, err
		}
	} else if _, err = accepter.AcceptInvite(req.ThreadId, invitee, invite.Nonce); err != nil {
		return nil, err
	}
	return s.threads.GetThread(req.ThreadId)
}

func (s *Server) RemoveThreadMember(ctx context.Context, req *pb.RemoveThreadMemberRequest) (*pb.ThreadMembershipChange, error) {
	if req == nil || req.ThreadId == "" || req.MemberDid == "" {
		return nil, fmt.Errorf("thread_id and member_did are required")
	}
	th, err := s.threads.GetThread(req.ThreadId)
	if err != nil {
		return nil, err
	}
	owner, err := s.scopedOwner(ctx)
	if err != nil {
		return nil, err
	}
	if owner != "" && owner != th.CreatorDid {
		return nil, fmt.Errorf("only thread creator may remove members")
	}
	lister, ok := s.threads.(threadMemberLister)
	if !ok {
		return nil, fmt.Errorf("thread manager cannot verify member removal")
	}
	members, listErr := lister.ListMembers(req.ThreadId)
	if listErr != nil {
		return nil, listErr
	}
	for _, member := range members {
		if member.Did == req.MemberDid && (member.Role == pb.ThreadMemberRole_THREAD_MEMBER_ROLE_VOTER || member.Role == pb.ThreadMemberRole_THREAD_MEMBER_ROLE_ADMIN) {
			changer, ok := s.threads.(threadVoterChanger)
			if !ok {
				return nil, fmt.Errorf("thread manager cannot commit voter removal")
			}
			if err := changer.ProposeVoterChange(ctx, req.ThreadId, req.MemberDid, false); err != nil {
				return nil, fmt.Errorf("commit voter removal: %w", err)
			}
			break
		}
	}
	remover, ok := s.threads.(threadMemberRemover)
	if !ok {
		return nil, fmt.Errorf("thread manager cannot remove members")
	}
	var epoch uint64
	if epochRemover, ok := s.threads.(threadMemberRemoverWithEpoch); ok {
		epoch, err = epochRemover.RemoveMemberWithEpoch(req.ThreadId, req.MemberDid)
	} else {
		err = remover.RemoveMember(req.ThreadId, req.MemberDid)
	}
	if err != nil {
		return nil, err
	}
	return &pb.ThreadMembershipChange{ThreadId: req.ThreadId, MembershipEpoch: epoch, Member: &pb.ThreadMember{Did: req.MemberDid}}, nil
}

func (s *Server) PromoteThreadMember(ctx context.Context, req *pb.PromoteThreadMemberRequest) (*pb.ThreadMembershipChange, error) {
	if req == nil || req.ThreadId == "" || req.MemberDid == "" || len(req.CatchupProof) == 0 {
		return nil, fmt.Errorf("thread_id, member_did, and catchup_proof are required")
	}
	th, err := s.threads.GetThread(req.ThreadId)
	if err != nil {
		return nil, err
	}
	owner, err := s.scopedOwner(ctx)
	if err != nil {
		return nil, err
	}
	if owner != "" && owner != th.CreatorDid {
		return nil, fmt.Errorf("only thread creator may promote members")
	}
	lister, ok := s.threads.(threadMemberLister)
	if !ok {
		return nil, fmt.Errorf("thread manager cannot verify observer promotion")
	}
	members, err := lister.ListMembers(req.ThreadId)
	if err != nil {
		return nil, err
	}
	isObserver := false
	for _, member := range members {
		if member.Did == req.MemberDid && member.Role == pb.ThreadMemberRole_THREAD_MEMBER_ROLE_OBSERVER {
			isObserver = true
			break
		}
	}
	if !isObserver {
		return nil, fmt.Errorf("only an existing observer may be promoted")
	}
	if err := s.verifyCatchupProof(req.ThreadId, req.MemberDid, req.CatchupProof); err != nil {
		return nil, err
	}
	// Promotion is not a local table update: the observer must already be a
	// known replica and its voter role becomes durable only after Raft commits
	// the matching ConfChangeV2.
	changer, ok := s.threads.(threadVoterChanger)
	if !ok {
		return nil, fmt.Errorf("thread manager cannot commit voter promotion")
	}
	if err := changer.ProposeVoterChange(ctx, req.ThreadId, req.MemberDid, true); err != nil {
		return nil, fmt.Errorf("commit voter promotion: %w", err)
	}
	// The committed voter count is recorded in the descriptor's N, and a
	// replica bootstraps its raft voter set from the first N ReplicaDids. The
	// promoted node already holds the pre-promotion descriptor, so re-send it;
	// otherwise it comes up with the old N, reports voters=(1) while the
	// leader is at voters=(1 2), and its writes never commit.
	if updated, gErr := s.threads.GetThread(req.ThreadId); gErr == nil {
		if wErr := s.enqueueThreadWake(updated); wErr != nil {
			return nil, fmt.Errorf("re-send descriptor after promotion: %w", wErr)
		}
	}
	p, ok := s.threads.(threadMemberPromoter)
	if !ok {
		return nil, fmt.Errorf("thread manager cannot promote members")
	}
	var m *pb.ThreadMember
	var epoch uint64
	if epochPromoter, ok := s.threads.(threadMemberPromoterWithEpoch); ok {
		m, epoch, err = epochPromoter.PromoteMemberWithEpoch(req.ThreadId, req.MemberDid)
	} else {
		m, err = p.PromoteMember(req.ThreadId, req.MemberDid)
	}
	if err != nil {
		return nil, err
	}
	return &pb.ThreadMembershipChange{ThreadId: req.ThreadId, MembershipEpoch: epoch, Member: m}, nil
}

// GetThreadCatchupState returns this authenticated member's locally committed
// history point. The SDK signs this exact state when requesting promotion from
// the creator; no unsigned claim is accepted by PromoteThreadMember.
func (s *Server) GetThreadCatchupState(ctx context.Context, req *pb.ThreadID) (*pb.ThreadCatchupState, error) {
	if req == nil || req.Id == "" {
		return nil, fmt.Errorf("thread id required")
	}
	// scopedOwner, not agentDID: with no session token and a single agent on
	// this daemon, the caller is the daemon's own identity. This is the same
	// legacy single-agent mode the rest of the thread RPCs accept, and it lets
	// the CLI request its own catchup state without an SDK session.
	observer, err := s.scopedOwner(ctx)
	if err != nil {
		return nil, err
	}
	if observer == "" {
		observer = s.id.DID
	}
	lister, ok := s.threads.(threadMemberLister)
	if !ok {
		return nil, fmt.Errorf("thread manager cannot authorize catchup state")
	}
	members, err := lister.ListMembers(req.Id)
	if err != nil {
		return nil, err
	}
	found := false
	for _, member := range members {
		if member.Did == observer {
			found = true
			break
		}
	}
	if !found {
		return nil, fmt.Errorf("catchup state requires current thread membership")
	}
	head, ok := s.threads.(threadCommittedHeadReader)
	if !ok {
		return nil, fmt.Errorf("thread manager cannot read committed head")
	}
	height, hash, err := head.CommittedHead(req.Id)
	if err != nil {
		return nil, err
	}
	return &pb.ThreadCatchupState{ThreadId: req.Id, ObserverDid: observer, CommittedHeight: height, HeadBlockHash: hash}, nil
}

func (s *Server) verifyCatchupProof(threadID, observerDID string, raw []byte) error {
	var proof pb.ThreadCatchupProof
	if err := proto.Unmarshal(raw, &proof); err != nil {
		return fmt.Errorf("unmarshal catchup proof: %w", err)
	}
	if proof.ThreadId != threadID || proof.ObserverDid != observerDID || proof.IssuedAtUnixMs <= 0 || len(proof.Signature) == 0 {
		return fmt.Errorf("invalid catchup proof")
	}
	if age := time.Since(time.UnixMilli(proof.IssuedAtUnixMs)); age < -time.Minute || age > time.Minute {
		return fmt.Errorf("catchup proof is expired")
	}
	signature := proof.Signature
	proof.Signature = nil
	data, err := proto.MarshalOptions{Deterministic: true}.Marshal(&proof)
	if err != nil {
		return err
	}
	ok, err := identity.VerifyDID(observerDID, data, signature)
	if err != nil || !ok {
		return fmt.Errorf("invalid catchup proof signature")
	}
	head, ok := s.threads.(threadCommittedHeadReader)
	if !ok {
		return fmt.Errorf("thread manager cannot verify committed head")
	}
	height, hash, err := head.CommittedHead(threadID)
	if err != nil {
		return err
	}
	if proof.CommittedHeight != height || proof.HeadBlockHash != hash {
		return fmt.Errorf("observer catchup proof does not match current committed head")
	}
	return nil
}

func (s *Server) LeaveThread(ctx context.Context, req *pb.ThreadID) (*pb.ThreadMembershipChange, error) {
	if req == nil || req.Id == "" {
		return nil, fmt.Errorf("thread id required")
	}
	owner, err := s.agentDID(ctx)
	if err != nil {
		return nil, err
	}
	r, ok := s.threads.(threadMemberRemover)
	if !ok {
		return nil, fmt.Errorf("thread manager cannot remove members")
	}
	var epoch uint64
	if epochRemover, ok := s.threads.(threadMemberRemoverWithEpoch); ok {
		epoch, err = epochRemover.RemoveMemberWithEpoch(req.Id, owner)
	} else {
		err = r.RemoveMember(req.Id, owner)
	}
	if err != nil {
		return nil, err
	}
	return &pb.ThreadMembershipChange{ThreadId: req.Id, MembershipEpoch: epoch, Member: &pb.ThreadMember{Did: owner}}, nil
}

func (s *Server) ListThreadMembers(_ context.Context, req *pb.ThreadID) (*pb.ThreadMembers, error) {
	if req == nil || req.Id == "" {
		return nil, fmt.Errorf("thread id is required")
	}
	lister, ok := s.threads.(threadMemberLister)
	if !ok {
		return nil, fmt.Errorf("thread manager cannot list members")
	}
	members, err := lister.ListMembers(req.Id)
	if err != nil {
		return nil, err
	}
	return &pb.ThreadMembers{Members: members}, nil
}

func (s *Server) CreateThreadWithRecovery(ctx context.Context, req *pb.CreateThreadRequest) (*pb.CreateThreadResponse, error) {
	owner, err := s.scopedOwner(ctx)
	if err != nil {
		return nil, err
	}
	if owner != "" && (req == nil || req.CreatorDid != owner) {
		return nil, fmt.Errorf("signed thread creator must match authenticated agent")
	}
	creator, ok := s.threads.(threadRecoveryCreator)
	if !ok {
		return nil, fmt.Errorf("thread manager cannot create recovery capabilities")
	}
	th, handle, err := creator.CreateThreadWithRecovery(ctx, req)
	if err != nil {
		return nil, err
	}
	if err := s.enqueueThreadWake(th); err != nil {
		s.log.Warn("enqueue initial thread wake", zap.Error(err))
	}
	return &pb.CreateThreadResponse{Thread: th, RecoveryHandle: handle}, nil
}

// RecoverThreadWithHandle requires the complete bearer capability before
// importing history. The imported thread is read-only: capability possession
// never adds a Raft/member role.
func (s *Server) RecoverThreadWithHandle(ctx context.Context, req *pb.RecoverThreadRequest) (*pb.RecoverThreadResponse, error) {
	if req == nil || req.Handle == nil || req.Handle.ThreadId == "" || req.Handle.Version != 1 || len(req.Handle.RecoverySecret) != 32 {
		return nil, fmt.Errorf("valid recovery handle required")
	}
	allowed, err := s.authorizeRecoveryHandle(ctx, req.Handle)
	if err != nil {
		return nil, err
	}
	if !allowed {
		return nil, fmt.Errorf("recovery capability is invalid")
	}
	th, err := s.recoverThreadHistory(ctx, req.Handle.ThreadId)
	if err != nil {
		return nil, err
	}
	return &pb.RecoverThreadResponse{Thread: th, Access: pb.ThreadAccess_THREAD_ACCESS_READ_ONLY}, nil
}

// authorizeRecoveryHandle first uses a locally retained capability hash. A
// newly provisioned recovery daemon has no local database yet, however, so it
// can also validate the secret against the creator-signed descriptor's public
// SHA-256 commitment before importing the chain. This makes recovery portable
// without turning a public thread ID into a read capability.
func (s *Server) authorizeRecoveryHandle(ctx context.Context, handle *pb.ThreadRecoveryHandle) (bool, error) {
	authorizer, ok := s.threads.(threadRecoveryAuthorizer)
	if !ok {
		return false, fmt.Errorf("thread manager cannot authorize recovery capabilities")
	}
	allowed, err := authorizer.AuthorizeRecoveryCapability(handle.ThreadId, handle.RecoverySecret)
	if err != nil || allowed {
		return allowed, err
	}
	if s.node == nil || s.node.DHT == nil {
		return false, nil
	}
	head, err := thread.ResolveHead(ctx, s.node.DHT, handle.ThreadId)
	if err != nil {
		return false, nil // absence is authorization failure, not a DHT oracle
	}
	if head.Thread == nil || head.Thread.Id != handle.ThreadId || thread.VerifyDescriptor(head.Thread) != nil {
		return false, nil
	}
	return threadcrypto.MatchesRecoveryCommitment(handle.RecoverySecret,
		head.Thread.Metadata[threadcrypto.RecoveryCommitmentMetadataKey]), nil
}

// RecoverThread is retained in the wire protocol solely to provide a clear
// migration error. A thread ID is public discovery metadata, not a read
// capability; recovery requires RecoverThreadWithHandle.
func (s *Server) RecoverThread(ctx context.Context, req *pb.ThreadID) (*pb.Thread, error) {
	_ = ctx
	_ = req
	return nil, fmt.Errorf("bare-ID recovery is retired; use RecoverThreadWithHandle")
}

// recoverThreadHistory resolves and cryptographically verifies
// content-addressed history before installing it locally for read access. Its
// caller authorizes recovery first; it never grants a consensus role.
func (s *Server) recoverThreadHistory(ctx context.Context, threadID string) (*pb.Thread, error) {
	if threadID == "" {
		return nil, fmt.Errorf("thread id is required")
	}
	if s.node == nil || s.node.DHT == nil || s.node.Bitswap == nil {
		return nil, fmt.Errorf("thread recovery requires DHT and Bitswap")
	}
	// Prime Bitswap with several archive candidates. Discovery is advisory: a
	// stale or malicious provider cannot affect recovery because the descriptor
	// and every fetched block are verified below.
	providerCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	providers, discoveryErr := thread.DiscoverArchiveProviders(providerCtx, s.node.DHT, threadID, 3)
	if discoveryErr == nil && s.node.Host != nil {
		for _, provider := range providers {
			if provider.ID == s.node.Host.ID() {
				continue
			}
			if err := s.node.Host.Connect(providerCtx, provider); err != nil {
				s.log.Debug("archive provider unavailable", zap.String("thread", threadID), zap.String("provider", provider.ID.String()), zap.Error(err))
			}
		}
	}
	cancel()
	head, err := thread.ResolveHead(ctx, s.node.DHT, threadID)
	if err != nil {
		return nil, err
	}
	if head.Thread == nil || head.Thread.Id != threadID {
		return nil, fmt.Errorf("published thread descriptor is unavailable")
	}
	if err := thread.VerifyDescriptor(head.Thread); err != nil {
		return nil, fmt.Errorf("verify thread descriptor: %w", err)
	}
	kind := thread.BackendKind(head.Thread.Metadata["backend"])
	if kind == "" {
		kind = thread.BackendRaft
	}
	blocks, err := thread.VerifyChainFromHead(ctx, kind, threadID,
		func(c context.Context, id string) (*thread.ThreadHead, error) {
			return thread.ResolveHead(c, s.node.DHT, id)
		},
		func(c context.Context, id string, height int64) (*thread.ThreadHead, error) {
			return thread.ResolveHeight(c, s.node.DHT, id, height)
		},
		func(c context.Context, cid string) (*pb.ThreadBlock, error) {
			return thread.FetchBlock(c, s.node.Bitswap, cid)
		},
	)
	if err != nil {
		return nil, err
	}
	importer, ok := s.threads.(threadHistoryImporter)
	if !ok {
		return nil, fmt.Errorf("thread manager cannot import history")
	}
	if err := importer.ImportHistory(head.Thread, blocks); err != nil {
		return nil, err
	}
	return head.Thread, nil
}

func (s *Server) AppendEntry(ctx context.Context, req *pb.AppendEntryRequest) (*pb.AppendEntryResult, error) {
	if s.threads == nil {
		return nil, fmt.Errorf("thread manager not available")
	}
	owner, err := s.scopedOwner(ctx)
	if err != nil {
		return nil, err
	}
	author := s.id.DID
	if owner != "" {
		author = owner
	}
	entry := &pb.ThreadEntry{AuthorDid: author, Payload: req.Payload, Kind: req.Kind, SubmittedAt: time.Now().UnixMilli()}
	if req.EncryptedEntry != nil {
		entry = req.EncryptedEntry
		if entry.AuthorDid != author || entry.EncodingVersion != 2 || len(entry.Nonce) != 24 || len(entry.AuthorSignature) == 0 {
			return nil, fmt.Errorf("invalid encrypted thread entry")
		}
		members, ok := s.threads.(threadMemberLister)
		if !ok {
			return nil, fmt.Errorf("thread manager cannot authorize encrypted entry membership")
		}
		memberList, err := members.ListMembers(req.ThreadId)
		if err != nil {
			return nil, err
		}
		isMember := false
		for _, member := range memberList {
			if member.Did == author {
				isMember = true
				break
			}
		}
		if !isMember {
			return nil, fmt.Errorf("encrypted entry author is not a current thread member")
		}
		epochs, ok := s.threads.(threadMembershipEpochReader)
		if !ok {
			return nil, fmt.Errorf("thread manager cannot validate encrypted entry epoch")
		}
		currentEpoch, err := epochs.MembershipEpoch(req.ThreadId)
		if err != nil {
			return nil, err
		}
		if entry.MembershipEpoch != currentEpoch || entry.EncryptionEpoch != currentEpoch {
			return nil, fmt.Errorf("encrypted entry must use current membership and encryption epoch %d", currentEpoch)
		}
		public, err := identity.PubKeyFromDID(entry.AuthorDid)
		if err != nil {
			return nil, err
		}
		ok, err = threadcrypto.Verify(public, threadcrypto.Header{ThreadID: req.ThreadId, Sequence: entry.Sequence, PreviousBlockHash: entry.PreviousBlockHash, AuthorDID: entry.AuthorDid, Kind: entry.Kind, MembershipEpoch: entry.MembershipEpoch, EncryptionEpoch: entry.EncryptionEpoch, Nonce: entry.Nonce}, entry.Payload, entry.AuthorSignature)
		if err != nil || !ok {
			return nil, fmt.Errorf("invalid encrypted thread entry signature")
		}
		entry.SubmittedAt = time.Now().UnixMilli()
	}
	if err := s.threads.AppendEntry(req.ThreadId, entry); err != nil {
		return nil, err
	}
	// THREAD_INVITE is idempotent and carries the complete thread descriptor,
	// so it doubles as a durable wake message for passivated replicas. Sending
	// it through the outbox avoids one always-live GossipSub subscription per
	// dormant thread.
	thread, err := s.threads.GetThread(req.ThreadId)
	if err != nil {
		return nil, err
	}
	if err := s.enqueueThreadWake(thread); err != nil {
		return nil, fmt.Errorf("entry is durable locally but replica wake enqueue failed: %w", err)
	}
	// Best-effort: return the current committed height + 1 as expected height.
	return &pb.AppendEntryResult{
		ThreadId: req.ThreadId,
		EntryId:  req.ThreadId + ":" + fmt.Sprintf("%d", entry.SubmittedAt),
	}, nil
}

func (s *Server) enqueueThreadWake(thread *pb.Thread) error {
	if s.outbox == nil {
		return fmt.Errorf("outbox not available")
	}
	payload, err := proto.Marshal(thread)
	if err != nil {
		return err
	}
	var errs []error
	for _, did := range thread.ReplicaDids {
		if did == s.id.DID {
			continue
		}
		msg := &pb.Message{
			Id:       uuid.New().String(),
			FromDid:  s.id.DID,
			ToDid:    did,
			ThreadId: thread.Id,
			Kind:     pb.MessageKind_MESSAGE_KIND_THREAD_INVITE,
			Payload:  payload,
			SentAt:   time.Now().UnixMilli(),
		}
		if err := s.outbox.Enqueue(msg); err != nil {
			errs = append(errs, fmt.Errorf("wake %s: %w", did, err))
			continue
		}
		appactors.Metrics.WakeQueued()
	}
	return errors.Join(errs...)
}

// PutThreadKeyEnvelope lets a thread creator publish an opaque epoch key for
// an existing member. The server only validates routing/authorization; it
// cannot decrypt the envelope.
func (s *Server) PutThreadKeyEnvelope(ctx context.Context, envelope *pb.ThreadKeyEnvelope) (*pb.Empty, error) {
	if envelope == nil || envelope.ThreadId == "" || (!envelope.RecoveryEnvelope && envelope.RecipientDid == "") {
		return nil, fmt.Errorf("valid thread key envelope required")
	}
	th, err := s.threads.GetThread(envelope.ThreadId)
	if err != nil {
		return nil, err
	}
	owner, err := s.scopedOwner(ctx)
	if err != nil {
		return nil, err
	}
	if owner != "" && owner != th.CreatorDid {
		return nil, fmt.Errorf("only thread creator may distribute epoch keys")
	}
	epochs, ok := s.threads.(threadMembershipEpochReader)
	if !ok {
		return nil, fmt.Errorf("thread manager cannot validate key-envelope epoch")
	}
	currentEpoch, err := epochs.MembershipEpoch(envelope.ThreadId)
	if err != nil {
		return nil, err
	}
	if envelope.EncryptionEpoch != currentEpoch {
		return nil, fmt.Errorf("key envelope must use current encryption epoch %d", currentEpoch)
	}
	if envelope.RecoveryEnvelope {
		if err := thread.VerifyRecoveryKeyEnvelopeCreator(th.CreatorDid, envelope); err != nil {
			return nil, err
		}
		store, ok := s.threads.(threadRecoveryKeyEnvelopeStore)
		if !ok {
			return nil, fmt.Errorf("thread manager cannot store recovery key envelopes")
		}
		if err := store.SaveRecoveryKeyEnvelope(envelope); err != nil {
			return nil, err
		}
		if s.node != nil && s.node.DHT != nil && s.node.Blockstore != nil {
			if err := thread.PublishRecoveryEnvelope(ctx, s.node.DHT, s.node.Blockstore, s.node.Bitswap, envelope); err != nil {
				return nil, fmt.Errorf("publish replayable recovery envelope: %w", err)
			}
		}
		// Archive workers on independent providers retain this opaque envelope.
		// It remains useless without the 256-bit recovery secret and is served
		// only after the same capability validation as history recovery.
		if s.gossip != nil {
			raw, err := proto.MarshalOptions{Deterministic: true}.Marshal(envelope)
			if err != nil {
				return nil, err
			}
			if err := s.gossip.Publish(ctx, thread.RecoveryEnvelopeTopic(envelope.ThreadId), raw); err != nil {
				return nil, fmt.Errorf("replicate recovery key envelope: %w", err)
			}
		}
		return &pb.Empty{}, nil
	}
	lister, ok := s.threads.(threadMemberLister)
	if !ok {
		return nil, fmt.Errorf("thread manager cannot list members")
	}
	members, err := lister.ListMembers(envelope.ThreadId)
	if err != nil {
		return nil, err
	}
	found := false
	for _, member := range members {
		if member.Did == envelope.RecipientDid {
			found = true
			break
		}
	}
	if !found {
		return nil, fmt.Errorf("key-envelope recipient is not a thread member")
	}
	store, ok := s.threads.(threadKeyEnvelopeStore)
	if !ok {
		return nil, fmt.Errorf("thread manager cannot store key envelopes")
	}
	if err := store.SaveKeyEnvelope(envelope); err != nil {
		return nil, err
	}
	return &pb.Empty{}, nil
}

// GetRecoveryThreadKeyEnvelopes returns opaque recovery envelopes only after
// proving possession of the same complete capability required to import
// history. Normal member sessions cannot use this endpoint.
func (s *Server) GetRecoveryThreadKeyEnvelopes(ctx context.Context, req *pb.RecoverThreadRequest) (*pb.ThreadKeyEnvelopes, error) {
	if req == nil || req.Handle == nil || req.Handle.ThreadId == "" || req.Handle.Version != 1 || len(req.Handle.RecoverySecret) != 32 {
		return nil, fmt.Errorf("valid recovery handle required")
	}
	allowed, err := s.authorizeRecoveryHandle(ctx, req.Handle)
	if err != nil {
		return nil, err
	}
	if !allowed {
		return nil, fmt.Errorf("recovery capability is invalid")
	}
	store, ok := s.threads.(threadRecoveryKeyEnvelopeStore)
	if !ok {
		return nil, fmt.Errorf("thread manager cannot retrieve recovery key envelopes")
	}
	envelopes, err := store.RecoveryKeyEnvelopes(req.Handle.ThreadId)
	if err != nil {
		return nil, err
	}
	// Replay manifests cover an archive worker that was offline when the live
	// pubsub envelope arrived. A bad DHT pointer is harmless: the fetched
	// envelope must still match the thread, epoch, and creator signature.
	if s.node != nil && s.node.DHT != nil && s.node.Bitswap != nil {
		seen := make(map[uint64]bool, len(envelopes))
		for _, envelope := range envelopes {
			seen[envelope.EncryptionEpoch] = true
		}
		for epoch := uint64(1); epoch <= 64; epoch++ {
			if seen[epoch] {
				continue
			}
			head, err := thread.ResolveRecoveryEnvelope(ctx, s.node.DHT, req.Handle.ThreadId, epoch)
			if err != nil {
				continue
			}
			envelope, err := thread.FetchRecoveryEnvelope(ctx, s.node.Bitswap, head.EnvelopeCID)
			if err != nil || envelope.ThreadId != req.Handle.ThreadId || envelope.EncryptionEpoch != epoch || thread.VerifyRecoveryKeyEnvelopeCreator(thCreatorDID(s.threads, req.Handle.ThreadId), envelope) != nil {
				continue
			}
			if err := store.SaveRecoveryKeyEnvelope(envelope); err == nil {
				envelopes = append(envelopes, envelope)
			}
		}
	}
	return &pb.ThreadKeyEnvelopes{Envelopes: envelopes}, nil
}

func thCreatorDID(manager ThreadManager, threadID string) string {
	th, err := manager.GetThread(threadID)
	if err != nil {
		return ""
	}
	return th.CreatorDid
}

// GetThreadKeyEnvelopes returns only envelopes addressed to the authenticated
// SDK identity, preventing one member from reading another's wrapped keys.
func (s *Server) GetThreadKeyEnvelopes(ctx context.Context, req *pb.ThreadKeyEnvelopeQuery) (*pb.ThreadKeyEnvelopes, error) {
	if req == nil || req.ThreadId == "" || req.EncryptionEpoch == 0 {
		return nil, fmt.Errorf("thread_id and encryption_epoch are required")
	}
	recipient, err := s.agentDID(ctx)
	if err != nil {
		return nil, err
	}
	store, ok := s.threads.(threadKeyEnvelopeStore)
	if !ok {
		return nil, fmt.Errorf("thread manager cannot retrieve key envelopes")
	}
	envelopes, err := store.KeyEnvelopes(req.ThreadId, req.EncryptionEpoch, recipient)
	if err != nil {
		return nil, err
	}
	return &pb.ThreadKeyEnvelopes{Envelopes: envelopes}, nil
}

func (s *Server) GetThreadEntries(req *pb.GetThreadEntriesRequest, stream pb.A2ANode_GetThreadEntriesServer) error {
	if s.threads == nil {
		return fmt.Errorf("thread manager not available")
	}
	entries, err := s.threads.GetEntries(req.ThreadId, req.SinceHeight, int(req.Limit))
	if err != nil {
		return err
	}
	for _, ep := range entries {
		if err := stream.Send(ep); err != nil {
			return err
		}
	}
	return nil
}

func (s *Server) SubscribeThread(req *pb.SubscribeThreadRequest, stream pb.A2ANode_SubscribeThreadServer) error {
	if s.threads == nil {
		return fmt.Errorf("thread manager not available")
	}

	// Flush historical entries first.
	entries, err := s.threads.GetEntries(req.ThreadId, req.SinceHeight, 0)
	if err != nil {
		return err
	}
	for _, ep := range entries {
		if err := stream.Send(ep); err != nil {
			return err
		}
	}

	// Subscribe to live commits.
	eng := s.threads.Engine(req.ThreadId)
	if eng == nil {
		// Thread exists but this node is not a validator — just block until ctx done.
		<-stream.Context().Done()
		return nil
	}

	ch := eng.Subscribe()
	defer eng.Unsubscribe(ch)

	for {
		select {
		case <-stream.Context().Done():
			return nil
		case ep, ok := <-ch:
			if !ok {
				return nil
			}
			if err := stream.Send(ep); err != nil {
				return err
			}
		}
	}
}
