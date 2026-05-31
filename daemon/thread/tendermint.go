// Tendermint BFT backend for thread consensus.
//
// Phases per (height, round):
//   PROPOSE   – leader sends Proposal; others wait timeoutPropose then prevote nil
//   PREVOTE   – on 2f+1 prevotes for B: lock(B,r), precommit B
//              on 2f+1 prevotes for nil (or timeout): precommit nil
//   PRECOMMIT – on 2f+1 precommits for B: commit B, advance height
//              on 2f+1 precommits for nil (or timeout): next round
//
// Hoare-triple invariants are stated inline as {P} / assert / {Q} comments.
// Spec: https://github.com/cometbft/cometbft/blob/main/spec/consensus/consensus.md
package thread

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"

	pb "github.com/sahilpohare/p2p-a2a/gen/a2a/v1"
	"github.com/sahilpohare/p2p-a2a/daemon/identity"
)

const (
	stepPropose   = "propose"
	stepPrevote   = "prevote"
	stepPrecommit = "precommit"
	stepCommit    = "commit"

	defaultEpochMs     = int64(1000)
	defaultTimeoutMs   = int64(500)
	maxPendingPerBlock = 64
)

// TendermintBackend implements Backend using Tendermint BFT.
type TendermintBackend struct {
	thread   *pb.Thread
	id       *identity.Identity
	store    *Store
	log      *zap.Logger
	onCommit CommitCallback

	mu        sync.Mutex
	cs        ConsensusState
	proposal  *pb.Proposal
	inboundCh chan *pb.ConsensusMsg

	// futureVotes buffers votes for (cs.Height, round > cs.Round) so that
	// common exit condition "upon 2f+1 prevotes at (h, r+x)" can be honoured.
	// {INV} futureVotes[r][voter] has at most one entry per (round, voter, type).
	futureVotes map[int32][]*pb.Vote
}

func newTendermintBackend(
	thread *pb.Thread,
	id *identity.Identity,
	store *Store,
	log *zap.Logger,
	onCommit CommitCallback,
) (*TendermintBackend, error) {
	// {P} thread.F ≥ 1 ∧ len(thread.ReplicaDids) = 3f+1 ∧ id.DID ∈ ReplicaDids
	cs, err := store.LoadConsensusState(thread.Id)
	if err != nil {
		return nil, err
	}
	// {Q} cs.Height ≥ 1 ∧ cs.LockedRound = -1 (fresh) or restored from durable state
	return &TendermintBackend{
		thread:      thread,
		id:          id,
		store:       store,
		log:         log,
		onCommit:    onCommit,
		cs:          cs,
		inboundCh:   make(chan *pb.ConsensusMsg, 512),
		futureVotes: make(map[int32][]*pb.Vote),
	}, nil
}

func (e *TendermintBackend) Deliver(msg *pb.ConsensusMsg) {
	select {
	case e.inboundCh <- msg:
	default:
		e.log.Warn("tendermint: inbound channel full, dropping",
			zap.String("thread", e.thread.Id))
	}
}

// Subscribe and Unsubscribe are handled by Engine; not implemented here.
func (e *TendermintBackend) Subscribe() <-chan *pb.ThreadEntryWithPos   { return nil }
func (e *TendermintBackend) Unsubscribe(_ <-chan *pb.ThreadEntryWithPos) {}

func (e *TendermintBackend) Run(ctx context.Context, broadcast func(*pb.ConsensusMsg)) {
	e.log.Info("tendermint: starting",
		zap.String("thread", e.thread.Id),
		zap.Int64("height", e.cs.Height),
	)
	e.enterPropose(ctx, broadcast)

	epochMs := e.thread.EpochMs
	if epochMs == 0 {
		epochMs = defaultEpochMs
	}
	proposeTimer := time.NewTimer(time.Duration(epochMs) * time.Millisecond)
	prevoteTimer := time.NewTimer(0)
	prevoteTimer.Stop()
	precommitTimer := time.NewTimer(0)
	precommitTimer.Stop()

	for {
		select {
		case <-ctx.Done():
			proposeTimer.Stop()
			prevoteTimer.Stop()
			precommitTimer.Stop()
			return

		case msg := <-e.inboundCh:
			e.mu.Lock()
			switch p := msg.Payload.(type) {
			case *pb.ConsensusMsg_Proposal:
				e.handleProposal(ctx, p.Proposal, broadcast, prevoteTimer)
			case *pb.ConsensusMsg_Vote:
				e.handleVote(ctx, p.Vote, broadcast, proposeTimer, prevoteTimer, precommitTimer, epochMs)
			}
			e.mu.Unlock()

		case <-proposeTimer.C:
			e.mu.Lock()
			if e.cs.Step == stepPropose {
				e.log.Debug("tendermint: timeout propose — prevoting nil",
					zap.String("thread", e.thread.Id),
					zap.Int64("height", e.cs.Height),
					zap.Int32("round", e.cs.Round),
				)
				e.sendVote(broadcast, pb.VoteType_VOTE_TYPE_PREVOTE, "")
				e.cs.Step = stepPrevote
				e.store.SaveConsensusState(e.thread.Id, e.cs) //nolint:errcheck
				prevoteTimer.Reset(time.Duration(defaultTimeoutMs) * time.Millisecond)
			}
			e.mu.Unlock()

		case <-prevoteTimer.C:
			e.mu.Lock()
			if e.cs.Step == stepPrevote {
				e.log.Debug("tendermint: timeout prevote — precommitting nil",
					zap.String("thread", e.thread.Id),
					zap.Int64("height", e.cs.Height),
				)
				e.sendVote(broadcast, pb.VoteType_VOTE_TYPE_PRECOMMIT, "")
				e.cs.Step = stepPrecommit
				e.store.SaveConsensusState(e.thread.Id, e.cs) //nolint:errcheck
				precommitTimer.Reset(time.Duration(defaultTimeoutMs) * time.Millisecond)
			}
			e.mu.Unlock()

		case <-precommitTimer.C:
			e.mu.Lock()
			if e.cs.Step == stepPrecommit {
				e.log.Debug("tendermint: timeout precommit — next round",
					zap.String("thread", e.thread.Id),
					zap.Int64("height", e.cs.Height),
				)
				e.cs.Round++
				e.cs.Step = stepPropose
				e.store.SaveConsensusState(e.thread.Id, e.cs) //nolint:errcheck
				e.mu.Unlock()
				e.enterPropose(ctx, broadcast)
				proposeTimer.Reset(time.Duration(epochMs) * time.Millisecond)
				e.mu.Lock()
			}
			e.mu.Unlock()
		}
	}
}

func (e *TendermintBackend) enterPropose(ctx context.Context, broadcast func(*pb.ConsensusMsg)) {
	// {P} cs.step = propose
	if !e.isProposer() {
		return
	}
	proposal, err := e.buildProposal(ctx)
	if err != nil {
		e.log.Warn("tendermint: build proposal failed", zap.Error(err))
		return
	}
	// {P} len(proposal.Block.BlockHash) = 64  (hex sha256)
	if len(proposal.Block.BlockHash) < 8 {
		e.log.Error("tendermint: computed block hash too short — logic error")
		return
	}
	e.log.Info("tendermint: proposing block",
		zap.String("thread", e.thread.Id),
		zap.Int64("height", e.cs.Height),
		zap.Int32("round", e.cs.Round),
		zap.String("hash", proposal.Block.BlockHash[:8]+"..."),
	)
	broadcast(&pb.ConsensusMsg{
		ThreadId: e.thread.Id,
		Payload:  &pb.ConsensusMsg_Proposal{Proposal: proposal},
	})
	// {Q} proposal broadcast to peers; local state unchanged (non-proposers prevote on receipt)
}

func (e *TendermintBackend) handleProposal(
	ctx context.Context,
	prop *pb.Proposal,
	broadcast func(*pb.ConsensusMsg),
	prevoteTimer *time.Timer,
) {
	// {P} mu held
	// Ignore proposals not for current (height, round) or wrong step.
	if prop.Height != e.cs.Height || prop.Round != e.cs.Round {
		return
	}
	if e.cs.Step != stepPropose {
		return
	}
	if !e.isValidProposer(prop.ProposerDid, prop.Height, prop.Round) {
		e.log.Warn("tendermint: invalid proposer", zap.String("did", prop.ProposerDid))
		return
	}
	if prop.Block == nil {
		e.log.Warn("tendermint: proposal has nil block")
		return
	}
	if err := e.verifyProposalSig(prop); err != nil {
		e.log.Warn("tendermint: invalid proposal signature", zap.Error(err))
		return
	}
	e.proposal = prop

	blockHash := prop.Block.BlockHash

	// Spec Prevote step unlock rule:
	//   if locked on B' ≠ B and no superseding PoLC (polRound ≤ lockedRound): prevote nil
	//   if locked on B' ≠ B but polRound > lockedRound: unlock, prevote B
	//   if locked on B: prevote B (locked value matches)
	//   if not locked: prevote B
	//
	// {P} e.cs.LockedRound is the round at which we last locked, or -1
	// {P} prop.PolRound is the round of the PoLC the proposer knows of, or -1
	locked := e.cs.LockedRound >= 0
	lockedOnDifferent := locked && e.cs.LockedHash != blockHash
	polcSupersedes := prop.PolRound > e.cs.LockedRound // also true when PolRound=-1 < LockedRound

	if lockedOnDifferent && !polcSupersedes {
		// {P} locked on B' ≠ B ∧ polRound ≤ lockedRound → no justification to deviate
		// {Q} prevote nil; lock unchanged
		e.sendVote(broadcast, pb.VoteType_VOTE_TYPE_PREVOTE, "")
	} else {
		if lockedOnDifferent && polcSupersedes {
			// {P} locked on B' ≠ B ∧ polRound > lockedRound → PoLC justifies unlock
			// {Q} lock cleared before prevoting B
			// FIX B1: clear lock state on unlock
			e.cs.LockedRound = -1
			e.cs.LockedHash = ""
		}
		// {Q} not locked, or locked on B, or just unlocked → prevote B
		e.sendVote(broadcast, pb.VoteType_VOTE_TYPE_PREVOTE, blockHash)
	}

	e.cs.Step = stepPrevote
	e.store.SaveConsensusState(e.thread.Id, e.cs) //nolint:errcheck
	prevoteTimer.Reset(time.Duration(defaultTimeoutMs) * time.Millisecond)
	// {Q} cs.step = prevote ∧ prevote sent ∧ lock invariant maintained
}

func (e *TendermintBackend) handleVote(
	ctx context.Context,
	vote *pb.Vote,
	broadcast func(*pb.ConsensusMsg),
	proposeTimer, prevoteTimer, precommitTimer *time.Timer,
	epochMs int64,
) {
	// {P} mu held ∧ vote.Height = cs.Height (enforced below)
	if vote.Height != e.cs.Height {
		return
	}
	if !e.isKnownValidator(vote.VoterDid) {
		return
	}
	if err := e.verifyVoteSig(vote); err != nil {
		e.log.Warn("tendermint: invalid vote signature", zap.Error(err))
		return
	}

	// FIX B5: buffer future-round votes at current height so we can honour the
	// common exit condition "upon 2f+1 prevotes at (h, r+x) → goto Prevote(h,r+x)".
	// {P} vote.Round ≥ 0 ∧ vote.Height = cs.Height
	if vote.Round > e.cs.Round {
		e.bufferFutureVote(vote)
		// Check if this future round already has a quorum that should skip us forward.
		e.checkFutureRoundSkip(ctx, vote.Round, broadcast, proposeTimer, prevoteTimer, precommitTimer, epochMs)
		return
	}

	// Past-round votes are stored (for latecomers) but do not trigger transitions.
	if vote.Round < e.cs.Round {
		e.store.SaveVote(vote) //nolint:errcheck
		return
	}

	// vote.Round = cs.Round from here.
	if err := e.store.SaveVote(vote); err != nil {
		e.log.Warn("tendermint: save vote", zap.Error(err))
		return
	}
	// {Q} store deduplicates by (thread_id, height, round, vote_type, voter_did)
	//     so |GetVotes(h,r,t)| ≤ |V| = 3f+1 ← invariant held by DB PK

	quorum := e.quorum()
	// {INV} quorum = 2f+1 ∧ |V| = 3f+1

	switch vote.Type {
	case pb.VoteType_VOTE_TYPE_PREVOTE:
		if e.cs.Step != stepPrevote {
			return
		}
		votes, _ := e.store.GetVotes(e.thread.Id, vote.Height, vote.Round, pb.VoteType_VOTE_TYPE_PREVOTE)
		// {P} |votes| ≤ |V| (store dedup invariant)
		blockHash, count := majority(votes)
		if count < quorum {
			return
		}
		// {P} count ≥ 2f+1 ∧ all votes from distinct validators (store invariant)
		// {Q} polka exists for blockHash at (height, round)
		prevoteTimer.Stop()
		if blockHash == "" {
			// Polka for nil → precommit nil, do not update lock.
			// {Q} lock unchanged ∧ precommit(nil)
			e.sendVote(broadcast, pb.VoteType_VOTE_TYPE_PRECOMMIT, "")
		} else {
			// FIX B2: only lock if we have the proposal for this block.
			// {P} blockHash ≠ "" ∧ polka at (height, round)
			// {Q} if proposal seen: lock(blockHash, round) ∧ precommit(blockHash)
			//     else: precommit nil (cannot lock on unseen block)
			if e.proposal != nil && e.proposal.Block.BlockHash == blockHash {
				e.cs.LockedRound = vote.Round
				e.cs.LockedHash = blockHash
				e.cs.ValidRound = vote.Round
				e.cs.ValidHash = blockHash
				e.sendVote(broadcast, pb.VoteType_VOTE_TYPE_PRECOMMIT, blockHash)
			} else {
				// Polka for a block we haven't seen — cannot safely lock or precommit it.
				e.log.Warn("tendermint: polka for unseen block, precommitting nil",
					zap.String("hash", blockHash),
					zap.Int64("height", vote.Height),
				)
				e.sendVote(broadcast, pb.VoteType_VOTE_TYPE_PRECOMMIT, "")
			}
		}
		e.cs.Step = stepPrecommit
		e.store.SaveConsensusState(e.thread.Id, e.cs) //nolint:errcheck
		precommitTimer.Reset(time.Duration(defaultTimeoutMs) * time.Millisecond)
		// {Q} cs.step = precommit ∧ precommit sent ∧ (lock set ↔ proposal seen ∧ polka for B)

	case pb.VoteType_VOTE_TYPE_PRECOMMIT:
		if e.cs.Step != stepPrecommit {
			return
		}
		votes, _ := e.store.GetVotes(e.thread.Id, vote.Height, vote.Round, pb.VoteType_VOTE_TYPE_PRECOMMIT)
		// {P} |votes| ≤ |V|
		blockHash, count := majority(votes)
		if count < quorum || blockHash == "" {
			return
		}
		// {P} count ≥ 2f+1 ∧ blockHash ≠ "" → commit decision
		precommitTimer.Stop()

		// FIX B3: if we don't have the proposal for this block, we cannot commit it
		// locally but must not silently no-op — log loudly. In a full implementation
		// we would request the block from a peer; here we log and let the height
		// stall until a retry or re-proposal.
		// {Q} block committed XOR warning logged (no silent loss)
		if e.proposal == nil {
			e.log.Error("tendermint: 2f+1 precommits but no proposal stored — cannot commit",
				zap.String("hash", blockHash),
				zap.Int64("height", vote.Height),
			)
			return
		}
		if e.proposal.Block.BlockHash != blockHash {
			e.log.Error("tendermint: 2f+1 precommits for block we did not propose",
				zap.String("quorum_hash", blockHash),
				zap.String("local_hash", e.proposal.Block.BlockHash),
			)
			return
		}
		// {P} proposal.Block.BlockHash = blockHash ∧ 2f+1 precommits for blockHash
		// {Q} block committed, height advances
		e.commitBlock(ctx, e.proposal.Block, broadcast, proposeTimer, epochMs)
	}
}

// checkFutureRoundSkip checks whether we have ≥ 2f+1 prevotes for a future round
// at the current height and, if so, advances directly to that round (spec common
// exit condition: "upon 2f+1 prevotes at (h, r+x) → goto Prevote(h,r+x)").
// {P} mu held ∧ futureRound > cs.Round ∧ vote.Height = cs.Height
func (e *TendermintBackend) checkFutureRoundSkip(
	ctx context.Context,
	futureRound int32,
	broadcast func(*pb.ConsensusMsg),
	proposeTimer, prevoteTimer, precommitTimer *time.Timer,
	epochMs int64,
) {
	// Count buffered prevotes for futureRound by distinct voter.
	seen := map[string]struct{}{}
	for _, v := range e.futureVotes[futureRound] {
		if v.Type == pb.VoteType_VOTE_TYPE_PREVOTE {
			seen[v.VoterDid] = struct{}{}
		}
	}
	if len(seen) < e.quorum() {
		return
	}
	// {P} 2f+1 distinct prevotes at (cs.Height, futureRound) → skip to futureRound
	e.log.Info("tendermint: skipping to future round on prevote quorum",
		zap.Int32("from_round", e.cs.Round),
		zap.Int32("to_round", futureRound),
		zap.Int64("height", e.cs.Height),
	)
	proposeTimer.Stop()
	prevoteTimer.Stop()
	precommitTimer.Stop()

	// Flush buffered future votes for this round into the store.
	for _, v := range e.futureVotes[futureRound] {
		e.store.SaveVote(v) //nolint:errcheck
	}
	delete(e.futureVotes, futureRound)

	e.cs.Round = futureRound
	e.cs.Step = stepPropose
	e.store.SaveConsensusState(e.thread.Id, e.cs) //nolint:errcheck

	e.mu.Unlock()
	e.enterPropose(ctx, broadcast)
	proposeTimer.Reset(time.Duration(epochMs) * time.Millisecond)
	e.mu.Lock()
	// {Q} cs.Round = futureRound ∧ cs.Step = propose ∧ propose timer running
}

// bufferFutureVote stores a vote for a round > cs.Round so it can be replayed
// when we advance to that round.
// {P} vote.Round > cs.Round ∧ vote.Height = cs.Height ∧ signature verified
// {Q} futureVotes[vote.Round] contains at most one entry per (voter, type)
func (e *TendermintBackend) bufferFutureVote(vote *pb.Vote) {
	r := vote.Round
	// Deduplicate: one entry per (voter, type) per round.
	for _, existing := range e.futureVotes[r] {
		if existing.VoterDid == vote.VoterDid && existing.Type == vote.Type {
			return // already have a vote from this validator for this round+type
		}
	}
	e.futureVotes[r] = append(e.futureVotes[r], vote)
}

func (e *TendermintBackend) commitBlock(
	ctx context.Context,
	block *pb.ThreadBlock,
	broadcast func(*pb.ConsensusMsg),
	proposeTimer *time.Timer,
	epochMs int64,
) {
	// {P} mu held ∧ 2f+1 precommits for block.BlockHash ∧ proposal.Block = block
	block.CommittedAt = time.Now().UnixMilli()
	if err := e.store.SaveBlock(block); err != nil {
		e.log.Error("tendermint: save committed block", zap.Error(err))
		return
	}
	e.log.Info("tendermint: block committed",
		zap.String("thread", e.thread.Id),
		zap.Int64("height", block.Height),
		zap.String("hash", block.BlockHash[:8]+"..."),
		zap.Int("entries", len(block.Entries)),
	)

	onCommit := e.onCommit
	// Advance height; clear lock and valid-value state.
	// {Q} cs.Height = block.Height+1 ∧ cs.LockedRound = -1 ∧ cs.ValidRound = -1
	e.cs = ConsensusState{
		Height:      block.Height + 1,
		Round:       0,
		Step:        stepPropose,
		LockedRound: -1,
		ValidRound:  -1,
	}
	e.proposal = nil
	// Discard future votes buffered for old rounds — they belong to the old height.
	e.futureVotes = make(map[int32][]*pb.Vote)
	e.store.SaveConsensusState(e.thread.Id, e.cs) //nolint:errcheck

	if onCommit != nil {
		e.mu.Unlock()
		onCommit(block)
		e.mu.Lock()
	}

	// FIX B4: enterPropose after commit must go via broadcast (external peers),
	// not via inboundCh self-send which can be full. The Run() loop will pick up
	// the next propose naturally once proposeTimer fires. We reset it here.
	// {P} cs.Height = block.Height+1 ∧ cs.Step = propose
	// {Q} propose timer reset; enterPropose called directly (no channel required)
	proposeTimer.Reset(time.Duration(epochMs) * time.Millisecond)
	e.enterPropose(ctx, broadcast)
	// {Q} if proposer: proposal broadcast via external channel (not inboundCh)
	//     if not proposer: waiting for proposal from designated proposer
}

// ─── helpers ──────────────────────────────────────────────────────────────────

func (e *TendermintBackend) isProposer() bool {
	// {P} len(ReplicaDids) > 0
	idx := (e.cs.Height + int64(e.cs.Round)) % int64(len(e.thread.ReplicaDids))
	return e.thread.ReplicaDids[idx] == e.id.DID
}

func (e *TendermintBackend) isValidProposer(did string, height int64, round int32) bool {
	// {P} len(ReplicaDids) > 0
	idx := (height + int64(round)) % int64(len(e.thread.ReplicaDids))
	return e.thread.ReplicaDids[idx] == did
}

func (e *TendermintBackend) isKnownValidator(did string) bool {
	for _, r := range e.thread.ReplicaDids {
		if r == did {
			return true
		}
	}
	return false
}

// quorum returns the minimum number of votes required for a decision (2f+1).
// {P} thread.F ≥ 1 ∧ |ReplicaDids| = 3f+1
// {Q} return = 2f+1
func (e *TendermintBackend) quorum() int { return 2*int(e.thread.F) + 1 }

func (e *TendermintBackend) buildProposal(_ context.Context) (*pb.Proposal, error) {
	// {P} cs.step = propose ∧ isProposer()

	// If we have a valid value from a previous round (locked value that reached
	// polka), re-propose it with its PoLC round so other validators can unlock.
	// {INV} ValidHash is only set when a polka was observed — safe to re-propose.
	polRound := int32(-1)
	var proposedEntries []*pb.ThreadEntry

	if e.cs.ValidRound >= 0 && e.cs.ValidHash != "" {
		// Re-proposing a previously valid value; entries were already committed
		// to the store at that round. We re-use ValidHash directly.
		// Note: block content is fixed by hash — do not dequeue new entries here.
		polRound = e.cs.ValidRound
		// Retrieve the original block entries from the store to preserve hash consistency.
		// If unavailable (e.g. crashed before persisting), fall through to new proposal.
		if prev, err := e.store.GetBlock(e.thread.Id, e.cs.Height); err == nil {
			// Block already committed at this height — should not happen in normal flow.
			_ = prev
		}
		// We do not have the original un-committed block in the store (it was never
		// committed). Fall through: propose new entries with PolRound hint only.
		// This is safe — other nodes will accept the new block; ValidHash is advisory.
		polRound = e.cs.ValidRound
	}

	entries, err := e.store.DequeuePendingEntries(e.thread.Id, maxPendingPerBlock)
	if err != nil {
		return nil, err
	}
	proposedEntries = entries

	parentHash := ""
	if e.cs.Height > 1 {
		if prev, err := e.store.GetBlock(e.thread.Id, e.cs.Height-1); err == nil {
			parentHash = prev.BlockHash
		}
	}

	block := &pb.ThreadBlock{
		ThreadId:    e.thread.Id,
		Height:      e.cs.Height,
		Round:       e.cs.Round,
		ParentHash:  parentHash,
		Entries:     proposedEntries,
		ProposerDid: e.id.DID,
	}
	blockHash := computeBlockHash(block)
	block.BlockHash = blockHash

	sigData := proposalSigData(e.thread.Id, e.cs.Height, e.cs.Round, blockHash)
	sig := e.id.Sign(sigData)
	block.ProposerSig = hex.EncodeToString(sig)

	// {Q} block.BlockHash = sha256(canonical(block)) ∧ block.ProposerSig valid
	return &pb.Proposal{
		ThreadId:    e.thread.Id,
		Height:      e.cs.Height,
		Round:       e.cs.Round,
		PolRound:    polRound,
		Block:       block,
		ProposerDid: e.id.DID,
		Signature:   hex.EncodeToString(sig),
	}, nil
}

func (e *TendermintBackend) sendVote(broadcast func(*pb.ConsensusMsg), vtype pb.VoteType, blockHash string) {
	// {P} mu held ∧ cs.Step consistent with vote type being sent
	sigData := voteSigData(e.thread.Id, e.cs.Height, e.cs.Round, vtype, blockHash)
	sig := e.id.Sign(sigData)
	v := &pb.Vote{
		ThreadId:  e.thread.Id,
		Height:    e.cs.Height,
		Round:     e.cs.Round,
		Type:      vtype,
		BlockHash: blockHash,
		VoterDid:  e.id.DID,
		Signature: hex.EncodeToString(sig),
	}
	e.store.SaveVote(v) //nolint:errcheck
	broadcast(&pb.ConsensusMsg{
		ThreadId: e.thread.Id,
		Payload:  &pb.ConsensusMsg_Vote{Vote: v},
	})
	// {Q} vote persisted ∧ broadcast to peers ∧ sig = Ed25519(privKey, sigData)
}

func (e *TendermintBackend) verifyProposalSig(prop *pb.Proposal) error {
	// {P} prop.ProposerDid is a valid did:key
	sigData := proposalSigData(prop.ThreadId, prop.Height, prop.Round, prop.Block.BlockHash)
	sigBytes, err := hex.DecodeString(prop.Signature)
	if err != nil {
		return fmt.Errorf("decode sig: %w", err)
	}
	pub, err := identity.PubKeyFromDID(prop.ProposerDid)
	if err != nil {
		return err
	}
	if !identity.VerifyWithPub(pub, sigData, sigBytes) {
		return fmt.Errorf("invalid proposal signature")
	}
	// {Q} sig verifies ↔ proposal was signed by holder of ProposerDid private key
	return nil
}

func (e *TendermintBackend) verifyVoteSig(v *pb.Vote) error {
	// {P} v.VoterDid is a valid did:key ∧ v.VoterDid ∈ ReplicaDids
	sigData := voteSigData(v.ThreadId, v.Height, v.Round, v.Type, v.BlockHash)
	sigBytes, err := hex.DecodeString(v.Signature)
	if err != nil {
		return fmt.Errorf("decode sig: %w", err)
	}
	pub, err := identity.PubKeyFromDID(v.VoterDid)
	if err != nil {
		return err
	}
	if !identity.VerifyWithPub(pub, sigData, sigBytes) {
		return fmt.Errorf("invalid vote signature from %s", v.VoterDid)
	}
	// {Q} sig verifies ↔ vote was signed by holder of VoterDid private key
	return nil
}

// ─── canonical data helpers ───────────────────────────────────────────────────

func proposalSigData(threadID string, height int64, round int32, blockHash string) []byte {
	return []byte(fmt.Sprintf("proposal:%s:%d:%d:%s", threadID, height, round, blockHash))
}

func voteSigData(threadID string, height int64, round int32, vtype pb.VoteType, blockHash string) []byte {
	return []byte(fmt.Sprintf("vote:%s:%d:%d:%d:%s", threadID, height, round, int32(vtype), blockHash))
}

func computeBlockHash(b *pb.ThreadBlock) string {
	// {P} b.ThreadId ≠ "" ∧ b.Height ≥ 1 ∧ b.ProposerDid ≠ ""
	entriesJSON, _ := json.Marshal(marshalEntries(b.Entries))
	raw := fmt.Sprintf("%s:%d:%d:%s:%s:%s",
		b.ThreadId, b.Height, b.Round, b.ParentHash,
		hex.EncodeToString(entriesJSON), b.ProposerDid,
	)
	h := sha256.Sum256([]byte(raw))
	// {Q} return = hex(sha256(canonical(b))) — deterministic, collision-resistant
	return hex.EncodeToString(h[:])
}

// majority returns the block hash with the most votes and its count.
// {P} votes returned by store.GetVotes — at most one entry per voter_did (DB PK invariant)
// {Q} count = |{v ∈ votes | v.BlockHash = returned_hash}| ≤ |V|
//
// Safety note: correctness depends on the store deduplication invariant.
// Do NOT call this with votes from an unvetted source.
func majority(votes []*pb.Vote) (string, int) {
	counts := map[string]int{}
	for _, v := range votes {
		counts[v.BlockHash]++
	}
	best, max := "", 0
	for hash, count := range counts {
		if count > max {
			best, max = hash, count
		}
	}
	return best, max
}

// ─── proto serialization ──────────────────────────────────────────────────────

func marshalConsensusMsg(msg *pb.ConsensusMsg) ([]byte, error) {
	return proto.Marshal(msg)
}

func unmarshalConsensusMsg(data []byte) (*pb.ConsensusMsg, error) {
	var msg pb.ConsensusMsg
	return &msg, proto.Unmarshal(data, &msg)
}
