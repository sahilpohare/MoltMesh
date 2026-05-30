// Tendermint BFT backend for thread consensus.
//
// Phases per (height, round):
//   PROPOSE   – leader sends Proposal; others wait timeoutPropose then prevote nil
//   PREVOTE   – on 2f+1 prevotes for B: lock B, precommit B
//              on 2f+1 prevotes for nil (or timeout): precommit nil
//   PRECOMMIT – on 2f+1 precommits for B: commit B, advance height
//              on 2f+1 precommits for nil (or timeout): next round
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
}

func newTendermintBackend(
	thread *pb.Thread,
	id *identity.Identity,
	store *Store,
	log *zap.Logger,
	onCommit CommitCallback,
) (*TendermintBackend, error) {
	cs, err := store.LoadConsensusState(thread.Id)
	if err != nil {
		return nil, err
	}
	return &TendermintBackend{
		thread:    thread,
		id:        id,
		store:     store,
		log:       log,
		onCommit:  onCommit,
		cs:        cs,
		inboundCh: make(chan *pb.ConsensusMsg, 256),
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
				e.handleVote(ctx, p.Vote, broadcast, prevoteTimer, precommitTimer)
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
	if !e.isProposer() {
		return
	}
	proposal, err := e.buildProposal(ctx)
	if err != nil {
		e.log.Warn("tendermint: build proposal failed", zap.Error(err))
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
}

func (e *TendermintBackend) handleProposal(
	ctx context.Context,
	prop *pb.Proposal,
	broadcast func(*pb.ConsensusMsg),
	prevoteTimer *time.Timer,
) {
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
	if err := e.verifyProposalSig(prop); err != nil {
		e.log.Warn("tendermint: invalid proposal signature", zap.Error(err))
		return
	}
	e.proposal = prop

	blockHash := prop.Block.BlockHash
	if e.cs.LockedRound >= 0 && e.cs.LockedHash != blockHash && prop.PolRound < int32(e.cs.LockedRound) {
		e.sendVote(broadcast, pb.VoteType_VOTE_TYPE_PREVOTE, "")
	} else {
		e.sendVote(broadcast, pb.VoteType_VOTE_TYPE_PREVOTE, blockHash)
	}
	e.cs.Step = stepPrevote
	e.store.SaveConsensusState(e.thread.Id, e.cs) //nolint:errcheck
	prevoteTimer.Reset(time.Duration(defaultTimeoutMs) * time.Millisecond)
}

func (e *TendermintBackend) handleVote(
	ctx context.Context,
	vote *pb.Vote,
	broadcast func(*pb.ConsensusMsg),
	prevoteTimer, precommitTimer *time.Timer,
) {
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
	if err := e.store.SaveVote(vote); err != nil {
		e.log.Warn("tendermint: save vote", zap.Error(err))
		return
	}

	quorum := e.quorum()

	switch vote.Type {
	case pb.VoteType_VOTE_TYPE_PREVOTE:
		if e.cs.Step != stepPrevote {
			return
		}
		votes, _ := e.store.GetVotes(e.thread.Id, vote.Height, vote.Round, pb.VoteType_VOTE_TYPE_PREVOTE)
		blockHash, count := majority(votes)
		if count < quorum {
			return
		}
		prevoteTimer.Stop()
		if blockHash == "" {
			e.sendVote(broadcast, pb.VoteType_VOTE_TYPE_PRECOMMIT, "")
		} else {
			e.cs.LockedRound = vote.Round
			e.cs.LockedHash = blockHash
			e.cs.ValidRound = vote.Round
			e.cs.ValidHash = blockHash
			e.sendVote(broadcast, pb.VoteType_VOTE_TYPE_PRECOMMIT, blockHash)
		}
		e.cs.Step = stepPrecommit
		e.store.SaveConsensusState(e.thread.Id, e.cs) //nolint:errcheck
		precommitTimer.Reset(time.Duration(defaultTimeoutMs) * time.Millisecond)

	case pb.VoteType_VOTE_TYPE_PRECOMMIT:
		if e.cs.Step != stepPrecommit {
			return
		}
		votes, _ := e.store.GetVotes(e.thread.Id, vote.Height, vote.Round, pb.VoteType_VOTE_TYPE_PRECOMMIT)
		blockHash, count := majority(votes)
		if count < quorum || blockHash == "" {
			return
		}
		precommitTimer.Stop()
		if e.proposal != nil && e.proposal.Block.BlockHash == blockHash {
			e.commitBlock(ctx, e.proposal.Block, broadcast)
		}
	}
}

func (e *TendermintBackend) commitBlock(ctx context.Context, block *pb.ThreadBlock, broadcast func(*pb.ConsensusMsg)) {
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
	e.cs = ConsensusState{
		Height:      block.Height + 1,
		Round:       0,
		Step:        stepPropose,
		LockedRound: -1,
		ValidRound:  -1,
	}
	e.proposal = nil
	e.store.SaveConsensusState(e.thread.Id, e.cs) //nolint:errcheck

	if onCommit != nil {
		e.mu.Unlock()
		onCommit(block)
		e.mu.Lock()
	}

	e.enterPropose(ctx, func(msg *pb.ConsensusMsg) {
		select {
		case e.inboundCh <- msg:
		default:
		}
	})
}

// ─── helpers ──────────────────────────────────────────────────────────────────

func (e *TendermintBackend) isProposer() bool {
	idx := (e.cs.Height + int64(e.cs.Round)) % int64(len(e.thread.ReplicaDids))
	return e.thread.ReplicaDids[idx] == e.id.DID
}

func (e *TendermintBackend) isValidProposer(did string, height int64, round int32) bool {
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

func (e *TendermintBackend) quorum() int { return 2*int(e.thread.F) + 1 }

func (e *TendermintBackend) buildProposal(_ context.Context) (*pb.Proposal, error) {
	entries, err := e.store.DequeuePendingEntries(e.thread.Id, maxPendingPerBlock)
	if err != nil {
		return nil, err
	}

	parentHash := ""
	if e.cs.Height > 1 {
		if prev, err := e.store.GetBlock(e.thread.Id, e.cs.Height-1); err == nil {
			parentHash = prev.BlockHash
		}
	}

	polRound := int32(-1)
	blockHash := ""
	if e.cs.ValidRound >= 0 && e.cs.ValidHash != "" {
		blockHash = e.cs.ValidHash
		polRound = e.cs.ValidRound
	}

	block := &pb.ThreadBlock{
		ThreadId:    e.thread.Id,
		Height:      e.cs.Height,
		Round:       e.cs.Round,
		ParentHash:  parentHash,
		Entries:     entries,
		ProposerDid: e.id.DID,
	}
	if blockHash == "" {
		blockHash = computeBlockHash(block)
	}
	block.BlockHash = blockHash

	sigData := proposalSigData(e.thread.Id, e.cs.Height, e.cs.Round, blockHash)
	sig := e.id.Sign(sigData)
	block.ProposerSig = hex.EncodeToString(sig)

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
}

func (e *TendermintBackend) verifyProposalSig(prop *pb.Proposal) error {
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
	return nil
}

func (e *TendermintBackend) verifyVoteSig(v *pb.Vote) error {
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
	entriesJSON, _ := json.Marshal(marshalEntries(b.Entries))
	raw := fmt.Sprintf("%s:%d:%d:%s:%s:%s",
		b.ThreadId, b.Height, b.Round, b.ParentHash,
		hex.EncodeToString(entriesJSON), b.ProposerDid,
	)
	h := sha256.Sum256([]byte(raw))
	return hex.EncodeToString(h[:])
}

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
