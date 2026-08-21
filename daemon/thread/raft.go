// Raft consensus backend using go.etcd.io/raft/v3.
//
// The etcd raft library is a pure state machine. We own:
//   - MemoryStorage (initial; entries are also persisted in SQLite)
//   - The Ready loop: apply committed entries, persist HardState, send messages
//   - Transport: ConsensusMsg carries raftpb.Message JSON-encoded in ProposerSig
//
// Only the leader proposes entries. Followers replicate via AppendEntries.
// Quorum = majority (N/2+1). Crash-fault tolerant.
package thread

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"runtime"
	"strconv"
	"sync"
	"time"

	"go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"

	appactors "github.com/sahilpohare/p2p-a2a/daemon/actors"
	"github.com/sahilpohare/p2p-a2a/daemon/identity"
	pb "github.com/sahilpohare/p2p-a2a/gen/a2a/v1"
)

const (
	raftTickMs           = 100 // one tick = 100 ms
	raftHeartbeatTicks   = 1   // leader sends heartbeat every tick
	raftElectionTicks    = 5   // follower times out after 5 ticks (500 ms)
	raftMaxEntriesPerMsg = 64
)

// RaftBackend implements Backend using go.etcd.io/raft/v3.
type RaftBackend struct {
	thread   *pb.Thread
	id       *identity.Identity
	store    *Store
	log      *zap.Logger
	onCommit CommitCallback

	// peerID maps DID → raft node ID (uint64, 1-based index in ReplicaDids)
	selfID  uint64
	peerIDs map[string]uint64 // DID → raftID
	didByID map[uint64]string // raftID → DID

	node         raft.Node
	storage      *raft.MemoryStorage
	appliedIndex uint64
	appliedTerm  uint64
	confState    raftpb.ConfState
	pendingReady *raft.Ready

	mu        sync.Mutex
	inboundCh chan *pb.ConsensusMsg
	changeSeq uint64
	changes   map[string]chan error
}

type raftProposal struct {
	PendingIDs []int64           `json:"pending_ids"`
	Entries    []*pb.ThreadEntry `json:"entries"`
	OwnerDID   string            `json:"owner_did"`
}

// ProposeVoterChange submits a ConfChangeV2 through the current Raft leader.
// The target must already be a known observer replica; callers are responsible
// for proving it has caught up before asking to promote it.
func (r *RaftBackend) beginVoterChange(ctx context.Context, did string, add bool) (<-chan error, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.node.Status().Lead != r.selfID {
		return nil, fmt.Errorf("raft: voter change must be proposed by leader")
	}
	id := r.peerIDs[did]
	if id == 0 {
		return nil, fmt.Errorf("raft: unknown observer %q", did)
	}
	changeType := raftpb.ConfChangeAddNode
	if !add {
		changeType = raftpb.ConfChangeRemoveNode
	}
	r.changeSeq++
	token := strconv.FormatUint(r.changeSeq, 10)
	done := make(chan error, 1)
	if r.changes == nil {
		r.changes = make(map[string]chan error)
	}
	r.changes[token] = done
	// ConfChangeV2 is used even for one change. etcd/raft serializes it with
	// pending config changes and handles joint configuration when required.
	if err := r.node.ProposeConfChange(ctx, raftpb.ConfChangeV2{Context: []byte(token), Changes: []raftpb.ConfChangeSingle{{Type: changeType, NodeID: id}}}); err != nil {
		delete(r.changes, token)
		return nil, err
	}
	return done, nil
}

// ProposeVoterChange waits until the exact ConfChangeV2 is committed and
// applied locally. A successful return is therefore safe to pair with a
// durable membership-role transition.
func (r *RaftBackend) ProposeVoterChange(ctx context.Context, did string, add bool) error {
	done, err := r.beginVoterChange(ctx, did, add)
	if err != nil {
		return err
	}
	select {
	case err := <-done:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

// BeginVoterChange proposes without waiting; actor callers use the returned
// completion channel so they never block Receive while Raft needs ticks to
// commit the same change.
func (r *RaftBackend) BeginVoterChange(ctx context.Context, did string, add bool) (<-chan error, error) {
	return r.beginVoterChange(ctx, did, add)
}

func (r *RaftBackend) completeVoterChange(context []byte, err error) {
	if len(context) == 0 {
		return
	}
	r.mu.Lock()
	done := r.changes[string(context)]
	delete(r.changes, string(context))
	r.mu.Unlock()
	if done != nil {
		done <- err
		close(done)
	}
}

func newRaftBackend(
	thread *pb.Thread,
	id *identity.Identity,
	store *Store,
	log *zap.Logger,
	onCommit CommitCallback,
) (*RaftBackend, error) {
	// Build peer ID maps (stable: index in ReplicaDids, 1-based).
	peerIDs := make(map[string]uint64)
	didByID := make(map[uint64]string)
	var selfID uint64
	for i, did := range thread.ReplicaDids {
		rid := uint64(i + 1)
		peerIDs[did] = rid
		didByID[rid] = did
		if did == id.DID {
			selfID = rid
		}
	}
	if selfID == 0 {
		return nil, fmt.Errorf("raft: local DID %q not in ReplicaDids", id.DID)
	}

	// Use thread.N (the BFT-derived cluster size) rather than the full replica
	// list. Extras are non-voting observers.
	n := int(thread.N)
	if n <= 0 || n > len(thread.ReplicaDids) {
		n = len(thread.ReplicaDids)
	}
	peers := make([]raft.Peer, n)
	voters := make([]uint64, n)
	for i := 0; i < n; i++ {
		id := peerIDs[thread.ReplicaDids[i]]
		peers[i] = raft.Peer{ID: id}
		voters[i] = id
	}

	storage := raft.NewMemoryStorage()
	// Any claim left behind belonged to an uncommitted proposal. A committed
	// proposal removes its inputs in the same transaction as SaveBlock, so all
	// remaining claims are safe to make available again on reconstruction.
	if err := store.ReleasePendingClaims(thread.Id); err != nil {
		return nil, fmt.Errorf("raft: release stale pending claims: %w", err)
	}

	// Prefer the exact persisted Raft log/snapshot. Legacy databases fall back
	// to block reconstruction once and populate the exact log on subsequent
	// Ready batches.
	commitIndex, err := store.GetCommittedHeight(thread.Id)
	if err != nil {
		return nil, err
	}
	snapshot, hardState, raftEntries, err := store.LoadRaftState(thread.Id)
	if err != nil {
		return nil, fmt.Errorf("raft: load durable state: %w", err)
	}
	hasExactState := !raft.IsEmptySnap(snapshot) || !raft.IsEmptyHardState(hardState) || len(raftEntries) > 0
	var lastLogIndex uint64
	if !raft.IsEmptySnap(snapshot) {
		if err := storage.ApplySnapshot(snapshot); err != nil {
			return nil, fmt.Errorf("raft: apply snapshot: %w", err)
		}
		lastLogIndex = snapshot.Metadata.Index
	}
	if len(raftEntries) > 0 {
		if err := storage.Append(raftEntries); err != nil {
			return nil, fmt.Errorf("raft: reload exact log: %w", err)
		}
		lastLogIndex = raftEntries[len(raftEntries)-1].Index
	}
	if hasExactState && !raft.IsEmptyHardState(hardState) {
		if err := storage.SetHardState(hardState); err != nil {
			return nil, fmt.Errorf("raft: restore hard state: %w", err)
		}
	} else if commitIndex > 0 {
		var raftEntries []raftpb.Entry
		blocks, err := store.GetBlocksSince(thread.Id, 0, int(commitIndex))
		if err != nil {
			return nil, err
		}
		for _, b := range blocks {
			data, _ := json.Marshal(b.Entries)
			raftEntries = append(raftEntries, raftpb.Entry{
				Type:  raftpb.EntryNormal,
				Index: uint64(b.Height),
				Term:  uint64(b.Round), // we store raft term in Round
				Data:  data,
			})
		}
		if len(raftEntries) > 0 {
			if err := storage.Append(raftEntries); err != nil {
				return nil, fmt.Errorf("raft: reload log: %w", err)
			}
			lastLogIndex = raftEntries[len(raftEntries)-1].Index
		}
	}

	// Legacy HardState restoration after the reconstructed log is loaded.
	cs, err := store.LoadConsensusState(thread.Id)
	if err != nil {
		return nil, err
	}
	if !hasExactState && (cs.Height > 1 || cs.Round > 0) {
		var vote uint64
		if cs.Step != "" {
			vote, _ = strconv.ParseUint(cs.Step, 10, 64)
		}
		commit := uint64(cs.Round)
		if commit > lastLogIndex {
			commit = lastLogIndex // clamp to avoid raft panic
		}
		hs := raftpb.HardState{
			Term:   uint64(cs.Height),
			Vote:   vote,
			Commit: commit,
		}
		if err := storage.SetHardState(hs); err != nil {
			return nil, fmt.Errorf("raft: set hard state: %w", err)
		}
	}

	cfg := &raft.Config{
		ID:              selfID,
		ElectionTick:    raftElectionTicks,
		HeartbeatTick:   raftHeartbeatTicks,
		Storage:         storage,
		MaxSizePerMsg:   1 << 20, // 1 MB
		MaxInflightMsgs: 256,
		Logger:          &raftZapLogger{log},
	}

	var node raft.Node
	if !hasExactState && commitIndex == 0 {
		node = raft.StartNode(cfg, peers)
	} else {
		node = raft.RestartNode(cfg)
	}

	return &RaftBackend{
		thread:       thread,
		id:           id,
		store:        store,
		log:          log,
		onCommit:     onCommit,
		selfID:       selfID,
		peerIDs:      peerIDs,
		didByID:      didByID,
		node:         node,
		storage:      storage,
		appliedIndex: snapshot.Metadata.Index,
		appliedTerm:  snapshot.Metadata.Term,
		confState: func() raftpb.ConfState {
			if !raft.IsEmptySnap(snapshot) {
				return snapshot.Metadata.ConfState
			}
			return raftpb.ConfState{Voters: voters}
		}(),
		inboundCh: make(chan *pb.ConsensusMsg, 512),
	}, nil
}

func (r *RaftBackend) Deliver(msg *pb.ConsensusMsg) {
	select {
	case r.inboundCh <- msg:
	default:
		r.log.Warn("raft: inbound channel full, dropping", zap.String("thread", r.thread.Id))
	}
}

func (r *RaftBackend) Subscribe() <-chan *pb.ThreadEntryWithPos    { return nil }
func (r *RaftBackend) Unsubscribe(_ <-chan *pb.ThreadEntryWithPos) {}

func (r *RaftBackend) Run(ctx context.Context, broadcast func(*pb.ConsensusMsg)) {
	ticker := time.NewTicker(time.Duration(raftTickMs) * time.Millisecond)
	defer ticker.Stop()

	r.log.Info("raft: starting",
		zap.String("thread", r.thread.Id),
		zap.Uint64("selfID", r.selfID),
	)

	for {
		select {
		case <-ctx.Done():
			r.node.Stop()
			return

		case <-ticker.C:
			if r.pendingReady != nil {
				r.processReady(*r.pendingReady, broadcast)
				continue
			}
			r.node.Tick()
			r.proposePending(ctx)

		case rd := <-r.node.Ready():
			r.processReady(rd, broadcast)

		case msg := <-r.inboundCh:
			r.handleInbound(ctx, msg)
		}
	}
}

// ─── Single-step API for callers that drive the Ready loop themselves ────────
// (e.g. ThreadActor's message-typed Receive, see actor.go, which must never
// block per GoAkt's framework rule — unlike Run's blocking select above).

// Tick advances the raft clock by one tick and proposes any pending entries
// if we are leader. Callers should call TryReady afterward to drain Ready.
func (r *RaftBackend) Tick(ctx context.Context) {
	if r.pendingReady != nil {
		return
	}
	r.node.Tick()
	r.proposePending(ctx)
}

// TryReady drains one Ready batch if available, without blocking. Returns
// true if a batch was processed. This is the well-known etcd/raft
// non-blocking idiom: select on Ready() with a default case.
func (r *RaftBackend) TryReady(broadcast func(*pb.ConsensusMsg)) bool {
	if r.pendingReady != nil {
		return r.processReady(*r.pendingReady, broadcast)
	}
	select {
	case rd := <-r.node.Ready():
		return r.processReady(rd, broadcast)
	default:
		return false
	}
}

func (r *RaftBackend) processReady(rd raft.Ready, broadcast func(*pb.ConsensusMsg)) bool {
	if !r.handleReady(rd, broadcast) {
		copy := rd
		r.pendingReady = &copy
		return false
	}
	r.pendingReady = nil
	r.node.Advance()
	return true
}

// StepInbound feeds one inbound ConsensusMsg (from Deliver's channel or
// directly from a Tell) into the raft node and proposes pending entries if
// we became leader as a result.
func (r *RaftBackend) StepInbound(ctx context.Context, msg *pb.ConsensusMsg) {
	r.handleInbound(ctx, msg)
}

// TryDrainInbound pops and steps one message queued via Deliver, without
// blocking. Returns true if a message was processed. Callers must not call
// this (or StepInbound) from inside handleReady/broadcast — see the comment
// on ThreadActor's bcast in actor.go for why: Deliver queues instead of
// stepping synchronously specifically so this drain can happen from a point
// outside any in-flight Ready/Advance pair.
func (r *RaftBackend) TryDrainInbound(ctx context.Context) bool {
	select {
	case msg := <-r.inboundCh:
		r.handleInbound(ctx, msg)
		return true
	default:
		return false
	}
}

// pumpMaxIterations bounds Pump's loop so a pathological message storm can't
// starve the actor's mailbox — the next TickMsg (100ms away) picks up
// wherever this call left off.
const pumpMaxIterations = 64

// pumpIdleYields bounds how many scheduler yields Pump spends waiting for
// etcd/raft's internal goroutine to catch up on a fire-and-forget Tick or
// Advance before giving up for this call.
const pumpIdleYields = 32

// Pump alternates draining queued inbound messages and Ready batches until
// both are empty (or pumpMaxIterations is hit), never blocking. A single
// raft state transition can chain several hops — e.g. tick → campaign →
// vote request queued via broadcast → drained here → vote response fed to
// Step → new Ready produced with the leader transition — and each hop only
// becomes visible after the previous one's Ready/Advance pair has fully
// completed. Draining once per call (as TryReady/TryDrainInbound do alone)
// only advances one hop per call; on a single-node cluster that stalls
// election indefinitely if the caller (e.g. an actor's Receive) is only
// invoked once per tick. Call this instead of TryReady/TryDrainInbound
// directly unless you specifically need single-hop control.
func (r *RaftBackend) Pump(ctx context.Context, broadcast func(*pb.ConsensusMsg)) {
	idle := 0
	for i := 0; i < pumpMaxIterations; i++ {
		drained := r.TryDrainInbound(ctx)
		ready := r.TryReady(broadcast)
		if drained || ready {
			idle = 0
			continue
		}
		// etcd/raft's async Node (see go.etcd.io/raft/v3's node.run()) is
		// designed around a dedicated goroutine that loops tightly on
		// Ready()/Advance() with no gaps — that assumption is baked into
		// its internal channel protocol. Driving it instead from sporadic,
		// scheduled polls (one per TickMsg, 100ms apart) measurably breaks
		// that assumption: on a single-node cluster, a candidacy's self-vote
		// can fail to resolve across five consecutive 100ms-apart polls
		// (a full election timeout, confirmed empirically — this is not a
		// microsecond scheduling gap that "the next tick" papers over) and
		// the node cycles through repeated failed candidacies indefinitely.
		// Spinning a bounded number of scheduler yields per Pump call closes
		// that gap by giving the raft goroutine enough back-to-back
		// opportunities to run inside one Tick's worth of work, without
		// ever blocking (Gosched never parks the caller). Confirmed via
		// go.etcd.io/raft/v3 v3.6.0; a different raft version's internal
		// goroutine scheduling could change this constant's sufficiency.
		if idle >= pumpIdleYields {
			return
		}
		idle++
		runtime.Gosched()
	}
}

// Stop stops the underlying raft node. Call from PostStop.
func (r *RaftBackend) Stop() {
	r.node.Stop()
}

// Snapshot persists a compact etcd/raft restart point. Thread blocks remain
// untouched as the auditable application history.
func (r *RaftBackend) Snapshot() error {
	if r.appliedIndex == 0 {
		return nil
	}
	snapshot, err := r.storage.CreateSnapshot(r.appliedIndex, &r.confState, nil)
	if err != nil && !errors.Is(err, raft.ErrSnapOutOfDate) {
		return err
	}
	if err == nil {
		if err := r.store.SaveRaftSnapshot(r.thread.Id, snapshot); err != nil {
			return err
		}
		if err := r.storage.Compact(r.appliedIndex); err != nil && !errors.Is(err, raft.ErrCompacted) {
			return err
		}
	}
	return nil
}

// ThreadID returns the ID of the thread this backend drives.
func (r *RaftBackend) ThreadID() string { return r.thread.Id }

// handleReady processes one Ready batch from etcd raft.
func (r *RaftBackend) handleReady(rd raft.Ready, broadcast func(*pb.ConsensusMsg)) bool {
	if err := r.store.SaveRaftReady(r.thread.Id, rd.HardState, rd.Entries); err != nil {
		appactors.Metrics.RaftReadyFailed()
		r.log.Error("raft: persist Ready", zap.Error(err))
		return false
	}
	// 1. Persist HardState if changed.
	if !raft.IsEmptyHardState(rd.HardState) {
		r.persistHardState(rd.HardState)
		r.storage.SetHardState(rd.HardState) //nolint:errcheck
	}

	// 2. Append new log entries to MemoryStorage.
	if len(rd.Entries) > 0 {
		r.storage.Append(rd.Entries) //nolint:errcheck
	}

	// 3. Send outbound messages to peers via broadcast.
	for _, m := range rd.Messages {
		r.sendRaftMsg(m, broadcast)
	}

	// 4. Apply committed entries.
	for _, entry := range rd.CommittedEntries {
		r.appliedIndex, r.appliedTerm = entry.Index, entry.Term
		switch entry.Type {
		case raftpb.EntryConfChange:
			// StartNode synthesizes one of these per initial peer. Until
			// ApplyConfChange is called, raft holds pendingConfIndex set
			// and refuses to campaign ("cannot campaign ... since there
			// are still pending configuration changes to apply") —
			// without this, single-node (and any) clusters can stall
			// leader election indefinitely.
			var cc raftpb.ConfChange
			if err := cc.Unmarshal(entry.Data); err != nil {
				r.log.Error("raft: unmarshal conf change", zap.Error(err))
				continue
			}
			if cs := r.node.ApplyConfChange(cc); cs != nil {
				r.confState = *cs
			}
		case raftpb.EntryConfChangeV2:
			var cc raftpb.ConfChangeV2
			if err := cc.Unmarshal(entry.Data); err != nil {
				r.log.Error("raft: unmarshal joint conf change", zap.Error(err))
				continue
			}
			if cs := r.node.ApplyConfChange(cc); cs != nil {
				r.confState = *cs
			}
			r.completeVoterChange(cc.Context, nil)

		case raftpb.EntryNormal:
			if len(entry.Data) == 0 {
				continue
			}
			var proposal raftProposal
			if err := json.Unmarshal(entry.Data, &proposal); err != nil {
				// Compatibility with logs written before the durable claim envelope.
				if err := json.Unmarshal(entry.Data, &proposal.Entries); err != nil {
					r.log.Error("raft: unmarshal entries", zap.Error(err))
					continue
				}
			}
			if proposal.OwnerDID == "" {
				proposal.OwnerDID = r.id.DID
			}
			pendingIDs := proposal.PendingIDs
			if proposal.OwnerDID != r.id.DID {
				pendingIDs = nil
			}
			r.commitBlock(entry, proposal.Entries, pendingIDs, proposal.OwnerDID, broadcast)
		}
	}
	return true
}

// handleInbound decodes a ConsensusMsg and feeds the raftpb.Message to the node,
// or applies a committed block broadcast from the leader.
func (r *RaftBackend) handleInbound(ctx context.Context, msg *pb.ConsensusMsg) {
	// Committed block broadcast — applies directly to non-voter replicas.
	if block := msg.GetCommittedBlock(); block != nil {
		r.applyCommittedBlock(block)
		return
	}

	raw := msg.GetRaftAppendEntries()
	if raw == nil {
		return
	}
	// We reuse RaftAppendEntries.Signature to carry the JSON-encoded raftpb.Message.
	var rm raftpb.Message
	if err := json.Unmarshal([]byte(raw.Signature), &rm); err != nil {
		r.log.Debug("raft: decode message", zap.Error(err))
		return
	}
	if err := r.node.Step(ctx, rm); err != nil {
		r.log.Debug("raft: step", zap.Error(err))
	}

	// If we are leader, also propose any pending entries.
	r.proposePending(ctx)
}

// applyCommittedBlock saves a block received from the leader to the local store.
// Used by non-voter replicas (f=0 observers) that are not part of the raft quorum.
func (r *RaftBackend) applyCommittedBlock(block *pb.ThreadBlock) {
	// Check if we already have this block (idempotent).
	existing, err := r.store.GetBlock(r.thread.Id, block.Height)
	if err == nil && existing != nil {
		return
	}
	if err := r.store.SaveBlock(block); err != nil {
		r.log.Warn("raft: apply committed block", zap.Int64("height", block.Height), zap.Error(err))
		return
	}
	r.applyMembershipEntries(block.Entries)
	r.log.Info("raft: applied committed block from leader",
		zap.String("thread", r.thread.Id),
		zap.Int64("height", block.Height),
	)
	if r.onCommit != nil {
		r.onCommit(block)
	}
}

// proposePending drains the store queue and proposes to raft if we are leader.
func (r *RaftBackend) proposePending(ctx context.Context) {
	status := r.node.Status()
	if status.Lead != r.selfID {
		return
	}
	batch, err := r.store.ClaimPendingEntries(r.thread.Id, raftMaxEntriesPerMsg)
	if err != nil || len(batch.Entries) == 0 {
		return
	}
	data, err := json.Marshal(raftProposal{PendingIDs: batch.IDs, Entries: batch.Entries, OwnerDID: r.id.DID})
	if err != nil {
		_ = r.store.ReleasePendingClaimIDs(batch.IDs)
		return
	}
	if err := r.node.Propose(ctx, data); err != nil {
		r.log.Warn("raft: propose", zap.Error(err))
		_ = r.store.ReleasePendingClaimIDs(batch.IDs)
	}
}

// sendRaftMsg encodes a raftpb.Message into ConsensusMsg and broadcasts it.
// We reuse RaftAppendEntries with Signature holding the JSON payload.
func (r *RaftBackend) sendRaftMsg(m raftpb.Message, broadcast func(*pb.ConsensusMsg)) {
	data, err := json.Marshal(m)
	if err != nil {
		return
	}
	toDID := r.didByID[m.To]
	if toDID == "" && m.To != 0 {
		return // unknown peer
	}

	broadcast(&pb.ConsensusMsg{
		ThreadId: r.thread.Id,
		Payload: &pb.ConsensusMsg_RaftAppendEntries{
			RaftAppendEntries: &pb.RaftAppendEntries{
				Term:      int64(m.Term),
				LeaderDid: r.id.DID,
				// Signature carries the raw raftpb.Message JSON.
				Signature: string(data),
			},
		},
	})
}

// commitBlock saves a committed block to SQLite, calls onCommit, and broadcasts
// the block to all replicas via GossipSub so non-voter nodes can apply it.
func (r *RaftBackend) commitBlock(entry raftpb.Entry, entries []*pb.ThreadEntry, pendingIDs []int64, proposerDID string, broadcast func(*pb.ConsensusMsg)) {
	// Use our own sequential height (committed block count + 1),
	// not the raft log index (which includes config/no-op entries).
	height, err := r.store.GetCommittedHeight(r.thread.Id)
	if err != nil {
		r.log.Error("raft: get committed height", zap.Error(err))
		return
	}
	height++

	parentHash := ""
	if height > 1 {
		if prev, err := r.store.GetBlock(r.thread.Id, height-1); err == nil {
			parentHash = prev.BlockHash
		}
	}

	block := &pb.ThreadBlock{
		ThreadId:    r.thread.Id,
		Height:      height,
		Round:       int32(entry.Term),
		ParentHash:  parentHash,
		Entries:     entries,
		ProposerDid: proposerDID,
		ProposerSig: "",
		CommittedAt: time.Now().UnixMilli(),
	}
	block.BlockHash = raftBlockHash(block)

	if err := r.store.SaveBlockAndAckPending(block, pendingIDs); err != nil {
		r.log.Error("raft: save block", zap.Error(err))
		return
	}
	r.applyMembershipEntries(entries)

	r.log.Info("raft: block committed",
		zap.String("thread", r.thread.Id),
		zap.Int64("height", height),
		zap.String("hash", block.BlockHash[:8]+"..."),
		zap.Int("entries", len(entries)),
	)

	// Broadcast committed block so non-voter replicas (f=0 observers) can apply it.
	if broadcast != nil {
		broadcast(&pb.ConsensusMsg{
			ThreadId: r.thread.Id,
			Payload:  &pb.ConsensusMsg_CommittedBlock{CommittedBlock: block},
		})
	}

	if r.onCommit != nil {
		r.onCommit(block)
	}
}

// applyMembershipEntries updates the durable observer set only after the
// membership command is committed. Existing Raft voters are deliberately not
// changed: voter promotion/removal requires etcd/raft joint consensus.
func (r *RaftBackend) applyMembershipEntries(entries []*pb.ThreadEntry) {
	if r.id.DID != r.thread.CreatorDid {
		return
	}
	changed := false
	for _, entry := range entries {
		if entry.Kind != "membership:add-observer" {
			continue
		}
		did := string(entry.Payload)
		if did == "" {
			continue
		}
		if _, exists := r.peerIDs[did]; exists {
			continue
		}
		r.thread.ReplicaDids = append(r.thread.ReplicaDids, did)
		rid := uint64(len(r.thread.ReplicaDids))
		r.peerIDs[did], r.didByID[rid] = rid, did
		changed = true
	}
	if changed {
		if err := SignDescriptor(r.thread, r.id); err != nil {
			r.log.Error("raft: sign committed membership", zap.Error(err))
			return
		}
		if err := r.store.SaveThread(r.thread); err != nil {
			r.log.Error("raft: save committed membership", zap.Error(err))
		}
	}
}

// persistHardState saves Term/Vote/Commit to SQLite ConsensusState.
func (r *RaftBackend) persistHardState(hs raftpb.HardState) {
	r.store.SaveConsensusState(r.thread.Id, ConsensusState{ //nolint:errcheck
		Height: int64(hs.Term),
		Round:  int32(hs.Commit),
		Step:   strconv.FormatUint(hs.Vote, 10),
	})
}

// raftBlockHash produces a deterministic hash for a committed block.
func raftBlockHash(b *pb.ThreadBlock) string {
	h := sha256.New()
	fmt.Fprintf(h, "raft:%s:%d:%d:%s:%s:",
		b.ThreadId, b.Height, b.Round, b.ParentHash, b.ProposerDid,
	)
	for _, entry := range b.Entries {
		data, err := proto.MarshalOptions{Deterministic: true}.Marshal(entry)
		if err != nil {
			continue
		}
		_, _ = h.Write(data)
	}
	return hex.EncodeToString(h.Sum(nil))
}

// ─── raftZapLogger bridges etcd raft's Logger to zap ──────────────────────────

type raftZapLogger struct{ log *zap.Logger }

func (l *raftZapLogger) Debug(v ...interface{}) {
	l.log.Sugar().Debug(v...)
}
func (l *raftZapLogger) Debugf(format string, v ...interface{}) {
	l.log.Sugar().Debugf(format, v...)
}
func (l *raftZapLogger) Info(v ...interface{}) {
	l.log.Sugar().Info(v...)
}
func (l *raftZapLogger) Infof(format string, v ...interface{}) {
	l.log.Sugar().Infof(format, v...)
}
func (l *raftZapLogger) Warning(v ...interface{}) {
	l.log.Sugar().Warn(v...)
}
func (l *raftZapLogger) Warningf(format string, v ...interface{}) {
	l.log.Sugar().Warnf(format, v...)
}
func (l *raftZapLogger) Error(v ...interface{}) {
	l.log.Sugar().Error(v...)
}
func (l *raftZapLogger) Errorf(format string, v ...interface{}) {
	l.log.Sugar().Errorf(format, v...)
}
func (l *raftZapLogger) Fatal(v ...interface{}) {
	l.log.Sugar().Fatal(v...)
}
func (l *raftZapLogger) Fatalf(format string, v ...interface{}) {
	l.log.Sugar().Fatalf(format, v...)
}
func (l *raftZapLogger) Panic(v ...interface{}) {
	l.log.Sugar().Panic(v...)
}
func (l *raftZapLogger) Panicf(format string, v ...interface{}) {
	l.log.Sugar().Panicf(format, v...)
}
