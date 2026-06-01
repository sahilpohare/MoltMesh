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
	"fmt"
	"strconv"
	"sync"
	"time"

	"go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"
	"go.uber.org/zap"

	pb "github.com/sahilpohare/p2p-a2a/gen/a2a/v1"
	"github.com/sahilpohare/p2p-a2a/daemon/identity"
)

const (
	raftTickMs           = 100  // one tick = 100 ms
	raftHeartbeatTicks   = 1    // leader sends heartbeat every tick
	raftElectionTicks    = 5    // follower times out after 5 ticks (500 ms)
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

	node    raft.Node
	storage *raft.MemoryStorage

	mu        sync.Mutex
	inboundCh chan *pb.ConsensusMsg
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

	storage := raft.NewMemoryStorage()

	// Reload committed log entries into MemoryStorage first.
	commitIndex, err := store.GetCommittedHeight(thread.Id)
	if err != nil {
		return nil, err
	}
	var lastLogIndex uint64
	if commitIndex > 0 {
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

	// Restore HardState after log is loaded so Commit never exceeds last log index.
	cs, err := store.LoadConsensusState(thread.Id)
	if err != nil {
		return nil, err
	}
	if cs.Height > 1 || cs.Round > 0 {
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

	// Build peer list for initial cluster.
	// Use thread.N (the BFT-derived cluster size) rather than the full replica
	// list, so that f=0 (n=1) creates a single-node raft with quorum=1.
	// The first N replicas form the voting set; extras are non-voting observers.
	n := int(thread.N)
	if n <= 0 || n > len(thread.ReplicaDids) {
		n = len(thread.ReplicaDids)
	}
	peers := make([]raft.Peer, n)
	for i := 0; i < n; i++ {
		peers[i] = raft.Peer{ID: peerIDs[thread.ReplicaDids[i]]}
	}

	cfg := &raft.Config{
		ID:                        selfID,
		ElectionTick:              raftElectionTicks,
		HeartbeatTick:             raftHeartbeatTicks,
		Storage:                   storage,
		MaxSizePerMsg:             1 << 20, // 1 MB
		MaxInflightMsgs:           256,
		Logger:                    &raftZapLogger{log},
	}

	var node raft.Node
	if commitIndex == 0 {
		node = raft.StartNode(cfg, peers)
	} else {
		node = raft.RestartNode(cfg)
	}

	return &RaftBackend{
		thread:    thread,
		id:        id,
		store:     store,
		log:       log,
		onCommit:  onCommit,
		selfID:    selfID,
		peerIDs:   peerIDs,
		didByID:   didByID,
		node:      node,
		storage:   storage,
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

func (r *RaftBackend) Subscribe() <-chan *pb.ThreadEntryWithPos   { return nil }
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
			r.node.Tick()
			r.proposePending(ctx)

		case rd := <-r.node.Ready():
			r.handleReady(rd, broadcast)
			r.node.Advance()

		case msg := <-r.inboundCh:
			r.handleInbound(ctx, msg)
		}
	}
}

// handleReady processes one Ready batch from etcd raft.
func (r *RaftBackend) handleReady(rd raft.Ready, broadcast func(*pb.ConsensusMsg)) {
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
		if entry.Type != raftpb.EntryNormal || len(entry.Data) == 0 {
			continue
		}
		var entries []*pb.ThreadEntry
		if err := json.Unmarshal(entry.Data, &entries); err != nil {
			r.log.Error("raft: unmarshal entries", zap.Error(err))
			continue
		}
		r.commitBlock(entry, entries, broadcast)
	}
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
	pending, err := r.store.DequeuePendingEntries(r.thread.Id, raftMaxEntriesPerMsg)
	if err != nil || len(pending) == 0 {
		return
	}
	data, err := json.Marshal(pending)
	if err != nil {
		return
	}
	if err := r.node.Propose(ctx, data); err != nil {
		r.log.Warn("raft: propose", zap.Error(err))
		// Re-enqueue on failure.
		for _, e := range pending {
			r.store.EnqueueEntry(r.thread.Id, e) //nolint:errcheck
		}
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
func (r *RaftBackend) commitBlock(entry raftpb.Entry, entries []*pb.ThreadEntry, broadcast func(*pb.ConsensusMsg)) {
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
		ProposerDid: r.id.DID,
		ProposerSig: "",
		CommittedAt: time.Now().UnixMilli(),
	}
	block.BlockHash = raftBlockHash(block)

	if err := r.store.SaveBlock(block); err != nil {
		r.log.Error("raft: save block", zap.Error(err))
		return
	}

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
	raw := fmt.Sprintf("raft:%s:%d:%d:%s:%s",
		b.ThreadId, b.Height, b.Round, b.ParentHash, b.ProposerDid,
	)
	h := sha256.Sum256([]byte(raw))
	return hex.EncodeToString(h[:])
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
