package thread

import (
	"context"
	"encoding/hex"
	"sync"
	"testing"
	"time"

	"go.uber.org/zap"

	pb "github.com/sahilpohare/p2p-a2a/gen/a2a/v1"
	"github.com/sahilpohare/p2p-a2a/daemon/identity"
)

// ─── helpers ──────────────────────────────────────────────────────────────────

func mustIdentity(t *testing.T) *identity.Identity {
	t.Helper()
	id, err := identity.Generate()
	if err != nil {
		t.Fatalf("identity.Generate: %v", err)
	}
	return id
}

func newTendermintTestStore(t *testing.T) *Store {
	t.Helper()
	s, err := NewStore(":memory:")
	if err != nil {
		t.Fatalf("NewStore: %v", err)
	}
	t.Cleanup(func() { s.Close() })
	return s
}

// makeThread builds a Thread with N=3f+1 replicas.
func makeThread(ids []*identity.Identity, f int32) *pb.Thread {
	dids := make([]string, len(ids))
	for i, id := range ids {
		dids[i] = id.DID
	}
	return &pb.Thread{
		Id:          "test-thread",
		CreatorDid:  dids[0],
		ReplicaDids: dids,
		N:           int32(len(ids)),
		F:           f,
		EpochMs:     100,
	}
}

// signedVote creates a properly-signed Vote.
func signedVote(id *identity.Identity, thread *pb.Thread, height int64, round int32, vtype pb.VoteType, blockHash string) *pb.Vote {
	sigData := voteSigData(thread.Id, height, round, vtype, blockHash)
	sig := id.Sign(sigData)
	return &pb.Vote{
		ThreadId:  thread.Id,
		Height:    height,
		Round:     round,
		Type:      vtype,
		BlockHash: blockHash,
		VoterDid:  id.DID,
		Signature: hex.EncodeToString(sig),
	}
}

// signedProposal creates a signed Proposal with a valid block.
func signedProposal(id *identity.Identity, thread *pb.Thread, height int64, round, polRound int32, entries []*pb.ThreadEntry) *pb.Proposal {
	block := &pb.ThreadBlock{
		ThreadId:    thread.Id,
		Height:      height,
		Round:       round,
		ParentHash:  "",
		Entries:     entries,
		ProposerDid: id.DID,
	}
	block.BlockHash = computeBlockHash(block)

	sigData := proposalSigData(thread.Id, height, round, block.BlockHash)
	sig := id.Sign(sigData)
	block.ProposerSig = hex.EncodeToString(sig)

	return &pb.Proposal{
		ThreadId:    thread.Id,
		Height:      height,
		Round:       round,
		PolRound:    polRound,
		Block:       block,
		ProposerDid: id.DID,
		Signature:   hex.EncodeToString(sig),
	}
}

func zapDev() *zap.Logger {
	l, _ := zap.NewDevelopment()
	return l
}

// ─── majority() ───────────────────────────────────────────────────────────────

func TestMajority_Empty(t *testing.T) {
	hash, count := majority(nil)
	if hash != "" || count != 0 {
		t.Errorf("expected (\"\", 0), got (%q, %d)", hash, count)
	}
}

func TestMajority_SingleVote(t *testing.T) {
	votes := []*pb.Vote{{BlockHash: "abc"}}
	hash, count := majority(votes)
	if hash != "abc" || count != 1 {
		t.Errorf("got (%q, %d)", hash, count)
	}
}

func TestMajority_ClearWinner(t *testing.T) {
	votes := []*pb.Vote{
		{BlockHash: "win"},
		{BlockHash: "win"},
		{BlockHash: "win"},
		{BlockHash: "lose"},
	}
	hash, count := majority(votes)
	if hash != "win" || count != 3 {
		t.Errorf("got (%q, %d)", hash, count)
	}
}

func TestMajority_NilVoteMajority(t *testing.T) {
	votes := []*pb.Vote{
		{BlockHash: ""},
		{BlockHash: ""},
		{BlockHash: ""},
	}
	hash, count := majority(votes)
	if hash != "" || count != 3 {
		t.Errorf("expected nil majority (3), got (%q, %d)", hash, count)
	}
}

func TestMajority_Tie(t *testing.T) {
	votes := []*pb.Vote{
		{BlockHash: "aaa"},
		{BlockHash: "bbb"},
		{BlockHash: "aaa"},
		{BlockHash: "bbb"},
	}
	_, count := majority(votes)
	if count != 2 {
		t.Errorf("expected count 2 on tie, got %d", count)
	}
}

// ─── computeBlockHash ─────────────────────────────────────────────────────────

func TestComputeBlockHash_Deterministic(t *testing.T) {
	block := &pb.ThreadBlock{
		ThreadId:    "t1",
		Height:      1,
		Round:       0,
		ParentHash:  "",
		ProposerDid: "did:key:z123",
	}
	if h1, h2 := computeBlockHash(block), computeBlockHash(block); h1 != h2 {
		t.Error("computeBlockHash not deterministic")
	}
	if len(computeBlockHash(block)) != 64 {
		t.Error("expected 64-char hex hash")
	}
}

func TestComputeBlockHash_DifferentInputs(t *testing.T) {
	b1 := &pb.ThreadBlock{ThreadId: "t1", Height: 1, ProposerDid: "did:key:zA"}
	b2 := &pb.ThreadBlock{ThreadId: "t1", Height: 2, ProposerDid: "did:key:zA"}
	if computeBlockHash(b1) == computeBlockHash(b2) {
		t.Error("different blocks should produce different hashes")
	}
}

// ─── Store dedup invariant ────────────────────────────────────────────────────

func TestStore_SaveVote_Dedup(t *testing.T) {
	ids := []*identity.Identity{mustIdentity(t), mustIdentity(t), mustIdentity(t), mustIdentity(t)}
	thread := makeThread(ids, 1)
	store := newTendermintTestStore(t)
	store.SaveThread(thread) //nolint:errcheck

	v := signedVote(ids[1], thread, 1, 0, pb.VoteType_VOTE_TYPE_PREVOTE, "hash1")
	store.SaveVote(v) //nolint:errcheck
	store.SaveVote(v) // duplicate — INSERT OR IGNORE

	votes, err := store.GetVotes(thread.Id, 1, 0, pb.VoteType_VOTE_TYPE_PREVOTE)
	if err != nil {
		t.Fatalf("GetVotes: %v", err)
	}
	if len(votes) != 1 {
		t.Errorf("expected 1 deduplicated vote, got %d", len(votes))
	}
}

func TestStore_SaveVote_DifferentVoters(t *testing.T) {
	ids := []*identity.Identity{mustIdentity(t), mustIdentity(t), mustIdentity(t), mustIdentity(t)}
	thread := makeThread(ids, 1)
	store := newTendermintTestStore(t)
	store.SaveThread(thread) //nolint:errcheck

	for _, id := range ids {
		v := signedVote(id, thread, 1, 0, pb.VoteType_VOTE_TYPE_PREVOTE, "hashA")
		store.SaveVote(v) //nolint:errcheck
	}

	votes, err := store.GetVotes(thread.Id, 1, 0, pb.VoteType_VOTE_TYPE_PREVOTE)
	if err != nil {
		t.Fatalf("GetVotes: %v", err)
	}
	if len(votes) != len(ids) {
		t.Errorf("expected %d votes, got %d", len(ids), len(votes))
	}
}

// ─── bufferFutureVote ─────────────────────────────────────────────────────────

func TestBufferFutureVote_Deduplication(t *testing.T) {
	ids := []*identity.Identity{mustIdentity(t), mustIdentity(t), mustIdentity(t), mustIdentity(t)}
	thread := makeThread(ids, 1)
	store := newTendermintTestStore(t)

	e := &TendermintBackend{
		thread:      thread,
		id:          ids[0],
		store:       store,
		futureVotes: make(map[int32][]*pb.Vote),
		cs:          ConsensusState{Height: 1, Round: 0, Step: stepPropose, LockedRound: -1, ValidRound: -1},
	}

	v := signedVote(ids[1], thread, 1, 2, pb.VoteType_VOTE_TYPE_PREVOTE, "hash1")
	e.bufferFutureVote(v)
	e.bufferFutureVote(v) // duplicate

	if len(e.futureVotes[2]) != 1 {
		t.Errorf("expected 1 buffered vote, got %d", len(e.futureVotes[2]))
	}
}

func TestBufferFutureVote_DifferentVoters(t *testing.T) {
	ids := []*identity.Identity{mustIdentity(t), mustIdentity(t), mustIdentity(t), mustIdentity(t)}
	thread := makeThread(ids, 1)
	store := newTendermintTestStore(t)

	e := &TendermintBackend{
		thread:      thread,
		id:          ids[0],
		store:       store,
		futureVotes: make(map[int32][]*pb.Vote),
		cs:          ConsensusState{Height: 1, Round: 0, Step: stepPropose, LockedRound: -1, ValidRound: -1},
	}

	for _, id := range ids[1:] {
		v := signedVote(id, thread, 1, 3, pb.VoteType_VOTE_TYPE_PREVOTE, "hashX")
		e.bufferFutureVote(v)
	}

	if len(e.futureVotes[3]) != 3 {
		t.Errorf("expected 3 votes from distinct voters, got %d", len(e.futureVotes[3]))
	}
}

// ─── quorum() ─────────────────────────────────────────────────────────────────

func TestQuorum_F1(t *testing.T) {
	ids := []*identity.Identity{mustIdentity(t), mustIdentity(t), mustIdentity(t), mustIdentity(t)}
	thread := makeThread(ids, 1) // f=1, N=4, quorum=3
	e := &TendermintBackend{thread: thread}
	if q := e.quorum(); q != 3 {
		t.Errorf("expected quorum=3 for f=1, got %d", q)
	}
}

func TestQuorum_F2(t *testing.T) {
	allIds := make([]*identity.Identity, 7)
	for i := range allIds {
		allIds[i] = mustIdentity(t)
	}
	thread := makeThread(allIds, 2) // f=2, N=7, quorum=5
	e := &TendermintBackend{thread: thread}
	if q := e.quorum(); q != 5 {
		t.Errorf("expected quorum=5 for f=2, got %d", q)
	}
}

// ─── isProposer round-robin ───────────────────────────────────────────────────

func TestIsProposer_RoundRobin(t *testing.T) {
	ids := []*identity.Identity{mustIdentity(t), mustIdentity(t), mustIdentity(t), mustIdentity(t)}
	thread := makeThread(ids, 1)
	store := newTendermintTestStore(t)

	for h := int64(1); h <= 8; h++ {
		expectedIdx := (h + 0) % 4
		for i, id := range ids {
			e := &TendermintBackend{
				thread: thread, id: id, store: store,
				cs: ConsensusState{Height: h, Round: 0},
			}
			got := e.isProposer()
			want := int64(i) == expectedIdx
			if got != want {
				t.Errorf("height=%d ids[%d].isProposer()=%v, want %v", h, i, got, want)
			}
		}
	}
}

// ─── handleProposal: unlock-on-polc (B1 fix) ─────────────────────────────────

func TestHandleProposal_UnlockOnPolC(t *testing.T) {
	// f=1, N=4; height=1, round=1; proposer=ids[(1+1)%4]=ids[2]
	ids := []*identity.Identity{mustIdentity(t), mustIdentity(t), mustIdentity(t), mustIdentity(t)}
	thread := makeThread(ids, 1)
	store := newTendermintTestStore(t)
	store.SaveThread(thread) //nolint:errcheck

	proposer := ids[2] // (1+1) % 4 = 2

	e := &TendermintBackend{
		thread:      thread,
		id:          ids[0],
		store:       store,
		futureVotes: make(map[int32][]*pb.Vote),
		cs: ConsensusState{
			Height:      1,
			Round:       1, // current round=1
			Step:        stepPropose,
			LockedRound: 0,
			LockedHash:  "old-hash",
			ValidRound:  -1,
		},
		log: zapDev(),
	}

	// PolRound=1 > LockedRound=0 → supersedes → unlock and prevote B
	prop := signedProposal(proposer, thread, 1, 1, 1, nil)

	prevoted := "sentinel"
	var mu sync.Mutex
	broadcast := func(msg *pb.ConsensusMsg) {
		if v, ok := msg.Payload.(*pb.ConsensusMsg_Vote); ok {
			mu.Lock()
			prevoted = v.Vote.BlockHash
			mu.Unlock()
		}
	}

	timer := time.NewTimer(0)
	timer.Stop()
	e.handleProposal(context.Background(), prop, broadcast, timer)

	mu.Lock()
	defer mu.Unlock()

	if prevoted == "" || prevoted == "sentinel" {
		t.Errorf("expected prevote for new block hash, got %q", prevoted)
	}
	if e.cs.LockedRound != -1 || e.cs.LockedHash != "" {
		t.Errorf("lock not cleared after PoLC supersedes: LockedRound=%d LockedHash=%q",
			e.cs.LockedRound, e.cs.LockedHash)
	}
}

func TestHandleProposal_NoUnlockWithoutPolC(t *testing.T) {
	// Locked on "locked-hash" at round 0; proposal for a different block with PolRound=-1.
	ids := []*identity.Identity{mustIdentity(t), mustIdentity(t), mustIdentity(t), mustIdentity(t)}
	thread := makeThread(ids, 1)
	store := newTendermintTestStore(t)
	store.SaveThread(thread) //nolint:errcheck

	proposer := ids[1] // (1+0)%4 = 1

	e := &TendermintBackend{
		thread:      thread,
		id:          ids[0],
		store:       store,
		futureVotes: make(map[int32][]*pb.Vote),
		cs: ConsensusState{
			Height:      1,
			Round:       0,
			Step:        stepPropose,
			LockedRound: 0,
			LockedHash:  "locked-hash",
			ValidRound:  -1,
		},
		log: zapDev(),
	}

	// PolRound=-1 ≤ LockedRound=0 → no unlock → prevote nil
	prop := signedProposal(proposer, thread, 1, 0, -1, nil)

	prevoted := "sentinel"
	var mu sync.Mutex
	broadcast := func(msg *pb.ConsensusMsg) {
		if v, ok := msg.Payload.(*pb.ConsensusMsg_Vote); ok {
			mu.Lock()
			prevoted = v.Vote.BlockHash
			mu.Unlock()
		}
	}

	timer := time.NewTimer(0)
	timer.Stop()
	e.handleProposal(context.Background(), prop, broadcast, timer)

	mu.Lock()
	defer mu.Unlock()

	if prevoted != "" {
		t.Errorf("expected prevote nil when locked without PoLC, got %q", prevoted)
	}
	if e.cs.LockedRound != 0 || e.cs.LockedHash != "locked-hash" {
		t.Errorf("lock modified unexpectedly: LockedRound=%d LockedHash=%q",
			e.cs.LockedRound, e.cs.LockedHash)
	}
}

// ─── handleProposal: wrong height/round ignored ────────────────────────────────

func TestHandleProposal_WrongHeight_Ignored(t *testing.T) {
	ids := []*identity.Identity{mustIdentity(t), mustIdentity(t), mustIdentity(t), mustIdentity(t)}
	thread := makeThread(ids, 1)
	store := newTendermintTestStore(t)
	store.SaveThread(thread) //nolint:errcheck

	proposer := ids[2] // (2+0)%4 = 2

	e := &TendermintBackend{
		thread: thread, id: ids[0], store: store,
		futureVotes: make(map[int32][]*pb.Vote),
		cs:          ConsensusState{Height: 2, Round: 0, Step: stepPropose, LockedRound: -1, ValidRound: -1},
		log:         zapDev(),
	}

	prop := signedProposal(proposer, thread, 1, 0, -1, nil) // height=1 ≠ cs.Height=2

	var voted bool
	broadcast := func(msg *pb.ConsensusMsg) {
		if _, ok := msg.Payload.(*pb.ConsensusMsg_Vote); ok {
			voted = true
		}
	}

	timer := time.NewTimer(0)
	timer.Stop()
	e.handleProposal(context.Background(), prop, broadcast, timer)

	if voted {
		t.Error("should not vote on proposal for wrong height")
	}
}

// ─── single-node commit ───────────────────────────────────────────────────────

func TestTendermintBackend_SingleNode_Commit(t *testing.T) {
	// f=0, N=1 → quorum=1, single node is always proposer and sole validator.
	id := mustIdentity(t)
	thread := &pb.Thread{
		Id:          "singlenode",
		CreatorDid:  id.DID,
		ReplicaDids: []string{id.DID},
		N:           1,
		F:           0,
		EpochMs:     50,
	}
	store := newTendermintTestStore(t)
	store.SaveThread(thread) //nolint:errcheck

	var committed []*pb.ThreadBlock
	var mu sync.Mutex
	onCommit := func(b *pb.ThreadBlock) {
		mu.Lock()
		committed = append(committed, b)
		mu.Unlock()
	}

	entry := &pb.ThreadEntry{Kind: "message", Payload: []byte("hello")}
	store.EnqueueEntry(thread.Id, entry) //nolint:errcheck

	backend, err := newTendermintBackend(thread, id, store, zapDev(), onCommit)
	if err != nil {
		t.Fatalf("newTendermintBackend: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	go backend.Run(ctx, func(msg *pb.ConsensusMsg) {
		backend.Deliver(msg) // loop back to self
	})

	deadline := time.Now().Add(4 * time.Second)
	for time.Now().Before(deadline) {
		mu.Lock()
		n := len(committed)
		mu.Unlock()
		if n >= 1 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}

	mu.Lock()
	n := len(committed)
	var block *pb.ThreadBlock
	if n > 0 {
		block = committed[0]
	}
	mu.Unlock()

	if n < 1 {
		t.Fatal("timed out: expected at least 1 committed block")
	}
	if block.Height != 1 {
		t.Errorf("expected height=1, got %d", block.Height)
	}
	if len(block.Entries) != 1 {
		t.Errorf("expected 1 entry, got %d", len(block.Entries))
	}
	if string(block.Entries[0].Payload) != "hello" {
		t.Errorf("entry payload: %q", block.Entries[0].Payload)
	}
}

// ─── consecutive commits ──────────────────────────────────────────────────────

func TestTendermintBackend_SingleNode_MultipleBlocks(t *testing.T) {
	id := mustIdentity(t)
	thread := &pb.Thread{
		Id:          "multiblock",
		CreatorDid:  id.DID,
		ReplicaDids: []string{id.DID},
		N:           1,
		F:           0,
		EpochMs:     30,
	}
	store := newTendermintTestStore(t)
	store.SaveThread(thread) //nolint:errcheck

	// Enqueue 3 entries — each should land in its own block (maxPendingPerBlock=64).
	for i := 0; i < 3; i++ {
		store.EnqueueEntry(thread.Id, &pb.ThreadEntry{Kind: "msg", Payload: []byte{byte(i)}}) //nolint:errcheck
	}

	var committed []*pb.ThreadBlock
	var mu sync.Mutex
	onCommit := func(b *pb.ThreadBlock) {
		mu.Lock()
		committed = append(committed, b)
		mu.Unlock()
	}

	backend, _ := newTendermintBackend(thread, id, store, zapDev(), onCommit)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	go backend.Run(ctx, func(msg *pb.ConsensusMsg) {
		backend.Deliver(msg)
	})

	deadline := time.Now().Add(4 * time.Second)
	for time.Now().Before(deadline) {
		mu.Lock()
		n := len(committed)
		mu.Unlock()
		if n >= 1 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}

	mu.Lock()
	n := len(committed)
	mu.Unlock()

	if n < 1 {
		t.Fatal("no blocks committed")
	}
	// Heights must be sequential starting at 1.
	mu.Lock()
	for i, b := range committed {
		if b.Height != int64(i+1) {
			t.Errorf("block[%d].Height=%d, want %d", i, b.Height, i+1)
		}
	}
	mu.Unlock()
}
