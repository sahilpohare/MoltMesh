package thread_test

import (
	"context"
	"testing"
	"time"

	pubsub "github.com/libp2p/go-libp2p-pubsub"
	"github.com/libp2p/go-libp2p"
	"go.uber.org/zap"

	pb "github.com/sahilpohare/p2p-a2a/gen/a2a/v1"
	"github.com/sahilpohare/p2p-a2a/daemon/identity"
	"github.com/sahilpohare/p2p-a2a/daemon/thread"
)

// ─── helpers ──────────────────────────────────────────────────────────────────

func newStore(t *testing.T) *thread.Store {
	t.Helper()
	s, err := thread.NewStore(":memory:")
	if err != nil {
		t.Fatalf("NewStore: %v", err)
	}
	t.Cleanup(func() { s.Close() })
	return s
}

func newID(t *testing.T) *identity.Identity {
	t.Helper()
	id, err := identity.Generate()
	if err != nil {
		t.Fatalf("identity.Generate: %v", err)
	}
	return id
}

// ─── Store tests ──────────────────────────────────────────────────────────────

func TestStore_SaveAndGetThread(t *testing.T) {
	s := newStore(t)
	th := &pb.Thread{
		Id:          "thread-1",
		CreatorDid:  "did:key:zA",
		ReplicaDids: []string{"did:key:zA", "did:key:zB", "did:key:zC", "did:key:zD"},
		N:           4,
		F:           1,
		EpochMs:     1000,
		CreatedAt:   time.Now().UnixMilli(),
		Metadata:    map[string]string{"env": "test"},
	}
	if err := s.SaveThread(th); err != nil {
		t.Fatalf("SaveThread: %v", err)
	}
	got, err := s.GetThread("thread-1")
	if err != nil {
		t.Fatalf("GetThread: %v", err)
	}
	if got.Id != "thread-1" {
		t.Errorf("ID mismatch: %q", got.Id)
	}
	if got.F != 1 {
		t.Errorf("F mismatch: %d", got.F)
	}
	if got.Metadata["env"] != "test" {
		t.Errorf("Metadata mismatch: %v", got.Metadata)
	}
}

func TestStore_GetThread_NotFound(t *testing.T) {
	s := newStore(t)
	_, err := s.GetThread("nonexistent")
	if err == nil {
		t.Fatal("expected error for nonexistent thread")
	}
}

func TestStore_SaveAndGetBlock(t *testing.T) {
	s := newStore(t)
	block := &pb.ThreadBlock{
		ThreadId:    "t1",
		Height:      1,
		Round:       0,
		ParentHash:  "",
		ProposerDid: "did:key:zA",
		ProposerSig: "sig",
		BlockHash:   "abc123",
		CommittedAt: time.Now().UnixMilli(),
		Entries: []*pb.ThreadEntry{
			{AuthorDid: "did:key:zA", Payload: []byte("hello"), Kind: "message"},
		},
	}
	if err := s.SaveBlock(block); err != nil {
		t.Fatalf("SaveBlock: %v", err)
	}
	got, err := s.GetBlock("t1", 1)
	if err != nil {
		t.Fatalf("GetBlock: %v", err)
	}
	if got.BlockHash != "abc123" {
		t.Errorf("BlockHash mismatch: %q", got.BlockHash)
	}
	if len(got.Entries) != 1 {
		t.Errorf("entries: expected 1, got %d", len(got.Entries))
	}
	if got.Entries[0].Kind != "message" {
		t.Errorf("entry kind: %q", got.Entries[0].Kind)
	}
}

func TestStore_GetCommittedHeight(t *testing.T) {
	s := newStore(t)

	h, err := s.GetCommittedHeight("t1")
	if err != nil {
		t.Fatalf("GetCommittedHeight: %v", err)
	}
	if h != 0 {
		t.Errorf("expected 0, got %d", h)
	}

	s.SaveBlock(&pb.ThreadBlock{
		ThreadId: "t1", Height: 1, BlockHash: "h1",
		ProposerDid: "did:key:zA", ProposerSig: "sig",
		CommittedAt: time.Now().UnixMilli(),
	})
	s.SaveBlock(&pb.ThreadBlock{
		ThreadId: "t1", Height: 2, BlockHash: "h2",
		ProposerDid: "did:key:zA", ProposerSig: "sig",
		CommittedAt: time.Now().UnixMilli(),
	})

	h, err = s.GetCommittedHeight("t1")
	if err != nil {
		t.Fatalf("GetCommittedHeight: %v", err)
	}
	if h != 2 {
		t.Errorf("expected 2, got %d", h)
	}
}

func TestStore_PendingEntries(t *testing.T) {
	s := newStore(t)
	entry := &pb.ThreadEntry{AuthorDid: "did:key:zA", Payload: []byte("data"), Kind: "message"}
	if err := s.EnqueueEntry("t1", entry); err != nil {
		t.Fatalf("EnqueueEntry: %v", err)
	}

	entries, err := s.DequeuePendingEntries("t1", 10)
	if err != nil {
		t.Fatalf("DequeuePendingEntries: %v", err)
	}
	if len(entries) != 1 {
		t.Fatalf("expected 1 entry, got %d", len(entries))
	}
	if entries[0].Kind != "message" {
		t.Errorf("kind mismatch: %q", entries[0].Kind)
	}

	// After dequeue, queue should be empty.
	entries2, _ := s.DequeuePendingEntries("t1", 10)
	if len(entries2) != 0 {
		t.Errorf("expected empty after dequeue, got %d", len(entries2))
	}
}

func TestStore_Votes(t *testing.T) {
	s := newStore(t)
	v := &pb.Vote{
		ThreadId:  "t1",
		Height:    1,
		Round:     0,
		Type:      pb.VoteType_VOTE_TYPE_PREVOTE,
		BlockHash: "abc",
		VoterDid:  "did:key:zA",
		Signature: "sig",
	}
	if err := s.SaveVote(v); err != nil {
		t.Fatalf("SaveVote: %v", err)
	}

	votes, err := s.GetVotes("t1", 1, 0, pb.VoteType_VOTE_TYPE_PREVOTE)
	if err != nil {
		t.Fatalf("GetVotes: %v", err)
	}
	if len(votes) != 1 {
		t.Fatalf("expected 1 vote, got %d", len(votes))
	}
	if votes[0].VoterDid != "did:key:zA" {
		t.Errorf("voter mismatch: %q", votes[0].VoterDid)
	}
}

func TestStore_ConsensusState(t *testing.T) {
	s := newStore(t)

	// Default (not stored) should return initial state.
	cs, err := s.LoadConsensusState("t1")
	if err != nil {
		t.Fatalf("LoadConsensusState: %v", err)
	}
	if cs.Height != 1 {
		t.Errorf("default height: %d", cs.Height)
	}

	cs.Height = 5
	cs.Round = 2
	if err := s.SaveConsensusState("t1", cs); err != nil {
		t.Fatalf("SaveConsensusState: %v", err)
	}

	got, err := s.LoadConsensusState("t1")
	if err != nil {
		t.Fatalf("LoadConsensusState after save: %v", err)
	}
	if got.Height != 5 || got.Round != 2 {
		t.Errorf("state mismatch: height=%d round=%d", got.Height, got.Round)
	}
}

// ─── Manager tests ────────────────────────────────────────────────────────────

func newManager(t *testing.T) (*thread.Manager, *identity.Identity) {
	t.Helper()
	log, _ := zap.NewDevelopment()
	s := newStore(t)
	id := newID(t)

	h, err := libp2p.New(libp2p.ListenAddrStrings("/ip4/127.0.0.1/tcp/0"))
	if err != nil {
		t.Fatalf("libp2p.New: %v", err)
	}
	t.Cleanup(func() { h.Close() })

	ps, err := pubsub.NewGossipSub(context.Background(), h,
		pubsub.WithMessageSignaturePolicy(pubsub.StrictSign),
	)
	if err != nil {
		t.Fatalf("pubsub.NewGossipSub: %v", err)
	}

	tm := thread.NewManager(context.Background(), s, id, ps, log)
	return tm, id
}

func TestManager_CreateThread(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	tm, id := newManager(t)

	// Need 3f+1 = 4 replicas for f=1
	replicas := []string{
		id.DID,
		"did:key:zReplica1Aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		"did:key:zReplica2Aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		"did:key:zReplica3Aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
	}

	th, err := tm.CreateThread(ctx, &pb.CreateThreadRequest{
		ReplicaDids: replicas,
		F:           1,
		EpochMs:     200,
	})
	if err != nil {
		t.Fatalf("CreateThread: %v", err)
	}
	if th.Id == "" {
		t.Error("thread ID is empty")
	}
	if th.CreatorDid != id.DID {
		t.Errorf("creator DID mismatch: %q", th.CreatorDid)
	}
	if th.N != 4 {
		t.Errorf("N should be 4, got %d", th.N)
	}

	// Engine should be running.
	eng := tm.Engine(th.Id)
	if eng == nil {
		t.Error("engine should be running after CreateThread")
	}
}

func TestManager_CreateThread_NotEnoughReplicas(t *testing.T) {
	ctx := context.Background()
	tm, _ := newManager(t)

	_, err := tm.CreateThread(ctx, &pb.CreateThreadRequest{
		ReplicaDids: []string{"did:key:zOnly1"},
		F:           1,
	})
	if err == nil {
		t.Fatal("expected error with insufficient replicas")
	}
}

func TestManager_AppendEntry(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	tm, id := newManager(t)

	replicas := []string{
		id.DID,
		"did:key:zReplica1Aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		"did:key:zReplica2Aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		"did:key:zReplica3Aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
	}
	th, err := tm.CreateThread(ctx, &pb.CreateThreadRequest{
		ReplicaDids: replicas, F: 1, EpochMs: 200,
	})
	if err != nil {
		t.Fatalf("CreateThread: %v", err)
	}

	entry := &pb.ThreadEntry{
		AuthorDid: id.DID,
		Payload:   []byte("hello world"),
		Kind:      "message",
	}
	if err := tm.AppendEntry(th.Id, entry); err != nil {
		t.Fatalf("AppendEntry: %v", err)
	}
}

func TestManager_GetThread(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	tm, id := newManager(t)
	replicas := []string{
		id.DID,
		"did:key:zReplica1Aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		"did:key:zReplica2Aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		"did:key:zReplica3Aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
	}
	th, _ := tm.CreateThread(ctx, &pb.CreateThreadRequest{
		ReplicaDids: replicas, F: 1,
	})

	got, err := tm.GetThread(th.Id)
	if err != nil {
		t.Fatalf("GetThread: %v", err)
	}
	if got.Id != th.Id {
		t.Errorf("ID mismatch: %q vs %q", got.Id, th.Id)
	}
}

// TestEngine_SingleNode verifies that a 1-validator thread (f=0, N=1)
// commits a block when an entry is appended and the epoch fires.
func TestEngine_SingleNode(t *testing.T) {
	log, _ := zap.NewDevelopment()
	s := newStore(t)
	id := newID(t)

	th := &pb.Thread{
		Id:          "single-node",
		CreatorDid:  id.DID,
		ReplicaDids: []string{id.DID},
		N:           1,
		F:           0,
		EpochMs:     100,
		CreatedAt:   time.Now().UnixMilli(),
	}
	if err := s.SaveThread(th); err != nil {
		t.Fatalf("SaveThread: %v", err)
	}

	committed := make(chan *pb.ThreadBlock, 1)
	eng, err := thread.NewEngine(th, id, s, log, thread.BackendRaft, func(b *pb.ThreadBlock) {
		committed <- b
	})
	if err != nil {
		t.Fatalf("NewEngine: %v", err)
	}

	// Enqueue an entry before starting.
	entry := &pb.ThreadEntry{AuthorDid: id.DID, Payload: []byte("test"), Kind: "message"}
	if err := s.EnqueueEntry(th.Id, entry); err != nil {
		t.Fatalf("EnqueueEntry: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Run with a no-op broadcast (single node, no gossip needed).
	go eng.Run(ctx, func(msg *pb.ConsensusMsg) {
		// Feed back to self so the engine receives its own proposals/votes.
		eng.Deliver(msg)
	})

	select {
	case block := <-committed:
		if block.Height != 1 {
			t.Errorf("expected height 1, got %d", block.Height)
		}
		if len(block.Entries) != 1 {
			t.Errorf("expected 1 entry, got %d", len(block.Entries))
		}
		if string(block.Entries[0].Payload) != "test" {
			t.Errorf("entry payload mismatch: %q", block.Entries[0].Payload)
		}
	case <-ctx.Done():
		t.Fatal("timeout waiting for block commit")
	}
}
