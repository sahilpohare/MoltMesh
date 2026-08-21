package thread

import (
	"context"
	"testing"
	"time"

	"go.uber.org/zap"

	"github.com/sahilpohare/p2p-a2a/daemon/actors"
	"github.com/sahilpohare/p2p-a2a/daemon/identity"
	pb "github.com/sahilpohare/p2p-a2a/gen/a2a/v1"
)

func newTestActorManager(t *testing.T, ctx context.Context) (*ActorManager, *identity.Identity) {
	t.Helper()

	log, _ := zap.NewDevelopment()

	store, err := NewStore(":memory:")
	if err != nil {
		t.Fatalf("NewStore: %v", err)
	}
	t.Cleanup(func() { store.Close() })

	id, err := identity.Generate()
	if err != nil {
		t.Fatalf("identity.Generate: %v", err)
	}

	sys, err := actors.NewSystem(ctx, log)
	if err != nil {
		t.Fatalf("NewSystem: %v", err)
	}
	t.Cleanup(func() { sys.Stop(context.Background()) })

	// ps=nil: no GossipSub fan-out needed for a single-node (f=0) thread.
	return NewActorManager(ctx, sys, store, id, nil, nil, log), id
}

// {P}: fresh ActorManager, no threads.
// {C}: CreateThread with f=0 (single-validator); AppendEntry one entry.
// {Q}: GetEntries eventually returns it, and Engine(threadID)'s Subscribe
//
//	channel also observes the live commit — proving both the
//	rpc.ThreadManager surface (GetEntries) and the SubscribeThread path
//	(Engine/Subscribe) work through the GoAkt actor, not just the
//	lower-level ThreadActor tested directly in thread_actor_test.go.
func TestActorManager_CreateAppendGetEntries(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	mgr, id := newTestActorManager(t, ctx)

	th, err := mgr.CreateThread(ctx, &pb.CreateThreadRequest{
		ReplicaDids: []string{id.DID},
		F:           0,
		EpochMs:     100,
	})
	if err != nil {
		t.Fatalf("CreateThread: %v", err)
	}
	if th.N != 1 {
		t.Fatalf("expected single-validator N=1, got %d", th.N)
	}

	eng := mgr.Engine(th.Id)
	if eng == nil {
		t.Fatal("expected a running Engine/ThreadActor for this validator")
	}
	sub := eng.Subscribe()
	defer eng.Unsubscribe(sub)

	if err := mgr.AppendEntry(th.Id, &pb.ThreadEntry{
		AuthorDid: id.DID,
		Payload:   []byte("hello"),
		Kind:      "message",
	}); err != nil {
		t.Fatalf("AppendEntry: %v", err)
	}

	select {
	case ep := <-sub:
		if string(ep.Entry.Payload) != "hello" {
			t.Errorf("unexpected payload: %q", ep.Entry.Payload)
		}
	case <-ctx.Done():
		t.Fatal("timeout waiting for live commit on subscribe channel")
	}

	deadline := time.Now().Add(10 * time.Second)
	var entries []*pb.ThreadEntryWithPos
	for time.Now().Before(deadline) {
		entries, err = mgr.GetEntries(th.Id, 0, 0)
		if err != nil {
			t.Fatalf("GetEntries: %v", err)
		}
		if len(entries) > 0 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if len(entries) != 1 {
		t.Fatalf("expected 1 entry, got %d", len(entries))
	}
	if string(entries[0].Entry.Payload) != "hello" {
		t.Errorf("unexpected payload: %q", entries[0].Entry.Payload)
	}
}

// A voter role is only returned after the actor has observed its matching
// ConfChangeV2 commit; merely submitting a raft proposal is insufficient.
func TestActorManager_ProposeVoterChangeWaitsForCommit(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	mgr, id := newTestActorManager(t, ctx)
	observer := "did:key:zObserverCatchup"
	th, err := mgr.CreateThread(ctx, &pb.CreateThreadRequest{
		ReplicaDids: []string{id.DID, observer}, F: 0, EpochMs: 100,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := mgr.store.SaveMember(th.Id, &pb.ThreadMember{Did: observer, Role: pb.ThreadMemberRole_THREAD_MEMBER_ROLE_OBSERVER, JoinedEpoch: 1}); err != nil {
		t.Fatal(err)
	}
	var proposalErr error
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		proposalErr = mgr.ProposeVoterChange(ctx, th.Id, observer, true)
		if proposalErr == nil {
			break
		}
		if ctx.Err() != nil {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if proposalErr != nil {
		t.Fatalf("voter change did not commit: %v", proposalErr)
	}
	member, epoch, err := mgr.PromoteMemberWithEpoch(th.Id, observer)
	if err != nil || member.Role != pb.ThreadMemberRole_THREAD_MEMBER_ROLE_VOTER || epoch != 2 {
		t.Fatalf("promote after committed config = %#v, %d, %v", member, epoch, err)
	}
}

// {P}: fresh ActorManager, no threads.
// {C}: InviteReceived with a pre-built thread (simulating a THREAD_INVITE
//
//	arriving from a peer, as deliver.go routes it).
//
// {Q}: the thread is persisted and its ThreadActor is running (Engine
//
//	returns non-nil) — proving ActorManager satisfies
//	deliver.ThreadInviter's contract.
func TestActorManager_InviteReceived(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	mgr, id := newTestActorManager(t, ctx)

	th := singleNodeThread(id, "thread-invited")
	if err := mgr.InviteReceived(th); err != nil {
		t.Fatalf("InviteReceived: %v", err)
	}

	got, err := mgr.GetThread(th.Id)
	if err != nil {
		t.Fatalf("GetThread: %v", err)
	}
	if got.Id != th.Id {
		t.Errorf("expected thread %q, got %q", th.Id, got.Id)
	}

	if mgr.Engine(th.Id) == nil {
		t.Error("expected a running Engine/ThreadActor after InviteReceived")
	}
}

func TestActorManager_StartAllIsLazy(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	mgr, id := newTestActorManager(t, ctx)
	th := singleNodeThread(id, "thread-lazy-start")
	if err := mgr.store.SaveThread(th); err != nil {
		t.Fatalf("SaveThread: %v", err)
	}
	if err := mgr.StartAll(ctx); err != nil {
		t.Fatalf("StartAll: %v", err)
	}
	if pid := mgr.sup.PID(th.Id); pid != nil {
		t.Fatal("StartAll materialized a dormant thread")
	}
	if eng := mgr.Engine(th.Id); eng == nil {
		t.Fatal("Engine did not activate the durable thread on demand")
	}
}
