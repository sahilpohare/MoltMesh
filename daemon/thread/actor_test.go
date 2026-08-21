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

func newTestSupervisor(t *testing.T, ctx context.Context) (*ThreadSupervisor, *Store, *identity.Identity) {
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

	return NewThreadSupervisor(ctx, sys, store, id, nil, nil, log), store, id
}

// singleNodeThread returns an f=0, N=1 thread — a single-validator raft
// cluster with no network round-trips, matching thread_test.go's
// TestEngine_SingleNode pattern.
func singleNodeThread(id *identity.Identity, threadID string) *pb.Thread {
	return &pb.Thread{
		Id:          threadID,
		CreatorDid:  id.DID,
		ReplicaDids: []string{id.DID},
		N:           1,
		F:           0,
		EpochMs:     100,
		CreatedAt:   time.Now().UnixMilli(),
	}
}

// {P}: store empty, thread not yet persisted, no actor spawned.
// {C}: save thread, enqueue one entry, spawn ThreadActor, poll via Ask until
//
//	a tick has driven raft to commit it.
//
// {Q}: QueryThreadReply.CommittedHeight == 1 and the actor is running.
func TestThreadActor_SpawnAskTick(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	sup, store, id := newTestSupervisor(t, ctx)

	th := singleNodeThread(id, "thread-spawn-ask-tick")
	if err := store.SaveThread(th); err != nil {
		t.Fatalf("SaveThread: %v", err)
	}
	entry := &pb.ThreadEntry{AuthorDid: id.DID, Payload: []byte("hello"), Kind: "message"}
	if err := store.EnqueueEntry(th.Id, entry); err != nil {
		t.Fatalf("EnqueueEntry: %v", err)
	}

	pid, err := sup.Spawn(ctx, th)
	if err != nil {
		t.Fatalf("Spawn: %v", err)
	}

	// Single-node raft can need a couple of election retries before it's
	// allowed to campaign (etcd/raft holds off while the initial peer-conf
	// entry is still pending apply) — give it room, same as CrashRestart.
	deadline := time.Now().Add(10 * time.Second)
	var reply *QueryThreadReply
	for time.Now().Before(deadline) {
		resp, err := pid.Ask(ctx, pid, QueryThreadMsg{}, time.Second)
		if err == nil {
			if r, ok := resp.(*QueryThreadReply); ok && r.CommittedHeight >= 1 {
				reply = r
				break
			}
		}
		time.Sleep(20 * time.Millisecond)
	}

	if reply == nil {
		t.Fatal("timeout waiting for committed height >= 1")
	}
	if reply.CommittedHeight != 1 {
		t.Errorf("expected committed height 1, got %d", reply.CommittedHeight)
	}
	if !pid.IsRunning() {
		t.Error("expected actor to be running")
	}
}

// {P}: ThreadActor spawned, running, thread committed to height 1 (state
//
//	that must survive the crash).
//
// {C}: Tell the actor an injectFaultMsg, forcing a panic inside Receive.
// {Q}: the supervisor restarts the actor (RestartDirective, not GoAkt's
//
//	default StopDirective for PanicError — see ThreadSupervisor's
//	WithDirective override) — pid.IsRunning() becomes true again, and
//	thread state (committed height) in the store is unchanged.
func TestThreadActor_CrashRestart(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	sup, store, id := newTestSupervisor(t, ctx)

	th := singleNodeThread(id, "thread-crash-restart")
	if err := store.SaveThread(th); err != nil {
		t.Fatalf("SaveThread: %v", err)
	}
	entry := &pb.ThreadEntry{AuthorDid: id.DID, Payload: []byte("pre-crash"), Kind: "message"}
	if err := store.EnqueueEntry(th.Id, entry); err != nil {
		t.Fatalf("EnqueueEntry: %v", err)
	}

	pid, err := sup.Spawn(ctx, th)
	if err != nil {
		t.Fatalf("Spawn: %v", err)
	}

	// Wait for the pre-crash entry to commit so we have state to check
	// survives the crash. Single-node raft can need a couple of election
	// retries before it's allowed to campaign (etcd/raft holds off while
	// the initial peer-conf entry is still pending apply) — give it room.
	deadline := time.Now().Add(10 * time.Second)
	var heightBefore int64
	for time.Now().Before(deadline) {
		h, err := store.GetCommittedHeight(th.Id)
		if err == nil && h >= 1 {
			heightBefore = h
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if heightBefore == 0 {
		t.Fatal("timeout waiting for pre-crash commit")
	}

	// Force the panic. Tell is fire-and-forget so this must not block/error
	// even though the actor dies processing it.
	if err := pid.Tell(ctx, pid, injectFaultMsg{}); err != nil {
		t.Fatalf("Tell(injectFaultMsg): %v", err)
	}

	// Give the supervisor time to observe the panic and restart the actor.
	restartDeadline := time.Now().Add(4 * time.Second)
	restarted := false
	for time.Now().Before(restartDeadline) {
		if pid.IsRunning() && !pid.IsSuspended() {
			restarted = true
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if !restarted {
		t.Fatalf("actor did not restart after injected fault (IsRunning=%v IsSuspended=%v)",
			pid.IsRunning(), pid.IsSuspended())
	}

	heightAfter, err := store.GetCommittedHeight(th.Id)
	if err != nil {
		t.Fatalf("GetCommittedHeight after restart: %v", err)
	}
	if heightAfter != heightBefore {
		t.Errorf("thread state not intact across restart: before=%d after=%d", heightBefore, heightAfter)
	}
}

func TestThreadActor_PassivatesCheckpointsAndResumes(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	sup, store, id := newTestSupervisor(t, ctx)
	sup.passivateAfter = 250 * time.Millisecond
	th := singleNodeThread(id, "thread-passivate-resume")
	if err := store.SaveThread(th); err != nil {
		t.Fatalf("SaveThread: %v", err)
	}

	pid, err := sup.Spawn(ctx, th)
	if err != nil {
		t.Fatalf("Spawn: %v", err)
	}
	deadline := time.Now().Add(3 * time.Second)
	for pid.IsRunning() && time.Now().Before(deadline) {
		time.Sleep(20 * time.Millisecond)
	}
	if pid.IsRunning() {
		t.Fatal("idle thread did not passivate")
	}
	checkpoint, err := store.LoadCheckpoint(th.Id)
	if err != nil {
		t.Fatalf("LoadCheckpoint: %v", err)
	}
	if checkpoint.PassivatedAt == 0 {
		t.Fatal("passivation did not write a durable checkpoint")
	}
	snapshot, _, _, err := store.LoadRaftState(th.Id)
	if err != nil {
		t.Fatalf("LoadRaftState: %v", err)
	}
	if snapshot.Metadata.Index == 0 {
		t.Fatal("passivation did not write an etcd/raft snapshot")
	}

	sup.passivateAfter = 3 * time.Second
	resumed, err := sup.Spawn(ctx, th)
	if err != nil {
		t.Fatalf("resume Spawn: %v", err)
	}
	if !resumed.IsRunning() || resumed == pid {
		t.Fatal("expected a fresh running actor after passivation")
	}
	if err := store.EnqueueEntry(th.Id, &pb.ThreadEntry{AuthorDid: id.DID, Kind: "message", Payload: []byte("after-resume")}); err != nil {
		t.Fatalf("EnqueueEntry after resume: %v", err)
	}
	if err := resumed.Tell(ctx, resumed, ActivateMsg{}); err != nil {
		t.Fatalf("ActivateMsg after resume: %v", err)
	}
	deadline = time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if height, err := store.GetCommittedHeight(th.Id); err == nil && height >= 1 {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("resumed snapshot did not commit new work")
}
