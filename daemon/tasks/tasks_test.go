package tasks

import (
	"context"
	"errors"
	appactors "github.com/sahilpohare/p2p-a2a/daemon/actors"
	"go.uber.org/zap"
	"testing"
	"time"

	pb "github.com/sahilpohare/p2p-a2a/gen/a2a/v1"
)

func TestTerminalTaskActorPassivates(t *testing.T) {
	ctx := context.Background()
	s := newTestStore(t)
	system, err := appactors.NewSystem(ctx, zap.NewNop())
	if err != nil {
		t.Fatal(err)
	}
	defer system.Stop(ctx)
	hierarchy, err := system.NewHierarchy(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.EnableActor(ctx, hierarchy); err != nil {
		t.Fatal(err)
	}
	task, err := s.Create(initiator, assignee, "", skill, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.StartWork(task.Id); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Complete(task.Id, nil); err != nil {
		t.Fatal(err)
	}
	if got := s.ActiveTaskActors(); got != 0 {
		t.Fatalf("terminal task retained %d actors", got)
	}
}

const (
	initiator = "did:key:zInitiator"
	assignee  = "did:key:zAssignee"
	skill     = "a2a:v1:cap:text-generation"
)

func newTestStore(t *testing.T) *Store {
	t.Helper()
	s, err := New(":memory:")
	if err != nil {
		t.Fatalf("New task store: %v", err)
	}
	t.Cleanup(func() { s.Close() })
	return s
}

func TestCreate(t *testing.T) {
	s := newTestStore(t)
	task, err := s.Create(initiator, assignee, "thread-1", skill, nil, nil)
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	if task.Id == "" {
		t.Error("task ID is empty")
	}
	if task.Status != pb.TaskStatus_TASK_STATUS_SUBMITTED {
		t.Errorf("expected SUBMITTED, got %v", task.Status)
	}
	if task.Initiator != initiator {
		t.Errorf("initiator mismatch: %q", task.Initiator)
	}
	if task.Assignee != assignee {
		t.Errorf("assignee mismatch: %q", task.Assignee)
	}
	if task.Skill != skill {
		t.Errorf("skill mismatch: %q", task.Skill)
	}
	if task.CreatedAt == 0 {
		t.Error("CreatedAt is zero")
	}
}

func TestDeliveriesHaveDurableCursorsAndRequeueExpiredLeases(t *testing.T) {
	s := newTestStore(t)
	first, err := s.Create(initiator, assignee, "", skill, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	deliveries, err := s.ListDeliveries(assignee, []string{skill}, 0, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(deliveries) != 1 || deliveries[0].Task.Id != first.Id || deliveries[0].Sequence == 0 {
		t.Fatalf("initial deliveries = %#v", deliveries)
	}
	firstCursor := deliveries[0].Sequence
	if got, err := s.ListDeliveries(assignee, nil, firstCursor, 10); err != nil || len(got) != 0 {
		t.Fatalf("cursor replay = %#v, %v; want none", got, err)
	}

	if _, err := s.Claim(first.Id, assignee, time.Millisecond); err != nil {
		t.Fatal(err)
	}
	time.Sleep(5 * time.Millisecond)
	deliveries, err = s.ListDeliveries(assignee, nil, firstCursor, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(deliveries) != 1 || deliveries[0].Task.Id != first.Id || deliveries[0].Sequence <= firstCursor {
		t.Fatalf("requeued deliveries = %#v", deliveries)
	}
	if deliveries[0].Task.Status != pb.TaskStatus_TASK_STATUS_SUBMITTED {
		t.Fatalf("requeued task status = %v, want submitted", deliveries[0].Task.Status)
	}
}

func TestGet(t *testing.T) {
	s := newTestStore(t)
	created, _ := s.Create(initiator, assignee, "", skill, nil, nil)

	got, err := s.Get(created.Id)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got.Id != created.Id {
		t.Errorf("ID mismatch: %q vs %q", got.Id, created.Id)
	}
}

func TestGet_NotFound(t *testing.T) {
	s := newTestStore(t)
	_, err := s.Get("nonexistent-id")
	if err == nil {
		t.Fatal("expected error for missing task")
	}
}

func TestUpdateStatus_Lifecycle(t *testing.T) {
	s := newTestStore(t)
	task, _ := s.Create(initiator, assignee, "", skill, nil, nil)

	// SUBMITTED → WORKING
	updated, err := s.UpdateStatus(task.Id, pb.TaskStatus_TASK_STATUS_WORKING, "", nil)
	if err != nil {
		t.Fatalf("UpdateStatus WORKING: %v", err)
	}
	if updated.Status != pb.TaskStatus_TASK_STATUS_WORKING {
		t.Errorf("expected WORKING, got %v", updated.Status)
	}

	// WORKING → COMPLETED with output artifact
	artifacts := []*pb.Artifact{{Cid: "sha256:abc", MimeType: "text/plain", Size: 42}}
	completed, err := s.UpdateStatus(task.Id, pb.TaskStatus_TASK_STATUS_COMPLETED, "", artifacts)
	if err != nil {
		t.Fatalf("UpdateStatus COMPLETED: %v", err)
	}
	if completed.Status != pb.TaskStatus_TASK_STATUS_COMPLETED {
		t.Errorf("expected COMPLETED, got %v", completed.Status)
	}
	if len(completed.OutputArtifacts) != 1 {
		t.Errorf("expected 1 output artifact, got %d", len(completed.OutputArtifacts))
	}
	if completed.OutputArtifacts[0].Cid != "sha256:abc" {
		t.Errorf("artifact CID mismatch: %q", completed.OutputArtifacts[0].Cid)
	}
}

func TestUpdateStatus_Failed(t *testing.T) {
	s := newTestStore(t)
	task, _ := s.Create(initiator, assignee, "", skill, nil, nil)

	failed, err := s.UpdateStatus(task.Id, pb.TaskStatus_TASK_STATUS_FAILED, "something went wrong", nil)
	if err != nil {
		t.Fatal(err)
	}
	if failed.Status != pb.TaskStatus_TASK_STATUS_FAILED {
		t.Errorf("expected FAILED, got %v", failed.Status)
	}
	if failed.Error != "something went wrong" {
		t.Errorf("error message mismatch: %q", failed.Error)
	}
}

func TestUpdateStatus_Cancelled(t *testing.T) {
	s := newTestStore(t)
	task, _ := s.Create(initiator, assignee, "", skill, nil, nil)

	cancelled, err := s.UpdateStatus(task.Id, pb.TaskStatus_TASK_STATUS_CANCELLED, "", nil)
	if err != nil {
		t.Fatal(err)
	}
	if cancelled.Status != pb.TaskStatus_TASK_STATUS_CANCELLED {
		t.Errorf("expected CANCELLED, got %v", cancelled.Status)
	}
}

func TestCreate_WithInputArtifacts(t *testing.T) {
	s := newTestStore(t)
	artifacts := []*pb.Artifact{
		{Cid: "sha256:001", MimeType: "application/pdf", Size: 1024},
		{Cid: "sha256:002", MimeType: "text/plain", Size: 256},
	}
	task, err := s.Create(initiator, assignee, "", skill, artifacts, nil)
	if err != nil {
		t.Fatal(err)
	}
	got, _ := s.Get(task.Id)
	if len(got.InputArtifacts) != 2 {
		t.Errorf("expected 2 input artifacts, got %d", len(got.InputArtifacts))
	}
}

func TestCreate_WithMetadata(t *testing.T) {
	s := newTestStore(t)
	meta := map[string]string{"priority": "high", "source": "test"}
	task, err := s.Create(initiator, assignee, "", skill, nil, meta)
	if err != nil {
		t.Fatal(err)
	}
	got, _ := s.Get(task.Id)
	if got.Metadata["priority"] != "high" {
		t.Errorf("metadata mismatch: %+v", got.Metadata)
	}
}

func TestList(t *testing.T) {
	s := newTestStore(t)

	// initiator tasks
	s.Create(initiator, "did:key:zOther", "", skill, nil, nil)
	s.Create(initiator, "did:key:zOther", "", skill, nil, nil)
	// assignee task
	s.Create("did:key:zOther", assignee, "", skill, nil, nil)
	// unrelated task
	s.Create("did:key:zX", "did:key:zY", "", skill, nil, nil)

	tasks, err := s.List(initiator, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(tasks) != 2 {
		t.Errorf("expected 2 tasks for initiator, got %d", len(tasks))
	}

	tasks, err = s.List(assignee, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(tasks) != 1 {
		t.Errorf("expected 1 task for assignee, got %d", len(tasks))
	}
}

func TestList_Limit(t *testing.T) {
	s := newTestStore(t)
	for i := 0; i < 5; i++ {
		s.Create(initiator, assignee, "", skill, nil, nil)
	}
	tasks, _ := s.List(initiator, 3)
	if len(tasks) != 3 {
		t.Errorf("expected 3 with limit, got %d", len(tasks))
	}
}

func TestUpdatedAt_Changes(t *testing.T) {
	s := newTestStore(t)
	task, _ := s.Create(initiator, assignee, "", skill, nil, nil)
	original := task.UpdatedAt

	// small sleep to ensure timestamp differs
	updated, _ := s.UpdateStatus(task.Id, pb.TaskStatus_TASK_STATUS_WORKING, "", nil)
	if updated.UpdatedAt < original {
		t.Error("UpdatedAt did not advance after status update")
	}
}

// ─── invariants ────────────────────────────────────────────────────────────────

func TestCreate_RejectsEmptySkill(t *testing.T) {
	s := newTestStore(t)
	_, err := s.Create(initiator, assignee, "", "", nil, nil)
	if !errors.Is(err, ErrInvalidTask) {
		t.Fatalf("expected ErrInvalidTask, got %v", err)
	}
}

func TestCreate_RejectsSelfDelegation(t *testing.T) {
	s := newTestStore(t)
	_, err := s.Create(initiator, initiator, "", skill, nil, nil)
	if !errors.Is(err, ErrInvalidTask) {
		t.Fatalf("expected ErrInvalidTask, got %v", err)
	}
}

// ─── transition guards ──────────────────────────────────────────────────────────

func TestUpdateStatus_RejectsSkippingWorking(t *testing.T) {
	s := newTestStore(t)
	task, _ := s.Create(initiator, assignee, "", skill, nil, nil)

	// SUBMITTED -> COMPLETED directly is not a valid transition; work must
	// start first.
	_, err := s.UpdateStatus(task.Id, pb.TaskStatus_TASK_STATUS_COMPLETED, "", nil)
	if !errors.Is(err, ErrInvalidTransition) {
		t.Fatalf("expected ErrInvalidTransition, got %v", err)
	}

	// The task must be unaffected by the rejected transition.
	got, _ := s.Get(task.Id)
	if got.Status != pb.TaskStatus_TASK_STATUS_SUBMITTED {
		t.Errorf("task status changed despite rejected transition: %v", got.Status)
	}
}

func TestUpdateStatus_RejectsTransitionOutOfTerminalState(t *testing.T) {
	s := newTestStore(t)
	task, _ := s.Create(initiator, assignee, "", skill, nil, nil)
	if _, err := s.Cancel(task.Id); err != nil {
		t.Fatalf("Cancel: %v", err)
	}

	// A cancelled task can never move again — not even back to WORKING.
	_, err := s.UpdateStatus(task.Id, pb.TaskStatus_TASK_STATUS_WORKING, "", nil)
	if !errors.Is(err, ErrInvalidTransition) {
		t.Fatalf("expected ErrInvalidTransition, got %v", err)
	}
}

func TestUpdateStatus_RejectsDoubleCompletion(t *testing.T) {
	s := newTestStore(t)
	task, _ := s.Create(initiator, assignee, "", skill, nil, nil)
	if _, err := s.StartWork(task.Id); err != nil {
		t.Fatalf("StartWork: %v", err)
	}
	if _, err := s.Complete(task.Id, nil); err != nil {
		t.Fatalf("Complete: %v", err)
	}

	// Completing an already-completed task must be rejected, not silently
	// re-applied — this is the concrete race a worker and a coordinator
	// could hit if both try to settle the same task.
	_, err := s.Complete(task.Id, nil)
	if !errors.Is(err, ErrInvalidTransition) {
		t.Fatalf("expected ErrInvalidTransition, got %v", err)
	}
}

func TestUpdateStatus_UnknownTaskID(t *testing.T) {
	s := newTestStore(t)
	_, err := s.UpdateStatus("does-not-exist", pb.TaskStatus_TASK_STATUS_WORKING, "", nil)
	if err == nil {
		t.Fatal("expected error for unknown task ID")
	}
}

func TestNamedTransitions_FullLifecycle(t *testing.T) {
	s := newTestStore(t)
	task, _ := s.Create(initiator, assignee, "", skill, nil, nil)

	if _, err := s.StartWork(task.Id); err != nil {
		t.Fatalf("StartWork: %v", err)
	}
	artifacts := []*pb.Artifact{{Cid: "sha256:abc", MimeType: "text/plain", Size: 3}}
	completed, err := s.Complete(task.Id, artifacts)
	if err != nil {
		t.Fatalf("Complete: %v", err)
	}
	if completed.Status != pb.TaskStatus_TASK_STATUS_COMPLETED {
		t.Errorf("expected COMPLETED, got %v", completed.Status)
	}
	if len(completed.OutputArtifacts) != 1 {
		t.Errorf("expected 1 output artifact, got %d", len(completed.OutputArtifacts))
	}
}

func TestNamedTransitions_Fail(t *testing.T) {
	s := newTestStore(t)
	task, _ := s.Create(initiator, assignee, "", skill, nil, nil)
	if _, err := s.StartWork(task.Id); err != nil {
		t.Fatalf("StartWork: %v", err)
	}

	failed, err := s.Fail(task.Id, "boom")
	if err != nil {
		t.Fatalf("Fail: %v", err)
	}
	if failed.Status != pb.TaskStatus_TASK_STATUS_FAILED {
		t.Errorf("expected FAILED, got %v", failed.Status)
	}
	if failed.Error != "boom" {
		t.Errorf("error mismatch: %q", failed.Error)
	}
}

func TestLeaseClaimRenewAndFinishAreWorkerBound(t *testing.T) {
	s := newTestStore(t)
	task, err := s.Create(initiator, assignee, "", skill, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	lease, err := s.Claim(task.Id, assignee, time.Minute)
	if err != nil || lease.LeaseToken == "" {
		t.Fatalf("Claim = %#v, %v", lease, err)
	}
	again, err := s.Claim(task.Id, assignee, time.Minute)
	if err != nil || again.LeaseToken != lease.LeaseToken {
		t.Fatalf("duplicate claim = %#v, %v", again, err)
	}
	if _, err := s.Claim(task.Id, "did:key:zOther", time.Minute); !errors.Is(err, ErrLeaseConflict) {
		t.Fatalf("other worker claim = %v", err)
	}
	if _, err := s.RenewLease(task.Id, assignee, lease.LeaseToken, time.Minute); err != nil {
		t.Fatalf("renew: %v", err)
	}
	if _, err := s.FinishLease(task.Id, assignee, "wrong", pb.TaskStatus_TASK_STATUS_COMPLETED, "", nil); !errors.Is(err, ErrLeaseExpired) {
		t.Fatalf("wrong lease completion = %v", err)
	}
	finished, err := s.FinishLease(task.Id, assignee, lease.LeaseToken, pb.TaskStatus_TASK_STATUS_COMPLETED, "", nil)
	if err != nil || finished.Status != pb.TaskStatus_TASK_STATUS_COMPLETED {
		t.Fatalf("finish = %#v, %v", finished, err)
	}
}

func TestCreateIdempotentReturnsOriginalTask(t *testing.T) {
	s := newTestStore(t)
	first, err := s.CreateIdempotent(initiator, assignee, "", skill, nil, nil, "request-1")
	if err != nil {
		t.Fatal(err)
	}
	second, err := s.CreateIdempotent(initiator, assignee, "", skill, nil, nil, "request-1")
	if err != nil {
		t.Fatal(err)
	}
	if first.Id != second.Id {
		t.Fatalf("idempotent create made %s and %s", first.Id, second.Id)
	}
}

func TestExpiredLeaseStopsAtAttemptBudget(t *testing.T) {
	s := newTestStore(t)
	task, err := s.Create(initiator, assignee, "", skill, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.SetMaxAttempts(task.Id, 1); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Claim(task.Id, assignee, time.Millisecond); err != nil {
		t.Fatal(err)
	}
	time.Sleep(3 * time.Millisecond)
	if _, err := s.Claim(task.Id, assignee, time.Second); !errors.Is(err, ErrAttemptsExhausted) {
		t.Fatalf("claim after exhausted lease = %v", err)
	}
	got, err := s.Get(task.Id)
	if err != nil {
		t.Fatal(err)
	}
	if got.Status != pb.TaskStatus_TASK_STATUS_FAILED {
		t.Fatalf("status = %v", got.Status)
	}
}

func TestDeadlinePreventsClaim(t *testing.T) {
	s := newTestStore(t)
	task, err := s.Create(initiator, assignee, "", skill, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.SetTimeout(task.Id, time.Millisecond); err != nil {
		t.Fatal(err)
	}
	time.Sleep(3 * time.Millisecond)
	if _, err := s.Claim(task.Id, assignee, time.Second); !errors.Is(err, ErrAttemptsExhausted) {
		t.Fatalf("expired task claim = %v", err)
	}
	got, err := s.Get(task.Id)
	if err != nil {
		t.Fatal(err)
	}
	if got.Status != pb.TaskStatus_TASK_STATUS_FAILED {
		t.Fatalf("status = %v", got.Status)
	}
}
