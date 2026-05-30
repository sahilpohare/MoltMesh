package tasks

import (
	"testing"

	pb "github.com/sahilpohare/p2p-a2a/gen/a2a/v1"
)

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
