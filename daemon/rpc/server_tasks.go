package rpc

import (
	"context"
	"fmt"
	"io"
	"time"

	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"

	"github.com/google/uuid"
	pb "github.com/sahilpohare/p2p-a2a/gen/a2a/v1"
)

// authorizeTask keeps task rows isolated between SDK identities sharing a
// daemon. Reads are available to the two task parties; mutations remain with
// the initiator until the leased-worker protocol replaces this legacy update
// path. Unscoped requests retain single-agent compatibility.
func (s *Server) authorizeTask(ctx context.Context, task *pb.Task, mutate bool) error {
	owner, err := s.scopedOwner(ctx)
	if err != nil || owner == "" {
		return err
	}
	if owner != task.Initiator && (mutate || owner != task.Assignee) {
		return fmt.Errorf("task %s is not owned by authenticated agent", task.Id)
	}
	return nil
}

// ─── Registry ────────────────────────────────────────────────────────────────

// ClaimTask gives an SDK worker an exclusive, renewable durable lease.
func (s *Server) ClaimTask(ctx context.Context, req *pb.ClaimTaskRequest) (*pb.TaskLease, error) {
	worker, err := s.agentDID(ctx)
	if err != nil {
		return nil, err
	}
	return s.tasks.Claim(req.TaskId, worker, time.Duration(req.LeaseSeconds)*time.Second)
}

func (s *Server) RenewTaskLease(ctx context.Context, req *pb.RenewTaskLeaseRequest) (*pb.TaskLease, error) {
	worker, err := s.agentDID(ctx)
	if err != nil {
		return nil, err
	}
	return s.tasks.RenewLease(req.TaskId, worker, req.LeaseToken, time.Duration(req.LeaseSeconds)*time.Second)
}

func (s *Server) CompleteTask(ctx context.Context, req *pb.CompleteTaskRequest) (*pb.Task, error) {
	worker, err := s.agentDID(ctx)
	if err != nil {
		return nil, err
	}
	task, err := s.tasks.FinishLease(req.TaskId, worker, req.LeaseToken, pb.TaskStatus_TASK_STATUS_COMPLETED, "", req.OutputArtifacts)
	if err != nil {
		return nil, err
	}
	s.notifyRemoteInitiator(worker, task, "")
	return task, nil
}

func (s *Server) FailTask(ctx context.Context, req *pb.FailTaskRequest) (*pb.Task, error) {
	worker, err := s.agentDID(ctx)
	if err != nil {
		return nil, err
	}
	task, err := s.tasks.FinishLease(req.TaskId, worker, req.LeaseToken, pb.TaskStatus_TASK_STATUS_FAILED, req.Error, nil)
	if err != nil {
		return nil, err
	}
	s.notifyRemoteInitiator(worker, task, req.Error)
	return task, nil
}

// notifyRemoteInitiator sends a TASK_RESULT message back to a task's
// initiator when it lives on a different daemon than this one. Without
// this, CompleteTask/FailTask (the RPCs a DurableWorker's completeTaskLease/
// failTaskLease call) only ever updated the local task row FinishLease
// wrote — this daemon's own materialized copy of a task CreateFromRemote
// created on delivery — and a remote initiator's own row stayed in
// SUBMITTED/WORKING forever, no matter how long it polled GetTask, because
// nothing ever told it the task was done. SendTaskResult (above) already
// does this send manually for callers that want to report a result
// out-of-band; this is the same message, sent automatically for the
// worker-lease completion path every DurableWorker actually uses.
func (s *Server) notifyRemoteInitiator(fromDID string, task *pb.Task, errMsg string) {
	if task == nil || task.Initiator == "" || task.Initiator == s.id.DID {
		return
	}
	result := &pb.TaskResult{TaskId: task.Id, Status: task.Status, Error: errMsg, OutputArtifacts: task.OutputArtifacts}
	payload, err := proto.Marshal(result)
	if err != nil {
		s.log.Warn("encode task result", zap.String("task_id", task.Id), zap.Error(err))
		return
	}
	msg := &pb.Message{
		Id:       uuid.NewString(),
		FromDid:  fromDID,
		ToDid:    task.Initiator,
		ThreadId: task.ThreadId,
		TaskId:   task.Id,
		Kind:     pb.MessageKind_MESSAGE_KIND_TASK_RESULT,
		Payload:  payload,
		SentAt:   time.Now().UnixMilli(),
	}
	if err := s.outbox.EnqueueForOwner(fromDID, msg); err != nil {
		s.log.Warn("enqueue task result", zap.String("task_id", task.Id), zap.Error(err))
	}
}

// SubscribeTasks is a durable, resumable worker stream. Delivery sequence is
// assigned by the task store (not by this connection), allowing an SDK to
// reconnect with AfterSequence and continue exactly after its persisted cursor.
// ClaimTask remains the atomic delivery acknowledgement: receiving a delivery
// alone never assigns work.
func (s *Server) SubscribeTasks(req *pb.WorkerSubscription, stream pb.A2ANode_SubscribeTasksServer) error {
	worker, err := s.agentDID(stream.Context())
	if err != nil {
		return err
	}
	after := req.AfterSequence
	ticker := time.NewTicker(250 * time.Millisecond)
	defer ticker.Stop()
	for {
		deliveries, err := s.tasks.ListDeliveries(worker, req.Skills, after, 100)
		if err != nil {
			return err
		}
		for _, delivery := range deliveries {
			if err := stream.Send(delivery); err != nil {
				return err
			}
			after = delivery.Sequence
		}
		select {
		case <-stream.Context().Done():
			return stream.Context().Err()
		case <-ticker.C:
		}
	}
}

func (s *Server) CreateTask(ctx context.Context, req *pb.CreateTaskRequest) (*pb.Task, error) {
	t := req.Task
	if t == nil {
		return nil, fmt.Errorf("create task: task is required")
	}
	owner, err := s.scopedOwner(ctx)
	if err != nil {
		return nil, err
	}
	initiator := s.id.DID
	if owner != "" {
		initiator = owner
	}
	task, err := s.tasks.CreateIdempotent(
		initiator, req.ToDid, t.ThreadId, t.Skill, t.InputArtifacts, t.Metadata, req.IdempotencyKey,
	)
	if err != nil {
		return nil, fmt.Errorf("create task: %w", err)
	}
	if err := s.tasks.SetMaxAttempts(task.Id, int(req.MaxAttempts)); err != nil {
		return nil, fmt.Errorf("create task: set max attempts: %w", err)
	}
	if err := s.tasks.SetTimeout(task.Id, time.Duration(req.TimeoutMs)*time.Millisecond); err != nil {
		return nil, fmt.Errorf("create task: set timeout: %w", err)
	}

	// Notify assignee via outbox. If this fails, the task would otherwise sit
	// in SUBMITTED forever with no notification ever sent and no way for the
	// initiator to learn that — so fail the task instead of swallowing the
	// error, and surface it to the caller.
	payload, err := proto.Marshal(t)
	if err != nil {
		return nil, fmt.Errorf("create task: encode request: %w", err)
	}
	msg := &pb.Message{
		Id:       uuid.New().String(),
		FromDid:  initiator,
		ToDid:    req.ToDid,
		TaskId:   task.Id,
		ThreadId: task.ThreadId,
		Kind:     pb.MessageKind_MESSAGE_KIND_TASK_REQUEST,
		Payload:  payload,
		SentAt:   time.Now().UnixMilli(),
	}
	if err := s.outbox.EnqueueForOwner(initiator, msg); err != nil {
		s.log.Warn("enqueue task request", zap.String("task_id", task.Id), zap.Error(err))
		if failed, failErr := s.tasks.Fail(task.Id, fmt.Sprintf("failed to notify assignee: %v", err)); failErr != nil {
			s.log.Error("mark task failed after enqueue failure", zap.String("task_id", task.Id), zap.Error(failErr))
		} else {
			task = failed
		}
		return task, fmt.Errorf("create task: notify assignee: %w", err)
	}

	return task, nil
}

func (s *Server) GetTask(ctx context.Context, req *pb.TaskID) (*pb.Task, error) {
	task, err := s.tasks.Get(req.Id)
	if err != nil {
		return nil, err
	}
	if err := s.authorizeTask(ctx, task, false); err != nil {
		return nil, err
	}
	return task, nil
}

func (s *Server) UpdateTask(ctx context.Context, req *pb.TaskStatusUpdate) (*pb.Task, error) {
	current, err := s.tasks.Get(req.TaskId)
	if err != nil {
		return nil, err
	}
	if err := s.authorizeTask(ctx, current, true); err != nil {
		return nil, err
	}
	task, err := s.tasks.UpdateStatus(req.TaskId, req.Status, req.Error, req.OutputArtifacts)
	if err != nil {
		return nil, err
	}

	// broadcast status change
	event := &pb.TaskEvent{
		TaskId: task.Id,
		Status: task.Status,
		Kind:   pb.EventKind_EVENT_KIND_STATUS_UPDATE,
	}
	if err := s.gossip.PublishTaskEvent(ctx, event); err != nil {
		s.log.Warn("publish task event", zap.Error(err))
	}

	// if terminal state, broadcast done
	if task.Status == pb.TaskStatus_TASK_STATUS_COMPLETED ||
		task.Status == pb.TaskStatus_TASK_STATUS_FAILED {
		if err := s.gossip.PublishTaskDone(ctx, task); err != nil {
			s.log.Warn("publish task done", zap.Error(err))
		}
	}

	return task, nil
}

func (s *Server) CancelTask(ctx context.Context, req *pb.TaskID) (*pb.Task, error) {
	task, err := s.tasks.Get(req.Id)
	if err != nil {
		return nil, err
	}
	if err := s.authorizeTask(ctx, task, true); err != nil {
		return nil, err
	}
	return s.tasks.Cancel(req.Id)
}

func (s *Server) PublishTaskEvent(ctx context.Context, event *pb.TaskEvent) (*pb.Empty, error) {
	task, err := s.tasks.Get(event.TaskId)
	if err != nil {
		return nil, err
	}
	if err := s.authorizeTask(ctx, task, false); err != nil {
		return nil, err
	}
	if event.EmittedAt == 0 {
		event.EmittedAt = time.Now().UnixMilli()
	}
	if event.Seq == 0 {
		event.Seq = time.Now().UnixNano()
	}
	if err := s.tasks.SaveEvent(event); err != nil {
		return nil, err
	}
	return &pb.Empty{}, s.gossip.PublishTaskEvent(ctx, event)
}

// SendTaskResult routes a terminal result back to the task initiator using
// the durable authenticated outbox path.
func (s *Server) SendTaskResult(ctx context.Context, req *pb.SendTaskResultRequest) (*pb.SendResult, error) {
	if req == nil || req.Result == nil || req.Result.TaskId == "" || req.ToDid == "" {
		return nil, fmt.Errorf("task result requires to_did and task_id")
	}
	if req.Result.Status != pb.TaskStatus_TASK_STATUS_COMPLETED && req.Result.Status != pb.TaskStatus_TASK_STATUS_FAILED {
		return nil, fmt.Errorf("task result status must be completed or failed")
	}
	payload, err := proto.Marshal(req.Result)
	if err != nil {
		return nil, err
	}
	owner, ownerErr := s.scopedOwner(ctx)
	if ownerErr != nil {
		return nil, ownerErr
	}
	fromDID := s.id.DID
	if owner != "" {
		fromDID = owner
	}
	msg := &pb.Message{Id: uuid.NewString(), FromDid: fromDID, ToDid: req.ToDid, ThreadId: req.ThreadId, TaskId: req.Result.TaskId, Kind: pb.MessageKind_MESSAGE_KIND_TASK_RESULT, Payload: payload, SentAt: time.Now().UnixMilli()}
	if err := s.outbox.EnqueueForOwner(fromDID, msg); err != nil {
		return nil, err
	}
	return &pb.SendResult{MessageId: msg.Id, Queued: true}, nil
}

// HandleIncoming applies authenticated application messages after Deliverer
// has persisted them. The libp2p peer/DID binding was already verified.
func (s *Server) HandleIncoming(msg *pb.Message) error {
	switch msg.Kind {
	case pb.MessageKind_MESSAGE_KIND_TASK_REQUEST:
		return s.handleTaskRequest(msg)
	case pb.MessageKind_MESSAGE_KIND_TASK_RESULT:
		return s.handleTaskResult(msg)
	default:
		return nil
	}
}

// handleTaskRequest materializes a task on the assignee's own daemon on
// receipt — without this, a task delegated across daemons is recorded only
// on the initiator's side, and the assignee's SubscribeTasks/ClaimTask have
// no local row to ever find.
func (s *Server) handleTaskRequest(msg *pb.Message) error {
	// CreateTask (above) marshals req.Task, a *pb.TaskRequest, into this same
	// Payload — TaskRequest.skill is field 1, while Task.skill is field 5, so
	// unmarshaling into a pb.Task here silently decoded the skill string into
	// the wrong field (Task.Id) instead of erroring, leaving t.Skill always
	// empty and every cross-daemon task delegation rejected downstream with
	// "skill is required".
	var t pb.TaskRequest
	if err := proto.Unmarshal(msg.Payload, &t); err != nil {
		return fmt.Errorf("decode task request: %w", err)
	}
	_, err := s.tasks.CreateFromRemote(msg.TaskId, msg.FromDid, msg.ToDid, msg.ThreadId, t.Skill, t.InputArtifacts, t.Metadata)
	return err
}

func (s *Server) handleTaskResult(msg *pb.Message) error {
	var result pb.TaskResult
	if err := proto.Unmarshal(msg.Payload, &result); err != nil {
		return fmt.Errorf("decode task result: %w", err)
	}
	task, err := s.tasks.Get(result.TaskId)
	if err != nil {
		return err
	}
	if task.Initiator != msg.ToDid || task.Assignee != msg.FromDid || msg.TaskId != task.Id || msg.ThreadId != task.ThreadId {
		return fmt.Errorf("task result sender or correlation mismatch")
	}
	if task.Status == result.Status && terminalTaskStatus(task.Status) {
		return nil
	}
	if task.Status == pb.TaskStatus_TASK_STATUS_SUBMITTED {
		if _, err := s.tasks.StartWork(task.Id); err != nil {
			return err
		}
	}
	updated, err := s.tasks.UpdateStatus(task.Id, result.Status, result.Error, result.OutputArtifacts)
	if err != nil {
		return err
	}
	event := &pb.TaskEvent{TaskId: task.Id, Status: updated.Status, Kind: pb.EventKind_EVENT_KIND_DONE, Data: result.Data, Seq: time.Now().UnixNano(), EmittedAt: time.Now().UnixMilli()}
	if result.Status == pb.TaskStatus_TASK_STATUS_FAILED {
		event.Kind = pb.EventKind_EVENT_KIND_ERROR
	}
	if err := s.tasks.SaveEvent(event); err != nil {
		return err
	}
	if task.ThreadId != "" && s.threads != nil {
		data, marshalErr := proto.Marshal(event)
		if marshalErr != nil {
			return marshalErr
		}
		if err := s.threads.AppendEntry(task.ThreadId, &pb.ThreadEntry{AuthorDid: msg.FromDid, Payload: data, Kind: "task_result", SubmittedAt: time.Now().UnixMilli()}); err != nil {
			return err
		}
	}
	if s.gossip != nil {
		_ = s.gossip.PublishTaskEvent(context.Background(), event)
	}
	return nil
}

func terminalTaskStatus(status pb.TaskStatus) bool {
	return status == pb.TaskStatus_TASK_STATUS_COMPLETED || status == pb.TaskStatus_TASK_STATUS_FAILED || status == pb.TaskStatus_TASK_STATUS_CANCELLED
}

func (s *Server) SubscribeTaskEvents(req *pb.TaskID, stream pb.A2ANode_SubscribeTaskEventsServer) error {
	task, err := s.tasks.Get(req.Id)
	if err != nil {
		return err
	}
	if err := s.authorizeTask(stream.Context(), task, false); err != nil {
		return err
	}
	ch := make(chan *pb.TaskEvent, 64)
	backlog, err := s.tasks.ListEvents(req.Id, req.AfterSequence)
	if err != nil {
		return err
	}
	for _, event := range backlog {
		if err := stream.Send(event); err != nil {
			return err
		}
	}

	if err := s.gossip.SubscribeTaskEvents(stream.Context(), req.Id, func(e *pb.TaskEvent) {
		select {
		case ch <- e:
		default:
			s.log.Warn("task event channel full, dropping", zap.String("task_id", req.Id))
		}
	}); err != nil {
		return err
	}

	for {
		select {
		case <-stream.Context().Done():
			return nil
		case event, ok := <-ch:
			if !ok {
				return io.EOF
			}
			if event.Seq <= req.AfterSequence {
				continue
			}
			if err := stream.Send(event); err != nil {
				return err
			}
		}
	}
}

// ─── Files ────────────────────────────────────────────────────────────────────
