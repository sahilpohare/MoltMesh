package rpc

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"time"

	"go.uber.org/zap"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/proto"

	"github.com/google/uuid"
	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	appactors "github.com/sahilpohare/p2p-a2a/daemon/actors"
	"github.com/sahilpohare/p2p-a2a/daemon/deliver"
	"github.com/sahilpohare/p2p-a2a/daemon/gossip"
	"github.com/sahilpohare/p2p-a2a/daemon/identity"
	"github.com/sahilpohare/p2p-a2a/daemon/inbox"
	"github.com/sahilpohare/p2p-a2a/daemon/names"
	"github.com/sahilpohare/p2p-a2a/daemon/network"
	"github.com/sahilpohare/p2p-a2a/daemon/node"
	"github.com/sahilpohare/p2p-a2a/daemon/outbox"
	"github.com/sahilpohare/p2p-a2a/daemon/registry"
	"github.com/sahilpohare/p2p-a2a/daemon/session"
	"github.com/sahilpohare/p2p-a2a/daemon/tasks"
	"github.com/sahilpohare/p2p-a2a/daemon/thread"
	"github.com/sahilpohare/p2p-a2a/daemon/webhook"
	pb "github.com/sahilpohare/p2p-a2a/gen/a2a/v1"
	"github.com/sahilpohare/p2p-a2a/pkg/p2putil"
	"github.com/sahilpohare/p2p-a2a/pkg/threadcrypto"
)

// ThreadManager is the thread-subsystem surface the gRPC server needs.
// Satisfied by *thread.Manager (Engine/GossipBridge/RaftBackend.Run path)
// and, once wired at boot, an actors.ActorManager (ThreadActor/GoAkt path).
// Engine must return a true nil thread.ThreadSubscription (not a nil pointer
// boxed into the interface) when the thread exists but this node is not a
// validator for it — see thread.Manager.Engine's doc comment for why.
type ThreadManager interface {
	CreateThread(ctx context.Context, req *pb.CreateThreadRequest) (*pb.Thread, error)
	GetThread(threadID string) (*pb.Thread, error)
	AppendEntry(threadID string, entry *pb.ThreadEntry) error
	GetEntries(threadID string, sinceHeight int64, limit int) ([]*pb.ThreadEntryWithPos, error)
	Engine(threadID string) thread.ThreadSubscription
}

// Server implements the A2ANode gRPC service.
type Server struct {
	pb.UnimplementedA2ANodeServer

	id       *identity.Identity
	inbox    *inbox.Inbox
	outbox   *outbox.Outbox
	tasks    *tasks.Store
	registry *registry.Registry
	gossip   *gossip.Manager
	// blobs field removed — Bitswap + blockstore accessed via s.node.Bitswap / s.node.Blockstore
	dlv       *deliver.Deliverer
	threads   ThreadManager
	networks  *network.Manager
	webhooks  *webhook.Dispatcher
	nameReg   *names.Registry
	node      *node.Node
	addrs     []string
	startedAt time.Time
	sessions  *session.Manager
	log       *zap.Logger
}

// New creates a new gRPC server. sessions is shared with the Deliverer
// (daemon/deliver) rather than constructed here, so both agree on which SDK
// identities currently have a live session on this daemon — see
// deliver.SessionChecker for why that agreement has to be exact.
func New(
	id *identity.Identity,
	ib *inbox.Inbox,
	ob *outbox.Outbox,
	ts *tasks.Store,
	reg *registry.Registry,
	gm *gossip.Manager,
	dlv *deliver.Deliverer,
	tm ThreadManager,
	nm *network.Manager,
	wh *webhook.Dispatcher,
	nr *names.Registry,
	n *node.Node,
	addrs []string,
	sessions *session.Manager,
	log *zap.Logger,
) *Server {
	if sessions == nil {
		// Callers that don't share a Deliverer-visible session.Manager (tests,
		// or any future caller that never needs cross-daemon delivery to a
		// session-scoped worker) still get a working one instead of a nil
		// pointer every session-authenticated RPC would panic on.
		sessions = session.New(id.DID)
	}
	return &Server{
		id:        id,
		inbox:     ib,
		outbox:    ob,
		tasks:     ts,
		registry:  reg,
		gossip:    gm,
		dlv:       dlv,
		threads:   tm,
		networks:  nm,
		webhooks:  wh,
		nameReg:   nr,
		node:      n,
		addrs:     addrs,
		startedAt: time.Now(),
		sessions:  sessions,
		log:       log,
	}
}

// ─── Identity ────────────────────────────────────────────────────────────────

func (s *Server) GetIdentity(_ context.Context, _ *pb.Empty) (*pb.AgentIdentity, error) {
	return &pb.AgentIdentity{
		Did:        s.id.DID,
		PublicKey:  s.id.PublicKeyBase64(),
		Multiaddrs: s.addrs,
	}, nil
}

// GetNodeIdentity returns the daemon transport identity. GetIdentity remains
// available for legacy single-agent callers; new SDK code uses this method to
// avoid confusing daemon and agent identities.
func (s *Server) GetNodeIdentity(_ context.Context, _ *pb.Empty) (*pb.NodeIdentity, error) {
	peerID := ""
	if s.node != nil && s.node.Host != nil {
		peerID = s.node.Host.ID().String()
	}
	return &pb.NodeIdentity{NodeId: s.id.DID, PeerId: peerID, Multiaddrs: s.addrs}, nil
}

func (s *Server) BeginAgentSession(_ context.Context, req *pb.BeginAgentSessionRequest) (*pb.AgentChallenge, error) {
	if req == nil {
		return nil, fmt.Errorf("agent identity is required")
	}
	return s.sessions.Begin(req.Identity)
}

func (s *Server) CompleteAgentSession(_ context.Context, req *pb.CompleteAgentSessionRequest) (*pb.AgentSession, error) {
	return s.sessions.Complete(req)
}

func (s *Server) GetAgentIdentity(ctx context.Context, _ *pb.Empty) (*pb.AgentIdentity, error) {
	return s.sessions.Identity(bearerToken(ctx))
}

func (s *Server) CloseAgentSession(ctx context.Context, _ *pb.Empty) (*pb.Empty, error) {
	s.sessions.Close(bearerToken(ctx))
	return &pb.Empty{}, nil
}

func (s *Server) agentDID(ctx context.Context) (string, error) {
	return s.sessions.Authenticate(bearerToken(ctx))
}

// scopedOwner returns the authenticated SDK DID when a session is supplied.
// The empty owner keeps the explicitly supported legacy single-agent mode.
func (s *Server) scopedOwner(ctx context.Context) (string, error) {
	if bearerToken(ctx) == "" {
		if s.sessions.MultipleAgents() {
			return "", fmt.Errorf("unscoped RPC is not allowed while multiple SDK agents are connected")
		}
		return "", nil
	}
	return s.agentDID(ctx)
}

func bearerToken(ctx context.Context) string {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return ""
	}
	for _, value := range md.Get("authorization") {
		const prefix = "Bearer "
		if len(value) > len(prefix) && value[:len(prefix)] == prefix {
			return value[len(prefix):]
		}
	}
	return ""
}

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

func (s *Server) PublishAgentCard(ctx context.Context, card *pb.AgentCard) (*pb.PublishResult, error) {
	owner, err := s.scopedOwner(ctx)
	if err != nil {
		return nil, err
	}
	if owner != "" {
		if card == nil || card.Did != owner {
			return &pb.PublishResult{Success: false, Error: "agent card DID must match authenticated session"}, nil
		}
		if s.node != nil && s.node.Host != nil && card.NodePeerId != s.node.Host.ID().String() {
			return &pb.PublishResult{Success: false, Error: "agent card node peer ID must match this daemon"}, nil
		}
		bound, err := s.sessions.Identity(bearerToken(ctx))
		if err != nil {
			return nil, err
		}
		if !bytes.Equal(card.EncryptionPublicKey, bound.EncryptionPublicKey) {
			return &pb.PublishResult{Success: false, Error: "agent card encryption key must match authenticated session"}, nil
		}
		if err := s.registry.PublishSigned(ctx, card); err != nil {
			return &pb.PublishResult{Success: false, Error: err.Error()}, nil
		}
		return &pb.PublishResult{Success: true}, nil
	}
	if err := s.registry.Publish(ctx, card); err != nil {
		return &pb.PublishResult{Success: false, Error: err.Error()}, nil
	}
	// advertise all skills in DHT
	for _, skill := range card.Skills {
		if err := s.registry.AdvertiseCapability(ctx, skill.Id); err != nil {
			s.log.Warn("advertise capability", zap.String("skill", skill.Id), zap.Error(err))
		}
	}
	return &pb.PublishResult{Success: true}, nil
}

func (s *Server) GetAgentCard(ctx context.Context, req *pb.AgentIdentityRequest) (*pb.AgentCard, error) {
	card, err := s.registry.Resolve(ctx, req.Did)
	if err != nil {
		return nil, fmt.Errorf("resolve %q: %w", req.Did, err)
	}
	return card, nil
}

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

func (s *Server) FindAgents(req *pb.CapabilityQuery, stream pb.A2ANode_FindAgentsServer) error {
	limit := int(req.Limit)
	if limit == 0 {
		limit = 10
	}
	cards, err := s.registry.FindByCapability(stream.Context(), req.Capability, limit)
	if err != nil {
		return err
	}
	for _, card := range cards {
		if err := stream.Send(card); err != nil {
			return err
		}
	}
	return nil
}

// ─── Messaging ───────────────────────────────────────────────────────────────

func (s *Server) SendMessage(ctx context.Context, msg *pb.Message) (*pb.SendResult, error) {
	owner, err := s.scopedOwner(ctx)
	if err != nil {
		return nil, err
	}
	if msg.Id == "" {
		msg.Id = uuid.New().String()
	}
	if owner != "" {
		msg.FromDid = owner
	} else {
		msg.FromDid = s.id.DID
	}

	if err := s.outbox.EnqueueForOwner(owner, msg); err != nil {
		return nil, fmt.Errorf("enqueue: %w", err)
	}
	return &pb.SendResult{MessageId: msg.Id, Queued: true}, nil
}

func (s *Server) SubscribeInbox(req *pb.SubscribeRequest, stream pb.A2ANode_SubscribeInboxServer) error {
	owner, err := s.scopedOwner(stream.Context())
	if err != nil {
		return err
	}
	// Flush existing messages first.
	msgs, err := s.inbox.GetForOwner(owner, req.ThreadId, req.TaskId, false, 100, 0)
	if err != nil {
		return err
	}
	for _, m := range msgs {
		if err := stream.Send(m); err != nil {
			return err
		}
	}

	// Stream live arrivals.
	ch := s.inbox.SubscribeForOwner(owner)
	defer s.inbox.Unsubscribe(ch)

	for {
		select {
		case <-stream.Context().Done():
			return nil
		case msg, ok := <-ch:
			if !ok {
				return nil
			}
			// Apply filters from the subscription request.
			if req.ThreadId != "" && msg.ThreadId != req.ThreadId {
				continue
			}
			if req.TaskId != "" && msg.TaskId != req.TaskId {
				continue
			}
			if err := stream.Send(msg); err != nil {
				return err
			}
		}
	}
}

func (s *Server) GetInbox(req *pb.InboxQuery, stream pb.A2ANode_GetInboxServer) error {
	owner, err := s.scopedOwner(stream.Context())
	if err != nil {
		return err
	}
	msgs, err := s.inbox.GetForOwner(owner, req.ThreadId, req.TaskId, req.UnreadOnly, int(req.Limit), req.Since)
	if err != nil {
		return err
	}
	for _, m := range msgs {
		if err := stream.Send(m); err != nil {
			return err
		}
	}
	return nil
}

func (s *Server) GetOutbox(req *pb.OutboxQuery, stream pb.A2ANode_GetOutboxServer) error {
	owner, err := s.scopedOwner(stream.Context())
	if err != nil {
		return err
	}
	status := req.Status
	if status == "" {
		status = "pending"
	}
	msgs, err := s.outbox.ListForOwner(owner, status, int(req.Limit))
	if err != nil {
		return err
	}
	for _, m := range msgs {
		if err := stream.Send(m); err != nil {
			return err
		}
	}
	return nil
}

func (s *Server) AckMessage(ctx context.Context, req *pb.AckRequest) (*pb.Empty, error) {
	owner, err := s.scopedOwner(ctx)
	if err != nil {
		return nil, err
	}
	return &pb.Empty{}, s.inbox.AckForOwner(owner, req.MessageId)
}

// webhookMessage fires the message webhook. Called by the deliver layer after inbox.Put.
func (s *Server) WebhookMessage(msg *pb.Message) {
	s.webhooks.SendForOwner(msg.ToDid, webhook.EventMessage, msg)
}

// ─── Tasks ───────────────────────────────────────────────────────────────────

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

const (
	fileChunkSize = 32 * 1024 // 32 KB per streaming chunk (HTTP/2 flow-control friendly)
	inlineMax     = 64 * 1024 // blobs ≤ 64 KB are returned inline in the Artifact
)

// SendFile stores a file in the IPFS blockstore via Bitswap and returns an Artifact.
// The CID is CIDv1 (bafy...). Blobs ≤ 64 KB are returned with Artifact.Inline populated.
func (s *Server) SendFile(ctx context.Context, req *pb.SendFileRequest) (*pb.Artifact, error) {
	if len(req.Data) == 0 {
		return nil, fmt.Errorf("file data is empty")
	}
	if s.node == nil {
		return nil, fmt.Errorf("file storage not available (node not initialised)")
	}

	blk, err := p2putil.CIDv1Block(req.Data)
	if err != nil {
		return nil, fmt.Errorf("build block: %w", err)
	}
	if err := s.node.Blockstore.Put(ctx, blk); err != nil {
		return nil, fmt.Errorf("store block: %w", err)
	}
	// Notify Bitswap so connected peers can pull it by CID.
	if s.node.Bitswap != nil {
		if err := s.node.Bitswap.NotifyNewBlocks(ctx, blk); err != nil {
			s.log.Warn("notify bitswap", zap.Error(err))
		}
	}

	cidStr := blk.Cid().String()
	artifact := &pb.Artifact{
		Cid:      cidStr,
		Name:     req.Name,
		MimeType: req.MimeType,
		Size:     int64(len(req.Data)),
	}
	if len(req.Data) <= inlineMax {
		artifact.Inline = req.Data
	} else {
		artifact.Uri = "ipfs://" + cidStr
	}

	s.log.Info("file stored",
		zap.String("cid", cidStr),
		zap.Int64("size", artifact.Size),
		zap.String("name", artifact.Name),
	)
	return artifact, nil
}

// ─── Threads ─────────────────────────────────────────────────────────────────

func (s *Server) CreateThread(ctx context.Context, req *pb.CreateThreadRequest) (*pb.Thread, error) {
	if s.threads == nil {
		return nil, fmt.Errorf("thread manager not available")
	}
	owner, err := s.scopedOwner(ctx)
	if err != nil {
		return nil, err
	}
	if owner != "" {
		if req == nil || req.CreatorDid != owner {
			return nil, fmt.Errorf("signed thread creator must match authenticated agent")
		}
	} else if req != nil && req.CreatorDid != "" {
		return nil, fmt.Errorf("signed thread creation requires an agent session")
	}
	thread, err := s.threads.CreateThread(ctx, req)
	if err != nil {
		return nil, err
	}

	if err := s.enqueueThreadWake(thread); err != nil {
		s.log.Warn("enqueue initial thread wake", zap.Error(err))
	}
	return thread, nil
}

func (s *Server) GetThread(_ context.Context, req *pb.ThreadID) (*pb.Thread, error) {
	if s.threads == nil {
		return nil, fmt.Errorf("thread manager not available")
	}
	return s.threads.GetThread(req.Id)
}

// AddThreadReplica commits a late participant as a non-voting observer. This
// expands replication/read access without pretending that an unsafe in-place
// Raft voter-set rewrite is joint consensus.
func (s *Server) AddThreadReplica(ctx context.Context, req *pb.ThreadReplicaRequest) (*pb.Thread, error) {
	if req == nil || req.ThreadId == "" || req.ReplicaDid == "" {
		return nil, fmt.Errorf("thread_id and replica_did are required")
	}
	th, err := s.threads.GetThread(req.ThreadId)
	if err != nil {
		return nil, err
	}
	owner, err := s.scopedOwner(ctx)
	if err != nil {
		return nil, err
	}
	creator := s.id.DID
	if owner != "" {
		creator = owner
	}
	if th.CreatorDid != creator {
		return nil, fmt.Errorf("only the thread creator may add an observer")
	}
	for _, did := range th.ReplicaDids {
		if did == req.ReplicaDid {
			return th, nil
		}
	}
	entry := &pb.ThreadEntry{AuthorDid: creator, Payload: []byte(req.ReplicaDid), Kind: "membership:add-observer", SubmittedAt: time.Now().UnixMilli()}
	if err := s.threads.AppendEntry(th.Id, entry); err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	deadline := time.NewTicker(50 * time.Millisecond)
	defer deadline.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-deadline.C:
			updated, getErr := s.threads.GetThread(th.Id)
			if getErr != nil {
				return nil, getErr
			}
			for _, did := range updated.ReplicaDids {
				if did == req.ReplicaDid {
					if err := s.enqueueThreadWake(updated); err != nil {
						return nil, err
					}
					return updated, nil
				}
			}
		}
	}
}

type threadHistoryImporter interface {
	ImportHistory(*pb.Thread, []*pb.ThreadBlock) error
}

type threadRecoveryAuthorizer interface {
	AuthorizeRecoveryCapability(threadID string, secret []byte) (bool, error)
}
type threadRecoveryCreator interface {
	CreateThreadWithRecovery(context.Context, *pb.CreateThreadRequest) (*pb.Thread, *pb.ThreadRecoveryHandle, error)
}
type threadMemberLister interface {
	ListMembers(string) ([]*pb.ThreadMember, error)
}
type threadMembershipEpochReader interface {
	MembershipEpoch(string) (uint64, error)
}
type threadKeyEnvelopeStore interface {
	SaveKeyEnvelope(*pb.ThreadKeyEnvelope) error
	KeyEnvelopes(string, uint64, string) ([]*pb.ThreadKeyEnvelope, error)
}
type threadRecoveryKeyEnvelopeStore interface {
	SaveRecoveryKeyEnvelope(*pb.ThreadKeyEnvelope) error
	RecoveryKeyEnvelopes(string) ([]*pb.ThreadKeyEnvelope, error)
}
type threadMemberRemover interface{ RemoveMember(string, string) error }
type threadMemberPromoter interface {
	PromoteMember(string, string) (*pb.ThreadMember, error)
}
type threadInviteSaver interface {
	SaveInvite(string, string, pb.ThreadMemberRole, []byte, int64) error
}
type threadInviteAccepter interface {
	AcceptInvite(string, string, []byte) (*pb.ThreadMember, error)
}
type threadInviteAccepterWithEpoch interface {
	AcceptInviteWithEpoch(string, string, []byte) (*pb.ThreadMember, uint64, error)
}
type threadMemberRemoverWithEpoch interface {
	RemoveMemberWithEpoch(string, string) (uint64, error)
}
type threadMemberPromoterWithEpoch interface {
	PromoteMemberWithEpoch(string, string) (*pb.ThreadMember, uint64, error)
}
type threadVoterChanger interface {
	ProposeVoterChange(context.Context, string, string, bool) error
}
type threadCommittedHeadReader interface {
	CommittedHead(string) (int64, string, error)
}

func (s *Server) InviteThreadMember(ctx context.Context, req *pb.InviteThreadMemberRequest) (*pb.ThreadMembershipChange, error) {
	if req == nil || req.ThreadId == "" || req.InviteeDid == "" || len(req.Nonce) == 0 {
		return nil, fmt.Errorf("invalid thread invite")
	}
	if req.Role != pb.ThreadMemberRole_THREAD_MEMBER_ROLE_OBSERVER {
		return nil, fmt.Errorf("new members must start as observers")
	}
	th, err := s.threads.GetThread(req.ThreadId)
	if err != nil {
		return nil, err
	}
	owner, err := s.scopedOwner(ctx)
	if err != nil {
		return nil, err
	}
	if owner != "" && owner != th.CreatorDid {
		return nil, fmt.Errorf("only thread creator may invite members")
	}
	saver, ok := s.threads.(threadInviteSaver)
	if !ok {
		return nil, fmt.Errorf("thread manager cannot save invites")
	}
	inviter := th.CreatorDid
	if owner != "" {
		inviter = owner
	}
	invite := &pb.ThreadInvitation{ThreadId: req.ThreadId, InviterDid: inviter, InviteeDid: req.InviteeDid, Role: req.Role, ExpiresAtUnixMs: req.ExpiresAtUnixMs, Nonce: req.Nonce}
	unsigned, err := proto.MarshalOptions{Deterministic: true}.Marshal(invite)
	if err != nil {
		return nil, err
	}
	if owner != "" {
		ok, err := identity.VerifyDID(owner, unsigned, req.Signature)
		if err != nil || !ok {
			return nil, fmt.Errorf("invalid invite signature")
		}
		invite.Signature = req.Signature
	} else {
		invite.Signature = s.id.Sign(unsigned)
	}
	if err := saver.SaveInvite(req.ThreadId, req.InviteeDid, req.Role, req.Nonce, req.ExpiresAtUnixMs); err != nil {
		return nil, err
	}
	wire, err := proto.Marshal(invite)
	if err != nil {
		return nil, err
	}
	return &pb.ThreadMembershipChange{ThreadId: req.ThreadId, Member: &pb.ThreadMember{Did: req.InviteeDid, Role: req.Role}, SignedInvitation: wire}, nil
}

func (s *Server) AcceptThreadInvite(ctx context.Context, req *pb.AcceptThreadInviteRequest) (*pb.Thread, error) {
	if req == nil || req.ThreadId == "" {
		return nil, fmt.Errorf("thread id required")
	}
	var invite pb.ThreadInvitation
	if err := proto.Unmarshal(req.Invite, &invite); err != nil {
		return nil, err
	}
	if invite.ThreadId != req.ThreadId || invite.ExpiresAtUnixMs <= time.Now().UnixMilli() {
		return nil, fmt.Errorf("invalid or expired invite")
	}
	sig := invite.Signature
	invite.Signature = nil
	unsigned, err := proto.MarshalOptions{Deterministic: true}.Marshal(&invite)
	if err != nil {
		return nil, err
	}
	ok, err := identity.VerifyDID(invite.InviterDid, unsigned, sig)
	if err != nil || !ok {
		return nil, fmt.Errorf("invalid invite signature")
	}
	invitee, err := s.agentDID(ctx)
	if err != nil {
		return nil, err
	}
	if invitee != invite.InviteeDid {
		return nil, fmt.Errorf("invitee does not match session")
	}
	accepter, ok := s.threads.(threadInviteAccepter)
	if !ok {
		return nil, fmt.Errorf("thread manager cannot accept invites")
	}
	if epochAccepter, ok := s.threads.(threadInviteAccepterWithEpoch); ok {
		if _, _, err = epochAccepter.AcceptInviteWithEpoch(req.ThreadId, invitee, invite.Nonce); err != nil {
			return nil, err
		}
	} else if _, err = accepter.AcceptInvite(req.ThreadId, invitee, invite.Nonce); err != nil {
		return nil, err
	}
	return s.threads.GetThread(req.ThreadId)
}

func (s *Server) RemoveThreadMember(ctx context.Context, req *pb.RemoveThreadMemberRequest) (*pb.ThreadMembershipChange, error) {
	if req == nil || req.ThreadId == "" || req.MemberDid == "" {
		return nil, fmt.Errorf("thread_id and member_did are required")
	}
	th, err := s.threads.GetThread(req.ThreadId)
	if err != nil {
		return nil, err
	}
	owner, err := s.scopedOwner(ctx)
	if err != nil {
		return nil, err
	}
	if owner != "" && owner != th.CreatorDid {
		return nil, fmt.Errorf("only thread creator may remove members")
	}
	lister, ok := s.threads.(threadMemberLister)
	if !ok {
		return nil, fmt.Errorf("thread manager cannot verify member removal")
	}
	members, listErr := lister.ListMembers(req.ThreadId)
	if listErr != nil {
		return nil, listErr
	}
	for _, member := range members {
		if member.Did == req.MemberDid && (member.Role == pb.ThreadMemberRole_THREAD_MEMBER_ROLE_VOTER || member.Role == pb.ThreadMemberRole_THREAD_MEMBER_ROLE_ADMIN) {
			changer, ok := s.threads.(threadVoterChanger)
			if !ok {
				return nil, fmt.Errorf("thread manager cannot commit voter removal")
			}
			if err := changer.ProposeVoterChange(ctx, req.ThreadId, req.MemberDid, false); err != nil {
				return nil, fmt.Errorf("commit voter removal: %w", err)
			}
			break
		}
	}
	remover, ok := s.threads.(threadMemberRemover)
	if !ok {
		return nil, fmt.Errorf("thread manager cannot remove members")
	}
	var epoch uint64
	if epochRemover, ok := s.threads.(threadMemberRemoverWithEpoch); ok {
		epoch, err = epochRemover.RemoveMemberWithEpoch(req.ThreadId, req.MemberDid)
	} else {
		err = remover.RemoveMember(req.ThreadId, req.MemberDid)
	}
	if err != nil {
		return nil, err
	}
	return &pb.ThreadMembershipChange{ThreadId: req.ThreadId, MembershipEpoch: epoch, Member: &pb.ThreadMember{Did: req.MemberDid}}, nil
}

func (s *Server) PromoteThreadMember(ctx context.Context, req *pb.PromoteThreadMemberRequest) (*pb.ThreadMembershipChange, error) {
	if req == nil || req.ThreadId == "" || req.MemberDid == "" || len(req.CatchupProof) == 0 {
		return nil, fmt.Errorf("thread_id, member_did, and catchup_proof are required")
	}
	th, err := s.threads.GetThread(req.ThreadId)
	if err != nil {
		return nil, err
	}
	owner, err := s.scopedOwner(ctx)
	if err != nil {
		return nil, err
	}
	if owner != "" && owner != th.CreatorDid {
		return nil, fmt.Errorf("only thread creator may promote members")
	}
	lister, ok := s.threads.(threadMemberLister)
	if !ok {
		return nil, fmt.Errorf("thread manager cannot verify observer promotion")
	}
	members, err := lister.ListMembers(req.ThreadId)
	if err != nil {
		return nil, err
	}
	isObserver := false
	for _, member := range members {
		if member.Did == req.MemberDid && member.Role == pb.ThreadMemberRole_THREAD_MEMBER_ROLE_OBSERVER {
			isObserver = true
			break
		}
	}
	if !isObserver {
		return nil, fmt.Errorf("only an existing observer may be promoted")
	}
	if err := s.verifyCatchupProof(req.ThreadId, req.MemberDid, req.CatchupProof); err != nil {
		return nil, err
	}
	// Promotion is not a local table update: the observer must already be a
	// known replica and its voter role becomes durable only after Raft commits
	// the matching ConfChangeV2.
	changer, ok := s.threads.(threadVoterChanger)
	if !ok {
		return nil, fmt.Errorf("thread manager cannot commit voter promotion")
	}
	if err := changer.ProposeVoterChange(ctx, req.ThreadId, req.MemberDid, true); err != nil {
		return nil, fmt.Errorf("commit voter promotion: %w", err)
	}
	p, ok := s.threads.(threadMemberPromoter)
	if !ok {
		return nil, fmt.Errorf("thread manager cannot promote members")
	}
	var m *pb.ThreadMember
	var epoch uint64
	if epochPromoter, ok := s.threads.(threadMemberPromoterWithEpoch); ok {
		m, epoch, err = epochPromoter.PromoteMemberWithEpoch(req.ThreadId, req.MemberDid)
	} else {
		m, err = p.PromoteMember(req.ThreadId, req.MemberDid)
	}
	if err != nil {
		return nil, err
	}
	return &pb.ThreadMembershipChange{ThreadId: req.ThreadId, MembershipEpoch: epoch, Member: m}, nil
}

// GetThreadCatchupState returns this authenticated member's locally committed
// history point. The SDK signs this exact state when requesting promotion from
// the creator; no unsigned claim is accepted by PromoteThreadMember.
func (s *Server) GetThreadCatchupState(ctx context.Context, req *pb.ThreadID) (*pb.ThreadCatchupState, error) {
	if req == nil || req.Id == "" {
		return nil, fmt.Errorf("thread id required")
	}
	observer, err := s.agentDID(ctx)
	if err != nil {
		return nil, err
	}
	lister, ok := s.threads.(threadMemberLister)
	if !ok {
		return nil, fmt.Errorf("thread manager cannot authorize catchup state")
	}
	members, err := lister.ListMembers(req.Id)
	if err != nil {
		return nil, err
	}
	found := false
	for _, member := range members {
		if member.Did == observer {
			found = true
			break
		}
	}
	if !found {
		return nil, fmt.Errorf("catchup state requires current thread membership")
	}
	head, ok := s.threads.(threadCommittedHeadReader)
	if !ok {
		return nil, fmt.Errorf("thread manager cannot read committed head")
	}
	height, hash, err := head.CommittedHead(req.Id)
	if err != nil {
		return nil, err
	}
	return &pb.ThreadCatchupState{ThreadId: req.Id, ObserverDid: observer, CommittedHeight: height, HeadBlockHash: hash}, nil
}

func (s *Server) verifyCatchupProof(threadID, observerDID string, raw []byte) error {
	var proof pb.ThreadCatchupProof
	if err := proto.Unmarshal(raw, &proof); err != nil {
		return fmt.Errorf("unmarshal catchup proof: %w", err)
	}
	if proof.ThreadId != threadID || proof.ObserverDid != observerDID || proof.IssuedAtUnixMs <= 0 || len(proof.Signature) == 0 {
		return fmt.Errorf("invalid catchup proof")
	}
	if age := time.Since(time.UnixMilli(proof.IssuedAtUnixMs)); age < -time.Minute || age > time.Minute {
		return fmt.Errorf("catchup proof is expired")
	}
	signature := proof.Signature
	proof.Signature = nil
	data, err := proto.MarshalOptions{Deterministic: true}.Marshal(&proof)
	if err != nil {
		return err
	}
	ok, err := identity.VerifyDID(observerDID, data, signature)
	if err != nil || !ok {
		return fmt.Errorf("invalid catchup proof signature")
	}
	head, ok := s.threads.(threadCommittedHeadReader)
	if !ok {
		return fmt.Errorf("thread manager cannot verify committed head")
	}
	height, hash, err := head.CommittedHead(threadID)
	if err != nil {
		return err
	}
	if proof.CommittedHeight != height || proof.HeadBlockHash != hash {
		return fmt.Errorf("observer catchup proof does not match current committed head")
	}
	return nil
}

func (s *Server) LeaveThread(ctx context.Context, req *pb.ThreadID) (*pb.ThreadMembershipChange, error) {
	if req == nil || req.Id == "" {
		return nil, fmt.Errorf("thread id required")
	}
	owner, err := s.agentDID(ctx)
	if err != nil {
		return nil, err
	}
	r, ok := s.threads.(threadMemberRemover)
	if !ok {
		return nil, fmt.Errorf("thread manager cannot remove members")
	}
	var epoch uint64
	if epochRemover, ok := s.threads.(threadMemberRemoverWithEpoch); ok {
		epoch, err = epochRemover.RemoveMemberWithEpoch(req.Id, owner)
	} else {
		err = r.RemoveMember(req.Id, owner)
	}
	if err != nil {
		return nil, err
	}
	return &pb.ThreadMembershipChange{ThreadId: req.Id, MembershipEpoch: epoch, Member: &pb.ThreadMember{Did: owner}}, nil
}

func (s *Server) ListThreadMembers(_ context.Context, req *pb.ThreadID) (*pb.ThreadMembers, error) {
	if req == nil || req.Id == "" {
		return nil, fmt.Errorf("thread id is required")
	}
	lister, ok := s.threads.(threadMemberLister)
	if !ok {
		return nil, fmt.Errorf("thread manager cannot list members")
	}
	members, err := lister.ListMembers(req.Id)
	if err != nil {
		return nil, err
	}
	return &pb.ThreadMembers{Members: members}, nil
}

func (s *Server) CreateThreadWithRecovery(ctx context.Context, req *pb.CreateThreadRequest) (*pb.CreateThreadResponse, error) {
	owner, err := s.scopedOwner(ctx)
	if err != nil {
		return nil, err
	}
	if owner != "" && (req == nil || req.CreatorDid != owner) {
		return nil, fmt.Errorf("signed thread creator must match authenticated agent")
	}
	creator, ok := s.threads.(threadRecoveryCreator)
	if !ok {
		return nil, fmt.Errorf("thread manager cannot create recovery capabilities")
	}
	th, handle, err := creator.CreateThreadWithRecovery(ctx, req)
	if err != nil {
		return nil, err
	}
	if err := s.enqueueThreadWake(th); err != nil {
		s.log.Warn("enqueue initial thread wake", zap.Error(err))
	}
	return &pb.CreateThreadResponse{Thread: th, RecoveryHandle: handle}, nil
}

// RecoverThreadWithHandle requires the complete bearer capability before
// importing history. The imported thread is read-only: capability possession
// never adds a Raft/member role.
func (s *Server) RecoverThreadWithHandle(ctx context.Context, req *pb.RecoverThreadRequest) (*pb.RecoverThreadResponse, error) {
	if req == nil || req.Handle == nil || req.Handle.ThreadId == "" || req.Handle.Version != 1 || len(req.Handle.RecoverySecret) != 32 {
		return nil, fmt.Errorf("valid recovery handle required")
	}
	allowed, err := s.authorizeRecoveryHandle(ctx, req.Handle)
	if err != nil {
		return nil, err
	}
	if !allowed {
		return nil, fmt.Errorf("recovery capability is invalid")
	}
	th, err := s.recoverThreadHistory(ctx, req.Handle.ThreadId)
	if err != nil {
		return nil, err
	}
	return &pb.RecoverThreadResponse{Thread: th, Access: pb.ThreadAccess_THREAD_ACCESS_READ_ONLY}, nil
}

// authorizeRecoveryHandle first uses a locally retained capability hash. A
// newly provisioned recovery daemon has no local database yet, however, so it
// can also validate the secret against the creator-signed descriptor's public
// SHA-256 commitment before importing the chain. This makes recovery portable
// without turning a public thread ID into a read capability.
func (s *Server) authorizeRecoveryHandle(ctx context.Context, handle *pb.ThreadRecoveryHandle) (bool, error) {
	authorizer, ok := s.threads.(threadRecoveryAuthorizer)
	if !ok {
		return false, fmt.Errorf("thread manager cannot authorize recovery capabilities")
	}
	allowed, err := authorizer.AuthorizeRecoveryCapability(handle.ThreadId, handle.RecoverySecret)
	if err != nil || allowed {
		return allowed, err
	}
	if s.node == nil || s.node.DHT == nil {
		return false, nil
	}
	head, err := thread.ResolveHead(ctx, s.node.DHT, handle.ThreadId)
	if err != nil {
		return false, nil // absence is authorization failure, not a DHT oracle
	}
	if head.Thread == nil || head.Thread.Id != handle.ThreadId || thread.VerifyDescriptor(head.Thread) != nil {
		return false, nil
	}
	return threadcrypto.MatchesRecoveryCommitment(handle.RecoverySecret,
		head.Thread.Metadata[threadcrypto.RecoveryCommitmentMetadataKey]), nil
}

// RecoverThread is retained in the wire protocol solely to provide a clear
// migration error. A thread ID is public discovery metadata, not a read
// capability; recovery requires RecoverThreadWithHandle.
func (s *Server) RecoverThread(ctx context.Context, req *pb.ThreadID) (*pb.Thread, error) {
	_ = ctx
	_ = req
	return nil, fmt.Errorf("bare-ID recovery is retired; use RecoverThreadWithHandle")
}

// recoverThreadHistory resolves and cryptographically verifies
// content-addressed history before installing it locally for read access. Its
// caller authorizes recovery first; it never grants a consensus role.
func (s *Server) recoverThreadHistory(ctx context.Context, threadID string) (*pb.Thread, error) {
	if threadID == "" {
		return nil, fmt.Errorf("thread id is required")
	}
	if s.node == nil || s.node.DHT == nil || s.node.Bitswap == nil {
		return nil, fmt.Errorf("thread recovery requires DHT and Bitswap")
	}
	// Prime Bitswap with several archive candidates. Discovery is advisory: a
	// stale or malicious provider cannot affect recovery because the descriptor
	// and every fetched block are verified below.
	providerCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	providers, discoveryErr := thread.DiscoverArchiveProviders(providerCtx, s.node.DHT, threadID, 3)
	if discoveryErr == nil && s.node.Host != nil {
		for _, provider := range providers {
			if provider.ID == s.node.Host.ID() {
				continue
			}
			if err := s.node.Host.Connect(providerCtx, provider); err != nil {
				s.log.Debug("archive provider unavailable", zap.String("thread", threadID), zap.String("provider", provider.ID.String()), zap.Error(err))
			}
		}
	}
	cancel()
	head, err := thread.ResolveHead(ctx, s.node.DHT, threadID)
	if err != nil {
		return nil, err
	}
	if head.Thread == nil || head.Thread.Id != threadID {
		return nil, fmt.Errorf("published thread descriptor is unavailable")
	}
	if err := thread.VerifyDescriptor(head.Thread); err != nil {
		return nil, fmt.Errorf("verify thread descriptor: %w", err)
	}
	kind := thread.BackendKind(head.Thread.Metadata["backend"])
	if kind == "" {
		kind = thread.BackendRaft
	}
	blocks, err := thread.VerifyChainFromHead(ctx, kind, threadID,
		func(c context.Context, id string) (*thread.ThreadHead, error) {
			return thread.ResolveHead(c, s.node.DHT, id)
		},
		func(c context.Context, id string, height int64) (*thread.ThreadHead, error) {
			return thread.ResolveHeight(c, s.node.DHT, id, height)
		},
		func(c context.Context, cid string) (*pb.ThreadBlock, error) {
			return thread.FetchBlock(c, s.node.Bitswap, cid)
		},
	)
	if err != nil {
		return nil, err
	}
	importer, ok := s.threads.(threadHistoryImporter)
	if !ok {
		return nil, fmt.Errorf("thread manager cannot import history")
	}
	if err := importer.ImportHistory(head.Thread, blocks); err != nil {
		return nil, err
	}
	return head.Thread, nil
}

func (s *Server) AppendEntry(ctx context.Context, req *pb.AppendEntryRequest) (*pb.AppendEntryResult, error) {
	if s.threads == nil {
		return nil, fmt.Errorf("thread manager not available")
	}
	owner, err := s.scopedOwner(ctx)
	if err != nil {
		return nil, err
	}
	author := s.id.DID
	if owner != "" {
		author = owner
	}
	entry := &pb.ThreadEntry{AuthorDid: author, Payload: req.Payload, Kind: req.Kind, SubmittedAt: time.Now().UnixMilli()}
	if req.EncryptedEntry != nil {
		entry = req.EncryptedEntry
		if entry.AuthorDid != author || entry.EncodingVersion != 2 || len(entry.Nonce) != 24 || len(entry.AuthorSignature) == 0 {
			return nil, fmt.Errorf("invalid encrypted thread entry")
		}
		members, ok := s.threads.(threadMemberLister)
		if !ok {
			return nil, fmt.Errorf("thread manager cannot authorize encrypted entry membership")
		}
		memberList, err := members.ListMembers(req.ThreadId)
		if err != nil {
			return nil, err
		}
		isMember := false
		for _, member := range memberList {
			if member.Did == author {
				isMember = true
				break
			}
		}
		if !isMember {
			return nil, fmt.Errorf("encrypted entry author is not a current thread member")
		}
		epochs, ok := s.threads.(threadMembershipEpochReader)
		if !ok {
			return nil, fmt.Errorf("thread manager cannot validate encrypted entry epoch")
		}
		currentEpoch, err := epochs.MembershipEpoch(req.ThreadId)
		if err != nil {
			return nil, err
		}
		if entry.MembershipEpoch != currentEpoch || entry.EncryptionEpoch != currentEpoch {
			return nil, fmt.Errorf("encrypted entry must use current membership and encryption epoch %d", currentEpoch)
		}
		public, err := identity.PubKeyFromDID(entry.AuthorDid)
		if err != nil {
			return nil, err
		}
		ok, err = threadcrypto.Verify(public, threadcrypto.Header{ThreadID: req.ThreadId, Sequence: entry.Sequence, PreviousBlockHash: entry.PreviousBlockHash, AuthorDID: entry.AuthorDid, Kind: entry.Kind, MembershipEpoch: entry.MembershipEpoch, EncryptionEpoch: entry.EncryptionEpoch, Nonce: entry.Nonce}, entry.Payload, entry.AuthorSignature)
		if err != nil || !ok {
			return nil, fmt.Errorf("invalid encrypted thread entry signature")
		}
		entry.SubmittedAt = time.Now().UnixMilli()
	}
	if err := s.threads.AppendEntry(req.ThreadId, entry); err != nil {
		return nil, err
	}
	// THREAD_INVITE is idempotent and carries the complete thread descriptor,
	// so it doubles as a durable wake message for passivated replicas. Sending
	// it through the outbox avoids one always-live GossipSub subscription per
	// dormant thread.
	thread, err := s.threads.GetThread(req.ThreadId)
	if err != nil {
		return nil, err
	}
	if err := s.enqueueThreadWake(thread); err != nil {
		return nil, fmt.Errorf("entry is durable locally but replica wake enqueue failed: %w", err)
	}
	// Best-effort: return the current committed height + 1 as expected height.
	return &pb.AppendEntryResult{
		ThreadId: req.ThreadId,
		EntryId:  req.ThreadId + ":" + fmt.Sprintf("%d", entry.SubmittedAt),
	}, nil
}

func (s *Server) enqueueThreadWake(thread *pb.Thread) error {
	if s.outbox == nil {
		return fmt.Errorf("outbox not available")
	}
	payload, err := proto.Marshal(thread)
	if err != nil {
		return err
	}
	var errs []error
	for _, did := range thread.ReplicaDids {
		if did == s.id.DID {
			continue
		}
		msg := &pb.Message{
			Id:       uuid.New().String(),
			FromDid:  s.id.DID,
			ToDid:    did,
			ThreadId: thread.Id,
			Kind:     pb.MessageKind_MESSAGE_KIND_THREAD_INVITE,
			Payload:  payload,
			SentAt:   time.Now().UnixMilli(),
		}
		if err := s.outbox.Enqueue(msg); err != nil {
			errs = append(errs, fmt.Errorf("wake %s: %w", did, err))
			continue
		}
		appactors.Metrics.WakeQueued()
	}
	return errors.Join(errs...)
}

// PutThreadKeyEnvelope lets a thread creator publish an opaque epoch key for
// an existing member. The server only validates routing/authorization; it
// cannot decrypt the envelope.
func (s *Server) PutThreadKeyEnvelope(ctx context.Context, envelope *pb.ThreadKeyEnvelope) (*pb.Empty, error) {
	if envelope == nil || envelope.ThreadId == "" || (!envelope.RecoveryEnvelope && envelope.RecipientDid == "") {
		return nil, fmt.Errorf("valid thread key envelope required")
	}
	th, err := s.threads.GetThread(envelope.ThreadId)
	if err != nil {
		return nil, err
	}
	owner, err := s.scopedOwner(ctx)
	if err != nil {
		return nil, err
	}
	if owner != "" && owner != th.CreatorDid {
		return nil, fmt.Errorf("only thread creator may distribute epoch keys")
	}
	epochs, ok := s.threads.(threadMembershipEpochReader)
	if !ok {
		return nil, fmt.Errorf("thread manager cannot validate key-envelope epoch")
	}
	currentEpoch, err := epochs.MembershipEpoch(envelope.ThreadId)
	if err != nil {
		return nil, err
	}
	if envelope.EncryptionEpoch != currentEpoch {
		return nil, fmt.Errorf("key envelope must use current encryption epoch %d", currentEpoch)
	}
	if envelope.RecoveryEnvelope {
		if err := thread.VerifyRecoveryKeyEnvelopeCreator(th.CreatorDid, envelope); err != nil {
			return nil, err
		}
		store, ok := s.threads.(threadRecoveryKeyEnvelopeStore)
		if !ok {
			return nil, fmt.Errorf("thread manager cannot store recovery key envelopes")
		}
		if err := store.SaveRecoveryKeyEnvelope(envelope); err != nil {
			return nil, err
		}
		if s.node != nil && s.node.DHT != nil && s.node.Blockstore != nil {
			if err := thread.PublishRecoveryEnvelope(ctx, s.node.DHT, s.node.Blockstore, s.node.Bitswap, envelope); err != nil {
				return nil, fmt.Errorf("publish replayable recovery envelope: %w", err)
			}
		}
		// Archive workers on independent providers retain this opaque envelope.
		// It remains useless without the 256-bit recovery secret and is served
		// only after the same capability validation as history recovery.
		if s.gossip != nil {
			raw, err := proto.MarshalOptions{Deterministic: true}.Marshal(envelope)
			if err != nil {
				return nil, err
			}
			if err := s.gossip.Publish(ctx, thread.RecoveryEnvelopeTopic(envelope.ThreadId), raw); err != nil {
				return nil, fmt.Errorf("replicate recovery key envelope: %w", err)
			}
		}
		return &pb.Empty{}, nil
	}
	lister, ok := s.threads.(threadMemberLister)
	if !ok {
		return nil, fmt.Errorf("thread manager cannot list members")
	}
	members, err := lister.ListMembers(envelope.ThreadId)
	if err != nil {
		return nil, err
	}
	found := false
	for _, member := range members {
		if member.Did == envelope.RecipientDid {
			found = true
			break
		}
	}
	if !found {
		return nil, fmt.Errorf("key-envelope recipient is not a thread member")
	}
	store, ok := s.threads.(threadKeyEnvelopeStore)
	if !ok {
		return nil, fmt.Errorf("thread manager cannot store key envelopes")
	}
	if err := store.SaveKeyEnvelope(envelope); err != nil {
		return nil, err
	}
	return &pb.Empty{}, nil
}

// GetRecoveryThreadKeyEnvelopes returns opaque recovery envelopes only after
// proving possession of the same complete capability required to import
// history. Normal member sessions cannot use this endpoint.
func (s *Server) GetRecoveryThreadKeyEnvelopes(ctx context.Context, req *pb.RecoverThreadRequest) (*pb.ThreadKeyEnvelopes, error) {
	if req == nil || req.Handle == nil || req.Handle.ThreadId == "" || req.Handle.Version != 1 || len(req.Handle.RecoverySecret) != 32 {
		return nil, fmt.Errorf("valid recovery handle required")
	}
	allowed, err := s.authorizeRecoveryHandle(ctx, req.Handle)
	if err != nil {
		return nil, err
	}
	if !allowed {
		return nil, fmt.Errorf("recovery capability is invalid")
	}
	store, ok := s.threads.(threadRecoveryKeyEnvelopeStore)
	if !ok {
		return nil, fmt.Errorf("thread manager cannot retrieve recovery key envelopes")
	}
	envelopes, err := store.RecoveryKeyEnvelopes(req.Handle.ThreadId)
	if err != nil {
		return nil, err
	}
	// Replay manifests cover an archive worker that was offline when the live
	// pubsub envelope arrived. A bad DHT pointer is harmless: the fetched
	// envelope must still match the thread, epoch, and creator signature.
	if s.node != nil && s.node.DHT != nil && s.node.Bitswap != nil {
		seen := make(map[uint64]bool, len(envelopes))
		for _, envelope := range envelopes {
			seen[envelope.EncryptionEpoch] = true
		}
		for epoch := uint64(1); epoch <= 64; epoch++ {
			if seen[epoch] {
				continue
			}
			head, err := thread.ResolveRecoveryEnvelope(ctx, s.node.DHT, req.Handle.ThreadId, epoch)
			if err != nil {
				continue
			}
			envelope, err := thread.FetchRecoveryEnvelope(ctx, s.node.Bitswap, head.EnvelopeCID)
			if err != nil || envelope.ThreadId != req.Handle.ThreadId || envelope.EncryptionEpoch != epoch || thread.VerifyRecoveryKeyEnvelopeCreator(thCreatorDID(s.threads, req.Handle.ThreadId), envelope) != nil {
				continue
			}
			if err := store.SaveRecoveryKeyEnvelope(envelope); err == nil {
				envelopes = append(envelopes, envelope)
			}
		}
	}
	return &pb.ThreadKeyEnvelopes{Envelopes: envelopes}, nil
}

func thCreatorDID(manager ThreadManager, threadID string) string {
	th, err := manager.GetThread(threadID)
	if err != nil {
		return ""
	}
	return th.CreatorDid
}

// GetThreadKeyEnvelopes returns only envelopes addressed to the authenticated
// SDK identity, preventing one member from reading another's wrapped keys.
func (s *Server) GetThreadKeyEnvelopes(ctx context.Context, req *pb.ThreadKeyEnvelopeQuery) (*pb.ThreadKeyEnvelopes, error) {
	if req == nil || req.ThreadId == "" || req.EncryptionEpoch == 0 {
		return nil, fmt.Errorf("thread_id and encryption_epoch are required")
	}
	recipient, err := s.agentDID(ctx)
	if err != nil {
		return nil, err
	}
	store, ok := s.threads.(threadKeyEnvelopeStore)
	if !ok {
		return nil, fmt.Errorf("thread manager cannot retrieve key envelopes")
	}
	envelopes, err := store.KeyEnvelopes(req.ThreadId, req.EncryptionEpoch, recipient)
	if err != nil {
		return nil, err
	}
	return &pb.ThreadKeyEnvelopes{Envelopes: envelopes}, nil
}

func (s *Server) GetThreadEntries(req *pb.GetThreadEntriesRequest, stream pb.A2ANode_GetThreadEntriesServer) error {
	if s.threads == nil {
		return fmt.Errorf("thread manager not available")
	}
	entries, err := s.threads.GetEntries(req.ThreadId, req.SinceHeight, int(req.Limit))
	if err != nil {
		return err
	}
	for _, ep := range entries {
		if err := stream.Send(ep); err != nil {
			return err
		}
	}
	return nil
}

func (s *Server) SubscribeThread(req *pb.SubscribeThreadRequest, stream pb.A2ANode_SubscribeThreadServer) error {
	if s.threads == nil {
		return fmt.Errorf("thread manager not available")
	}

	// Flush historical entries first.
	entries, err := s.threads.GetEntries(req.ThreadId, req.SinceHeight, 0)
	if err != nil {
		return err
	}
	for _, ep := range entries {
		if err := stream.Send(ep); err != nil {
			return err
		}
	}

	// Subscribe to live commits.
	eng := s.threads.Engine(req.ThreadId)
	if eng == nil {
		// Thread exists but this node is not a validator — just block until ctx done.
		<-stream.Context().Done()
		return nil
	}

	ch := eng.Subscribe()
	defer eng.Unsubscribe(ch)

	for {
		select {
		case <-stream.Context().Done():
			return nil
		case ep, ok := <-ch:
			if !ok {
				return nil
			}
			if err := stream.Send(ep); err != nil {
				return err
			}
		}
	}
}

// FetchFile fetches a block by CIDv1, preferring the local blockstore, and
// streams it back in chunks. If from_did is provided and the peer is already
// connected, Bitswap will prefer fetching from that peer directly (fast
// path). Otherwise it uses content routing.
func (s *Server) FetchFile(req *pb.FetchFileRequest, stream pb.A2ANode_FetchFileServer) error {
	if req.Cid == "" {
		return fmt.Errorf("cid is required")
	}
	if s.node == nil {
		return fmt.Errorf("file storage not available (node not initialised)")
	}

	c, err := cid.Decode(req.Cid)
	if err != nil {
		return fmt.Errorf("invalid CID %q: %w", req.Cid, err)
	}

	// Bitswap.GetBlock is a pure network-fetch protocol call — it does not
	// check the local blockstore first (confirmed against boxo's
	// bitswap/client.Client.GetBlock, which always creates a fetch session).
	// Skipping this check meant every FetchFile for a block this node
	// already had locally (e.g. immediately after its own SendFile) still
	// ran the full peer-discovery/fetch protocol and hung waiting for a
	// remote provider that was never going to appear.
	var blk blocks.Block
	if local, err := s.node.Blockstore.Get(stream.Context(), c); err == nil {
		blk = local
	} else {
		// If caller knows which peer has it, connect first so Bitswap finds it immediately.
		if req.FromDid != "" && s.registry != nil {
			if card, err := s.registry.Resolve(stream.Context(), req.FromDid); err == nil {
				if ai, err := p2putil.AddrsToAddrInfo(card.Multiaddrs); err == nil {
					s.node.Host.Connect(stream.Context(), *ai) //nolint:errcheck — best effort
				}
			}
		}

		blk, err = s.node.Bitswap.GetBlock(stream.Context(), c)
		if err != nil {
			return fmt.Errorf("fetch block %s: %w", req.Cid, err)
		}
	}
	data := blk.RawData()

	// Stream back in 32 KB chunks.
	total := int64(len(data))
	for offset := int64(0); offset < total; offset += fileChunkSize {
		end := offset + fileChunkSize
		if end > total {
			end = total
		}
		if err := stream.Send(&pb.FileChunk{
			Data:   data[offset:end],
			Offset: offset,
			Total:  total,
		}); err != nil {
			return err
		}
	}
	return nil
}
