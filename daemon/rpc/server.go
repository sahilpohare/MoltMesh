package rpc

import (
	"context"
	"fmt"
	"io"
	"time"

	"go.uber.org/zap"

	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/multiformats/go-multiaddr"
	pb "github.com/sahilpohare/p2p-a2a/gen/a2a/v1"
	"github.com/sahilpohare/p2p-a2a/daemon/blob"
	"github.com/sahilpohare/p2p-a2a/daemon/deliver"
	"github.com/sahilpohare/p2p-a2a/daemon/gossip"
	"github.com/sahilpohare/p2p-a2a/daemon/identity"
	"github.com/sahilpohare/p2p-a2a/daemon/inbox"
	"github.com/sahilpohare/p2p-a2a/daemon/network"
	"github.com/sahilpohare/p2p-a2a/daemon/node"
	"github.com/sahilpohare/p2p-a2a/daemon/outbox"
	"github.com/sahilpohare/p2p-a2a/daemon/registry"
	"github.com/sahilpohare/p2p-a2a/daemon/tasks"
	"github.com/sahilpohare/p2p-a2a/daemon/thread"
	"github.com/sahilpohare/p2p-a2a/daemon/webhook"
	"github.com/google/uuid"
)

func peerAddrInfo(addrs []string) (*peer.AddrInfo, error) {
	var maddrs []multiaddr.Multiaddr
	for _, a := range addrs {
		ma, err := multiaddr.NewMultiaddr(a)
		if err != nil {
			continue
		}
		maddrs = append(maddrs, ma)
	}
	if len(maddrs) == 0 {
		return nil, fmt.Errorf("no valid multiaddrs")
	}
	infos, err := peer.AddrInfosFromP2pAddrs(maddrs...)
	if err != nil {
		return nil, err
	}
	if len(infos) == 0 {
		return nil, fmt.Errorf("could not extract peer info")
	}
	return &infos[0], nil
}

// Server implements the A2ANode gRPC service.
type Server struct {
	pb.UnimplementedA2ANodeServer
	pb.UnimplementedDiagServer
	pb.UnimplementedExtServer

	id        *identity.Identity
	inbox     *inbox.Inbox
	outbox    *outbox.Outbox
	tasks     *tasks.Store
	registry  *registry.Registry
	gossip    *gossip.Manager
	blobs     *blob.Store
	dlv       *deliver.Deliverer
	threads   *thread.Manager
	networks  *network.Manager
	webhooks  *webhook.Dispatcher
	node      *node.Node
	addrs     []string
	startedAt time.Time
	log       *zap.Logger
}

// New creates a new gRPC server.
func New(
	id *identity.Identity,
	ib *inbox.Inbox,
	ob *outbox.Outbox,
	ts *tasks.Store,
	reg *registry.Registry,
	gm *gossip.Manager,
	bs *blob.Store,
	dlv *deliver.Deliverer,
	tm *thread.Manager,
	nm *network.Manager,
	wh *webhook.Dispatcher,
	n *node.Node,
	addrs []string,
	log *zap.Logger,
) *Server {
	return &Server{
		id:        id,
		inbox:     ib,
		outbox:    ob,
		tasks:     ts,
		registry:  reg,
		gossip:    gm,
		blobs:     bs,
		dlv:       dlv,
		threads:   tm,
		networks:  nm,
		webhooks:  wh,
		node:      n,
		addrs:     addrs,
		startedAt: time.Now(),
		log:       log,
	}
}

// ─── Identity ────────────────────────────────────────────────────────────────

func (s *Server) GetIdentity(_ context.Context, _ *pb.Empty) (*pb.AgentIdentity, error) {
	return &pb.AgentIdentity{
		Did:       s.id.DID,
		PublicKey: s.id.PublicKeyBase64(),
		Multiaddrs: s.addrs,
	}, nil
}

// ─── Registry ────────────────────────────────────────────────────────────────

func (s *Server) PublishAgentCard(ctx context.Context, card *pb.AgentCard) (*pb.PublishResult, error) {
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
	if msg.Id == "" {
		msg.Id = uuid.New().String()
	}
	msg.FromDid = s.id.DID

	if err := s.outbox.Enqueue(msg); err != nil {
		return nil, fmt.Errorf("enqueue: %w", err)
	}
	return &pb.SendResult{MessageId: msg.Id, Queued: true}, nil
}

func (s *Server) SubscribeInbox(req *pb.SubscribeRequest, stream pb.A2ANode_SubscribeInboxServer) error {
	// Flush existing messages first.
	msgs, err := s.inbox.Get(req.ThreadId, req.TaskId, false, 100, 0)
	if err != nil {
		return err
	}
	for _, m := range msgs {
		if err := stream.Send(m); err != nil {
			return err
		}
	}

	// Stream live arrivals.
	ch := s.inbox.Subscribe()
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
	msgs, err := s.inbox.Get(req.ThreadId, req.TaskId, req.UnreadOnly, int(req.Limit), req.Since)
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
	status := req.Status
	if status == "" {
		status = "pending"
	}
	msgs, err := s.outbox.List(status, int(req.Limit))
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
	return &pb.Empty{}, s.inbox.Ack(req.MessageId)
}

// webhookMessage fires the message webhook. Called by the deliver layer after inbox.Put.
func (s *Server) WebhookMessage(msg *pb.Message) {
	s.webhooks.Send(webhook.EventMessage, msg)
}

// ─── Tasks ───────────────────────────────────────────────────────────────────

func (s *Server) CreateTask(ctx context.Context, req *pb.CreateTaskRequest) (*pb.Task, error) {
	t := req.Task
	task, err := s.tasks.Create(
		s.id.DID, req.ToDid, t.ThreadId, t.Skill, t.InputArtifacts, t.Metadata,
	)
	if err != nil {
		return nil, fmt.Errorf("create task: %w", err)
	}

	// notify assignee via outbox
	msg := &pb.Message{
		Id:      uuid.New().String(),
		FromDid: s.id.DID,
		ToDid:   req.ToDid,
		TaskId:  task.Id,
		Kind:    pb.MessageKind_MESSAGE_KIND_TASK_REQUEST,
	}
	if err := s.outbox.Enqueue(msg); err != nil {
		s.log.Warn("enqueue task request", zap.Error(err))
	}

	return task, nil
}

func (s *Server) GetTask(ctx context.Context, req *pb.TaskID) (*pb.Task, error) {
	return s.tasks.Get(req.Id)
}

func (s *Server) UpdateTask(ctx context.Context, req *pb.TaskStatusUpdate) (*pb.Task, error) {
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
	return s.tasks.UpdateStatus(req.Id, pb.TaskStatus_TASK_STATUS_CANCELLED, "", nil)
}

func (s *Server) PublishTaskEvent(ctx context.Context, event *pb.TaskEvent) (*pb.Empty, error) {
	return &pb.Empty{}, s.gossip.PublishTaskEvent(ctx, event)
}

func (s *Server) SubscribeTaskEvents(req *pb.TaskID, stream pb.A2ANode_SubscribeTaskEventsServer) error {
	ch := make(chan *pb.TaskEvent, 64)

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
			if err := stream.Send(event); err != nil {
				return err
			}
		}
	}
}

// ─── Files ────────────────────────────────────────────────────────────────────

const fileChunkSize = 256 * 1024 // 256 KB per streaming chunk

// SendFile stores a file in the local blob store and returns an Artifact.
// Files ≤ 64 KB are returned with Artifact.Inline populated (no disk write).
// Larger files are written to disk; Artifact.Uri = "blob://<cid>".
func (s *Server) SendFile(_ context.Context, req *pb.SendFileRequest) (*pb.Artifact, error) {
	if len(req.Data) == 0 {
		return nil, fmt.Errorf("file data is empty")
	}
	artifact, err := s.blobs.Put(req.Data, req.Name, req.MimeType)
	if err != nil {
		return nil, fmt.Errorf("store file: %w", err)
	}
	s.log.Info("file stored",
		zap.String("cid", artifact.Cid),
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
	return s.threads.CreateThread(ctx, req)
}

func (s *Server) GetThread(_ context.Context, req *pb.ThreadID) (*pb.Thread, error) {
	if s.threads == nil {
		return nil, fmt.Errorf("thread manager not available")
	}
	return s.threads.GetThread(req.Id)
}

func (s *Server) AppendEntry(_ context.Context, req *pb.AppendEntryRequest) (*pb.AppendEntryResult, error) {
	if s.threads == nil {
		return nil, fmt.Errorf("thread manager not available")
	}
	entry := &pb.ThreadEntry{
		AuthorDid:   s.id.DID,
		Payload:     req.Payload,
		Kind:        req.Kind,
		SubmittedAt: time.Now().UnixMilli(),
	}
	if err := s.threads.AppendEntry(req.ThreadId, entry); err != nil {
		return nil, err
	}
	// Best-effort: return the current committed height + 1 as expected height.
	th, err := s.threads.GetThread(req.ThreadId)
	if err != nil {
		return &pb.AppendEntryResult{ThreadId: req.ThreadId}, nil
	}
	_ = th
	return &pb.AppendEntryResult{
		ThreadId: req.ThreadId,
		EntryId:  req.ThreadId + ":" + fmt.Sprintf("%d", entry.SubmittedAt),
	}, nil
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

// FetchFile fetches a blob by CID from a remote peer and streams it back
// to the caller in 256 KB chunks.
func (s *Server) FetchFile(req *pb.FetchFileRequest, stream pb.A2ANode_FetchFileServer) error {
	if req.Cid == "" {
		return fmt.Errorf("cid is required")
	}

	// Check local store first
	data, err := s.blobs.Get(req.Cid)
	if err != nil && req.FromDid == "" {
		return fmt.Errorf("blob %s not found locally and no from_did specified", req.Cid)
	}

	// Fetch from remote peer if not local
	if err != nil {
		card, resolveErr := s.registry.Resolve(stream.Context(), req.FromDid)
		if resolveErr != nil {
			return fmt.Errorf("resolve %q: %w", req.FromDid, resolveErr)
		}
		addrInfo, parseErr := peerAddrInfo(card.Multiaddrs)
		if parseErr != nil {
			return fmt.Errorf("parse multiaddrs: %w", parseErr)
		}
		data, err = s.dlv.FetchBlob(stream.Context(), addrInfo.ID, req.Cid)
		if err != nil {
			return fmt.Errorf("fetch blob from peer: %w", err)
		}
		// Cache locally
		if saveErr := s.blobs.Save(req.Cid, data); saveErr != nil {
			s.log.Warn("cache fetched blob", zap.Error(saveErr))
		}
	}

	// Stream back in chunks
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
