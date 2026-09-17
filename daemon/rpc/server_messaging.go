package rpc

import (
	"context"
	"fmt"

	"github.com/google/uuid"
	"github.com/sahilpohare/p2p-a2a/daemon/webhook"
	pb "github.com/sahilpohare/p2p-a2a/gen/a2a/v1"
)

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
