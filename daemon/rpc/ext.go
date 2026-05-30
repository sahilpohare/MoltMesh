package rpc

import (
	"context"
	"fmt"
	"time"

	"go.uber.org/zap"
	"google.golang.org/grpc"

	pb "github.com/sahilpohare/p2p-a2a/gen/a2a/v1"
	"github.com/sahilpohare/p2p-a2a/daemon/network"
	"github.com/sahilpohare/p2p-a2a/daemon/names"
	"github.com/sahilpohare/p2p-a2a/daemon/webhook"
)

// ── PubSub ────────────────────────────────────────────────────────────────────

func (s *Server) Publish(ctx context.Context, req *pb.PublishRequest) (*pb.PublishResponse, error) {
	if req.Topic == "" {
		return nil, fmt.Errorf("topic is required")
	}
	if err := s.gossip.Publish(ctx, req.Topic, req.Payload); err != nil {
		return nil, err
	}
	return &pb.PublishResponse{Topic: req.Topic}, nil
}

func (s *Server) SubscribeTopic(req *pb.SubscribeTopicRequest, stream grpc.ServerStreamingServer[pb.TopicMessage]) error {
	if req.Topic == "" {
		return fmt.Errorf("topic is required")
	}
	ch, cancel, err := s.gossip.SubscribeTopic(stream.Context(), req.Topic)
	if err != nil {
		return err
	}
	defer cancel()

	for {
		select {
		case <-stream.Context().Done():
			return nil
		case data, ok := <-ch:
			if !ok {
				return nil
			}
			if err := stream.Send(&pb.TopicMessage{
				Topic:     req.Topic,
				Payload:   data,
				EmittedAt: time.Now().UnixMilli(),
			}); err != nil {
				return err
			}
		}
	}
}

// ── Webhook ───────────────────────────────────────────────────────────────────

func (s *Server) SetWebhook(ctx context.Context, req *pb.SetWebhookRequest) (*pb.WebhookResponse, error) {
	if req.Url == "" {
		return nil, fmt.Errorf("url is required")
	}
	s.webhooks.Set(req.Url, req.Secret)
	s.log.Info("webhook configured", zap.String("url", req.Url))
	return &pb.WebhookResponse{Url: req.Url}, nil
}

func (s *Server) ClearWebhook(ctx context.Context, _ *pb.Empty) (*pb.Empty, error) {
	s.webhooks.Clear()
	return &pb.Empty{}, nil
}

func (s *Server) GetWebhook(ctx context.Context, _ *pb.Empty) (*pb.WebhookResponse, error) {
	return &pb.WebhookResponse{Url: s.webhooks.URL()}, nil
}

// ── Networks ──────────────────────────────────────────────────────────────────

func (s *Server) CreateNetwork(ctx context.Context, req *pb.CreateNetworkRequest) (*pb.NetworkInfo, error) {
	if req.Name == "" {
		return nil, fmt.Errorf("name is required")
	}
	net, err := s.networks.Store().Create(req.Name, s.id.DID, nil)
	if err != nil {
		return nil, err
	}
	return networkToProto(net), nil
}

func (s *Server) JoinNetwork(ctx context.Context, req *pb.JoinNetworkRequest) (*pb.NetworkInfo, error) {
	if req.NetworkId == "" {
		return nil, fmt.Errorf("network_id is required")
	}
	if err := s.networks.Store().Join(req.NetworkId, s.id.DID); err != nil {
		return nil, err
	}
	net, err := s.networks.Store().Get(req.NetworkId)
	if err != nil {
		return nil, err
	}
	return networkToProto(net), nil
}

func (s *Server) LeaveNetwork(ctx context.Context, req *pb.NetworkIDRequest) (*pb.Empty, error) {
	if req.NetworkId == "" {
		return nil, fmt.Errorf("network_id is required")
	}
	return &pb.Empty{}, s.networks.Store().Leave(req.NetworkId, s.id.DID)
}

func (s *Server) ListNetworks(ctx context.Context, _ *pb.Empty) (*pb.ListNetworksResponse, error) {
	nets, err := s.networks.Store().List(s.id.DID)
	if err != nil {
		return nil, err
	}
	resp := &pb.ListNetworksResponse{}
	for _, n := range nets {
		resp.Networks = append(resp.Networks, networkToProto(n))
	}
	return resp, nil
}

func (s *Server) NetworkMembers(ctx context.Context, req *pb.NetworkIDRequest) (*pb.NetworkMembersResponse, error) {
	if req.NetworkId == "" {
		return nil, fmt.Errorf("network_id is required")
	}
	members, err := s.networks.Store().Members(req.NetworkId)
	if err != nil {
		return nil, err
	}
	resp := &pb.NetworkMembersResponse{}
	for _, m := range members {
		resp.Members = append(resp.Members, &pb.NetworkMember{
			Did:      m.DID,
			JoinedAt: m.JoinedAt,
		})
	}
	return resp, nil
}

func (s *Server) BroadcastNetwork(ctx context.Context, req *pb.BroadcastRequest) (*pb.Empty, error) {
	if req.NetworkId == "" {
		return nil, fmt.Errorf("network_id is required")
	}
	ok, err := s.networks.Store().IsMember(req.NetworkId, s.id.DID)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, fmt.Errorf("not a member of network %q", req.NetworkId)
	}
	if err := s.networks.Broadcast(ctx, req.NetworkId, req.Payload); err != nil {
		return nil, err
	}
	s.webhooks.Send(webhook.EventPubSub, map[string]interface{}{
		"network_id": req.NetworkId,
		"payload":    req.Payload,
	})
	return &pb.Empty{}, nil
}

func (s *Server) SubscribeNetwork(req *pb.NetworkIDRequest, stream grpc.ServerStreamingServer[pb.BroadcastMessage]) error {
	if req.NetworkId == "" {
		return fmt.Errorf("network_id is required")
	}
	ch, cancel, err := s.networks.SubscribeBroadcast(stream.Context(), req.NetworkId)
	if err != nil {
		return err
	}
	defer cancel()

	for {
		select {
		case <-stream.Context().Done():
			return nil
		case data, ok := <-ch:
			if !ok {
				return nil
			}
			if err := stream.Send(&pb.BroadcastMessage{
				NetworkId: req.NetworkId,
				Payload:   data,
				EmittedAt: time.Now().UnixMilli(),
			}); err != nil {
				return err
			}
		}
	}
}

// ── Names ─────────────────────────────────────────────────────────────────────

func (s *Server) ClaimName(ctx context.Context, req *pb.ClaimNameRequest) (*pb.NameClaimResponse, error) {
	if req.Name == "" {
		return nil, fmt.Errorf("name is required")
	}
	if s.nameReg == nil {
		return nil, fmt.Errorf("name registry not initialised")
	}
	claim, err := s.nameReg.Claim(ctx, req.Name)
	if err != nil {
		return nil, err
	}
	return claimToProto(claim), nil
}

func (s *Server) ResolveName(ctx context.Context, req *pb.ResolveNameRequest) (*pb.NameClaimResponse, error) {
	if req.Name == "" {
		return nil, fmt.Errorf("name is required")
	}
	if s.nameReg == nil {
		return nil, fmt.Errorf("name registry not initialised")
	}
	claim, err := s.nameReg.Resolve(ctx, req.Name)
	if err != nil {
		return nil, err
	}
	return claimToProto(claim), nil
}

// ── helpers ───────────────────────────────────────────────────────────────────

func claimToProto(c *names.Claim) *pb.NameClaimResponse {
	return &pb.NameClaimResponse{
		Name:        c.Name,
		Did:         c.DID,
		PublishedAt: c.PublishedAt,
		ExpiresAt:   c.ExpiresAt,
	}
}

func networkToProto(n *network.Network) *pb.NetworkInfo {
	return &pb.NetworkInfo{
		Id:         n.ID,
		Name:       n.Name,
		CreatorDid: n.CreatorDID,
		CreatedAt:  n.CreatedAt,
	}
}
