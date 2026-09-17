package rpc

import (
	"bytes"
	"context"
	"fmt"

	"go.uber.org/zap"

	pb "github.com/sahilpohare/p2p-a2a/gen/a2a/v1"
)

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
