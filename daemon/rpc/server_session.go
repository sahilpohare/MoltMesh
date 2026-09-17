package rpc

import (
	"context"
	"fmt"

	pb "github.com/sahilpohare/p2p-a2a/gen/a2a/v1"
)

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
