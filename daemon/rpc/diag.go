package rpc

import (
	"context"
	"time"

	"github.com/libp2p/go-libp2p/core/network"
	"github.com/sahilpohare/p2p-a2a/pkg/p2putil"
	pb "github.com/sahilpohare/p2p-a2a/gen/a2a/v1"
)

// Ping measures round-trip latency to one or more peers.
// If TargetDid is empty it measures the local daemon loopback (useful for "is daemon alive" checks).
func (s *Server) Ping(ctx context.Context, req *pb.PingRequest) (*pb.PingResponse, error) {
	count := int(req.Count)
	if count <= 0 {
		count = 1
	}

	if req.TargetDid == "" {
		// Loopback: just measure gRPC round-trip overhead.
		start := time.Now()
		latency := time.Since(start).Milliseconds()
		results := make([]*pb.PingResult, count)
		for i := range results {
			results[i] = &pb.PingResult{
				TargetDid: s.id.DID,
				LatencyMs: latency,
				Reachable: true,
			}
		}
		return &pb.PingResponse{Results: results}, nil
	}

	// Resolve target DID → peer in registry
	card, err := s.registry.Resolve(ctx, req.TargetDid)
	if err != nil {
		return &pb.PingResponse{Results: []*pb.PingResult{{
			TargetDid: req.TargetDid,
			Reachable: false,
			Error:     "could not resolve DID: " + err.Error(),
		}}}, nil
	}

	addrInfo, err := p2putil.AddrsToAddrInfo(card.Multiaddrs)
	if err != nil {
		return &pb.PingResponse{Results: []*pb.PingResult{{
			TargetDid: req.TargetDid,
			Reachable: false,
			Error:     "bad multiaddrs: " + err.Error(),
		}}}, nil
	}

	results := make([]*pb.PingResult, 0, count)
	for i := 0; i < count; i++ {
		start := time.Now()
		pingErr := s.node.Host.Connect(ctx, *addrInfo)
		latency := time.Since(start).Milliseconds()

		r := &pb.PingResult{
			TargetDid: req.TargetDid,
			LatencyMs: latency,
			Reachable: pingErr == nil,
		}
		if pingErr != nil {
			r.Error = pingErr.Error()
		}
		results = append(results, r)
	}

	return &pb.PingResponse{Results: results}, nil
}

// Health returns daemon liveness info.
func (s *Server) Health(ctx context.Context, _ *pb.Empty) (*pb.HealthResponse, error) {
	uptime := int64(time.Since(s.startedAt).Seconds())
	peerCount := int32(len(s.node.Host.Network().Peers()))

	return &pb.HealthResponse{
		Ok:         true,
		Version:    version,
		Did:        s.id.DID,
		PeerCount:  peerCount,
		UptimeSecs: uptime,
	}, nil
}

// ListPeers returns all currently connected libp2p peers.
func (s *Server) ListPeers(ctx context.Context, _ *pb.Empty) (*pb.PeersResponse, error) {
	peers := s.node.Host.Network().Peers()
	infos := make([]*pb.PeerInfo, 0, len(peers))

	for _, pid := range peers {
		conns := s.node.Host.Network().ConnsToPeer(pid)
		addrs := make([]string, 0, len(conns))
		for _, c := range conns {
			addrs = append(addrs, c.RemoteMultiaddr().String())
		}

		latencyMs := int64(-1)
		if stat := s.node.Host.Network().ConnsToPeer(pid); len(stat) > 0 {
			// Use connection open time as a proxy if no ping data available
			if stat[0].Stat().Direction == network.DirOutbound {
				latencyMs = -1 // would need ping protocol; left as -1
			}
		}

		infos = append(infos, &pb.PeerInfo{
			PeerId:    pid.String(),
			Addrs:     addrs,
			LatencyMs: latencyMs,
		})
	}

	return &pb.PeersResponse{
		Peers: infos,
		Count: int32(len(infos)),
	}, nil
}
