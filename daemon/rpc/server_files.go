package rpc

import (
	"context"
	"fmt"

	"go.uber.org/zap"

	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	pb "github.com/sahilpohare/p2p-a2a/gen/a2a/v1"
	"github.com/sahilpohare/p2p-a2a/pkg/p2putil"
)

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
