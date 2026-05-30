package deliver

// /a2a/blob/1.0.0 — content-addressed blob fetch protocol.
//
// Request wire format (sent by fetcher):
//   msgio-framed UTF-8 CID string (e.g. "sha256:abc123...")
//
// Response wire format (sent by server):
//   - If found:     msgio-framed raw bytes
//   - If not found: msgio-framed single byte 0x00

import (
	"context"
	"fmt"
	"time"

	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-msgio"

	"github.com/sahilpohare/p2p-a2a/daemon/blob"
	"go.uber.org/zap"
)

const BlobProtocol = "/a2a/blob/1.0.0"

// RegisterBlobHandler registers the /a2a/blob/1.0.0 serve handler on the
// Deliverer's host, serving blobs from the given store.
func (d *Deliverer) RegisterBlobHandler(bs *blob.Store) {
	d.host.SetStreamHandler(BlobProtocol, func(s network.Stream) {
		defer s.Close()
		s.SetDeadline(time.Now().Add(streamTimeout)) //nolint:errcheck

		r := msgio.NewReaderSize(s, 256) // CID is short
		cidBytes, err := r.ReadMsg()
		if err != nil {
			d.log.Warn("blob: read cid request", zap.Error(err))
			return
		}
		cid := string(cidBytes)

		data, err := bs.Get(cid)
		w := msgio.NewWriter(s)
		if err != nil {
			d.log.Warn("blob: cid not found locally", zap.String("cid", cid))
			w.WriteMsg([]byte{0x00}) //nolint:errcheck
			return
		}

		if err := w.WriteMsg(data); err != nil {
			d.log.Warn("blob: write response", zap.String("cid", cid), zap.Error(err))
			return
		}
		d.log.Info("blob served", zap.String("cid", cid), zap.Int("bytes", len(data)))
	})
}

// FetchBlob fetches a blob by CID from the given peer and returns its bytes.
// The caller should verify the returned bytes against the CID.
func (d *Deliverer) FetchBlob(ctx context.Context, peerID peer.ID, cid string) ([]byte, error) {
	streamCtx, cancel := context.WithTimeout(ctx, streamTimeout)
	defer cancel()

	s, err := d.host.NewStream(streamCtx, peerID, BlobProtocol)
	if err != nil {
		return nil, fmt.Errorf("open blob stream to %q: %w", peerID, err)
	}
	defer s.Close()
	s.SetDeadline(time.Now().Add(streamTimeout)) //nolint:errcheck

	// Send CID request
	w := msgio.NewWriter(s)
	if err := w.WriteMsg([]byte(cid)); err != nil {
		return nil, fmt.Errorf("send cid request: %w", err)
	}

	// Read response
	r := msgio.NewReaderSize(s, maxMsgSize)
	data, err := r.ReadMsg()
	if err != nil {
		return nil, fmt.Errorf("read blob response: %w", err)
	}
	if len(data) == 1 && data[0] == 0x00 {
		return nil, fmt.Errorf("peer does not have blob %s", cid)
	}

	d.log.Info("blob fetched", zap.String("cid", cid), zap.Int("bytes", len(data)))
	return data, nil
}
