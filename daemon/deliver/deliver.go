// Package deliver implements remote message delivery over libp2p streams.
//
// Protocol: /a2a/msg/1.0.0
// Wire format: length-prefixed protobuf Message (msgio framing)
//
// Sender side (makeDeliverFunc):
//  1. Resolve recipient DID → AgentCard via DHT
//  2. Connect to peer multiaddrs
//  3. Open /a2a/msg/1.0.0 stream
//  4. Write msgio-framed serialized pb.Message
//  5. Read ack byte (0x01 = ok, 0x00 = rejected)
//
// Receiver side (Handler):
//  1. Registered on the host for /a2a/msg/1.0.0
//  2. Read msgio-framed bytes
//  3. Unmarshal pb.Message, call inbox.Put
//  4. Write ack byte
package deliver

import (
	"context"
	"fmt"
	"time"

	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-msgio"
	"github.com/multiformats/go-multiaddr"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"

	pb "github.com/sahilpohare/p2p-a2a/gen/a2a/v1"
	"github.com/sahilpohare/p2p-a2a/daemon/inbox"
	"github.com/sahilpohare/p2p-a2a/daemon/registry"
)

const (
	Protocol      = "/a2a/msg/1.0.0"
	streamTimeout = 15 * time.Second
	maxMsgSize    = 4 << 20 // 4 MB
)

// ackOK is written back to the sender after successful inbox.Put.
const ackOK = byte(0x01)

// Deliverer sends messages to remote peers.
type Deliverer struct {
	host     host.Host
	registry *registry.Registry
	log      *zap.Logger
}

// New creates a Deliverer and registers the receive handler on the host.
func New(h host.Host, reg *registry.Registry, ib *inbox.Inbox, log *zap.Logger) *Deliverer {
	d := &Deliverer{host: h, registry: reg, log: log}
	h.SetStreamHandler(Protocol, d.receiveHandler(ib))
	return d
}

// DeliverFunc returns an outbox.DeliverFunc that sends messages via libp2p streams.
func (d *Deliverer) DeliverFunc() func(ctx context.Context, msg *pb.Message) error {
	return func(ctx context.Context, msg *pb.Message) error {
		return d.Send(ctx, msg)
	}
}

// Host returns the underlying libp2p host (useful for tests and e2e harness).
func (d *Deliverer) Host() host.Host { return d.host }

// SendDirect delivers a message to a known peer.ID without a DHT lookup.
// Used for testing and for cases where the caller already knows the peer ID.
func (d *Deliverer) SendDirect(ctx context.Context, peerID peer.ID, msg *pb.Message) error {
	return d.sendToPeer(ctx, peerID, msg)
}

// Send delivers a message to the remote peer identified by msg.ToDid.
func (d *Deliverer) Send(ctx context.Context, msg *pb.Message) error {
	// 1. Resolve AgentCard from DHT
	card, err := d.registry.Resolve(ctx, msg.ToDid)
	if err != nil {
		return fmt.Errorf("resolve %q: %w", msg.ToDid, err)
	}
	if len(card.Multiaddrs) == 0 {
		return fmt.Errorf("agent %q has no multiaddrs in card", msg.ToDid)
	}

	// 2. Parse multiaddrs and connect
	addrInfo, err := addrsToAddrInfo(card.Multiaddrs)
	if err != nil {
		return fmt.Errorf("parse multiaddrs for %q: %w", msg.ToDid, err)
	}
	connectCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	if err := d.host.Connect(connectCtx, *addrInfo); err != nil {
		return fmt.Errorf("connect to %q: %w", msg.ToDid, err)
	}

	return d.sendToPeer(ctx, addrInfo.ID, msg)
}

// sendToPeer opens a stream to peerID and writes the message.
func (d *Deliverer) sendToPeer(ctx context.Context, peerID peer.ID, msg *pb.Message) error {
	streamCtx, cancel := context.WithTimeout(ctx, streamTimeout)
	defer cancel()
	s, err := d.host.NewStream(streamCtx, peerID, Protocol)
	if err != nil {
		return fmt.Errorf("open stream to %q: %w", peerID, err)
	}
	defer s.Close()
	s.SetDeadline(time.Now().Add(streamTimeout)) //nolint:errcheck

	data, err := proto.Marshal(msg)
	if err != nil {
		return fmt.Errorf("marshal message: %w", err)
	}
	w := msgio.NewWriter(s)
	if err := w.WriteMsg(data); err != nil {
		return fmt.Errorf("write message: %w", err)
	}

	ack := make([]byte, 1)
	if _, err := s.Read(ack); err != nil {
		return fmt.Errorf("read ack: %w", err)
	}
	if ack[0] != ackOK {
		return fmt.Errorf("remote %q rejected message", peerID)
	}

	d.log.Info("message delivered",
		zap.String("to", msg.ToDid),
		zap.String("peer", peerID.String()),
		zap.String("msg_id", msg.Id),
	)
	return nil
}

// ─── receive handler ──────────────────────────────────────────────────────────

func (d *Deliverer) receiveHandler(ib *inbox.Inbox) network.StreamHandler {
	return func(s network.Stream) {
		defer s.Close()
		s.SetDeadline(time.Now().Add(streamTimeout)) //nolint:errcheck

		r := msgio.NewReaderSize(s, maxMsgSize)
		data, err := r.ReadMsg()
		if err != nil {
			d.log.Warn("read incoming message", zap.Error(err))
			s.Write([]byte{0x00}) //nolint:errcheck
			return
		}

		var msg pb.Message
		if err := proto.Unmarshal(data, &msg); err != nil {
			d.log.Warn("unmarshal incoming message", zap.Error(err))
			s.Write([]byte{0x00}) //nolint:errcheck
			return
		}

		if err := ib.Put(&msg); err != nil {
			d.log.Warn("inbox put", zap.String("msg_id", msg.Id), zap.Error(err))
			s.Write([]byte{0x00}) //nolint:errcheck
			return
		}

		d.log.Info("message received",
			zap.String("from", msg.FromDid),
			zap.String("msg_id", msg.Id),
		)
		s.Write([]byte{ackOK}) //nolint:errcheck
	}
}

// ─── helpers ──────────────────────────────────────────────────────────────────

// addrsToAddrInfo parses a list of multiaddr strings and returns a peer.AddrInfo.
// All addrs must embed the same peer ID (p2p component).
func addrsToAddrInfo(addrs []string) (*peer.AddrInfo, error) {
	var maddrs []multiaddr.Multiaddr
	for _, a := range addrs {
		ma, err := multiaddr.NewMultiaddr(a)
		if err != nil {
			continue // skip malformed
		}
		maddrs = append(maddrs, ma)
	}
	if len(maddrs) == 0 {
		return nil, fmt.Errorf("no valid multiaddrs")
	}
	// peer.AddrInfosFromP2pAddrs deduplicates peer ID and collects all addrs
	infos, err := peer.AddrInfosFromP2pAddrs(maddrs...)
	if err != nil {
		return nil, err
	}
	if len(infos) == 0 {
		return nil, fmt.Errorf("could not extract peer info")
	}
	return &infos[0], nil
}
