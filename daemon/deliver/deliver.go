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
	"sync"
	"time"

	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-msgio"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"

	appactors "github.com/sahilpohare/p2p-a2a/daemon/actors"
	"github.com/sahilpohare/p2p-a2a/daemon/identity"
	"github.com/sahilpohare/p2p-a2a/daemon/inbox"
	"github.com/sahilpohare/p2p-a2a/daemon/registry"
	threadpkg "github.com/sahilpohare/p2p-a2a/daemon/thread"
	pb "github.com/sahilpohare/p2p-a2a/gen/a2a/v1"
	"github.com/sahilpohare/p2p-a2a/pkg/p2putil"
)

// marshalBufPool reduces GC pressure by reusing marshal buffers on the
// send hot path. Each buffer starts at 4KB and grows as needed.
var marshalBufPool = sync.Pool{
	New: func() any {
		b := make([]byte, 0, 4096)
		return &b
	},
}

const (
	Protocol      = "/a2a/msg/1.0.0"
	streamTimeout = 15 * time.Second
	maxMsgSize    = 4 << 20 // 4 MB
)

// ackOK is written back to the sender after successful inbox.Put.
const ackOK = byte(0x01)

// Deliverer sends messages to remote peers.
// ThreadInviter is implemented by thread.Manager.
type ThreadInviter interface {
	InviteReceived(thread *pb.Thread) error
}

type Deliverer struct {
	host       host.Host
	registry   *registry.Registry
	threads    ThreadInviter
	log        *zap.Logger
	hierarchy  *appactors.Hierarchy
	peerMu     sync.Mutex
	peerExec   map[peer.ID]*peerExecutor
	supervisor *appactors.Executor
	onMessage  func(*pb.Message) error
}

// SetMessageHandler installs an application-level handler invoked after an
// incoming message is durably stored and before it is acknowledged.
func (d *Deliverer) SetMessageHandler(handler func(*pb.Message) error) { d.onMessage = handler }

type peerExecutor struct {
	exec *appactors.Executor
	refs int
}

// New creates a Deliverer and registers the receive handler on the host.
func New(h host.Host, reg *registry.Registry, ib *inbox.Inbox, tm ThreadInviter, log *zap.Logger) *Deliverer {
	d := &Deliverer{host: h, registry: reg, threads: tm, log: log, peerExec: make(map[peer.ID]*peerExecutor)}
	h.SetStreamHandler(Protocol, d.receiveHandler(ib))
	return d
}

func (d *Deliverer) EnableActor(h *appactors.Hierarchy) error {
	exec, err := appactors.NewExecutor(context.Background(), h, "delivery")
	if err != nil {
		return err
	}
	d.hierarchy, d.supervisor = h, exec
	return nil
}

func (d *Deliverer) executor(ctx context.Context, id peer.ID) (*appactors.Executor, error) {
	d.peerMu.Lock()
	defer d.peerMu.Unlock()
	if entry := d.peerExec[id]; entry != nil && entry.exec.PID().IsRunning() {
		entry.refs++
		return entry.exec, nil
	}
	exec, err := appactors.NewExecutorUnder(ctx, d.hierarchy, d.supervisor.PID(), "peer-"+id.String())
	if err != nil {
		return nil, err
	}
	d.peerExec[id] = &peerExecutor{exec: exec, refs: 1}
	appactors.Metrics.PeerActivated()
	return exec, nil
}

func (d *Deliverer) releaseExecutor(id peer.ID, exec *appactors.Executor) {
	d.peerMu.Lock()
	entry := d.peerExec[id]
	if entry == nil || entry.exec != exec {
		d.peerMu.Unlock()
		return
	}
	entry.refs--
	if entry.refs > 0 {
		d.peerMu.Unlock()
		return
	}
	delete(d.peerExec, id)
	d.peerMu.Unlock()
	_ = exec.Stop(context.Background())
	appactors.Metrics.PeerPassivated()
}

func (d *Deliverer) ActivePeerActors() int {
	d.peerMu.Lock()
	defer d.peerMu.Unlock()
	return len(d.peerExec)
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
	if d.registry == nil {
		return fmt.Errorf("no registry configured: cannot resolve %q", msg.ToDid)
	}
	// 1. Resolve AgentCard from DHT
	card, err := d.registry.Resolve(ctx, msg.ToDid)
	if err != nil {
		return fmt.Errorf("resolve %q: %w", msg.ToDid, err)
	}
	if len(card.Multiaddrs) == 0 {
		return fmt.Errorf("agent %q has no multiaddrs in card", msg.ToDid)
	}

	// 2. Parse multiaddrs and connect
	addrInfo, err := p2putil.AddrsToAddrInfo(card.Multiaddrs)
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
	if d.hierarchy != nil {
		exec, err := d.executor(ctx, peerID)
		if err != nil {
			return err
		}
		defer d.releaseExecutor(peerID, exec)
		_, err = exec.Call(ctx, func() (any, error) { return nil, d.sendToPeerRaw(ctx, peerID, msg) })
		return err
	}
	return d.sendToPeerRaw(ctx, peerID, msg)
}
func (d *Deliverer) sendToPeerRaw(ctx context.Context, peerID peer.ID, msg *pb.Message) error {
	streamCtx, cancel := context.WithTimeout(ctx, streamTimeout)
	defer cancel()
	s, err := d.host.NewStream(streamCtx, peerID, Protocol)
	if err != nil {
		return fmt.Errorf("open stream to %q: %w", peerID, err)
	}
	defer s.Close()
	s.SetDeadline(time.Now().Add(streamTimeout)) //nolint:errcheck

	bufp := marshalBufPool.Get().(*[]byte)
	data, err := proto.MarshalOptions{}.MarshalAppend((*bufp)[:0], msg)
	if err != nil {
		marshalBufPool.Put(bufp)
		return fmt.Errorf("marshal message: %w", err)
	}
	w := msgio.NewWriter(s)
	if err := w.WriteMsg(data); err != nil {
		marshalBufPool.Put(bufp)
		return fmt.Errorf("write message: %w", err)
	}
	*bufp = data[:0]
	marshalBufPool.Put(bufp)

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
	raw := d.receiveHandlerRaw(ib)
	return func(s network.Stream) {
		if d.hierarchy == nil {
			raw(s)
			return
		}
		exec, err := d.executor(context.Background(), s.Conn().RemotePeer())
		if err != nil {
			_ = s.Reset()
			return
		}
		defer d.releaseExecutor(s.Conn().RemotePeer(), exec)
		_, _ = exec.Call(context.Background(), func() (any, error) { raw(s); return nil, nil })
	}
}

func (d *Deliverer) receiveHandlerRaw(ib *inbox.Inbox) network.StreamHandler {
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

		// Verify the sender's DID matches the libp2p peer identity.
		// The stream's remote peer is authenticated by libp2p's secure
		// transport, so we derive the expected DID from the peer's
		// public key and reject mismatches to prevent spoofing.
		remotePeer := s.Conn().RemotePeer()
		remotePubKey, err := remotePeer.ExtractPublicKey()
		if err != nil {
			d.log.Warn("extract remote peer pubkey", zap.Error(err))
			s.Write([]byte{0x00}) //nolint:errcheck
			return
		}
		rawPub, err := remotePubKey.Raw()
		if err != nil {
			d.log.Warn("extract raw pubkey", zap.Error(err))
			s.Write([]byte{0x00}) //nolint:errcheck
			return
		}
		expectedDID := identity.DIDFromPubBytes(rawPub)
		if msg.FromDid != expectedDID {
			d.log.Warn("sender DID mismatch",
				zap.String("claimed", msg.FromDid),
				zap.String("expected", expectedDID),
				zap.String("peer", remotePeer.String()),
			)
			s.Write([]byte{0x00}) //nolint:errcheck
			return
		}
		localPubKey, err := s.Conn().LocalPeer().ExtractPublicKey()
		if err != nil {
			s.Write([]byte{0x00}) //nolint:errcheck
			return
		}
		localRaw, err := localPubKey.Raw()
		if err != nil || msg.ToDid != identity.DIDFromPubBytes(localRaw) {
			d.log.Warn("recipient DID mismatch", zap.String("claimed", msg.ToDid))
			s.Write([]byte{0x00}) //nolint:errcheck
			return
		}

		// Handle thread invites before hitting the inbox.
		if msg.Kind == pb.MessageKind_MESSAGE_KIND_THREAD_INVITE {
			var thread pb.Thread
			if err := proto.Unmarshal(msg.Payload, &thread); err != nil {
				d.log.Warn("unmarshal thread invite", zap.Error(err))
				s.Write([]byte{0x00}) //nolint:errcheck
				return
			}
			if d.threads == nil {
				d.log.Warn("thread invite rejected: thread manager unavailable", zap.String("thread", thread.Id))
				s.Write([]byte{0x00}) //nolint:errcheck
				return
			}
			member := false
			for _, did := range thread.ReplicaDids {
				if did == msg.ToDid {
					member = true
					break
				}
			}
			if threadpkg.VerifyDescriptor(&thread) != nil || !member {
				d.log.Warn("unauthorized thread invite", zap.String("thread", thread.Id), zap.String("from", msg.FromDid))
				s.Write([]byte{0x00}) //nolint:errcheck
				return
			}
			if err := d.threads.InviteReceived(&thread); err != nil {
				d.log.Warn("thread invite", zap.String("thread", thread.Id), zap.Error(err))
				s.Write([]byte{0x00}) //nolint:errcheck
				return
			}
			d.log.Info("thread invite accepted", zap.String("thread", thread.Id))
			s.Write([]byte{ackOK}) //nolint:errcheck
			return
		}

		if err := ib.Put(&msg); err != nil {
			d.log.Warn("inbox put", zap.String("msg_id", msg.Id), zap.Error(err))
			s.Write([]byte{0x00}) //nolint:errcheck
			return
		}
		if d.onMessage != nil {
			if err := d.onMessage(&msg); err != nil {
				d.log.Warn("handle incoming message", zap.String("msg_id", msg.Id), zap.Error(err))
				s.Write([]byte{0x00}) //nolint:errcheck
				return
			}
		}

		d.log.Info("message received",
			zap.String("from", msg.FromDid),
			zap.String("msg_id", msg.Id),
		)
		s.Write([]byte{ackOK}) //nolint:errcheck
	}
}

// ─── helpers ──────────────────────────────────────────────────────────────────
