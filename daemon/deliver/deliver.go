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

// writeAck writes the single-byte ack and half-closes the stream for
// writing. network.Stream.Close (called via defer once receiveHandlerRaw
// returns) "will not wait for any form of acknowledgment" and does not
// guarantee the peer receives data written just before it — the interface's
// own docs prescribe CloseWrite after the final write as the way to get a
// graceful, guaranteed-flushed half-close instead. Without it, sendToPeerRaw
// intermittently read back io.EOF instead of the ack byte, and its outbox
// caller retried a message the receiver had in fact already processed
// successfully — forever, since nothing on the receiving side changes on
// the next identical retry either.
func writeAck(s network.Stream, ack byte) {
	s.Write([]byte{ack}) //nolint:errcheck
	s.CloseWrite()       //nolint:errcheck
}

// Deliverer sends messages to remote peers.
// ThreadInviter is implemented by thread.Manager.
type ThreadInviter interface {
	InviteReceived(thread *pb.Thread) error
}

// SessionChecker reports whether an SDK agent is currently authenticated as
// a given DID on this daemon. Implemented by session.Manager. A message
// addressed to such a DID is accepted even though it isn't the daemon's own
// libp2p identity — see the recipient check in receiveHandler for why this
// exists: without it, a task delegated to a specific session-scoped worker
// (ADR-0020, ADR-0022) can never be delivered across daemons at all.
type SessionChecker interface {
	HasSession(did string) bool
}

type Deliverer struct {
	host       host.Host
	registry   *registry.Registry
	threads    ThreadInviter
	sessions   SessionChecker
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
func New(h host.Host, reg *registry.Registry, ib *inbox.Inbox, tm ThreadInviter, sessions SessionChecker, log *zap.Logger) *Deliverer {
	d := &Deliverer{host: h, registry: reg, threads: tm, sessions: sessions, log: log, peerExec: make(map[peer.ID]*peerExecutor)}
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
	if d.hierarchy == nil {
		return d.sendToPeerRaw(ctx, peerID, msg)
	}
	exec, err := d.executor(ctx, peerID)
	if err != nil {
		return err
	}
	defer d.releaseExecutor(peerID, exec)
	return appactors.DispatchErr(exec, func() error { return d.sendToPeerRaw(ctx, peerID, msg) })
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
		appactors.DispatchVoid(exec, func() { raw(s) })
	}
}

// senderIsSessionOnPeer reports whether claimedDID is a legitimate SDK
// session hosted by remotePeer, rather than an attempted spoof. A claimed
// FromDid that isn't the peer's own libp2p-derived DID is normal — ADR-0020
// makes SDK session identities independent keypairs from the daemon's own
// identity, so a message from a session-scoped worker (e.g. a task result
// sent by whoever completed the lease) never matches the raw peer check —
// but it must still prove the claim: its own self-signed AgentCard, fetched
// from the DHT and verified against its own DID (Resolve rejects a bad
// signature), must itself claim remotePeer as its node_peer_id.
// PublishAgentCard already refuses to accept a card whose node_peer_id
// isn't the publishing daemon's own peer ID, so a peer cannot forge this by
// publishing a card claiming a peer it doesn't control.
func (d *Deliverer) senderIsSessionOnPeer(claimedDID string, remotePeer peer.ID) bool {
	if d.registry == nil {
		return false
	}
	card, err := d.registry.Resolve(context.Background(), claimedDID)
	if err != nil {
		return false
	}
	return card.NodePeerId == remotePeer.String()
}

func (d *Deliverer) receiveHandlerRaw(ib *inbox.Inbox) network.StreamHandler {
	return func(s network.Stream) {
		defer s.Close()
		s.SetDeadline(time.Now().Add(streamTimeout)) //nolint:errcheck

		r := msgio.NewReaderSize(s, maxMsgSize)
		data, err := r.ReadMsg()
		if err != nil {
			d.log.Warn("read incoming message", zap.Error(err))
			writeAck(s, 0x00)
			return
		}

		var msg pb.Message
		if err := proto.Unmarshal(data, &msg); err != nil {
			d.log.Warn("unmarshal incoming message", zap.Error(err))
			writeAck(s, 0x00)
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
			writeAck(s, 0x00)
			return
		}
		rawPub, err := remotePubKey.Raw()
		if err != nil {
			d.log.Warn("extract raw pubkey", zap.Error(err))
			writeAck(s, 0x00)
			return
		}
		expectedDID := identity.DIDFromPubBytes(rawPub)
		if msg.FromDid != expectedDID && !d.senderIsSessionOnPeer(msg.FromDid, remotePeer) {
			d.log.Warn("sender DID mismatch",
				zap.String("claimed", msg.FromDid),
				zap.String("expected", expectedDID),
				zap.String("peer", remotePeer.String()),
			)
			writeAck(s, 0x00)
			return
		}
		localPubKey, err := s.Conn().LocalPeer().ExtractPublicKey()
		if err != nil {
			writeAck(s, 0x00)
			return
		}
		localRaw, err := localPubKey.Raw()
		// A message may be addressed either to this daemon's own libp2p
		// identity, or to an SDK agent (ADR-0020) currently holding a live
		// session on this daemon — the latter is what makes it possible to
		// delegate a task (ADR-0022) to a specific session-scoped worker
		// hosted here rather than only to "this daemon" as a whole.
		toDaemon := err == nil && msg.ToDid == identity.DIDFromPubBytes(localRaw)
		toSessionAgent := d.sessions != nil && d.sessions.HasSession(msg.ToDid)
		if !toDaemon && !toSessionAgent {
			d.log.Warn("recipient DID mismatch", zap.String("claimed", msg.ToDid))
			writeAck(s, 0x00)
			return
		}

		// Handle thread invites before hitting the inbox.
		if msg.Kind == pb.MessageKind_MESSAGE_KIND_THREAD_INVITE {
			var thread pb.Thread
			if err := proto.Unmarshal(msg.Payload, &thread); err != nil {
				d.log.Warn("unmarshal thread invite", zap.Error(err))
				writeAck(s, 0x00)
				return
			}
			if d.threads == nil {
				d.log.Warn("thread invite rejected: thread manager unavailable", zap.String("thread", thread.Id))
				writeAck(s, 0x00)
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
				writeAck(s, 0x00)
				return
			}
			if err := d.threads.InviteReceived(&thread); err != nil {
				d.log.Warn("thread invite", zap.String("thread", thread.Id), zap.Error(err))
				writeAck(s, 0x00)
				return
			}
			d.log.Info("thread invite accepted", zap.String("thread", thread.Id))
			writeAck(s, ackOK)
			return
		}

		if err := ib.PutForOwner(msg.ToDid, &msg); err != nil {
			d.log.Warn("inbox put", zap.String("msg_id", msg.Id), zap.Error(err))
			writeAck(s, 0x00)
			return
		}
		if d.onMessage != nil {
			if err := d.onMessage(&msg); err != nil {
				d.log.Warn("handle incoming message", zap.String("msg_id", msg.Id), zap.Error(err))
				writeAck(s, 0x00)
				return
			}
		}

		d.log.Info("message received",
			zap.String("from", msg.FromDid),
			zap.String("msg_id", msg.Id),
		)
		writeAck(s, ackOK)
	}
}

// ─── helpers ──────────────────────────────────────────────────────────────────
