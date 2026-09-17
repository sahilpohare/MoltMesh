package rpc

import (
	"context"
	"fmt"
	"time"

	"go.uber.org/zap"
	"google.golang.org/grpc/metadata"

	"github.com/sahilpohare/p2p-a2a/daemon/deliver"
	"github.com/sahilpohare/p2p-a2a/daemon/gossip"
	"github.com/sahilpohare/p2p-a2a/daemon/identity"
	"github.com/sahilpohare/p2p-a2a/daemon/inbox"
	"github.com/sahilpohare/p2p-a2a/daemon/names"
	"github.com/sahilpohare/p2p-a2a/daemon/network"
	"github.com/sahilpohare/p2p-a2a/daemon/node"
	"github.com/sahilpohare/p2p-a2a/daemon/outbox"
	"github.com/sahilpohare/p2p-a2a/daemon/registry"
	"github.com/sahilpohare/p2p-a2a/daemon/session"
	"github.com/sahilpohare/p2p-a2a/daemon/tasks"
	"github.com/sahilpohare/p2p-a2a/daemon/thread"
	"github.com/sahilpohare/p2p-a2a/daemon/webhook"
	pb "github.com/sahilpohare/p2p-a2a/gen/a2a/v1"
)

// ThreadManager is the thread-subsystem surface the gRPC server needs.
// Satisfied by *thread.Manager (Engine/GossipBridge/RaftBackend.Run path)
// and, once wired at boot, an actors.ActorManager (ThreadActor/GoAkt path).
// Engine must return a true nil thread.ThreadSubscription (not a nil pointer
// boxed into the interface) when the thread exists but this node is not a
// validator for it — see thread.Manager.Engine's doc comment for why.
type ThreadManager interface {
	CreateThread(ctx context.Context, req *pb.CreateThreadRequest) (*pb.Thread, error)
	GetThread(threadID string) (*pb.Thread, error)
	AppendEntry(threadID string, entry *pb.ThreadEntry) error
	GetEntries(threadID string, sinceHeight int64, limit int) ([]*pb.ThreadEntryWithPos, error)
	Engine(threadID string) thread.ThreadSubscription
}

// Server implements the A2ANode gRPC service.
type Server struct {
	pb.UnimplementedA2ANodeServer

	id       *identity.Identity
	inbox    *inbox.Inbox
	outbox   *outbox.Outbox
	tasks    *tasks.Store
	registry *registry.Registry
	gossip   *gossip.Manager
	// blobs field removed — Bitswap + blockstore accessed via s.node.Bitswap / s.node.Blockstore
	dlv       *deliver.Deliverer
	threads   ThreadManager
	networks  *network.Manager
	webhooks  *webhook.Dispatcher
	nameReg   *names.Registry
	node      *node.Node
	addrs     []string
	startedAt time.Time
	sessions  *session.Manager
	log       *zap.Logger
}

// New creates a new gRPC server. sessions is shared with the Deliverer
// (daemon/deliver) rather than constructed here, so both agree on which SDK
// identities currently have a live session on this daemon — see
// deliver.SessionChecker for why that agreement has to be exact.
func New(
	id *identity.Identity,
	ib *inbox.Inbox,
	ob *outbox.Outbox,
	ts *tasks.Store,
	reg *registry.Registry,
	gm *gossip.Manager,
	dlv *deliver.Deliverer,
	tm ThreadManager,
	nm *network.Manager,
	wh *webhook.Dispatcher,
	nr *names.Registry,
	n *node.Node,
	addrs []string,
	sessions *session.Manager,
	log *zap.Logger,
) *Server {
	if sessions == nil {
		// Callers that don't share a Deliverer-visible session.Manager (tests,
		// or any future caller that never needs cross-daemon delivery to a
		// session-scoped worker) still get a working one instead of a nil
		// pointer every session-authenticated RPC would panic on.
		sessions = session.New(id.DID)
	}
	return &Server{
		id:        id,
		inbox:     ib,
		outbox:    ob,
		tasks:     ts,
		registry:  reg,
		gossip:    gm,
		dlv:       dlv,
		threads:   tm,
		networks:  nm,
		webhooks:  wh,
		nameReg:   nr,
		node:      n,
		addrs:     addrs,
		startedAt: time.Now(),
		sessions:  sessions,
		log:       log,
	}
}

// ─── Identity ────────────────────────────────────────────────────────────────

func (s *Server) agentDID(ctx context.Context) (string, error) {
	return s.sessions.Authenticate(bearerToken(ctx))
}

// scopedOwner returns the authenticated SDK DID when a session is supplied.
// The empty owner keeps the explicitly supported legacy single-agent mode.
func (s *Server) scopedOwner(ctx context.Context) (string, error) {
	if bearerToken(ctx) == "" {
		if s.sessions.MultipleAgents() {
			return "", fmt.Errorf("unscoped RPC is not allowed while multiple SDK agents are connected")
		}
		return "", nil
	}
	return s.agentDID(ctx)
}

func bearerToken(ctx context.Context) string {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return ""
	}
	for _, value := range md.Get("authorization") {
		const prefix = "Bearer "
		if len(value) > len(prefix) && value[:len(prefix)] == prefix {
			return value[len(prefix):]
		}
	}
	return ""
}
