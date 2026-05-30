package main

import (
	"context"
	"fmt"
	"net"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"

	"go.uber.org/zap"
	"google.golang.org/grpc"

	pb "github.com/sahilpohare/p2p-a2a/gen/a2a/v1"
	"github.com/sahilpohare/p2p-a2a/daemon/blob"
	"github.com/sahilpohare/p2p-a2a/daemon/deliver"
	"github.com/sahilpohare/p2p-a2a/daemon/gossip"
	"github.com/sahilpohare/p2p-a2a/daemon/identity"
	"github.com/sahilpohare/p2p-a2a/daemon/inbox"
	"github.com/sahilpohare/p2p-a2a/daemon/node"
	"github.com/sahilpohare/p2p-a2a/daemon/outbox"
	"github.com/sahilpohare/p2p-a2a/daemon/registry"
	"github.com/sahilpohare/p2p-a2a/daemon/rpc"
	"github.com/sahilpohare/p2p-a2a/daemon/tasks"
	"github.com/sahilpohare/p2p-a2a/daemon/thread"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}

func run() error {
	log, _ := zap.NewDevelopment()
	defer log.Sync()

	// ── data directory ──────────────────────────────────────────────────────
	dataDir := os.Getenv("A2A_DATA_DIR")
	if dataDir == "" {
		home, _ := os.UserHomeDir()
		dataDir = filepath.Join(home, ".p2p-a2a")
	}
	if err := os.MkdirAll(dataDir, 0700); err != nil {
		return fmt.Errorf("create data dir: %w", err)
	}

	// ── identity ────────────────────────────────────────────────────────────
	idPath := filepath.Join(dataDir, "identity.json")
	var id *identity.Identity
	var err error

	if _, statErr := os.Stat(idPath); os.IsNotExist(statErr) {
		log.Info("generating new identity")
		id, err = identity.Generate()
		if err != nil {
			return fmt.Errorf("generate identity: %w", err)
		}
		if err := id.Save(idPath); err != nil {
			return fmt.Errorf("save identity: %w", err)
		}
	} else {
		id, err = identity.Load(idPath)
		if err != nil {
			return fmt.Errorf("load identity: %w", err)
		}
	}
	log.Info("identity loaded", zap.String("did", id.DID))

	// ── context ─────────────────────────────────────────────────────────────
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// ── libp2p node ─────────────────────────────────────────────────────────
	listenAddrs := []string{"/ip4/0.0.0.0/udp/0/quic-v1", "/ip4/0.0.0.0/tcp/0"}
	if port := os.Getenv("A2A_PORT"); port != "" {
		listenAddrs = []string{
			fmt.Sprintf("/ip4/0.0.0.0/udp/%s/quic-v1", port),
			fmt.Sprintf("/ip4/0.0.0.0/tcp/%s", port),
		}
	}

	n, err := node.New(ctx, id, node.Config{
		ListenAddrs: listenAddrs,
		DataDir:     dataDir,
	}, log)
	if err != nil {
		return fmt.Errorf("create node: %w", err)
	}
	defer n.Close()

	// ── storage ─────────────────────────────────────────────────────────────
	ib, err := inbox.New(filepath.Join(dataDir, "inbox.db"))
	if err != nil {
		return fmt.Errorf("inbox: %w", err)
	}
	defer ib.Close()

	ts, err := tasks.New(filepath.Join(dataDir, "tasks.db"))
	if err != nil {
		return fmt.Errorf("tasks: %w", err)
	}
	defer ts.Close()

	// ── registry ────────────────────────────────────────────────────────────
	reg := registry.New(n.DHT, id, log)
	go reg.RunRepublish(ctx)

	// ── gossip ──────────────────────────────────────────────────────────────
	gm := gossip.New(n.PubSub, log)

	// ── blob store ───────────────────────────────────────────────────────────
	bs, err := blob.New(filepath.Join(dataDir, "blobs"))
	if err != nil {
		return fmt.Errorf("blob store: %w", err)
	}

	// ── delivery layer (libp2p stream protocol) ──────────────────────────────
	dlv := deliver.New(n.Host, reg, ib, log)
	dlv.RegisterBlobHandler(bs)

	// ── outbox (with real delivery function) ─────────────────────────────────
	ob, err := outbox.New(
		filepath.Join(dataDir, "outbox.db"),
		dlv.DeliverFunc(),
		log,
	)
	if err != nil {
		return fmt.Errorf("outbox: %w", err)
	}
	defer ob.Close()
	go ob.Run(ctx)

	// ── thread manager ───────────────────────────────────────────────────────
	threadStore, err := thread.NewStore(filepath.Join(dataDir, "threads.db"))
	if err != nil {
		return fmt.Errorf("thread store: %w", err)
	}
	defer threadStore.Close()

	tm := thread.NewManager(ctx, threadStore, id, n.PubSub, log)

	// ── gRPC server ─────────────────────────────────────────────────────────
	grpcAddr := os.Getenv("A2A_GRPC_ADDR")
	if grpcAddr == "" {
		grpcAddr = filepath.Join(dataDir, "a2a.sock")
	}

	srv := rpc.New(id, ib, ob, ts, reg, gm, bs, dlv, tm, n.Addrs(), log)
	grpcServer := grpc.NewServer()
	pb.RegisterA2ANodeServer(grpcServer, srv)

	var lis net.Listener
	if grpcAddr[0] == '/' {
		// Unix socket
		os.Remove(grpcAddr)
		lis, err = net.Listen("unix", grpcAddr)
	} else {
		lis, err = net.Listen("tcp", grpcAddr)
	}
	if err != nil {
		return fmt.Errorf("listen grpc: %w", err)
	}

	go func() {
		log.Info("gRPC server listening", zap.String("addr", grpcAddr))
		if err := grpcServer.Serve(lis); err != nil {
			log.Error("gRPC serve", zap.Error(err))
		}
	}()

	// ── shutdown ─────────────────────────────────────────────────────────────
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	log.Info("shutting down")
	grpcServer.GracefulStop()
	cancel()
	return nil
}

