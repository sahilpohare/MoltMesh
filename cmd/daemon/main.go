package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"net"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

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

const version = "0.1.0"

func main() {
	if len(os.Args) < 2 {
		printUsage()
		os.Exit(1)
	}

	switch os.Args[1] {
	case "start":
		if err := cmdStart(os.Args[2:]); err != nil {
			fmt.Fprintln(os.Stderr, "error:", err)
			os.Exit(1)
		}
	case "status":
		if err := cmdStatus(os.Args[2:]); err != nil {
			fmt.Fprintln(os.Stderr, "error:", err)
			os.Exit(1)
		}
	case "info":
		if err := cmdInfo(os.Args[2:]); err != nil {
			fmt.Fprintln(os.Stderr, "error:", err)
			os.Exit(1)
		}
	case "stop":
		if err := cmdStop(os.Args[2:]); err != nil {
			fmt.Fprintln(os.Stderr, "error:", err)
			os.Exit(1)
		}
	case "version":
		cmdVersion()
	case "identity":
		if err := cmdIdentity(os.Args[2:]); err != nil {
			fmt.Fprintln(os.Stderr, "error:", err)
			os.Exit(1)
		}
	case "config":
		if err := cmdConfig(os.Args[2:]); err != nil {
			fmt.Fprintln(os.Stderr, "error:", err)
			os.Exit(1)
		}
	case "help", "-h", "--help":
		printUsage()
	default:
		fmt.Fprintf(os.Stderr, "unknown command: %s\n\n", os.Args[1])
		printUsage()
		os.Exit(1)
	}
}

func printUsage() {
	fmt.Fprintf(os.Stderr, `MoltMesh Daemon

Usage:
  moltmesh-daemon [command] [options]

Commands:
  start        Start the daemon in the foreground
  status       Check if daemon is running
  info         Show daemon info (addresses, peer count, etc.)
  stop         Gracefully stop a running daemon
  identity     Show daemon identity and DID
  config       Show daemon configuration
  version      Show version
  help         Show this help message

Options for 'start':
  --data-dir string     Data directory (default: ~/.moltmesh)
  --port string         Network port (default: auto-assign)
  --grpc-addr string    gRPC server address (default: unix socket in data-dir)
  --verbose             Enable verbose logging

Options for 'status', 'info', 'stop':
  --data-dir string     Data directory (default: ~/.moltmesh)
  --grpc-addr string    gRPC server address (default: unix socket in data-dir)

Examples:
  moltmesh-daemon start
  moltmesh-daemon start --data-dir ~/.moltmesh --port 9000
  moltmesh-daemon status
  moltmesh-daemon info
  moltmesh-daemon stop
  moltmesh-daemon identity
  moltmesh-daemon config
`)
}

func cmdStart(args []string) error {
	fs := flag.NewFlagSet("start", flag.ExitOnError)
	dataDir := fs.String("data-dir", "", "Data directory")
	port := fs.String("port", "", "Network port")
	grpcAddr := fs.String("grpc-addr", "", "gRPC server address")
	verbose := fs.Bool("verbose", false, "Enable verbose logging")

	fs.Parse(args)

	if *dataDir == "" {
		home, _ := os.UserHomeDir()
		*dataDir = filepath.Join(home, ".moltmesh")
	}

	return runDaemon(*dataDir, *port, *grpcAddr, *verbose)
}

func cmdVersion() {
	fmt.Printf("MoltMesh Daemon version %s\n", version)
}

func cmdIdentity(args []string) error {
	fs := flag.NewFlagSet("identity", flag.ExitOnError)
	dataDir := fs.String("data-dir", "", "Data directory")
	fs.Parse(args)

	if *dataDir == "" {
		home, _ := os.UserHomeDir()
		*dataDir = filepath.Join(home, ".moltmesh")
	}

	idPath := filepath.Join(*dataDir, "identity.json")
	id, err := identity.Load(idPath)
	if err != nil {
		if os.IsNotExist(err) {
			return fmt.Errorf("identity not found. run 'moltmesh-daemon start' first to generate")
		}
		return fmt.Errorf("load identity: %w", err)
	}

	fmt.Printf("DID: %s\n", id.DID)
	return nil
}

func cmdConfig(args []string) error {
	fs := flag.NewFlagSet("config", flag.ExitOnError)
	dataDir := fs.String("data-dir", "", "Data directory")
	fs.Parse(args)

	if *dataDir == "" {
		home, _ := os.UserHomeDir()
		*dataDir = filepath.Join(home, ".moltmesh")
	}

	config := map[string]interface{}{
		"data_dir":   *dataDir,
		"identity":   filepath.Join(*dataDir, "identity.json"),
		"grpc_sock":  filepath.Join(*dataDir, "a2a.sock"),
		"databases": map[string]string{
			"inbox":   filepath.Join(*dataDir, "inbox.db"),
			"tasks":   filepath.Join(*dataDir, "tasks.db"),
			"threads": filepath.Join(*dataDir, "threads.db"),
		},
		"blob_store": filepath.Join(*dataDir, "blobs"),
	}

	data, _ := json.MarshalIndent(config, "", "  ")
	fmt.Println(string(data))
	return nil
}

func runDaemon(dataDir, port, grpcAddr string, verbose bool) error {
	var log *zap.Logger
	if verbose {
		log, _ = zap.NewDevelopment()
	} else {
		log, _ = zap.NewProduction()
	}
	defer log.Sync()

	log.Info("MoltMesh Daemon starting", zap.String("version", version))

	if err := os.MkdirAll(dataDir, 0700); err != nil {
		return fmt.Errorf("create data dir: %w", err)
	}
	log.Info("data directory ready", zap.String("path", dataDir))

	return run(dataDir, port, grpcAddr, log)
}

func run(dataDir, port, grpcAddr string, log *zap.Logger) error {
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
	if port != "" {
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

func cmdStatus(args []string) error {
	fs := flag.NewFlagSet("status", flag.ExitOnError)
	dataDir := fs.String("data-dir", "", "Data directory")
	grpcAddr := fs.String("grpc-addr", "", "gRPC server address")
	fs.Parse(args)

	if *dataDir == "" {
		home, _ := os.UserHomeDir()
		*dataDir = filepath.Join(home, ".moltmesh")
	}
	if *grpcAddr == "" {
		*grpcAddr = filepath.Join(*dataDir, "a2a.sock")
	}

	conn, err := dialGRPC(*grpcAddr)
	if err != nil {
		return fmt.Errorf("daemon not responding: %w", err)
	}
	defer conn.Close()

	client := pb.NewA2ANodeClient(conn)
	identity, err := client.GetIdentity(context.Background(), &pb.Empty{})
	if err != nil {
		return fmt.Errorf("failed to get identity: %w", err)
	}

	fmt.Printf("Status: running\n")
	fmt.Printf("DID: %s\n", identity.Did)
	fmt.Printf("Addresses: %d\n", len(identity.Multiaddrs))
	for _, addr := range identity.Multiaddrs {
		fmt.Printf("  - %s\n", addr)
	}
	return nil
}

func cmdInfo(args []string) error {
	fs := flag.NewFlagSet("info", flag.ExitOnError)
	dataDir := fs.String("data-dir", "", "Data directory")
	grpcAddr := fs.String("grpc-addr", "", "gRPC server address")
	fs.Parse(args)

	if *dataDir == "" {
		home, _ := os.UserHomeDir()
		*dataDir = filepath.Join(home, ".moltmesh")
	}
	if *grpcAddr == "" {
		*grpcAddr = filepath.Join(*dataDir, "a2a.sock")
	}

	conn, err := dialGRPC(*grpcAddr)
	if err != nil {
		return fmt.Errorf("daemon not responding: %w", err)
	}
	defer conn.Close()

	client := pb.NewA2ANodeClient(conn)
	identity, err := client.GetIdentity(context.Background(), &pb.Empty{})
	if err != nil {
		return fmt.Errorf("failed to get identity: %w", err)
	}

	data := map[string]interface{}{
		"did":         identity.Did,
		"public_key":  identity.PublicKey,
		"addresses":   identity.Multiaddrs,
	}

	output, _ := json.MarshalIndent(data, "", "  ")
	fmt.Println(string(output))
	return nil
}

func cmdStop(args []string) error {
	fs := flag.NewFlagSet("stop", flag.ExitOnError)
	dataDir := fs.String("data-dir", "", "Data directory")
	grpcAddr := fs.String("grpc-addr", "", "gRPC server address")
	fs.Parse(args)

	if *dataDir == "" {
		home, _ := os.UserHomeDir()
		*dataDir = filepath.Join(home, ".moltmesh")
	}
	if *grpcAddr == "" {
		*grpcAddr = filepath.Join(*dataDir, "a2a.sock")
	}

	conn, err := dialGRPC(*grpcAddr)
	if err != nil {
		return fmt.Errorf("daemon not responding: %w", err)
	}
	defer conn.Close()

	// Verify the daemon is running
	client := pb.NewA2ANodeClient(conn)
	_, err = client.GetIdentity(context.Background(), &pb.Empty{})
	if err != nil {
		return fmt.Errorf("failed to contact daemon: %w", err)
	}

	fmt.Println("Note: To stop the daemon, send SIGTERM to the process")
	fmt.Println("Example: pkill -f 'moltmesh-daemon start'")
	fmt.Println("\nOr find the process and kill it:")
	fmt.Println("  ps aux | grep moltmesh-daemon")
	fmt.Println("  kill <PID>")
	return nil
}

func dialGRPC(addr string) (*grpc.ClientConn, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	opts := []grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithBlock(),
	}

	if addr[0] == '/' {
		// Unix socket
		return grpc.DialContext(ctx, "unix:"+addr, opts...)
	}
	return grpc.DialContext(ctx, addr, opts...)
}