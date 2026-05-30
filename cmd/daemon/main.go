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
	"github.com/sahilpohare/p2p-a2a/pkg/format"
	"github.com/sahilpohare/p2p-a2a/daemon/blob"
	"github.com/sahilpohare/p2p-a2a/daemon/deliver"
	"github.com/sahilpohare/p2p-a2a/daemon/gossip"
	"github.com/sahilpohare/p2p-a2a/daemon/identity"
	"github.com/sahilpohare/p2p-a2a/daemon/inbox"
	"github.com/sahilpohare/p2p-a2a/daemon/network"
	"github.com/sahilpohare/p2p-a2a/daemon/node"
	"github.com/sahilpohare/p2p-a2a/daemon/outbox"
	"github.com/sahilpohare/p2p-a2a/daemon/registry"
	"github.com/sahilpohare/p2p-a2a/daemon/rpc"
	"github.com/sahilpohare/p2p-a2a/daemon/tasks"
	"github.com/sahilpohare/p2p-a2a/daemon/thread"
	"github.com/sahilpohare/p2p-a2a/daemon/webhook"
)

const version = "0.1.0"

func main() {
	// Strip --json global flag before dispatching so sub-commands don't see it.
	args := os.Args[1:]
	filtered := args[:0]
	for _, a := range args {
		if a == "--json" || a == "-json" {
			jsonMode = true
		} else {
			filtered = append(filtered, a)
		}
	}
	args = filtered

	if len(args) == 0 {
		printUsage()
		os.Exit(1)
	}

	var err error
	switch args[0] {
	// Daemon management
	case "start":
		err = cmdStart(args[1:])
	case "version":
		cmdVersion()
	case "help", "-h", "--help":
		printUsage()

	// Convenience
	case "status":
		err = cmdStatus(args[1:])
	case "info":
		err = cmdInfo(args[1:])
	case "identity":
		err = cmdIdentity(args[1:])
	case "config":
		err = cmdConfig(args[1:])
	case "stop":
		err = cmdStop(args[1:])

	// Identity & Registry
	case "get-identity":
		err = cmdGetIdentity(args[1:])
	case "get-agent-card":
		err = cmdGetAgentCard(args[1:])
	case "publish-agent-card":
		err = cmdPublishAgentCard(args[1:])
	case "find-agents":
		err = cmdFindAgents(args[1:])

	// Messaging
	case "send-message":
		err = cmdSendMessage(args[1:])
	case "subscribe-inbox":
		err = cmdSubscribeInbox(args[1:])
	case "get-inbox":
		err = cmdGetInbox(args[1:])
	case "get-outbox":
		err = cmdGetOutbox(args[1:])
	case "ack-message":
		err = cmdAckMessage(args[1:])

	// Tasks
	case "create-task":
		err = cmdCreateTask(args[1:])
	case "get-task":
		err = cmdGetTask(args[1:])
	case "update-task":
		err = cmdUpdateTask(args[1:])
	case "cancel-task":
		err = cmdCancelTask(args[1:])
	case "publish-task-event":
		err = cmdPublishTaskEvent(args[1:])
	case "subscribe-task-events":
		err = cmdSubscribeTaskEvents(args[1:])

	// Files
	case "send-file":
		err = cmdSendFile(args[1:])
	case "fetch-file":
		err = cmdFetchFile(args[1:])

	// Threads
	case "create-thread":
		err = cmdCreateThread(args[1:])
	case "get-thread":
		err = cmdGetThread(args[1:])
	case "append-entry":
		err = cmdAppendEntry(args[1:])
	case "get-thread-entries":
		err = cmdGetThreadEntries(args[1:])
	case "subscribe-thread":
		err = cmdSubscribeThread(args[1:])

	// Diagnostics
	case "ping":
		err = cmdPing(args[1:])
	case "health":
		err = cmdHealth(args[1:])
	case "peers":
		err = cmdPeers(args[1:])

	// Formatting utilities
	case "format":
		err = cmdFormat(args[1:])

	// PubSub
	case "publish":
		err = cmdPublish(args[1:])
	case "subscribe-topic":
		err = cmdSubscribeTopic(args[1:])

	// Webhook
	case "set-webhook":
		err = cmdSetWebhook(args[1:])
	case "clear-webhook":
		err = cmdClearWebhook(args[1:])
	case "get-webhook":
		err = cmdGetWebhook(args[1:])

	// Networks
	case "network":
		err = cmdNetwork(args[1:])

	default:
		fmt.Fprintf(os.Stderr, "unknown command: %s\n\n", args[0])
		printUsage()
		os.Exit(1)
	}

	if err != nil {
		jsonErr("error", err.Error())
		os.Exit(1)
	}
}

func printUsage() {
	fmt.Fprint(os.Stderr, `MoltMesh Daemon

Usage:
  moltmesh-daemon [command] [options]

Daemon management:
  start        Start the daemon in the foreground
  status       Check if daemon is running
  info         Show daemon info (addresses, peer count, etc.)
  stop         Gracefully stop a running daemon
  identity     Show local identity DID (no daemon required)
  config       Show daemon configuration
  version      Show version
  help         Show this help message

Identity & Registry:
  get-identity            Get this node's identity
  get-agent-card          Get agent card for a DID (--did)
  publish-agent-card      Publish agent card (--name, --description)
  find-agents             Find agents by capability (--capability, --limit)

Messaging:
  send-message            Send a message (--to, --text)
  get-inbox               List inbox messages (--limit, --unread)
  get-outbox              List outbox messages (--status, --limit)
  subscribe-inbox         Stream incoming messages (--thread-id)
  ack-message             Acknowledge a message (--id)

Tasks:
  create-task             Create a task (--to, --skill)
  get-task                Get task by ID (--id)
  update-task             Update task status (--id, --status)
  cancel-task             Cancel a task (--id)
  publish-task-event      Publish a task event (--task-id, --kind)
  subscribe-task-events   Stream task events (--id)

Files:
  send-file               Upload a file (--file)
  fetch-file              Download a file (--cid, --from)

Diagnostics:
  health                  Show daemon health (version, peers, uptime)
  ping [did] [--count]    Measure latency to a peer (or loopback if no DID)
  peers                   List connected peers

PubSub:
  publish                 Publish to a topic (--topic, --payload)
  subscribe-topic         Stream topic messages (--topic)

Webhook:
  set-webhook             Set webhook URL (--url, --secret)
  clear-webhook           Remove webhook configuration
  get-webhook             Show current webhook URL

Networks:
  network create <name>         Create a named network
  network join <id>             Join a network
  network leave <id>            Leave a network
  network list                  List networks you belong to
  network members <id>          List members of a network
  network broadcast <id> <msg>  Broadcast a message to a network
  network subscribe <id>        Stream broadcasts from a network

Format utilities (no daemon required):
  format did <did>...          Validate and shorten a did:key
  format capability <cap>...   Parse a capability ID
  format multiaddr <addr>...   Shorten a multiaddr
  format bytes <n>...          Human-readable byte size
  format time <unix_ms>...     Format a Unix millisecond timestamp

Threads:
  create-thread           Create a thread (--replicas, --f)
  get-thread              Get thread info (--id)
  append-entry            Append entry to thread (--thread-id, --payload, --kind)
  get-thread-entries      List thread entries (--id, --since, --limit)
  subscribe-thread        Stream thread entries (--id, --since)

Global options for client commands:
  --data-dir string     Data directory (default: ~/.moltmesh)
  --grpc-addr string    gRPC server address (default: unix socket in data-dir)

Options for 'start':
  --data-dir string     Data directory (default: ~/.moltmesh)
  --port string         Network port (default: auto-assign)
  --grpc-addr string    gRPC server address (default: unix socket in data-dir)
  --verbose             Enable verbose logging

Examples:
  moltmesh-daemon start
  moltmesh-daemon start --data-dir ~/.moltmesh --port 9000
  moltmesh-daemon status
  moltmesh-daemon send-message --to did:key:z6Mk... --text "hello"
  moltmesh-daemon get-inbox --unread --limit 20
  moltmesh-daemon find-agents --capability a2a:v1:cap:text-generation
  moltmesh-daemon ping did:key:z6Mk... --count 3
  moltmesh-daemon health
  moltmesh-daemon peers
  moltmesh-daemon --json health
  moltmesh-daemon --json peers

Global flags (before command):
  --json    Emit JSON on stdout ({"status":"ok","data":...} or {"status":"error",...})
`)
}

// defaultDataDir returns the default data directory, erroring if home dir lookup fails.
func defaultDataDir() (string, error) {
	home, err := os.UserHomeDir()
	if err != nil {
		return "", fmt.Errorf("could not determine home directory: %w", err)
	}
	return filepath.Join(home, ".moltmesh"), nil
}

// resolveDataDir fills in dataDir if empty.
func resolveDataDir(dataDir string) (string, error) {
	if dataDir != "" {
		return dataDir, nil
	}
	return defaultDataDir()
}

// resolveGRPCAddr fills in grpcAddr based on dataDir if empty.
func resolveGRPCAddr(grpcAddr, dataDir string) string {
	if grpcAddr != "" {
		return grpcAddr
	}
	return filepath.Join(dataDir, "a2a.sock")
}

func cmdStart(args []string) error {
	fs := flag.NewFlagSet("start", flag.ExitOnError)
	dataDir := fs.String("data-dir", "", "Data directory")
	port := fs.String("port", "", "Network port")
	grpcAddr := fs.String("grpc-addr", "", "gRPC server address")
	verbose := fs.Bool("verbose", false, "Enable verbose logging")
	fs.Parse(args)

	dir, err := resolveDataDir(*dataDir)
	if err != nil {
		return err
	}
	return runDaemon(dir, *port, *grpcAddr, *verbose)
}

func cmdVersion() {
	fmt.Printf("MoltMesh Daemon version %s\n", version)
}

func cmdIdentity(args []string) error {
	fs := flag.NewFlagSet("identity", flag.ExitOnError)
	dataDir := fs.String("data-dir", "", "Data directory")
	fs.Parse(args)

	dir, err := resolveDataDir(*dataDir)
	if err != nil {
		return err
	}

	idPath := filepath.Join(dir, "identity.json")
	id, err := identity.Load(idPath)
	if err != nil {
		if os.IsNotExist(err) {
			return fmt.Errorf("identity not found; run 'moltmesh-daemon start' first to generate")
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

	dir, err := resolveDataDir(*dataDir)
	if err != nil {
		return err
	}

	config := map[string]interface{}{
		"data_dir":  dir,
		"identity":  filepath.Join(dir, "identity.json"),
		"grpc_sock": filepath.Join(dir, "a2a.sock"),
		"databases": map[string]string{
			"inbox":   filepath.Join(dir, "inbox.db"),
			"tasks":   filepath.Join(dir, "tasks.db"),
			"threads": filepath.Join(dir, "threads.db"),
		},
		"blob_store": filepath.Join(dir, "blobs"),
	}

	data, err := json.MarshalIndent(config, "", "  ")
	if err != nil {
		return err
	}
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

	// ── network store + manager ───────────────────────────────────────────────
	netStore, err := network.New(filepath.Join(dataDir, "networks.db"))
	if err != nil {
		return fmt.Errorf("network store: %w", err)
	}
	defer netStore.Close()
	nm := network.NewManager(netStore, gm)

	// ── webhook dispatcher ────────────────────────────────────────────────────
	wh := webhook.New(log)

	// ── gRPC server ─────────────────────────────────────────────────────────
	if grpcAddr == "" {
		grpcAddr = filepath.Join(dataDir, "a2a.sock")
	}

	rpc.SetVersion(version)
	srv := rpc.New(id, ib, ob, ts, reg, gm, bs, dlv, tm, nm, wh, n, n.Addrs(), log)
	grpcServer := grpc.NewServer()
	pb.RegisterA2ANodeServer(grpcServer, srv)
	pb.RegisterDiagServer(grpcServer, srv)
	pb.RegisterExtServer(grpcServer, srv)

	var lis net.Listener
	if grpcAddr[0] == '/' {
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

	// ── write PID file ───────────────────────────────────────────────────────
	pidFile := filepath.Join(dataDir, "daemon.pid")
	if err := os.WriteFile(pidFile, []byte(fmt.Sprintf("%d\n", os.Getpid())), 0600); err != nil {
		log.Warn("could not write PID file", zap.Error(err))
	}
	defer os.Remove(pidFile)

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
	conn, client, _, err := dialClient(args, "status")
	if err != nil {
		return err
	}
	defer conn.Close()

	id, err := client.GetIdentity(context.Background(), &pb.Empty{})
	if err != nil {
		return fmt.Errorf("get identity: %w", err)
	}

	fmt.Printf("status:     running\n")
	fmt.Printf("did:        %s\n", id.Did)
	fmt.Printf("addresses:  %d\n", len(id.Multiaddrs))
	for _, addr := range id.Multiaddrs {
		fmt.Printf("  %s\n", format.Multiaddr(addr))
	}
	return nil
}

func cmdInfo(args []string) error {
	conn, client, _, err := dialClient(args, "info")
	if err != nil {
		return err
	}
	defer conn.Close()

	id, err := client.GetIdentity(context.Background(), &pb.Empty{})
	if err != nil {
		return fmt.Errorf("get identity: %w", err)
	}

	data, _ := json.MarshalIndent(map[string]interface{}{
		"did":        id.Did,
		"public_key": id.PublicKey,
		"addresses":  id.Multiaddrs,
	}, "", "  ")
	fmt.Println(string(data))
	return nil
}

func cmdStop(args []string) error {
	conn, client, dataDir, err := dialClient(args, "stop")
	if err != nil {
		return fmt.Errorf("daemon not responding: %w", err)
	}
	defer conn.Close()

	// Verify running
	if _, err := client.GetIdentity(context.Background(), &pb.Empty{}); err != nil {
		return fmt.Errorf("failed to contact daemon: %w", err)
	}

	// Try PID file first
	pidFile := filepath.Join(dataDir, "daemon.pid")
	data, err := os.ReadFile(pidFile)
	if err == nil {
		var pid int
		if _, err := fmt.Sscanf(string(data), "%d", &pid); err == nil && pid > 0 {
			proc, err := os.FindProcess(pid)
			if err == nil {
				if err := proc.Signal(syscall.SIGTERM); err == nil {
					fmt.Printf("Sent SIGTERM to daemon (PID %d)\n", pid)
					return nil
				}
			}
		}
	}

	fmt.Println("Could not read PID file. Stop the daemon manually:")
	fmt.Println("  pkill -f 'moltmesh-daemon start'")
	return nil
}

// dialClient parses --data-dir and --grpc-addr, then connects.
// Returns conn, client, resolved dataDir, error.
func dialClient(args []string, cmdName string) (*grpc.ClientConn, pb.A2ANodeClient, string, error) {
	fs := flag.NewFlagSet(cmdName, flag.ExitOnError)
	dataDir := fs.String("data-dir", "", "Data directory")
	grpcAddr := fs.String("grpc-addr", "", "gRPC server address")
	fs.Parse(args)

	dir, err := resolveDataDir(*dataDir)
	if err != nil {
		return nil, nil, "", err
	}
	addr := resolveGRPCAddr(*grpcAddr, dir)

	conn, err := dialGRPC(addr)
	if err != nil {
		return nil, nil, "", err
	}
	return conn, pb.NewA2ANodeClient(conn), dir, nil
}

func dialGRPC(addr string) (*grpc.ClientConn, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	opts := []grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithBlock(),
	}

	if addr[0] == '/' {
		return grpc.DialContext(ctx, "unix:"+addr, opts...)
	}
	return grpc.DialContext(ctx, addr, opts...)
}
