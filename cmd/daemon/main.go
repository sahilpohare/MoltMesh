package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"net"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"

	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/sahilpohare/p2p-a2a/daemon/actors"
	"github.com/sahilpohare/p2p-a2a/daemon/deliver"
	"github.com/sahilpohare/p2p-a2a/daemon/gossip"
	"github.com/sahilpohare/p2p-a2a/daemon/identity"
	"github.com/sahilpohare/p2p-a2a/daemon/inbox"
	"github.com/sahilpohare/p2p-a2a/daemon/names"
	"github.com/sahilpohare/p2p-a2a/daemon/network"
	"github.com/sahilpohare/p2p-a2a/daemon/node"
	"github.com/sahilpohare/p2p-a2a/daemon/outbox"
	"github.com/sahilpohare/p2p-a2a/daemon/registry"
	"github.com/sahilpohare/p2p-a2a/daemon/rpc"
	"github.com/sahilpohare/p2p-a2a/daemon/tasks"
	"github.com/sahilpohare/p2p-a2a/daemon/thread"
	"github.com/sahilpohare/p2p-a2a/daemon/webhook"
	pb "github.com/sahilpohare/p2p-a2a/gen/a2a/v1"
	"github.com/sahilpohare/p2p-a2a/pkg/capability"
	"github.com/sahilpohare/p2p-a2a/pkg/config"
	"github.com/sahilpohare/p2p-a2a/pkg/format"
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
	case "init":
		err = cmdInit(args[1:])
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
	case "send-task-result":
		err = cmdSendTaskResult(args[1:])

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
	case "add-thread-replica":
		err = cmdAddThreadReplica(args[1:])
	case "recover-thread":
		err = cmdRecoverThread(args[1:])

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

	// Names
	case "name":
		err = cmdName(args[1:])

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
  init         Scaffold a new agent (identity + moltbook.toml)
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
  send-task-result        Return a remote result (--to, --task-id, --result)

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

Names:
  name claim <name>             Claim a human-readable name on the network
  name resolve <name>           Resolve a name to the DID that claims it

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
  create-thread           Create a thread (--replicas, --f, --with-recovery)
  get-thread              Get thread info (--id)
  append-entry            Append entry to thread (--thread-id, --payload, --kind)
  get-thread-entries      List thread entries (--id, --since, --limit)
  subscribe-thread        Stream thread entries (--id, --since)
  add-thread-replica      Add a committed observer (--thread-id, --did)
  recover-thread          Recover verified history (--id, --secret-base64)

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

// resolveDataDir fills in dataDir if empty, and always returns an absolute
// path — a relative dataDir would otherwise produce a relative default
// Unix socket path ("<dataDir>/a2a.sock"), which net.Listen("unix", ...)
// cannot distinguish from a TCP host:port.
func resolveDataDir(dataDir string) (string, error) {
	if dataDir == "" {
		var err error
		dataDir, err = defaultDataDir()
		if err != nil {
			return "", err
		}
	}
	return filepath.Abs(dataDir)
}

// resolveGRPCAddr fills in grpcAddr based on dataDir if empty. When the
// daemon auto-picked a TCP port (grpc-addr file written by listenTCPFromPort),
// that resolved address is used; otherwise falls back to the Unix socket path.
func resolveGRPCAddr(grpcAddr, dataDir string) string {
	if grpcAddr != "" {
		return grpcAddr
	}
	if data, err := os.ReadFile(filepath.Join(dataDir, "grpc-addr")); err == nil {
		if addr := strings.TrimSpace(string(data)); addr != "" {
			return addr
		}
	}
	return filepath.Join(dataDir, "a2a.sock")
}

// defaultAgentName derives a short, stable placeholder name from the tail of
// a DID so `init` never leaves the name blank. Lowercase, hyphenated to match
// names.Normalize's expected shape — meant to be edited, not claimed as-is.
func defaultAgentName(did string) string {
	tail := did
	if i := strings.LastIndex(did, ":"); i >= 0 {
		tail = did[i+1:]
	}
	tail = strings.ToLower(tail)
	if len(tail) > 8 {
		tail = tail[len(tail)-8:]
	}
	return "agent-" + tail
}

// quotedCSV renders a string slice as a TOML inline array of quoted strings.
func quotedCSV(items []string) string {
	quoted := make([]string, len(items))
	for i, s := range items {
		quoted[i] = fmt.Sprintf("%q", s)
	}
	return strings.Join(quoted, ", ")
}

// listenTCPFromPort binds a TCP listener starting at addr's port, incrementing
// the port and retrying while it's already in use. Returns the listener and
// the address it actually bound to.
func listenTCPFromPort(addr string) (net.Listener, string, error) {
	host, portStr, err := net.SplitHostPort(addr)
	if err != nil {
		return nil, "", fmt.Errorf("invalid grpc-addr %q: %w", addr, err)
	}
	port, err := strconv.Atoi(portStr)
	if err != nil {
		return nil, "", fmt.Errorf("invalid grpc-addr port %q: %w", portStr, err)
	}

	const maxAttempts = 1000
	for i := 0; i < maxAttempts; i++ {
		tryAddr := net.JoinHostPort(host, strconv.Itoa(port+i))
		lis, err := net.Listen("tcp", tryAddr)
		if err == nil {
			return lis, tryAddr, nil
		}
		if !errors.Is(err, syscall.EADDRINUSE) {
			return nil, "", err
		}
	}
	return nil, "", fmt.Errorf("no free port found starting at %d after %d attempts", port, maxAttempts)
}

func cmdStart(args []string) error {
	fs := flag.NewFlagSet("start", flag.ExitOnError)
	dataDir := fs.String("data-dir", "", "Data directory")
	port := fs.String("port", "", "Network port")
	listenHost := fs.String("listen-host", "", "IP to bind libp2p listeners to (default: 0.0.0.0 = all interfaces). Set to a LAN IP to avoid VPN/utun interfaces interfering with mDNS discovery.")
	grpcAddr := fs.String("grpc-addr", "", "gRPC server address")
	verbose := fs.Bool("verbose", false, "Enable verbose logging")
	cfgPath := fs.String("config", "", "Path to moltbook.toml (default: auto-detect)")
	fs.Parse(args)

	cfg, err := config.Load(*cfgPath)
	if err != nil {
		return fmt.Errorf("load config: %w", err)
	}

	// CLI flags override config file
	if *dataDir != "" {
		cfg.Daemon.DataDir = *dataDir
	}
	if *port != "" {
		cfg.Network.Port = *port
	}
	if *listenHost != "" {
		cfg.Network.ListenHost = *listenHost
	}
	if *grpcAddr != "" {
		cfg.Daemon.GRPCAddr = *grpcAddr
	}
	if *verbose {
		cfg.Daemon.Verbose = true
	}

	dir, err := resolveDataDir(cfg.Daemon.DataDir)
	if err != nil {
		return err
	}
	cfg.Daemon.DataDir = dir

	return runDaemon(cfg)
}

func cmdVersion() {
	fmt.Printf("MoltMesh Daemon version %s\n", version)
}

// cmdInit scaffolds a new agent: creates the data directory, generates an
// identity if one doesn't already exist, and writes a starter moltbook.toml
// if one doesn't already exist at the target path. Safe to re-run; existing
// identity/config files are left untouched.
func cmdInit(args []string) error {
	fs := flag.NewFlagSet("init", flag.ExitOnError)
	dataDir := fs.String("data-dir", "", "Data directory (default: ~/.moltmesh)")
	name := fs.String("name", "", "Agent name to claim on the network (optional)")
	description := fs.String("description", "", "Agent description (optional)")
	capabilities := fs.String("capabilities", "", "Comma-separated capabilities to advertise (optional)")
	port := fs.String("port", "0", "libp2p TCP/UDP port (default: 0 = OS-assigned)")
	grpcAddr := fs.String("grpc-addr", "", "gRPC listen address, e.g. 127.0.0.1:21500 (default: unix socket in data-dir)")
	cfgPath := fs.String("config", "", "Path to write moltbook.toml (default: <data-dir>/moltbook.toml)")
	force := fs.Bool("force", false, "Overwrite an existing identity/config")
	fs.Parse(args)

	var capList []string
	for _, c := range strings.Split(*capabilities, ",") {
		c = strings.TrimSpace(c)
		if c != "" {
			capList = append(capList, c)
		}
	}

	dir, err := resolveDataDir(*dataDir)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(dir, 0700); err != nil {
		return fmt.Errorf("create data dir: %w", err)
	}

	idPath := filepath.Join(dir, "identity.json")
	var id *identity.Identity
	identityCreated := false
	if _, statErr := os.Stat(idPath); statErr == nil && !*force {
		id, err = identity.Load(idPath)
		if err != nil {
			return fmt.Errorf("load existing identity: %w", err)
		}
	} else {
		id, err = identity.Generate()
		if err != nil {
			return fmt.Errorf("generate identity: %w", err)
		}
		if err := id.Save(idPath); err != nil {
			return fmt.Errorf("save identity: %w", err)
		}
		identityCreated = true
	}

	// Apply sensible defaults for anything the caller didn't specify, so a
	// bare `init` never leaves name/description/capabilities blank.
	if *name == "" {
		*name = defaultAgentName(id.DID)
	}
	if *description == "" {
		*description = "MoltMesh agent"
	}
	if len(capList) == 0 {
		capList = []string{capability.TaskOrchestrate}
	}

	if *cfgPath == "" {
		*cfgPath = filepath.Join(dir, "moltbook.toml")
	}
	configCreated := false
	if _, statErr := os.Stat(*cfgPath); statErr != nil || *force {
		var b strings.Builder
		fmt.Fprintf(&b, "# Generated by `moltmesh-daemon init`.\n")
		fmt.Fprintf(&b, "# did: %s (static — derived from %s, do not edit here)\n\n", id.DID, idPath)

		fmt.Fprintf(&b, "[agent]\n")
		fmt.Fprintf(&b, "# Human-readable name to claim on the network.\n")
		fmt.Fprintf(&b, "name = %q\n", *name)
		fmt.Fprintf(&b, "# Short description shown in agent card discovery.\n")
		fmt.Fprintf(&b, "description = %q\n", *description)
		fmt.Fprintf(&b, "# Capabilities advertised to the network. Short names are normalized\n")
		fmt.Fprintf(&b, "# to \"a2a:v1:cap:<name>\" automatically. Well-known capabilities:\n")
		fmt.Fprintf(&b, "#   text-generation, code-execution, image-analysis, file-processing,\n")
		fmt.Fprintf(&b, "#   data-retrieval, task-orchestration, voice-synthesis, search\n")
		fmt.Fprintf(&b, "capabilities = [%s]\n", quotedCSV(capList))
		fmt.Fprintf(&b, "# Examples:\n")
		fmt.Fprintf(&b, "# capabilities = [\"text-generation\", \"code-execution\"]\n")
		fmt.Fprintf(&b, "# capabilities = [\"image-analysis\", \"file-processing\", \"search\"]\n\n")

		fmt.Fprintf(&b, "[network]\n")
		fmt.Fprintf(&b, "# libp2p TCP/UDP port. \"0\" = OS-assigned (auto-picks a free port).\n")
		fmt.Fprintf(&b, "port = %q\n\n", *port)

		fmt.Fprintf(&b, "[daemon]\n")
		fmt.Fprintf(&b, "# Directory for identity.json, databases, and the gRPC socket.\n")
		fmt.Fprintf(&b, "data_dir = %q\n", dir)
		fmt.Fprintf(&b, "# gRPC listen address. Empty = unix socket inside data_dir.\n")
		fmt.Fprintf(&b, "# Set a host:port (e.g. \"127.0.0.1:21500\") to listen on TCP instead —\n")
		fmt.Fprintf(&b, "# the daemon auto-increments the port if it's already taken, and\n")
		fmt.Fprintf(&b, "# records the address it actually bound to in <data_dir>/grpc-addr.\n")
		fmt.Fprintf(&b, "grpc_addr = %q\n", *grpcAddr)

		if err := os.WriteFile(*cfgPath, []byte(b.String()), 0600); err != nil {
			return fmt.Errorf("write moltbook.toml: %w", err)
		}
		configCreated = true
	}

	if jsonMode {
		jsonOut(map[string]interface{}{
			"data_dir":         dir,
			"did":              id.DID,
			"name":             *name,
			"capabilities":     capList,
			"port":             *port,
			"grpc_addr":        *grpcAddr,
			"identity_created": identityCreated,
			"config_path":      *cfgPath,
			"config_created":   configCreated,
		})
		return nil
	}

	fmt.Printf("data dir:  %s\n", dir)
	fmt.Printf("did:       %s\n", id.DID)
	if identityCreated {
		fmt.Println("identity:  generated")
	} else {
		fmt.Println("identity:  already exists (unchanged)")
	}
	if configCreated {
		fmt.Printf("config:    written to %s\n", *cfgPath)
	} else {
		fmt.Printf("config:    already exists at %s (unchanged)\n", *cfgPath)
	}
	fmt.Println("\nStart the daemon with:")
	fmt.Printf("  moltmesh-daemon start --config %s\n", *cfgPath)
	return nil
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

	if jsonMode {
		jsonOut(map[string]string{"did": id.DID})
		return nil
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

	if jsonMode {
		jsonOut(config)
		return nil
	}
	data, err := json.MarshalIndent(config, "", "  ")
	if err != nil {
		return err
	}
	fmt.Println(string(data))
	return nil
}

func agentCardFromConfig(cfg *config.Config) *pb.AgentCard {
	card := &pb.AgentCard{
		Name:        names.Normalize(cfg.Agent.Name),
		Description: cfg.Agent.Description,
	}
	for _, cap := range cfg.Agent.Capabilities {
		card.Skills = append(card.Skills, &pb.Skill{Id: cap})
	}
	return card
}

func runDaemon(cfg *config.Config) error {
	var log *zap.Logger
	if cfg.Daemon.Verbose && !jsonMode {
		log, _ = zap.NewDevelopment()
	} else {
		log, _ = zap.NewProduction()
	}
	defer log.Sync()

	log.Info("MoltMesh Daemon starting", zap.String("version", version))

	if err := os.MkdirAll(cfg.Daemon.DataDir, 0700); err != nil {
		return fmt.Errorf("create data dir: %w", err)
	}
	log.Info("data directory ready", zap.String("path", cfg.Daemon.DataDir))

	return run(cfg, log)
}

func run(cfg *config.Config, log *zap.Logger) error {
	dataDir := cfg.Daemon.DataDir
	port := cfg.Network.Port
	listenHost := cfg.Network.ListenHost
	if listenHost == "" {
		listenHost = "0.0.0.0"
	}
	grpcAddr := cfg.Daemon.GRPCAddr
	_ = port // used below
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
	listenAddrs := []string{
		fmt.Sprintf("/ip4/%s/udp/0/quic-v1", listenHost),
		fmt.Sprintf("/ip4/%s/tcp/0", listenHost),
	}
	if port != "" {
		listenAddrs = []string{
			fmt.Sprintf("/ip4/%s/udp/%s/quic-v1", listenHost, port),
			fmt.Sprintf("/ip4/%s/tcp/%s", listenHost, port),
		}
	}

	n, err := node.New(ctx, id, node.Config{
		ListenAddrs:    listenAddrs,
		BootstrapPeers: cfg.Network.BootstrapPeers,
		IPFSBootstrap:  cfg.IPFSBootstrapEnabled(),
		DataDir:        dataDir,
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
	actorSystem, err := actors.NewSystem(ctx, log)
	if err != nil {
		return fmt.Errorf("actor system: %w", err)
	}
	hierarchy, err := actorSystem.NewHierarchy(ctx)
	if err != nil {
		return err
	}
	if err := ib.EnableActor(ctx, hierarchy); err != nil {
		return err
	}
	if err := ts.EnableActor(ctx, hierarchy); err != nil {
		return err
	}

	// ── registry ────────────────────────────────────────────────────────────
	reg := registry.New(n.DHT, id, log)
	if err := reg.EnableActor(ctx, hierarchy); err != nil {
		return err
	}

	// ── name registry ────────────────────────────────────────────────────────
	nameReg := names.New(n.DHT, id, log)
	if err := nameReg.EnableActor(ctx, hierarchy); err != nil {
		return err
	}
	if cfg.Agent.Name != "" {
		claimCtx, claimCancel := context.WithTimeout(ctx, 15*time.Second)
		if _, err := nameReg.Claim(claimCtx, cfg.Agent.Name); err != nil {
			log.Warn("name claim failed", zap.String("name", cfg.Agent.Name), zap.Error(err))
		}
		claimCancel()
	}

	// Auto-publish agent card from config
	if cfg.Agent.Name != "" || cfg.Agent.Description != "" || len(cfg.Agent.Capabilities) > 0 {
		card := agentCardFromConfig(cfg)
		card.Multiaddrs = n.P2PAddrs()
		cardCtx, cardCancel := context.WithTimeout(ctx, 15*time.Second)
		if err := reg.Publish(cardCtx, card); err != nil {
			log.Warn("auto-publish agent card", zap.Error(err))
		}
		cardCancel()
		for _, cap := range cfg.Agent.Capabilities {
			capCtx, capCancel := context.WithTimeout(ctx, 10*time.Second)
			if err := reg.AdvertiseCapability(capCtx, cap); err != nil {
				log.Warn("advertise capability", zap.String("cap", cap), zap.Error(err))
			}
			capCancel()
		}
	}

	// ── gossip ──────────────────────────────────────────────────────────────
	gm := gossip.New(n.PubSub, log)
	if err := gm.EnableActor(ctx, hierarchy); err != nil {
		return err
	}

	// ── thread manager (GoAkt actor path — see ADR-0015) ─────────────────────
	threadStore, err := thread.NewStore(filepath.Join(dataDir, "threads.db"))
	if err != nil {
		return fmt.Errorf("thread store: %w", err)
	}
	defer threadStore.Close()

	// Publishes committed blocks to Bitswap/DHT so threads stay fetchable
	// after every replica goes offline (ADR-0015's "stays on the network").
	durability := thread.NewPublisher(n.DHT, n.Blockstore, n.Bitswap, log)
	if err := durability.EnableActor(ctx, hierarchy); err != nil {
		return err
	}
	// Refresh verified content-addressed history in the background.  This gives
	// every daemon a real archive-provider role after restart without trusting
	// a remote provider's claimed history.
	go thread.NewArchiveWorker(n.DHT, n.Host, n.PubSub, n.Blockstore, n.Bitswap, threadStore, id, log).Run(ctx, time.Minute)

	tm := thread.NewActorManager(ctx, actorSystem, threadStore, id, n.PubSub, durability, log)
	tm.SetPassivationAfter(cfg.ThreadPassivationAfter())
	if err := tm.UseHierarchy(hierarchy); err != nil {
		return err
	}
	if err := tm.StartAll(ctx); err != nil {
		log.Warn("thread: start all on boot", zap.Error(err))
	}

	// ── delivery layer (libp2p stream protocol) ──────────────────────────────
	// Blob transport is now handled by Bitswap (mounted on n.Host).
	dlv := deliver.New(n.Host, reg, ib, tm, log)
	if err := dlv.EnableActor(hierarchy); err != nil {
		return err
	}

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
	if err := ob.EnableActor(ctx, hierarchy); err != nil {
		return err
	}

	// ── network store + manager ───────────────────────────────────────────────
	netStore, err := network.New(filepath.Join(dataDir, "networks.db"))
	if err != nil {
		return fmt.Errorf("network store: %w", err)
	}
	defer netStore.Close()
	if err := netStore.EnableActor(ctx, hierarchy); err != nil {
		return err
	}
	nm := network.NewManager(netStore, gm)

	// ── webhook dispatcher ────────────────────────────────────────────────────
	wh := webhook.New(log)
	if err := wh.EnableActor(ctx, hierarchy); err != nil {
		return err
	}
	defer actorSystem.Stop(context.Background())

	// ── gRPC server ─────────────────────────────────────────────────────────
	if grpcAddr == "" {
		grpcAddr = filepath.Join(dataDir, "a2a.sock")
	}

	rpc.SetVersion(version)
	srv := rpc.New(id, ib, ob, ts, reg, gm, dlv, tm, nm, wh, nameReg, n, n.P2PAddrs(), log)
	dlv.SetMessageHandler(srv.HandleIncoming)
	grpcServer := grpc.NewServer()
	pb.RegisterA2ANodeServer(grpcServer, srv)

	var lis net.Listener
	if grpcAddr[0] == '/' {
		os.Remove(grpcAddr)
		lis, err = net.Listen("unix", grpcAddr)
	} else {
		lis, grpcAddr, err = listenTCPFromPort(grpcAddr)
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

	// ── write PID + resolved gRPC address files ────────────────────────────────
	pidFile := filepath.Join(dataDir, "daemon.pid")
	if err := os.WriteFile(pidFile, []byte(fmt.Sprintf("%d\n", os.Getpid())), 0600); err != nil {
		log.Warn("could not write PID file", zap.Error(err))
	}
	defer os.Remove(pidFile)

	addrFile := filepath.Join(dataDir, "grpc-addr")
	if err := os.WriteFile(addrFile, []byte(grpcAddr+"\n"), 0600); err != nil {
		log.Warn("could not write gRPC address file", zap.Error(err))
	}
	defer os.Remove(addrFile)

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

	if jsonMode {
		jsonOut(map[string]interface{}{
			"status":    "running",
			"did":       id.Did,
			"addresses": id.Multiaddrs,
		})
		return nil
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

	info := map[string]interface{}{
		"did":        id.Did,
		"public_key": id.PublicKey,
		"addresses":  id.Multiaddrs,
	}
	if jsonMode {
		jsonOut(info)
		return nil
	}
	data, _ := json.MarshalIndent(info, "", "  ")
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
					if jsonMode {
						jsonOut(map[string]interface{}{"pid": pid, "signaled": true})
						return nil
					}
					fmt.Printf("Sent SIGTERM to daemon (PID %d)\n", pid)
					return nil
				}
			}
		}
	}

	if jsonMode {
		jsonErr("pid_unavailable", "could not read PID file; stop the daemon manually with: pkill -f 'moltmesh-daemon start'")
		return nil
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
