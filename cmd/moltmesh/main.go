// cmd/moltmesh/main.go — unified MoltMesh binary
//
// moltmesh start          → launch daemon in background (detached)
// moltmesh tui            → open the interactive TUI
// moltmesh <any command>  → all daemon CLI commands (send-message, peers, etc.)
//
// Background start uses a re-exec pattern: the binary detects
// __DAEMON_CHILD=1 in env and runs the daemon in the foreground.

package main

import (
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"syscall"
	"time"
)

const version = "0.1.0"

func main() {
	// ── daemon child mode ────────────────────────────────────────────────────
	// When we re-exec ourselves with __DAEMON_CHILD=1 we just run the daemon.
	if os.Getenv("__DAEMON_CHILD") == "1" {
		if err := runDaemonChild(os.Args[1:]); err != nil {
			fmt.Fprintln(os.Stderr, "daemon error:", err)
			os.Exit(1)
		}
		return
	}

	// ── strip global --json flag ─────────────────────────────────────────────
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
	// ── unified commands ────────────────────────────────────────────────────
	case "start":
		err = cmdStartBackground(args[1:])
	case "tui":
		err = cmdTUI(args[1:])
	case "version":
		cmdVersion()
	case "help", "-h", "--help":
		printUsage()

	// ── daemon management ───────────────────────────────────────────────────
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

	// ── identity & registry ─────────────────────────────────────────────────
	case "get-identity":
		err = cmdGetIdentity(args[1:])
	case "get-agent-card":
		err = cmdGetAgentCard(args[1:])
	case "publish-agent-card":
		err = cmdPublishAgentCard(args[1:])
	case "find-agents":
		err = cmdFindAgents(args[1:])

	// ── messaging ───────────────────────────────────────────────────────────
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

	// ── tasks ───────────────────────────────────────────────────────────────
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

	// ── files ───────────────────────────────────────────────────────────────
	case "send-file":
		err = cmdSendFile(args[1:])
	case "fetch-file":
		err = cmdFetchFile(args[1:])

	// ── threads ─────────────────────────────────────────────────────────────
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

	// ── diagnostics ─────────────────────────────────────────────────────────
	case "ping":
		err = cmdPing(args[1:])
	case "health":
		err = cmdHealth(args[1:])
	case "peers":
		err = cmdPeers(args[1:])

	// ── format utilities ────────────────────────────────────────────────────
	case "format":
		err = cmdFormat(args[1:])

	// ── pubsub ──────────────────────────────────────────────────────────────
	case "publish":
		err = cmdPublish(args[1:])
	case "subscribe-topic":
		err = cmdSubscribeTopic(args[1:])

	// ── webhook ─────────────────────────────────────────────────────────────
	case "set-webhook":
		err = cmdSetWebhook(args[1:])
	case "clear-webhook":
		err = cmdClearWebhook(args[1:])
	case "get-webhook":
		err = cmdGetWebhook(args[1:])

	// ── networks ────────────────────────────────────────────────────────────
	case "network":
		err = cmdNetwork(args[1:])

	// ── names ───────────────────────────────────────────────────────────────
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

// cmdStartBackground forks the current binary as a background daemon.
// It re-execs itself with __DAEMON_CHILD=1 and the same start args,
// waits up to 10 s for the gRPC socket to appear, then returns.
func cmdStartBackground(args []string) error {
	// Resolve data-dir / grpc-addr early so we know what socket to wait for.
	dataDir, grpcAddr := resolveStartFlags(args)

	// Check if daemon is already running.
	if isSocketLive(grpcAddr) {
		fmt.Printf("daemon already running at %s\n", grpcAddr)
		return nil
	}

	exe, err := os.Executable()
	if err != nil {
		return fmt.Errorf("resolve executable: %w", err)
	}

	// Build child args: "start" + original args
	childArgs := append([]string{"start"}, args...)

	// Open a log file for the daemon's stderr.
	if err := os.MkdirAll(dataDir, 0700); err != nil {
		return fmt.Errorf("create data dir: %w", err)
	}
	logPath := filepath.Join(dataDir, "daemon.log")
	logFile, err := os.OpenFile(logPath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0600)
	if err != nil {
		return fmt.Errorf("open daemon log: %w", err)
	}

	cmd := exec.Command(exe, childArgs...)
	cmd.Env = append(os.Environ(), "__DAEMON_CHILD=1")
	cmd.Stdin = nil
	cmd.Stdout = logFile
	cmd.Stderr = logFile
	cmd.SysProcAttr = &syscall.SysProcAttr{Setsid: true}

	if err := cmd.Start(); err != nil {
		logFile.Close()
		return fmt.Errorf("start daemon process: %w", err)
	}
	logFile.Close()

	pid := cmd.Process.Pid
	// Detach — we don't Wait() on this child.
	cmd.Process.Release() //nolint:errcheck

	fmt.Printf("daemon starting (PID %d)…\n", pid)
	fmt.Printf("log: %s\n", logPath)

	// Wait for gRPC socket to become available.
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		if isSocketLive(grpcAddr) {
			fmt.Printf("daemon ready at %s\n", grpcAddr)
			return nil
		}
		time.Sleep(300 * time.Millisecond)
	}

	fmt.Fprintf(os.Stderr, "warning: daemon did not become ready within 15s\n")
	fmt.Fprintf(os.Stderr, "  check log: %s\n", logPath)
	return nil
}

// runDaemonChild is called when __DAEMON_CHILD=1 — runs the daemon inline.
func runDaemonChild(args []string) error {
	// args[0] == "start"
	if len(args) > 0 && args[0] == "start" {
		return cmdStart(args[1:])
	}
	return cmdStart(args)
}

// isSocketLive returns true if the unix socket (or TCP addr) is accepting connections.
func isSocketLive(addr string) bool {
	if addr == "" {
		return false
	}
	var c net.Conn
	var err error
	if addr[0] == '/' {
		c, err = net.DialTimeout("unix", addr, 300*time.Millisecond)
	} else {
		c, err = net.DialTimeout("tcp", addr, 300*time.Millisecond)
	}
	if err != nil {
		return false
	}
	c.Close()
	return true
}

// resolveStartFlags parses --data-dir and --grpc-addr from start args
// without consuming them, returning resolved paths for readiness check.
func resolveStartFlags(args []string) (dataDir, grpcAddr string) {
	for i, a := range args {
		if a == "--data-dir" || a == "-data-dir" {
			if i+1 < len(args) {
				dataDir = args[i+1]
			}
		} else if len(a) > 11 && a[:11] == "--data-dir=" {
			dataDir = a[11:]
		}
		if a == "--grpc-addr" || a == "-grpc-addr" {
			if i+1 < len(args) {
				grpcAddr = args[i+1]
			}
		} else if len(a) > 12 && a[:12] == "--grpc-addr=" {
			grpcAddr = a[12:]
		}
	}

	var err error
	dataDir, err = resolveDataDir(dataDir)
	if err != nil {
		home, _ := os.UserHomeDir()
		dataDir = filepath.Join(home, ".moltmesh")
	}
	grpcAddr = resolveGRPCAddr(grpcAddr, dataDir)
	return
}

func printUsage() {
	fmt.Fprint(os.Stderr, `MoltMesh — unified CLI

Usage:
  moltmesh [command] [options]

Primary commands:
  start        Start daemon in background (detached)
  tui          Open interactive TUI
  stop         Stop running daemon
  status       Check daemon status
  version      Show version
  help         Show this help

Identity & Registry:
  get-identity            Get this node's identity
  get-agent-card          Get agent card for a DID (--did)
  publish-agent-card      Publish agent card (--name, --description)
  find-agents             Find agents by capability (--capability, --limit)

Messaging:
  send-message            Send a message (--to, --text)
  get-inbox               List inbox messages (--limit, --unread)
  get-outbox              List outbox messages (--status, --limit)
  subscribe-inbox         Stream incoming messages
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
  health                  Show daemon health
  ping [did]              Measure latency to a peer
  peers                   List connected peers

PubSub:
  publish                 Publish to a topic (--topic, --payload)
  subscribe-topic         Stream topic messages (--topic)

Webhook:
  set-webhook             Set webhook URL (--url, --secret)
  clear-webhook           Remove webhook configuration
  get-webhook             Show current webhook URL

Names:
  name claim <name>       Claim a human-readable name
  name resolve <name>     Resolve a name to DID

Networks:
  network create <name>   Create a named network
  network join <id>       Join a network
  network leave <id>      Leave a network
  network list            List networks you belong to
  network members <id>    List members
  network broadcast <id>  Broadcast a message

Threads:
  create-thread           Create a thread
  get-thread              Get thread info (--id)
  append-entry            Append entry (--thread-id, --payload)
  get-thread-entries      List entries (--id)
  subscribe-thread        Stream entries (--id)

Global options:
  --data-dir string    Data directory (default: ~/.moltmesh)
  --grpc-addr string   gRPC address (default: unix socket in data-dir)
  --json               Emit JSON output

Examples:
  moltmesh start
  moltmesh tui
  moltmesh send-message --to did:key:z6Mk... --text "hello"
  moltmesh health
  moltmesh peers
  moltmesh stop
`)
}
