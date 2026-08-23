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

// cliCommands is the single source of truth for moltmesh's top-level
// commands: main() dispatches through it, and completion.go's allCommands
// (and the bash/fish completion scripts) are generated from it, so the
// dispatch switch and shell completions cannot drift the way the old
// hand-maintained duplicate lists did (recover-thread was missing from all
// three shell completions before this). A function, not a package-level
// var: cmdCompletion (listed below) reads it back through allCommands(),
// and a var initializer here would create an initialization cycle.
type cliCommand struct {
	name string
	run  func([]string) error
}

func cliCommands() []cliCommand {
	return []cliCommand{
		// ── unified commands ────────────────────────────────────────────────────
		{"start", cmdStartBackground},
		{"init", cmdInit},
		{"tui", cmdTUI},
		{"version", func([]string) error { cmdVersion(); return nil }},
		{"help", func([]string) error { printUsage(); return nil }},

		// ── daemon management ───────────────────────────────────────────────────
		{"status", cmdStatus},
		{"info", cmdInfo},
		{"identity", cmdIdentity},
		{"config", cmdConfig},
		{"stop", cmdStop},

		// ── identity & registry ─────────────────────────────────────────────────
		{"get-identity", cmdGetIdentity},
		{"get-agent-card", cmdGetAgentCard},
		{"publish-agent-card", cmdPublishAgentCard},
		{"find-agents", cmdFindAgents},

		// ── messaging ───────────────────────────────────────────────────────────
		{"send-message", cmdSendMessage},
		{"subscribe-inbox", cmdSubscribeInbox},
		{"get-inbox", cmdGetInbox},
		{"get-outbox", cmdGetOutbox},
		{"ack-message", cmdAckMessage},

		// ── tasks ───────────────────────────────────────────────────────────────
		{"create-task", cmdCreateTask},
		{"send-task-result", cmdSendTaskResult},
		{"get-task", cmdGetTask},
		{"update-task", cmdUpdateTask},
		{"cancel-task", cmdCancelTask},
		{"publish-task-event", cmdPublishTaskEvent},
		{"subscribe-task-events", cmdSubscribeTaskEvents},

		// ── files ───────────────────────────────────────────────────────────────
		{"send-file", cmdSendFile},
		{"fetch-file", cmdFetchFile},

		// ── threads ─────────────────────────────────────────────────────────────
		{"create-thread", cmdCreateThread},
		{"get-thread", cmdGetThread},
		{"append-entry", cmdAppendEntry},
		{"get-thread-entries", cmdGetThreadEntries},
		{"subscribe-thread", cmdSubscribeThread},
		{"add-thread-replica", cmdAddThreadReplica},
		{"recover-thread", cmdRecoverThread},

		// ── diagnostics ─────────────────────────────────────────────────────────
		{"ping", cmdPing},
		{"health", cmdHealth},
		{"peers", cmdPeers},

		// ── format utilities ────────────────────────────────────────────────────
		{"format", cmdFormat},

		// ── shell completion ─────────────────────────────────────────────────────
		{"completion", cmdCompletion},

		// ── pubsub ──────────────────────────────────────────────────────────────
		{"publish", cmdPublish},
		{"subscribe-topic", cmdSubscribeTopic},

		// ── webhook ─────────────────────────────────────────────────────────────
		{"set-webhook", cmdSetWebhook},
		{"clear-webhook", cmdClearWebhook},
		{"get-webhook", cmdGetWebhook},

		// ── networks ────────────────────────────────────────────────────────────
		{"network", cmdNetwork},

		// ── names ───────────────────────────────────────────────────────────────
		{"name", cmdName},
	}
}

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

	if args[0] == "-h" || args[0] == "--help" {
		printUsage()
		return
	}

	var run func([]string) error
	for _, c := range cliCommands() {
		if c.name == args[0] {
			run = c.run
			break
		}
	}
	if run == nil {
		fmt.Fprintf(os.Stderr, "unknown command: %s\n\n", args[0])
		printUsage()
		os.Exit(1)
	}

	if err := run(args[1:]); err != nil {
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
  init         Scaffold a new agent (identity + moltbook.toml)
  tui          Open interactive TUI
  stop         Stop running daemon
  status       Check daemon status
  version      Show version
  help         Show this help
  completion   Generate shell completion (bash|zsh|fish)

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
  send-task-result        Send a terminal result to a task's initiator (--to, --task-id)
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
  create-thread           Create a thread (--with-recovery returns a recovery handle)
  get-thread              Get thread info (--id)
  append-entry            Append entry (--thread-id, --payload)
  get-thread-entries      List entries (--id)
  subscribe-thread        Stream entries (--id)
  add-thread-replica      Add an observer DID as a replica (--thread-id, --did)
  recover-thread          Recover verified history (--id, --secret-base64)

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
