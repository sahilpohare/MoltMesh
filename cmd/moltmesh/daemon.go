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
	"github.com/sahilpohare/p2p-a2a/pkg/config"
	"github.com/sahilpohare/p2p-a2a/pkg/format"
)

// defaultDataDir returns the default data directory.
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
	dataDir  := fs.String("data-dir",  "", "Data directory")
	port     := fs.String("port",      "", "Network port")
	grpcAddr := fs.String("grpc-addr", "", "gRPC server address")
	verbose  := fs.Bool("verbose",  false, "Enable verbose logging")
	cfgPath  := fs.String("config",    "", "Path to moltbook.toml")
	fs.Parse(args)

	cfg, err := config.Load(*cfgPath)
	if err != nil {
		return fmt.Errorf("load config: %w", err)
	}

	if *dataDir  != "" { cfg.Daemon.DataDir  = *dataDir }
	if *port     != "" { cfg.Network.Port     = *port }
	if *grpcAddr != "" { cfg.Daemon.GRPCAddr  = *grpcAddr }
	if *verbose           { cfg.Daemon.Verbose  = true }

	dir, err := resolveDataDir(cfg.Daemon.DataDir)
	if err != nil {
		return err
	}
	cfg.Daemon.DataDir = dir

	return runDaemon(cfg)
}

func cmdVersion() {
	fmt.Printf("MoltMesh version %s\n", version)
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
			return fmt.Errorf("identity not found; run 'moltmesh start' first")
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

	cfg := map[string]interface{}{
		"data_dir":  dir,
		"identity":  filepath.Join(dir, "identity.json"),
		"grpc_sock": filepath.Join(dir, "a2a.sock"),
		"databases": map[string]string{
			"inbox":   filepath.Join(dir, "inbox.db"),
			"tasks":   filepath.Join(dir, "tasks.db"),
			"threads": filepath.Join(dir, "threads.db"),
		},
	}

	data, err := json.MarshalIndent(cfg, "", "  ")
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
	if cfg.Daemon.Verbose {
		log, _ = zap.NewDevelopment()
	} else {
		log, _ = zap.NewProduction()
	}
	defer log.Sync()

	log.Info("MoltMesh starting", zap.String("version", version))

	if err := os.MkdirAll(cfg.Daemon.DataDir, 0700); err != nil {
		return fmt.Errorf("create data dir: %w", err)
	}

	return run(cfg, log)
}

func run(cfg *config.Config, log *zap.Logger) error {
	dataDir  := cfg.Daemon.DataDir
	port     := cfg.Network.Port
	grpcAddr := cfg.Daemon.GRPCAddr
	_ = port

	// ── identity ─────────────────────────────────────────────────────────────
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

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// ── libp2p node ──────────────────────────────────────────────────────────
	listenAddrs := []string{"/ip4/0.0.0.0/udp/0/quic-v1", "/ip4/0.0.0.0/tcp/0"}
	if port != "" {
		listenAddrs = []string{
			fmt.Sprintf("/ip4/0.0.0.0/udp/%s/quic-v1", port),
			fmt.Sprintf("/ip4/0.0.0.0/tcp/%s", port),
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

	// ── storage ──────────────────────────────────────────────────────────────
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

	// ── registry ─────────────────────────────────────────────────────────────
	reg := registry.New(n.DHT, id, log)
	go reg.RunRepublish(ctx)

	// ── name registry ─────────────────────────────────────────────────────────
	nameReg := names.New(n.DHT, id, log)
	if cfg.Agent.Name != "" {
		claimCtx, claimCancel := context.WithTimeout(ctx, 15*time.Second)
		if _, err := nameReg.Claim(claimCtx, cfg.Agent.Name); err != nil {
			log.Warn("name claim failed", zap.String("name", cfg.Agent.Name), zap.Error(err))
		}
		claimCancel()
		go nameReg.RunRepublish(ctx)
	}

	if cfg.Agent.Name != "" || cfg.Agent.Description != "" || len(cfg.Agent.Capabilities) > 0 {
		card := agentCardFromConfig(cfg)
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

	// ── gossip ───────────────────────────────────────────────────────────────
	gm := gossip.New(n.PubSub, log)

	// ── thread manager ────────────────────────────────────────────────────────
	threadStore, err := thread.NewStore(filepath.Join(dataDir, "threads.db"))
	if err != nil {
		return fmt.Errorf("thread store: %w", err)
	}
	defer threadStore.Close()

	tm := thread.NewManager(ctx, threadStore, id, n.PubSub, log)
	if err := tm.StartAll(); err != nil {
		log.Warn("thread: start all on boot", zap.Error(err))
	}

	// ── delivery ─────────────────────────────────────────────────────────────
	dlv := deliver.New(n.Host, reg, ib, tm, log)

	// ── outbox ───────────────────────────────────────────────────────────────
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

	// ── network manager ───────────────────────────────────────────────────────
	netStore, err := network.New(filepath.Join(dataDir, "networks.db"))
	if err != nil {
		return fmt.Errorf("network store: %w", err)
	}
	defer netStore.Close()
	nm := network.NewManager(netStore, gm)

	// ── webhook ───────────────────────────────────────────────────────────────
	wh := webhook.New(log)

	// ── gRPC server ───────────────────────────────────────────────────────────
	if grpcAddr == "" {
		grpcAddr = filepath.Join(dataDir, "a2a.sock")
	}

	rpc.SetVersion(version)
	srv := rpc.New(id, ib, ob, ts, reg, gm, dlv, tm, nm, wh, nameReg, n, n.P2PAddrs(), log)
	grpcServer := grpc.NewServer()
	pb.RegisterA2ANodeServer(grpcServer, srv)

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

	// ── PID file ──────────────────────────────────────────────────────────────
	pidFile := filepath.Join(dataDir, "daemon.pid")
	if err := os.WriteFile(pidFile, []byte(fmt.Sprintf("%d\n", os.Getpid())), 0600); err != nil {
		log.Warn("could not write PID file", zap.Error(err))
	}
	defer os.Remove(pidFile)

	// ── shutdown ──────────────────────────────────────────────────────────────
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

	if _, err := client.GetIdentity(context.Background(), &pb.Empty{}); err != nil {
		return fmt.Errorf("failed to contact daemon: %w", err)
	}

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
	fmt.Println("  pkill -f 'moltmesh start'")
	return nil
}

// dialClient parses --data-dir and --grpc-addr, then connects.
func dialClient(args []string, cmdName string) (*grpc.ClientConn, pb.A2ANodeClient, string, error) {
	fs := flag.NewFlagSet(cmdName, flag.ExitOnError)
	dataDir  := fs.String("data-dir",  "", "Data directory")
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
