package node

import (
	"context"
	"fmt"
	"time"

	"github.com/ipfs/boxo/bitswap"
	bsnet "github.com/ipfs/boxo/bitswap/network/bsnet"
	"github.com/ipfs/boxo/blockstore"
	flatfs "github.com/ipfs/go-ds-flatfs"
	dht "github.com/libp2p/go-libp2p-kad-dht"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/routing"
	"github.com/libp2p/go-libp2p/p2p/net/connmgr"
	"github.com/libp2p/go-libp2p/p2p/transport/tcp"
	libp2pquic "github.com/libp2p/go-libp2p/p2p/transport/quic"
	"github.com/multiformats/go-multiaddr"
	"go.uber.org/zap"

	"github.com/sahilpohare/p2p-a2a/daemon/identity"
)

// Config holds node configuration.
type Config struct {
	ListenAddrs    []string
	BootstrapPeers []string // explicit bootstrap peers (multiaddr strings)
	IPFSBootstrap  bool     // if true, also use IPFS public bootstrap peers (default true)
	DataDir        string
	ConnLowWater   int // min connections before pruning (default 50)
	ConnHighWater  int // max connections before pruning (default 200)
}

// Node wraps a libp2p host with DHT, GossipSub, and IPFS Bitswap.
type Node struct {
	Host       host.Host
	DHT        *dht.IpfsDHT
	PubSub     *pubsub.PubSub
	Identity   *identity.Identity
	Blockstore blockstore.Blockstore
	Bitswap    *bitswap.Bitswap
	log        *zap.Logger
}

// New creates and starts a libp2p node backed by the global IPFS DHT and Bitswap.
func New(ctx context.Context, id *identity.Identity, cfg Config, log *zap.Logger) (*Node, error) {
	listenAddrs := make([]multiaddr.Multiaddr, 0, len(cfg.ListenAddrs))
	for _, addr := range cfg.ListenAddrs {
		ma, err := multiaddr.NewMultiaddr(addr)
		if err != nil {
			return nil, fmt.Errorf("invalid listen addr %q: %w", addr, err)
		}
		listenAddrs = append(listenAddrs, ma)
	}

	lo, hi := cfg.ConnLowWater, cfg.ConnHighWater
	if lo <= 0 {
		lo = 50
	}
	if hi <= 0 {
		hi = 200
	}
	connMgr, err := connmgr.NewConnManager(lo, hi, connmgr.WithGracePeriod(30*time.Second))
	if err != nil {
		return nil, fmt.Errorf("conn manager: %w", err)
	}

	var kadDHT *dht.IpfsDHT

	h, err := libp2p.New(
		libp2p.Identity(id.LibP2PKey),
		libp2p.ListenAddrs(listenAddrs...),
		libp2p.Transport(libp2pquic.NewTransport),
		libp2p.Transport(tcp.NewTCPTransport),
		libp2p.ConnectionManager(connMgr),
		libp2p.NATPortMap(),
		libp2p.EnableNATService(),
		libp2p.EnableHolePunching(),
		libp2p.Routing(func(h host.Host) (routing.PeerRouting, error) {
			var err error
			// No ProtocolPrefix — joins the global IPFS Kademlia DHT.
			kadDHT, err = dht.New(ctx, h,
				dht.Mode(dht.ModeAutoServer),
			)
			return kadDHT, err
		}),
	)
	if err != nil {
		return nil, fmt.Errorf("create libp2p host: %w", err)
	}

	// ── GossipSub ─────────────────────────────────────────────────────────────
	ps, err := pubsub.NewGossipSub(ctx, h,
		pubsub.WithMessageSignaturePolicy(pubsub.StrictSign),
	)
	if err != nil {
		h.Close()
		return nil, fmt.Errorf("create gossipsub: %w", err)
	}

	// ── Flatfs blockstore ─────────────────────────────────────────────────────
	// CreateOrOpen handles first-run (creates dir + shard config) and reopens on restart.
	// IPFS_DEF_SHARD = NextToLast(2) — same sharding as Kubo.
	blocksDir := cfg.DataDir + "/blocks"
	fds, err := flatfs.CreateOrOpen(blocksDir, flatfs.IPFS_DEF_SHARD, false)
	if err != nil {
		h.Close()
		return nil, fmt.Errorf("open flatfs blockstore at %q: %w", blocksDir, err)
	}
	bs := blockstore.NewBlockstore(fds)

	// ── Bitswap ───────────────────────────────────────────────────────────────
	// kadDHT implements routing.ContentDiscovery — used for provider routing.
	bswapNet := bsnet.NewFromIpfsHost(h)
	bswap := bitswap.New(ctx, bswapNet, kadDHT, bs)

	n := &Node{
		Host:       h,
		DHT:        kadDHT,
		PubSub:     ps,
		Identity:   id,
		Blockstore: bs,
		Bitswap:    bswap,
		log:        log,
	}

	if err := n.bootstrap(ctx, cfg); err != nil {
		log.Warn("bootstrap incomplete", zap.Error(err))
	}

	log.Info("node started",
		zap.String("peer_id", h.ID().String()),
		zap.String("did", id.DID),
		zap.Strings("addrs", addrsToStrings(h.Addrs())),
	)

	return n, nil
}

// Close shuts down the node.
func (n *Node) Close() error {
	n.Bitswap.Close()
	if err := n.DHT.Close(); err != nil {
		n.log.Warn("dht close error", zap.Error(err))
	}
	return n.Host.Close()
}

// PeerID returns the libp2p peer ID as a string.
func (n *Node) PeerID() string { return n.Host.ID().String() }

// Addrs returns the node's listen multiaddrs.
func (n *Node) Addrs() []string { return addrsToStrings(n.Host.Addrs()) }

// ─── internal ─────────────────────────────────────────────────────────────────

func (n *Node) bootstrap(ctx context.Context, cfg Config) error {
	var bootstrapPeers []peer.AddrInfo

	if cfg.IPFSBootstrap {
		for _, ma := range dht.DefaultBootstrapPeers {
			ai, err := peer.AddrInfoFromP2pAddr(ma)
			if err != nil {
				n.log.Debug("skip ipfs bootstrap peer", zap.Error(err))
				continue
			}
			bootstrapPeers = append(bootstrapPeers, *ai)
		}
	}

	for _, p := range cfg.BootstrapPeers {
		ma, err := multiaddr.NewMultiaddr(p)
		if err != nil {
			n.log.Warn("invalid bootstrap peer", zap.String("addr", p), zap.Error(err))
			continue
		}
		ai, err := peer.AddrInfoFromP2pAddr(ma)
		if err != nil {
			n.log.Warn("parse bootstrap peer", zap.Error(err))
			continue
		}
		bootstrapPeers = append(bootstrapPeers, *ai)
	}

	connected := 0
	for _, pi := range bootstrapPeers {
		bCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
		if err := n.Host.Connect(bCtx, pi); err != nil {
			n.log.Debug("bootstrap peer unreachable", zap.String("peer", pi.ID.String()), zap.Error(err))
		} else {
			connected++
		}
		cancel()
	}

	if connected == 0 {
		return fmt.Errorf("could not connect to any bootstrap peer")
	}

	return n.DHT.Bootstrap(ctx)
}

func addrsToStrings(addrs []multiaddr.Multiaddr) []string {
	out := make([]string, len(addrs))
	for i, a := range addrs {
		out[i] = a.String()
	}
	return out
}
