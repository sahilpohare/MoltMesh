// Package config loads moltbook.toml — the MoltMesh node configuration file.
//
// moltbook.toml is searched in the following order:
//  1. Path given by --config flag
//  2. ./moltbook.toml (current directory)
//  3. ~/.moltmesh/moltbook.toml
//
// All fields are optional; sensible defaults are applied automatically.
package config

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/BurntSushi/toml"
)

// Config is the top-level structure of moltbook.toml.
type Config struct {
	// [agent] — who this node is
	Agent AgentConfig `toml:"agent"`

	// [network] — libp2p / DHT settings
	Network NetworkConfig `toml:"network"`

	// [daemon] — gRPC socket and data directory
	Daemon DaemonConfig `toml:"daemon"`
}

// AgentConfig describes the identity and capabilities of this node.
type AgentConfig struct {
	// Human-readable name to claim on the network (e.g. "swift-falcon").
	// Words separated by spaces, hyphens, or underscores; normalised to lowercase-hyphen.
	Name string `toml:"name"`

	// Short description shown in agent card discovery.
	Description string `toml:"description"`

	// Capabilities advertised to the network.
	// Use full IDs ("a2a:v1:cap:text-generation") or short names ("text-generation").
	Capabilities []string `toml:"capabilities"`
}

// NetworkConfig controls libp2p and DHT behaviour.
type NetworkConfig struct {
	// TCP/UDP port. "0" = OS-assigned (default).
	Port string `toml:"port"`

	// Additional bootstrap peer multiaddrs.
	// IPFS public bootstrap peers are always included unless ipfs_bootstrap = false.
	BootstrapPeers []string `toml:"bootstrap_peers"`

	// Set to false to disable IPFS bootstrap peers.
	// Default: true.
	IPFSBootstrap *bool `toml:"ipfs_bootstrap"`

	// Announce these multiaddrs to the DHT instead of auto-detected ones.
	AnnounceAddrs []string `toml:"announce_addrs"`
}

// DaemonConfig controls the daemon's local socket and data storage.
type DaemonConfig struct {
	// Directory for identity, databases, and the Unix socket.
	// Default: ~/.moltmesh
	DataDir string `toml:"data_dir"`

	// gRPC listen address.
	// Default: unix socket inside DataDir.
	GRPCAddr string `toml:"grpc_addr"`

	// Enable verbose (development) logging.
	Verbose bool `toml:"verbose"`
}

// Defaults returns a Config with all defaults applied.
func Defaults() *Config {
	t := true
	return &Config{
		Network: NetworkConfig{
			IPFSBootstrap: &t,
		},
	}
}

// Load reads a moltbook.toml file. If path is empty it searches default locations.
// Missing file is not an error — defaults are returned.
func Load(path string) (*Config, error) {
	cfg := Defaults()

	resolved, err := resolve(path)
	if err != nil {
		// No config file found; return defaults silently.
		return cfg, nil
	}

	data, err := os.ReadFile(resolved)
	if err != nil {
		return nil, fmt.Errorf("read moltbook.toml: %w", err)
	}

	if _, err := toml.Decode(string(data), cfg); err != nil {
		return nil, fmt.Errorf("parse moltbook.toml: %w", err)
	}

	// Re-apply default for IPFSBootstrap if it was not set in the file.
	if cfg.Network.IPFSBootstrap == nil {
		t := true
		cfg.Network.IPFSBootstrap = &t
	}

	return cfg, nil
}

// IPFSBootstrapEnabled returns whether IPFS bootstrap peers should be used.
func (c *Config) IPFSBootstrapEnabled() bool {
	return c.Network.IPFSBootstrap == nil || *c.Network.IPFSBootstrap
}

// resolve finds the config file path.
func resolve(explicit string) (string, error) {
	if explicit != "" {
		if _, err := os.Stat(explicit); err != nil {
			return "", fmt.Errorf("config file %q not found", explicit)
		}
		return explicit, nil
	}

	// Current directory
	if _, err := os.Stat("moltbook.toml"); err == nil {
		return "moltbook.toml", nil
	}

	// ~/.moltmesh/moltbook.toml
	home, err := os.UserHomeDir()
	if err != nil {
		return "", err
	}
	p := filepath.Join(home, ".moltmesh", "moltbook.toml")
	if _, err := os.Stat(p); err == nil {
		return p, nil
	}

	return "", fmt.Errorf("no moltbook.toml found")
}
