package config

import (
	"os"
	"path/filepath"
	"testing"
)

func TestDefaults(t *testing.T) {
	cfg := Defaults()
	if !cfg.IPFSBootstrapEnabled() {
		t.Error("IPFS bootstrap should be enabled by default")
	}
}

func TestLoadMissing(t *testing.T) {
	// No config file → returns defaults without error
	cfg, err := Load("/nonexistent/path/moltbook.toml")
	if err != nil {
		t.Fatalf("Load with missing file should return defaults, got error: %v", err)
	}
	if !cfg.IPFSBootstrapEnabled() {
		t.Error("default IPFS bootstrap should be true")
	}
}

func TestLoadFull(t *testing.T) {
	dir := t.TempDir()
	content := `
[agent]
name        = "swift falcon"
description = "Test agent"
capabilities = ["a2a:v1:cap:text-generation"]

[network]
port            = "9100"
ipfs_bootstrap  = false
bootstrap_peers = ["/ip4/1.2.3.4/tcp/9000/p2p/12D3KooWTest"]

[daemon]
grpc_addr = "localhost:50051"
verbose   = true
`
	path := filepath.Join(dir, "moltbook.toml")
	if err := os.WriteFile(path, []byte(content), 0600); err != nil {
		t.Fatal(err)
	}

	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}

	if cfg.Agent.Name != "swift falcon" {
		t.Errorf("agent.name = %q, want %q", cfg.Agent.Name, "swift falcon")
	}
	if cfg.Agent.Description != "Test agent" {
		t.Errorf("agent.description = %q", cfg.Agent.Description)
	}
	if len(cfg.Agent.Capabilities) != 1 || cfg.Agent.Capabilities[0] != "a2a:v1:cap:text-generation" {
		t.Errorf("agent.capabilities = %v", cfg.Agent.Capabilities)
	}
	if cfg.Network.Port != "9100" {
		t.Errorf("network.port = %q", cfg.Network.Port)
	}
	if cfg.IPFSBootstrapEnabled() {
		t.Error("ipfs_bootstrap = false but IPFSBootstrapEnabled() returned true")
	}
	if len(cfg.Network.BootstrapPeers) != 1 {
		t.Errorf("bootstrap_peers len = %d", len(cfg.Network.BootstrapPeers))
	}
	if cfg.Daemon.GRPCAddr != "localhost:50051" {
		t.Errorf("daemon.grpc_addr = %q", cfg.Daemon.GRPCAddr)
	}
	if !cfg.Daemon.Verbose {
		t.Error("daemon.verbose should be true")
	}
}

func TestLoadInvalidTOML(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "moltbook.toml")
	if err := os.WriteFile(path, []byte("not: valid: toml: :::"), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := Load(path); err == nil {
		t.Error("expected error for invalid TOML")
	}
}

func TestIPFSBootstrapDefaultAfterPartialConfig(t *testing.T) {
	dir := t.TempDir()
	// Config that sets [agent] but doesn't mention ipfs_bootstrap
	content := "[agent]\nname = \"test\"\n"
	path := filepath.Join(dir, "moltbook.toml")
	if err := os.WriteFile(path, []byte(content), 0600); err != nil {
		t.Fatal(err)
	}

	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if !cfg.IPFSBootstrapEnabled() {
		t.Error("ipfs_bootstrap should default to true when not specified")
	}
}
