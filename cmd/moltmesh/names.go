package main

import (
	"context"
	"fmt"
	"os"
	"strings"

	pb "github.com/sahilpohare/p2p-a2a/gen/a2a/v1"
	"github.com/sahilpohare/p2p-a2a/pkg/format"
)

func cmdName(args []string) error {
	if len(args) == 0 {
		fmt.Fprintln(os.Stderr, "usage: moltmesh name <claim|resolve> ...")
		return fmt.Errorf("sub-command required")
	}
	switch args[0] {
	case "claim":
		return cmdNameClaim(args[1:])
	case "resolve":
		return cmdNameResolve(args[1:])
	default:
		return fmt.Errorf("unknown name sub-command: %s", args[0])
	}
}

func cmdNameClaim(args []string) error {
	if len(args) == 0 {
		return fmt.Errorf("usage: name claim <human-readable-name>")
	}
	name := strings.Join(args, " ")
	conn, _, _, err := dialClient(nil, "ext")
	if err != nil {
		return err
	}
	defer conn.Close()

	extClient := pb.NewA2ANodeClient(conn)
	resp, err := extClient.ClaimName(context.Background(), &pb.ClaimNameRequest{Name: name})
	if err != nil {
		return err
	}
	if jsonMode {
		jsonOut(resp)
	} else {
		fmt.Printf("Name claimed: %s\n  DID:        %s\n  Expires:    %s\n",
			resp.Name, resp.Did, format.UnixMs(resp.ExpiresAt))
	}
	return nil
}

func cmdNameResolve(args []string) error {
	if len(args) == 0 {
		return fmt.Errorf("usage: name resolve <name>")
	}
	name := strings.Join(args, " ")
	conn, _, _, err := dialClient(nil, "ext")
	if err != nil {
		return err
	}
	defer conn.Close()

	extClient := pb.NewA2ANodeClient(conn)
	resp, err := extClient.ResolveName(context.Background(), &pb.ResolveNameRequest{Name: name})
	if err != nil {
		return fmt.Errorf("name %q not found: %w", name, err)
	}
	if jsonMode {
		jsonOut(resp)
	} else {
		fmt.Printf("Name: %s\n  DID:        %s\n  Published:  %s\n  Expires:    %s\n",
			resp.Name, resp.Did, format.UnixMs(resp.PublishedAt), format.UnixMs(resp.ExpiresAt))
	}
	return nil
}

// cmdInit scaffolds a new agent: creates the data directory, generates an
// identity if one doesn't already exist, and writes a starter moltbook.toml
// if one doesn't already exist at the target path. Safe to re-run; existing
// identity/config files are left untouched. Ported from cmd/daemon's init
// command (retired) so cmd/moltmesh remains a strict superset of it.
