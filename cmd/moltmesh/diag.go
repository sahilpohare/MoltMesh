package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"strings"

	pb "github.com/sahilpohare/p2p-a2a/gen/a2a/v1"
	"github.com/sahilpohare/p2p-a2a/pkg/format"
)

func cmdPing(args []string) error {
	fs := flag.NewFlagSet("ping", flag.ExitOnError)
	dataDir, grpcAddr := clientFlags(fs)
	count := fs.Int("count", 1, "Number of pings")
	fs.Parse(args)

	target := ""
	if fs.NArg() > 0 {
		target = fs.Arg(0)
	}

	dir, err := resolveDataDir(*dataDir)
	if err != nil {
		return err
	}
	conn, err := dialGRPC(resolveGRPCAddr(*grpcAddr, dir))
	if err != nil {
		return err
	}
	defer conn.Close()

	client := pb.NewA2ANodeClient(conn)
	resp, err := client.Ping(context.Background(), &pb.PingRequest{
		TargetDid: target,
		Count:     int32(*count),
	})
	if err != nil {
		return err
	}

	if jsonMode {
		jsonOut(resp)
		return nil
	}

	for _, r := range resp.Results {
		if r.Reachable {
			fmt.Printf("pong from %s  latency=%dms\n", r.TargetDid, r.LatencyMs)
		} else {
			fmt.Printf("unreachable %s  error=%s\n", r.TargetDid, r.Error)
		}
	}
	return nil
}

func cmdHealth(args []string) error {
	conn, _, _, err := dialClient(args, "health")
	if err != nil {
		return err
	}
	defer conn.Close()

	diagClient := pb.NewA2ANodeClient(conn)
	resp, err := diagClient.Health(context.Background(), &pb.Empty{})
	if err != nil {
		return err
	}

	if jsonMode {
		jsonOut(resp)
		return nil
	}

	status := "ok"
	if !resp.Ok {
		status = "degraded"
	}
	fmt.Printf("status:     %s\n", status)
	fmt.Printf("version:    %s\n", resp.Version)
	fmt.Printf("did:        %s\n", resp.Did)
	fmt.Printf("peers:      %d\n", resp.PeerCount)
	fmt.Printf("uptime:     %s\n", format.Uptime(resp.UptimeSecs))
	return nil
}

func cmdPeers(args []string) error {
	fs := flag.NewFlagSet("peers", flag.ExitOnError)
	dataDir, grpcAddr := clientFlags(fs)
	fs.Parse(args)

	dir, err := resolveDataDir(*dataDir)
	if err != nil {
		return err
	}
	conn, err := dialGRPC(resolveGRPCAddr(*grpcAddr, dir))
	if err != nil {
		return err
	}
	defer conn.Close()

	client := pb.NewA2ANodeClient(conn)
	resp, err := client.ListPeers(context.Background(), &pb.Empty{})
	if err != nil {
		return err
	}

	if jsonMode {
		jsonOut(resp)
		return nil
	}

	if resp.Count == 0 {
		fmt.Println("No connected peers.")
		return nil
	}
	fmt.Printf("%d connected peers:\n", resp.Count)
	for _, p := range resp.Peers {
		fmt.Printf("  %s\n", p.PeerId)
		for _, a := range p.Addrs {
			fmt.Printf("    %s\n", format.Multiaddr(a))
		}
	}
	return nil
}

// ── Format utilities ──────────────────────────────────────────────────────────

func cmdFormat(args []string) error {
	if len(args) == 0 {
		fmt.Fprint(os.Stderr, `Usage: moltmesh format <type> <value> [value2 ...]

Types: did, capability, multiaddr, bytes, time
`)
		return nil
	}

	typ := args[0]
	vals := args[1:]
	if len(vals) == 0 {
		return fmt.Errorf("format %s: no value provided", typ)
	}

	switch typ {
	case "did":
		for _, v := range vals {
			if jsonMode {
				jsonOut(map[string]string{
					"input":  v,
					"short":  format.DID(v),
					"full":   v,
					"method": didMethod(v),
					"valid":  boolStr(isDIDValid(v)),
				})
			} else {
				valid := isDIDValid(v)
				validStr := "valid"
				if !valid {
					validStr = "INVALID"
				}
				fmt.Printf("input:   %s\n", v)
				fmt.Printf("short:   %s\n", format.DID(v))
				fmt.Printf("method:  %s\n", didMethod(v))
				fmt.Printf("status:  %s\n", validStr)
				if len(vals) > 1 {
					fmt.Println()
				}
			}
		}

	case "capability", "cap":
		for _, v := range vals {
			if jsonMode {
				jsonOut(map[string]string{
					"input": v,
					"short": format.Capability(v),
					"valid": boolStr(isCapValid(v)),
				})
			} else {
				valid := isCapValid(v)
				validStr := "valid"
				if !valid {
					validStr = "INVALID"
				}
				fmt.Printf("input:   %s\n", v)
				fmt.Printf("short:   %s\n", format.Capability(v))
				fmt.Printf("status:  %s\n", validStr)
				if len(vals) > 1 {
					fmt.Println()
				}
			}
		}

	case "multiaddr", "addr":
		for _, v := range vals {
			short := format.Multiaddr(v)
			if jsonMode {
				jsonOut(map[string]string{"input": v, "short": short})
			} else {
				fmt.Printf("%s  →  %s\n", v, short)
			}
		}

	case "bytes":
		for _, v := range vals {
			var n int64
			if _, err := fmt.Sscanf(v, "%d", &n); err != nil {
				return fmt.Errorf("format bytes: %q is not an integer", v)
			}
			human := format.Bytes(n)
			if jsonMode {
				jsonOut(map[string]interface{}{"input": n, "human": human})
			} else {
				fmt.Printf("%d  →  %s\n", n, human)
			}
		}

	case "time", "ts":
		for _, v := range vals {
			var ms int64
			if _, err := fmt.Sscanf(v, "%d", &ms); err != nil {
				return fmt.Errorf("format time: %q is not an integer", v)
			}
			abs := format.UnixMs(ms)
			ago := format.UnixMsAgo(ms)
			if jsonMode {
				jsonOut(map[string]string{"input": v, "absolute": abs, "relative": ago})
			} else {
				fmt.Printf("%s  (%s)\n", abs, ago)
			}
		}

	default:
		return fmt.Errorf("unknown format type %q; valid: did, capability, multiaddr, bytes, time", typ)
	}

	return nil
}

func boolStr(b bool) string {
	if b {
		return "true"
	}
	return "false"
}

func isDIDValid(s string) bool {
	return format.DID(s) != s || strings.HasPrefix(s, "did:key:z")
}

func isCapValid(s string) bool {
	parts := strings.SplitN(s, ":", 4)
	return len(parts) == 4 && parts[0] == "a2a" && parts[2] == "cap"
}

func didMethod(s string) string {
	if !strings.HasPrefix(s, "did:") {
		return ""
	}
	rest := s[4:]
	idx := strings.IndexByte(rest, ':')
	if idx < 0 {
		return rest
	}
	return rest[:idx]
}

// ── Helpers ───────────────────────────────────────────────────────────────────
