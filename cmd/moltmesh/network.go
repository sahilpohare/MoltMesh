package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"os"

	pb "github.com/sahilpohare/p2p-a2a/gen/a2a/v1"
	"github.com/sahilpohare/p2p-a2a/pkg/format"
)

func cmdNetwork(args []string) error {
	if len(args) == 0 {
		fmt.Fprintln(os.Stderr, "usage: network <create|join|leave|list|members|broadcast|subscribe>")
		return fmt.Errorf("subcommand required")
	}
	switch args[0] {
	case "create":
		return cmdNetworkCreate(args[1:])
	case "join":
		return cmdNetworkJoin(args[1:])
	case "leave":
		return cmdNetworkLeave(args[1:])
	case "list":
		return cmdNetworkList(args[1:])
	case "members":
		return cmdNetworkMembers(args[1:])
	case "broadcast":
		return cmdNetworkBroadcast(args[1:])
	case "subscribe":
		return cmdNetworkSubscribe(args[1:])
	default:
		return fmt.Errorf("unknown network subcommand %q", args[0])
	}
}

func cmdNetworkCreate(args []string) error {
	fs := flag.NewFlagSet("network create", flag.ContinueOnError)
	name := fs.String("name", "", "network name (required)")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *name == "" && len(fs.Args()) > 0 {
		*name = fs.Args()[0]
	}
	if *name == "" {
		return fmt.Errorf("usage: network create <name>")
	}
	conn, _, _, err := dialClient(nil, "ext")
	if err != nil {
		return err
	}
	defer conn.Close()

	extClient := pb.NewA2ANodeClient(conn)
	net, err := extClient.CreateNetwork(context.Background(), &pb.CreateNetworkRequest{Name: *name})
	if err != nil {
		return err
	}
	jsonOut(net)
	return nil
}

func cmdNetworkJoin(args []string) error {
	if len(args) == 0 {
		return fmt.Errorf("usage: network join <network-id>")
	}
	conn, _, _, err := dialClient(nil, "ext")
	if err != nil {
		return err
	}
	defer conn.Close()

	extClient := pb.NewA2ANodeClient(conn)
	net, err := extClient.JoinNetwork(context.Background(), &pb.JoinNetworkRequest{NetworkId: args[0]})
	if err != nil {
		return err
	}
	jsonOut(net)
	return nil
}

func cmdNetworkLeave(args []string) error {
	if len(args) == 0 {
		return fmt.Errorf("usage: network leave <network-id>")
	}
	conn, _, _, err := dialClient(nil, "ext")
	if err != nil {
		return err
	}
	defer conn.Close()

	extClient := pb.NewA2ANodeClient(conn)
	if _, err := extClient.LeaveNetwork(context.Background(), &pb.NetworkIDRequest{NetworkId: args[0]}); err != nil {
		return err
	}
	fmt.Printf("left network %s\n", args[0])
	return nil
}

func cmdNetworkList(_ []string) error {
	conn, _, _, err := dialClient(nil, "ext")
	if err != nil {
		return err
	}
	defer conn.Close()

	extClient := pb.NewA2ANodeClient(conn)
	resp, err := extClient.ListNetworks(context.Background(), &pb.Empty{})
	if err != nil {
		return err
	}
	if jsonMode {
		jsonOut(resp.Networks)
		return nil
	}
	if len(resp.Networks) == 0 {
		fmt.Println("no networks")
		return nil
	}
	rows := make([][]string, len(resp.Networks))
	for i, n := range resp.Networks {
		rows[i] = []string{truncate(n.Id, 8), n.Name, truncate(n.CreatorDid, 20), format.UnixMs(n.CreatedAt)}
	}
	fmt.Print(format.Table([]string{"ID", "NAME", "CREATOR", "CREATED"}, rows))
	return nil
}

func cmdNetworkMembers(args []string) error {
	if len(args) == 0 {
		return fmt.Errorf("usage: network members <network-id>")
	}
	conn, _, _, err := dialClient(nil, "ext")
	if err != nil {
		return err
	}
	defer conn.Close()

	extClient := pb.NewA2ANodeClient(conn)
	resp, err := extClient.NetworkMembers(context.Background(), &pb.NetworkIDRequest{NetworkId: args[0]})
	if err != nil {
		return err
	}
	if jsonMode {
		jsonOut(resp.Members)
		return nil
	}
	if len(resp.Members) == 0 {
		fmt.Println("no members")
		return nil
	}
	rows := make([][]string, len(resp.Members))
	for i, m := range resp.Members {
		rows[i] = []string{m.Did, format.UnixMs(m.JoinedAt)}
	}
	fmt.Print(format.Table([]string{"DID", "JOINED"}, rows))
	return nil
}

func cmdNetworkBroadcast(args []string) error {
	fs := flag.NewFlagSet("network broadcast", flag.ContinueOnError)
	netID := fs.String("network", "", "network ID (required)")
	payload := fs.String("payload", "", "payload string")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *netID == "" && len(fs.Args()) > 0 {
		*netID = fs.Args()[0]
		if len(fs.Args()) > 1 {
			*payload = fs.Args()[1]
		}
	}
	if *netID == "" {
		return fmt.Errorf("usage: network broadcast <network-id> <payload>")
	}
	conn, _, _, err := dialClient(nil, "ext")
	if err != nil {
		return err
	}
	defer conn.Close()

	extClient := pb.NewA2ANodeClient(conn)
	if _, err := extClient.BroadcastNetwork(context.Background(), &pb.BroadcastRequest{
		NetworkId: *netID,
		Payload:   []byte(*payload),
	}); err != nil {
		return err
	}
	fmt.Println("broadcast sent")
	return nil
}

func cmdNetworkSubscribe(args []string) error {
	if len(args) == 0 {
		return fmt.Errorf("usage: network subscribe <network-id>")
	}
	conn, _, _, err := dialClient(nil, "ext")
	if err != nil {
		return err
	}
	defer conn.Close()

	extClient := pb.NewA2ANodeClient(conn)
	stream, err := extClient.SubscribeNetwork(context.Background(), &pb.NetworkIDRequest{NetworkId: args[0]})
	if err != nil {
		return err
	}
	fmt.Fprintf(os.Stderr, "subscribed to network %q — waiting for broadcasts (Ctrl+C to stop)\n", args[0])
	for {
		msg, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		if jsonMode {
			jsonOut(msg)
		} else {
			fmt.Printf("[%s] network=%s payload=%q\n", format.UnixMs(msg.EmittedAt), msg.NetworkId, string(msg.Payload))
		}
	}
}

// ── Names ─────────────────────────────────────────────────────────────────────
