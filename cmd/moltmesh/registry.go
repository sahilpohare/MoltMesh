package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"

	pb "github.com/sahilpohare/p2p-a2a/gen/a2a/v1"
)

func cmdGetIdentity(args []string) error {
	conn, client, _, err := dialClient(args, "get-identity")
	if err != nil {
		return err
	}
	defer conn.Close()

	id, err := client.GetIdentity(context.Background(), &pb.Empty{})
	if err != nil {
		return err
	}

	data, _ := json.MarshalIndent(id, "", "  ")
	fmt.Println(string(data))
	return nil
}

func cmdGetAgentCard(args []string) error {
	fs := flag.NewFlagSet("get-agent-card", flag.ExitOnError)
	dataDir, grpcAddr := clientFlags(fs)
	did := fs.String("did", "", "DID to look up (required)")
	fs.Parse(args)

	if *did == "" {
		return fmt.Errorf("--did is required")
	}

	client, conn, err := connect(*dataDir, *grpcAddr)
	if err != nil {
		return err
	}
	defer conn.Close()

	card, err := client.GetAgentCard(context.Background(), &pb.AgentIdentityRequest{Did: *did})
	if err != nil {
		return err
	}

	data, _ := json.MarshalIndent(card, "", "  ")
	fmt.Println(string(data))
	return nil
}

func cmdPublishAgentCard(args []string) error {
	fs := flag.NewFlagSet("publish-agent-card", flag.ExitOnError)
	dataDir, grpcAddr := clientFlags(fs)
	name := fs.String("name", "", "Agent name (required)")
	description := fs.String("description", "", "Agent description")
	fs.Parse(args)

	if *name == "" {
		return fmt.Errorf("--name is required")
	}

	client, conn, err := connect(*dataDir, *grpcAddr)
	if err != nil {
		return err
	}
	defer conn.Close()

	id, err := client.GetIdentity(context.Background(), &pb.Empty{})
	if err != nil {
		return fmt.Errorf("get identity: %w", err)
	}

	result, err := client.PublishAgentCard(context.Background(), &pb.AgentCard{
		Did:         id.Did,
		Name:        *name,
		Description: *description,
		Multiaddrs:  id.Multiaddrs,
		PublicKey:   id.PublicKey,
	})
	if err != nil {
		return err
	}

	if !result.Success {
		return fmt.Errorf("publish failed: %s", result.Error)
	}
	fmt.Println("Agent card published.")
	return nil
}

func cmdFindAgents(args []string) error {
	fs := flag.NewFlagSet("find-agents", flag.ExitOnError)
	dataDir, grpcAddr := clientFlags(fs)
	capability := fs.String("capability", "", "Capability ID to search for (required)")
	limit := fs.Int("limit", 10, "Max results")
	fs.Parse(args)

	if *capability == "" {
		return fmt.Errorf("--capability is required")
	}

	client, conn, err := connect(*dataDir, *grpcAddr)
	if err != nil {
		return err
	}
	defer conn.Close()

	stream, err := client.FindAgents(context.Background(), &pb.CapabilityQuery{
		Capability: *capability,
		Limit:      int32(*limit),
	})
	if err != nil {
		return err
	}

	count := 0
	for {
		card, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		data, _ := json.MarshalIndent(card, "", "  ")
		fmt.Println(string(data))
		count++
	}
	if count == 0 {
		fmt.Println("No agents found.")
	}
	return nil
}

// ── Messaging ─────────────────────────────────────────────────────────────────
