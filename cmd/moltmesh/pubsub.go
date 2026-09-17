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

func cmdPublish(args []string) error {
	fs := flag.NewFlagSet("publish", flag.ContinueOnError)
	topic := fs.String("topic", "", "topic name (required)")
	payload := fs.String("payload", "", "payload string")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *topic == "" {
		return fmt.Errorf("--topic is required")
	}
	conn, _, _, err := dialClient(nil, "ext")
	if err != nil {
		return err
	}
	defer conn.Close()

	extClient := pb.NewA2ANodeClient(conn)
	resp, err := extClient.Publish(context.Background(), &pb.PublishRequest{
		Topic:   *topic,
		Payload: []byte(*payload),
	})
	if err != nil {
		return err
	}
	jsonOut(map[string]string{"topic": resp.Topic})
	return nil
}

func cmdSubscribeTopic(args []string) error {
	fs := flag.NewFlagSet("subscribe-topic", flag.ContinueOnError)
	topic := fs.String("topic", "", "topic name (required)")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *topic == "" {
		return fmt.Errorf("--topic is required")
	}
	conn, _, _, err := dialClient(nil, "ext")
	if err != nil {
		return err
	}
	defer conn.Close()

	extClient := pb.NewA2ANodeClient(conn)
	stream, err := extClient.SubscribeTopic(context.Background(), &pb.SubscribeTopicRequest{Topic: *topic})
	if err != nil {
		return err
	}
	fmt.Fprintf(os.Stderr, "subscribed to topic %q — waiting for messages (Ctrl+C to stop)\n", *topic)
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
			fmt.Printf("[%s] topic=%s payload=%q\n", format.UnixMs(msg.EmittedAt), msg.Topic, string(msg.Payload))
		}
	}
}

// ── Webhook ───────────────────────────────────────────────────────────────────

func cmdSetWebhook(args []string) error {
	fs := flag.NewFlagSet("set-webhook", flag.ContinueOnError)
	url := fs.String("url", "", "webhook URL (required)")
	secret := fs.String("secret", "", "shared secret")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *url == "" && len(fs.Args()) > 0 {
		*url = fs.Args()[0]
	}
	if *url == "" {
		return fmt.Errorf("usage: set-webhook <url> [--secret <s>]")
	}
	conn, _, _, err := dialClient(nil, "ext")
	if err != nil {
		return err
	}
	defer conn.Close()

	extClient := pb.NewA2ANodeClient(conn)
	resp, err := extClient.SetWebhook(context.Background(), &pb.SetWebhookRequest{Url: *url, Secret: *secret})
	if err != nil {
		return err
	}
	jsonOut(map[string]string{"url": resp.Url})
	return nil
}

func cmdClearWebhook(_ []string) error {
	conn, _, _, err := dialClient(nil, "ext")
	if err != nil {
		return err
	}
	defer conn.Close()

	extClient := pb.NewA2ANodeClient(conn)
	if _, err := extClient.ClearWebhook(context.Background(), &pb.Empty{}); err != nil {
		return err
	}
	fmt.Println("webhook cleared")
	return nil
}

func cmdGetWebhook(_ []string) error {
	conn, _, _, err := dialClient(nil, "ext")
	if err != nil {
		return err
	}
	defer conn.Close()

	extClient := pb.NewA2ANodeClient(conn)
	resp, err := extClient.GetWebhook(context.Background(), &pb.Empty{})
	if err != nil {
		return err
	}
	if resp.Url == "" {
		fmt.Println("no webhook configured")
	} else {
		jsonOut(map[string]string{"url": resp.Url})
	}
	return nil
}

// ── Networks ──────────────────────────────────────────────────────────────────
