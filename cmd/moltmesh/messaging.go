package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"

	pb "github.com/sahilpohare/p2p-a2a/gen/a2a/v1"
	"github.com/sahilpohare/p2p-a2a/pkg/format"
	"google.golang.org/protobuf/proto"
)

func cmdSendMessage(args []string) error {
	fs := flag.NewFlagSet("send-message", flag.ExitOnError)
	dataDir, grpcAddr := clientFlags(fs)
	to := fs.String("to", "", "Recipient DID (required)")
	text := fs.String("text", "", "Message text (required)")
	threadID := fs.String("thread-id", "", "Thread ID (optional)")
	fs.Parse(args)

	if *to == "" {
		return fmt.Errorf("--to is required")
	}
	if *text == "" {
		return fmt.Errorf("--text is required")
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

	textMsg := &pb.TextMessage{Text: *text}
	payload, err := json.Marshal(textMsg)
	if err != nil {
		return err
	}

	result, err := client.SendMessage(context.Background(), &pb.Message{
		FromDid:  id.Did,
		ToDid:    *to,
		ThreadId: *threadID,
		Kind:     pb.MessageKind_MESSAGE_KIND_TEXT,
		Payload:  payload,
	})
	if err != nil {
		return err
	}

	if result.Queued {
		fmt.Printf("Message queued (recipient offline): %s\n", result.MessageId)
	} else {
		fmt.Printf("Message sent: %s\n", result.MessageId)
	}
	return nil
}

func cmdGetInbox(args []string) error {
	fs := flag.NewFlagSet("get-inbox", flag.ExitOnError)
	dataDir, grpcAddr := clientFlags(fs)
	limit := fs.Int("limit", 20, "Max messages")
	unread := fs.Bool("unread", false, "Unread only")
	threadID := fs.String("thread-id", "", "Filter by thread ID")
	taskID := fs.String("task-id", "", "Filter by task ID")
	decode := fs.Bool("decode", false, "Emit JSON and decode each message payload")
	fs.Parse(args)

	client, conn, err := connect(*dataDir, *grpcAddr)
	if err != nil {
		return err
	}
	defer conn.Close()

	stream, err := client.GetInbox(context.Background(), &pb.InboxQuery{
		ThreadId:   *threadID,
		TaskId:     *taskID,
		UnreadOnly: *unread,
		Limit:      int32(*limit),
	})
	if err != nil {
		return err
	}

	// --decode emits structured output: each message alongside its unmarshalled
	// payload, so callers can read task input without a second round trip.
	if *decode {
		items := []map[string]interface{}{}
		for {
			msg, err := stream.Recv()
			if err == io.EOF {
				break
			}
			if err != nil {
				return err
			}
			items = append(items, map[string]interface{}{
				"message": msg,
				"decoded": decodePayload(msg),
			})
		}
		jsonOut(items)
		return nil
	}

	count := 0
	for {
		msg, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		printMessage(msg)
		count++
	}
	if count == 0 {
		fmt.Println("Inbox empty.")
	}
	return nil
}

// decodePayload unmarshals a message payload according to its kind. Returns nil
// when the kind carries no structured body or the bytes do not parse.
func decodePayload(m *pb.Message) interface{} {
	if len(m.Payload) == 0 {
		return nil
	}
	// Payload encoding differs by kind: send-message marshals TextMessage as
	// JSON, while task payloads are protobuf. Decode each the way it was sent.
	if m.Kind == pb.MessageKind_MESSAGE_KIND_TEXT {
		var tm pb.TextMessage
		if err := json.Unmarshal(m.Payload, &tm); err == nil {
			return &tm
		}
		if err := proto.Unmarshal(m.Payload, &tm); err == nil {
			return &tm
		}
		return nil
	}

	var out proto.Message
	switch m.Kind {
	case pb.MessageKind_MESSAGE_KIND_TASK_REQUEST:
		out = &pb.TaskRequest{}
	case pb.MessageKind_MESSAGE_KIND_TASK_RESULT:
		out = &pb.TaskResult{}
	case pb.MessageKind_MESSAGE_KIND_TASK_EVENT:
		out = &pb.TaskEvent{}
	default:
		return nil
	}
	if err := proto.Unmarshal(m.Payload, out); err != nil {
		return nil
	}
	return out
}

func cmdGetOutbox(args []string) error {
	fs := flag.NewFlagSet("get-outbox", flag.ExitOnError)
	dataDir, grpcAddr := clientFlags(fs)
	status := fs.String("status", "", "Filter: pending|delivered|failed|expired")
	limit := fs.Int("limit", 20, "Max messages")
	fs.Parse(args)

	client, conn, err := connect(*dataDir, *grpcAddr)
	if err != nil {
		return err
	}
	defer conn.Close()

	stream, err := client.GetOutbox(context.Background(), &pb.OutboxQuery{
		Status: *status,
		Limit:  int32(*limit),
	})
	if err != nil {
		return err
	}

	count := 0
	for {
		msg, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		printMessage(msg)
		count++
	}
	if count == 0 {
		fmt.Println("Outbox empty.")
	}
	return nil
}

func cmdSubscribeInbox(args []string) error {
	fs := flag.NewFlagSet("subscribe-inbox", flag.ExitOnError)
	dataDir, grpcAddr := clientFlags(fs)
	threadID := fs.String("thread-id", "", "Subscribe to specific thread")
	taskID := fs.String("task-id", "", "Subscribe to specific task")
	decode := fs.Bool("decode", false, "Emit one JSON object per message, payload decoded")
	fs.Parse(args)

	client, conn, err := connect(*dataDir, *grpcAddr)
	if err != nil {
		return err
	}
	defer conn.Close()

	fmt.Fprintln(os.Stderr, "Subscribing to inbox (Ctrl+C to stop)...")
	stream, err := client.SubscribeInbox(context.Background(), &pb.SubscribeRequest{
		ThreadId: *threadID,
		TaskId:   *taskID,
	})
	if err != nil {
		return err
	}

	// --decode emits newline-delimited JSON so a listener can read each message
	// and its payload as it arrives, rather than a truncated display line.
	enc := json.NewEncoder(os.Stdout)
	for {
		msg, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		if *decode {
			if err := enc.Encode(map[string]interface{}{
				"message": msg,
				"decoded": decodePayload(msg),
			}); err != nil {
				return err
			}
			continue
		}
		printMessage(msg)
	}
	return nil
}

func cmdAckMessage(args []string) error {
	fs := flag.NewFlagSet("ack-message", flag.ExitOnError)
	dataDir, grpcAddr := clientFlags(fs)
	id := fs.String("id", "", "Message ID to acknowledge (required)")
	fs.Parse(args)

	if *id == "" {
		return fmt.Errorf("--id is required")
	}

	client, conn, err := connect(*dataDir, *grpcAddr)
	if err != nil {
		return err
	}
	defer conn.Close()

	_, err = client.AckMessage(context.Background(), &pb.AckRequest{MessageId: *id})
	if err != nil {
		return err
	}
	fmt.Printf("Acknowledged: %s\n", *id)
	return nil
}

// ── Tasks ─────────────────────────────────────────────────────────────────────

func printMessage(msg *pb.Message) {
	fmt.Println(format.Message(msg))
}
