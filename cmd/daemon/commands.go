package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"strings"

	pb "github.com/sahilpohare/p2p-a2a/gen/a2a/v1"
	"github.com/sahilpohare/p2p-a2a/pkg/format"
)

// jsonMode is set by the global --json flag.
var jsonMode bool

// jsonOut prints data as a JSON envelope {"status":"ok","data":...} when --json is set,
// otherwise marshals the value with indentation.
func jsonOut(data interface{}) {
	if jsonMode {
		b, _ := json.Marshal(map[string]interface{}{"status": "ok", "data": data})
		fmt.Println(string(b))
	} else {
		b, _ := json.MarshalIndent(data, "", "  ")
		fmt.Println(string(b))
	}
}

// jsonErr prints a structured error when --json is set, otherwise prints plain text.
func jsonErr(code, msg string) {
	if jsonMode {
		b, _ := json.Marshal(map[string]string{"status": "error", "code": code, "message": msg})
		fmt.Fprintln(os.Stderr, string(b))
	} else {
		fmt.Fprintln(os.Stderr, "error:", msg)
	}
}

// ── Identity & Registry ───────────────────────────────────────────────────────

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
	dataDir := fs.String("data-dir", "", "Data directory")
	grpcAddr := fs.String("grpc-addr", "", "gRPC server address")
	did := fs.String("did", "", "DID to look up (required)")
	fs.Parse(args)

	if *did == "" {
		return fmt.Errorf("--did is required")
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
	dataDir := fs.String("data-dir", "", "Data directory")
	grpcAddr := fs.String("grpc-addr", "", "gRPC server address")
	name := fs.String("name", "", "Agent name (required)")
	description := fs.String("description", "", "Agent description")
	fs.Parse(args)

	if *name == "" {
		return fmt.Errorf("--name is required")
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

	// Get our own identity to populate the card
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
	dataDir := fs.String("data-dir", "", "Data directory")
	grpcAddr := fs.String("grpc-addr", "", "gRPC server address")
	capability := fs.String("capability", "", "Capability ID to search for (required)")
	limit := fs.Int("limit", 10, "Max results")
	fs.Parse(args)

	if *capability == "" {
		return fmt.Errorf("--capability is required")
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

func cmdSendMessage(args []string) error {
	fs := flag.NewFlagSet("send-message", flag.ExitOnError)
	dataDir := fs.String("data-dir", "", "Data directory")
	grpcAddr := fs.String("grpc-addr", "", "gRPC server address")
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

	// Get sender DID
	id, err := client.GetIdentity(context.Background(), &pb.Empty{})
	if err != nil {
		return fmt.Errorf("get identity: %w", err)
	}

	// Encode text payload
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
	dataDir := fs.String("data-dir", "", "Data directory")
	grpcAddr := fs.String("grpc-addr", "", "gRPC server address")
	limit := fs.Int("limit", 20, "Max messages")
	unread := fs.Bool("unread", false, "Unread only")
	threadID := fs.String("thread-id", "", "Filter by thread ID")
	taskID := fs.String("task-id", "", "Filter by task ID")
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

	stream, err := client.GetInbox(context.Background(), &pb.InboxQuery{
		ThreadId:   *threadID,
		TaskId:     *taskID,
		UnreadOnly: *unread,
		Limit:      int32(*limit),
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
		fmt.Println("Inbox empty.")
	}
	return nil
}

func cmdGetOutbox(args []string) error {
	fs := flag.NewFlagSet("get-outbox", flag.ExitOnError)
	dataDir := fs.String("data-dir", "", "Data directory")
	grpcAddr := fs.String("grpc-addr", "", "gRPC server address")
	status := fs.String("status", "", "Filter: pending|delivered|failed|expired")
	limit := fs.Int("limit", 20, "Max messages")
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
	dataDir := fs.String("data-dir", "", "Data directory")
	grpcAddr := fs.String("grpc-addr", "", "gRPC server address")
	threadID := fs.String("thread-id", "", "Subscribe to specific thread")
	taskID := fs.String("task-id", "", "Subscribe to specific task")
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

	fmt.Fprintln(os.Stderr, "Subscribing to inbox (Ctrl+C to stop)...")
	stream, err := client.SubscribeInbox(context.Background(), &pb.SubscribeRequest{
		ThreadId: *threadID,
		TaskId:   *taskID,
	})
	if err != nil {
		return err
	}

	for {
		msg, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		printMessage(msg)
	}
	return nil
}

func cmdAckMessage(args []string) error {
	fs := flag.NewFlagSet("ack-message", flag.ExitOnError)
	dataDir := fs.String("data-dir", "", "Data directory")
	grpcAddr := fs.String("grpc-addr", "", "gRPC server address")
	id := fs.String("id", "", "Message ID to acknowledge (required)")
	fs.Parse(args)

	if *id == "" {
		return fmt.Errorf("--id is required")
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

	_, err = client.AckMessage(context.Background(), &pb.AckRequest{MessageId: *id})
	if err != nil {
		return err
	}
	fmt.Printf("Acknowledged: %s\n", *id)
	return nil
}

// ── Tasks ─────────────────────────────────────────────────────────────────────

func cmdCreateTask(args []string) error {
	fs := flag.NewFlagSet("create-task", flag.ExitOnError)
	dataDir := fs.String("data-dir", "", "Data directory")
	grpcAddr := fs.String("grpc-addr", "", "gRPC server address")
	to := fs.String("to", "", "Assignee DID (required)")
	skill := fs.String("skill", "", "Skill/capability ID (required)")
	threadID := fs.String("thread-id", "", "Attach to existing thread (optional)")
	fs.Parse(args)

	if *to == "" {
		return fmt.Errorf("--to is required")
	}
	if *skill == "" {
		return fmt.Errorf("--skill is required")
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

	task, err := client.CreateTask(context.Background(), &pb.CreateTaskRequest{
		ToDid: *to,
		Task: &pb.TaskRequest{
			Skill:    *skill,
			ThreadId: *threadID,
		},
	})
	if err != nil {
		return err
	}

	data, _ := json.MarshalIndent(task, "", "  ")
	fmt.Println(string(data))
	return nil
}

func cmdGetTask(args []string) error {
	fs := flag.NewFlagSet("get-task", flag.ExitOnError)
	dataDir := fs.String("data-dir", "", "Data directory")
	grpcAddr := fs.String("grpc-addr", "", "gRPC server address")
	id := fs.String("id", "", "Task ID (required)")
	fs.Parse(args)

	if *id == "" {
		return fmt.Errorf("--id is required")
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

	task, err := client.GetTask(context.Background(), &pb.TaskID{Id: *id})
	if err != nil {
		return err
	}

	data, _ := json.MarshalIndent(task, "", "  ")
	fmt.Println(string(data))
	return nil
}

func cmdUpdateTask(args []string) error {
	fs := flag.NewFlagSet("update-task", flag.ExitOnError)
	dataDir := fs.String("data-dir", "", "Data directory")
	grpcAddr := fs.String("grpc-addr", "", "gRPC server address")
	id := fs.String("id", "", "Task ID (required)")
	status := fs.String("status", "", "New status: working|completed|failed|cancelled (required)")
	errMsg := fs.String("error", "", "Error message (for failed status)")
	fs.Parse(args)

	if *id == "" {
		return fmt.Errorf("--id is required")
	}
	if *status == "" {
		return fmt.Errorf("--status is required")
	}

	statusVal, err := parseTaskStatus(*status)
	if err != nil {
		return err
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

	task, err := client.UpdateTask(context.Background(), &pb.TaskStatusUpdate{
		TaskId: *id,
		Status: statusVal,
		Error:  *errMsg,
	})
	if err != nil {
		return err
	}

	data, _ := json.MarshalIndent(task, "", "  ")
	fmt.Println(string(data))
	return nil
}

func cmdCancelTask(args []string) error {
	fs := flag.NewFlagSet("cancel-task", flag.ExitOnError)
	dataDir := fs.String("data-dir", "", "Data directory")
	grpcAddr := fs.String("grpc-addr", "", "gRPC server address")
	id := fs.String("id", "", "Task ID (required)")
	fs.Parse(args)

	if *id == "" {
		return fmt.Errorf("--id is required")
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

	task, err := client.CancelTask(context.Background(), &pb.TaskID{Id: *id})
	if err != nil {
		return err
	}

	data, _ := json.MarshalIndent(task, "", "  ")
	fmt.Println(string(data))
	return nil
}

func cmdPublishTaskEvent(args []string) error {
	fs := flag.NewFlagSet("publish-task-event", flag.ExitOnError)
	dataDir := fs.String("data-dir", "", "Data directory")
	grpcAddr := fs.String("grpc-addr", "", "gRPC server address")
	taskID := fs.String("task-id", "", "Task ID (required)")
	kind := fs.String("kind", "status_update", "Event kind: token_chunk|tool_call|tool_result|status_update|artifact|done|error")
	data := fs.String("data", "", "Event data (string)")
	fs.Parse(args)

	if *taskID == "" {
		return fmt.Errorf("--task-id is required")
	}

	kindVal, err := parseEventKind(*kind)
	if err != nil {
		return err
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

	_, err = client.PublishTaskEvent(context.Background(), &pb.TaskEvent{
		TaskId: *taskID,
		Kind:   kindVal,
		Data:   []byte(*data),
	})
	if err != nil {
		return err
	}
	fmt.Println("Event published.")
	return nil
}

func cmdSubscribeTaskEvents(args []string) error {
	fs := flag.NewFlagSet("subscribe-task-events", flag.ExitOnError)
	dataDir := fs.String("data-dir", "", "Data directory")
	grpcAddr := fs.String("grpc-addr", "", "gRPC server address")
	id := fs.String("id", "", "Task ID (required)")
	fs.Parse(args)

	if *id == "" {
		return fmt.Errorf("--id is required")
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

	fmt.Fprintln(os.Stderr, "Subscribing to task events (Ctrl+C to stop)...")
	stream, err := client.SubscribeTaskEvents(context.Background(), &pb.TaskID{Id: *id})
	if err != nil {
		return err
	}

	for {
		ev, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		data, _ := json.MarshalIndent(ev, "", "  ")
		fmt.Println(string(data))
	}
	return nil
}

// ── Files ─────────────────────────────────────────────────────────────────────

func cmdSendFile(args []string) error {
	fs := flag.NewFlagSet("send-file", flag.ExitOnError)
	dataDir := fs.String("data-dir", "", "Data directory")
	grpcAddr := fs.String("grpc-addr", "", "gRPC server address")
	filePath := fs.String("file", "", "File to upload (required)")
	mimeType := fs.String("mime-type", "application/octet-stream", "MIME type")
	fs.Parse(args)

	if *filePath == "" {
		return fmt.Errorf("--file is required")
	}

	fileData, err := os.ReadFile(*filePath)
	if err != nil {
		return fmt.Errorf("read file: %w", err)
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

	artifact, err := client.SendFile(context.Background(), &pb.SendFileRequest{
		Data:     fileData,
		Name:     *filePath,
		MimeType: *mimeType,
	})
	if err != nil {
		return err
	}

	data, _ := json.MarshalIndent(artifact, "", "  ")
	fmt.Println(string(data))
	return nil
}

func cmdFetchFile(args []string) error {
	fs := flag.NewFlagSet("fetch-file", flag.ExitOnError)
	dataDir := fs.String("data-dir", "", "Data directory")
	grpcAddr := fs.String("grpc-addr", "", "gRPC server address")
	cid := fs.String("cid", "", "Content ID to fetch (required)")
	from := fs.String("from", "", "Source DID (required)")
	out := fs.String("out", "", "Output file path (default: stdout)")
	fs.Parse(args)

	if *cid == "" {
		return fmt.Errorf("--cid is required")
	}
	if *from == "" {
		return fmt.Errorf("--from is required")
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

	stream, err := client.FetchFile(context.Background(), &pb.FetchFileRequest{
		Cid:     *cid,
		FromDid: *from,
	})
	if err != nil {
		return err
	}

	var w io.Writer = os.Stdout
	if *out != "" {
		f, err := os.Create(*out)
		if err != nil {
			return fmt.Errorf("create output file: %w", err)
		}
		defer f.Close()
		w = f
	}

	var total int64
	for {
		chunk, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		if _, err := w.Write(chunk.Data); err != nil {
			return err
		}
		total = chunk.Total
	}

	if *out != "" {
		fmt.Fprintf(os.Stderr, "Saved %d bytes to %s\n", total, *out)
	}
	return nil
}

// ── Threads ───────────────────────────────────────────────────────────────────

func cmdCreateThread(args []string) error {
	fs := flag.NewFlagSet("create-thread", flag.ExitOnError)
	dataDir := fs.String("data-dir", "", "Data directory")
	grpcAddr := fs.String("grpc-addr", "", "gRPC server address")
	replicas := fs.String("replicas", "", "Comma-separated replica DIDs")
	f := fs.Int("f", 0, "Max byzantine faults to tolerate")
	epochMs := fs.Int64("epoch-ms", 0, "Timeout propose in ms (0 = default 1000ms)")
	fs.Parse(args)

	var replicaDIDs []string
	if *replicas != "" {
		for _, r := range strings.Split(*replicas, ",") {
			r = strings.TrimSpace(r)
			if r != "" {
				replicaDIDs = append(replicaDIDs, r)
			}
		}
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

	thread, err := client.CreateThread(context.Background(), &pb.CreateThreadRequest{
		ReplicaDids: replicaDIDs,
		F:           int32(*f),
		EpochMs:     *epochMs,
	})
	if err != nil {
		return err
	}

	data, _ := json.MarshalIndent(thread, "", "  ")
	fmt.Println(string(data))
	return nil
}

func cmdGetThread(args []string) error {
	fs := flag.NewFlagSet("get-thread", flag.ExitOnError)
	dataDir := fs.String("data-dir", "", "Data directory")
	grpcAddr := fs.String("grpc-addr", "", "gRPC server address")
	id := fs.String("id", "", "Thread ID (required)")
	fs.Parse(args)

	if *id == "" {
		return fmt.Errorf("--id is required")
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

	thread, err := client.GetThread(context.Background(), &pb.ThreadID{Id: *id})
	if err != nil {
		return err
	}

	data, _ := json.MarshalIndent(thread, "", "  ")
	fmt.Println(string(data))
	return nil
}

func cmdAppendEntry(args []string) error {
	fs := flag.NewFlagSet("append-entry", flag.ExitOnError)
	dataDir := fs.String("data-dir", "", "Data directory")
	grpcAddr := fs.String("grpc-addr", "", "gRPC server address")
	threadID := fs.String("thread-id", "", "Thread ID (required)")
	payload := fs.String("payload", "", "Entry payload (string)")
	kind := fs.String("kind", "custom", "Entry kind: message|task_event|custom")
	fs.Parse(args)

	if *threadID == "" {
		return fmt.Errorf("--thread-id is required")
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

	result, err := client.AppendEntry(context.Background(), &pb.AppendEntryRequest{
		ThreadId: *threadID,
		Payload:  []byte(*payload),
		Kind:     *kind,
	})
	if err != nil {
		return err
	}

	data, _ := json.MarshalIndent(result, "", "  ")
	fmt.Println(string(data))
	return nil
}

func cmdGetThreadEntries(args []string) error {
	fs := flag.NewFlagSet("get-thread-entries", flag.ExitOnError)
	dataDir := fs.String("data-dir", "", "Data directory")
	grpcAddr := fs.String("grpc-addr", "", "gRPC server address")
	id := fs.String("id", "", "Thread ID (required)")
	since := fs.Int64("since", 0, "Since height (0 = from genesis)")
	limit := fs.Int("limit", 50, "Max entries")
	fs.Parse(args)

	if *id == "" {
		return fmt.Errorf("--id is required")
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

	stream, err := client.GetThreadEntries(context.Background(), &pb.GetThreadEntriesRequest{
		ThreadId:    *id,
		SinceHeight: *since,
		Limit:       int32(*limit),
	})
	if err != nil {
		return err
	}

	count := 0
	for {
		entry, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		data, _ := json.MarshalIndent(entry, "", "  ")
		fmt.Println(string(data))
		count++
	}
	if count == 0 {
		fmt.Println("No entries.")
	}
	return nil
}

func cmdSubscribeThread(args []string) error {
	fs := flag.NewFlagSet("subscribe-thread", flag.ExitOnError)
	dataDir := fs.String("data-dir", "", "Data directory")
	grpcAddr := fs.String("grpc-addr", "", "gRPC server address")
	id := fs.String("id", "", "Thread ID (required)")
	since := fs.Int64("since", 0, "Since height")
	fs.Parse(args)

	if *id == "" {
		return fmt.Errorf("--id is required")
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

	fmt.Fprintln(os.Stderr, "Subscribing to thread (Ctrl+C to stop)...")
	stream, err := client.SubscribeThread(context.Background(), &pb.SubscribeThreadRequest{
		ThreadId:    *id,
		SinceHeight: *since,
	})
	if err != nil {
		return err
	}

	for {
		entry, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		data, _ := json.MarshalIndent(entry, "", "  ")
		fmt.Println(string(data))
	}
	return nil
}

// ── Diagnostics ───────────────────────────────────────────────────────────────

func cmdPing(args []string) error {
	fs := flag.NewFlagSet("ping", flag.ExitOnError)
	dataDir := fs.String("data-dir", "", "Data directory")
	grpcAddr := fs.String("grpc-addr", "", "gRPC server address")
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
	dataDir := fs.String("data-dir", "", "Data directory")
	grpcAddr := fs.String("grpc-addr", "", "gRPC server address")
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
		fmt.Fprint(os.Stderr, `Usage: moltmesh-daemon format <type> <value> [value2 ...]

Types:
  did          Shorten / validate a did:key identifier
  capability   Parse and display a capability ID
  multiaddr    Shorten a libp2p multiaddr
  bytes        Human-readable byte size
  time         Format a Unix millisecond timestamp

Examples:
  moltmesh-daemon format did did:key:z6MkhaXgBZDvotDkL5257faiztiGiC2QtKLGpbnnEGta2doK
  moltmesh-daemon format capability a2a:v1:cap:text-generation
  moltmesh-daemon format multiaddr /ip4/192.168.1.5/udp/9000/quic-v1
  moltmesh-daemon format bytes 1048576
  moltmesh-daemon format time 1700000000000
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
					"full":   format.DIDFull(v),
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

// boolStr returns "true"/"false" as a string (used in JSON maps).
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

func printMessage(msg *pb.Message) {
	fmt.Println(format.Message(msg))
}

func parseTaskStatus(s string) (pb.TaskStatus, error) {
	switch strings.ToLower(s) {
	case "working":
		return pb.TaskStatus_TASK_STATUS_WORKING, nil
	case "completed":
		return pb.TaskStatus_TASK_STATUS_COMPLETED, nil
	case "failed":
		return pb.TaskStatus_TASK_STATUS_FAILED, nil
	case "cancelled":
		return pb.TaskStatus_TASK_STATUS_CANCELLED, nil
	default:
		return pb.TaskStatus_TASK_STATUS_UNSPECIFIED, fmt.Errorf("unknown status %q; valid: working|completed|failed|cancelled", s)
	}
}

func parseEventKind(s string) (pb.EventKind, error) {
	switch strings.ToLower(s) {
	case "token_chunk":
		return pb.EventKind_EVENT_KIND_TOKEN_CHUNK, nil
	case "tool_call":
		return pb.EventKind_EVENT_KIND_TOOL_CALL, nil
	case "tool_result":
		return pb.EventKind_EVENT_KIND_TOOL_RESULT, nil
	case "status_update":
		return pb.EventKind_EVENT_KIND_STATUS_UPDATE, nil
	case "artifact":
		return pb.EventKind_EVENT_KIND_ARTIFACT, nil
	case "done":
		return pb.EventKind_EVENT_KIND_DONE, nil
	case "error":
		return pb.EventKind_EVENT_KIND_ERROR, nil
	default:
		return pb.EventKind_EVENT_KIND_UNSPECIFIED, fmt.Errorf("unknown event kind %q; valid: token_chunk|tool_call|tool_result|status_update|artifact|done|error", s)
	}
}

// ── PubSub ────────────────────────────────────────────────────────────────────

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
	secret := fs.String("secret", "", "shared secret for X-MoltMesh-Secret header")
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

func cmdClearWebhook(args []string) error {
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

func cmdGetWebhook(args []string) error {
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

func cmdNetworkList(args []string) error {
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
		rows[i] = []string{n.Id[:8] + "…", n.Name, n.CreatorDid[:20] + "…", format.UnixMs(n.CreatedAt)}
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

// ── Name commands ─────────────────────────────────────────────────────────────

func cmdName(args []string) error {
	if len(args) == 0 {
		fmt.Fprintln(os.Stderr, "usage: moltmesh-daemon name <claim|resolve> ...")
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
