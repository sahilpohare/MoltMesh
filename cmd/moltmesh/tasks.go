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
)

func cmdCreateTask(args []string) error {
	fs := flag.NewFlagSet("create-task", flag.ExitOnError)
	dataDir, grpcAddr := clientFlags(fs)
	to := fs.String("to", "", "Assignee DID (required)")
	skill := fs.String("skill", "", "Skill/capability ID (required)")
	threadID := fs.String("thread-id", "", "Attach to existing thread (optional)")
	input := fs.String("input", "", "Task input text (optional)")
	fs.Parse(args)

	if *to == "" {
		return fmt.Errorf("--to is required")
	}
	if *skill == "" {
		return fmt.Errorf("--skill is required")
	}

	client, conn, err := connect(*dataDir, *grpcAddr)
	if err != nil {
		return err
	}
	defer conn.Close()

	req := &pb.TaskRequest{
		Skill:    *skill,
		ThreadId: *threadID,
	}
	// Carry the work itself, matching how send-task-result returns one.
	if *input != "" {
		req.InputArtifacts = []*pb.Artifact{{
			Name:     "input.txt",
			MimeType: "text/plain",
			Size:     int64(len(*input)),
			Inline:   []byte(*input),
		}}
	}

	task, err := client.CreateTask(context.Background(), &pb.CreateTaskRequest{
		ToDid: *to,
		Task:  req,
	})
	if err != nil {
		return err
	}

	data, _ := json.MarshalIndent(task, "", "  ")
	fmt.Println(string(data))
	return nil
}

// cmdSendTaskResult sends a terminal task result back to a task's initiator.
// Ported from cmd/daemon's send-task-result command (retired) so cmd/moltmesh
// remains a strict superset of it.
func cmdSendTaskResult(args []string) error {
	fs := flag.NewFlagSet("send-task-result", flag.ExitOnError)
	dataDir, grpcAddr := clientFlags(fs)
	to := fs.String("to", "", "Task initiator DID")
	taskID := fs.String("task-id", "", "Task ID")
	threadID := fs.String("thread-id", "", "Associated thread ID")
	result := fs.String("result", "", "UTF-8 result")
	errMsg := fs.String("error", "", "Failure text")
	fs.Parse(args)
	if *to == "" || *taskID == "" {
		return fmt.Errorf("--to and --task-id are required")
	}
	status := pb.TaskStatus_TASK_STATUS_COMPLETED
	if *errMsg != "" {
		status = pb.TaskStatus_TASK_STATUS_FAILED
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
	response, err := pb.NewA2ANodeClient(conn).SendTaskResult(context.Background(), &pb.SendTaskResultRequest{
		ToDid:    *to,
		ThreadId: *threadID,
		Result: &pb.TaskResult{
			TaskId:          *taskID,
			Status:          status,
			Error:           *errMsg,
			Data:            []byte(*result),
			OutputArtifacts: []*pb.Artifact{{Name: "result.txt", MimeType: "text/plain", Size: int64(len(*result)), Inline: []byte(*result)}},
		},
	})
	if err != nil {
		return err
	}
	jsonOut(response)
	return nil
}

func cmdGetTask(args []string) error {
	fs := flag.NewFlagSet("get-task", flag.ExitOnError)
	dataDir, grpcAddr := clientFlags(fs)
	id := fs.String("id", "", "Task ID (required)")
	fs.Parse(args)

	if *id == "" {
		return fmt.Errorf("--id is required")
	}

	client, conn, err := connect(*dataDir, *grpcAddr)
	if err != nil {
		return err
	}
	defer conn.Close()

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
	dataDir, grpcAddr := clientFlags(fs)
	id := fs.String("id", "", "Task ID (required)")
	status := fs.String("status", "", "New status: working|completed|failed|cancelled (required)")
	errText := fs.String("error", "", "Error message (for failed status)")
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

	client, conn, err := connect(*dataDir, *grpcAddr)
	if err != nil {
		return err
	}
	defer conn.Close()

	task, err := client.UpdateTask(context.Background(), &pb.TaskStatusUpdate{
		TaskId: *id,
		Status: statusVal,
		Error:  *errText,
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
	dataDir, grpcAddr := clientFlags(fs)
	id := fs.String("id", "", "Task ID (required)")
	fs.Parse(args)

	if *id == "" {
		return fmt.Errorf("--id is required")
	}

	client, conn, err := connect(*dataDir, *grpcAddr)
	if err != nil {
		return err
	}
	defer conn.Close()

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
	dataDir, grpcAddr := clientFlags(fs)
	taskID := fs.String("task-id", "", "Task ID (required)")
	kind := fs.String("kind", "status_update", "Event kind")
	data := fs.String("data", "", "Event data (string)")
	fs.Parse(args)

	if *taskID == "" {
		return fmt.Errorf("--task-id is required")
	}

	kindVal, err := parseEventKind(*kind)
	if err != nil {
		return err
	}

	client, conn, err := connect(*dataDir, *grpcAddr)
	if err != nil {
		return err
	}
	defer conn.Close()

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
	dataDir, grpcAddr := clientFlags(fs)
	id := fs.String("id", "", "Task ID (required)")
	fs.Parse(args)

	if *id == "" {
		return fmt.Errorf("--id is required")
	}

	client, conn, err := connect(*dataDir, *grpcAddr)
	if err != nil {
		return err
	}
	defer conn.Close()

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
		return pb.EventKind_EVENT_KIND_UNSPECIFIED, fmt.Errorf("unknown event kind %q", s)
	}
}

// ── PubSub ────────────────────────────────────────────────────────────────────
