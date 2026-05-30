package rpc_test

import (
	"context"
	"io"
	"net"
	"testing"
	"time"

	pubsub "github.com/libp2p/go-libp2p-pubsub"
	"github.com/libp2p/go-libp2p"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	pb "github.com/sahilpohare/p2p-a2a/gen/a2a/v1"
	"github.com/sahilpohare/p2p-a2a/daemon/gossip"
	"github.com/sahilpohare/p2p-a2a/daemon/identity"
	"github.com/sahilpohare/p2p-a2a/daemon/inbox"
	"github.com/sahilpohare/p2p-a2a/daemon/outbox"
	"github.com/sahilpohare/p2p-a2a/daemon/rpc"
	"github.com/sahilpohare/p2p-a2a/daemon/tasks"

	// blank import for sqlite3 driver
	_ "github.com/mattn/go-sqlite3"
)

// ─── test harness ─────────────────────────────────────────────────────────────

type testEnv struct {
	client pb.A2ANodeClient
	id     *identity.Identity
	ib     *inbox.Inbox
	ob     *outbox.Outbox
	ts     *tasks.Store
}

func newTestEnv(t *testing.T) *testEnv {
	t.Helper()
	log, _ := zap.NewDevelopment()

	id, err := identity.Generate()
	if err != nil {
		t.Fatalf("identity.Generate: %v", err)
	}

	ib, err := inbox.New(":memory:")
	if err != nil {
		t.Fatalf("inbox.New: %v", err)
	}
	t.Cleanup(func() { ib.Close() })

	ob, err := outbox.New(":memory:", func(_ context.Context, _ *pb.Message) error {
		return nil
	}, log)
	if err != nil {
		t.Fatalf("outbox.New: %v", err)
	}
	t.Cleanup(func() { ob.Close() })

	ts, err := tasks.New(":memory:")
	if err != nil {
		t.Fatalf("tasks.New: %v", err)
	}
	t.Cleanup(func() { ts.Close() })

	// Minimal gossip manager backed by an in-process libp2p host.
	h, err := libp2p.New(libp2p.ListenAddrStrings("/ip4/127.0.0.1/tcp/0"))
	if err != nil {
		t.Fatalf("libp2p.New: %v", err)
	}
	t.Cleanup(func() { h.Close() })

	ps, err := pubsub.NewGossipSub(context.Background(), h,
		pubsub.WithMessageSignaturePolicy(pubsub.StrictSign),
	)
	if err != nil {
		t.Fatalf("pubsub.NewGossipSub: %v", err)
	}
	gm := gossip.New(ps, log)

	// Stub registry — nil; methods under test don't call registry.
	srv := rpc.New(id, ib, ob, ts, nil, gm, nil, nil, nil, []string{"/ip4/127.0.0.1/tcp/0"}, log)

	// Start gRPC server on an in-process listener.
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("net.Listen: %v", err)
	}
	grpcSrv := grpc.NewServer()
	pb.RegisterA2ANodeServer(grpcSrv, srv)
	go grpcSrv.Serve(lis) //nolint:errcheck
	t.Cleanup(func() { grpcSrv.Stop() })

	conn, err := grpc.NewClient(lis.Addr().String(),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("grpc.NewClient: %v", err)
	}
	t.Cleanup(func() { conn.Close() })

	return &testEnv{
		client: pb.NewA2ANodeClient(conn),
		id:     id,
		ib:     ib,
		ob:     ob,
		ts:     ts,
	}
}

// ─── identity ─────────────────────────────────────────────────────────────────

func TestGetIdentity(t *testing.T) {
	env := newTestEnv(t)
	got, err := env.client.GetIdentity(context.Background(), &pb.Empty{})
	if err != nil {
		t.Fatalf("GetIdentity: %v", err)
	}
	if got.Did != env.id.DID {
		t.Errorf("DID mismatch: got %q, want %q", got.Did, env.id.DID)
	}
	if got.PublicKey == "" {
		t.Error("PublicKey is empty")
	}
	if len(got.Multiaddrs) == 0 {
		t.Error("Multiaddrs is empty")
	}
}

// ─── messaging ────────────────────────────────────────────────────────────────

func TestSendMessage_Queued(t *testing.T) {
	env := newTestEnv(t)
	res, err := env.client.SendMessage(context.Background(), &pb.Message{
		ToDid: "did:key:zRemote",
		Kind:  pb.MessageKind_MESSAGE_KIND_TEXT,
	})
	if err != nil {
		t.Fatalf("SendMessage: %v", err)
	}
	if res.MessageId == "" {
		t.Error("MessageId is empty")
	}
	if !res.Queued {
		t.Error("Queued should be true")
	}

	// verify in outbox
	msgs, err := env.ob.List("pending", 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(msgs) != 1 {
		t.Errorf("expected 1 pending, got %d", len(msgs))
	}
	if msgs[0].FromDid != env.id.DID {
		t.Errorf("FromDid not set: %q", msgs[0].FromDid)
	}
}

func TestSendMessage_IDAssigned(t *testing.T) {
	env := newTestEnv(t)
	// send without ID — server should assign one
	res, err := env.client.SendMessage(context.Background(), &pb.Message{
		ToDid: "did:key:zRemote",
		Kind:  pb.MessageKind_MESSAGE_KIND_TEXT,
	})
	if err != nil {
		t.Fatal(err)
	}
	if res.MessageId == "" {
		t.Error("server should assign a message ID")
	}
}

func TestGetInbox_Empty(t *testing.T) {
	env := newTestEnv(t)
	stream, err := env.client.GetInbox(context.Background(), &pb.InboxQuery{})
	if err != nil {
		t.Fatalf("GetInbox: %v", err)
	}
	msgs := collectMessages(t, stream)
	if len(msgs) != 0 {
		t.Errorf("expected 0 inbox messages, got %d", len(msgs))
	}
}

func TestGetInbox_WithMessages(t *testing.T) {
	env := newTestEnv(t)

	// put two messages directly into inbox
	env.ib.Put(&pb.Message{Id: "m1", FromDid: "did:key:zA", Kind: pb.MessageKind_MESSAGE_KIND_TEXT})
	env.ib.Put(&pb.Message{Id: "m2", FromDid: "did:key:zB", Kind: pb.MessageKind_MESSAGE_KIND_TEXT})

	stream, err := env.client.GetInbox(context.Background(), &pb.InboxQuery{})
	if err != nil {
		t.Fatalf("GetInbox: %v", err)
	}
	msgs := collectMessages(t, stream)
	if len(msgs) != 2 {
		t.Errorf("expected 2 messages, got %d", len(msgs))
	}
}

func TestAckMessage(t *testing.T) {
	env := newTestEnv(t)
	env.ib.Put(&pb.Message{Id: "ack-me", FromDid: "did:key:zA", Kind: pb.MessageKind_MESSAGE_KIND_TEXT})

	_, err := env.client.AckMessage(context.Background(), &pb.AckRequest{MessageId: "ack-me"})
	if err != nil {
		t.Fatalf("AckMessage: %v", err)
	}

	// check unread-only query returns 0
	stream, err := env.client.GetInbox(context.Background(), &pb.InboxQuery{UnreadOnly: true})
	if err != nil {
		t.Fatal(err)
	}
	msgs := collectMessages(t, stream)
	if len(msgs) != 0 {
		t.Errorf("expected 0 unread after ack, got %d", len(msgs))
	}
}

func TestGetOutbox(t *testing.T) {
	env := newTestEnv(t)
	env.client.SendMessage(context.Background(), &pb.Message{
		ToDid: "did:key:zRemote",
		Kind:  pb.MessageKind_MESSAGE_KIND_TEXT,
	})

	stream, err := env.client.GetOutbox(context.Background(), &pb.OutboxQuery{Status: "pending"})
	if err != nil {
		t.Fatalf("GetOutbox: %v", err)
	}
	msgs := collectMessages(t, stream)
	if len(msgs) != 1 {
		t.Errorf("expected 1 pending outbox, got %d", len(msgs))
	}
}

// ─── tasks ────────────────────────────────────────────────────────────────────

func TestCreateTask(t *testing.T) {
	env := newTestEnv(t)
	res, err := env.client.CreateTask(context.Background(), &pb.CreateTaskRequest{
		ToDid: "did:key:zAssignee",
		Task: &pb.TaskRequest{
			Skill: "a2a:v1:cap:text-generation",
		},
	})
	if err != nil {
		t.Fatalf("CreateTask: %v", err)
	}
	if res.Id == "" {
		t.Error("task ID is empty")
	}
	if res.Status != pb.TaskStatus_TASK_STATUS_SUBMITTED {
		t.Errorf("expected SUBMITTED, got %v", res.Status)
	}
	if res.Initiator != env.id.DID {
		t.Errorf("initiator mismatch: %q", res.Initiator)
	}
}

func TestGetTask(t *testing.T) {
	env := newTestEnv(t)
	created, _ := env.client.CreateTask(context.Background(), &pb.CreateTaskRequest{
		ToDid: "did:key:zAssignee",
		Task:  &pb.TaskRequest{Skill: "test-skill"},
	})

	got, err := env.client.GetTask(context.Background(), &pb.TaskID{Id: created.Id})
	if err != nil {
		t.Fatalf("GetTask: %v", err)
	}
	if got.Id != created.Id {
		t.Errorf("ID mismatch: %q vs %q", got.Id, created.Id)
	}
}

func TestUpdateTask_Lifecycle(t *testing.T) {
	env := newTestEnv(t)
	task, _ := env.client.CreateTask(context.Background(), &pb.CreateTaskRequest{
		ToDid: "did:key:zAssignee",
		Task:  &pb.TaskRequest{Skill: "test-skill"},
	})

	// SUBMITTED → WORKING
	updated, err := env.client.UpdateTask(context.Background(), &pb.TaskStatusUpdate{
		TaskId: task.Id,
		Status: pb.TaskStatus_TASK_STATUS_WORKING,
	})
	if err != nil {
		t.Fatalf("UpdateTask WORKING: %v", err)
	}
	if updated.Status != pb.TaskStatus_TASK_STATUS_WORKING {
		t.Errorf("expected WORKING, got %v", updated.Status)
	}

	// WORKING → COMPLETED
	completed, err := env.client.UpdateTask(context.Background(), &pb.TaskStatusUpdate{
		TaskId:          task.Id,
		Status:          pb.TaskStatus_TASK_STATUS_COMPLETED,
		OutputArtifacts: []*pb.Artifact{{Cid: "sha256:abc", MimeType: "text/plain", Size: 10}},
	})
	if err != nil {
		t.Fatalf("UpdateTask COMPLETED: %v", err)
	}
	if completed.Status != pb.TaskStatus_TASK_STATUS_COMPLETED {
		t.Errorf("expected COMPLETED, got %v", completed.Status)
	}
	if len(completed.OutputArtifacts) != 1 {
		t.Errorf("expected 1 artifact, got %d", len(completed.OutputArtifacts))
	}
}

func TestCancelTask(t *testing.T) {
	env := newTestEnv(t)
	task, _ := env.client.CreateTask(context.Background(), &pb.CreateTaskRequest{
		ToDid: "did:key:zAssignee",
		Task:  &pb.TaskRequest{Skill: "test-skill"},
	})

	cancelled, err := env.client.CancelTask(context.Background(), &pb.TaskID{Id: task.Id})
	if err != nil {
		t.Fatalf("CancelTask: %v", err)
	}
	if cancelled.Status != pb.TaskStatus_TASK_STATUS_CANCELLED {
		t.Errorf("expected CANCELLED, got %v", cancelled.Status)
	}
}

func TestCreateTask_EnqueuesOutboxMessage(t *testing.T) {
	env := newTestEnv(t)
	assignee := "did:key:zAssignee"

	env.client.CreateTask(context.Background(), &pb.CreateTaskRequest{
		ToDid: assignee,
		Task:  &pb.TaskRequest{Skill: "test-skill"},
	})

	msgs, err := env.ob.List("pending", 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(msgs) == 0 {
		t.Fatal("expected task request message in outbox")
	}
	if msgs[0].ToDid != assignee {
		t.Errorf("outbox message ToDid: %q", msgs[0].ToDid)
	}
	if msgs[0].Kind != pb.MessageKind_MESSAGE_KIND_TASK_REQUEST {
		t.Errorf("outbox message kind: %v", msgs[0].Kind)
	}
}

func TestSubscribeInbox_InitialFlush(t *testing.T) {
	env := newTestEnv(t)
	env.ib.Put(&pb.Message{Id: "flush-1", FromDid: "did:key:zA", Kind: pb.MessageKind_MESSAGE_KIND_TEXT})
	env.ib.Put(&pb.Message{Id: "flush-2", FromDid: "did:key:zB", Kind: pb.MessageKind_MESSAGE_KIND_TEXT})

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	stream, err := env.client.SubscribeInbox(ctx, &pb.SubscribeRequest{})
	if err != nil {
		t.Fatalf("SubscribeInbox: %v", err)
	}

	// SubscribeInbox sends existing messages then blocks; collect until context done.
	var msgs []*pb.Message
	for {
		m, err := stream.Recv()
		if err == io.EOF || ctx.Err() != nil {
			break
		}
		if err != nil {
			break
		}
		msgs = append(msgs, m)
		if len(msgs) == 2 {
			cancel()
			break
		}
	}

	if len(msgs) != 2 {
		t.Errorf("expected 2 initial flush messages, got %d", len(msgs))
	}
}

// ─── helpers ──────────────────────────────────────────────────────────────────

// collectMessages drains a streaming RPC that returns pb.Message until EOF.
type msgStream interface {
	Recv() (*pb.Message, error)
}

func collectMessages(t *testing.T, stream msgStream) []*pb.Message {
	t.Helper()
	var out []*pb.Message
	for {
		m, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("stream.Recv: %v", err)
		}
		out = append(out, m)
	}
	return out
}
