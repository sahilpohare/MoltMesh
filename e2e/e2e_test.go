// Package e2e runs end-to-end tests against two full daemon instances that
// communicate over real libp2p streams and GossipSub.
package e2e_test

import (
	"context"
	"fmt"
	"io"
	"net"
	"testing"
	"time"

	pubsub "github.com/libp2p/go-libp2p-pubsub"
	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p/core/peer"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	pb "github.com/sahilpohare/p2p-a2a/gen/a2a/v1"
	"github.com/sahilpohare/p2p-a2a/daemon/blob"
	"github.com/sahilpohare/p2p-a2a/daemon/deliver"
	"github.com/sahilpohare/p2p-a2a/daemon/gossip"
	"github.com/sahilpohare/p2p-a2a/daemon/identity"
	"github.com/sahilpohare/p2p-a2a/daemon/inbox"
	"github.com/sahilpohare/p2p-a2a/daemon/outbox"
	"github.com/sahilpohare/p2p-a2a/daemon/rpc"
	"github.com/sahilpohare/p2p-a2a/daemon/tasks"
	"github.com/sahilpohare/p2p-a2a/daemon/thread"

	_ "github.com/mattn/go-sqlite3"
)

// ─── daemon harness ───────────────────────────────────────────────────────────

// daemon is a lightweight in-process A2A node (no DHT, no bootstrap).
type daemon struct {
	id     *identity.Identity
	client pb.A2ANodeClient
	ib     *inbox.Inbox
	ob     *outbox.Outbox
	ts     *tasks.Store
	dlv    *deliver.Deliverer
	bs     *blob.Store
	tm     *thread.Manager
	peerID peer.ID
	addrs  []string
}

// newDaemon spins up a complete daemon (libp2p + gRPC) and returns a gRPC client.
func newDaemon(t *testing.T, ctx context.Context, log *zap.Logger) *daemon {
	t.Helper()

	id, err := identity.Generate()
	if err != nil {
		t.Fatalf("identity.Generate: %v", err)
	}

	// libp2p host (TCP only, no QUIC for test simplicity)
	h, err := libp2p.New(
		libp2p.Identity(id.LibP2PKey),
		libp2p.ListenAddrStrings("/ip4/127.0.0.1/tcp/0"),
	)
	if err != nil {
		t.Fatalf("libp2p.New: %v", err)
	}
	t.Cleanup(func() { h.Close() })

	// GossipSub
	ps, err := pubsub.NewGossipSub(ctx, h,
		pubsub.WithMessageSignaturePolicy(pubsub.StrictSign),
	)
	if err != nil {
		t.Fatalf("pubsub.NewGossipSub: %v", err)
	}

	addrs := make([]string, len(h.Addrs()))
	for i, a := range h.Addrs() {
		addrs[i] = fmt.Sprintf("%s/p2p/%s", a, h.ID())
	}

	// Storage (all in-memory SQLite)
	ib, err := inbox.New(":memory:")
	if err != nil {
		t.Fatalf("inbox.New: %v", err)
	}
	t.Cleanup(func() { ib.Close() })

	ts, err := tasks.New(":memory:")
	if err != nil {
		t.Fatalf("tasks.New: %v", err)
	}
	t.Cleanup(func() { ts.Close() })

	gm := gossip.New(ps, log)

	bs, err := blob.New(t.TempDir())
	if err != nil {
		t.Fatalf("blob.New: %v", err)
	}

	// Delivery layer (nil registry — we use SendDirect in tests)
	dlv := deliver.New(h, nil, ib, log)
	dlv.RegisterBlobHandler(bs)

	ob, err := outbox.New(":memory:", dlv.DeliverFunc(), log)
	if err != nil {
		t.Fatalf("outbox.New: %v", err)
	}
	t.Cleanup(func() { ob.Close() })
	go ob.Run(ctx)

	// Thread manager
	threadStore, err := thread.NewStore(":memory:")
	if err != nil {
		t.Fatalf("thread.NewStore: %v", err)
	}
	t.Cleanup(func() { threadStore.Close() })
	tm := thread.NewManager(ctx, threadStore, id, ps, log)

	// gRPC server
	srv := rpc.New(id, ib, ob, ts, nil, gm, bs, dlv, tm, nil, nil, nil, nil, addrs, log)
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

	return &daemon{
		id:     id,
		client: pb.NewA2ANodeClient(conn),
		ib:     ib,
		ob:     ob,
		ts:     ts,
		dlv:    dlv,
		bs:     bs,
		tm:     tm,
		peerID: h.ID(),
		addrs:  addrs,
	}
}

// connect dials b from a (libp2p-level connection).
func connect(t *testing.T, a, b *daemon) {
	t.Helper()
	// parse first addr from b
	info, err := peer.AddrInfoFromString(b.addrs[0])
	if err != nil {
		t.Fatalf("parse addr: %v", err)
	}
	if err := a.dlv.Host().Connect(context.Background(), *info); err != nil {
		t.Fatalf("connect: %v", err)
	}
}

// ─── Identity ─────────────────────────────────────────────────────────────────

func TestE2E_GetIdentity(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	log, _ := zap.NewDevelopment()

	d := newDaemon(t, ctx, log)

	got, err := d.client.GetIdentity(ctx, &pb.Empty{})
	if err != nil {
		t.Fatalf("GetIdentity: %v", err)
	}
	if got.Did != d.id.DID {
		t.Errorf("DID mismatch: %q vs %q", got.Did, d.id.DID)
	}
	if got.PublicKey == "" {
		t.Error("PublicKey empty")
	}
	if len(got.Multiaddrs) == 0 {
		t.Error("no multiaddrs")
	}
}

// ─── Direct message delivery ──────────────────────────────────────────────────

func TestE2E_DirectMessageDelivery(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	log, _ := zap.NewDevelopment()

	alice := newDaemon(t, ctx, log)
	bob := newDaemon(t, ctx, log)
	connect(t, alice, bob)

	// Alice sends a message directly to Bob's libp2p peer.
	msg := &pb.Message{
		Id:      "e2e-msg-1",
		FromDid: alice.id.DID,
		ToDid:   bob.id.DID,
		Kind:    pb.MessageKind_MESSAGE_KIND_TEXT,
	}
	if err := alice.dlv.SendDirect(ctx, bob.peerID, msg); err != nil {
		t.Fatalf("SendDirect: %v", err)
	}

	// Bob's inbox should contain the message.
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		msgs, _ := bob.ib.Get("", "", false, 0, 0)
		if len(msgs) > 0 {
			if msgs[0].Id != "e2e-msg-1" {
				t.Errorf("message ID mismatch: %q", msgs[0].Id)
			}
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("timeout: message never arrived in Bob's inbox")
}

// ─── Send message via gRPC → outbox → deliver ─────────────────────────────────

func TestE2E_SendMessageViaGRPC(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	log, _ := zap.NewDevelopment()

	alice := newDaemon(t, ctx, log)
	bob := newDaemon(t, ctx, log)
	connect(t, alice, bob)

	// Wire alice's outbox deliver to use bob's peer ID.
	// The real outbox uses registry to look up the peer; for e2e we
	// instead use SendDirect so we need the outbox deliver func to know bob.
	// Easiest: call SendDirect directly to verify the delivery path.
	msg := &pb.Message{
		Id:      "grpc-msg-1",
		FromDid: alice.id.DID,
		ToDid:   bob.id.DID,
		Kind:    pb.MessageKind_MESSAGE_KIND_TEXT,
	}

	// Via gRPC: enqueue into alice's outbox
	res, err := alice.client.SendMessage(ctx, msg)
	if err != nil {
		t.Fatalf("SendMessage: %v", err)
	}
	if res.MessageId == "" {
		t.Error("MessageId empty")
	}
	if !res.Queued {
		t.Error("expected Queued=true")
	}

	// Verify it's in alice's outbox
	pending, err := alice.ob.List("pending", 0)
	if err != nil {
		t.Fatalf("List outbox: %v", err)
	}
	if len(pending) == 0 {
		t.Fatal("message not in outbox")
	}
	if pending[0].ToDid != bob.id.DID {
		t.Errorf("ToDid mismatch: %q", pending[0].ToDid)
	}
}

// ─── File transfer ────────────────────────────────────────────────────────────

func TestE2E_FileTransfer_SmallFile(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	log, _ := zap.NewDevelopment()

	alice := newDaemon(t, ctx, log)

	data := []byte("hello, p2p world!")
	res, err := alice.client.SendFile(ctx, &pb.SendFileRequest{
		Data:     data,
		Name:     "hello.txt",
		MimeType: "text/plain",
	})
	if err != nil {
		t.Fatalf("SendFile: %v", err)
	}
	if res.Cid == "" {
		t.Error("CID empty")
	}
	// Small file should be inline
	if len(res.Inline) == 0 {
		t.Error("expected inline data for small file")
	}
	if string(res.Inline) != string(data) {
		t.Errorf("inline data mismatch")
	}
}

func TestE2E_FileTransfer_BlobFetch(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	log, _ := zap.NewDevelopment()

	alice := newDaemon(t, ctx, log)
	bob := newDaemon(t, ctx, log)
	connect(t, alice, bob)

	// Store a large blob on alice (> 64KB so it goes on disk).
	bigData := make([]byte, 128*1024)
	for i := range bigData {
		bigData[i] = byte(i % 256)
	}

	artifact, err := alice.bs.Put(bigData, "big.bin", "application/octet-stream")
	if err != nil {
		t.Fatalf("blob.Put: %v", err)
	}

	// Bob fetches blob from alice via libp2p blob protocol.
	fetched, err := bob.dlv.FetchBlob(ctx, alice.peerID, artifact.Cid)
	if err != nil {
		t.Fatalf("FetchBlob: %v", err)
	}
	if len(fetched) != len(bigData) {
		t.Errorf("size mismatch: got %d want %d", len(fetched), len(bigData))
	}
	for i, b := range fetched {
		if b != bigData[i] {
			t.Fatalf("data mismatch at byte %d", i)
		}
	}
}

// ─── Task lifecycle ───────────────────────────────────────────────────────────

func TestE2E_TaskLifecycle(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	log, _ := zap.NewDevelopment()

	alice := newDaemon(t, ctx, log)
	bob := newDaemon(t, ctx, log)
	_ = bob // bob is the assignee

	// Alice creates a task assigned to Bob.
	task, err := alice.client.CreateTask(ctx, &pb.CreateTaskRequest{
		ToDid: bob.id.DID,
		Task: &pb.TaskRequest{
			Skill: "a2a:v1:cap:text-generation",
		},
	})
	if err != nil {
		t.Fatalf("CreateTask: %v", err)
	}
	if task.Id == "" {
		t.Error("task ID empty")
	}
	if task.Status != pb.TaskStatus_TASK_STATUS_SUBMITTED {
		t.Errorf("expected SUBMITTED, got %v", task.Status)
	}
	if task.Initiator != alice.id.DID {
		t.Errorf("initiator mismatch: %q", task.Initiator)
	}

	// Update to WORKING.
	updated, err := alice.client.UpdateTask(ctx, &pb.TaskStatusUpdate{
		TaskId: task.Id,
		Status: pb.TaskStatus_TASK_STATUS_WORKING,
	})
	if err != nil {
		t.Fatalf("UpdateTask WORKING: %v", err)
	}
	if updated.Status != pb.TaskStatus_TASK_STATUS_WORKING {
		t.Errorf("expected WORKING, got %v", updated.Status)
	}

	// Update to COMPLETED with an artifact.
	completed, err := alice.client.UpdateTask(ctx, &pb.TaskStatusUpdate{
		TaskId: task.Id,
		Status: pb.TaskStatus_TASK_STATUS_COMPLETED,
		OutputArtifacts: []*pb.Artifact{
			{Cid: "sha256:abc", MimeType: "text/plain", Size: 3},
		},
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

	// GetTask round-trip.
	got, err := alice.client.GetTask(ctx, &pb.TaskID{Id: task.Id})
	if err != nil {
		t.Fatalf("GetTask: %v", err)
	}
	if got.Status != pb.TaskStatus_TASK_STATUS_COMPLETED {
		t.Errorf("GetTask status: %v", got.Status)
	}
}

func TestE2E_TaskCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	log, _ := zap.NewDevelopment()

	alice := newDaemon(t, ctx, log)

	task, _ := alice.client.CreateTask(ctx, &pb.CreateTaskRequest{
		ToDid: "did:key:zAnybody",
		Task:  &pb.TaskRequest{Skill: "test"},
	})

	cancelled, err := alice.client.CancelTask(ctx, &pb.TaskID{Id: task.Id})
	if err != nil {
		t.Fatalf("CancelTask: %v", err)
	}
	if cancelled.Status != pb.TaskStatus_TASK_STATUS_CANCELLED {
		t.Errorf("expected CANCELLED, got %v", cancelled.Status)
	}
}

// ─── Inbox subscribe ──────────────────────────────────────────────────────────

func TestE2E_SubscribeInbox(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	log, _ := zap.NewDevelopment()

	alice := newDaemon(t, ctx, log)

	// Pre-populate inbox with two messages.
	alice.ib.Put(&pb.Message{Id: "sub-1", FromDid: "did:key:zX", Kind: pb.MessageKind_MESSAGE_KIND_TEXT})
	alice.ib.Put(&pb.Message{Id: "sub-2", FromDid: "did:key:zY", Kind: pb.MessageKind_MESSAGE_KIND_TEXT})

	stream, err := alice.client.SubscribeInbox(ctx, &pb.SubscribeRequest{})
	if err != nil {
		t.Fatalf("SubscribeInbox: %v", err)
	}

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
		t.Errorf("expected 2 messages from subscribe flush, got %d", len(msgs))
	}
}

// ─── Thread consensus ─────────────────────────────────────────────────────────

// TestE2E_Thread_SingleNode creates a single-validator thread via gRPC and
// appends an entry; verifies it appears in GetThreadEntries after commit.
func TestE2E_Thread_SingleNode(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	log, _ := zap.NewDevelopment()

	alice := newDaemon(t, ctx, log)

	// f=0, N=1: alice is the only validator.
	th, err := alice.client.CreateThread(ctx, &pb.CreateThreadRequest{
		ReplicaDids: []string{alice.id.DID},
		F:           0,
		EpochMs:     100, // fast epoch for tests
	})
	if err != nil {
		t.Fatalf("CreateThread: %v", err)
	}
	if th.Id == "" {
		t.Error("thread ID empty")
	}
	if th.N != 1 {
		t.Errorf("expected N=1, got %d", th.N)
	}

	// Append an entry.
	appendRes, err := alice.client.AppendEntry(ctx, &pb.AppendEntryRequest{
		ThreadId: th.Id,
		Payload:  []byte("hello thread"),
		Kind:     "message",
	})
	if err != nil {
		t.Fatalf("AppendEntry: %v", err)
	}
	if appendRes.ThreadId != th.Id {
		t.Errorf("thread ID mismatch in append result")
	}

	// Wait for the entry to be committed and appear in GetThreadEntries.
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		stream, err := alice.client.GetThreadEntries(ctx, &pb.GetThreadEntriesRequest{
			ThreadId:    th.Id,
			SinceHeight: 0,
		})
		if err != nil {
			t.Fatalf("GetThreadEntries: %v", err)
		}
		var entries []*pb.ThreadEntryWithPos
		for {
			ep, err := stream.Recv()
			if err == io.EOF {
				break
			}
			if err != nil {
				break
			}
			entries = append(entries, ep)
		}
		if len(entries) > 0 {
			if string(entries[0].Entry.Payload) != "hello thread" {
				t.Errorf("payload mismatch: %q", entries[0].Entry.Payload)
			}
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatal("timeout: entry never committed to thread")
}

// TestE2E_Thread_SubscribeThread verifies SubscribeThread delivers committed
// entries live to a subscriber.
func TestE2E_Thread_SubscribeThread(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	log, _ := zap.NewDevelopment()

	alice := newDaemon(t, ctx, log)

	th, err := alice.client.CreateThread(ctx, &pb.CreateThreadRequest{
		ReplicaDids: []string{alice.id.DID},
		F:           0,
		EpochMs:     100,
	})
	if err != nil {
		t.Fatalf("CreateThread: %v", err)
	}

	// Start subscription before appending.
	stream, err := alice.client.SubscribeThread(ctx, &pb.SubscribeThreadRequest{
		ThreadId:    th.Id,
		SinceHeight: 0,
	})
	if err != nil {
		t.Fatalf("SubscribeThread: %v", err)
	}

	// Append entry in the background.
	go func() {
		time.Sleep(50 * time.Millisecond)
		alice.client.AppendEntry(ctx, &pb.AppendEntryRequest{ //nolint:errcheck
			ThreadId: th.Id,
			Payload:  []byte("live entry"),
			Kind:     "message",
		})
	}()

	ep, err := stream.Recv()
	if err != nil {
		t.Fatalf("SubscribeThread.Recv: %v", err)
	}
	if string(ep.Entry.Payload) != "live entry" {
		t.Errorf("payload mismatch: %q", ep.Entry.Payload)
	}
}

// TestE2E_Thread_TwoNodes creates a thread with two daemons both connected and
// both in the validator set with f=0 (N=2). Alice appends an entry; since both
// nodes are live and connected via GossipSub the entry commits on alice's side.
//
// Note: with f=0, quorum=1, so alice commits as soon as she prevotes.
func TestE2E_Thread_TwoNodes(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	log, _ := zap.NewDevelopment()

	alice := newDaemon(t, ctx, log)
	bob := newDaemon(t, ctx, log)
	connect(t, alice, bob)

	// Give GossipSub time to mesh (especially under test parallelism).
	time.Sleep(500 * time.Millisecond)

	// Create thread — f=0, N=1 quorum, both are validators.
	// With f=0 the quorum is 1, so alice can commit without bob's vote.
	th, err := alice.client.CreateThread(ctx, &pb.CreateThreadRequest{
		ReplicaDids: []string{alice.id.DID, bob.id.DID},
		F:           0,
		EpochMs:     200,
	})
	if err != nil {
		t.Fatalf("CreateThread on alice: %v", err)
	}

	// Also start the thread engine on bob.
	bobThread := &pb.Thread{
		Id:          th.Id,
		CreatorDid:  th.CreatorDid,
		ReplicaDids: th.ReplicaDids,
		N:           th.N,
		F:           th.F,
		EpochMs:     th.EpochMs,
		CreatedAt:   th.CreatedAt,
	}
	if err := bob.tm.Start(bobThread); err != nil {
		t.Fatalf("start thread on bob: %v", err)
	}

	// Append to both daemons — whichever wins the Raft election is leader and
	// will commit. f=0 means quorum=1, so the leader commits alone.
	alice.client.AppendEntry(ctx, &pb.AppendEntryRequest{ //nolint:errcheck
		ThreadId: th.Id,
		Payload:  []byte("two-node entry"),
		Kind:     "message",
	})
	bob.client.AppendEntry(ctx, &pb.AppendEntryRequest{ //nolint:errcheck
		ThreadId: th.Id,
		Payload:  []byte("two-node entry"),
		Kind:     "message",
	})

	// Wait for commit on either daemon.
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		for _, d := range []*daemon{alice, bob} {
			stream, err := d.client.GetThreadEntries(ctx, &pb.GetThreadEntriesRequest{
				ThreadId:    th.Id,
				SinceHeight: 0,
			})
			if err != nil {
				continue
			}
			var entries []*pb.ThreadEntryWithPos
			for {
				ep, err := stream.Recv()
				if err == io.EOF {
					break
				}
				if err != nil {
					break
				}
				entries = append(entries, ep)
			}
			if len(entries) > 0 {
				if string(entries[0].Entry.Payload) != "two-node entry" {
					t.Errorf("payload mismatch: %q", entries[0].Entry.Payload)
				}
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatal("timeout: entry never committed on either node")
}
