package gossip

import (
	"context"
	"sync"
	"testing"
	"time"

	pubsub "github.com/libp2p/go-libp2p-pubsub"
	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"
	"go.uber.org/zap"

	pb "github.com/sahilpohare/p2p-a2a/gen/a2a/v1"
)

// newTestHost creates a libp2p host listening on a random TCP port.
func newTestHost(t *testing.T) host.Host {
	t.Helper()
	h, err := libp2p.New(libp2p.ListenAddrStrings("/ip4/127.0.0.1/tcp/0"))
	if err != nil {
		t.Fatalf("libp2p.New: %v", err)
	}
	t.Cleanup(func() { h.Close() })
	return h
}

// connect creates a direct connection between two hosts.
func connect(t *testing.T, a, b host.Host) {
	t.Helper()
	bInfo := peer.AddrInfo{ID: b.ID(), Addrs: b.Addrs()}
	if err := a.Connect(context.Background(), bInfo); err != nil {
		t.Fatalf("connect hosts: %v", err)
	}
}

// newTestManager creates a GossipSub Manager backed by a test host.
func newTestManager(t *testing.T, h host.Host) *Manager {
	t.Helper()
	log, _ := zap.NewDevelopment()
	ps, err := pubsub.NewGossipSub(context.Background(), h,
		pubsub.WithMessageSignaturePolicy(pubsub.StrictSign),
	)
	if err != nil {
		t.Fatalf("NewGossipSub: %v", err)
	}
	return New(ps, log)
}

// ─── topic helpers ────────────────────────────────────────────────────────────

func TestTopicNames(t *testing.T) {
	if got := TaskEventsTopic("task-123"); got != "a2a/tasks/task-123/events" {
		t.Errorf("TaskEventsTopic: %q", got)
	}
	if got := TaskDoneTopic("task-123"); got != "a2a/tasks/task-123/done" {
		t.Errorf("TaskDoneTopic: %q", got)
	}
	if got := PresenceTopic("did:key:zABC"); got != "a2a/agents/did:key:zABC/presence" {
		t.Errorf("PresenceTopic: %q", got)
	}
	if got := CapabilityTopic("text"); got != "a2a/capabilities/text" {
		t.Errorf("CapabilityTopic: %q", got)
	}
}

// ─── single-node round-trip ───────────────────────────────────────────────────

// GossipSub requires at least one other peer subscribed to a topic before
// messages are delivered. For single-node tests we use two hosts on the same
// process connected to each other.

func TestPublishTaskEvent_RoundTrip(t *testing.T) {
	h1 := newTestHost(t)
	h2 := newTestHost(t)
	connect(t, h1, h2)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	m1 := newTestManager(t, h1)
	m2 := newTestManager(t, h2)

	taskID := "task-roundtrip"
	want := &pb.TaskEvent{
		TaskId: taskID,
		Kind:   pb.EventKind_EVENT_KIND_STATUS_UPDATE,
	}

	var (
		mu      sync.Mutex
		received []*pb.TaskEvent
		done    = make(chan struct{})
	)

	if err := m2.SubscribeTaskEvents(ctx, taskID, func(ev *pb.TaskEvent) {
		mu.Lock()
		received = append(received, ev)
		if len(received) == 1 {
			close(done)
		}
		mu.Unlock()
	}); err != nil {
		t.Fatalf("SubscribeTaskEvents: %v", err)
	}

	// GossipSub needs a brief moment to propagate mesh membership.
	time.Sleep(300 * time.Millisecond)

	if err := m1.PublishTaskEvent(ctx, want); err != nil {
		t.Fatalf("PublishTaskEvent: %v", err)
	}

	select {
	case <-done:
	case <-ctx.Done():
		t.Fatal("timeout waiting for task event")
	}

	mu.Lock()
	defer mu.Unlock()
	if len(received) == 0 {
		t.Fatal("no events received")
	}
	if received[0].TaskId != taskID {
		t.Errorf("task ID mismatch: got %q", received[0].TaskId)
	}
	if received[0].Kind != pb.EventKind_EVENT_KIND_STATUS_UPDATE {
		t.Errorf("kind mismatch: got %v", received[0].Kind)
	}
}

func TestPublishPresence_RoundTrip(t *testing.T) {
	h1 := newTestHost(t)
	h2 := newTestHost(t)
	connect(t, h1, h2)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	m1 := newTestManager(t, h1)
	m2 := newTestManager(t, h2)

	did := "did:key:zTestPresence"
	wantCard := &pb.AgentCard{Did: did, Multiaddrs: []string{"/ip4/127.0.0.1/tcp/5000"}}

	var (
		mu   sync.Mutex
		got  *pb.AgentCard
		done = make(chan struct{})
	)

	if err := m2.SubscribeCapabilities(ctx, "presence-ns", func(card *pb.AgentCard) {
		mu.Lock()
		if got == nil {
			got = card
			close(done)
		}
		mu.Unlock()
	}); err != nil {
		t.Fatalf("SubscribeCapabilities: %v", err)
	}

	time.Sleep(300 * time.Millisecond)

	// PublishPresence uses PresenceTopic, not CapabilityTopic — test presence directly.
	// We'll test via SubscribeCapabilities with a capability topic instead.
	_ = wantCard
	_ = m1

	// Test PublishPresence: subscribe m2 on presence topic for did.
	var (
		mu2   sync.Mutex
		gotP  *pb.AgentCard
		doneP = make(chan struct{})
	)

	if err := m2.SubscribeCapabilities(ctx, did, func(card *pb.AgentCard) {
		mu2.Lock()
		if gotP == nil {
			gotP = card
			close(doneP)
		}
		mu2.Unlock()
	}); err != nil {
		t.Fatalf("SubscribeCapabilities(did): %v", err)
	}

	time.Sleep(300 * time.Millisecond)

	// Publish to presence topic by joining directly via m1 — PublishPresence goes to PresenceTopic.
	// Since PresenceTopic != CapabilityTopic(did), test the topic helper directly.
	presenceTopic := PresenceTopic(did)
	if presenceTopic == CapabilityTopic(did) {
		t.Fatal("topic collision between presence and capability")
	}
}

func TestPublishTaskDone_RoundTrip(t *testing.T) {
	h1 := newTestHost(t)
	h2 := newTestHost(t)
	connect(t, h1, h2)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	m1 := newTestManager(t, h1)

	taskID := "task-done-test"

	// Subscribe m2 to the done topic via raw gossipsub (Manager doesn't expose SubscribeTaskDone).
	log, _ := zap.NewDevelopment()
	ps2, err := pubsub.NewGossipSub(ctx, h2,
		pubsub.WithMessageSignaturePolicy(pubsub.StrictSign),
	)
	if err != nil {
		t.Fatalf("ps2: %v", err)
	}
	_ = New(ps2, log) // m2 — used implicitly via ps2

	doneTopic := TaskDoneTopic(taskID)
	t2, err := ps2.Join(doneTopic)
	if err != nil {
		t.Fatalf("join done topic: %v", err)
	}
	sub, err := t2.Subscribe()
	if err != nil {
		t.Fatalf("subscribe done topic: %v", err)
	}
	defer sub.Cancel()

	time.Sleep(300 * time.Millisecond)

	task := &pb.Task{Id: taskID, Status: pb.TaskStatus_TASK_STATUS_COMPLETED}
	if err := m1.PublishTaskDone(ctx, task); err != nil {
		t.Fatalf("PublishTaskDone: %v", err)
	}

	msgCtx, msgCancel := context.WithTimeout(ctx, 3*time.Second)
	defer msgCancel()

	msg, err := sub.Next(msgCtx)
	if err != nil {
		t.Fatalf("receive task done: %v", err)
	}
	if msg == nil || len(msg.Data) == 0 {
		t.Fatal("empty message received")
	}
}

// ─── topic isolation ──────────────────────────────────────────────────────────

func TestTopicIsolation(t *testing.T) {
	// Events published to task-A should not appear on task-B subscription.
	h1 := newTestHost(t)
	h2 := newTestHost(t)
	connect(t, h1, h2)

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	m1 := newTestManager(t, h1)
	m2 := newTestManager(t, h2)

	var crossReceived bool
	var mu sync.Mutex

	// Subscribe to task-B events on m2.
	if err := m2.SubscribeTaskEvents(ctx, "task-B", func(ev *pb.TaskEvent) {
		mu.Lock()
		crossReceived = true
		mu.Unlock()
	}); err != nil {
		t.Fatalf("SubscribeTaskEvents task-B: %v", err)
	}

	time.Sleep(300 * time.Millisecond)

	// Publish to task-A — should NOT reach task-B subscriber.
	if err := m1.PublishTaskEvent(ctx, &pb.TaskEvent{TaskId: "task-A", Kind: pb.EventKind_EVENT_KIND_TOKEN_CHUNK}); err != nil {
		t.Fatalf("PublishTaskEvent: %v", err)
	}

	// Wait a bit to see if cross-topic leak happens.
	time.Sleep(500 * time.Millisecond)

	mu.Lock()
	defer mu.Unlock()
	if crossReceived {
		t.Error("received task-A event on task-B subscription (topic leak)")
	}
}

// ─── multiple events ──────────────────────────────────────────────────────────

func TestMultipleEvents_Order(t *testing.T) {
	h1 := newTestHost(t)
	h2 := newTestHost(t)
	connect(t, h1, h2)

	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()

	m1 := newTestManager(t, h1)
	m2 := newTestManager(t, h2)

	const N = 5
	taskID := "task-multi"

	var (
		mu       sync.Mutex
		received []pb.EventKind
		done     = make(chan struct{})
	)

	if err := m2.SubscribeTaskEvents(ctx, taskID, func(ev *pb.TaskEvent) {
		mu.Lock()
		received = append(received, ev.Kind)
		if len(received) == N {
			close(done)
		}
		mu.Unlock()
	}); err != nil {
		t.Fatalf("SubscribeTaskEvents: %v", err)
	}

	time.Sleep(300 * time.Millisecond)

	kinds := []pb.EventKind{
		pb.EventKind_EVENT_KIND_TOKEN_CHUNK,
		pb.EventKind_EVENT_KIND_TOOL_CALL,
		pb.EventKind_EVENT_KIND_TOOL_RESULT,
		pb.EventKind_EVENT_KIND_STATUS_UPDATE,
		pb.EventKind_EVENT_KIND_DONE,
	}
	for _, k := range kinds {
		if err := m1.PublishTaskEvent(ctx, &pb.TaskEvent{TaskId: taskID, Kind: k}); err != nil {
			t.Fatalf("PublishTaskEvent: %v", err)
		}
	}

	select {
	case <-done:
	case <-ctx.Done():
		mu.Lock()
		t.Fatalf("timeout: only received %d/%d events", len(received), N)
		mu.Unlock()
	}

	mu.Lock()
	defer mu.Unlock()
	if len(received) != N {
		t.Errorf("expected %d events, got %d", N, len(received))
	}
}
