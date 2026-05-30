package deliver_test

import (
	"bytes"
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-msgio"
	"go.uber.org/zap"

	pb "github.com/sahilpohare/p2p-a2a/gen/a2a/v1"
	"github.com/sahilpohare/p2p-a2a/daemon/blob"
	"github.com/sahilpohare/p2p-a2a/daemon/deliver"
	"github.com/sahilpohare/p2p-a2a/daemon/identity"
	"github.com/sahilpohare/p2p-a2a/daemon/inbox"
)

func newHost(t *testing.T) host.Host {
	t.Helper()
	h, err := libp2p.New(libp2p.ListenAddrStrings("/ip4/127.0.0.1/tcp/0"))
	if err != nil {
		t.Fatalf("libp2p.New: %v", err)
	}
	t.Cleanup(func() { h.Close() })
	return h
}

func connectHosts(t *testing.T, a, b host.Host) {
	t.Helper()
	if err := a.Connect(context.Background(), peer.AddrInfo{ID: b.ID(), Addrs: b.Addrs()}); err != nil {
		t.Fatalf("connect: %v", err)
	}
}

func newInbox(t *testing.T) *inbox.Inbox {
	t.Helper()
	ib, err := inbox.New(":memory:")
	if err != nil {
		t.Fatalf("inbox.New: %v", err)
	}
	t.Cleanup(func() { ib.Close() })
	return ib
}

// ─── tests ────────────────────────────────────────────────────────────────────

func TestSendDirect_DeliveredToInbox(t *testing.T) {
	log, _ := zap.NewDevelopment()

	senderHost := newHost(t)
	receiverHost := newHost(t)
	connectHosts(t, senderHost, receiverHost)

	receiverInbox := newInbox(t)

	// Register receive handler on receiver (nil registry — receiver never calls it)
	deliver.New(receiverHost, nil, receiverInbox, log)

	// Sender Deliverer (nil registry — using SendDirect)
	senderDlv := deliver.New(senderHost, nil, newInbox(t), log)

	senderID, err := identity.Generate()
	if err != nil {
		t.Fatalf("identity.Generate: %v", err)
	}

	msg := &pb.Message{
		Id:      "test-msg-001",
		FromDid: senderID.DID,
		ToDid:   "did:key:zReceiver",
		Kind:    pb.MessageKind_MESSAGE_KIND_TEXT,
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := senderDlv.SendDirect(ctx, receiverHost.ID(), msg); err != nil {
		t.Fatalf("SendDirect: %v", err)
	}

	msgs, err := receiverInbox.Get("", "", false, 0, 0)
	if err != nil {
		t.Fatalf("inbox.Get: %v", err)
	}
	if len(msgs) != 1 {
		t.Fatalf("expected 1 message in inbox, got %d", len(msgs))
	}
	if msgs[0].Id != "test-msg-001" {
		t.Errorf("message ID mismatch: %q", msgs[0].Id)
	}
	if msgs[0].FromDid != senderID.DID {
		t.Errorf("FromDid mismatch: %q", msgs[0].FromDid)
	}
}

func TestSendDirect_MultipleMessages(t *testing.T) {
	log, _ := zap.NewDevelopment()

	h1 := newHost(t)
	h2 := newHost(t)
	connectHosts(t, h1, h2)

	ib := newInbox(t)
	deliver.New(h2, nil, ib, log)
	dlv := deliver.New(h1, nil, newInbox(t), log)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	for i := 0; i < 3; i++ {
		msg := &pb.Message{
			Id:   fmt.Sprintf("msg-%d", i),
			Kind: pb.MessageKind_MESSAGE_KIND_TEXT,
		}
		if err := dlv.SendDirect(ctx, h2.ID(), msg); err != nil {
			t.Fatalf("SendDirect msg-%d: %v", i, err)
		}
	}

	msgs, _ := ib.Get("", "", false, 0, 0)
	if len(msgs) != 3 {
		t.Errorf("expected 3 messages in inbox, got %d", len(msgs))
	}
}

func TestSendDirect_UnknownPeer(t *testing.T) {
	log, _ := zap.NewDevelopment()
	h := newHost(t)
	dlv := deliver.New(h, nil, newInbox(t), log)

	// Random peer ID that we've never connected to
	unknownID, _ := peer.Decode("12D3KooWGHpBMeZbestVEWkfdnC9VX5XUZ8jKEAqoTVAsy3WP3DL")

	msg := &pb.Message{Id: "x", Kind: pb.MessageKind_MESSAGE_KIND_TEXT}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	if err := dlv.SendDirect(ctx, unknownID, msg); err == nil {
		t.Fatal("expected error delivering to unknown peer, got nil")
	}
}

func TestFetchBlob_RoundTrip(t *testing.T) {
	log, _ := zap.NewDevelopment()

	h1 := newHost(t)
	h2 := newHost(t)
	connectHosts(t, h1, h2)

	// h2 serves blobs
	bs, err := blob.New(t.TempDir())
	if err != nil {
		t.Fatalf("blob.New: %v", err)
	}
	data := bytes.Repeat([]byte("blobdata"), 10000) // >64KB, on disk
	artifact, err := bs.Put(data, "test.bin", "application/octet-stream")
	if err != nil {
		t.Fatalf("blob.Put: %v", err)
	}

	dlv2 := deliver.New(h2, nil, newInbox(t), log)
	dlv2.RegisterBlobHandler(bs)

	// h1 fetches from h2
	dlv1 := deliver.New(h1, nil, newInbox(t), log)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	got, err := dlv1.FetchBlob(ctx, h2.ID(), artifact.Cid)
	if err != nil {
		t.Fatalf("FetchBlob: %v", err)
	}
	if !bytes.Equal(got, data) {
		t.Error("fetched blob data mismatch")
	}
}

func TestFetchBlob_NotFound(t *testing.T) {
	log, _ := zap.NewDevelopment()

	h1 := newHost(t)
	h2 := newHost(t)
	connectHosts(t, h1, h2)

	emptyStore, _ := blob.New(t.TempDir())
	dlv2 := deliver.New(h2, nil, newInbox(t), log)
	dlv2.RegisterBlobHandler(emptyStore)

	dlv1 := deliver.New(h1, nil, newInbox(t), log)

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	_, err := dlv1.FetchBlob(ctx, h2.ID(), "sha256:doesnotexist")
	if err == nil {
		t.Fatal("expected error for missing blob, got nil")
	}
}

func TestReceive_MalformedData(t *testing.T) {
	log, _ := zap.NewDevelopment()
	senderHost := newHost(t)
	receiverHost := newHost(t)
	connectHosts(t, senderHost, receiverHost)

	receiverInbox := newInbox(t)
	deliver.New(receiverHost, nil, receiverInbox, log)

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	// Open raw stream and write garbage bytes (not valid protobuf)
	s, err := senderHost.NewStream(ctx, receiverHost.ID(), deliver.Protocol)
	if err != nil {
		t.Fatalf("NewStream: %v", err)
	}
	defer s.Close()

	w := msgio.NewWriter(s)
	w.WriteMsg([]byte("not valid protobuf!!!")) //nolint:errcheck

	// Read ack — should be 0x00 (rejected)
	ack := make([]byte, 1)
	s.SetDeadline(time.Now().Add(2 * time.Second)) //nolint:errcheck
	s.Read(ack)                                    //nolint:errcheck

	// Inbox should be empty
	msgs, _ := receiverInbox.Get("", "", false, 0, 0)
	if len(msgs) != 0 {
		t.Errorf("expected empty inbox after malformed message, got %d", len(msgs))
	}
}
