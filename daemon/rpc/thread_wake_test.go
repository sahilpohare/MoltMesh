package rpc

import (
	"bytes"
	"context"
	"strings"
	"testing"
	"time"

	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"

	"github.com/sahilpohare/p2p-a2a/daemon/identity"
	"github.com/sahilpohare/p2p-a2a/daemon/outbox"
	threadpkg "github.com/sahilpohare/p2p-a2a/daemon/thread"
	pb "github.com/sahilpohare/p2p-a2a/gen/a2a/v1"
	"github.com/sahilpohare/p2p-a2a/pkg/threadcrypto"
)

type wakeThreadManager struct {
	thread            *pb.Thread
	members           []*pb.ThreadMember
	epoch             uint64
	height            int64
	headHash          string
	envelopes         []*pb.ThreadKeyEnvelope
	recoverySecret    []byte
	recoveryEnvelopes []*pb.ThreadKeyEnvelope
}

func (m *wakeThreadManager) CreateThread(context.Context, *pb.CreateThreadRequest) (*pb.Thread, error) {
	return m.thread, nil
}
func (m *wakeThreadManager) GetThread(string) (*pb.Thread, error)      { return m.thread, nil }
func (m *wakeThreadManager) AppendEntry(string, *pb.ThreadEntry) error { return nil }
func (m *wakeThreadManager) GetEntries(string, int64, int) ([]*pb.ThreadEntryWithPos, error) {
	return nil, nil
}
func (m *wakeThreadManager) Engine(string) threadpkg.ThreadSubscription     { return nil }
func (m *wakeThreadManager) ListMembers(string) ([]*pb.ThreadMember, error) { return m.members, nil }
func (m *wakeThreadManager) MembershipEpoch(string) (uint64, error)         { return m.epoch, nil }
func (m *wakeThreadManager) CommittedHead(string) (int64, string, error) {
	return m.height, m.headHash, nil
}
func (m *wakeThreadManager) SaveKeyEnvelope(envelope *pb.ThreadKeyEnvelope) error {
	m.envelopes = append(m.envelopes, envelope)
	return nil
}
func (m *wakeThreadManager) KeyEnvelopes(string, uint64, string) ([]*pb.ThreadKeyEnvelope, error) {
	return nil, nil
}
func (m *wakeThreadManager) SaveRecoveryKeyEnvelope(envelope *pb.ThreadKeyEnvelope) error {
	m.recoveryEnvelopes = append(m.recoveryEnvelopes, envelope)
	return nil
}
func (m *wakeThreadManager) RecoveryKeyEnvelopes(string) ([]*pb.ThreadKeyEnvelope, error) {
	return m.recoveryEnvelopes, nil
}
func (m *wakeThreadManager) AuthorizeRecoveryCapability(_ string, secret []byte) (bool, error) {
	return bytes.Equal(m.recoverySecret, secret), nil
}

func TestAppendEntryEnqueuesDurableReplicaWake(t *testing.T) {
	id, err := identity.Generate()
	if err != nil {
		t.Fatal(err)
	}
	log := zap.NewNop()
	ob, err := outbox.New(":memory:", func(context.Context, *pb.Message) error { return nil }, log)
	if err != nil {
		t.Fatal(err)
	}
	defer ob.Close()
	mgr := &wakeThreadManager{thread: &pb.Thread{
		Id: "thread-wake", ReplicaDids: []string{id.DID, "did:key:zRemote"},
	}}
	srv := New(id, nil, ob, nil, nil, nil, nil, mgr, nil, nil, nil, nil, nil, nil, log)
	if _, err := srv.AppendEntry(context.Background(), &pb.AppendEntryRequest{ThreadId: mgr.thread.Id, Payload: []byte("wake"), Kind: "message"}); err != nil {
		t.Fatalf("AppendEntry: %v", err)
	}
	messages, err := ob.List("pending", 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(messages) != 1 || messages[0].Kind != pb.MessageKind_MESSAGE_KIND_THREAD_INVITE || messages[0].ThreadId != mgr.thread.Id {
		t.Fatalf("expected one durable thread wake, got %#v", messages)
	}
}

func TestAppendEncryptedEntryRequiresCurrentMemberAndEpoch(t *testing.T) {
	id, err := identity.Generate()
	if err != nil {
		t.Fatal(err)
	}
	log := zap.NewNop()
	ob, err := outbox.New(":memory:", func(context.Context, *pb.Message) error { return nil }, log)
	if err != nil {
		t.Fatal(err)
	}
	defer ob.Close()
	mgr := &wakeThreadManager{
		thread:  &pb.Thread{Id: "encrypted-thread", ReplicaDids: []string{id.DID}},
		members: []*pb.ThreadMember{{Did: id.DID, Role: pb.ThreadMemberRole_THREAD_MEMBER_ROLE_ADMIN}},
		epoch:   3,
	}
	srv := New(id, nil, ob, nil, nil, nil, nil, mgr, nil, nil, nil, nil, nil, nil, log)
	makeEntry := func(membershipEpoch, encryptionEpoch uint64) *pb.ThreadEntry {
		t.Helper()
		entry := &pb.ThreadEntry{AuthorDid: id.DID, Payload: []byte("ciphertext"), Kind: "message", EncodingVersion: 2, Sequence: 1, MembershipEpoch: membershipEpoch, EncryptionEpoch: encryptionEpoch, Nonce: make([]byte, 24)}
		sig, err := threadcrypto.Sign(id.PrivateKey, threadcrypto.Header{ThreadID: mgr.thread.Id, Sequence: entry.Sequence, AuthorDID: entry.AuthorDid, Kind: entry.Kind, MembershipEpoch: entry.MembershipEpoch, EncryptionEpoch: entry.EncryptionEpoch, Nonce: entry.Nonce}, entry.Payload)
		if err != nil {
			t.Fatal(err)
		}
		entry.AuthorSignature = sig
		return entry
	}

	if _, err := srv.AppendEntry(context.Background(), &pb.AppendEntryRequest{ThreadId: mgr.thread.Id, EncryptedEntry: makeEntry(3, 3)}); err != nil {
		t.Fatalf("current encrypted member was rejected: %v", err)
	}
	if _, err := srv.AppendEntry(context.Background(), &pb.AppendEntryRequest{ThreadId: mgr.thread.Id, EncryptedEntry: makeEntry(2, 2)}); err == nil || !strings.Contains(err.Error(), "current membership") {
		t.Fatalf("stale epoch error = %v", err)
	}
	mgr.members = nil
	if _, err := srv.AppendEntry(context.Background(), &pb.AppendEntryRequest{ThreadId: mgr.thread.Id, EncryptedEntry: makeEntry(3, 3)}); err == nil || !strings.Contains(err.Error(), "not a current thread member") {
		t.Fatalf("non-member error = %v", err)
	}
}

func TestPutThreadKeyEnvelopeRequiresCurrentEpoch(t *testing.T) {
	id, err := identity.Generate()
	if err != nil {
		t.Fatal(err)
	}
	mgr := &wakeThreadManager{
		thread:  &pb.Thread{Id: "envelope-thread", CreatorDid: id.DID},
		members: []*pb.ThreadMember{{Did: id.DID, Role: pb.ThreadMemberRole_THREAD_MEMBER_ROLE_ADMIN}},
		epoch:   4,
	}
	srv := New(id, nil, nil, nil, nil, nil, nil, mgr, nil, nil, nil, nil, nil, nil, zap.NewNop())
	valid := &pb.ThreadKeyEnvelope{ThreadId: mgr.thread.Id, EncryptionEpoch: 4, RecipientDid: id.DID, EphemeralPublicKey: make([]byte, 32), Nonce: make([]byte, 24), Ciphertext: []byte("opaque")}
	if _, err := srv.PutThreadKeyEnvelope(context.Background(), valid); err != nil {
		t.Fatalf("current envelope was rejected: %v", err)
	}
	if len(mgr.envelopes) != 1 {
		t.Fatalf("stored envelopes = %d", len(mgr.envelopes))
	}
	stale := proto.Clone(valid).(*pb.ThreadKeyEnvelope)
	stale.EncryptionEpoch = 3
	if _, err := srv.PutThreadKeyEnvelope(context.Background(), stale); err == nil || !strings.Contains(err.Error(), "current encryption epoch") {
		t.Fatalf("stale envelope error = %v", err)
	}
}

func TestRecoveryKeyEnvelopesRequireRecoveryCapability(t *testing.T) {
	id, err := identity.Generate()
	if err != nil {
		t.Fatal(err)
	}
	secret := bytes.Repeat([]byte{7}, 32)
	mgr := &wakeThreadManager{thread: &pb.Thread{Id: "recovery-thread", CreatorDid: id.DID}, epoch: 1, recoverySecret: secret}
	srv := New(id, nil, nil, nil, nil, nil, nil, mgr, nil, nil, nil, nil, nil, nil, zap.NewNop())
	envelope := &pb.ThreadKeyEnvelope{ThreadId: mgr.thread.Id, EncryptionEpoch: 1, EphemeralPublicKey: make([]byte, 32), Nonce: make([]byte, 24), Ciphertext: []byte("opaque"), RecoveryEnvelope: true}
	raw, err := proto.MarshalOptions{Deterministic: true}.Marshal(envelope)
	if err != nil {
		t.Fatal(err)
	}
	envelope.AuthorSignature = id.Sign(raw)
	if _, err := srv.PutThreadKeyEnvelope(context.Background(), envelope); err != nil {
		t.Fatalf("publish recovery envelope: %v", err)
	}
	handle := &pb.ThreadRecoveryHandle{ThreadId: mgr.thread.Id, RecoverySecret: secret, Version: 1}
	got, err := srv.GetRecoveryThreadKeyEnvelopes(context.Background(), &pb.RecoverThreadRequest{Handle: handle})
	if err != nil || len(got.Envelopes) != 1 || !got.Envelopes[0].RecoveryEnvelope {
		t.Fatalf("recovery envelopes = %#v, %v", got, err)
	}
	bad := proto.Clone(handle).(*pb.ThreadRecoveryHandle)
	bad.RecoverySecret[0] ^= 1
	if _, err := srv.GetRecoveryThreadKeyEnvelopes(context.Background(), &pb.RecoverThreadRequest{Handle: bad}); err == nil || !strings.Contains(err.Error(), "capability") {
		t.Fatalf("invalid recovery handle error = %v", err)
	}
}

func TestCatchupProofMustMatchSignedCurrentHead(t *testing.T) {
	id, err := identity.Generate()
	if err != nil {
		t.Fatal(err)
	}
	mgr := &wakeThreadManager{thread: &pb.Thread{Id: "catchup-thread", CreatorDid: id.DID}, height: 5, headHash: "head"}
	srv := New(id, nil, nil, nil, nil, nil, nil, mgr, nil, nil, nil, nil, nil, nil, zap.NewNop())
	proof := &pb.ThreadCatchupProof{ThreadId: mgr.thread.Id, ObserverDid: id.DID, CommittedHeight: 5, HeadBlockHash: "head", IssuedAtUnixMs: time.Now().UnixMilli()}
	data, err := proto.MarshalOptions{Deterministic: true}.Marshal(proof)
	if err != nil {
		t.Fatal(err)
	}
	proof.Signature = id.Sign(data)
	raw, err := proto.Marshal(proof)
	if err != nil {
		t.Fatal(err)
	}
	if err := srv.verifyCatchupProof(mgr.thread.Id, id.DID, raw); err != nil {
		t.Fatalf("valid proof rejected: %v", err)
	}
	proof.HeadBlockHash = "forged"
	raw, _ = proto.Marshal(proof)
	if err := srv.verifyCatchupProof(mgr.thread.Id, id.DID, raw); err == nil || !strings.Contains(err.Error(), "signature") {
		t.Fatalf("forged proof error = %v", err)
	}
}
