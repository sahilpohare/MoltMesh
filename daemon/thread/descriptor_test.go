package thread

import (
	"encoding/base64"
	"testing"

	"github.com/sahilpohare/p2p-a2a/daemon/identity"
	pb "github.com/sahilpohare/p2p-a2a/gen/a2a/v1"
)

func TestThreadDescriptorSignatureCoversMembership(t *testing.T) {
	id, err := identity.Generate()
	if err != nil {
		t.Fatal(err)
	}
	th := &pb.Thread{Id: "thread-1", CreatorDid: id.DID, ReplicaDids: []string{id.DID}, N: 1, Metadata: map[string]string{"backend": "raft"}}
	if err := SignDescriptor(th, id); err != nil {
		t.Fatal(err)
	}
	if err := VerifyDescriptor(th); err != nil {
		t.Fatalf("valid descriptor: %v", err)
	}
	th.ReplicaDids = append(th.ReplicaDids, "did:key:zForged")
	if err := VerifyDescriptor(th); err == nil {
		t.Fatal("membership mutation did not invalidate creator signature")
	}
}

func TestSignedThreadCreationPreservesSDKCreator(t *testing.T) {
	creator, err := identity.Generate()
	if err != nil {
		t.Fatal(err)
	}
	req := &pb.CreateThreadRequest{
		ThreadId: "sdk-thread", CreatorDid: creator.DID, CreatedAt: 1234,
		ReplicaDids: []string{creator.DID}, EpochMs: 200,
		Metadata: map[string]string{"backend": "raft"},
	}
	unsigned, err := newThreadFromRequest(creator.DID, req, req.ThreadId, req.CreatedAt)
	if err != nil {
		t.Fatal(err)
	}
	if err := SignDescriptor(unsigned, creator); err != nil {
		t.Fatal(err)
	}
	req.CreatorSignature, err = base64.StdEncoding.DecodeString(unsigned.Metadata[descriptorSignatureKey])
	if err != nil {
		t.Fatal(err)
	}
	created, err := NewThreadFromSignedRequest(req)
	if err != nil {
		t.Fatalf("valid SDK descriptor rejected: %v", err)
	}
	if created.CreatorDid != creator.DID || created.Id != req.ThreadId {
		t.Fatalf("created descriptor = %#v", created)
	}
	req.ReplicaDids = append(req.ReplicaDids, "did:key:zForged")
	if _, err := NewThreadFromSignedRequest(req); err == nil {
		t.Fatal("modified signed descriptor was accepted")
	}
}
