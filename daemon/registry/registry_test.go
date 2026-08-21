package registry

import (
	"encoding/base64"
	"testing"
	"time"

	"github.com/sahilpohare/p2p-a2a/daemon/identity"
	pb "github.com/sahilpohare/p2p-a2a/gen/a2a/v1"
)

func signedTestCard(t *testing.T) *pb.AgentCard {
	t.Helper()
	id, err := identity.Generate()
	if err != nil {
		t.Fatal(err)
	}
	card := &pb.AgentCard{
		Did: id.DID, PublicKey: id.PublicKeyBase64(), Name: "agent",
		Multiaddrs:  []string{"/ip4/127.0.0.1/tcp/1"},
		Skills:      []*pb.Skill{{Id: "cap:test", Name: "test"}},
		PublishedAt: time.Now().UnixMilli(), ExpiresAt: time.Now().Add(time.Hour).UnixMilli(),
	}
	canonical, err := cardCanonical(card)
	if err != nil {
		t.Fatal(err)
	}
	card.Signature = base64.StdEncoding.EncodeToString(id.Sign(canonical))
	return card
}

func TestVerifyCardRejectsRoutingMutation(t *testing.T) {
	card := signedTestCard(t)
	card.Multiaddrs[0] = "/ip4/203.0.113.1/tcp/9999"
	if err := verifyCard(card); err == nil {
		t.Fatal("expected mutated multiaddr to invalidate signature")
	}
}

func TestVerifyCardRejectsSkillMutation(t *testing.T) {
	card := signedTestCard(t)
	card.Skills[0].Name = "forged"
	if err := verifyCard(card); err == nil {
		t.Fatal("expected mutated skill to invalidate signature")
	}
}
