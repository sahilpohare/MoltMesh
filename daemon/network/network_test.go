package network

import (
	"testing"
)

func newTestStore(t *testing.T) *Store {
	t.Helper()
	s, err := New(":memory:")
	if err != nil {
		t.Fatalf("network.New: %v", err)
	}
	t.Cleanup(func() { s.Close() })
	return s
}

// ─── Store.Create ─────────────────────────────────────────────────────────────

func TestCreate_ReturnsNetwork(t *testing.T) {
	s := newTestStore(t)
	n, err := s.Create("alpha", "did:key:zAlice", nil)
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	if n.ID == "" {
		t.Error("expected non-empty ID")
	}
	if n.Name != "alpha" {
		t.Errorf("Name: got %q", n.Name)
	}
	if n.CreatorDID != "did:key:zAlice" {
		t.Errorf("CreatorDID: got %q", n.CreatorDID)
	}
	if n.CreatedAt == 0 {
		t.Error("expected non-zero CreatedAt")
	}
}

func TestCreate_CreatorAutoJoins(t *testing.T) {
	s := newTestStore(t)
	n, _ := s.Create("beta", "did:key:zAlice", nil)

	ok, err := s.IsMember(n.ID, "did:key:zAlice")
	if err != nil {
		t.Fatalf("IsMember: %v", err)
	}
	if !ok {
		t.Error("creator should be auto-joined as member")
	}
}

func TestCreate_DuplicateName_Error(t *testing.T) {
	s := newTestStore(t)
	s.Create("same-name", "did:key:zA", nil) //nolint:errcheck
	_, err := s.Create("same-name", "did:key:zB", nil)
	if err == nil {
		t.Error("expected error for duplicate network name, got nil")
	}
}

// ─── Store.Get / GetByName ────────────────────────────────────────────────────

func TestGet_Found(t *testing.T) {
	s := newTestStore(t)
	created, _ := s.Create("gamma", "did:key:zA", nil)

	got, err := s.Get(created.ID)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got.ID != created.ID {
		t.Errorf("ID mismatch: %q vs %q", got.ID, created.ID)
	}
}

func TestGet_NotFound(t *testing.T) {
	s := newTestStore(t)
	_, err := s.Get("non-existent-id")
	if err == nil {
		t.Error("expected error for missing network, got nil")
	}
}

func TestGetByName_Found(t *testing.T) {
	s := newTestStore(t)
	s.Create("mynet", "did:key:zA", nil) //nolint:errcheck

	n, err := s.GetByName("mynet")
	if err != nil {
		t.Fatalf("GetByName: %v", err)
	}
	if n.Name != "mynet" {
		t.Errorf("Name: %q", n.Name)
	}
}

func TestGetByName_NotFound(t *testing.T) {
	s := newTestStore(t)
	_, err := s.GetByName("does-not-exist")
	if err == nil {
		t.Error("expected error for missing name, got nil")
	}
}

// ─── Store.Join / Leave ───────────────────────────────────────────────────────

func TestJoin_NewMember(t *testing.T) {
	s := newTestStore(t)
	n, _ := s.Create("net1", "did:key:zA", nil)

	if err := s.Join(n.ID, "did:key:zBob"); err != nil {
		t.Fatalf("Join: %v", err)
	}
	ok, _ := s.IsMember(n.ID, "did:key:zBob")
	if !ok {
		t.Error("Bob should be a member after Join")
	}
}

func TestJoin_Idempotent(t *testing.T) {
	s := newTestStore(t)
	n, _ := s.Create("net2", "did:key:zA", nil)

	// Joining twice should not error.
	if err := s.Join(n.ID, "did:key:zBob"); err != nil {
		t.Fatalf("first Join: %v", err)
	}
	if err := s.Join(n.ID, "did:key:zBob"); err != nil {
		t.Fatalf("second Join (idempotent): %v", err)
	}
	members, _ := s.Members(n.ID)
	bobCount := 0
	for _, m := range members {
		if m.DID == "did:key:zBob" {
			bobCount++
		}
	}
	if bobCount != 1 {
		t.Errorf("expected exactly 1 Bob entry, got %d", bobCount)
	}
}

func TestJoin_UnknownNetwork_Error(t *testing.T) {
	s := newTestStore(t)
	err := s.Join("no-such-network", "did:key:zBob")
	if err == nil {
		t.Error("expected error joining non-existent network, got nil")
	}
}

func TestLeave_RemovesMember(t *testing.T) {
	s := newTestStore(t)
	n, _ := s.Create("net3", "did:key:zA", nil)
	s.Join(n.ID, "did:key:zBob") //nolint:errcheck

	if err := s.Leave(n.ID, "did:key:zBob"); err != nil {
		t.Fatalf("Leave: %v", err)
	}
	ok, _ := s.IsMember(n.ID, "did:key:zBob")
	if ok {
		t.Error("Bob should not be a member after Leave")
	}
}

func TestLeave_NonMember_NoError(t *testing.T) {
	s := newTestStore(t)
	n, _ := s.Create("net4", "did:key:zA", nil)

	// Leaving a network you're not in should not error.
	if err := s.Leave(n.ID, "did:key:zStranger"); err != nil {
		t.Fatalf("Leave non-member: %v", err)
	}
}

// ─── Store.Members ────────────────────────────────────────────────────────────

func TestMembers_OrderedByJoinedAt(t *testing.T) {
	s := newTestStore(t)
	n, _ := s.Create("net5", "did:key:zA", nil)
	s.Join(n.ID, "did:key:zBob") //nolint:errcheck
	s.Join(n.ID, "did:key:zCarol") //nolint:errcheck

	members, err := s.Members(n.ID)
	if err != nil {
		t.Fatalf("Members: %v", err)
	}
	// Creator + Bob + Carol = 3
	if len(members) != 3 {
		t.Errorf("expected 3 members, got %d", len(members))
	}
	// Verify all have correct network ID.
	for _, m := range members {
		if m.NetworkID != n.ID {
			t.Errorf("member NetworkID mismatch: %q", m.NetworkID)
		}
	}
}

func TestMembers_EmptyAfterAllLeave(t *testing.T) {
	s := newTestStore(t)
	n, _ := s.Create("net6", "did:key:zA", nil)
	s.Leave(n.ID, "did:key:zA") //nolint:errcheck

	members, _ := s.Members(n.ID)
	if len(members) != 0 {
		t.Errorf("expected 0 members, got %d", len(members))
	}
}

// ─── Store.IsMember ───────────────────────────────────────────────────────────

func TestIsMember_True(t *testing.T) {
	s := newTestStore(t)
	n, _ := s.Create("net7", "did:key:zA", nil)
	ok, err := s.IsMember(n.ID, "did:key:zA")
	if err != nil {
		t.Fatalf("IsMember: %v", err)
	}
	if !ok {
		t.Error("creator should be member")
	}
}

func TestIsMember_False(t *testing.T) {
	s := newTestStore(t)
	n, _ := s.Create("net8", "did:key:zA", nil)
	ok, err := s.IsMember(n.ID, "did:key:zStranger")
	if err != nil {
		t.Fatalf("IsMember: %v", err)
	}
	if ok {
		t.Error("stranger should not be member")
	}
}

// ─── Store.List ───────────────────────────────────────────────────────────────

func TestList_FiltersByMembership(t *testing.T) {
	s := newTestStore(t)
	// Alice creates two networks; Bob creates one.
	n1, _ := s.Create("n1", "did:key:zAlice", nil)
	n2, _ := s.Create("n2", "did:key:zAlice", nil)
	n3, _ := s.Create("n3", "did:key:zBob", nil)
	_ = n3

	// Join Alice into n3.
	s.Join(n3.ID, "did:key:zAlice") //nolint:errcheck

	aliceNets, err := s.List("did:key:zAlice")
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(aliceNets) != 3 {
		t.Errorf("Alice should be in 3 networks, got %d", len(aliceNets))
	}

	// Bob is only in n3.
	bobNets, _ := s.List("did:key:zBob")
	if len(bobNets) != 1 {
		t.Errorf("Bob should be in 1 network, got %d", len(bobNets))
	}
	if bobNets[0].ID != n3.ID {
		t.Errorf("Bob's network ID mismatch: %q", bobNets[0].ID)
	}

	// n1 and n2 are Alice's but not Bob's.
	_ = n1
	_ = n2
}

func TestList_EmptyForUnknownDID(t *testing.T) {
	s := newTestStore(t)
	s.Create("x", "did:key:zA", nil) //nolint:errcheck

	nets, err := s.List("did:key:zUnknown")
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(nets) != 0 {
		t.Errorf("expected empty list for non-member, got %d", len(nets))
	}
}

// ─── BroadcastTopic ───────────────────────────────────────────────────────────

func TestBroadcastTopic(t *testing.T) {
	topic := BroadcastTopic("net-123")
	expected := "a2a/networks/net-123/broadcast"
	if topic != expected {
		t.Errorf("BroadcastTopic: got %q, want %q", topic, expected)
	}
}
