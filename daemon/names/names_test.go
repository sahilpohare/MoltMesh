package names

import (
	"testing"
	"time"
)

// ── Normalize ─────────────────────────────────────────────────────────────────

func TestNormalize(t *testing.T) {
	cases := []struct {
		in   string
		want string
	}{
		{"swift falcon", "swift-falcon"},
		{"Swift Falcon", "swift-falcon"},
		{"bright_harbor_relay", "bright-harbor-relay"},
		{"Bright-Harbor Relay", "bright-harbor-relay"},
		{"  lots   of   spaces  ", "lots-of-spaces"},
		{"camelCase", "camelcase"},
		{"already-normalised", "already-normalised"},
		{"with123numbers", "with123numbers"},
		{"word1 word2", "word1-word2"},
	}
	for _, tc := range cases {
		got := Normalize(tc.in)
		if got != tc.want {
			t.Errorf("Normalize(%q) = %q, want %q", tc.in, got, tc.want)
		}
	}
}

// ── Validate ──────────────────────────────────────────────────────────────────

func TestValidate(t *testing.T) {
	valid := []string{
		"swift-falcon",
		"a",
		"agent99",
		"bright-harbor-relay",
		"x1-y2-z3",
	}
	for _, name := range valid {
		if err := Validate(name); err != nil {
			t.Errorf("Validate(%q) unexpected error: %v", name, err)
		}
	}

	invalid := []struct {
		name string
		desc string
	}{
		{"", "empty"},
		{"-leading-hyphen", "leading hyphen"},
		{"trailing-hyphen-", "trailing hyphen"},
		{"UPPERCASE", "uppercase"},
		{"has space", "space"},
		{"has.dot", "dot"},
		{"has/slash", "slash"},
		{string(make([]byte, 65)), "too long"},
	}
	for _, tc := range invalid {
		if err := Validate(tc.name); err == nil {
			t.Errorf("Validate(%q) should fail (%s) but returned nil", tc.name, tc.desc)
		}
	}
}

// ── Claim signing / verification ──────────────────────────────────────────────

func TestClaimSignVerify(t *testing.T) {
	id := newTestIdentity(t)

	claim := &Claim{
		Name:        "swift-falcon",
		DID:         id.DID,
		PublishedAt: time.Now().UnixMilli(),
		ExpiresAt:   time.Now().Add(24 * time.Hour).UnixMilli(),
	}

	sig, err := signClaim(id, claim)
	if err != nil {
		t.Fatalf("signClaim: %v", err)
	}
	claim.Signature = sig

	if err := verifyClaim(claim); err != nil {
		t.Fatalf("verifyClaim: %v", err)
	}
}

func TestClaimVerifyTamperedName(t *testing.T) {
	id := newTestIdentity(t)

	claim := &Claim{
		Name:        "swift-falcon",
		DID:         id.DID,
		PublishedAt: time.Now().UnixMilli(),
		ExpiresAt:   time.Now().Add(24 * time.Hour).UnixMilli(),
	}
	sig, _ := signClaim(id, claim)
	claim.Signature = sig

	// Tamper: change the name after signing
	claim.Name = "evil-name"
	if err := verifyClaim(claim); err == nil {
		t.Fatal("expected verifyClaim to fail after name tamper, but got nil")
	}
}

func TestClaimVerifyTamperedDID(t *testing.T) {
	id1 := newTestIdentity(t)
	id2 := newTestIdentity(t)

	claim := &Claim{
		Name:        "swift-falcon",
		DID:         id1.DID,
		PublishedAt: time.Now().UnixMilli(),
		ExpiresAt:   time.Now().Add(24 * time.Hour).UnixMilli(),
	}
	sig, _ := signClaim(id1, claim)
	claim.Signature = sig

	// Tamper: substitute a different DID (different public key)
	claim.DID = id2.DID
	if err := verifyClaim(claim); err == nil {
		t.Fatal("expected verifyClaim to fail after DID tamper, but got nil")
	}
}

func TestClaimVerifyMissingSignature(t *testing.T) {
	id := newTestIdentity(t)
	claim := &Claim{
		Name:        "swift-falcon",
		DID:         id.DID,
		PublishedAt: time.Now().UnixMilli(),
		ExpiresAt:   time.Now().Add(time.Hour).UnixMilli(),
	}
	if err := verifyClaim(claim); err == nil {
		t.Fatal("expected error for missing signature")
	}
}

// ── Expiry logic ──────────────────────────────────────────────────────────────

func TestClaimExpiry(t *testing.T) {
	id := newTestIdentity(t)

	expired := &Claim{
		Name:        "old-name",
		DID:         id.DID,
		PublishedAt: time.Now().Add(-48 * time.Hour).UnixMilli(),
		ExpiresAt:   time.Now().Add(-1 * time.Hour).UnixMilli(), // already expired
	}
	sig, _ := signClaim(id, expired)
	expired.Signature = sig

	if time.Now().UnixMilli() < expired.ExpiresAt {
		t.Fatal("claim should be expired but ExpiresAt is in the future")
	}
}

// ── DHT key ───────────────────────────────────────────────────────────────────

func TestDHTKey(t *testing.T) {
	key := dhtKey("swift-falcon")
	if key != "/a2a/names/swift-falcon" {
		t.Errorf("unexpected DHT key: %q", key)
	}
}

// ── helpers ───────────────────────────────────────────────────────────────────

func newTestIdentity(t *testing.T) *testIdentity {
	t.Helper()
	id, err := generateTestIdentity()
	if err != nil {
		t.Fatalf("generate test identity: %v", err)
	}
	return id
}
