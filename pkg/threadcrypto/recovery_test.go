package threadcrypto

import "testing"

func TestRecoveryHandleRoundTripAndChecksum(t *testing.T) {
	h, err := NewRecoveryHandle("thread-1")
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := h.String()
	if err != nil {
		t.Fatal(err)
	}
	got, err := ParseRecoveryHandle(encoded)
	if err != nil || got.ThreadID != h.ThreadID || string(got.Secret) != string(h.Secret) {
		t.Fatalf("round trip = %#v, %v", got, err)
	}
	last := "A"
	if encoded[len(encoded)-1:] == "A" {
		last = "B"
	}
	if _, err := ParseRecoveryHandle(encoded[:len(encoded)-1] + last); err == nil {
		t.Fatal("tampered handle accepted")
	}
}

func TestRecoveryCommitmentValidatesWithoutRetainingSecret(t *testing.T) {
	secret := make([]byte, 32)
	secret[0] = 7
	commitment, err := RecoveryCommitment(secret)
	if err != nil {
		t.Fatal(err)
	}
	if !MatchesRecoveryCommitment(secret, commitment) {
		t.Fatal("valid secret did not match commitment")
	}
	secret[0] = 8
	if MatchesRecoveryCommitment(secret, commitment) {
		t.Fatal("wrong secret matched commitment")
	}
}
