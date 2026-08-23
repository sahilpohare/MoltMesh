package threadcrypto

import (
	"strings"
	"testing"
)

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
	// Tamper inside the secret rather than at the very end of the string. The
	// checksum is four bytes rendered as six base64 characters, so its final
	// character carries two significant bits and four slack bits: substituting
	// it decodes to the same four bytes roughly a quarter of the time, which
	// made the original form of this assertion flaky.
	parts := strings.Split(encoded, ":")
	secretPart := []byte(parts[2])
	if secretPart[0] == 'A' {
		secretPart[0] = 'B'
	} else {
		secretPart[0] = 'A'
	}
	parts[2] = string(secretPart)
	if _, err := ParseRecoveryHandle(strings.Join(parts, ":")); err == nil {
		t.Fatal("tampered handle accepted")
	}

	// A corrupted checksum must also be rejected. Use the first checksum
	// character, where all six bits are significant.
	parts = strings.Split(encoded, ":")
	sumPart := []byte(parts[3])
	if sumPart[0] == 'A' {
		sumPart[0] = 'B'
	} else {
		sumPart[0] = 'A'
	}
	parts[3] = string(sumPart)
	if _, err := ParseRecoveryHandle(strings.Join(parts, ":")); err == nil {
		t.Fatal("handle with corrupted checksum accepted")
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
