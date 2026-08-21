package threadcrypto

import (
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"strings"
)

const RecoveryCommitmentMetadataKey = "recovery_capability_sha256"

// RecoveryCommitment is public, fixed-length validation material for a
// recovery handle. It is safe to place in the creator-signed descriptor: it
// reveals neither the 256-bit bearer secret nor a practical way to guess it.
func RecoveryCommitment(secret []byte) (string, error) {
	if len(secret) != 32 {
		return "", fmt.Errorf("recovery secret must be 32 bytes")
	}
	digest := sha256.Sum256(secret)
	return hex.EncodeToString(digest[:]), nil
}

func MatchesRecoveryCommitment(secret []byte, commitment string) bool {
	actual, err := RecoveryCommitment(secret)
	if err != nil || len(commitment) != sha256.Size*2 {
		return false
	}
	return subtle.ConstantTimeCompare([]byte(actual), []byte(commitment)) == 1
}

// RecoveryHandle is a bearer capability. The secret is deliberately absent
// from thread descriptors and discovery records; possession grants recovery
// read authority only when a recovery service is available.
type RecoveryHandle struct {
	ThreadID string
	Secret   []byte
	Version  uint32
}

const recoveryPrefix = "moltmesh-recovery-v1"

func NewRecoveryHandle(threadID string) (RecoveryHandle, error) {
	if threadID == "" {
		return RecoveryHandle{}, fmt.Errorf("recovery handle requires thread ID")
	}
	secret := make([]byte, 32)
	if _, err := rand.Read(secret); err != nil {
		return RecoveryHandle{}, err
	}
	return RecoveryHandle{ThreadID: threadID, Secret: secret, Version: 1}, nil
}

// String is safe for secret storage but must never be logged or published.
func (h RecoveryHandle) String() (string, error) {
	if h.Version != 1 || h.ThreadID == "" || len(h.Secret) != 32 {
		return "", fmt.Errorf("invalid recovery handle")
	}
	body := recoveryPrefix + ":" + h.ThreadID + ":" + base64.RawURLEncoding.EncodeToString(h.Secret)
	sum := sha256.Sum256([]byte(body))
	return body + ":" + base64.RawURLEncoding.EncodeToString(sum[:4]), nil
}

func ParseRecoveryHandle(encoded string) (RecoveryHandle, error) {
	parts := strings.Split(encoded, ":")
	if len(parts) != 4 || parts[0] != recoveryPrefix || parts[1] == "" {
		return RecoveryHandle{}, fmt.Errorf("invalid recovery handle format")
	}
	body := strings.Join(parts[:3], ":")
	checksum, err := base64.RawURLEncoding.DecodeString(parts[3])
	if err != nil || len(checksum) != 4 {
		return RecoveryHandle{}, fmt.Errorf("invalid recovery handle checksum")
	}
	sum := sha256.Sum256([]byte(body))
	if subtle.ConstantTimeCompare(checksum, sum[:4]) != 1 {
		return RecoveryHandle{}, fmt.Errorf("recovery handle checksum mismatch")
	}
	secret, err := base64.RawURLEncoding.DecodeString(parts[2])
	if err != nil || len(secret) != 32 {
		return RecoveryHandle{}, fmt.Errorf("invalid recovery handle secret")
	}
	return RecoveryHandle{ThreadID: parts[1], Secret: secret, Version: 1}, nil
}
