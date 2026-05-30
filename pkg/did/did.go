// Package did provides utilities for working with did:key identifiers.
package did

import (
	"fmt"
	"strings"
)

const (
	schemePrefix = "did:"
	keyMethod    = "did:key:"
	keyPrefix    = "did:key:z" // z = base58btc multibase prefix
)

// Validate returns an error if s is not a well-formed did:key.
func Validate(s string) error {
	if !strings.HasPrefix(s, keyPrefix) {
		return fmt.Errorf("not a did:key (must start with %q): %q", keyPrefix, s)
	}
	key := s[len(keyPrefix):]
	if len(key) < 44 { // base58 of 2+32 bytes is ~46 chars; 44 is a safe lower bound
		return fmt.Errorf("did:key key material too short: %q", s)
	}
	for _, c := range key {
		if !isBase58Char(c) {
			return fmt.Errorf("invalid base58 character %q in did:key", c)
		}
	}
	return nil
}

// IsValid reports whether s is a valid did:key.
func IsValid(s string) bool { return Validate(s) == nil }

// Method returns the DID method (e.g. "key" from "did:key:z6Mk...").
// Returns "" if s is not a well-formed DID.
func Method(s string) string {
	if !strings.HasPrefix(s, schemePrefix) {
		return ""
	}
	rest := s[len(schemePrefix):]
	idx := strings.IndexByte(rest, ':')
	if idx < 0 {
		return rest
	}
	return rest[:idx]
}

// KeyMaterial returns the raw key string after "did:key:z" (base58btc-encoded bytes).
// Returns "" if s is not a did:key.
func KeyMaterial(s string) string {
	if !strings.HasPrefix(s, keyPrefix) {
		return ""
	}
	return s[len(keyPrefix):]
}

// Short returns a shortened display form suitable for terminal output:
//   "did:key:z6MkhaX…r2jP"  (prefix + 6 chars + … + 4 chars)
//
// If s is shorter than the threshold, it is returned unchanged.
func Short(s string) string {
	const threshold = 24
	if len(s) <= threshold {
		return s
	}
	// keep "did:key:z" + 6 key chars + … + last 4 chars
	head := s[:len(keyPrefix)+6]
	tail := s[len(s)-4:]
	return head + "\u2026" + tail // "…"
}

// Equal reports whether a and b refer to the same DID (case-sensitive, exact).
func Equal(a, b string) bool { return a == b }

// isBase58Char reports whether r is in the Bitcoin base58 alphabet.
func isBase58Char(r rune) bool {
	const alpha = "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz"
	for _, c := range alpha {
		if r == c {
			return true
		}
	}
	return false
}
