package names

import (
	"crypto/ed25519"
	"crypto/rand"
	"fmt"

	"github.com/mr-tron/base58"
	"github.com/sahilpohare/p2p-a2a/daemon/identity"
)

// testIdentity is a minimal wrapper so tests can call signClaim / verifyClaim
// without a full daemon identity (which requires libp2p keys).
// We use the real identity package for DID generation but expose DID + Sign.
type testIdentity = identity.Identity

func generateTestIdentity() (*identity.Identity, error) {
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		return nil, fmt.Errorf("generate keypair: %w", err)
	}
	// Build a did:key manually (same logic as identity.didKeyFromPub)
	const ed25519Prefix = "\xed\x01"
	prefixed := append([]byte(ed25519Prefix), pub...)
	did := "did:key:z" + base58.Encode(prefixed)

	return &identity.Identity{
		DID:        did,
		PublicKey:  pub,
		PrivateKey: priv,
	}, nil
}
