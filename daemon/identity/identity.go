package identity

import (
	"crypto/ed25519"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"os"

	"github.com/mr-tron/base58"
	"github.com/multiformats/go-multiaddr"
	crypto "github.com/libp2p/go-libp2p/core/crypto"
)

const (
	// multicodec prefix for ed25519 public key: 0xed01
	ed25519Prefix = "\xed\x01"
)

// Identity holds the agent's DID, keypair, and signing capability.
type Identity struct {
	DID        string
	PublicKey  ed25519.PublicKey
	PrivateKey ed25519.PrivateKey
	LibP2PKey  crypto.PrivKey
}

// Generate creates a new Ed25519 identity and derives a did:key DID.
func Generate() (*Identity, error) {
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		return nil, fmt.Errorf("generate keypair: %w", err)
	}
	return fromKeys(pub, priv)
}

// Load reads a persisted identity from a JSON file.
func Load(path string) (*Identity, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read identity file: %w", err)
	}
	var stored struct {
		PrivateKey string `json:"private_key"`
	}
	if err := json.Unmarshal(data, &stored); err != nil {
		return nil, fmt.Errorf("parse identity file: %w", err)
	}
	privBytes, err := base64.StdEncoding.DecodeString(stored.PrivateKey)
	if err != nil {
		return nil, fmt.Errorf("decode private key: %w", err)
	}
	priv := ed25519.PrivateKey(privBytes)
	pub := priv.Public().(ed25519.PublicKey)
	return fromKeys(pub, priv)
}

// Save persists the identity to a JSON file (mode 0600).
func (id *Identity) Save(path string) error {
	stored := struct {
		DID        string `json:"did"`
		PrivateKey string `json:"private_key"`
	}{
		DID:        id.DID,
		PrivateKey: base64.StdEncoding.EncodeToString(id.PrivateKey),
	}
	data, err := json.MarshalIndent(stored, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, data, 0600)
}

// Sign signs a message with the agent's private key.
func (id *Identity) Sign(msg []byte) []byte {
	return ed25519.Sign(id.PrivateKey, msg)
}

// Verify verifies a signature against this identity's public key.
func (id *Identity) Verify(msg, sig []byte) bool {
	return ed25519.Verify(id.PublicKey, msg, sig)
}

// PublicKeyBase64 returns the base64-encoded public key.
func (id *Identity) PublicKeyBase64() string {
	return base64.StdEncoding.EncodeToString(id.PublicKey)
}

// MultiaddrString returns the agent's libp2p peer ID as a multiaddr string.
func (id *Identity) PeerMultiaddr(listenAddr multiaddr.Multiaddr) string {
	return listenAddr.String()
}

// ─── internal ────────────────────────────────────────────────────────────────

func fromKeys(pub ed25519.PublicKey, priv ed25519.PrivateKey) (*Identity, error) {
	did := didKeyFromPub(pub)

	// wrap for libp2p (KeyPairFromStdKey requires *ed25519.PrivateKey)
	lp2pPriv, _, err := crypto.KeyPairFromStdKey(&priv)
	if err != nil {
		return nil, fmt.Errorf("wrap libp2p key: %w", err)
	}

	return &Identity{
		DID:        did,
		PublicKey:  pub,
		PrivateKey: priv,
		LibP2PKey:  lp2pPriv,
	}, nil
}

// didKeyFromPub derives a did:key from an Ed25519 public key.
// Format: did:key:z<base58btc(varint(0xed01) + pubkey)>
func didKeyFromPub(pub ed25519.PublicKey) string {
	// multicodec-prefixed key: 0xed 0x01 followed by raw pubkey bytes
	prefixed := append([]byte(ed25519Prefix), pub...)
	encoded := base58.Encode(prefixed)
	return "did:key:z" + encoded
}

// PubKeyFromDID extracts the Ed25519 public key from a did:key DID.
func PubKeyFromDID(did string) (ed25519.PublicKey, error) {
	const prefix = "did:key:z"
	if len(did) < len(prefix) || did[:len(prefix)] != prefix {
		return nil, fmt.Errorf("invalid did:key format")
	}
	decoded, err := base58.Decode(did[len(prefix):])
	if err != nil {
		return nil, fmt.Errorf("base58 decode: %w", err)
	}
	if len(decoded) < 2+ed25519.PublicKeySize {
		return nil, fmt.Errorf("did:key too short")
	}
	// strip 2-byte multicodec prefix
	return ed25519.PublicKey(decoded[2:]), nil
}

// VerifyWithPub verifies a signature directly against a public key.
func VerifyWithPub(pub ed25519.PublicKey, msg, sig []byte) bool {
	return ed25519.Verify(pub, msg, sig)
}

// VerifyDID verifies a signature using the public key embedded in a did:key.
func VerifyDID(did string, msg, sig []byte) (bool, error) {
	pub, err := PubKeyFromDID(did)
	if err != nil {
		return false, err
	}
	return ed25519.Verify(pub, msg, sig), nil
}
