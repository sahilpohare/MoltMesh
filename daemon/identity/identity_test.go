package identity

import (
	"os"
	"path/filepath"
	"testing"
)

func TestGenerate(t *testing.T) {
	id, err := Generate()
	if err != nil {
		t.Fatalf("Generate: %v", err)
	}
	if id.DID == "" {
		t.Fatal("DID is empty")
	}
	if len(id.PublicKey) == 0 {
		t.Fatal("PublicKey is empty")
	}
	if len(id.PrivateKey) == 0 {
		t.Fatal("PrivateKey is empty")
	}
	if id.LibP2PKey == nil {
		t.Fatal("LibP2PKey is nil")
	}
}

func TestDIDFormat(t *testing.T) {
	id, err := Generate()
	if err != nil {
		t.Fatal(err)
	}
	const prefix = "did:key:z"
	if len(id.DID) < len(prefix) || id.DID[:len(prefix)] != prefix {
		t.Fatalf("DID %q does not start with %q", id.DID, prefix)
	}
}

func TestDIDUniqueness(t *testing.T) {
	a, _ := Generate()
	b, _ := Generate()
	if a.DID == b.DID {
		t.Fatal("two generated identities have the same DID")
	}
}

func TestSignAndVerify(t *testing.T) {
	id, err := Generate()
	if err != nil {
		t.Fatal(err)
	}
	msg := []byte("hello p2p-a2a")
	sig := id.Sign(msg)
	if !id.Verify(msg, sig) {
		t.Fatal("Verify returned false for valid signature")
	}
	// tampered message
	if id.Verify([]byte("tampered"), sig) {
		t.Fatal("Verify returned true for tampered message")
	}
	// tampered signature
	tampered := make([]byte, len(sig))
	copy(tampered, sig)
	tampered[0] ^= 0xFF
	if id.Verify(msg, tampered) {
		t.Fatal("Verify returned true for tampered signature")
	}
}

func TestPubKeyFromDID(t *testing.T) {
	id, err := Generate()
	if err != nil {
		t.Fatal(err)
	}
	recovered, err := PubKeyFromDID(id.DID)
	if err != nil {
		t.Fatalf("PubKeyFromDID: %v", err)
	}
	if string(recovered) != string(id.PublicKey) {
		t.Fatal("recovered public key does not match original")
	}
}

func TestPubKeyFromDID_Invalid(t *testing.T) {
	cases := []string{
		"",
		"did:web:example.com",
		"did:key:",
		"did:key:zinvalid!!!",
	}
	for _, c := range cases {
		if _, err := PubKeyFromDID(c); err == nil {
			t.Errorf("PubKeyFromDID(%q) expected error, got nil", c)
		}
	}
}

func TestVerifyDID(t *testing.T) {
	id, err := Generate()
	if err != nil {
		t.Fatal(err)
	}
	msg := []byte("verify via DID")
	sig := id.Sign(msg)

	ok, err := VerifyDID(id.DID, msg, sig)
	if err != nil {
		t.Fatalf("VerifyDID: %v", err)
	}
	if !ok {
		t.Fatal("VerifyDID returned false for valid signature")
	}

	ok, err = VerifyDID(id.DID, []byte("different"), sig)
	if err != nil {
		t.Fatal(err)
	}
	if ok {
		t.Fatal("VerifyDID returned true for wrong message")
	}
}

func TestSaveAndLoad(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "identity.json")

	original, err := Generate()
	if err != nil {
		t.Fatal(err)
	}
	if err := original.Save(path); err != nil {
		t.Fatalf("Save: %v", err)
	}

	// check file permissions
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm() != 0600 {
		t.Errorf("identity file permissions = %o, want 0600", info.Mode().Perm())
	}

	loaded, err := Load(path)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if loaded.DID != original.DID {
		t.Errorf("DID mismatch: got %q, want %q", loaded.DID, original.DID)
	}
	if string(loaded.PublicKey) != string(original.PublicKey) {
		t.Error("PublicKey mismatch after load")
	}
	if string(loaded.PrivateKey) != string(original.PrivateKey) {
		t.Error("PrivateKey mismatch after load")
	}

	// loaded identity can still sign and verify
	msg := []byte("post-load signing")
	sig := loaded.Sign(msg)
	if !original.Verify(msg, sig) {
		t.Error("loaded identity signature not verifiable by original")
	}
}

func TestLoad_NotFound(t *testing.T) {
	if _, err := Load("/nonexistent/path/identity.json"); err == nil {
		t.Fatal("Load from missing file expected error")
	}
}

func TestPublicKeyBase64RoundTrip(t *testing.T) {
	id, _ := Generate()
	b64 := id.PublicKeyBase64()
	if b64 == "" {
		t.Fatal("PublicKeyBase64 is empty")
	}
	// must be non-empty and deterministic
	if id.PublicKeyBase64() != b64 {
		t.Fatal("PublicKeyBase64 is not deterministic")
	}
}
