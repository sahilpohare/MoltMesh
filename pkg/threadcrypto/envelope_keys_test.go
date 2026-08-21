package threadcrypto

import (
	"crypto/ecdh"
	"crypto/rand"
	"testing"
)

func TestEpochKeyEnvelopeRoundTrip(t *testing.T) {
	recipient, err := ecdh.X25519().GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	key := make([]byte, 32)
	if _, err := rand.Read(key); err != nil {
		t.Fatal(err)
	}
	context := []byte("thread-1:epoch-1")
	eph, nonce, ciphertext, err := WrapEpochKey(recipient.PublicKey(), key, context)
	if err != nil {
		t.Fatal(err)
	}
	got, err := UnwrapEpochKey(recipient, eph, nonce, ciphertext, context)
	if err != nil || string(got) != string(key) {
		t.Fatalf("unwrap = %x, %v", got, err)
	}
	other, _ := ecdh.X25519().GenerateKey(rand.Reader)
	if _, err := UnwrapEpochKey(other, eph, nonce, ciphertext, context); err == nil {
		t.Fatal("wrong recipient unwrapped epoch key")
	}
}
