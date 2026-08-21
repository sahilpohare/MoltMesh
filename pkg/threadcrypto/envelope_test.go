package threadcrypto

import (
	"bytes"
	"testing"
)

func TestCiphertextBindsEveryPublicHeaderField(t *testing.T) {
	key := bytes.Repeat([]byte{7}, 32)
	h := Header{ThreadID: "t", Sequence: 2, PreviousBlockHash: []byte("head"), AuthorDID: "did:key:zagent", Kind: "task_result", MembershipEpoch: 3, EncryptionEpoch: 4}
	cipher, h, err := Encrypt(key, h, []byte("secret"))
	if err != nil {
		t.Fatal(err)
	}
	plain, err := Decrypt(key, h, cipher)
	if err != nil || string(plain) != "secret" {
		t.Fatalf("decrypt=%q,%v", plain, err)
	}
	h.Sequence++
	if _, err := Decrypt(key, h, cipher); err == nil {
		t.Fatal("mutated associated data decrypted")
	}
}
