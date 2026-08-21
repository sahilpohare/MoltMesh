package threadcrypto

import (
	"crypto/ecdh"
	"crypto/rand"
	"crypto/sha256"
	"fmt"
	"hash"

	"golang.org/x/crypto/chacha20poly1305"
	"golang.org/x/crypto/hkdf"
)

func WrapEpochKey(recipient *ecdh.PublicKey, key []byte, context []byte) (ephemeral, nonce, ciphertext []byte, err error) {
	if recipient == nil || len(key) != chacha20poly1305.KeySize {
		return nil, nil, nil, fmt.Errorf("invalid recipient or epoch key")
	}
	eph, err := ecdh.X25519().GenerateKey(rand.Reader)
	if err != nil {
		return nil, nil, nil, err
	}
	shared, err := eph.ECDH(recipient)
	if err != nil {
		return nil, nil, nil, err
	}
	wrap := make([]byte, chacha20poly1305.KeySize)
	if _, err := hkdf.New(func() hash.Hash { return sha256.New() }, shared, nil, context).Read(wrap); err != nil {
		return nil, nil, nil, err
	}
	aead, err := chacha20poly1305.NewX(wrap)
	if err != nil {
		return nil, nil, nil, err
	}
	nonce = make([]byte, chacha20poly1305.NonceSizeX)
	if _, err = rand.Read(nonce); err != nil {
		return nil, nil, nil, err
	}
	return eph.PublicKey().Bytes(), nonce, aead.Seal(nil, nonce, key, context), nil
}

func UnwrapEpochKey(recipient *ecdh.PrivateKey, ephemeral, nonce, ciphertext, context []byte) ([]byte, error) {
	if recipient == nil || len(nonce) != chacha20poly1305.NonceSizeX {
		return nil, fmt.Errorf("invalid key envelope")
	}
	pub, err := ecdh.X25519().NewPublicKey(ephemeral)
	if err != nil {
		return nil, err
	}
	shared, err := recipient.ECDH(pub)
	if err != nil {
		return nil, err
	}
	wrap := make([]byte, chacha20poly1305.KeySize)
	if _, err := hkdf.New(sha256.New, shared, nil, context).Read(wrap); err != nil {
		return nil, err
	}
	aead, err := chacha20poly1305.NewX(wrap)
	if err != nil {
		return nil, err
	}
	return aead.Open(nil, nonce, ciphertext, context)
}
