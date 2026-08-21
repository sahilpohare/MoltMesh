// Package threadcrypto implements the authenticated ciphertext envelope used
// by SDKs before a thread entry is submitted to a daemon.
package threadcrypto

import (
	"crypto/ed25519"
	"crypto/rand"
	"encoding/binary"
	"fmt"

	"golang.org/x/crypto/chacha20poly1305"
)

// Sign binds the canonical header and ciphertext to the entry author. It is
// intentionally separate from Encrypt so callers can use SDK-owned keys.
func Sign(private ed25519.PrivateKey, header Header, ciphertext []byte) ([]byte, error) {
	if len(private) != ed25519.PrivateKeySize {
		return nil, fmt.Errorf("invalid Ed25519 private key")
	}
	aad, err := header.Canonical()
	if err != nil {
		return nil, err
	}
	return ed25519.Sign(private, append(aad, ciphertext...)), nil
}

func Verify(public ed25519.PublicKey, header Header, ciphertext, signature []byte) (bool, error) {
	if len(public) != ed25519.PublicKeySize {
		return false, fmt.Errorf("invalid Ed25519 public key")
	}
	aad, err := header.Canonical()
	if err != nil {
		return false, err
	}
	return ed25519.Verify(public, append(aad, ciphertext...), signature), nil
}

// Header is public, signed routing metadata. Its canonical encoding is AEAD
// associated data, so a daemon cannot alter epochs, author, ordering, or kind
// without making decryption fail.
type Header struct {
	ThreadID          string
	Sequence          uint64
	PreviousBlockHash []byte
	AuthorDID         string
	Kind              string
	MembershipEpoch   uint64
	EncryptionEpoch   uint64
	Nonce             []byte
}

func (h Header) Canonical() ([]byte, error) {
	if len(h.Nonce) != chacha20poly1305.NonceSizeX {
		return nil, fmt.Errorf("thread nonce must be %d bytes", chacha20poly1305.NonceSizeX)
	}
	buf := make([]byte, 0, 96+len(h.ThreadID)+len(h.PreviousBlockHash)+len(h.AuthorDID)+len(h.Kind))
	put := func(v []byte) {
		var n [4]byte
		binary.BigEndian.PutUint32(n[:], uint32(len(v)))
		buf = append(buf, n[:]...)
		buf = append(buf, v...)
	}
	put([]byte(h.ThreadID))
	var n [8]byte
	binary.BigEndian.PutUint64(n[:], h.Sequence)
	buf = append(buf, n[:]...)
	put(h.PreviousBlockHash)
	put([]byte(h.AuthorDID))
	put([]byte(h.Kind))
	binary.BigEndian.PutUint64(n[:], h.MembershipEpoch)
	buf = append(buf, n[:]...)
	binary.BigEndian.PutUint64(n[:], h.EncryptionEpoch)
	buf = append(buf, n[:]...)
	put(h.Nonce)
	return buf, nil
}

func Encrypt(key []byte, header Header, plaintext []byte) ([]byte, Header, error) {
	if len(key) != chacha20poly1305.KeySize {
		return nil, header, fmt.Errorf("thread key must be %d bytes", chacha20poly1305.KeySize)
	}
	if len(header.Nonce) == 0 {
		header.Nonce = make([]byte, chacha20poly1305.NonceSizeX)
		if _, err := rand.Read(header.Nonce); err != nil {
			return nil, header, err
		}
	}
	aad, err := header.Canonical()
	if err != nil {
		return nil, header, err
	}
	aead, err := chacha20poly1305.NewX(key)
	if err != nil {
		return nil, header, err
	}
	return aead.Seal(nil, header.Nonce, plaintext, aad), header, nil
}

func Decrypt(key []byte, header Header, ciphertext []byte) ([]byte, error) {
	if len(key) != chacha20poly1305.KeySize {
		return nil, fmt.Errorf("thread key must be %d bytes", chacha20poly1305.KeySize)
	}
	aad, err := header.Canonical()
	if err != nil {
		return nil, err
	}
	aead, err := chacha20poly1305.NewX(key)
	if err != nil {
		return nil, err
	}
	return aead.Open(nil, header.Nonce, ciphertext, aad)
}
