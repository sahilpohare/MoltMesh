// Package session implements the daemon side of short-lived authenticated SDK
// sessions. It is intentionally independent of daemon/identity: a daemon's
// libp2p identity must never become an SDK agent's signing identity.
package session

import (
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"fmt"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/sahilpohare/p2p-a2a/daemon/identity"
	pb "github.com/sahilpohare/p2p-a2a/gen/a2a/v1"
)

const (
	DefaultChallengeTTL = 60 * time.Second
	DefaultSessionTTL   = 15 * time.Minute
)

// Manager owns ephemeral challenge and session state. Tokens are represented
// internally only by SHA-256 hashes, so application logs and heap dumps never
// need retain a usable session credential after validation.
type Manager struct {
	nodeID       string
	challengeTTL time.Duration
	sessionTTL   time.Duration
	now          func() time.Time

	mu         sync.Mutex
	challenges map[string]challenge
	sessions   map[[sha256.Size]byte]session
}

type challenge struct {
	identity *pb.AgentIdentity
	nonce    []byte
	expires  time.Time
}

type session struct {
	agentDID string
	expires  time.Time
	identity *pb.AgentIdentity
}

func New(nodeID string) *Manager {
	return &Manager{
		nodeID: nodeID, challengeTTL: DefaultChallengeTTL, sessionTTL: DefaultSessionTTL,
		now: time.Now, challenges: make(map[string]challenge), sessions: make(map[[sha256.Size]byte]session),
	}
}

// Begin creates a single-use nonce challenge after checking that the supplied
// signing key actually derives the supplied did:key identity.
func (m *Manager) Begin(agent *pb.AgentIdentity) (*pb.AgentChallenge, error) {
	if err := validateIdentity(agent); err != nil {
		return nil, err
	}
	nonce := make([]byte, 32)
	if _, err := rand.Read(nonce); err != nil {
		return nil, fmt.Errorf("session nonce: %w", err)
	}
	now := m.now()
	expires := now.Add(m.challengeTTL)
	id := uuid.NewString()
	m.mu.Lock()
	m.pruneLocked(now)
	m.challenges[id] = challenge{identity: cloneIdentity(agent), nonce: nonce, expires: expires}
	m.mu.Unlock()
	return &pb.AgentChallenge{ChallengeId: id, Nonce: nonce, ExpiresAtUnixMs: expires.UnixMilli()}, nil
}

// Complete consumes a challenge and returns a random opaque session token.
func (m *Manager) Complete(req *pb.CompleteAgentSessionRequest) (*pb.AgentSession, error) {
	if req == nil || req.ChallengeId == "" || len(req.Signature) != ed25519.SignatureSize {
		return nil, fmt.Errorf("invalid session completion")
	}
	now := m.now()
	m.mu.Lock()
	m.pruneLocked(now)
	challenge, ok := m.challenges[req.ChallengeId]
	delete(m.challenges, req.ChallengeId) // single-use even for an invalid signature
	m.mu.Unlock()
	if !ok || !now.Before(challenge.expires) {
		return nil, fmt.Errorf("session challenge is expired or unknown")
	}
	if req.Card != nil && req.Card.Did != "" && req.Card.Did != challenge.identity.Did {
		return nil, fmt.Errorf("session card DID does not match challenge identity")
	}
	if !ed25519.Verify(ed25519.PublicKey(challenge.identity.SigningPublicKey), signingPayload(m.nodeID, req.ChallengeId, challenge.nonce, challenge.expires, challenge.identity.Did), req.Signature) {
		return nil, fmt.Errorf("invalid session challenge signature")
	}
	tokenBytes := make([]byte, 32)
	if _, err := rand.Read(tokenBytes); err != nil {
		return nil, fmt.Errorf("session token: %w", err)
	}
	token := base64.RawURLEncoding.EncodeToString(tokenBytes)
	hash := sha256.Sum256([]byte(token))
	expires := now.Add(m.sessionTTL)
	m.mu.Lock()
	m.sessions[hash] = session{agentDID: challenge.identity.Did, expires: expires, identity: cloneIdentity(challenge.identity)}
	m.mu.Unlock()
	return &pb.AgentSession{Token: token, AgentDid: challenge.identity.Did, ExpiresAtUnixMs: expires.UnixMilli()}, nil
}

func (m *Manager) Authenticate(token string) (string, error) {
	s, err := m.get(token)
	if err != nil {
		return "", err
	}
	return s.agentDID, nil
}

// Identity returns the authenticated agent's public identity material.
func (m *Manager) Identity(token string) (*pb.AgentIdentity, error) {
	s, err := m.get(token)
	if err != nil {
		return nil, err
	}
	return cloneIdentity(s.identity), nil
}

func (m *Manager) get(token string) (session, error) {
	if token == "" {
		return session{}, fmt.Errorf("missing agent session token")
	}
	hash := sha256.Sum256([]byte(token))
	now := m.now()
	m.mu.Lock()
	defer m.mu.Unlock()
	m.pruneLocked(now)
	s, ok := m.sessions[hash]
	if !ok || !now.Before(s.expires) {
		return session{}, fmt.Errorf("invalid or expired agent session token")
	}
	return s, nil
}

func (m *Manager) Close(token string) {
	if token == "" {
		return
	}
	hash := sha256.Sum256([]byte(token))
	m.mu.Lock()
	delete(m.sessions, hash)
	m.mu.Unlock()
}

// MultipleAgents reports whether more than one distinct SDK identity is
// currently attached. In that case legacy unscoped calls are ambiguous and
// must be rejected instead of silently selecting daemon-owned state.
func (m *Manager) MultipleAgents() bool {
	now := m.now()
	m.mu.Lock()
	defer m.mu.Unlock()
	m.pruneLocked(now)
	agents := make(map[string]struct{})
	for _, s := range m.sessions {
		agents[s.agentDID] = struct{}{}
		if len(agents) > 1 {
			return true
		}
	}
	return false
}

// SigningPayload is exported for SDK implementations and tests. The explicit
// separators make all signed fields unambiguous without exposing a token.
func SigningPayload(nodeID, challengeID string, nonce []byte, expires time.Time, did string) []byte {
	return signingPayload(nodeID, challengeID, nonce, expires, did)
}

func signingPayload(nodeID, challengeID string, nonce []byte, expires time.Time, did string) []byte {
	return []byte("moltmesh-agent-session-v1\x00" + nodeID + "\x00" + challengeID + "\x00" + base64.RawURLEncoding.EncodeToString(nonce) + "\x00" + fmt.Sprintf("%d", expires.UnixMilli()) + "\x00" + did)
}

func (m *Manager) pruneLocked(now time.Time) {
	for id, c := range m.challenges {
		if !now.Before(c.expires) {
			delete(m.challenges, id)
		}
	}
	for hash, s := range m.sessions {
		if !now.Before(s.expires) {
			delete(m.sessions, hash)
		}
	}
}

func validateIdentity(agent *pb.AgentIdentity) error {
	if agent == nil || agent.Did == "" || len(agent.SigningPublicKey) != ed25519.PublicKeySize || len(agent.EncryptionPublicKey) != 32 {
		return fmt.Errorf("agent identity requires did:key Ed25519 and X25519 public keys")
	}
	if identity.DIDFromPubBytes(agent.SigningPublicKey) != agent.Did {
		return fmt.Errorf("agent DID does not match signing public key")
	}
	return nil
}

func cloneIdentity(in *pb.AgentIdentity) *pb.AgentIdentity {
	return &pb.AgentIdentity{Did: in.Did, PublicKey: in.PublicKey, Multiaddrs: append([]string(nil), in.Multiaddrs...), SigningPublicKey: append([]byte(nil), in.SigningPublicKey...), EncryptionPublicKey: append([]byte(nil), in.EncryptionPublicKey...)}
}
