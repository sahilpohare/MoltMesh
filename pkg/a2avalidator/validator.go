package a2avalidator

import (
	"crypto/ed25519"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	record "github.com/libp2p/go-libp2p-record"
	"google.golang.org/protobuf/proto"

	"github.com/sahilpohare/p2p-a2a/daemon/identity"
	pb "github.com/sahilpohare/p2p-a2a/gen/a2a/v1"
)

const maxClockSkew = 5 * time.Minute

// AgentCardValidator validates DHT records stored under the /a2a/ namespace.
// Keys follow the format /a2a/agents/<did>.
// Values are JSON-encoded AgentCard objects.
// Non-A2A peers that don't have this validator will reject puts — this is
// acceptable in a mixed network; A2A peers will validate and store correctly.
type AgentCardValidator struct{}

// Validate checks that the key is a valid /a2a/agents/<did> path and the
// value is parseable JSON with a non-empty DID field.
func (v AgentCardValidator) Validate(key string, value []byte) error {
	// NamespacedValidator strips the namespace prefix before calling Validate,
	// but retains the separating slash in current go-libp2p-record releases.
	key = strings.TrimPrefix(key, "/")
	key = strings.TrimPrefix(key, "agents/")
	if key == "" {
		return fmt.Errorf("a2avalidator: empty key")
	}
	var card pb.AgentCard
	if err := json.Unmarshal(value, &card); err != nil {
		return fmt.Errorf("a2avalidator: invalid agent card JSON: %w", err)
	}
	if card.Did == "" {
		return fmt.Errorf("a2avalidator: agent card missing did field")
	}
	if card.Did != key {
		return fmt.Errorf("a2avalidator: card DID does not match key")
	}
	return validateAgentCard(&card, time.Now())
}

// Select returns the index of the best record among candidates.
// Prefers the record with the highest published_at field.
func (v AgentCardValidator) Select(key string, vals [][]byte) (int, error) {
	if len(vals) == 0 {
		return 0, fmt.Errorf("a2avalidator: no values to select from")
	}
	type card struct {
		PublishedAt int64 `json:"published_at"`
	}
	best := -1
	var bestCard card
	for i := 0; i < len(vals); i++ {
		if err := v.Validate(key, vals[i]); err != nil {
			continue
		}
		var c card
		if err := json.Unmarshal(vals[i], &c); err != nil {
			continue
		}
		if best < 0 || c.PublishedAt > bestCard.PublishedAt {
			best = i
			bestCard = c
		}
	}
	if best < 0 {
		return 0, fmt.Errorf("a2avalidator: no valid agent cards")
	}
	return best, nil
}

func validateAgentCard(card *pb.AgentCard, now time.Time) error {
	pub, err := identity.PubKeyFromDID(card.Did)
	if err != nil {
		return fmt.Errorf("a2avalidator: invalid DID: %w", err)
	}
	if card.PublicKey != base64.StdEncoding.EncodeToString(pub) {
		return fmt.Errorf("a2avalidator: public key does not match DID")
	}
	if card.PublishedAt > now.Add(maxClockSkew).UnixMilli() {
		return fmt.Errorf("a2avalidator: published_at is too far in the future")
	}
	if card.ExpiresAt <= now.UnixMilli() || card.ExpiresAt <= card.PublishedAt {
		return fmt.Errorf("a2avalidator: expired or invalid validity interval")
	}
	sig, err := base64.StdEncoding.DecodeString(card.Signature)
	if err != nil {
		return fmt.Errorf("a2avalidator: invalid signature encoding: %w", err)
	}
	clone := proto.Clone(card).(*pb.AgentCard)
	clone.Signature = ""
	canonical, err := proto.MarshalOptions{Deterministic: true}.Marshal(clone)
	if err != nil || !ed25519.Verify(pub, canonical, sig) {
		return fmt.Errorf("a2avalidator: invalid card signature")
	}
	return nil
}

// NameClaimValidator validates signed records stored below /a2a/names/.
type NameClaimValidator struct{}

type nameClaim struct {
	Name        string `json:"name"`
	DID         string `json:"did"`
	PublishedAt int64  `json:"published_at"`
	ExpiresAt   int64  `json:"expires_at"`
	Signature   string `json:"signature"`
}

func (NameClaimValidator) Validate(key string, value []byte) error {
	key = strings.TrimPrefix(key, "/")
	key = strings.TrimPrefix(key, "names/")
	var claim nameClaim
	if err := json.Unmarshal(value, &claim); err != nil {
		return fmt.Errorf("a2avalidator: invalid name claim: %w", err)
	}
	if key == "" || strings.Contains(key, "/") || claim.Name != key {
		return fmt.Errorf("a2avalidator: claim name does not match key")
	}
	pub, err := identity.PubKeyFromDID(claim.DID)
	if err != nil {
		return fmt.Errorf("a2avalidator: invalid claim DID: %w", err)
	}
	now := time.Now()
	if claim.PublishedAt > now.Add(maxClockSkew).UnixMilli() || claim.ExpiresAt <= now.UnixMilli() || claim.ExpiresAt <= claim.PublishedAt {
		return fmt.Errorf("a2avalidator: invalid claim validity interval")
	}
	sig, err := base64.StdEncoding.DecodeString(claim.Signature)
	if err != nil {
		return fmt.Errorf("a2avalidator: invalid claim signature encoding: %w", err)
	}
	claim.Signature = ""
	canonical, err := json.Marshal(claim)
	if err != nil || !ed25519.Verify(pub, canonical, sig) {
		return fmt.Errorf("a2avalidator: invalid claim signature")
	}
	return nil
}

func (v NameClaimValidator) Select(key string, vals [][]byte) (int, error) {
	best, bestPublished := -1, int64(0)
	for i, value := range vals {
		if err := v.Validate(key, value); err != nil {
			continue
		}
		var claim nameClaim
		_ = json.Unmarshal(value, &claim)
		if best < 0 || claim.PublishedAt > bestPublished {
			best, bestPublished = i, claim.PublishedAt
		}
	}
	if best < 0 {
		return 0, fmt.Errorf("a2avalidator: no valid name claims")
	}
	return best, nil
}

// Ensure AgentCardValidator implements record.Validator.
var _ record.Validator = AgentCardValidator{}
var _ record.Validator = NameClaimValidator{}
