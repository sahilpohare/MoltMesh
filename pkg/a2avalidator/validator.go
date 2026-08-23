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

// stripNamespace strips the leading slash NamespacedValidator retains before
// calling Validate, then the DHT sub-namespace segment (e.g. "agents/",
// "names/", "threads/") — the same two-step prefix trim every validator's
// Validate repeats before parsing what's left of the key.
func stripNamespace(key, segment string) string {
	key = strings.TrimPrefix(key, "/")
	return strings.TrimPrefix(key, segment)
}

// selectBest picks the index of the highest-ranked record among vals,
// skipping any that fail validate — the shape AgentCardValidator,
// NameClaimValidator, and ThreadHeadValidator's Select methods all repeated:
// validate each candidate, unmarshal the survivors, and keep the one with
// the greatest rank (a published_at timestamp for the first two, a thread
// height for the third). what names the record kind for the "no valid ..."
// error when nothing survives.
func selectBest[T any](key string, vals [][]byte, validate func(string, []byte) error, rank func(T) int64, what string) (int, error) {
	best := -1
	var bestRank int64
	for i, val := range vals {
		if err := validate(key, val); err != nil {
			continue
		}
		var v T
		if err := json.Unmarshal(val, &v); err != nil {
			continue
		}
		if r := rank(v); best < 0 || r > bestRank {
			best, bestRank = i, r
		}
	}
	if best < 0 {
		return 0, fmt.Errorf("a2avalidator: no valid %s", what)
	}
	return best, nil
}

// AgentCardValidator validates DHT records stored under the /a2a/ namespace.
// Keys follow the format /a2a/agents/<did>.
// Values are JSON-encoded AgentCard objects.
// Non-A2A peers that don't have this validator will reject puts — this is
// acceptable in a mixed network; A2A peers will validate and store correctly.
type AgentCardValidator struct{}

// capabilityIndexPrefix marks the sub-path under the agents/ namespace that
// stores a capability's DID list (see daemon/registry's putCapabilityAgent),
// not an AgentCard — /agents/capabilities/<peerID>/<capability>. It shares
// the agents/ namespace instead of getting its own so a single daemon-wide
// capability index can be found starting from a resolved AgentCard's peer,
// but that means this validator — otherwise entirely AgentCard-shaped — has
// to recognize and validate this one different record shape too, the same
// way ThreadHeadValidator branches on its own recovery/ sub-path.
const capabilityIndexPrefix = "capabilities/"

// Validate checks that the key is a valid /a2a/agents/<did> path and the
// value is parseable JSON with a non-empty DID field — or, for a
// capabilities/ sub-key, a JSON array of DIDs.
func (v AgentCardValidator) Validate(key string, value []byte) error {
	// NamespacedValidator strips the namespace prefix before calling Validate,
	// but retains the separating slash in current go-libp2p-record releases.
	key = stripNamespace(key, "agents/")
	if key == "" {
		return fmt.Errorf("a2avalidator: empty key")
	}
	if strings.HasPrefix(key, capabilityIndexPrefix) {
		return validateCapabilityIndex(value)
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

func validateCapabilityIndex(value []byte) error {
	var dids []string
	if err := json.Unmarshal(value, &dids); err != nil {
		return fmt.Errorf("a2avalidator: invalid capability index JSON: %w", err)
	}
	return nil
}

// Select returns the index of the best record among candidates. For agent
// cards, prefers the highest published_at. For a capabilities/ index, prefers
// the longest valid DID list — these records only ever grow (see
// putCapabilityAgent's existing-DID check), so more entries is more current,
// unlike an AgentCard there's no publish timestamp to compare instead.
func (v AgentCardValidator) Select(key string, vals [][]byte) (int, error) {
	if strings.HasPrefix(stripNamespace(key, "agents/"), capabilityIndexPrefix) {
		best, bestLen := -1, -1
		for i, val := range vals {
			var dids []string
			if err := json.Unmarshal(val, &dids); err != nil {
				continue
			}
			if len(dids) > bestLen {
				best, bestLen = i, len(dids)
			}
		}
		if best < 0 {
			return 0, fmt.Errorf("a2avalidator: no valid capability index records")
		}
		return best, nil
	}
	type card struct {
		PublishedAt int64 `json:"published_at"`
	}
	return selectBest(key, vals, v.Validate, func(c card) int64 { return c.PublishedAt }, "agent cards")
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
	key = stripNamespace(key, "names/")
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
	return selectBest(key, vals, v.Validate, func(c nameClaim) int64 { return c.PublishedAt }, "name claims")
}

// Ensure AgentCardValidator implements record.Validator.
var _ record.Validator = AgentCardValidator{}
var _ record.Validator = NameClaimValidator{}
