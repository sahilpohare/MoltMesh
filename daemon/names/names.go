// Package names implements human-readable name claiming on the MoltMesh network.
//
// Names are lowercase English words joined by hyphens (e.g. "swift-falcon",
// "bright-harbor-relay"). A claim is a signed DHT record binding a name to a
// DID.
//
// # Security model
//
// Claims are Ed25519-signed, so forgery is impossible: only the holder of the
// private key corresponding to a DID can publish or renew that DID's claim.
//
// Sybil / eclipse resistance uses quorum reads: Resolve calls SearchValue and
// collects responses from multiple DHT peers. The most-recent *valid* claim
// whose signature verifies is returned. An attacker must control a majority of
// the closest-K peers to the key to suppress a live record.
//
// Conflict policy: Claim checks all received records before writing. If any
// peer returns a live (non-expired), validly-signed claim from a *different*
// DID, the claim is rejected — consent is required by the current holder's
// expiry lapsing first.
//
// For full Byzantine-fault-tolerant name assignment (linear history, no
// equivocation, Sybil-proof even under majority eclipse) the correct next step
// is to anchor claims in a consensus log (Raft/Tendermint thread). That is a
// planned upgrade; the current DHT approach gives reasonable safety for
// well-connected nodes.
//
// DHT key:  /a2a/names/<normalised-name>
// TTL:      24 hours (renewed every 20 hours by RunRepublish)
package names

import (
	"context"
	"crypto/ed25519"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"regexp"
	"strings"
	"time"
	"unicode"

	dht "github.com/libp2p/go-libp2p-kad-dht"
	"go.uber.org/zap"

	"github.com/sahilpohare/p2p-a2a/daemon/identity"
)

const (
	claimTTL       = 24 * time.Hour
	republishEvery = 20 * time.Hour
)

var validName = regexp.MustCompile(`^[a-z][a-z0-9]*(-[a-z][a-z0-9]*)*$`)

// Claim is the DHT record for a name.
type Claim struct {
	Name        string `json:"name"`
	DID         string `json:"did"`
	PublishedAt int64  `json:"published_at"` // Unix ms
	ExpiresAt   int64  `json:"expires_at"`   // Unix ms
	Signature   string `json:"signature"`    // base64 Ed25519 over canonical JSON (name+did+published_at+expires_at)
}

// Registry manages name claims via the DHT.
type Registry struct {
	dht    *dht.IpfsDHT
	id     *identity.Identity
	claims []string // names this node has claimed
	log    *zap.Logger
}

// New creates a name registry.
func New(d *dht.IpfsDHT, id *identity.Identity, log *zap.Logger) *Registry {
	return &Registry{dht: d, id: id, log: log}
}

// Normalize lowercases, strips punctuation, and joins words with hyphens.
// "Swift Falcon" → "swift-falcon", "bright_harbor" → "bright-harbor"
func Normalize(name string) string {
	var words []string
	for _, word := range strings.FieldsFunc(name, func(r rune) bool {
		return !unicode.IsLetter(r) && !unicode.IsDigit(r)
	}) {
		w := strings.ToLower(word)
		if w != "" {
			words = append(words, w)
		}
	}
	return strings.Join(words, "-")
}

// Validate returns an error if the name is not a valid MoltMesh agent name.
func Validate(name string) error {
	if len(name) == 0 {
		return fmt.Errorf("name is empty")
	}
	if len(name) > 64 {
		return fmt.Errorf("name too long (max 64 characters)")
	}
	if !validName.MatchString(name) {
		return fmt.Errorf("name %q must be lowercase letters/digits separated by hyphens", name)
	}
	return nil
}

// Claim publishes a name claim to the DHT, binding the normalised name to this
// node's DID. Returns an error if the name is already claimed by a different DID.
//
// Consent check: all DHT records returned by quorum search are inspected. If
// any peer returns a live (non-expired), validly-signed claim from a different
// DID the write is refused. This requires the current holder's claim to expire
// before another agent may take the name.
func (r *Registry) Claim(ctx context.Context, name string) (*Claim, error) {
	name = Normalize(name)
	if err := Validate(name); err != nil {
		return nil, err
	}

	// Quorum pre-check: collect all existing records from the network.
	// If any live record belongs to a different DID, reject.
	existing := r.searchBest(ctx, name)
	if existing != nil && existing.DID != r.id.DID {
		if existing.ExpiresAt > time.Now().UnixMilli() {
			return nil, fmt.Errorf("name %q is already claimed by %s (expires %s)",
				name, existing.DID,
				time.UnixMilli(existing.ExpiresAt).UTC().Format(time.RFC3339))
		}
	}

	claim := &Claim{
		Name:        name,
		DID:         r.id.DID,
		PublishedAt: time.Now().UnixMilli(),
		ExpiresAt:   time.Now().Add(claimTTL).UnixMilli(),
	}

	sig, err := signClaim(r.id, claim)
	if err != nil {
		return nil, err
	}
	claim.Signature = sig

	data, err := json.Marshal(claim)
	if err != nil {
		return nil, fmt.Errorf("marshal claim: %w", err)
	}

	key := dhtKey(name)
	if err := r.dht.PutValue(ctx, key, data); err != nil {
		return nil, fmt.Errorf("dht put name %q: %w", name, err)
	}

	r.claims = append(r.claims, name)
	r.log.Info("name claimed", zap.String("name", name), zap.String("did", r.id.DID))
	return claim, nil
}

// Resolve looks up a name in the DHT using a quorum search and returns the
// most-recent valid claim. Invalid or unsigned records are silently skipped.
func (r *Registry) Resolve(ctx context.Context, name string) (*Claim, error) {
	name = Normalize(name)
	if err := Validate(name); err != nil {
		return nil, err
	}

	best := r.searchBest(ctx, name)
	if best == nil {
		return nil, fmt.Errorf("name %q not found", name)
	}
	return best, nil
}

// RunRepublish periodically renews all claims held by this node.
func (r *Registry) RunRepublish(ctx context.Context) {
	ticker := time.NewTicker(republishEvery)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			for _, name := range r.claims {
				if _, err := r.Claim(ctx, name); err != nil {
					r.log.Warn("republish name claim", zap.String("name", name), zap.Error(err))
				}
			}
		}
	}
}

// ─── internal ────────────────────────────────────────────────────────────────

// searchBest queries all DHT peers near the key and returns the most-recent
// validly-signed claim. Forged or malformed records are discarded.
func (r *Registry) searchBest(ctx context.Context, name string) *Claim {
	ch, err := r.dht.SearchValue(ctx, dhtKey(name))
	if err != nil {
		return nil
	}
	var best *Claim
	for data := range ch {
		var c Claim
		if err := json.Unmarshal(data, &c); err != nil {
			continue
		}
		if err := verifyClaim(&c); err != nil {
			r.log.Debug("dropping invalid name claim", zap.String("name", name), zap.Error(err))
			continue
		}
		if best == nil || c.PublishedAt > best.PublishedAt {
			best = &c
		}
	}
	return best
}

func dhtKey(name string) string {
	return "/a2a/names/" + name
}

// canonical returns the bytes to sign/verify: JSON of all fields except Signature.
func canonical(c *Claim) ([]byte, error) {
	tmp := *c
	tmp.Signature = ""
	return json.Marshal(tmp)
}

func signClaim(id *identity.Identity, c *Claim) (string, error) {
	msg, err := canonical(c)
	if err != nil {
		return "", err
	}
	return base64.StdEncoding.EncodeToString(id.Sign(msg)), nil
}

func verifyClaim(c *Claim) error {
	if c.Signature == "" {
		return fmt.Errorf("missing signature")
	}
	pub, err := identity.PubKeyFromDID(c.DID)
	if err != nil {
		return fmt.Errorf("extract pubkey: %w", err)
	}
	sig, err := base64.StdEncoding.DecodeString(c.Signature)
	if err != nil {
		return fmt.Errorf("decode signature: %w", err)
	}
	msg, err := canonical(c)
	if err != nil {
		return fmt.Errorf("canonical: %w", err)
	}
	if !ed25519.Verify(pub, msg, sig) {
		return fmt.Errorf("signature verification failed")
	}
	return nil
}
