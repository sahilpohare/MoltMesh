package registry

import (
	"context"
	"crypto/ed25519"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"time"

	cid "github.com/ipfs/go-cid"
	dht "github.com/libp2p/go-libp2p-kad-dht"
	"github.com/multiformats/go-multihash"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"

	appactors "github.com/sahilpohare/p2p-a2a/daemon/actors"
	"github.com/sahilpohare/p2p-a2a/daemon/identity"
	pb "github.com/sahilpohare/p2p-a2a/gen/a2a/v1"
)

const (
	agentCardTTL    = 1 * time.Hour
	republishPeriod = 45 * time.Minute
)

// Registry handles Agent Card publishing and resolution via DHT.
type Registry struct {
	dht   *dht.IpfsDHT
	id    *identity.Identity
	card  *pb.AgentCard
	cards map[string]*pb.AgentCard
	log   *zap.Logger
	exec  *appactors.Executor
}

func (r *Registry) EnableActor(ctx context.Context, h *appactors.Hierarchy) error {
	exec, err := appactors.NewExecutor(ctx, h, "registry")
	if err != nil {
		return err
	}
	r.exec = exec
	return exec.Schedule(ctx, "registry-republish", republishPeriod, func() (any, error) {
		if r.card == nil {
			return nil, nil
		}
		return nil, r.publish(ctx, r.card)
	})
}

// New creates a new registry.
func New(d *dht.IpfsDHT, id *identity.Identity, log *zap.Logger) *Registry {
	return &Registry{dht: d, id: id, log: log, cards: make(map[string]*pb.AgentCard)}
}

// PublishSigned stores an SDK-signed card without replacing its DID or
// signature. It is used only after daemon session authentication has bound the
// caller to the same DID.
func (r *Registry) PublishSigned(ctx context.Context, card *pb.AgentCard) error {
	if r.exec != nil {
		_, err := r.exec.Call(ctx, func() (any, error) { return nil, r.publishSigned(ctx, card) })
		return err
	}
	return r.publishSigned(ctx, card)
}

func (r *Registry) publishSigned(ctx context.Context, card *pb.AgentCard) error {
	if err := verifyCard(card); err != nil {
		return fmt.Errorf("verify SDK agent card: %w", err)
	}
	data, err := json.Marshal(card)
	if err != nil {
		return fmt.Errorf("marshal card: %w", err)
	}
	if err := r.dht.PutValue(ctx, dhtKey(card.Did), data); err != nil {
		return fmt.Errorf("dht put: %w", err)
	}
	r.cards[card.Did] = proto.Clone(card).(*pb.AgentCard)
	for _, skill := range card.Skills {
		if err := r.putCapabilityAgent(ctx, skill.Id, card.Did); err != nil {
			return fmt.Errorf("index capability %q: %w", skill.Id, err)
		}
		if err := r.advertiseCapability(ctx, skill.Id); err != nil {
			return fmt.Errorf("advertise capability %q: %w", skill.Id, err)
		}
	}
	return nil
}

// Publish signs and publishes an Agent Card to the DHT.
func (r *Registry) Publish(ctx context.Context, card *pb.AgentCard) error {
	if r.exec != nil {
		_, err := r.exec.Call(ctx, func() (any, error) { return nil, r.publish(ctx, card) })
		return err
	}
	return r.publish(ctx, card)
}
func (r *Registry) publish(ctx context.Context, card *pb.AgentCard) error {
	card.Did = r.id.DID
	card.PublicKey = r.id.PublicKeyBase64()
	card.PublishedAt = time.Now().UnixMilli()
	card.ExpiresAt = time.Now().Add(agentCardTTL).UnixMilli()
	card.Signature = "" // clear before signing

	canonical, err := cardCanonical(card)
	if err != nil {
		return fmt.Errorf("canonical card: %w", err)
	}
	card.Signature = base64.StdEncoding.EncodeToString(r.id.Sign(canonical))

	data, err := json.Marshal(card)
	if err != nil {
		return fmt.Errorf("marshal card: %w", err)
	}

	key := dhtKey(r.id.DID)
	if err := r.dht.PutValue(ctx, key, data); err != nil {
		return fmt.Errorf("dht put: %w", err)
	}

	r.card = card
	r.log.Info("agent card published", zap.String("did", card.Did))
	return nil
}

// Resolve fetches an Agent Card by DID from the DHT and verifies its signature.
func (r *Registry) Resolve(ctx context.Context, did string) (*pb.AgentCard, error) {
	if r.exec != nil {
		value, err := r.exec.Call(ctx, func() (any, error) { return r.resolve(ctx, did) })
		if err != nil {
			return nil, err
		}
		return value.(*pb.AgentCard), nil
	}
	return r.resolve(ctx, did)
}
func (r *Registry) resolve(ctx context.Context, did string) (*pb.AgentCard, error) {
	key := dhtKey(did)
	data, err := r.dht.GetValue(ctx, key)
	if err != nil {
		return nil, fmt.Errorf("dht get %q: %w", did, err)
	}
	var card pb.AgentCard
	if err := json.Unmarshal(data, &card); err != nil {
		return nil, fmt.Errorf("unmarshal card: %w", err)
	}
	if err := verifyCard(&card); err != nil {
		return nil, fmt.Errorf("invalid agent card signature for %q: %w", did, err)
	}
	if card.Did != did {
		return nil, fmt.Errorf("agent card DID %q does not match lookup key %q", card.Did, did)
	}
	if card.ExpiresAt <= time.Now().UnixMilli() {
		return nil, fmt.Errorf("agent card for %q expired at %s", did, time.UnixMilli(card.ExpiresAt).UTC())
	}
	return &card, nil
}

// FindByCapability searches the DHT for agents advertising a capability
// using FindProviders (the counterpart to Provide/AdvertiseCapability).
// For each provider found, it resolves their AgentCard from the DHT.
func (r *Registry) FindByCapability(ctx context.Context, capability string, limit int) ([]*pb.AgentCard, error) {
	if r.exec != nil {
		value, err := r.exec.Call(ctx, func() (any, error) { return r.findByCapability(ctx, capability, limit) })
		if err != nil {
			return nil, err
		}
		return value.([]*pb.AgentCard), nil
	}
	return r.findByCapability(ctx, capability, limit)
}
func (r *Registry) findByCapability(ctx context.Context, capability string, limit int) ([]*pb.AgentCard, error) {
	c, err := capabilityCID(capability)
	if err != nil {
		return nil, fmt.Errorf("capability CID: %w", err)
	}

	provCh := r.dht.FindProvidersAsync(ctx, c, limit)
	var cards []*pb.AgentCard
	seen := make(map[string]bool)
	for did, card := range r.cards {
		if cardHasCapability(card, capability) {
			cards = append(cards, proto.Clone(card).(*pb.AgentCard))
			seen[did] = true
			if limit > 0 && len(cards) >= limit {
				return cards, nil
			}
		}
	}
	for prov := range provCh {
		// A libp2p provider is the shared daemon transport, not necessarily the
		// application agent that signed a card. Resolve the daemon-owned index
		// first so one daemon can advertise several independent SDK agents.
		for _, did := range r.capabilityAgents(ctx, prov.ID.String(), capability) {
			if seen[did] {
				continue
			}
			card, err := r.resolve(ctx, did)
			if err != nil {
				r.log.Debug("skip indexed provider card", zap.String("did", did), zap.Error(err))
				continue
			}
			if !cardHasCapability(card, capability) {
				continue
			}
			cards = append(cards, card)
			seen[did] = true
			if limit > 0 && len(cards) >= limit {
				return cards, nil
			}
		}
		if prov.ID == r.dht.Host().ID() {
			// skip self
			if r.card != nil && !seen[r.card.Did] {
				cards = append(cards, r.card)
				seen[r.card.Did] = true
			}
			continue
		}
		// Resolve the provider's AgentCard by their peer ID.
		// We derive a did:key from their public key.
		pubKey, err := prov.ID.ExtractPublicKey()
		if err != nil {
			r.log.Debug("skip provider, cannot extract pubkey", zap.String("peer", prov.ID.String()))
			continue
		}
		rawPub, err := pubKey.Raw()
		if err != nil {
			continue
		}
		did := identity.DIDFromPubBytes(rawPub)
		card, err := r.resolve(ctx, did)
		if err != nil {
			r.log.Debug("skip provider, card not found", zap.String("did", did), zap.Error(err))
			continue
		}
		if seen[card.Did] {
			continue
		}
		cards = append(cards, card)
		seen[card.Did] = true
		if limit > 0 && len(cards) >= limit {
			break
		}
	}
	return cards, nil
}

// AdvertiseCapability announces this agent as a provider of a capability via
// DHT Provide. Unlike PutValue (single-writer), Provide allows multiple agents
// to advertise the same capability without overwriting each other.
func (r *Registry) AdvertiseCapability(ctx context.Context, capability string) error {
	if r.exec != nil {
		_, err := r.exec.Call(ctx, func() (any, error) { return nil, r.advertiseCapability(ctx, capability) })
		return err
	}
	return r.advertiseCapability(ctx, capability)
}
func (r *Registry) advertiseCapability(ctx context.Context, capability string) error {
	if r.card == nil && len(r.cards) == 0 {
		return fmt.Errorf("publish agent card first")
	}
	c, err := capabilityCID(capability)
	if err != nil {
		return fmt.Errorf("capability CID: %w", err)
	}
	return r.dht.Provide(ctx, c, true)
}

func cardHasCapability(card *pb.AgentCard, capability string) bool {
	for _, skill := range card.Skills {
		if skill.Id == capability {
			return true
		}
	}
	return false
}

// RunRepublish periodically re-publishes the Agent Card before TTL expiry.
func (r *Registry) RunRepublish(ctx context.Context) {
	if r.exec != nil {
		<-ctx.Done()
		return
	}
	ticker := time.NewTicker(republishPeriod)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if r.card == nil {
				continue
			}
			if err := r.publish(ctx, r.card); err != nil {
				r.log.Warn("republish agent card", zap.Error(err))
			}
		}
	}
}

// ─── internal ────────────────────────────────────────────────────────────────

func dhtKey(did string) string {
	return "/agents/" + did
}

func capabilityAgentsKey(peerID, capability string) string {
	return "/agents/capabilities/" + peerID + "/" + capability
}

// putCapabilityAgent records the signed agent DID advertised by this daemon
// for a capability. The card remains the trust-bearing object: callers always
// fetch and verify it before returning a discovery result.
func (r *Registry) putCapabilityAgent(ctx context.Context, capability, did string) error {
	peerID := r.dht.Host().ID().String()
	key := capabilityAgentsKey(peerID, capability)
	var dids []string
	if raw, err := r.dht.GetValue(ctx, key); err == nil {
		_ = json.Unmarshal(raw, &dids)
	}
	for _, existing := range dids {
		if existing == did {
			return nil
		}
	}
	dids = append(dids, did)
	raw, err := json.Marshal(dids)
	if err != nil {
		return err
	}
	return r.dht.PutValue(ctx, key, raw)
}

func (r *Registry) capabilityAgents(ctx context.Context, peerID, capability string) []string {
	raw, err := r.dht.GetValue(ctx, capabilityAgentsKey(peerID, capability))
	if err != nil {
		return nil
	}
	var dids []string
	if err := json.Unmarshal(raw, &dids); err != nil {
		return nil
	}
	return dids
}

// capabilityCID derives a deterministic CID from a capability name for use
// with DHT Provide/FindProviders.
func capabilityCID(capability string) (cid.Cid, error) {
	hash, err := multihash.Sum([]byte("/a2a/caps/"+capability), multihash.SHA2_256, -1)
	if err != nil {
		return cid.Undef, err
	}
	return cid.NewCidV1(cid.Raw, hash), nil
}

func cardCanonical(card *pb.AgentCard) ([]byte, error) {
	// Deterministic protobuf covers every semantic field (including skills and
	// multiaddrs) while avoiding JSON map-order and future-field omissions.
	clone := proto.Clone(card).(*pb.AgentCard)
	clone.Signature = ""
	return proto.MarshalOptions{Deterministic: true}.Marshal(clone)
}

// CanonicalAgentCard returns the exact deterministic payload SDKs sign. It is
// exported for daemon-side tests and protocol implementations.
func CanonicalAgentCard(card *pb.AgentCard) ([]byte, error) { return cardCanonical(card) }

// verifyCard verifies the Ed25519 signature on a resolved agent card.
// It extracts the public key from the DID itself (did:key), so no external
// trust anchor is needed — the DID is the key.
func verifyCard(card *pb.AgentCard) error {
	if card.Signature == "" {
		return fmt.Errorf("card has no signature")
	}
	now := time.Now()
	if card.PublishedAt > now.Add(5*time.Minute).UnixMilli() || card.ExpiresAt <= now.UnixMilli() || card.ExpiresAt <= card.PublishedAt {
		return fmt.Errorf("expired or invalid card validity interval")
	}

	pub, err := identity.PubKeyFromDID(card.Did)
	if err != nil {
		return fmt.Errorf("extract pubkey from DID: %w", err)
	}
	if card.PublicKey != base64.StdEncoding.EncodeToString(pub) {
		return fmt.Errorf("public key does not match DID")
	}

	sig, err := base64.StdEncoding.DecodeString(card.Signature)
	if err != nil {
		return fmt.Errorf("decode signature: %w", err)
	}

	canonical, err := cardCanonical(card)
	if err != nil {
		return fmt.Errorf("canonical card: %w", err)
	}

	if !ed25519.Verify(pub, canonical, sig) {
		return fmt.Errorf("signature verification failed")
	}
	return nil
}

// VerifyAgentCard verifies all signed AgentCard fields without performing a DHT lookup.
func VerifyAgentCard(card *pb.AgentCard) error { return verifyCard(card) }
