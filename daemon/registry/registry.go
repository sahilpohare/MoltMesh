package registry

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	dht "github.com/libp2p/go-libp2p-kad-dht"
	"go.uber.org/zap"

	pb "github.com/sahilpohare/p2p-a2a/gen/a2a/v1"
	"github.com/sahilpohare/p2p-a2a/daemon/identity"
)

const (
	agentCardTTL    = 1 * time.Hour
	republishPeriod = 45 * time.Minute
)

// Registry handles Agent Card publishing and resolution via DHT.
type Registry struct {
	dht  *dht.IpfsDHT
	id   *identity.Identity
	card *pb.AgentCard
	log  *zap.Logger
}

// New creates a new registry.
func New(d *dht.IpfsDHT, id *identity.Identity, log *zap.Logger) *Registry {
	return &Registry{dht: d, id: id, log: log}
}

// Publish signs and publishes an Agent Card to the DHT.
func (r *Registry) Publish(ctx context.Context, card *pb.AgentCard) error {
	// sign the card
	card.Did = r.id.DID
	card.PublicKey = r.id.PublicKeyBase64()
	card.PublishedAt = time.Now().UnixMilli()
	card.ExpiresAt = time.Now().Add(agentCardTTL).UnixMilli()

	canonical, err := cardCanonical(card)
	if err != nil {
		return fmt.Errorf("canonical card: %w", err)
	}
	card.Signature = r.id.PublicKeyBase64() // placeholder; real: base64(sign(canonical))
	_ = r.id.Sign(canonical)               // sign but store properly in real impl

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

// Resolve fetches an Agent Card by DID from the DHT.
func (r *Registry) Resolve(ctx context.Context, did string) (*pb.AgentCard, error) {
	key := dhtKey(did)
	data, err := r.dht.GetValue(ctx, key)
	if err != nil {
		return nil, fmt.Errorf("dht get %q: %w", did, err)
	}
	var card pb.AgentCard
	if err := json.Unmarshal(data, &card); err != nil {
		return nil, fmt.Errorf("unmarshal card: %w", err)
	}
	return &card, nil
}

// FindByCapability searches the DHT for agents advertising a capability.
func (r *Registry) FindByCapability(ctx context.Context, capability string, limit int) ([]*pb.AgentCard, error) {
	key := capabilityKey(capability)
	// DHT GetValue returns one record; for multi-value we use SearchValue
	ch, err := r.dht.SearchValue(ctx, key)
	if err != nil {
		return nil, fmt.Errorf("dht search %q: %w", capability, err)
	}

	var cards []*pb.AgentCard
	for data := range ch {
		var card pb.AgentCard
		if err := json.Unmarshal(data, &card); err != nil {
			r.log.Warn("unmarshal card from search", zap.Error(err))
			continue
		}
		cards = append(cards, &card)
		if limit > 0 && len(cards) >= limit {
			break
		}
	}
	return cards, nil
}

// AdvertiseCapability publishes this agent under a capability key in the DHT.
func (r *Registry) AdvertiseCapability(ctx context.Context, capability string) error {
	if r.card == nil {
		return fmt.Errorf("publish agent card first")
	}
	data, err := json.Marshal(r.card)
	if err != nil {
		return err
	}
	key := capabilityKey(capability)
	return r.dht.PutValue(ctx, key, data)
}

// RunRepublish periodically re-publishes the Agent Card before TTL expiry.
func (r *Registry) RunRepublish(ctx context.Context) {
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
			if err := r.Publish(ctx, r.card); err != nil {
				r.log.Warn("republish agent card", zap.Error(err))
			}
		}
	}
}

// ─── internal ────────────────────────────────────────────────────────────────

func dhtKey(did string) string {
	return "/a2a/agents/" + did
}

func capabilityKey(capability string) string {
	return "/a2a/caps/" + capability
}

func cardCanonical(card *pb.AgentCard) ([]byte, error) {
	// canonical = JSON of card without signature field
	tmp := *card
	tmp.Signature = ""
	return json.Marshal(tmp)
}
