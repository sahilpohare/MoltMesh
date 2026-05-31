package a2avalidator

import (
	"encoding/json"
	"fmt"
	"strings"

	record "github.com/libp2p/go-libp2p-record"
)

// AgentCardValidator validates DHT records stored under the /a2a/ namespace.
// Keys follow the format /a2a/agents/<did>.
// Values are JSON-encoded AgentCard objects.
// Non-A2A peers that don't have this validator will reject puts — this is
// acceptable in a mixed network; A2A peers will validate and store correctly.
type AgentCardValidator struct{}

// Validate checks that the key is a valid /a2a/agents/<did> path and the
// value is parseable JSON with a non-empty DID field.
func (v AgentCardValidator) Validate(key string, value []byte) error {
	if !strings.HasPrefix(key, "/a2a/agents/") {
		return fmt.Errorf("a2avalidator: unexpected key %q (want /a2a/agents/<did>)", key)
	}
	did := strings.TrimPrefix(key, "/a2a/agents/")
	if did == "" {
		return fmt.Errorf("a2avalidator: empty DID in key %q", key)
	}
	var card struct {
		DID string `json:"did"`
	}
	if err := json.Unmarshal(value, &card); err != nil {
		return fmt.Errorf("a2avalidator: invalid agent card JSON: %w", err)
	}
	if card.DID == "" {
		return fmt.Errorf("a2avalidator: agent card missing did field")
	}
	return nil
}

// Select returns the index of the best record among candidates.
// Prefers the record with the highest published_at field.
func (v AgentCardValidator) Select(_ string, vals [][]byte) (int, error) {
	if len(vals) == 0 {
		return 0, fmt.Errorf("a2avalidator: no values to select from")
	}
	type card struct {
		PublishedAt int64 `json:"published_at"`
	}
	best := 0
	var bestCard card
	_ = json.Unmarshal(vals[0], &bestCard)
	for i := 1; i < len(vals); i++ {
		var c card
		if err := json.Unmarshal(vals[i], &c); err != nil {
			continue
		}
		if c.PublishedAt > bestCard.PublishedAt {
			best = i
			bestCard = c
		}
	}
	return best, nil
}

// Ensure AgentCardValidator implements record.Validator.
var _ record.Validator = AgentCardValidator{}
