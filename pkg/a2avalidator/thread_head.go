package a2avalidator

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"

	"github.com/ipfs/go-cid"
	record "github.com/libp2p/go-libp2p-record"
)

// ThreadHeadValidator validates DHT records stored under the /a2a/threads/
// namespace. Keys follow the format /a2a/threads/<thread_id>. Values are
// JSON-encoded ThreadHead pointers (see daemon/thread's publisher) —
// published so a peer with no ongoing involvement in a thread (not in
// replica_dids, or joining after every replica went offline) can still
// discover the thread's latest committed block CID and fetch/verify the
// chain via Bitswap, independent of any live consensus participant.
type ThreadHeadValidator struct{}

// Validate checks that the value is parseable JSON with a non-empty
// thread_id and block_cid, and a positive height. Ownership/authenticity of
// a given height's content is established by content addressing (the
// pointer names a CID; fetching and independently verifying that CID's
// chain, not trusting whoever served the DHT record, is what makes the
// pointer safe to consume — see daemon/thread.VerifyChain).
func (v ThreadHeadValidator) Validate(key string, value []byte) error {
	key = stripNamespace(key, "threads/")
	if key == "" {
		return fmt.Errorf("a2avalidator: empty key")
	}
	var head struct {
		ThreadID string `json:"thread_id"`
		BlockCid string `json:"block_cid"`
		Height   int64  `json:"height"`
	}
	if err := json.Unmarshal(value, &head); err != nil {
		return fmt.Errorf("a2avalidator: invalid thread head JSON: %w", err)
	}
	if head.ThreadID == "" {
		return fmt.Errorf("a2avalidator: thread head missing thread_id field")
	}
	parts := strings.Split(key, "/")
	if len(parts) == 3 && parts[1] == "recovery" {
		var envelope struct {
			ThreadID    string `json:"thread_id"`
			Epoch       uint64 `json:"epoch"`
			EnvelopeCID string `json:"envelope_cid"`
		}
		if err := json.Unmarshal(value, &envelope); err != nil {
			return fmt.Errorf("a2avalidator: invalid recovery envelope JSON: %w", err)
		}
		if parts[0] == "" || envelope.ThreadID != parts[0] || envelope.Epoch == 0 || envelope.EnvelopeCID == "" {
			return fmt.Errorf("a2avalidator: invalid recovery envelope record")
		}
		if epoch, err := strconv.ParseUint(parts[2], 10, 64); err != nil || epoch != envelope.Epoch {
			return fmt.Errorf("a2avalidator: recovery envelope epoch does not match key")
		}
		if _, err := cid.Decode(envelope.EnvelopeCID); err != nil {
			return fmt.Errorf("a2avalidator: invalid recovery envelope CID: %w", err)
		}
		return nil
	}
	if len(parts) > 2 || parts[0] == "" || head.ThreadID != parts[0] {
		return fmt.Errorf("a2avalidator: thread_id does not match key")
	}
	if len(parts) == 2 {
		height, err := strconv.ParseInt(parts[1], 10, 64)
		if err != nil || height != head.Height {
			return fmt.Errorf("a2avalidator: height does not match key")
		}
	}
	if head.BlockCid == "" {
		return fmt.Errorf("a2avalidator: thread head missing block_cid field")
	}
	if _, err := cid.Decode(head.BlockCid); err != nil {
		return fmt.Errorf("a2avalidator: invalid block CID: %w", err)
	}
	if head.Height < 1 {
		return fmt.Errorf("a2avalidator: thread head height must be >= 1")
	}
	return nil
}

// Select returns the index of the record with the greatest height — the
// furthest-advanced head is always the best one to serve, since a thread's
// committed height only ever increases (see ADR-0010's Raft/Tendermint
// safety properties) and content addressing means a lower-height pointer is
// just stale, never a competing fork.
func (v ThreadHeadValidator) Select(key string, vals [][]byte) (int, error) {
	type head struct {
		Height int64 `json:"height"`
	}
	return selectBest(key, vals, v.Validate, func(h head) int64 { return h.Height }, "thread heads")
}

// Ensure ThreadHeadValidator implements record.Validator.
var _ record.Validator = ThreadHeadValidator{}
