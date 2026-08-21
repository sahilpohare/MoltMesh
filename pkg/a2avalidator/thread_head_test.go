package a2avalidator

import (
	"encoding/json"
	"testing"

	"github.com/ipfs/go-cid"
	"github.com/multiformats/go-multihash"
)

func validThreadHeadJSON(t *testing.T, threadID string, height int64, blockCid string) []byte {
	t.Helper()
	if blockCid != "" {
		h, err := multihash.Sum([]byte(blockCid), multihash.SHA2_256, -1)
		if err != nil {
			t.Fatalf("hash fixture: %v", err)
		}
		blockCid = cid.NewCidV1(cid.Raw, h).String()
	}
	data, err := json.Marshal(struct {
		ThreadID string `json:"thread_id"`
		Height   int64  `json:"height"`
		BlockCid string `json:"block_cid"`
	}{threadID, height, blockCid})
	if err != nil {
		t.Fatalf("marshal fixture: %v", err)
	}
	return data
}

func TestThreadHeadValidator_Validate_Valid(t *testing.T) {
	v := ThreadHeadValidator{}
	data := validThreadHeadJSON(t, "thread-1", 5, "bafyabc123")
	if err := v.Validate("thread-1", data); err != nil {
		t.Fatalf("expected valid record, got error: %v", err)
	}
}

func TestThreadHeadValidatorAcceptsNamespacedRecordKey(t *testing.T) {
	v := ThreadHeadValidator{}
	data := validThreadHeadJSON(t, "thread-1", 5, "bafyabc123")
	if err := v.Validate("/threads/thread-1", data); err != nil {
		t.Fatalf("namespaced key rejected: %v", err)
	}
}

func TestThreadHeadValidator_Validate_EmptyKey(t *testing.T) {
	v := ThreadHeadValidator{}
	data := validThreadHeadJSON(t, "thread-1", 5, "bafyabc123")
	if err := v.Validate("", data); err == nil {
		t.Fatal("expected error for empty key")
	}
}

func TestThreadHeadValidator_Validate_InvalidJSON(t *testing.T) {
	v := ThreadHeadValidator{}
	if err := v.Validate("thread-1", []byte("not json")); err == nil {
		t.Fatal("expected error for invalid JSON")
	}
}

func TestThreadHeadValidator_Validate_MissingThreadID(t *testing.T) {
	v := ThreadHeadValidator{}
	data := validThreadHeadJSON(t, "", 5, "bafyabc123")
	if err := v.Validate("thread-1", data); err == nil {
		t.Fatal("expected error for missing thread_id")
	}
}

func TestThreadHeadValidator_Validate_MissingBlockCid(t *testing.T) {
	v := ThreadHeadValidator{}
	data := validThreadHeadJSON(t, "thread-1", 5, "")
	if err := v.Validate("thread-1", data); err == nil {
		t.Fatal("expected error for missing block_cid")
	}
}

func TestThreadHeadValidator_Validate_ZeroHeight(t *testing.T) {
	v := ThreadHeadValidator{}
	data := validThreadHeadJSON(t, "thread-1", 0, "bafyabc123")
	if err := v.Validate("thread-1", data); err == nil {
		t.Fatal("expected error for height < 1")
	}
}

func TestThreadHeadValidator_Select_PrefersHighestHeight(t *testing.T) {
	v := ThreadHeadValidator{}
	vals := [][]byte{
		validThreadHeadJSON(t, "thread-1", 3, "bafy3"),
		validThreadHeadJSON(t, "thread-1", 10, "bafy10"),
		validThreadHeadJSON(t, "thread-1", 5, "bafy5"),
	}
	best, err := v.Select("thread-1", vals)
	if err != nil {
		t.Fatalf("Select: %v", err)
	}
	if best != 1 {
		t.Fatalf("expected index 1 (height 10) to win, got %d", best)
	}
}

func TestThreadHeadValidator_Select_Empty(t *testing.T) {
	v := ThreadHeadValidator{}
	if _, err := v.Select("thread-1", nil); err == nil {
		t.Fatal("expected error for empty candidate list")
	}
}

func TestThreadHeadValidator_Select_IgnoresUnparseable(t *testing.T) {
	v := ThreadHeadValidator{}
	vals := [][]byte{
		validThreadHeadJSON(t, "thread-1", 5, "bafy5"),
		[]byte("garbage"),
	}
	best, err := v.Select("thread-1", vals)
	if err != nil {
		t.Fatalf("Select: %v", err)
	}
	if best != 0 {
		t.Fatalf("expected the parseable record (index 0) to win, got %d", best)
	}
}

func TestThreadHeadValidator_RecoveryEnvelopeManifest(t *testing.T) {
	v := ThreadHeadValidator{}
	h, err := multihash.Sum([]byte("recovery-envelope"), multihash.SHA2_256, -1)
	if err != nil {
		t.Fatal(err)
	}
	data, err := json.Marshal(struct {
		ThreadID    string `json:"thread_id"`
		Epoch       uint64 `json:"epoch"`
		EnvelopeCID string `json:"envelope_cid"`
	}{"thread-1", 2, cid.NewCidV1(cid.Raw, h).String()})
	if err != nil {
		t.Fatal(err)
	}
	if err := v.Validate("/threads/thread-1/recovery/2", data); err != nil {
		t.Fatalf("valid recovery manifest rejected: %v", err)
	}
	if err := v.Validate("/threads/thread-1/recovery/3", data); err == nil {
		t.Fatal("manifest accepted at a mismatched epoch key")
	}
}
