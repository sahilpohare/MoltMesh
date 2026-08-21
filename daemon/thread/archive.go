package thread

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"time"

	"github.com/ipfs/boxo/bitswap"
	"github.com/ipfs/boxo/blockstore"
	"github.com/ipfs/go-cid"
	dht "github.com/libp2p/go-libp2p-kad-dht"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/multiformats/go-multihash"
	"google.golang.org/protobuf/proto"

	"github.com/sahilpohare/p2p-a2a/daemon/identity"
	pb "github.com/sahilpohare/p2p-a2a/gen/a2a/v1"
	"github.com/sahilpohare/p2p-a2a/pkg/p2putil"
)

// NewArchiveAcknowledgement creates a receipt that can safely be counted by
// an archive durability policy.  Signing the exact deterministic protobuf
// representation keeps the receipt independent from its transport.
func NewArchiveAcknowledgement(id *identity.Identity, threadID, blockHash string, at time.Time) (*pb.ArchiveAcknowledgement, error) {
	if id == nil || threadID == "" || blockHash == "" {
		return nil, fmt.Errorf("archive acknowledgement requires identity, thread id, and block hash")
	}
	ack := &pb.ArchiveAcknowledgement{
		ThreadId:             threadID,
		ProviderDid:          id.DID,
		BlockHash:            blockHash,
		AcknowledgedAtUnixMs: at.UnixMilli(),
	}
	data, err := proto.MarshalOptions{Deterministic: true}.Marshal(ack)
	if err != nil {
		return nil, err
	}
	ack.Signature = id.Sign(data)
	return ack, nil
}

// ArchiveProviderCID is the stable rendezvous key under which archive
// providers advertise availability for a thread. It deliberately derives from
// public thread metadata only; payload keys and ciphertext never enter DHT
// routing records.
func ArchiveProviderCID(threadID string) (cid.Cid, error) {
	if threadID == "" {
		return cid.Undef, fmt.Errorf("thread id required")
	}
	hash, err := multihash.Sum([]byte("moltmesh/archive/v1/"+threadID), multihash.SHA2_256, -1)
	if err != nil {
		return cid.Undef, err
	}
	return cid.NewCidV1(cid.Raw, hash), nil
}

// RecoveryEnvelopeTopic is deliberately separate from the public head and
// archive-provider rendezvous. It carries only capability-encrypted envelope
// ciphertext, never a recovery secret or plaintext epoch key. Archive workers
// persist verified-shaped envelopes so a recovery service can later return
// them after validating the caller's capability commitment.
func RecoveryEnvelopeTopic(threadID string) string {
	return "moltmesh/thread/" + threadID + "/recovery-envelopes/v1"
}

type RecoveryEnvelopeHead struct {
	ThreadID    string `json:"thread_id"`
	Epoch       uint64 `json:"epoch"`
	EnvelopeCID string `json:"envelope_cid"`
	PublishedAt int64  `json:"published_at"`
}

func recoveryEnvelopeDHTKey(threadID string, epoch uint64) string {
	return "/threads/" + threadID + "/recovery/" + strconv.FormatUint(epoch, 10)
}

// PublishRecoveryEnvelope creates a replayable DHT pointer to a
// content-addressed, creator-signed ciphertext envelope.
func PublishRecoveryEnvelope(ctx context.Context, d *dht.IpfsDHT, bs blockstore.Blockstore, bsw *bitswap.Bitswap, envelope *pb.ThreadKeyEnvelope) error {
	if d == nil || bs == nil || envelope == nil {
		return fmt.Errorf("recovery envelope publishing requires DHT, blockstore, and envelope")
	}
	raw, err := proto.MarshalOptions{Deterministic: true}.Marshal(envelope)
	if err != nil {
		return err
	}
	blk, err := p2putil.CIDv1Block(raw)
	if err != nil {
		return err
	}
	if err := bs.Put(ctx, blk); err != nil {
		return err
	}
	if bsw != nil {
		_ = bsw.NotifyNewBlocks(ctx, blk)
	}
	head, err := json.Marshal(RecoveryEnvelopeHead{ThreadID: envelope.ThreadId, Epoch: envelope.EncryptionEpoch, EnvelopeCID: blk.Cid().String(), PublishedAt: time.Now().UnixMilli()})
	if err != nil {
		return err
	}
	return d.PutValue(ctx, recoveryEnvelopeDHTKey(envelope.ThreadId, envelope.EncryptionEpoch), head)
}

func ResolveRecoveryEnvelope(ctx context.Context, d *dht.IpfsDHT, threadID string, epoch uint64) (*RecoveryEnvelopeHead, error) {
	raw, err := d.GetValue(ctx, recoveryEnvelopeDHTKey(threadID, epoch))
	if err != nil {
		return nil, err
	}
	head := &RecoveryEnvelopeHead{}
	if err := json.Unmarshal(raw, head); err != nil {
		return nil, err
	}
	if head.ThreadID != threadID || head.Epoch != epoch {
		return nil, fmt.Errorf("recovery envelope manifest does not match request")
	}
	if _, err := cid.Decode(head.EnvelopeCID); err != nil {
		return nil, err
	}
	return head, nil
}

func FetchRecoveryEnvelope(ctx context.Context, bsw *bitswap.Bitswap, cidString string) (*pb.ThreadKeyEnvelope, error) {
	if bsw == nil {
		return nil, fmt.Errorf("bitswap required")
	}
	c, err := cid.Decode(cidString)
	if err != nil {
		return nil, err
	}
	blk, err := bsw.GetBlock(ctx, c)
	if err != nil {
		return nil, err
	}
	envelope := &pb.ThreadKeyEnvelope{}
	if err := proto.Unmarshal(blk.RawData(), envelope); err != nil {
		return nil, err
	}
	return envelope, nil
}

// VerifyArchiveAcknowledgement authenticates a provider receipt over the
// deterministic acknowledgement fields before it can satisfy durability policy.
func VerifyArchiveAcknowledgement(ack *pb.ArchiveAcknowledgement) error {
	if ack == nil || ack.ThreadId == "" || ack.ProviderDid == "" || ack.BlockHash == "" || len(ack.Signature) == 0 {
		return fmt.Errorf("invalid archive acknowledgement")
	}
	signature := ack.Signature
	copy := proto.Clone(ack).(*pb.ArchiveAcknowledgement)
	copy.Signature = nil
	data, err := proto.MarshalOptions{Deterministic: true}.Marshal(copy)
	if err != nil {
		return err
	}
	ok, err := identity.VerifyDID(ack.ProviderDid, data, signature)
	if err != nil || !ok {
		return fmt.Errorf("invalid archive acknowledgement signature")
	}
	return nil
}

// VerifyRecoveryKeyEnvelopeCreator prevents a relay or archive provider from
// replacing opaque recovery ciphertext. The creator signs the deterministic
// envelope with this field cleared; providers can verify it without learning
// the epoch key or recovery secret.
func VerifyRecoveryKeyEnvelopeCreator(creatorDID string, envelope *pb.ThreadKeyEnvelope) error {
	if creatorDID == "" || envelope == nil || !envelope.RecoveryEnvelope || len(envelope.AuthorSignature) == 0 {
		return fmt.Errorf("signed recovery key envelope required")
	}
	signature := append([]byte(nil), envelope.AuthorSignature...)
	copy := proto.Clone(envelope).(*pb.ThreadKeyEnvelope)
	copy.AuthorSignature = nil
	raw, err := proto.MarshalOptions{Deterministic: true}.Marshal(copy)
	if err != nil {
		return err
	}
	ok, err := identity.VerifyDID(creatorDID, raw, signature)
	if err != nil || !ok {
		return fmt.Errorf("invalid recovery key envelope signature")
	}
	return nil
}

// AdvertiseArchiveProvider makes this daemon discoverable as an archive source
// for a thread. The caller must ensure ciphertext blocks are already durable.
func AdvertiseArchiveProvider(ctx context.Context, d *dht.IpfsDHT, threadID string) error {
	if d == nil {
		return fmt.Errorf("archive provider discovery requires DHT")
	}
	c, err := ArchiveProviderCID(threadID)
	if err != nil {
		return err
	}
	return d.Provide(ctx, c, true)
}

// DiscoverArchiveProviders returns candidate libp2p providers for a thread.
// Consumers still cryptographically verify every fetched block and chain.
func DiscoverArchiveProviders(ctx context.Context, d *dht.IpfsDHT, threadID string, limit int) ([]peer.AddrInfo, error) {
	if d == nil {
		return nil, fmt.Errorf("archive provider discovery requires DHT")
	}
	c, err := ArchiveProviderCID(threadID)
	if err != nil {
		return nil, err
	}
	seen := make(map[peer.ID]struct{})
	var providers []peer.AddrInfo
	for provider := range d.FindProvidersAsync(ctx, c, limit) {
		if _, ok := seen[provider.ID]; ok {
			continue
		}
		seen[provider.ID] = struct{}{}
		providers = append(providers, provider)
	}
	return providers, nil
}
