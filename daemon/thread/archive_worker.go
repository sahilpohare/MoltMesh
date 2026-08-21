package thread

// ArchiveWorker turns the existing content-addressed history format into a
// real archive-provider role.  It never accepts a provider's statement about
// history: it discovers candidates, fetches through Bitswap, validates the
// descriptor and the complete hash chain, and only then makes the blocks
// durable locally and signs a receipt.

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/ipfs/boxo/bitswap"
	"github.com/ipfs/boxo/blockstore"
	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	dht "github.com/libp2p/go-libp2p-kad-dht"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
	"github.com/libp2p/go-libp2p/core/host"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"

	"github.com/sahilpohare/p2p-a2a/daemon/identity"
	pb "github.com/sahilpohare/p2p-a2a/gen/a2a/v1"
)

type ArchiveWorker struct {
	dht        *dht.IpfsDHT
	host       host.Host
	blockstore blockstore.Blockstore
	bitswap    *bitswap.Bitswap
	store      *Store
	id         *identity.Identity
	log        *zap.Logger
	ps         *pubsub.PubSub

	mu             sync.Mutex
	inFlight       map[string]struct{}
	ackTopics      map[string]*pubsub.Topic
	recoveryTopics map[string]*pubsub.Topic
}

func NewArchiveWorker(d *dht.IpfsDHT, h host.Host, ps *pubsub.PubSub, bs blockstore.Blockstore, bsw *bitswap.Bitswap, store *Store, id *identity.Identity, log *zap.Logger) *ArchiveWorker {
	return &ArchiveWorker{dht: d, host: h, ps: ps, blockstore: bs, bitswap: bsw, store: store, id: id, log: log, inFlight: make(map[string]struct{}), ackTopics: make(map[string]*pubsub.Topic), recoveryTopics: make(map[string]*pubsub.Topic)}
}

// Run refreshes every thread this daemon already knows about.  This is safe
// for validator and archive-only nodes alike: Sync is idempotent and only
// installs history after independent verification.
func (w *ArchiveWorker) Run(ctx context.Context, interval time.Duration) {
	if interval <= 0 {
		interval = time.Minute
	}
	w.syncKnown(ctx)
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			w.syncKnown(ctx)
		}
	}
}

func (w *ArchiveWorker) syncKnown(ctx context.Context) {
	if w == nil || w.store == nil {
		return
	}
	threads, err := w.store.ListThreads()
	if err != nil {
		w.log.Warn("archive worker: list threads", zap.Error(err))
		return
	}
	for _, th := range threads {
		w.ensureAckSubscription(ctx, th.Id)
		w.ensureRecoveryEnvelopeSubscription(ctx, th.Id)
		if _, err := w.Sync(ctx, th.Id); err != nil && ctx.Err() == nil {
			w.log.Debug("archive worker: sync failed", zap.String("thread", th.Id), zap.Error(err))
		}
	}
}

func (w *ArchiveWorker) ensureRecoveryEnvelopeSubscription(ctx context.Context, threadID string) {
	if w.ps == nil || threadID == "" {
		return
	}
	w.mu.Lock()
	if _, ok := w.recoveryTopics[threadID]; ok {
		w.mu.Unlock()
		return
	}
	topic, err := w.ps.Join(RecoveryEnvelopeTopic(threadID))
	if err != nil {
		w.mu.Unlock()
		w.log.Debug("recovery envelope topic", zap.Error(err))
		return
	}
	w.recoveryTopics[threadID] = topic
	w.mu.Unlock()
	sub, err := topic.Subscribe()
	if err != nil {
		w.log.Debug("recovery envelope subscription", zap.Error(err))
		return
	}
	go func() {
		defer sub.Cancel()
		for {
			msg, err := sub.Next(ctx)
			if err != nil {
				return
			}
			envelope := &pb.ThreadKeyEnvelope{}
			if err := proto.Unmarshal(msg.Data, envelope); err != nil || !envelope.RecoveryEnvelope || envelope.ThreadId != threadID {
				continue
			}
			th, err := w.store.GetThread(threadID)
			if err != nil || VerifyRecoveryKeyEnvelopeCreator(th.CreatorDid, envelope) != nil {
				_ = w.store.RecordArchiveDiagnostic(ArchiveDiagnostic{ThreadID: threadID, ProviderDID: "unknown", Kind: "invalid-recovery-envelope", Detail: "creator signature verification failed"})
				continue
			}
			if err := w.store.SaveRecoveryKeyEnvelope(envelope); err != nil {
				_ = w.store.RecordArchiveDiagnostic(ArchiveDiagnostic{ThreadID: threadID, ProviderDID: "unknown", Kind: "invalid-recovery-envelope", Detail: err.Error()})
			}
		}
	}()
}

func archiveAcknowledgementTopic(threadID string) string {
	return "moltmesh/thread/" + threadID + "/archive-acks/v1"
}

// ensureAckSubscription creates one durable receipt channel per known thread.
// Receipts are independently signed, so receiving one is never sufficient to
// trust a provider without VerifyArchiveAcknowledgement in SaveArchiveAcknowledgement.
func (w *ArchiveWorker) ensureAckSubscription(ctx context.Context, threadID string) {
	if w.ps == nil || threadID == "" {
		return
	}
	w.mu.Lock()
	if _, ok := w.ackTopics[threadID]; ok {
		w.mu.Unlock()
		return
	}
	topic, err := w.ps.Join(archiveAcknowledgementTopic(threadID))
	if err != nil {
		w.mu.Unlock()
		w.log.Debug("archive receipt topic", zap.Error(err))
		return
	}
	w.ackTopics[threadID] = topic
	w.mu.Unlock()
	sub, err := topic.Subscribe()
	if err != nil {
		w.log.Debug("archive receipt subscription", zap.Error(err))
		return
	}
	go func() {
		defer sub.Cancel()
		for {
			msg, err := sub.Next(ctx)
			if err != nil {
				return
			}
			ack := &pb.ArchiveAcknowledgement{}
			if err := proto.Unmarshal(msg.Data, ack); err != nil {
				continue
			}
			if err := w.store.SaveArchiveAcknowledgement(ack); err != nil {
				_ = w.store.RecordArchiveDiagnostic(ArchiveDiagnostic{ThreadID: threadID, ProviderDID: ack.ProviderDid, Kind: "invalid-ack", Detail: err.Error()})
			}
		}
	}()
}

func (w *ArchiveWorker) publishAcknowledgement(ctx context.Context, ack *pb.ArchiveAcknowledgement) {
	if w.ps == nil || ack == nil {
		return
	}
	w.ensureAckSubscription(ctx, ack.ThreadId)
	w.mu.Lock()
	topic := w.ackTopics[ack.ThreadId]
	w.mu.Unlock()
	if topic == nil {
		return
	}
	raw, err := proto.MarshalOptions{Deterministic: true}.Marshal(ack)
	if err == nil {
		_ = topic.Publish(ctx, raw)
	}
}

// Sync fetches and retains a verified history and returns this provider's
// signed receipt for its current head.  The caller may publish that receipt
// through its policy-specific control plane; merely having a receipt does not
// make an unverified transfer count toward a quorum.
func (w *ArchiveWorker) Sync(ctx context.Context, threadID string) (*pb.ArchiveAcknowledgement, error) {
	if w == nil || w.dht == nil || w.bitswap == nil || w.blockstore == nil || w.store == nil || w.id == nil {
		return nil, fmt.Errorf("archive worker is not fully configured")
	}
	if threadID == "" {
		return nil, fmt.Errorf("thread id required")
	}
	w.mu.Lock()
	if _, busy := w.inFlight[threadID]; busy {
		w.mu.Unlock()
		return nil, fmt.Errorf("archive sync already in progress for %q", threadID)
	}
	w.inFlight[threadID] = struct{}{}
	w.mu.Unlock()
	defer func() { w.mu.Lock(); delete(w.inFlight, threadID); w.mu.Unlock() }()

	providerCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	providers, err := DiscoverArchiveProviders(providerCtx, w.dht, threadID, 8)
	if err == nil && w.host != nil {
		for _, provider := range providers {
			if provider.ID != w.host.ID() {
				_ = w.host.Connect(providerCtx, provider) // Bitswap will select healthy peers.
			}
		}
	}
	cancel()

	head, err := ResolveHead(ctx, w.dht, threadID)
	if err != nil {
		return nil, err
	}
	if head.Thread == nil || head.Thread.Id != threadID {
		return nil, fmt.Errorf("published thread descriptor is unavailable")
	}
	if err := VerifyDescriptor(head.Thread); err != nil {
		return nil, fmt.Errorf("verify archive descriptor: %w", err)
	}
	kind := BackendKind(head.Thread.Metadata["backend"])
	if kind == "" {
		kind = BackendRaft
	}
	fetched := make(map[string]blocks.Block)
	chain, err := VerifyChainFromHead(ctx, kind, threadID,
		func(c context.Context, id string) (*ThreadHead, error) { return ResolveHead(c, w.dht, id) },
		func(c context.Context, id string, height int64) (*ThreadHead, error) {
			return ResolveHeight(c, w.dht, id, height)
		},
		func(fetchCtx context.Context, cidStr string) (*pb.ThreadBlock, error) {
			blockCID, err := cid.Decode(cidStr)
			if err != nil {
				return nil, fmt.Errorf("invalid archive block CID %q: %w", cidStr, err)
			}
			blk, err := w.bitswap.GetBlock(fetchCtx, blockCID)
			if err != nil {
				return nil, err
			}
			var block pb.ThreadBlock
			if err := proto.Unmarshal(blk.RawData(), &block); err != nil {
				return nil, err
			}
			fetched[cidStr] = blk
			return &block, nil
		},
	)
	if err != nil {
		return nil, fmt.Errorf("verify archive chain: %w", err)
	}
	if err := w.store.ImportHistory(head.Thread, chain); err != nil {
		return nil, fmt.Errorf("persist verified archive: %w", err)
	}
	retained := make([]blocks.Block, 0, len(fetched))
	for _, blk := range fetched {
		if err := w.blockstore.Put(ctx, blk); err != nil {
			return nil, fmt.Errorf("retain archive block: %w", err)
		}
		retained = append(retained, blk)
	}
	if len(retained) > 0 {
		_ = w.bitswap.NotifyNewBlocks(ctx, retained...)
	}
	if err := AdvertiseArchiveProvider(ctx, w.dht, threadID); err != nil {
		return nil, err
	}
	ack, err := NewArchiveAcknowledgement(w.id, threadID, chain[len(chain)-1].BlockHash, time.Now())
	if err != nil {
		return nil, err
	}
	if err := w.store.SaveArchiveAcknowledgement(ack); err != nil {
		return nil, err
	}
	w.publishAcknowledgement(ctx, ack)
	return ack, nil
}
