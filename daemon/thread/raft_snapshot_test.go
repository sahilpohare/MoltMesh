package thread

import (
	"context"
	"testing"

	"go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"
	"go.uber.org/zap"

	"github.com/sahilpohare/p2p-a2a/daemon/identity"

	pb "github.com/sahilpohare/p2p-a2a/gen/a2a/v1"
)

// A follower that catches up via a leader snapshot has no log entries covering
// the snapshot index, so the snapshot is the only thing justifying the commit
// index in the HardState persisted alongside it. handleReady must persist a
// received snapshot; applying it to memory alone meant that on restart
// LoadRaftState returned commit=N with an empty log and newRaft panicked with
// "state.commit N is out of range [1, 1]" before the thread actor could start.
func TestHandleReady_PersistsSnapshotFromLeader(t *testing.T) {
	store, err := NewStore(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	const threadID = "t-snap"
	r := &RaftBackend{
		thread:  &pb.Thread{Id: threadID},
		store:   store,
		storage: raft.NewMemoryStorage(),
		log:     zap.NewNop(),
	}

	snap := raftpb.Snapshot{Metadata: raftpb.SnapshotMetadata{
		Index:     5,
		Term:      2,
		ConfState: raftpb.ConfState{Voters: []uint64{1, 2}},
	}}
	ok := r.handleReady(raft.Ready{
		Snapshot:  snap,
		HardState: raftpb.HardState{Term: 2, Vote: 1, Commit: 5},
	}, func(*pb.ConsensusMsg) {})
	if !ok {
		t.Fatal("handleReady returned false")
	}

	// Reload exactly as newRaftBackend does.
	gotSnap, hs, entries, err := store.LoadRaftState(threadID)
	if err != nil {
		t.Fatal(err)
	}
	if raft.IsEmptySnap(gotSnap) {
		t.Fatal("received snapshot was not persisted: restart panics on a commit index the log cannot cover")
	}

	storage := raft.NewMemoryStorage()
	if err := storage.ApplySnapshot(gotSnap); err != nil {
		t.Fatal(err)
	}
	if len(entries) > 0 {
		if err := storage.Append(entries); err != nil {
			t.Fatal(err)
		}
	}
	if err := storage.SetHardState(hs); err != nil {
		t.Fatal(err)
	}
	first, err := storage.FirstIndex()
	if err != nil {
		t.Fatal(err)
	}
	last, err := storage.LastIndex()
	if err != nil {
		t.Fatal(err)
	}
	if hs.Commit < first-1 || hs.Commit > last {
		t.Fatalf("commit %d outside [%d, %d]: newRaft would panic", hs.Commit, first-1, last)
	}
}

// fakeRaftNode reports a fixed Status and records whether Propose was called.
type fakeRaftNode struct {
	raft.Node
	lead, self uint64
	proposed   bool
}

func (f *fakeRaftNode) Status() raft.Status {
	st := raft.Status{}
	st.ID = f.self
	st.Lead = f.lead
	return st
}

func (f *fakeRaftNode) Propose(context.Context, []byte) error {
	f.proposed = true
	return nil
}

// proposePending must run on any voter that knows the leader, not only on the
// leader itself. etcd/raft turns a follower's Propose into a MsgProp addressed
// to the leader and the transport forwards it. Returning early on a follower
// left its entry claimed-but-never-proposed, so append-entry reported success
// and the entry committed on no node at all.
func TestProposePending_FollowerProposesToLeader(t *testing.T) {
	for _, c := range []struct {
		name       string
		lead, self uint64
		want       bool
	}{
		{"leader", 1, 1, true},
		{"follower with known leader", 1, 2, true},
		{"no leader elected yet", raft.None, 2, false},
	} {
		t.Run(c.name, func(t *testing.T) {
			store, err := NewStore(":memory:")
			if err != nil {
				t.Fatal(err)
			}
			defer store.Close()

			const threadID = "t-propose"
			if err := store.EnqueueEntry(threadID, &pb.ThreadEntry{
				AuthorDid: "did:key:zAuthor", Payload: []byte("x"), Kind: "message",
			}); err != nil {
				t.Fatal(err)
			}

			id, err := identity.Generate()
			if err != nil {
				t.Fatal(err)
			}
			fake := &fakeRaftNode{lead: c.lead, self: c.self}
			r := &RaftBackend{
				thread: &pb.Thread{Id: threadID},
				store:  store,
				node:   fake,
				id:     id,
				selfID: c.self,
				log:    zap.NewNop(),
			}

			r.proposePending(context.Background())

			if fake.proposed != c.want {
				t.Fatalf("proposed = %v, want %v", fake.proposed, c.want)
			}
		})
	}
}
