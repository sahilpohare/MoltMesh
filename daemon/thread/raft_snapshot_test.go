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

// A promoted voter must survive a restart as a voter. The raft voter set is
// bootstrapped from the first N ReplicaDids, so a ConfChange that only altered
// raft's in-memory config left the descriptor at its pre-promotion N: the
// promoted node came back up with the old voter set, its MsgProp never counted
// toward a quorum, and its appends committed nowhere.
func TestPersistVoterCount_RecordsCommittedVoterSet(t *testing.T) {
	store, err := NewStore(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	id, err := identity.Generate()
	if err != nil {
		t.Fatal(err)
	}
	th := &pb.Thread{
		Id:          "t-voters",
		CreatorDid:  id.DID,
		ReplicaDids: []string{id.DID, "did:key:zObserver"},
		N:           1,
	}
	if err := store.SaveThread(th); err != nil {
		t.Fatal(err)
	}
	r := &RaftBackend{thread: th, id: id, store: store, log: zap.NewNop()}

	r.persistVoterCount(2)

	if th.N != 2 {
		t.Fatalf("in-memory N = %d, want 2", th.N)
	}
	reloaded, err := store.GetThread(th.Id)
	if err != nil {
		t.Fatal(err)
	}
	if reloaded.N != 2 {
		t.Fatalf("persisted N = %d, want 2: a restart rebuilds the old voter set", reloaded.N)
	}
}

// applyMembershipEntries maintains the raft routing maps, which every replica
// needs: sendRaftMsg drops any message whose destination is missing from
// didByID. Restricting the whole function to the creator left a late-added
// replica unaddressable by its own peers.
func TestApplyMembershipEntries_UpdatesRoutingOnNonCreator(t *testing.T) {
	store, err := NewStore(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	id, err := identity.Generate() // this node is NOT the creator
	if err != nil {
		t.Fatal(err)
	}
	const creator, newcomer = "did:key:zCreator", "did:key:zNewcomer"
	th := &pb.Thread{
		Id:          "t-routing",
		CreatorDid:  creator,
		ReplicaDids: []string{creator, id.DID},
		N:           1,
	}
	r := &RaftBackend{
		thread:  th,
		id:      id,
		store:   store,
		log:     zap.NewNop(),
		peerIDs: map[string]uint64{creator: 1, id.DID: 2},
		didByID: map[uint64]string{1: creator, 2: id.DID},
	}

	r.applyMembershipEntries([]*pb.ThreadEntry{{
		Kind: "membership:add-observer", Payload: []byte(newcomer),
	}})

	if got := r.peerIDs[newcomer]; got != 3 {
		t.Fatalf("peerIDs[newcomer] = %d, want 3", got)
	}
	if got := r.didByID[3]; got != newcomer {
		t.Fatalf("didByID[3] = %q, want the newcomer: sendRaftMsg would drop every message to it", got)
	}
}
