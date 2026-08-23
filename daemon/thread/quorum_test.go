package thread

import (
	"fmt"
	"testing"

	pb "github.com/sahilpohare/p2p-a2a/gen/a2a/v1"
)

// TestValidatorSetSizeByBackend pins the validator-set sizing rule, which
// differs by failure model: Tendermint needs 3f+1 to keep any two 2f+1 quorums
// intersecting in an honest validator, Raft needs only 2f+1 because a majority
// of 2f+1 already intersects. Getting this wrong is silent in tests that only
// use f=0, so it is asserted directly.
func TestValidatorSetSizeByBackend(t *testing.T) {
	dids := func(n int) []string {
		out := make([]string, n)
		for i := range out {
			out[i] = fmt.Sprintf("did:key:zReplica%d", i)
		}
		return out
	}

	// extra counts replicas supplied *in addition to* the creator: the creator
	// is prepended to the replica set automatically when absent, so the
	// effective set size is extra+1.
	cases := []struct {
		name    string
		backend string
		f       int32
		extra   int
		wantN   int32
		wantErr bool
	}{
		{"raft f=1 needs 3 total", "raft", 1, 2, 3, false},
		{"raft f=1 with spare replicas keeps N=3", "raft", 1, 5, 3, false},
		{"raft f=1 with only 2 total is rejected", "raft", 1, 1, 0, true},
		{"raft f=2 needs 5 total", "raft", 2, 4, 5, false},
		{"tendermint f=1 needs 4 total", "tendermint", 1, 3, 4, false},
		{"tendermint f=1 with only 3 total is rejected", "tendermint", 1, 2, 0, true},
		{"default backend is raft", "", 1, 2, 3, false},
		{"f=0 is single validator", "raft", 0, 0, 1, false},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			meta := map[string]string{}
			if tc.backend != "" {
				meta["backend"] = tc.backend
			}
			th, err := NewThreadFromRequest("did:key:zCreator", &pb.CreateThreadRequest{
				ReplicaDids: dids(tc.extra),
				F:           tc.f,
				Metadata:    meta,
			})
			if tc.wantErr {
				if err == nil {
					t.Fatalf("expected rejection, got thread with N=%d", th.N)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if th.N != tc.wantN {
				t.Errorf("N = %d, want %d", th.N, tc.wantN)
			}
			if th.F != tc.f {
				t.Errorf("F = %d, want %d", th.F, tc.f)
			}
		})
	}
}

// TestRaftVoterSetToleratesF checks the property the sizing rule exists to
// guarantee: with N = 2f+1 voters, an etcd/raft majority still leaves the
// cluster live after f failures.
func TestRaftVoterSetToleratesF(t *testing.T) {
	for f := int32(1); f <= 3; f++ {
		n := 2*f + 1
		majority := n/2 + 1
		survivors := n - f
		if survivors < majority {
			t.Errorf("f=%d: N=%d leaves %d survivors, below majority %d", f, n, survivors, majority)
		}
	}
}

// TestMaxTolerableFailures checks the inverse of the sizing rule, which the
// error message quotes back to callers who supply too few replicas.
func TestMaxTolerableFailures(t *testing.T) {
	cases := []struct {
		backend  BackendKind
		replicas int
		want     int
	}{
		{BackendRaft, 1, 0},
		{BackendRaft, 2, 0}, // two nodes cannot survive one failure
		{BackendRaft, 3, 1},
		{BackendRaft, 5, 2},
		{BackendTendermint, 3, 0},
		{BackendTendermint, 4, 1},
		{BackendTendermint, 7, 2},
	}
	for _, tc := range cases {
		if got := maxTolerableFailures(tc.backend, tc.replicas); got != tc.want {
			t.Errorf("%s with %d replicas: max f = %d, want %d", tc.backend, tc.replicas, got, tc.want)
		}
	}
}
