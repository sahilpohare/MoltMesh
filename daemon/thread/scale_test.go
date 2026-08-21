package thread

import (
	"context"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"go.uber.org/zap"

	"github.com/sahilpohare/p2p-a2a/daemon/actors"
	"github.com/sahilpohare/p2p-a2a/daemon/identity"
)

func TestLegacyDatabaseMigratesActorDurabilityTables(t *testing.T) {
	path := filepath.Join(t.TempDir(), "threads.db")
	store, err := NewStore(path)
	if err != nil {
		t.Fatal(err)
	}
	for _, table := range []string{"pending_entry_claims", "thread_checkpoints", "raft_hard_state", "raft_entries", "raft_snapshots"} {
		if _, err := store.db.Exec(`DROP TABLE ` + table); err != nil {
			t.Fatal(err)
		}
	}
	store.Close()
	store, err = NewStore(path)
	if err != nil {
		t.Fatalf("open legacy database: %v", err)
	}
	defer store.Close()
	for _, table := range []string{"pending_entry_claims", "thread_checkpoints", "raft_hard_state", "raft_entries", "raft_snapshots"} {
		var name string
		if err := store.db.QueryRow(`SELECT name FROM sqlite_master WHERE type='table' AND name=?`, table).Scan(&name); err != nil {
			t.Fatalf("migration did not recreate %s: %v", table, err)
		}
	}
}

// This opt-in soak is intentionally excluded from ordinary unit runs. It
// validates the defining scale property: 100k durable rows do not materialize
// 100k actors or make StartAll scan the table.
func TestSoakHundredThousandDormantThreads(t *testing.T) {
	if os.Getenv("MOLTMESH_SOAK") != "1" {
		t.Skip("set MOLTMESH_SOAK=1 to run the 100k-thread soak")
	}
	store, err := NewStore(filepath.Join(t.TempDir(), "soak.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	tx, err := store.db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	stmt, err := tx.Prepare(`INSERT INTO threads (id, creator_did, replica_dids, n, f, epoch_ms, created_at, metadata) VALUES (?, 'did:key:zLocal', '["did:key:zLocal"]', 1, 0, 1000, ?, '{}')`)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 100_000; i++ {
		if _, err := stmt.Exec("thread-"+strconv.Itoa(i), time.Now().UnixMilli()); err != nil {
			t.Fatal(err)
		}
	}
	stmt.Close()
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	id, err := identity.Generate()
	if err != nil {
		t.Fatal(err)
	}
	system, err := actors.NewSystem(ctx, zap.NewNop())
	if err != nil {
		t.Fatal(err)
	}
	defer system.Stop(ctx)
	mgr := NewActorManager(ctx, system, store, id, nil, nil, zap.NewNop())
	started := time.Now()
	if err := mgr.StartAll(ctx); err != nil {
		t.Fatal(err)
	}
	if elapsed := time.Since(started); elapsed > time.Second {
		t.Fatalf("lazy StartAll took %v", elapsed)
	}
	if pid := mgr.sup.PID("thread-99999"); pid != nil {
		t.Fatal("dormant row unexpectedly materialized an actor")
	}
}
