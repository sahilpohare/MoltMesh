package actors

import (
	"sync/atomic"
	"time"
)

// Metrics is a dependency-free process-wide actor lifecycle metric set. Its
// Snapshot method is suitable for diagnostics exporters without coupling the
// actor layer to one telemetry backend.
var Metrics runtimeMetrics

type runtimeMetrics struct {
	activeThreads      atomic.Int64
	threadActivations  atomic.Uint64
	threadPassivations atomic.Uint64
	raftSnapshots      atomic.Uint64
	raftSnapshotNanos  atomic.Uint64
	raftReadyFailures  atomic.Uint64
	wakeEnqueued       atomic.Uint64
	activeTasks        atomic.Int64
	activePeers        atomic.Int64
}

type MetricsSnapshot struct {
	ActiveThreads      int64
	ThreadActivations  uint64
	ThreadPassivations uint64
	RaftSnapshots      uint64
	RaftSnapshotTime   time.Duration
	RaftReadyFailures  uint64
	WakeEnqueued       uint64
	ActiveTasks        int64
	ActivePeers        int64
}

func (m *runtimeMetrics) ThreadActivated()  { m.activeThreads.Add(1); m.threadActivations.Add(1) }
func (m *runtimeMetrics) ThreadPassivated() { m.activeThreads.Add(-1); m.threadPassivations.Add(1) }
func (m *runtimeMetrics) RaftSnapshot(elapsed time.Duration) {
	m.raftSnapshots.Add(1)
	m.raftSnapshotNanos.Add(uint64(elapsed))
}
func (m *runtimeMetrics) RaftReadyFailed() { m.raftReadyFailures.Add(1) }
func (m *runtimeMetrics) WakeQueued()      { m.wakeEnqueued.Add(1) }
func (m *runtimeMetrics) TaskActivated()   { m.activeTasks.Add(1) }
func (m *runtimeMetrics) TaskPassivated()  { m.activeTasks.Add(-1) }
func (m *runtimeMetrics) PeerActivated()   { m.activePeers.Add(1) }
func (m *runtimeMetrics) PeerPassivated()  { m.activePeers.Add(-1) }

func (m *runtimeMetrics) Snapshot() MetricsSnapshot {
	return MetricsSnapshot{
		ActiveThreads: m.activeThreads.Load(), ThreadActivations: m.threadActivations.Load(),
		ThreadPassivations: m.threadPassivations.Load(), RaftSnapshots: m.raftSnapshots.Load(),
		RaftSnapshotTime: time.Duration(m.raftSnapshotNanos.Load()), RaftReadyFailures: m.raftReadyFailures.Load(),
		WakeEnqueued: m.wakeEnqueued.Load(), ActiveTasks: m.activeTasks.Load(), ActivePeers: m.activePeers.Load(),
	}
}
