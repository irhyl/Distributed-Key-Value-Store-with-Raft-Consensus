// metrics.go - Prometheus instrumentation for the KV server.
//
// This is the only file in the project that imports a metrics library.
// raft/wal/storage stay decoupled from any specific backend: they expose
// plain optional callback hooks (Node.OnBecomeLeader, WAL.OnFsync,
// Engine.OnCompaction), and this file is what wires those hooks to
// Prometheus. Swapping metrics backends later only touches this file.

package server

import (
	"log"
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

var (
	kvOpsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "raftkv_kv_ops_total",
		Help: "Total KV operations applied to the state machine, by op and result.",
	}, []string{"op", "result"})

	commitLatencySeconds = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "raftkv_commit_latency_seconds",
		Help:    "Time from ProposeWrite being called to the entry being committed and applied.",
		Buckets: prometheus.DefBuckets,
	})

	leaderElectionsTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "raftkv_leader_elections_total",
		Help: "Total number of times this node became Raft leader.",
	})

	walFsyncSeconds = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "raftkv_wal_fsync_seconds",
		Help:    "Duration of WAL fsync calls (single-entry and batch).",
		Buckets: prometheus.DefBuckets,
	})

	compactionDurationSeconds = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "raftkv_compaction_duration_seconds",
		Help:    "Duration of LSM SSTable compaction runs.",
		Buckets: prometheus.ExponentialBuckets(0.01, 2, 12), // 10ms .. ~20s
	})

	replicationLagEntries = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "raftkv_replication_lag_entries",
		Help: "Log entries a peer is behind this node's own log (leader-only; 0 on followers).",
	}, []string{"peer"})
)

// wireMetrics connects a KVServer's Raft node, WAL, and storage engine to
// the package-level Prometheus metrics via their optional instrumentation
// hooks. Called once during NewKVServer construction, before Start().
func (s *KVServer) wireMetrics() {
	s.node.OnBecomeLeader = func() {
		leaderElectionsTotal.Inc()
	}
	s.engine.OnCompaction = func(d time.Duration) {
		compactionDurationSeconds.Observe(d.Seconds())
	}
	// walState.w (*wal.WAL) is an unexported field, but metrics.go and
	// walstate.go are in the same package, so this is a direct field
	// access, not a layering violation.
	if s.walState != nil {
		s.walState.w.OnFsync = func(d time.Duration) {
			walFsyncSeconds.Observe(d.Seconds())
		}
	}
}

// recordOp increments the kv_ops_total counter for a single applied command.
func recordOp(op string, err string) {
	result := "ok"
	if err != "" {
		result = "error"
	}
	kvOpsTotal.WithLabelValues(op, result).Inc()
}

// startReplicationLagUpdater periodically samples the leader's per-peer
// match index and publishes the gap to replicationLagEntries. Stops when
// stopCh is closed. No-op work (an empty MatchIndexes map) on a follower -
// the gauges just go stale at their last-known values, which is acceptable
// for a periodic sampler like this.
func startReplicationLagUpdater(node interface {
	IsLeader() bool
	LastLogIndex() uint64
	MatchIndexes() map[string]uint64
}, stopCh <-chan struct{}) {
	ticker := time.NewTicker(time.Second)
	go func() {
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				if !node.IsLeader() {
					continue
				}
				last := node.LastLogIndex()
				for peer, match := range node.MatchIndexes() {
					lag := float64(0)
					if last > match {
						lag = float64(last - match)
					}
					replicationLagEntries.WithLabelValues(peer).Set(lag)
				}
			case <-stopCh:
				return
			}
		}
	}()
}

// ServeMetrics starts an HTTP server exposing Prometheus metrics at
// /metrics on addr. Runs until the process exits. A metrics endpoint
// failing to bind shouldn't take down the KV server, so errors are logged,
// not fatal.
func ServeMetrics(addr string) {
	if addr == "" {
		return
	}
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())
	go func() {
		if err := http.ListenAndServe(addr, mux); err != nil {
			log.Printf("metrics: server error: %v", err)
		}
	}()
	log.Printf("metrics: serving /metrics on %s", addr)
}
