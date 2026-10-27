// Package metrics defines the Prometheus metrics exported on /metrics.
package metrics

import "github.com/prometheus/client_golang/prometheus"

var (
	ChainHeight = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "indexer_chain_block",
		Help: "Latest block height reported by the node, by finality level.",
	}, []string{"level"})

	IndexedBlock = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "indexer_indexed_block",
		Help: "Highest block the indexer has stored.",
	})

	TransfersIndexed = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "indexer_transfers_indexed_total",
		Help: "Transfers written to the database.",
	})

	Reorgs = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "indexer_reorgs_total",
		Help: "Chain reorganisations detected.",
	})

	ReorgDepth = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    "indexer_reorg_depth_blocks",
		Help:    "Number of blocks rolled back per reorg.",
		Buckets: []float64{1, 2, 3, 5, 10, 20, 50, 100},
	})

	OrphanedTransfers = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "indexer_orphaned_transfers_total",
		Help: "Transfers removed because their block was reorged out.",
	})

	RPCErrors = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "indexer_rpc_errors_total",
		Help: "Failed JSON-RPC calls, by method.",
	}, []string{"method"})

	RPCDuration = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "indexer_rpc_duration_seconds",
		Help:    "JSON-RPC call latency, by method.",
		Buckets: prometheus.DefBuckets,
	}, []string{"method"})

	ReconcileMismatch = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "indexer_reconcile_mismatch",
		Help: "1 when the indexed balance of a watched address differs from balanceOf on chain.",
	}, []string{"address"})

	OutboxPublished = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "indexer_outbox_published_total",
		Help: "Events published from the outbox.",
	})
)

// Register adds every metric to the given registry.
func Register(r prometheus.Registerer) {
	r.MustRegister(ChainHeight, IndexedBlock, TransfersIndexed, Reorgs, ReorgDepth, OrphanedTransfers,
		RPCErrors, RPCDuration, ReconcileMismatch, OutboxPublished)
}
