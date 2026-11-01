// Package metrics defines the gateway's Prometheus series. Time to first
// token is the headline: it is what a user feels, and what KV pressure on a
// replica destroys first.
package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	ttftBuckets  = []float64{.05, .1, .2, .3, .5, .75, 1, 1.5, 2, 3, 5, 8, 13, 21}
	totalBuckets = []float64{.25, .5, 1, 2, 3, 5, 8, 13, 21, 34, 55}

	Requests = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "herd_requests_total", Help: "Chat completions by backend and outcome",
	}, []string{"backend", "policy", "status"})
	TTFT = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name: "herd_ttft_seconds", Help: "Time to first token", Buckets: ttftBuckets,
	}, []string{"backend", "policy"})
	Duration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name: "herd_request_duration_seconds", Help: "End-to-end request time", Buckets: totalBuckets,
	}, []string{"backend", "policy"})
	TPOT = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name: "herd_tpot_seconds", Help: "Mean time per output token after the first", Buckets: []float64{.005, .01, .02, .03, .05, .075, .1, .15, .2, .3, .5},
	}, []string{"backend"})
	Decisions = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "herd_decisions_total", Help: "Scheduler decisions by reason",
	}, []string{"policy", "reason"})
	Shed = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "herd_shed_total", Help: "Requests rejected under saturation",
	}, []string{"criticality", "reason"})
	Spillover = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "herd_spillover_total", Help: "Requests sent to a cloud provider",
	}, []string{"provider", "reason"})
	Cost = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "herd_cost_usd_total", Help: "Estimated spend on cloud providers",
	}, []string{"provider", "tenant"})
	QueueDepth = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "herd_queue_depth", Help: "Requests waiting for admission",
	}, []string{"class"})
	QueueWait = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name: "herd_queue_wait_seconds", Help: "Time spent waiting for admission", Buckets: ttftBuckets,
	}, []string{"class"})
	BackendKV = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "herd_backend_kv_utilization", Help: "KV cache utilization estimate per replica",
	}, []string{"backend"})
	BackendInflight = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "herd_backend_inflight", Help: "In-flight requests per replica from the gateway's view",
	}, []string{"backend"})
	PrefixHits = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "herd_prefix_blocks_total", Help: "Prompt blocks routed to a replica expected to have them cached vs not",
	}, []string{"outcome"})
)
