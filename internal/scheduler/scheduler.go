// Package scheduler chooses a replica for a request. Policies range from
// round-robin (the baseline every benchmark compares against) to the
// prefix-aware policy that combines KV-cache pressure with prefix-cache
// affinity, which is the one the gateway runs by default.
package scheduler

import (
	"math"
	"sort"
	"sync/atomic"

	"github.com/harkawataryan/herd/internal/backend"
)

// Request carries the routing-relevant view of a chat completion.
type Request struct {
	Model        string
	Chain        []uint64 // prefix block hashes of the prompt
	PromptTokens int
	MaxTokens    int
}

type Decision struct {
	Replica      *backend.Replica
	Reason       string
	Scores       map[string]float64 // load score per candidate, 0 (idle) to 1 (full)
	PrefixBlocks int                // blocks expected to hit on the chosen replica
}

type Policy interface {
	Name() string
	Pick(req Request, cands []*backend.Replica) (Decision, bool)
}

// Thresholds tune the load-aware policies.
type Thresholds struct {
	KVWeight    float64 // weight of KV estimate in the load score
	QueueWeight float64 // weight of inflight/max_batch
	Hysteresis  float64 // a replica must beat the prefix-affine choice by this much to override it
	PrefixMax   float64 // prefix affinity is only honored below this load score
	Saturated   float64 // min score at or above this means every replica is saturated
}

func DefaultThresholds() Thresholds {
	return Thresholds{KVWeight: 0.6, QueueWeight: 0.4, Hysteresis: 0.05, PrefixMax: 0.8, Saturated: 0.9}
}

// Score is the load estimate for one replica: a blend of KV-cache pressure
// (scraped plus what we've sent since) and batch occupancy from our own
// in-flight count.
func Score(st backend.State, t Thresholds) float64 {
	// No free batch slot, or requests already queued on the server, means
	// the replica cannot take more work regardless of KV headroom.
	if st.Inflight >= st.MaxBatch || st.Waiting > 0 {
		return 1
	}
	q := float64(st.Inflight) / float64(max(1, st.MaxBatch))
	return t.KVWeight*st.KVEstimate + t.QueueWeight*q
}

// Saturated reports whether every candidate is at or above the threshold.
func Saturated(cands []*backend.Replica, t Thresholds) bool {
	if len(cands) == 0 {
		return true
	}
	lo := math.Inf(1)
	for _, r := range cands {
		if s := Score(r.State(), t); s < lo {
			lo = s
		}
	}
	return lo >= t.Saturated
}

func scores(cands []*backend.Replica, t Thresholds) map[string]float64 {
	m := make(map[string]float64, len(cands))
	for _, r := range cands {
		m[r.Name] = Score(r.State(), t)
	}
	return m
}

// RoundRobin ignores load entirely.
type RoundRobin struct{ n atomic.Uint64 }

func (*RoundRobin) Name() string { return "round_robin" }
func (p *RoundRobin) Pick(_ Request, cands []*backend.Replica) (Decision, bool) {
	if len(cands) == 0 {
		return Decision{}, false
	}
	i := int(p.n.Add(1)-1) % len(cands)
	return Decision{Replica: cands[i], Reason: "round_robin"}, true
}

// LeastQueue picks the replica with the fewest in-flight requests from the
// gateway's own view: the classic least-connections balancer.
type LeastQueue struct{}

func (LeastQueue) Name() string { return "least_queue" }
func (LeastQueue) Pick(_ Request, cands []*backend.Replica) (Decision, bool) {
	if len(cands) == 0 {
		return Decision{}, false
	}
	best, bestIn := cands[0], cands[0].State().Inflight
	for _, r := range cands[1:] {
		if in := r.State().Inflight; in < bestIn {
			best, bestIn = r, in
		}
	}
	return Decision{Replica: best, Reason: "least_inflight"}, true
}

// KVAware picks the lowest load score, i.e. the replica with the most KV
// headroom and batch capacity.
type KVAware struct{ T Thresholds }

func (KVAware) Name() string { return "kv_aware" }
func (p KVAware) Pick(_ Request, cands []*backend.Replica) (Decision, bool) {
	if len(cands) == 0 {
		return Decision{}, false
	}
	sc := scores(cands, p.T)
	best := lowest(cands, sc)
	return Decision{Replica: best, Reason: "lowest_load", Scores: sc}, true
}

// PrefixAware prefers a replica that already holds the prompt's prefix
// blocks, as long as it isn't too loaded; otherwise it behaves like KVAware.
// Skipping prefill for cached blocks is the single biggest TTFT win for
// workloads with shared system prompts.
type PrefixAware struct{ T Thresholds }

func (PrefixAware) Name() string { return "prefix_aware" }
func (p PrefixAware) Pick(req Request, cands []*backend.Replica) (Decision, bool) {
	if len(cands) == 0 {
		return Decision{}, false
	}
	sc := scores(cands, p.T)
	var affine *backend.Replica
	bestBlocks := 0
	for _, r := range cands {
		if sc[r.Name] > p.T.PrefixMax {
			continue
		}
		if n := r.Prefix.Match(req.Chain); n > bestBlocks || (n == bestBlocks && n > 0 && sc[r.Name] < sc[affine.Name]) {
			affine, bestBlocks = r, n
		}
	}
	low := lowest(cands, sc)
	if affine != nil && bestBlocks > 0 {
		// Only abandon the cache hit when another replica is clearly idler.
		if sc[low.Name]+p.T.Hysteresis < sc[affine.Name] && bestBlocks*64 < req.PromptTokens/2 {
			return Decision{Replica: low, Reason: "lowest_load_over_prefix", Scores: sc}, true
		}
		return Decision{Replica: affine, Reason: "prefix_hit", Scores: sc, PrefixBlocks: bestBlocks}, true
	}
	return Decision{Replica: low, Reason: "lowest_load", Scores: sc}, true
}

func lowest(cands []*backend.Replica, sc map[string]float64) *backend.Replica {
	sorted := make([]*backend.Replica, len(cands))
	copy(sorted, cands)
	sort.SliceStable(sorted, func(i, j int) bool { return sc[sorted[i].Name] < sc[sorted[j].Name] })
	return sorted[0]
}

// New builds a policy by name.
func New(name string, t Thresholds) Policy {
	switch name {
	case "round_robin":
		return &RoundRobin{}
	case "least_queue":
		return LeastQueue{}
	case "kv_aware":
		return KVAware{T: t}
	default:
		return PrefixAware{T: t}
	}
}
