// Package scheduler chooses a replica for a request. Policies range from
// round-robin (the baseline every benchmark compares against) through
// load- and prefix-aware routing to pack, which prices every placement in
// prefill blocks: what it must prefill, how long it would wait, and what it
// would evict. Two policies reproduce the documented scoring of other
// open-source routers so Herd can be compared against them on the same
// workload.
package scheduler

import (
	"math"
	"sort"
	"sync/atomic"

	"github.com/harkawataryan/herd/internal/backend"
	"github.com/harkawataryan/herd/internal/prefix"
	"github.com/harkawataryan/herd/internal/value"
)

// Request carries the routing-relevant view of a chat completion.
type Request struct {
	Model        string
	Chain        []uint64 // prefix block hashes of the prompt
	PromptTokens int
	MaxTokens    int
	SessionID    string // conversation this request continues, "" if unknown
}

type Decision struct {
	Replica      *backend.Replica
	Reason       string
	Scores       map[string]float64 // load score per candidate, 0 (idle) to 1 (full)
	Costs        map[string]float64 // pack: total cost per candidate, in prefill blocks
	Waits        map[string]float64 // pack: expected wait per candidate, in prefill blocks
	Inflight     map[string]int     // pack: gateway in-flight count per candidate
	PrefixBlocks int                // blocks expected to hit on the chosen replica
	EvictCost    float64            // expected future hits lost to the eviction this placement causes
	Protected    float64            // eviction cost of the best plain prefix/load choice, i.e. what was avoided

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

	// pack only. EvictWeight is how many prefill blocks one expected future
	// cache hit is worth (1: a hit lost later costs the same as a block
	// prefilled now). PrefillTPS converts an expected wait into prefill
	// blocks so waiting and prefilling trade off in one unit; set it to the
	// servers' prompt throughput in the gateway's clock. PackSlack is how
	// many blocks of extra cost the policy accepts to keep a conversation or
	// prompt family where it is.
	EvictWeight float64
	PrefillTPS  float64
	PackSlack   float64
}

func DefaultThresholds() Thresholds {
	return Thresholds{KVWeight: 0.6, QueueWeight: 0.4, Hysteresis: 0.05, PrefixMax: 0.8, Saturated: 0.9, EvictWeight: 1, PrefillTPS: 4000, PackSlack: 8}
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
		if n := r.Prefix.Peek(req.Chain); n > bestBlocks || (n == bestBlocks && n > 0 && sc[r.Name] < sc[affine.Name]) {
			affine, bestBlocks = r, n
		}
	}
	low := lowest(cands, sc)
	if affine != nil && bestBlocks > 0 {
		// Only abandon the cache hit when another replica is clearly idler.
		if sc[low.Name]+p.T.Hysteresis < sc[affine.Name] && bestBlocks*prefix.BlockTokens < req.PromptTokens/2 {
			return Decision{Replica: low, Reason: "lowest_load_over_prefix", Scores: sc}, true
		}
		return Decision{Replica: affine, Reason: "prefix_hit", Scores: sc, PrefixBlocks: bestBlocks}, true
	}
	return Decision{Replica: low, Reason: "lowest_load", Scores: sc}, true
}

// LLMDStyle reproduces the default scheduling profile documented for the
// llm-d inference scheduler (the engine behind GKE Inference Gateway):
// a prefix-cache score weighted 3 plus a queue-depth score weighted 2, highest
// total wins. The prefix score is the fraction of the prompt's blocks the
// replica is believed to hold; the queue score ranks replicas by in-flight
// requests, 1 for the emptiest and 0 for the fullest. This is a
// reimplementation of the documented behaviour, not llm-d's code.
type LLMDStyle struct{ T Thresholds }

func (LLMDStyle) Name() string { return "llmd_style" }
func (p LLMDStyle) Pick(req Request, cands []*backend.Replica) (Decision, bool) {
	if len(cands) == 0 {
		return Decision{}, false
	}
	sc := scores(cands, p.T)
	minQ, maxQ := math.MaxInt, 0
	for _, r := range cands {
		q := r.State().Inflight
		minQ, maxQ = min(minQ, q), max(maxQ, q)
	}
	var best *backend.Replica
	bestTotal, bestHit := -1.0, 0
	for _, r := range cands {
		hit := r.Prefix.Peek(req.Chain)
		prefixScore := 0.0
		if len(req.Chain) > 0 {
			prefixScore = float64(hit) / float64(len(req.Chain))
		}
		queueScore := 1.0
		if maxQ > minQ {
			queueScore = 1 - float64(r.State().Inflight-minQ)/float64(maxQ-minQ)
		}
		total := 3*prefixScore + 2*queueScore
		if total > bestTotal || (total == bestTotal && sc[r.Name] < sc[best.Name]) {
			best, bestTotal, bestHit = r, total, hit
		}
	}
	reason := "weighted_score"
	if bestHit > 0 {
		reason = "prefix_hit"
	}
	return Decision{Replica: best, Reason: reason, Scores: sc, PrefixBlocks: bestHit}, true
}

// DynamoStyle reproduces the cost function documented for NVIDIA Dynamo's
// KV router: cost = overlap_weight × blocks still to prefill + blocks of
// active decode on the worker, lowest cost wins, with the default weight
// of 1. Decode blocks are estimated from the replica's KV usage. Again a
// reimplementation of the documented formula, not Dynamo's code.
type DynamoStyle struct{ T Thresholds }

func (DynamoStyle) Name() string { return "dynamo_style" }
func (p DynamoStyle) Pick(req Request, cands []*backend.Replica) (Decision, bool) {
	if len(cands) == 0 {
		return Decision{}, false
	}
	sc := scores(cands, p.T)
	var best *backend.Replica
	bestCost, bestHit := math.Inf(1), 0
	for _, r := range cands {
		hit := r.Prefix.Peek(req.Chain)
		prefillBlocks := float64(len(req.Chain) - hit)
		decodeBlocks := r.State().KVEstimate * float64(r.KVCapacity) / prefix.BlockTokens
		cost := 1.0*prefillBlocks + decodeBlocks
		if cost < bestCost || (cost == bestCost && sc[r.Name] < sc[best.Name]) {
			best, bestCost, bestHit = r, cost, hit
		}
	}
	reason := "lowest_cost"
	if bestHit > 0 {
		reason = "prefix_hit"
	}
	return Decision{Replica: best, Reason: reason, Scores: sc, PrefixBlocks: bestHit}, true
}

// WaitBlocks estimates how long a request needing `need` tokens would wait
// before prefill can start on a replica, expressed as prefill blocks at
// prefillTPS. With a batch slot free and KV room the wait is zero. Otherwise
// slots and KV free up as running requests complete, roughly one every
// (mean request duration / running) seconds, each releasing a request's
// worth of tokens, and every request already queued ahead takes one of
// those completions first. The gateway's own in-flight count is what makes
// a replica look more expensive each time it queues something there.
func WaitBlocks(st backend.State, kvCapacity int, need, prefillTPS float64) float64 {
	if st.Inflight <= 0 || prefillTPS <= 0 {
		return 0
	}
	dur := st.EWMADurMs / 1000
	if dur <= 0 {
		return 0 // nothing observed yet; assume it can start
	}
	running := min(st.Inflight, max(1, st.MaxBatch))
	perCompletion := dur / float64(running)
	queued := max(st.Waiting, st.Inflight-st.MaxBatch)
	wait := 0.0
	if st.Inflight >= st.MaxBatch || st.Waiting > 0 {
		wait = perCompletion * float64(1+queued)
	}
	// KV: the gateway's ledger of what it has sent is the demand; whatever
	// doesn't fit waits for completions to free a request's worth each.
	free := float64(kvCapacity) - float64(st.InflightTokens)
	if need > free {
		perReq := st.EWMATokens
		if perReq <= 0 {
			perReq = need
		}
		wait = max(wait, perCompletion*math.Ceil((need-free)/perReq))
	}
	return wait * prefillTPS / prefix.BlockTokens
}

func lowest(cands []*backend.Replica, sc map[string]float64) *backend.Replica {
	sorted := make([]*backend.Replica, len(cands))
	copy(sorted, cands)
	sort.SliceStable(sorted, func(i, j int) bool { return sc[sorted[i].Name] < sc[sorted[j].Name] })
	return sorted[0]
}

// Names lists every policy New understands.
var Names = []string{"round_robin", "least_queue", "kv_aware", "prefix_aware", "llmd_style", "dynamo_style", "pack"}

// New builds a policy by name. v may be nil for policies that don't price
// evictions.
func New(name string, t Thresholds, v *value.Tracker) Policy {
	switch name {
	case "round_robin":
		return &RoundRobin{}
	case "least_queue":
		return LeastQueue{}
	case "kv_aware":
		return KVAware{T: t}
	case "llmd_style":
		return LLMDStyle{T: t}
	case "dynamo_style":
		return DynamoStyle{T: t}
	case "pack":
		if v == nil {
			v = value.New(0, 0)
		}
		return NewPack(t, v)
	default:
		return PrefixAware{T: t}
	}
}
