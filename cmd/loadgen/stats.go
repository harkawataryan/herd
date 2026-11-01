package main

import (
	"fmt"
	"sort"
	"strings"
	"time"
)

// Sample is one completed request as measured by the load generator.
type Sample struct {
	Seq         int           `json:"seq"`
	Tenant      string        `json:"tenant"`
	Class       string        `json:"class"`
	Criticality string        `json:"criticality"`
	Status      int           `json:"status"`
	Backend     string        `json:"backend"`
	Reason      string        `json:"reason"`
	TTFT        time.Duration `json:"ttft_ns"`
	Total       time.Duration `json:"total_ns"`
	Tokens      int           `json:"completion_tokens"`
	Err         string        `json:"error,omitempty"`

	Session         string `json:"session,omitempty"`
	Turn            int    `json:"turn,omitempty"`
	SystemIdx       int    `json:"system_idx"`
	PromptBlocks    int    `json:"prompt_blocks"`           // full 64-token blocks in the prompt
	CachedBlocks    int    `json:"cached_blocks,omitempty"` // blocks the server reported as prefix-cache hits
	PredictedBlocks int    `json:"predicted_blocks"`        // blocks the gateway expected to hit
	HasCacheInfo    bool   `json:"-"`
}

// Report is the aggregate written to -out and printed as a table row.
type Report struct {
	Policy          string             `json:"policy"`
	Requests        int                `json:"requests"`
	OK              int                `json:"ok"`
	Shed            int                `json:"shed"` // 503
	Errors          int                `json:"errors"`
	WallSeconds     float64            `json:"wall_seconds"`
	SimSeconds      float64            `json:"sim_seconds"`
	TTFTMs          Percentiles        `json:"ttft_ms"`
	E2EMs           Percentiles        `json:"e2e_ms"`
	ReqPerSec       float64            `json:"req_per_sec"`    // in simulated time
	TokensPerSec    float64            `json:"tokens_per_sec"` // in simulated time
	ShedPct         float64            `json:"shed_pct"`
	SpillPct        float64            `json:"spill_pct"`
	PrefixHitPct    float64            `json:"prefix_hit_pct"`    // of locally routed OK requests, as the gateway predicted
	HitBlockPct     float64            `json:"hit_block_pct"`     // of prompt blocks, as the servers measured
	PredictedHitPct float64            `json:"predicted_hit_pct"` // of prompt blocks, as the gateway's mirror expected
	MirrorErrBlocks float64            `json:"mirror_err_blocks"` // mean |predicted - measured| per request
	GoodputPct      float64            `json:"goodput_pct"`       // OK requests with TTFT within the SLO, over all requests
	SLOTTFTMs       float64            `json:"slo_ttft_ms"`
	ByBackend       map[string]float64 `json:"backend_share"`
	ShedByCrit      map[string]float64 `json:"shed_pct_by_criticality"`
	TTFTByClassMs   map[string]float64 `json:"p99_ttft_ms_by_class"`
	P50ByClassMs    map[string]float64 `json:"p50_ttft_ms_by_class"`
}

type Percentiles struct {
	P50 float64 `json:"p50"`
	P95 float64 `json:"p95"`
	P99 float64 `json:"p99"`
	Max float64 `json:"max"`
}

// percentile uses nearest-rank on a sorted copy; p in [0,100].
func percentile(vals []float64, p float64) float64 {
	if len(vals) == 0 {
		return 0
	}
	s := append([]float64(nil), vals...)
	sort.Float64s(s)
	rank := int(float64(len(s))*p/100+0.999999) - 1
	if rank < 0 {
		rank = 0
	}
	if rank >= len(s) {
		rank = len(s) - 1
	}
	return s[rank]
}

func percentiles(vals []float64) Percentiles {
	return Percentiles{P50: percentile(vals, 50), P95: percentile(vals, 95), P99: percentile(vals, 99), Max: percentile(vals, 100)}
}

func summarize(policy string, samples []Sample, wall, sim time.Duration, timeScale float64, sloMs float64) Report {
	r := Report{Policy: policy, Requests: len(samples), WallSeconds: wall.Seconds(), SimSeconds: sim.Seconds(), SLOTTFTMs: sloMs,
		ByBackend: map[string]float64{}, ShedByCrit: map[string]float64{}, TTFTByClassMs: map[string]float64{}, P50ByClassMs: map[string]float64{}}
	var ttft, e2e []float64
	byClass := map[string][]float64{}
	critTotal, critShed := map[string]int{}, map[string]int{}
	backendCount := map[string]int{}
	tokens, local, prefix, spill, good := 0, 0, 0, 0, 0
	blocks, cached, predicted, mirrorErr, withInfo := 0, 0, 0, 0, 0
	for _, s := range samples {
		critTotal[s.Criticality]++
		switch {
		case s.Err != "":
			r.Errors++
		case s.Status == 503:
			r.Shed++
			critShed[s.Criticality]++
		case s.Status == 200:
			r.OK++
			tokens += s.Tokens
			// Latencies are measured in wall time; divide by the time scale to
			// report them in simulated seconds, comparable to the sims' own clocks.
			t := float64(s.TTFT.Milliseconds()) / timeScale
			ttft = append(ttft, t)
			e2e = append(e2e, float64(s.Total.Milliseconds())/timeScale)
			byClass[s.Class] = append(byClass[s.Class], t)
			backendCount[s.Backend]++
			if t <= sloMs {
				good++
			}
			if strings.HasPrefix(s.Reason, "spillover") || s.Reason == "cloud_only" {
				spill++
			} else {
				local++
				if s.Reason == "prefix_hit" {
					prefix++
				}
				if s.HasCacheInfo {
					blocks += s.PromptBlocks
					cached += s.CachedBlocks
					predicted += s.PredictedBlocks
					mirrorErr += max(s.PredictedBlocks-s.CachedBlocks, s.CachedBlocks-s.PredictedBlocks)
					withInfo++
				}
			}
		default:
			r.Errors++
		}
	}
	r.TTFTMs, r.E2EMs = percentiles(ttft), percentiles(e2e)
	if sim > 0 {
		r.ReqPerSec = float64(r.OK) / sim.Seconds()
		r.TokensPerSec = float64(tokens) / sim.Seconds()
	}
	if r.Requests > 0 {
		r.ShedPct = 100 * float64(r.Shed) / float64(r.Requests)
	}
	if r.OK > 0 {
		r.SpillPct = 100 * float64(spill) / float64(r.OK)
	}
	if local > 0 {
		r.PrefixHitPct = 100 * float64(prefix) / float64(local)
	}
	if blocks > 0 {
		r.HitBlockPct = 100 * float64(cached) / float64(blocks)
		r.PredictedHitPct = 100 * float64(predicted) / float64(blocks)
		r.MirrorErrBlocks = float64(mirrorErr) / float64(max(1, withInfo))
	}
	if r.Requests > 0 {
		r.GoodputPct = 100 * float64(good) / float64(r.Requests)
	}
	for b, n := range backendCount {
		r.ByBackend[b] = 100 * float64(n) / float64(max(1, r.OK))
	}
	for c, n := range critTotal {
		r.ShedByCrit[c] = 100 * float64(critShed[c]) / float64(n)
	}
	for c, v := range byClass {
		r.TTFTByClassMs[c] = percentile(v, 99)
		r.P50ByClassMs[c] = percentile(v, 50)
	}
	return r
}

func (r Report) MarkdownHeader() string {
	return "| policy | p50 TTFT ms | p95 TTFT ms | p99 TTFT ms | p99 agent TTFT ms | goodput % | req/s | tokens/s | shed % | spill % | cache hit % |\n|---|---|---|---|---|---|---|---|---|---|---|"
}

func (r Report) MarkdownRow() string {
	agent := "-"
	if v, ok := r.TTFTByClassMs["agent"]; ok {
		agent = fmt.Sprintf("%.0f", v)
	}
	return fmt.Sprintf("| %s | %.0f | %.0f | %.0f | %s | %.1f | %.2f | %.0f | %.1f | %.1f | %.1f |",
		r.Policy, r.TTFTMs.P50, r.TTFTMs.P95, r.TTFTMs.P99, agent, r.GoodputPct, r.ReqPerSec, r.TokensPerSec, r.ShedPct, r.SpillPct, r.HitBlockPct)
}
