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
}

// Report is the aggregate written to -out and printed as a table row.
type Report struct {
	Policy        string             `json:"policy"`
	Requests      int                `json:"requests"`
	OK            int                `json:"ok"`
	Shed          int                `json:"shed"` // 503
	Errors        int                `json:"errors"`
	WallSeconds   float64            `json:"wall_seconds"`
	SimSeconds    float64            `json:"sim_seconds"`
	TTFTMs        Percentiles        `json:"ttft_ms"`
	E2EMs         Percentiles        `json:"e2e_ms"`
	ReqPerSec     float64            `json:"req_per_sec"`    // in simulated time
	TokensPerSec  float64            `json:"tokens_per_sec"` // in simulated time
	ShedPct       float64            `json:"shed_pct"`
	SpillPct      float64            `json:"spill_pct"`
	PrefixHitPct  float64            `json:"prefix_hit_pct"` // of locally routed OK requests
	ByBackend     map[string]float64 `json:"backend_share"`
	ShedByCrit    map[string]float64 `json:"shed_pct_by_criticality"`
	TTFTByClassMs map[string]float64 `json:"p99_ttft_ms_by_class"`
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

func summarize(policy string, samples []Sample, wall, sim time.Duration, timeScale float64) Report {
	r := Report{Policy: policy, Requests: len(samples), WallSeconds: wall.Seconds(), SimSeconds: sim.Seconds(),
		ByBackend: map[string]float64{}, ShedByCrit: map[string]float64{}, TTFTByClassMs: map[string]float64{}}
	var ttft, e2e []float64
	byClass := map[string][]float64{}
	critTotal, critShed := map[string]int{}, map[string]int{}
	backendCount := map[string]int{}
	tokens, local, prefix, spill := 0, 0, 0, 0
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
			ttft = append(ttft, float64(s.TTFT.Milliseconds())/timeScale)
			e2e = append(e2e, float64(s.Total.Milliseconds())/timeScale)
			byClass[s.Class] = append(byClass[s.Class], float64(s.TTFT.Milliseconds())/timeScale)
			backendCount[s.Backend]++
			if strings.HasPrefix(s.Reason, "spillover") || s.Reason == "cloud_only" {
				spill++
			} else {
				local++
				if s.Reason == "prefix_hit" {
					prefix++
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
	for b, n := range backendCount {
		r.ByBackend[b] = 100 * float64(n) / float64(max(1, r.OK))
	}
	for c, n := range critTotal {
		r.ShedByCrit[c] = 100 * float64(critShed[c]) / float64(n)
	}
	for c, v := range byClass {
		r.TTFTByClassMs[c] = percentile(v, 99)
	}
	return r
}

func (r Report) MarkdownHeader() string {
	return "| policy | p50 TTFT ms | p95 TTFT ms | p99 TTFT ms | p99 e2e ms | req/s | tokens/s | shed % | spill % | prefix hit % |\n|---|---|---|---|---|---|---|---|---|---|"
}

func (r Report) MarkdownRow() string {
	return fmt.Sprintf("| %s | %.0f | %.0f | %.0f | %.0f | %.2f | %.0f | %.1f | %.1f | %.1f |",
		r.Policy, r.TTFTMs.P50, r.TTFTMs.P95, r.TTFTMs.P99, r.E2EMs.P99, r.ReqPerSec, r.TokensPerSec, r.ShedPct, r.SpillPct, r.PrefixHitPct)
}
