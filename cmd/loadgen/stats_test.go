package main

import (
	"testing"
	"time"
)

func TestPercentiles(t *testing.T) {
	vals := []float64{10, 20, 30, 40, 50, 60, 70, 80, 90, 100}
	cases := []struct {
		p, want float64
	}{{50, 50}, {95, 100}, {99, 100}, {100, 100}, {10, 10}, {0, 10}}
	for _, c := range cases {
		if got := percentile(vals, c.p); got != c.want {
			t.Errorf("p%.0f = %v, want %v", c.p, got, c.want)
		}
	}
	if percentile(nil, 50) != 0 {
		t.Error("empty input should give 0")
	}
}

func TestSummarize(t *testing.T) {
	samples := []Sample{
		{Status: 200, Backend: "a", Reason: "prefix_hit", TTFT: 100 * time.Millisecond, Total: time.Second, Tokens: 10, Criticality: "standard", Class: "short"},
		{Status: 200, Backend: "b", Reason: "lowest_load", TTFT: 300 * time.Millisecond, Total: 2 * time.Second, Tokens: 10, Criticality: "standard", Class: "short"},
		{Status: 200, Backend: "cloud", Reason: "spillover_saturated", TTFT: 600 * time.Millisecond, Total: 3 * time.Second, Tokens: 10, Criticality: "critical", Class: "rag"},
		{Status: 503, Criticality: "sheddable", Class: "rag"},
	}
	r := summarize("test", samples, 10*time.Second, 100*time.Second, 0.1)
	if r.OK != 3 || r.Shed != 1 || r.Requests != 4 {
		t.Fatalf("counts wrong: %+v", r)
	}
	// Wall 100ms at scale 0.1 is 1000ms of simulated time.
	if r.TTFTMs.P50 != 3000 || r.TTFTMs.P99 != 6000 {
		t.Fatalf("ttft percentiles wrong: %+v", r.TTFTMs)
	}
	if r.ShedPct != 25 || r.ShedByCrit["sheddable"] != 100 || r.ShedByCrit["standard"] != 0 {
		t.Fatalf("shed stats wrong: %+v", r)
	}
	if r.SpillPct < 33.3 || r.SpillPct > 33.4 || r.PrefixHitPct != 50 {
		t.Fatalf("spill/prefix wrong: spill=%.2f prefix=%.2f", r.SpillPct, r.PrefixHitPct)
	}
	if r.TokensPerSec != 0.3 || r.ReqPerSec != 0.03 {
		t.Fatalf("throughput wrong: %+v", r)
	}
}
