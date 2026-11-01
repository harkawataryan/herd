package scheduler

import (
	"strings"
	"testing"
	"time"

	"github.com/harkawataryan/herd/internal/backend"
	"github.com/harkawataryan/herd/internal/prefix"
)

func replica(name string, inflight int, kvTokens int) *backend.Replica {
	r := backend.NewReplica(name, "http://"+name, backend.Local, []string{"m"})
	for i := 0; i < inflight; i++ {
		r.Begin(0)
	}
	if kvTokens > 0 {
		r.Begin(kvTokens) // counts toward the KV estimate until the next scrape
		r.End(time.Millisecond, true)
	}
	return r
}

func TestScoreBlendsKVAndQueue(t *testing.T) {
	th := DefaultThresholds()
	idle := Score(replica("a", 0, 0).State(), th)
	busyQueue := Score(replica("b", 8, 0).State(), th)  // half the batch
	busyKV := Score(replica("c", 0, 16384).State(), th) // half the KV cache
	if idle != 0 {
		t.Fatalf("idle score should be 0, got %v", idle)
	}
	if busyQueue < 0.19 || busyQueue > 0.21 {
		t.Fatalf("half batch should score ~0.2, got %v", busyQueue)
	}
	if busyKV < 0.29 || busyKV > 0.31 {
		t.Fatalf("half KV should score ~0.3, got %v", busyKV)
	}
}

func TestKVAwarePicksLowestLoad(t *testing.T) {
	cands := []*backend.Replica{replica("hot", 10, 20000), replica("warm", 4, 8000), replica("cold", 0, 0)}
	d, ok := KVAware{T: DefaultThresholds()}.Pick(Request{}, cands)
	if !ok || d.Replica.Name != "cold" {
		t.Fatalf("expected cold, got %+v", d)
	}
}

func TestLeastQueueIgnoresKV(t *testing.T) {
	cands := []*backend.Replica{replica("a", 2, 30000), replica("b", 3, 0)}
	d, _ := LeastQueue{}.Pick(Request{}, cands)
	if d.Replica.Name != "a" {
		t.Fatalf("least_queue should pick fewest inflight regardless of KV, got %s", d.Replica.Name)
	}
}

func TestRoundRobinCycles(t *testing.T) {
	cands := []*backend.Replica{replica("a", 0, 0), replica("b", 0, 0)}
	rr := &RoundRobin{}
	var seq []string
	for i := 0; i < 4; i++ {
		d, _ := rr.Pick(Request{}, cands)
		seq = append(seq, d.Replica.Name)
	}
	if strings.Join(seq, "") != "abab" {
		t.Fatalf("expected abab, got %v", seq)
	}
}

func TestPrefixAwarePrefersCachedReplicaUnlessOverloaded(t *testing.T) {
	th := DefaultThresholds()
	prompt := strings.Repeat("system prompt text ", 200) + "user question"
	chain := prefix.Chain(prompt)
	req := Request{Chain: chain, PromptTokens: len(prompt) / 4}

	cached := replica("cached", 2, 0)
	cached.Prefix.Add(chain)
	idle := replica("idle", 0, 0)
	d, _ := PrefixAware{T: th}.Pick(req, []*backend.Replica{idle, cached})
	if d.Replica.Name != "cached" || d.Reason != "prefix_hit" || d.PrefixBlocks != len(chain) {
		t.Fatalf("expected prefix hit on cached, got %+v", d)
	}

	// Load the cached replica past PrefixMax: affinity must yield to load.
	for i := 0; i < 14; i++ {
		cached.Begin(2000)
	}
	d, _ = PrefixAware{T: th}.Pick(req, []*backend.Replica{idle, cached})
	if d.Replica.Name != "idle" || d.Reason != "lowest_load" {
		t.Fatalf("overloaded cached replica should be skipped, got %+v", d)
	}
}

func TestFullBatchScoresAsSaturated(t *testing.T) {
	th := DefaultThresholds()
	full := replica("full", 16, 0) // every batch slot taken, KV nearly empty
	if s := Score(full.State(), th); s != 1 {
		t.Fatalf("a full batch must score 1, got %v", s)
	}
}

func TestSaturated(t *testing.T) {
	th := DefaultThresholds()
	full := replica("full", 16, 32768)
	if !Saturated([]*backend.Replica{full}, th) {
		t.Fatal("a full replica should be saturated")
	}
	if Saturated([]*backend.Replica{full, replica("free", 0, 0)}, th) {
		t.Fatal("one free replica means not saturated")
	}
	if !Saturated(nil, th) {
		t.Fatal("no candidates counts as saturated")
	}
}
