package scheduler

import (
	"strings"
	"testing"
	"time"

	"github.com/harkawataryan/herd/internal/backend"
	"github.com/harkawataryan/herd/internal/prefix"
	"github.com/harkawataryan/herd/internal/value"
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

func chainOf(seed string, blocks int) []uint64 {
	return prefix.Chain(strings.Repeat(seed+" ", (blocks*prefix.BlockChars)/(len(seed)+1)+1))[:blocks]
}

// Fill a small index so any new block evicts its oldest entries.
func smallIndex(r *backend.Replica, capBlocks int, chains ...[]uint64) {
	r.SetKVCapacity(capBlocks * prefix.BlockTokens)
	for _, c := range chains {
		r.Prefix.Add(c)
	}
}

func TestPackRoutesMissesAwayFromValuableTails(t *testing.T) {
	th := DefaultThresholds()
	tr := value.New(time.Minute, 5*time.Second)
	hot := chainOf("hot system prompt", 10)
	junkA := chainOf("old document a", 10)
	junkB := chainOf("old document b", 10)
	for i := 0; i < 40; i++ {
		tr.Observe(hot) // requested constantly
	}
	tr.Observe(junkA)
	tr.Observe(junkB)

	// Both replicas are full (cap 20). a's oldest blocks are the hot prompt,
	// b's oldest blocks are a one-off document.
	a := replica("a", 0, 0)
	smallIndex(a, 20, hot, junkA)
	b := replica("b", 0, 0)
	smallIndex(b, 20, junkB, junkA)

	newDoc := Request{Chain: chainOf("brand new document", 10), PromptTokens: 640, MaxTokens: 64}
	d, _ := NewPack(th, tr).Pick(newDoc, []*backend.Replica{a, b})
	if d.Replica.Name != "b" {
		t.Fatalf("a new document should land where it evicts junk (b), got %+v", d)
	}
	if d.Protected <= 0 {
		t.Fatalf("expected a cheaper eviction than the plain choice: %+v", d)
	}

	// A request for the hot prompt still goes to the replica holding it.
	hotReq := Request{Chain: hot, PromptTokens: 640, MaxTokens: 64}
	d, _ = NewPack(th, tr).Pick(hotReq, []*backend.Replica{a, b})
	if d.Replica.Name != "a" || d.Reason != "prefix_hit" || d.PrefixBlocks != 10 {
		t.Fatalf("hot prompt should hit on a, got %+v", d)
	}
}

func TestPackProtectsALiveSession(t *testing.T) {
	th := DefaultThresholds()
	tr := value.New(time.Minute, 5*time.Second)
	conv := chainOf("agent conversation", 10)
	junk := chainOf("stale doc", 10)
	tr.BeginTurn("s1", "a", conv) // an agent is mid tool-call on a
	tr.EndTurn("s1")
	tr.Observe(junk)

	a := replica("a", 0, 0)
	smallIndex(a, 20, conv, junk) // conv is a's oldest; junk newest
	b := replica("b", 0, 0)
	smallIndex(b, 20, chainOf("other stale doc", 10), chainOf("another stale doc", 10))

	// 15 new blocks: on a this would evict the conversation and part of junk,
	// on b only stale documents.
	d, _ := NewPack(th, tr).Pick(Request{Chain: chainOf("big new rag", 15), PromptTokens: 960, MaxTokens: 64}, []*backend.Replica{a, b})
	if d.Replica.Name != "b" {
		t.Fatalf("the live session on a should be protected, got %+v", d)
	}
}

func TestPackPrefersTheReplicaThatCanStartSooner(t *testing.T) {
	th := DefaultThresholds()
	tr := value.New(time.Minute, 5*time.Second)
	// Both hold the prompt; "full" has every batch slot taken and requests
	// there take a minute, so a new one waits ~3.75s for a slot: about 230
	// blocks of prefill at 4000 tok/s.
	chain := chainOf("shared", 10)
	full := replica("full", 16, 0)
	full.Prefix.Add(chain)
	full.Observe(time.Minute, 1000)
	idle := replica("idle", 0, 0)
	idle.Prefix.Add(chain)
	d, _ := NewPack(th, tr).Pick(Request{Chain: chain, PromptTokens: 640, MaxTokens: 64}, []*backend.Replica{full, idle})
	if d.Replica.Name != "idle" {
		t.Fatalf("expected the replica that can start now, got %+v", d)
	}
	// With nowhere else holding it, that wait still loses to prefilling 10
	// blocks on a cold replica.
	cold := replica("cold", 0, 0)
	d, _ = NewPack(th, tr).Pick(Request{Chain: chain, PromptTokens: 640, MaxTokens: 64}, []*backend.Replica{full, cold})
	if d.Replica.Name != "cold" {
		t.Fatalf("a 230-block wait should lose to a 10-block prefill, got %+v", d)
	}
	// But a short wait (requests take a second) does not.
	full.Observe(-time.Minute, 0) // ignored: only positive durations count
	quick := replica("quick", 16, 0)
	quick.Prefix.Add(chain)
	quick.Observe(time.Second, 1000)
	d, _ = NewPack(th, tr).Pick(Request{Chain: chain, PromptTokens: 640, MaxTokens: 64}, []*backend.Replica{quick, cold})
	if d.Replica.Name != "quick" {
		t.Fatalf("a 4-block wait should beat a 10-block prefill, got %+v", d)
	}
}

func TestWaitBlocks(t *testing.T) {
	st := backend.State{Inflight: 16, MaxBatch: 16, EWMADurMs: 1000, EWMATokens: 500}
	// One completion frees a slot: 1s/16 = 62.5ms = 250 tokens = ~3.9 blocks.
	if w := WaitBlocks(st, 32768, 300, 4000); w < 3.8 || w > 4.0 {
		t.Fatalf("batch wait = %v blocks, want ~3.9", w)
	}
	// KV-bound: 2000 tokens over capacity with 500 freed per completion
	// means 4 completions.
	st = backend.State{Inflight: 8, MaxBatch: 16, EWMADurMs: 1000, EWMATokens: 500, InflightTokens: 32768}
	if w := WaitBlocks(st, 32768, 2000, 4000); w < 31 || w > 32 {
		t.Fatalf("KV wait = %v blocks, want ~31.25", w)
	}
	if w := WaitBlocks(backend.State{Inflight: 2, MaxBatch: 16, EWMADurMs: 1000}, 32768, 300, 4000); w != 0 {
		t.Fatalf("free replica should not wait, got %v", w)
	}
}

func TestBaselinesPreferTheCachedReplica(t *testing.T) {
	th := DefaultThresholds()
	chain := chainOf("shared prompt", 8)
	cached := replica("cached", 3, 0)
	cached.Prefix.Add(chain)
	idle := replica("idle", 0, 0)
	req := Request{Chain: chain, PromptTokens: 512, MaxTokens: 64}
	for _, p := range []Policy{LLMDStyle{T: th}, DynamoStyle{T: th}} {
		d, _ := p.Pick(req, []*backend.Replica{idle, cached})
		if d.Replica.Name != "cached" || d.PrefixBlocks != 8 {
			t.Fatalf("%s: expected the cached replica, got %+v", p.Name(), d)
		}
	}
	// With no cache anywhere both fall back to the emptier replica.
	for _, p := range []Policy{LLMDStyle{T: th}, DynamoStyle{T: th}} {
		d, _ := p.Pick(Request{Chain: chainOf("new", 8), PromptTokens: 512}, []*backend.Replica{cached, idle})
		if d.Replica.Name != "idle" {
			t.Fatalf("%s: expected the idle replica, got %+v", p.Name(), d)
		}
	}
}
