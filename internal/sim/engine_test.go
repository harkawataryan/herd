package sim

import (
	"context"
	"strings"
	"sync"
	"testing"
	"time"
)

func fastEngine(kv, batch int) *Engine {
	return NewEngine(Config{Name: "t", KVCapacity: kv, MaxBatch: batch, PrefillTPS: 4000, DecodeTPS: 40, TimeScale: 0.01, Seed: 1})
}

func TestPrefixCacheSkipsPrefill(t *testing.T) {
	e := fastEngine(32768, 16)
	prompt := strings.Repeat("shared system prompt ", 400) // ~8k chars, 32 blocks
	cold, err := e.Generate(context.Background(), Request{Prompt: prompt, MaxTokens: 1}, func(string) {})
	if err != nil {
		t.Fatal(err)
	}
	warm, _ := e.Generate(context.Background(), Request{Prompt: prompt, MaxTokens: 1}, func(string) {})
	if cold.CachedBlocks != 0 || warm.CachedBlocks == 0 {
		t.Fatalf("expected cache miss then hit, got %d and %d", cold.CachedBlocks, warm.CachedBlocks)
	}
	if warm.TTFT >= cold.TTFT {
		t.Fatalf("cached prefill should be faster: cold %v warm %v", cold.TTFT, warm.TTFT)
	}
}

func TestAdmissionQueuesWhenBatchFull(t *testing.T) {
	e := fastEngine(32768, 1)
	var wg sync.WaitGroup
	results := make([]Result, 2)
	for i := range results {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			results[i], _ = e.Generate(context.Background(), Request{Prompt: "hello", MaxTokens: 20}, func(string) {})
		}(i)
	}
	wg.Wait()
	// With a batch of one, the second request waits for the first to finish.
	if results[0].QueueWait == 0 && results[1].QueueWait == 0 {
		t.Fatal("one of the two requests should have queued")
	}
	if st := e.Stats(); st.Running != 0 || st.Waiting != 0 || st.KVUtil != 0 {
		t.Fatalf("engine should be idle after completion: %+v", st)
	}
}

func TestKVCapacityIsRespected(t *testing.T) {
	e := fastEngine(300, 16) // room for one request of 200 prompt + 50 output
	prompt := strings.Repeat("x", 800)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	var wg sync.WaitGroup
	peak := 0
	var mu sync.Mutex
	for i := 0; i < 3; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			e.Generate(ctx, Request{Prompt: prompt, MaxTokens: 50}, func(string) {
				st := e.Stats()
				mu.Lock()
				if st.Running > peak {
					peak = st.Running
				}
				mu.Unlock()
			})
		}()
	}
	wg.Wait()
	if peak != 1 {
		t.Fatalf("KV capacity should allow only one sequence at a time, peak was %d", peak)
	}
}

func TestCloudModeHasNoCapacityLimit(t *testing.T) {
	e := NewEngine(Config{Name: "c", Mode: "cloud", TimeScale: 0.01, DecodeTPS: 1000, Seed: 1})
	res, err := e.Generate(context.Background(), Request{Prompt: "hi", MaxTokens: 3}, func(string) {})
	if err != nil || res.CompletionTokens != 3 || res.TTFT == 0 {
		t.Fatalf("cloud generate: %+v %v", res, err)
	}
}
