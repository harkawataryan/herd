// Package sim is a model server simulator with the capacity dynamics that
// matter for routing: a token-budgeted KV cache, a bounded batch, prefill
// that skips prefix-cached blocks, and decode speed that degrades as the
// batch grows. It reports the same Prometheus metrics vLLM does, so the
// gateway scrapes it exactly as it would a real server.
package sim

import (
	"context"
	"math"
	"math/rand"
	"sync"
	"time"

	"github.com/harkawataryan/herd/internal/prefix"
)

type Config struct {
	Name              string
	Models            []string
	KVCapacity        int     // tokens the cache can hold across all sequences
	MaxBatch          int     // sequences decoded concurrently
	PrefillTPS        float64 // prompt tokens processed per second (uncached)
	DecodeTPS         float64 // generated tokens per second for a single sequence
	BatchPenalty      float64 // decode slows by this fraction per extra sequence
	PrefixCacheBlocks int     // blocks the server's prefix cache can retain
	TimeScale         float64 // 1.0 = real time; 0.1 runs ten times faster
	Mode              string  // "local" (capacity-bound) or "cloud" (unbounded, fixed latency, priced)
	CloudTTFTMs       float64
	CloudJitterMs     float64
	Seed              int64
}

func (c *Config) defaults() {
	if c.KVCapacity == 0 {
		c.KVCapacity = 32768
	}
	if c.MaxBatch == 0 {
		c.MaxBatch = 16
	}
	if c.PrefillTPS == 0 {
		c.PrefillTPS = 4000
	}
	if c.DecodeTPS == 0 {
		c.DecodeTPS = 40
	}
	if c.BatchPenalty == 0 {
		c.BatchPenalty = 0.15
	}
	if c.PrefixCacheBlocks == 0 {
		c.PrefixCacheBlocks = c.KVCapacity / 2 / prefix.BlockTokens
	}
	if c.TimeScale == 0 {
		c.TimeScale = 1
	}
	if c.Mode == "" {
		c.Mode = "local"
	}
	if c.CloudTTFTMs == 0 {
		c.CloudTTFTMs = 600
	}
	if c.CloudJitterMs == 0 {
		c.CloudJitterMs = 150
	}
}

type Engine struct {
	cfg   Config
	cache *prefix.Index

	mu           sync.Mutex
	cond         *sync.Cond
	activeTokens int // prompt + generated tokens of sequences currently in the batch
	active       int
	waiting      int

	// counters for /metrics
	requests, cacheQueries, cacheHits int64
	rng                               *rand.Rand
	rngMu                             sync.Mutex
}

func NewEngine(cfg Config) *Engine {
	cfg.defaults()
	e := &Engine{cfg: cfg, cache: prefix.NewIndex(cfg.PrefixCacheBlocks), rng: rand.New(rand.NewSource(cfg.Seed))}
	e.cond = sync.NewCond(&e.mu)
	// Periodic broadcast lets waiters re-check their context deadlines.
	go func() {
		for range time.Tick(50 * time.Millisecond) {
			e.cond.Broadcast()
		}
	}()
	return e
}

func (e *Engine) Config() Config { return e.cfg }

// Stats is a point-in-time view for /metrics.
type Stats struct {
	KVUtil                              float64
	Running, Waiting                    int
	Requests, CacheQueries, CacheHits   int64
	PrefixCacheBlocks, PrefixCacheInUse int
}

func (e *Engine) Stats() Stats {
	e.mu.Lock()
	defer e.mu.Unlock()
	return Stats{
		KVUtil: float64(e.activeTokens) / float64(e.cfg.KVCapacity), Running: e.active, Waiting: e.waiting,
		Requests: e.requests, CacheQueries: e.cacheQueries, CacheHits: e.cacheHits,
		PrefixCacheBlocks: e.cfg.PrefixCacheBlocks, PrefixCacheInUse: e.cache.Len(),
	}
}

// Request is what the engine needs from a chat completion call.
type Request struct {
	Prompt    string
	MaxTokens int
}

// Result reports the timing the simulator produced.
type Result struct {
	PromptTokens, CompletionTokens int
	CachedBlocks                   int
	QueueWait, TTFT, Total         time.Duration
}

func (e *Engine) sleep(ctx context.Context, d time.Duration) error {
	d = time.Duration(float64(d) * e.cfg.TimeScale)
	if d <= 0 {
		return ctx.Err()
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}

func (e *Engine) gauss(mean, sd float64) float64 {
	e.rngMu.Lock()
	defer e.rngMu.Unlock()
	return math.Max(0, mean+e.rng.NormFloat64()*sd)
}

// Generate runs one request, calling emit for every generated token. It
// blocks for the simulated time, so a caller streaming tokens sees realistic
// pacing.
func (e *Engine) Generate(ctx context.Context, req Request, emit func(token string)) (Result, error) {
	start := time.Now()
	promptTokens := (len(req.Prompt) + 3) / 4
	maxTokens := req.MaxTokens
	if maxTokens <= 0 {
		maxTokens = 128
	}
	var res Result
	res.PromptTokens = promptTokens

	if e.cfg.Mode == "cloud" {
		// Hosted APIs: effectively unbounded capacity, latency drawn from a
		// distribution, no visibility into their cache.
		if err := e.sleep(ctx, time.Duration(e.gauss(e.cfg.CloudTTFTMs, e.cfg.CloudJitterMs))*time.Millisecond); err != nil {
			return res, err
		}
		res.TTFT = time.Since(start)
		for i := 0; i < maxTokens; i++ {
			emit(word(i))
			if err := e.sleep(ctx, time.Duration(float64(time.Second)/e.cfg.DecodeTPS)); err != nil {
				return res, err
			}
		}
		res.CompletionTokens = maxTokens
		res.Total = time.Since(start)
		e.mu.Lock()
		e.requests++
		e.mu.Unlock()
		return res, nil
	}

	// Admission: wait for KV room (prompt plus the reservation for generated
	// tokens) and a batch slot. This is where a saturated replica's TTFT goes
	// to pieces, and what the gateway is trying to avoid.
	need := promptTokens + maxTokens
	e.mu.Lock()
	e.requests++
	e.waiting++
	for e.activeTokens+need > e.cfg.KVCapacity || e.active >= e.cfg.MaxBatch {
		if ctx.Err() != nil {
			e.waiting--
			e.mu.Unlock()
			return res, ctx.Err()
		}
		e.cond.Wait()
	}
	e.waiting--
	e.active++
	e.activeTokens += promptTokens
	e.mu.Unlock()
	res.QueueWait = time.Since(start)

	release := func(generated int) {
		e.mu.Lock()
		e.active--
		e.activeTokens -= promptTokens + generated
		e.mu.Unlock()
		e.cond.Broadcast()
	}

	// Prefill, skipping blocks the prefix cache already holds.
	chain := prefix.Chain(req.Prompt)
	hit := e.cache.Match(chain)
	e.mu.Lock()
	e.cacheQueries += int64(len(chain))
	e.cacheHits += int64(hit)
	e.mu.Unlock()
	uncached := promptTokens - hit*prefix.BlockTokens
	if uncached < 0 {
		uncached = 0
	}
	res.CachedBlocks = hit
	if err := e.sleep(ctx, time.Duration(float64(uncached)/e.cfg.PrefillTPS*float64(time.Second))); err != nil {
		release(0)
		return res, err
	}
	e.cache.Add(chain)

	// Decode: each token's latency depends on how many sequences share the
	// batch right now, which reproduces the throughput/latency curve of
	// continuous batching.
	generated := 0
	for i := 0; i < maxTokens; i++ {
		e.mu.Lock()
		batch := e.active
		e.mu.Unlock()
		tps := e.cfg.DecodeTPS / (1 + e.cfg.BatchPenalty*float64(batch-1))
		if err := e.sleep(ctx, time.Duration(float64(time.Second)/tps)); err != nil {
			release(generated)
			return res, err
		}
		if i == 0 {
			res.TTFT = time.Since(start)
		}
		emit(word(i))
		generated++
		e.mu.Lock()
		e.activeTokens++
		e.mu.Unlock()
	}
	release(generated)
	res.CompletionTokens = generated
	res.Total = time.Since(start)
	return res, nil
}

var words = []string{"the ", "model ", "returns ", "tokens ", "at ", "a ", "steady ", "pace ", "while ", "batched ", "with ", "others "}

func word(i int) string { return words[i%len(words)] }
