// Package backend tracks the live state of every replica and provider the
// gateway can route to: the gateway's own in-flight view, the replica's
// scraped KV-cache and queue metrics, health, and the prefix index.
package backend

import (
	"bufio"
	"context"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/harkawataryan/herd/internal/prefix"
)

// Kind distinguishes capacity-bound replicas we can observe from hosted
// providers we pay per token and cannot observe.
type Kind string

const (
	Local Kind = "local"
	Cloud Kind = "cloud"
)

type Replica struct {
	Name       string
	URL        string
	Kind       Kind
	Models     map[string]bool
	ModelMap   map[string]string // cloud: local model id -> provider model id
	KVCapacity int
	MaxBatch   int
	PriceIn    float64 // USD per million prompt tokens (cloud)
	PriceOut   float64 // USD per million completion tokens (cloud)
	APIKey     string  // cloud: forwarded as the bearer token
	Prefix     *prefix.Index

	mu                sync.Mutex
	inflight          int
	tokensSinceScrape int
	kv                float64
	running, waiting  int
	lastScrape        time.Time
	healthy           bool
	scrapeErr         string
	ewmaTTFT          float64 // seconds
	completed         int64
}

func NewReplica(name, url string, kind Kind, models []string) *Replica {
	r := &Replica{Name: name, URL: strings.TrimRight(url, "/"), Kind: kind, Models: map[string]bool{}, KVCapacity: 32768, MaxBatch: 16, healthy: true}
	for _, m := range models {
		r.Models[m] = true
	}
	r.Prefix = prefix.NewIndex(r.KVCapacity / prefix.BlockTokens)
	return r
}

// SetKVCapacity resizes the capacity and the prefix index to match the server.
func (r *Replica) SetKVCapacity(tokens int) {
	r.KVCapacity = tokens
	r.Prefix = prefix.NewIndex(tokens / prefix.BlockTokens)
}

// State is a consistent snapshot for the scheduler and the admin API.
type State struct {
	Name       string  `json:"name"`
	Kind       Kind    `json:"kind"`
	Healthy    bool    `json:"healthy"`
	Inflight   int     `json:"inflight"`
	KV         float64 `json:"kv_scraped"`
	KVEstimate float64 `json:"kv_estimate"`
	Running    int     `json:"running"`
	Waiting    int     `json:"waiting"`
	MaxBatch   int     `json:"max_batch"`
	EWMATTFTMs float64 `json:"ewma_ttft_ms"`
	ScrapeAge  float64 `json:"scrape_age_s"`
	ScrapeErr  string  `json:"scrape_error,omitempty"`
	Completed  int64   `json:"completed"`
	PrefixLen  int     `json:"prefix_blocks_indexed"`
}

func (r *Replica) State() State {
	r.mu.Lock()
	defer r.mu.Unlock()
	st := State{
		Name: r.Name, Kind: r.Kind, Healthy: r.healthy, Inflight: r.inflight, KV: r.kv,
		Running: r.running, Waiting: r.waiting, MaxBatch: r.MaxBatch, EWMATTFTMs: r.ewmaTTFT * 1000,
		ScrapeErr: r.scrapeErr, Completed: r.completed, PrefixLen: r.Prefix.Len(),
	}
	if !r.lastScrape.IsZero() {
		st.ScrapeAge = time.Since(r.lastScrape).Seconds()
	}
	// Scrapes are periodic; tokens dispatched since the last one haven't shown
	// up in the replica's own gauge yet, so add them to avoid stale decisions.
	st.KVEstimate = r.kv + float64(r.tokensSinceScrape)/float64(r.KVCapacity)
	if st.KVEstimate > 1 {
		st.KVEstimate = 1
	}
	return st
}

// Begin records a dispatch; tokens is the prompt plus reserved output.
func (r *Replica) Begin(tokens int) {
	r.mu.Lock()
	r.inflight++
	r.tokensSinceScrape += tokens
	r.mu.Unlock()
}

// End records completion and folds the observed TTFT into the EWMA.
func (r *Replica) End(ttft time.Duration, ok bool) {
	r.mu.Lock()
	r.inflight--
	if ok {
		r.completed++
		if ttft > 0 {
			if r.ewmaTTFT == 0 {
				r.ewmaTTFT = ttft.Seconds()
			} else {
				r.ewmaTTFT = 0.8*r.ewmaTTFT + 0.2*ttft.Seconds()
			}
		}
	}
	r.mu.Unlock()
}

// Serves reports whether the replica can take the model; an empty model
// matches any replica (used for capacity checks that aren't model-specific).
func (r *Replica) Serves(model string) bool {
	if model == "" || len(r.Models) == 0 {
		return true
	}
	if r.Models[model] {
		return true
	}
	_, mapped := r.ModelMap[model]
	return mapped
}

// Registry owns every replica and runs the scrape loop.
type Registry struct {
	mu       sync.RWMutex
	replicas []*Replica
	client   *http.Client
}

func NewRegistry(rs ...*Replica) *Registry {
	return &Registry{replicas: rs, client: &http.Client{Timeout: 2 * time.Second}}
}

func (g *Registry) All() []*Replica {
	g.mu.RLock()
	defer g.mu.RUnlock()
	out := make([]*Replica, len(g.replicas))
	copy(out, g.replicas)
	return out
}

func (g *Registry) Add(r *Replica) {
	g.mu.Lock()
	g.replicas = append(g.replicas, r)
	g.mu.Unlock()
}

// Candidates returns healthy replicas of the given kind that serve the model.
func (g *Registry) Candidates(model string, kind Kind) []*Replica {
	var out []*Replica
	for _, r := range g.All() {
		if r.Kind != kind || !r.Serves(model) {
			continue
		}
		r.mu.Lock()
		ok := r.healthy
		r.mu.Unlock()
		if ok {
			out = append(out, r)
		}
	}
	return out
}

// Run scrapes every local replica's /metrics on the interval until ctx ends.
func (g *Registry) Run(ctx context.Context, interval time.Duration) {
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		var wg sync.WaitGroup
		for _, r := range g.All() {
			if r.Kind != Local {
				continue
			}
			wg.Add(1)
			go func(r *Replica) {
				defer wg.Done()
				g.scrape(ctx, r)
			}(r)
		}
		wg.Wait()
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
	}
}

func (g *Registry) scrape(ctx context.Context, r *Replica) {
	req, _ := http.NewRequestWithContext(ctx, "GET", r.URL+"/metrics", nil)
	resp, err := g.client.Do(req)
	r.mu.Lock()
	defer r.mu.Unlock()
	if err != nil {
		r.healthy, r.scrapeErr = false, err.Error()
		return
	}
	defer resp.Body.Close()
	m := ParseMetrics(bufio.NewScanner(resp.Body))
	r.healthy, r.scrapeErr = true, ""
	r.lastScrape = time.Now()
	r.tokensSinceScrape = 0
	if v, ok := m["vllm:gpu_cache_usage_perc"]; ok {
		r.kv = v
	}
	if v, ok := m["vllm:num_requests_running"]; ok {
		r.running = int(v)
	}
	if v, ok := m["vllm:num_requests_waiting"]; ok {
		r.waiting = int(v)
	}
}

// ParseMetrics reads Prometheus text format into name -> value, ignoring
// labels (one series per name is all the scheduler needs). Also understands
// llama.cpp's names so a real server works unchanged.
func ParseMetrics(sc *bufio.Scanner) map[string]float64 {
	out := map[string]float64{}
	for sc.Scan() {
		line := sc.Text()
		if line == "" || line[0] == '#' {
			continue
		}
		name := line
		if i := strings.IndexAny(line, "{ "); i > 0 {
			name = line[:i]
		}
		sp := strings.LastIndexByte(line, ' ')
		if sp < 0 {
			continue
		}
		v, err := strconv.ParseFloat(strings.TrimSpace(line[sp+1:]), 64)
		if err != nil {
			continue
		}
		out[name] = v
	}
	// llama.cpp server aliases
	if v, ok := out["llamacpp:kv_cache_usage_ratio"]; ok {
		out["vllm:gpu_cache_usage_perc"] = v
	}
	if v, ok := out["llamacpp:requests_processing"]; ok {
		out["vllm:num_requests_running"] = v
	}
	if v, ok := out["llamacpp:requests_deferred"]; ok {
		out["vllm:num_requests_waiting"] = v
	}
	return out
}
