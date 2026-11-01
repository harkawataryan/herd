// Package workload generates deterministic, replayable request traces so
// every scheduling policy is benchmarked against the identical sequence of
// prompts, tenants, criticalities and arrival times.
package workload

import (
	"encoding/json"
	"fmt"
	"math"
	"math/rand"
	"os"
	"sort"
	"strings"
	"time"

	"github.com/harkawataryan/herd/internal/openai"
)

type Class string

const (
	Short Class = "short" // chat turn, ~200 chars of user text
	RAG   Class = "rag"   // retrieved context, 8–16k chars
	Long  Class = "long"  // long document, ~32k chars
)

type Tenant struct {
	Name   string `json:"name"`
	APIKey string `json:"api_key"`
	Weight int    `json:"weight"`
}

// Params fully determine a trace given a seed.
type Params struct {
	Seed              int64              `json:"seed"`
	Model             string             `json:"model"`
	Tenants           []Tenant           `json:"tenants"`
	Mix               map[Class]float64  `json:"mix"`         // fraction per class
	Criticality       map[string]float64 `json:"criticality"` // fraction per criticality
	MaxTokens         map[Class]int      `json:"max_tokens"`  // completion length per class
	SystemPrompts     int                `json:"system_prompts"`
	SystemPromptChars int                `json:"system_prompt_chars"` // mean size; each prompt varies ±50%
	Zipf              float64            `json:"zipf"`                // popularity skew exponent (>1)
	RPS               float64            `json:"rps"`
	Duration          time.Duration      `json:"duration"`
}

func DefaultParams() Params {
	return Params{
		Seed:  1,
		Model: "llama-3-8b",
		Tenants: []Tenant{
			{Name: "alpha", APIKey: "alpha-key", Weight: 3},
			{Name: "beta", APIKey: "beta-key", Weight: 2},
			{Name: "gamma", APIKey: "gamma-key", Weight: 1},
		},
		Mix:               map[Class]float64{Short: 0.4, RAG: 0.4, Long: 0.2},
		Criticality:       map[string]float64{"standard": 0.7, "sheddable": 0.2, "critical": 0.1},
		MaxTokens:         map[Class]int{Short: 64, RAG: 128, Long: 128},
		SystemPrompts:     30,
		SystemPromptChars: 3000,
		Zipf:              1.1,
		RPS:               10,
		Duration:          60 * time.Second,
	}
}

// Request is one entry of the trace. System is resolved from the pool when
// generating; traces on disk store only the index to stay small.
type Request struct {
	Seq         int           `json:"seq"`
	At          time.Duration `json:"at"` // offset from trace start, unscaled
	Tenant      string        `json:"tenant"`
	APIKey      string        `json:"api_key"`
	Class       Class         `json:"class"`
	Criticality string        `json:"criticality"`
	Model       string        `json:"model"`
	SystemIdx   int           `json:"system_idx"`
	System      string        `json:"-"`
	User        string        `json:"user"`
	MaxTokens   int           `json:"max_tokens"`
}

// Body is the OpenAI chat request the gateway receives.
func (r Request) Body() []byte {
	b, _ := json.Marshal(map[string]any{
		"model":      r.Model,
		"stream":     true,
		"max_tokens": r.MaxTokens,
		"messages": []openai.Message{
			{Role: "system", Content: r.System},
			{Role: "user", Content: r.User},
		},
	})
	return b
}

// PromptChars is the approximate prompt size, for capacity planning.
func (r Request) PromptChars() int { return len(r.System) + len(r.User) + 16 }

// Trace is a generated workload plus the prompt pool it references.
type Trace struct {
	Params   Params    `json:"params"`
	Pool     []string  `json:"pool"`
	Requests []Request `json:"requests"`
}

// Generate builds the trace for the parameters. Same params, same trace.
func Generate(p Params) *Trace {
	rng := rand.New(rand.NewSource(p.Seed))
	pool := make([]string, p.SystemPrompts)
	for i := range pool {
		size := int(float64(p.SystemPromptChars) * (0.5 + rng.Float64()))
		pool[i] = text(rng, size, fmt.Sprintf("SYSTEM PROMPT %d. ", i))
	}
	// Zipf over prompt indices: a few system prompts dominate, like a handful
	// of apps sharing one gateway.
	zipf := rand.NewZipf(rng, p.Zipf, 1, uint64(p.SystemPrompts-1))

	classes, classW := weightedKeys(p.Mix)
	crits, critW := weightedKeysS(p.Criticality)
	tenantW := make([]float64, len(p.Tenants))
	for i, t := range p.Tenants {
		tenantW[i] = float64(max(1, t.Weight))
	}

	var reqs []Request
	var at time.Duration
	for seq := 0; ; seq++ {
		// Poisson arrivals: exponential inter-arrival times.
		at += time.Duration(rng.ExpFloat64() / p.RPS * float64(time.Second))
		if at > p.Duration {
			break
		}
		cls := classes[pick(rng, classW)]
		t := p.Tenants[pick(rng, tenantW)]
		sys := int(zipf.Uint64())
		reqs = append(reqs, Request{
			Seq: seq, At: at, Tenant: t.Name, APIKey: t.APIKey, Class: cls, Criticality: crits[pick(rng, critW)],
			Model: p.Model, SystemIdx: sys, System: pool[sys], User: userText(rng, cls, seq), MaxTokens: p.MaxTokens[cls],
		})
	}
	return &Trace{Params: p, Pool: pool, Requests: reqs}
}

func userText(rng *rand.Rand, cls Class, seq int) string {
	prefix := fmt.Sprintf("Request %d. ", seq)
	switch cls {
	case RAG:
		return text(rng, 8000+rng.Intn(8000), prefix)
	case Long:
		return text(rng, 32000, prefix)
	default:
		return text(rng, 200, prefix)
	}
}

var vocab = strings.Fields("the a of to and in that is for with as on by this from at or an be are was were it its which their has have had not but can will one two data model system user request service cache token latency throughput queue replica route cost budget prompt context document section result")

// text produces pseudo-English of roughly n chars starting with prefix, so
// prompts are unique but look like prose to a tokenizer-free estimate.
func text(rng *rand.Rand, n int, prefix string) string {
	var sb strings.Builder
	sb.WriteString(prefix)
	for sb.Len() < n {
		sb.WriteString(vocab[rng.Intn(len(vocab))])
		if rng.Intn(12) == 0 {
			sb.WriteString(". ")
		} else {
			sb.WriteByte(' ')
		}
	}
	return sb.String()[:n]
}

func weightedKeys(m map[Class]float64) ([]Class, []float64) {
	keys := make([]Class, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool { return keys[i] < keys[j] })
	w := make([]float64, len(keys))
	for i, k := range keys {
		w[i] = m[k]
	}
	return keys, w
}

func weightedKeysS(m map[string]float64) ([]string, []float64) {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	w := make([]float64, len(keys))
	for i, k := range keys {
		w[i] = m[k]
	}
	return keys, w
}

func pick(rng *rand.Rand, w []float64) int {
	total := 0.0
	for _, x := range w {
		total += x
	}
	r := rng.Float64() * total
	for i, x := range w {
		r -= x
		if r < 0 {
			return i
		}
	}
	return len(w) - 1
}

// Save writes a replayable trace; Load reads one and re-resolves prompts.
func (t *Trace) Save(path string) error {
	b, err := json.Marshal(t)
	if err != nil {
		return err
	}
	return os.WriteFile(path, b, 0o644)
}

func Load(path string) (*Trace, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var t Trace
	if err := json.Unmarshal(b, &t); err != nil {
		return nil, err
	}
	for i := range t.Requests {
		idx := t.Requests[i].SystemIdx
		if idx < 0 || idx >= len(t.Pool) {
			return nil, fmt.Errorf("request %d references missing system prompt %d", i, idx)
		}
		t.Requests[i].System = t.Pool[idx]
	}
	return &t, nil
}

// Summary describes a trace for the bench README and sanity checks.
type Summary struct {
	Requests     int            `json:"requests"`
	ByClass      map[Class]int  `json:"by_class"`
	ByCrit       map[string]int `json:"by_criticality"`
	ByTenant     map[string]int `json:"by_tenant"`
	TopPrompts   map[int]int    `json:"top_prompts"` // index -> count for the 5 most popular
	PromptTokens int            `json:"prompt_tokens_total"`
	MeanPromptTk float64        `json:"mean_prompt_tokens"`
}

func (t *Trace) Summarize() Summary {
	s := Summary{ByClass: map[Class]int{}, ByCrit: map[string]int{}, ByTenant: map[string]int{}, TopPrompts: map[int]int{}}
	counts := map[int]int{}
	for _, r := range t.Requests {
		s.Requests++
		s.ByClass[r.Class]++
		s.ByCrit[r.Criticality]++
		s.ByTenant[r.Tenant]++
		counts[r.SystemIdx]++
		s.PromptTokens += openai.EstimateTokens(r.System) + openai.EstimateTokens(r.User)
	}
	if s.Requests > 0 {
		s.MeanPromptTk = float64(s.PromptTokens) / float64(s.Requests)
	}
	type kv struct{ k, v int }
	var top []kv
	for k, v := range counts {
		top = append(top, kv{k, v})
	}
	sort.Slice(top, func(i, j int) bool { return top[i].v > top[j].v })
	for i := 0; i < len(top) && i < 5; i++ {
		s.TopPrompts[top[i].k] = top[i].v
	}
	return s
}

// ZipfShare returns the share of requests hitting the single most popular
// system prompt; useful as a quick skew check.
func (s Summary) ZipfShare() float64 {
	best := 0
	for _, v := range s.TopPrompts {
		best = max(best, v)
	}
	if s.Requests == 0 {
		return 0
	}
	return float64(best) / float64(s.Requests)
}

func round2(x float64) float64 { return math.Round(x*100) / 100 }
