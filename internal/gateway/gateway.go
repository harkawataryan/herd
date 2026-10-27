// Package gateway is the request pipeline: parse, identify the tenant,
// decide where the request goes (local replica, cloud spillover, or
// rejection), proxy it, and record what happened.
package gateway

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/harkawataryan/herd/internal/backend"
	"github.com/harkawataryan/herd/internal/config"
	"github.com/harkawataryan/herd/internal/metrics"
	"github.com/harkawataryan/herd/internal/openai"
	"github.com/harkawataryan/herd/internal/prefix"
	"github.com/harkawataryan/herd/internal/proxy"
	"github.com/harkawataryan/herd/internal/queue"
	"github.com/harkawataryan/herd/internal/scheduler"
)

const maxBody = 4 << 20

// Criticality is the client's statement of how much a request matters,
// read from the X-Criticality header.
type Criticality string

const (
	Critical  Criticality = "critical"  // always admitted; spills to cloud if needed
	Standard  Criticality = "standard"  // default; spills if budget allows, else waits
	Sheddable Criticality = "sheddable" // rejected immediately under saturation
)

// DecisionRecord is what the admin API and the UI show per request.
type DecisionRecord struct {
	Time         time.Time          `json:"time"`
	ID           string             `json:"id"`
	Tenant       string             `json:"tenant"`
	Model        string             `json:"model"`
	Criticality  Criticality        `json:"criticality"`
	Policy       string             `json:"policy"`
	Backend      string             `json:"backend"`
	Kind         backend.Kind       `json:"kind"`
	Reason       string             `json:"reason"`
	Scores       map[string]float64 `json:"scores,omitempty"`
	PrefixBlocks int                `json:"prefix_blocks"`
	PromptTokens int                `json:"prompt_tokens"`
	Status       int                `json:"status"`
	TTFTMs       float64            `json:"ttft_ms"`
	TotalMs      float64            `json:"total_ms"`
	QueuedMs     float64            `json:"queued_ms"`
	Tokens       int                `json:"completion_tokens"`
	CostUSD      float64            `json:"cost_usd"`
}

type Gateway struct {
	cfg    *config.Config
	reg    *backend.Registry
	policy scheduler.Policy
	thr    scheduler.Thresholds

	tenants map[string]config.Tenant // api key -> tenant
	spend   map[string]float64       // tenant -> USD spent on cloud
	spendMu sync.Mutex
	queue   *queue.Queue

	ring   []DecisionRecord
	ringMu sync.Mutex
	seq    uint64
}

func New(cfg *config.Config, reg *backend.Registry, policy scheduler.Policy, thr scheduler.Thresholds) *Gateway {
	g := &Gateway{cfg: cfg, reg: reg, policy: policy, thr: thr, tenants: map[string]config.Tenant{}, spend: map[string]float64{}}
	weights := map[string]int{}
	for _, t := range cfg.Tenants {
		g.tenants[t.APIKey] = t
		weights[t.Name] = t.Weight
	}
	// A queued request may dispatch once any local replica is below saturation.
	g.queue = queue.New(weights, time.Duration(cfg.MaxQueueWaitMs)*time.Millisecond, func() bool {
		return !scheduler.Saturated(reg.Candidates("", backend.Local), thr)
	})
	return g
}

// Run starts the queue dispatcher; it returns when ctx ends.
func (g *Gateway) Run(ctx context.Context) {
	g.queue.Run(ctx, 50*time.Millisecond)
}

func (g *Gateway) Register(mux *http.ServeMux) {
	mux.HandleFunc("POST /v1/chat/completions", g.chat)
	mux.HandleFunc("GET /v1/models", g.models)
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) { w.Write([]byte(`{"ok":true}`)) })
	mux.HandleFunc("GET /admin/backends", g.adminBackends)
	mux.HandleFunc("GET /admin/decisions", g.adminDecisions)
}

func (g *Gateway) tenant(r *http.Request) config.Tenant {
	key := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
	if t, ok := g.tenants[key]; ok {
		return t
	}
	return config.Tenant{Name: "anonymous", Weight: 1}
}

func criticalityOf(r *http.Request) Criticality {
	switch Criticality(strings.ToLower(r.Header.Get("X-Criticality"))) {
	case Critical:
		return Critical
	case Sheddable:
		return Sheddable
	default:
		return Standard
	}
}

func (g *Gateway) chat(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(io.LimitReader(r.Body, maxBody))
	if err != nil {
		writeJSON(w, 400, openai.NewError("could not read body", "invalid_request_error", ""))
		return
	}
	var req openai.ChatRequest
	if err := json.Unmarshal(body, &req); err != nil {
		writeJSON(w, 400, openai.NewError("invalid JSON: "+err.Error(), "invalid_request_error", ""))
		return
	}
	if req.Model == "" {
		writeJSON(w, 400, openai.NewError("model is required", "invalid_request_error", ""))
		return
	}
	tenant := g.tenant(r)
	crit := criticalityOf(r)
	prompt := req.PromptText()
	maxTokens := req.MaxTokens
	if maxTokens <= 0 {
		maxTokens = g.cfg.DefaultMaxTokens
	}
	sreq := scheduler.Request{Model: req.Model, Chain: prefix.Chain(prompt), PromptTokens: openai.EstimateTokens(prompt), MaxTokens: maxTokens}

	rec := DecisionRecord{Time: time.Now(), ID: fmt.Sprintf("req-%d", g.next()), Tenant: tenant.Name, Model: req.Model,
		Criticality: crit, Policy: g.policy.Name(), PromptTokens: sreq.PromptTokens}

	target, reason, ok := g.route(r.Context(), sreq, tenant, crit, &rec)
	if !ok {
		metrics.Shed.WithLabelValues(string(crit), reason).Inc()
		rec.Status, rec.Reason = 503, reason
		g.record(rec)
		w.Header().Set("Retry-After", "2")
		writeJSON(w, 503, openai.NewError("all replicas saturated: "+reason, "server_error", "saturated"))
		return
	}

	opt := proxy.Options{}
	if target.Kind == backend.Cloud {
		opt.APIKey = target.APIKey
		if m, mapped := target.ModelMap[req.Model]; mapped {
			opt.Model = m
		}
	}
	target.Begin(sreq.PromptTokens + maxTokens)
	w.Header().Set("X-Herd-Backend", target.Name)
	w.Header().Set("X-Herd-Reason", rec.Reason)
	res := proxy.Do(r.Context(), w, body, req.Stream, target.URL, opt)
	target.End(res.TTFT, res.Status == 200 && res.Err == nil)
	g.queue.Kick()
	if target.Kind == backend.Local && res.Status == 200 {
		target.Prefix.Add(sreq.Chain)
	}

	rec.Status, rec.TTFTMs, rec.TotalMs, rec.Tokens = res.Status, ms(res.TTFT), ms(res.Duration), res.CompletionTokens
	if target.Kind == backend.Cloud {
		pt := res.PromptTokens
		if pt == 0 {
			pt = sreq.PromptTokens
		}
		rec.CostUSD = float64(pt)/1e6*target.PriceIn + float64(res.CompletionTokens)/1e6*target.PriceOut
		g.addSpend(tenant.Name, rec.CostUSD)
		metrics.Cost.WithLabelValues(target.Name, tenant.Name).Add(rec.CostUSD)
	}
	status := "ok"
	if res.Err != nil {
		status = "error"
	} else if res.Status != 200 {
		status = fmt.Sprint(res.Status)
	}
	metrics.Requests.WithLabelValues(target.Name, g.policy.Name(), status).Inc()
	if res.Status == 200 && res.Err == nil {
		metrics.TTFT.WithLabelValues(target.Name, g.policy.Name()).Observe(res.TTFT.Seconds())
		metrics.Duration.WithLabelValues(target.Name, g.policy.Name()).Observe(res.Duration.Seconds())
		if res.CompletionTokens > 1 {
			metrics.TPOT.WithLabelValues(target.Name).Observe((res.Duration - res.TTFT).Seconds() / float64(res.CompletionTokens-1))
		}
	}
	g.record(rec)
}

// route decides between a local replica, cloud spillover, and rejection.
func (g *Gateway) route(ctx context.Context, req scheduler.Request, tenant config.Tenant, crit Criticality, rec *DecisionRecord) (*backend.Replica, string, bool) {
	local := g.reg.Candidates(req.Model, backend.Local)
	cloud := g.reg.Candidates(req.Model, backend.Cloud)

	if len(local) > 0 {
		if !scheduler.Saturated(local, g.thr) {
			if d, ok := g.policy.Pick(req, local); ok {
				g.applyDecision(rec, d, "")
				return d.Replica, d.Reason, true
			}
		}
		// Every local replica is at capacity.
		switch crit {
		case Sheddable:
			return nil, "sheddable_shed", false
		case Critical:
			if p := g.cheapest(cloud); p != nil {
				return g.spill(rec, p, "spillover_saturated"), "spillover_saturated", true
			}
			// No cloud to spill to: send to the least-bad replica rather than fail.
			d, _ := scheduler.KVAware{T: g.thr}.Pick(req, local)
			g.applyDecision(rec, d, "critical_least_loaded")
			return d.Replica, "critical_least_loaded", true
		case Standard:
			if p := g.cheapest(cloud); p != nil && g.budgetLeft(tenant) > 0 {
				return g.spill(rec, p, "spillover_saturated"), "spillover_saturated", true
			}
			// No budget: wait fairly for local capacity, bounded by max_queue_wait.
			class := queue.Standard
			waited, released := g.queue.Wait(ctx, tenant.Name, class)
			metrics.QueueWait.WithLabelValues("standard").Observe(waited.Seconds())
			rec.QueuedMs = ms(waited)
			if !released {
				return nil, "queue_timeout", false
			}
			local = g.reg.Candidates(req.Model, backend.Local)
			if d, ok := g.policy.Pick(req, local); ok {
				g.applyDecision(rec, d, "queued_then_"+d.Reason)
				return d.Replica, rec.Reason, true
			}
			return nil, "no_backend", false
		}
	}
	// No local replica serves this model at all: cloud only.
	if p := g.cheapest(cloud); p != nil && (crit == Critical || g.budgetLeft(tenant) > 0) {
		return g.spill(rec, p, "cloud_only"), "cloud_only", true
	}
	return nil, "no_backend", false
}

func (g *Gateway) spill(rec *DecisionRecord, p *backend.Replica, reason string) *backend.Replica {
	metrics.Spillover.WithLabelValues(p.Name, reason).Inc()
	metrics.Decisions.WithLabelValues(g.policy.Name(), reason).Inc()
	rec.Backend, rec.Kind, rec.Reason = p.Name, backend.Cloud, reason
	return p
}

func (g *Gateway) applyDecision(rec *DecisionRecord, d scheduler.Decision, reasonOverride string) {
	rec.Backend, rec.Kind, rec.Reason, rec.Scores, rec.PrefixBlocks = d.Replica.Name, backend.Local, d.Reason, d.Scores, d.PrefixBlocks
	if reasonOverride != "" {
		rec.Reason = reasonOverride
	}
	metrics.Decisions.WithLabelValues(g.policy.Name(), rec.Reason).Inc()
	if d.PrefixBlocks > 0 {
		metrics.PrefixHits.WithLabelValues("hit").Add(float64(d.PrefixBlocks))
	}
}

// cheapest picks the provider with the lowest blended price; ties by name.
func (g *Gateway) cheapest(cloud []*backend.Replica) *backend.Replica {
	var best *backend.Replica
	for _, p := range cloud {
		if best == nil || p.PriceIn+p.PriceOut < best.PriceIn+best.PriceOut {
			best = p
		}
	}
	return best
}

func (g *Gateway) budgetLeft(t config.Tenant) float64 {
	g.spendMu.Lock()
	defer g.spendMu.Unlock()
	return t.BudgetUSD - g.spend[t.Name]
}

func (g *Gateway) addSpend(tenant string, usd float64) {
	g.spendMu.Lock()
	g.spend[tenant] += usd
	g.spendMu.Unlock()
}

func (g *Gateway) next() uint64 {
	g.ringMu.Lock()
	defer g.ringMu.Unlock()
	g.seq++
	return g.seq
}

func (g *Gateway) record(rec DecisionRecord) {
	g.ringMu.Lock()
	g.ring = append(g.ring, rec)
	if len(g.ring) > 500 {
		g.ring = g.ring[len(g.ring)-500:]
	}
	g.ringMu.Unlock()
}

func (g *Gateway) models(w http.ResponseWriter, _ *http.Request) {
	seen := map[string]bool{}
	type m struct {
		ID     string `json:"id"`
		Object string `json:"object"`
	}
	out := struct {
		Object string `json:"object"`
		Data   []m    `json:"data"`
	}{Object: "list", Data: []m{}}
	for _, r := range g.reg.All() {
		for id := range r.Models {
			if !seen[id] {
				seen[id] = true
				out.Data = append(out.Data, m{ID: id, Object: "model"})
			}
		}
	}
	writeJSON(w, 200, out)
}

func (g *Gateway) adminBackends(w http.ResponseWriter, _ *http.Request) {
	type row struct {
		backend.State
		Score float64 `json:"score"`
	}
	var rows []row
	for _, r := range g.reg.All() {
		st := r.State()
		rows = append(rows, row{State: st, Score: scheduler.Score(st, g.thr)})
		metrics.BackendKV.WithLabelValues(r.Name).Set(st.KVEstimate)
		metrics.BackendInflight.WithLabelValues(r.Name).Set(float64(st.Inflight))
	}
	s, c := g.queue.Depth()
	metrics.QueueDepth.WithLabelValues("standard").Set(float64(s))
	metrics.QueueDepth.WithLabelValues("critical").Set(float64(c))
	g.spendMu.Lock()
	spend := map[string]float64{}
	for k, v := range g.spend {
		spend[k] = v
	}
	g.spendMu.Unlock()
	writeJSON(w, 200, map[string]any{"policy": g.policy.Name(), "backends": rows, "spend_usd": spend, "queue": map[string]int{"standard": s, "critical": c}})
}

func (g *Gateway) adminDecisions(w http.ResponseWriter, r *http.Request) {
	g.ringMu.Lock()
	out := make([]DecisionRecord, len(g.ring))
	copy(out, g.ring)
	g.ringMu.Unlock()
	// newest first
	for i, j := 0, len(out)-1; i < j; i, j = i+1, j-1 {
		out[i], out[j] = out[j], out[i]
	}
	if len(out) > 100 {
		out = out[:100]
	}
	writeJSON(w, 200, out)
}

func ms(d time.Duration) float64 { return float64(d.Microseconds()) / 1000 }

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(v)
}
