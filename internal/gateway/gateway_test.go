package gateway

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/harkawataryan/herd/internal/backend"
	"github.com/harkawataryan/herd/internal/config"
	"github.com/harkawataryan/herd/internal/scheduler"
	"github.com/harkawataryan/herd/internal/sim"
)

// harness runs simulated replicas and a gateway entirely in-process.
type harness struct {
	t       *testing.T
	gw      *Gateway
	srv     *httptest.Server
	engines map[string]*sim.Engine
	cancel  context.CancelFunc
}

func newHarness(t *testing.T, policy string, replicas int, simCfg sim.Config, withCloud bool, budget float64) *harness {
	t.Helper()
	reg := backend.NewRegistry()
	engines := map[string]*sim.Engine{}
	for i := 1; i <= replicas; i++ {
		cfg := simCfg
		cfg.Name = fmt.Sprintf("sim-%d", i)
		cfg.Seed = int64(i)
		e := sim.NewEngine(cfg)
		engines[cfg.Name] = e
		s := httptest.NewServer(sim.NewServer(e).Handler())
		t.Cleanup(s.Close)
		r := backend.NewReplica(cfg.Name, s.URL, backend.Local, []string{"m"})
		r.KVCapacity, r.MaxBatch = e.Config().KVCapacity, e.Config().MaxBatch
		reg.Add(r)
	}
	if withCloud {
		e := sim.NewEngine(sim.Config{Name: "cloud", Mode: "cloud", TimeScale: 0.01, DecodeTPS: 1000, Seed: 9})
		s := httptest.NewServer(sim.NewServer(e).Handler())
		t.Cleanup(s.Close)
		r := backend.NewReplica("cloud", s.URL, backend.Cloud, []string{"cloud-m"})
		r.ModelMap = map[string]string{"m": "cloud-m"}
		r.PriceIn, r.PriceOut = 1, 2
		reg.Add(r)
	}
	cfg := &config.Config{Policy: policy, MaxQueueWaitMs: 3000, DefaultMaxTokens: 8,
		Tenants: []config.Tenant{{Name: "t", APIKey: "k", Weight: 1, BudgetUSD: budget}}}
	cfg.Defaults()
	cfg.MaxQueueWaitMs = 3000
	thr := scheduler.DefaultThresholds()
	ctx, cancel := context.WithCancel(context.Background())
	go reg.Run(ctx, 20*time.Millisecond)
	gw := New(cfg, reg, scheduler.New(policy, thr), thr)
	go gw.Run(ctx)
	mux := http.NewServeMux()
	gw.Register(mux)
	srv := httptest.NewServer(mux)
	t.Cleanup(func() { cancel(); srv.Close() })
	time.Sleep(50 * time.Millisecond) // first scrape
	return &harness{t: t, gw: gw, srv: srv, engines: engines, cancel: cancel}
}

func (h *harness) chat(system, user string, crit string, maxTokens int) (*http.Response, string) {
	body := fmt.Sprintf(`{"model":"m","max_tokens":%d,"messages":[{"role":"system","content":%q},{"role":"user","content":%q}]}`, maxTokens, system, user)
	req, _ := http.NewRequest("POST", h.srv.URL+"/v1/chat/completions", strings.NewReader(body))
	req.Header.Set("Authorization", "Bearer k")
	if crit != "" {
		req.Header.Set("X-Criticality", crit)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		h.t.Fatal(err)
	}
	defer resp.Body.Close()
	var buf strings.Builder
	b := make([]byte, 4096)
	for {
		n, err := resp.Body.Read(b)
		buf.Write(b[:n])
		if err != nil {
			break
		}
	}
	return resp, buf.String()
}

func fast() sim.Config {
	return sim.Config{KVCapacity: 4096, MaxBatch: 4, PrefillTPS: 4000, DecodeTPS: 40, TimeScale: 0.01}
}

func TestPrefixAwareRoutesRepeatPromptToSameReplica(t *testing.T) {
	h := newHarness(t, "prefix_aware", 3, fast(), false, 0)
	system := strings.Repeat("You are a concert assistant. ", 60) // ~7 blocks
	first, _ := h.chat(system, "q1", "", 4)
	second, _ := h.chat(system, "q2", "", 4)
	if first.StatusCode != 200 || second.StatusCode != 200 {
		t.Fatalf("status %d %d", first.StatusCode, second.StatusCode)
	}
	if second.Header.Get("X-Herd-Reason") != "prefix_hit" || second.Header.Get("X-Herd-Backend") != first.Header.Get("X-Herd-Backend") {
		t.Fatalf("expected prefix hit on %s, got %s via %s", first.Header.Get("X-Herd-Backend"), second.Header.Get("X-Herd-Backend"), second.Header.Get("X-Herd-Reason"))
	}
	if second.Header.Get("X-Sim-Cached-Blocks") == "0" {
		t.Fatal("simulator should report cached blocks on the second request")
	}
}

func TestSaturationShedsQueuesAndSpills(t *testing.T) {
	// One tiny replica: batch of 1, so one in-flight request saturates it.
	cfg := fast()
	cfg.MaxBatch = 1
	cfg.KVCapacity = 100000
	h := newHarness(t, "kv_aware", 1, cfg, true, 1.0)

	// Occupy the replica with a long generation.
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		h.chat("sys", "long", "standard", 2000)
	}()
	time.Sleep(60 * time.Millisecond)

	// Sheddable traffic is rejected outright.
	if resp, _ := h.chat("sys", "cheap", "sheddable", 4); resp.StatusCode != 503 {
		t.Fatalf("sheddable should be shed under saturation, got %d", resp.StatusCode)
	}
	// Critical traffic spills to the cloud provider.
	if resp, _ := h.chat("sys", "urgent", "critical", 4); resp.StatusCode != 200 || resp.Header.Get("X-Herd-Backend") != "cloud" {
		t.Fatalf("critical should spill to cloud, got %d via %s", resp.StatusCode, resp.Header.Get("X-Herd-Backend"))
	}
	// Standard traffic with budget also spills.
	if resp, _ := h.chat("sys", "normal", "standard", 4); resp.Header.Get("X-Herd-Reason") != "spillover_saturated" {
		t.Fatalf("standard with budget should spill, got %s", resp.Header.Get("X-Herd-Reason"))
	}
	wg.Wait()
	// Spend was recorded for the tenant.
	resp, err := http.Get(h.srv.URL + "/admin/backends")
	if err != nil {
		t.Fatal(err)
	}
	var admin struct {
		Spend map[string]float64 `json:"spend_usd"`
	}
	json.NewDecoder(resp.Body).Decode(&admin)
	if admin.Spend["t"] <= 0 {
		t.Fatalf("expected cloud spend recorded, got %v", admin.Spend)
	}
}

func TestStandardWaitsInQueueWithoutBudget(t *testing.T) {
	cfg := fast()
	cfg.MaxBatch = 1
	cfg.KVCapacity = 100000
	h := newHarness(t, "kv_aware", 1, cfg, false, 0)

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		h.chat("sys", "long", "standard", 2000)
	}()
	time.Sleep(60 * time.Millisecond)
	resp, _ := h.chat("sys", "waits", "standard", 2)
	wg.Wait()
	if resp.StatusCode != 200 || !strings.HasPrefix(resp.Header.Get("X-Herd-Reason"), "queued_then_") {
		t.Fatalf("standard request should queue then dispatch, got %d via %s", resp.StatusCode, resp.Header.Get("X-Herd-Reason"))
	}
}
