package workload

import (
	"path/filepath"
	"testing"
	"time"
)

func TestDeterministic(t *testing.T) {
	p := DefaultParams()
	p.Duration = 20 * time.Second
	a, b := Generate(p), Generate(p)
	if len(a.Requests) == 0 || len(a.Requests) != len(b.Requests) {
		t.Fatalf("expected identical non-empty traces, got %d and %d", len(a.Requests), len(b.Requests))
	}
	for i := range a.Requests {
		x, y := a.Requests[i], b.Requests[i]
		if x.At != y.At || x.Tenant != y.Tenant || x.Class != y.Class || x.SystemIdx != y.SystemIdx || x.User != y.User || x.Criticality != y.Criticality {
			t.Fatalf("request %d differs between runs", i)
		}
	}
	p.Seed = 2
	c := Generate(p)
	same := len(c.Requests) == len(a.Requests)
	if same {
		for i := range a.Requests {
			if a.Requests[i].User != c.Requests[i].User {
				same = false
				break
			}
		}
	}
	if same {
		t.Fatal("different seeds should produce different traces")
	}
}

func TestMixAndSkew(t *testing.T) {
	p := DefaultParams()
	p.RPS = 50
	p.Duration = 120 * time.Second // ~6000 requests
	s := Generate(p).Summarize()
	n := float64(s.Requests)
	for cls, want := range p.Mix {
		got := float64(s.ByClass[cls]) / n
		if got < want-0.03 || got > want+0.03 {
			t.Fatalf("class %s: want %.2f got %.3f", cls, want, got)
		}
	}
	for crit, want := range p.Criticality {
		got := float64(s.ByCrit[crit]) / n
		if got < want-0.03 || got > want+0.03 {
			t.Fatalf("criticality %s: want %.2f got %.3f", crit, want, got)
		}
	}
	// Weighted tenants: alpha (3) should see about half the traffic.
	if got := float64(s.ByTenant["alpha"]) / n; got < 0.45 || got > 0.55 {
		t.Fatalf("tenant alpha share %.3f, want ~0.5", got)
	}
	// Zipf(1.1) over 30 prompts: the top prompt should take well over a uniform 1/30.
	if share := s.ZipfShare(); share < 0.15 {
		t.Fatalf("top prompt share %.3f, expected skew", share)
	}
	// Arrival rate should match RPS within a few percent.
	last := Generate(p).Requests
	rate := float64(len(last)) / p.Duration.Seconds()
	if rate < p.RPS*0.9 || rate > p.RPS*1.1 {
		t.Fatalf("arrival rate %.2f, want ~%.0f", rate, p.RPS)
	}
}

func TestSizesAndReplay(t *testing.T) {
	p := DefaultParams()
	p.Duration = 30 * time.Second
	tr := Generate(p)
	for _, r := range tr.Requests {
		switch r.Class {
		case Short:
			if len(r.User) != 200 {
				t.Fatalf("short user text %d chars", len(r.User))
			}
		case RAG:
			if len(r.User) < 8000 || len(r.User) > 16000 {
				t.Fatalf("rag user text %d chars", len(r.User))
			}
		case Long:
			if len(r.User) != 32000 {
				t.Fatalf("long user text %d chars", len(r.User))
			}
		}
		if r.System == "" || r.MaxTokens == 0 {
			t.Fatal("system prompt and max_tokens must be set")
		}
	}
	path := filepath.Join(t.TempDir(), "trace.json")
	if err := tr.Save(path); err != nil {
		t.Fatal(err)
	}
	back, err := Load(path)
	if err != nil {
		t.Fatal(err)
	}
	if len(back.Requests) != len(tr.Requests) || back.Requests[3].System != tr.Requests[3].System {
		t.Fatal("replayed trace should match the generated one")
	}
}
