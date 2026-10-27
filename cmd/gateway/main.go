// Command gateway is Herd: one OpenAI-compatible endpoint in front of your
// own model servers, routed by live KV-cache and prefix-cache signals, with
// budgeted spillover to hosted providers when local capacity runs out.
package main

import (
	"context"
	"flag"
	"log"
	"net/http"
	"os"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/harkawataryan/herd/internal/backend"
	"github.com/harkawataryan/herd/internal/config"
	"github.com/harkawataryan/herd/internal/gateway"
	"github.com/harkawataryan/herd/internal/scheduler"
)

func main() {
	cfgPath := flag.String("config", "herd.yaml", "config file")
	policyFlag := flag.String("policy", "", "override policy: round_robin | least_queue | kv_aware | prefix_aware")
	listen := flag.String("listen", "", "override listen address")
	flag.Parse()

	cfg, err := config.Load(*cfgPath)
	if err != nil {
		log.Fatalf("config: %v", err)
	}
	if *policyFlag != "" {
		cfg.Policy = *policyFlag
	}
	if *listen != "" {
		cfg.Listen = *listen
	}

	reg := backend.NewRegistry()
	for _, b := range cfg.Backends {
		r := backend.NewReplica(b.Name, b.URL, backend.Local, b.Models)
		if b.KVCapacity > 0 {
			r.KVCapacity = b.KVCapacity
		}
		if b.MaxBatch > 0 {
			r.MaxBatch = b.MaxBatch
		}
		reg.Add(r)
	}
	for _, p := range cfg.Providers {
		r := backend.NewReplica(p.Name, p.URL, backend.Cloud, p.Models)
		r.ModelMap = p.ModelMap
		r.PriceIn, r.PriceOut = p.PriceInPerM, p.PriceOutPerM
		if p.APIKeyEnv != "" {
			r.APIKey = os.Getenv(p.APIKeyEnv)
		}
		reg.Add(r)
	}

	t := scheduler.DefaultThresholds()
	t.Saturated = cfg.SaturationThreshold
	policy := scheduler.New(cfg.Policy, t)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go reg.Run(ctx, time.Duration(cfg.ScrapeIntervalMs)*time.Millisecond)

	gw := gateway.New(cfg, reg, policy, t)
	go gw.Run(ctx)
	mux := http.NewServeMux()
	gw.Register(mux)
	mux.Handle("GET /metrics", promhttp.Handler())
	if cfg.UIDir != "" {
		mux.Handle("GET /", http.FileServer(http.Dir(cfg.UIDir)))
	}

	log.Printf("herd listening on %s policy=%s backends=%d providers=%d", cfg.Listen, policy.Name(), len(cfg.Backends), len(cfg.Providers))
	srv := &http.Server{Addr: cfg.Listen, Handler: mux, ReadHeaderTimeout: 10 * time.Second}
	log.Fatal(srv.ListenAndServe())
}
