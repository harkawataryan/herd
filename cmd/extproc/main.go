// Command extproc runs Herd's scheduler as an Envoy external processor so an
// existing Envoy (or any Gateway API implementation built on it) can use
// Herd's routing without proxying through the Herd gateway.
package main

import (
	"context"
	"flag"
	"log"
	"net"
	"net/http"
	"os"
	"time"

	extprocv3 "github.com/envoyproxy/go-control-plane/envoy/service/ext_proc/v3"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"

	"github.com/harkawataryan/herd/internal/backend"
	"github.com/harkawataryan/herd/internal/config"
	"github.com/harkawataryan/herd/internal/extproc"
	"github.com/harkawataryan/herd/internal/gateway"
	"github.com/harkawataryan/herd/internal/scheduler"
)

func main() {
	cfgPath := flag.String("config", "herd.yaml", "config file (same format as the gateway)")
	addr := flag.String("addr", ":9002", "gRPC listen address for Envoy")
	adminAddr := flag.String("admin", ":8081", "HTTP address for /metrics and /admin")
	policyFlag := flag.String("policy", "", "override policy")
	flag.Parse()

	cfg, err := config.Load(*cfgPath)
	if err != nil {
		log.Fatalf("config: %v", err)
	}
	if *policyFlag != "" {
		cfg.Policy = *policyFlag
	}
	reg := backend.NewRegistry()
	for _, b := range cfg.Backends {
		r := backend.NewReplica(b.Name, b.URL, backend.Local, b.Models)
		if b.KVCapacity > 0 {
			r.SetKVCapacity(b.KVCapacity)
		}
		if b.MaxBatch > 0 {
			r.MaxBatch = b.MaxBatch
		}
		reg.Add(r)
	}
	for _, p := range cfg.Providers {
		r := backend.NewReplica(p.Name, p.URL, backend.Cloud, p.Models)
		r.ModelMap, r.PriceIn, r.PriceOut = p.ModelMap, p.PriceInPerM, p.PriceOutPerM
		if p.APIKeyEnv != "" {
			r.APIKey = os.Getenv(p.APIKeyEnv)
		}
		reg.Add(r)
	}
	t := scheduler.DefaultThresholds()
	t.Saturated = cfg.SaturationThreshold
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go reg.Run(ctx, time.Duration(cfg.ScrapeIntervalMs)*time.Millisecond)
	gw := gateway.New(cfg, reg, scheduler.New(cfg.Policy, t), t)
	go gw.Run(ctx)

	lis, err := net.Listen("tcp", *addr)
	if err != nil {
		log.Fatal(err)
	}
	gs := grpc.NewServer()
	extprocv3.RegisterExternalProcessorServer(gs, extproc.New(gw))
	healthpb.RegisterHealthServer(gs, health.NewServer())

	mux := http.NewServeMux()
	gw.Register(mux)
	mux.Handle("GET /metrics", promhttp.Handler())
	go func() { log.Fatal(http.ListenAndServe(*adminAddr, mux)) }()

	log.Printf("herd extproc on %s (admin %s) policy=%s backends=%d", *addr, *adminAddr, cfg.Policy, len(cfg.Backends))
	log.Fatal(gs.Serve(lis))
}
