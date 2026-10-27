// Command simbackend runs one simulated model server.
package main

import (
	"flag"
	"log"
	"net/http"
	"strings"

	"github.com/harkawataryan/herd/internal/sim"
)

func main() {
	var cfg sim.Config
	addr := flag.String("addr", ":9001", "listen address")
	models := flag.String("models", "llama-3-8b", "comma-separated model ids served")
	flag.StringVar(&cfg.Name, "name", "sim", "replica name reported in metrics")
	flag.IntVar(&cfg.KVCapacity, "kv-capacity", 32768, "KV cache capacity in tokens")
	flag.IntVar(&cfg.MaxBatch, "max-batch", 16, "max concurrent sequences")
	flag.Float64Var(&cfg.PrefillTPS, "prefill-tps", 4000, "prefill tokens per second")
	flag.Float64Var(&cfg.DecodeTPS, "decode-tps", 40, "decode tokens per second per sequence")
	flag.Float64Var(&cfg.BatchPenalty, "batch-penalty", 0.15, "decode slowdown fraction per extra sequence")
	flag.IntVar(&cfg.PrefixCacheBlocks, "prefix-blocks", 0, "prefix cache capacity in blocks (default: the whole KV cache)")
	flag.Float64Var(&cfg.TimeScale, "time-scale", 1, "scale simulated time (0.1 = 10x faster)")
	flag.StringVar(&cfg.Mode, "mode", "local", "local (capacity-bound) or cloud (unbounded, priced)")
	flag.Float64Var(&cfg.CloudTTFTMs, "cloud-ttft-ms", 600, "cloud mode: mean time to first token")
	flag.Float64Var(&cfg.CloudJitterMs, "cloud-jitter-ms", 150, "cloud mode: TTFT standard deviation")
	flag.Int64Var(&cfg.Seed, "seed", 1, "random seed")
	flag.Parse()

	cfg.Models = strings.Split(*models, ",")
	engine := sim.NewEngine(cfg)
	log.Printf("simbackend %s (%s) serving %v on %s: kv=%d batch=%d prefill=%.0f decode=%.0f scale=%.2f",
		cfg.Name, cfg.Mode, cfg.Models, *addr, cfg.KVCapacity, cfg.MaxBatch, cfg.PrefillTPS, cfg.DecodeTPS, cfg.TimeScale)
	log.Fatal(http.ListenAndServe(*addr, sim.NewServer(engine).Handler()))
}
