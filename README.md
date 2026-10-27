# Herd

One OpenAI-compatible endpoint in front of your own model servers, routed by live KV-cache and prefix-cache signals, with budgeted spillover to hosted providers when local capacity runs out.

Point anything that speaks the OpenAI API (Cursor, Open WebUI, LangChain, a curl) at Herd instead of at a single vLLM, llama.cpp or Ollama instance. Herd pools the replicas and, for every request, picks the one that will answer fastest: the replica with KV-cache headroom, or the one that already has this prompt's prefix cached and can skip prefill. When every replica is saturated it sheds low-priority traffic, queues normal traffic fairly across tenants, and spills critical traffic (or anything with budget) to a cloud provider. Every decision is visible in a live dashboard and in Prometheus.

## Why

Load balancing for model servers is not load balancing for web servers. A request's cost depends on its prompt length and on whether the server already holds its prefix in cache; a replica with a free connection slot can still be out of KV-cache memory; and time to first token, which is what a user feels, collapses the moment a replica starts queueing internally. Round-robin and least-connections can't see any of that. Herd routes on the signals that matter, the way GKE Inference Gateway and similar systems do, and measures the difference.

## Results

Eight simulated replicas (32k-token KV cache, batch 16), a shared-context workload (a pool of 20 large system prompts with Zipf popularity, short user turns, 30% with extra retrieved context), Poisson arrivals at 7 requests/s, identical seeded trace for every policy. `bench/run.sh` reproduces this in about 40 seconds.

| policy | p50 TTFT | p95 TTFT | p99 TTFT | p99 end-to-end | prefix hits |
|---|---|---|---|---|---|
| round_robin | 1460 ms | 5080 ms | 10040 ms | 15920 ms | 0% |
| least_queue | 1040 ms | 3780 ms | 6240 ms | 11160 ms | 0% |
| kv_aware | 1120 ms | 4120 ms | 5560 ms | 11080 ms | 0% |
| **prefix_aware** | **740 ms** | **2760 ms** | **4480 ms** | **10080 ms** | 82% |

Prefix-aware routing cuts p99 time to first token by 55% and p50 by 49% against round-robin at the same throughput, with no shedding.

With two of the eight replicas at half capacity (`bench/hetero.sh`, 6 req/s), static balancing falls apart while live signals route around the weak replicas:

| policy | p50 TTFT | p99 TTFT | share sent to each weak replica |
|---|---|---|---|
| round_robin | 1480 ms | 292140 ms | 12.4% |
| least_queue | 1220 ms | 23440 ms | ~5% |
| kv_aware | 1140 ms | 12560 ms | 4.4% |
| prefix_aware | 1080 ms | 13480 ms | 4.9% |

Full tables with throughput, shed and spillover rates: [`bench/results.md`](bench/results.md), [`bench/results-hetero.md`](bench/results-hetero.md). Methodology and caveats: [`docs/benchmarks.md`](docs/benchmarks.md).

## How it works

```
clients ──HTTP/SSE──▶ gateway ─┬─▶ replica 1 (vLLM / llama.cpp / Ollama / simulator)
                      │        ├─▶ replica 2            each scraped for KV-cache and queue gauges
   tenant + criticality        └─▶ cloud provider        (spillover, priced per token, budgeted)
   ▼
   saturated? ── no ──▶ policy picks a replica ──▶ streaming proxy, TTFT/TPOT measured
       │ yes
       ├─ sheddable ──▶ 503 + Retry-After
       ├─ critical  ──▶ cloud spillover (or least-loaded replica if no provider)
       └─ standard  ──▶ cloud if the tenant has budget, else fair queue until a replica frees
```

**Signals.** Every 500 ms the gateway scrapes each replica's `/metrics` for `vllm:gpu_cache_usage_perc` and `vllm:num_requests_waiting` (llama.cpp's names are understood too). Between scrapes it adds the tokens it has dispatched since, so the estimate doesn't go stale. A replica's load score is `0.6 × KV estimate + 0.4 × in-flight / batch`, and a full batch or a non-empty server-side queue scores 1.

**Prefix affinity.** Prompts are hashed in 256-character blocks with each hash folded over the previous one, the same scheme servers use for prefix caching. The gateway keeps an LRU index of which block chains it has sent to which replica. `prefix_aware` sends a request to the replica with the longest matching chain unless that replica's score is above 0.8, in which case it falls back to the least-loaded replica; a tiny hysteresis stops it flapping.

**Saturation handling.** Criticality comes from the `X-Criticality` header (`critical`, `standard`, `sheddable`). Tenants are identified by API key and carry a weight and a cloud budget. The queue releases critical requests first and shares freed capacity across tenants by deficit round robin.

**Spillover.** Providers are configured with per-token prices and a model mapping (`llama-3-8b → gpt-4o-mini`). Spend is tracked per tenant and stops at the budget.

**Envoy mode.** `cmd/extproc` exposes the same scheduler as an Envoy external processor: Envoy buffers the body, asks Herd for an endpoint, and an `ORIGINAL_DST` cluster forwards the request there. That is the shape of the Kubernetes Gateway API Inference Extension, so the routing can live inside an existing mesh instead of behind an extra hop.

## Run it

```bash
# 1. three simulated replicas + a mock cloud provider + the gateway with the dashboard
go build -o bin/ ./cmd/...
for i in 1 2 3; do bin/simbackend -addr :900$i -name sim-$i -time-scale 0.1 & done
bin/simbackend -addr :9101 -name cloud -mode cloud -models gpt-4o-mini &
cp herd.example.yaml herd.yaml   # edit URLs/ports if needed
bin/gateway -config herd.yaml    # dashboard at http://localhost:8080

# 2. talk to it like any OpenAI endpoint
curl -N localhost:8080/v1/chat/completions -H 'Authorization: Bearer demo-key' -H 'X-Criticality: standard' \
  -H 'Content-Type: application/json' \
  -d '{"model":"llama-3-8b","stream":true,"messages":[{"role":"user","content":"hello"}]}'
```

Responses carry `X-Herd-Backend` and `X-Herd-Reason` so you can see what happened. `GET /admin/backends` and `GET /admin/decisions` feed the dashboard; `GET /metrics` is Prometheus.

**Real servers.** Any backend that serves `/v1/chat/completions` works. vLLM exposes the KV-cache gauges Herd scrapes by default; llama.cpp's `llama-server --metrics` does too. Ollama has no KV metrics, so it is routed on in-flight count alone.

**Compose stack** with eight replicas, Prometheus and a Grafana dashboard: `docker compose -f deploy/compose/docker-compose.yml up --build` (gateway :8080, Grafana :3000).

**Kubernetes:** `deploy/k8s/herd.yaml` runs the replicas as a StatefulSet and the gateway as a Deployment with an HPA (CPU by default; `hpa-queue.yaml` scales on `herd_queue_depth` with prometheus-adapter). Tested on kind.

**Envoy:** `bin/extproc -config herd.yaml` then `envoy -c deploy/envoy/envoy.yaml`.

## Benchmarks

```bash
bench/run.sh                 # four policies, identical trace, results.md
bench/hetero.sh              # two replicas at half capacity
BENCH_RPS=9 BENCH_DURATION=300s bench/run.sh
LOADGEN_ARGS="-system-chars 4000 -mix short=0.4,rag=0.4,long=0.2" bench/run.sh
```

The load generator (`cmd/loadgen`) replays a seeded, recordable trace at a scaled clock so a five-minute run takes fifteen seconds; see `bench/README.md`.

## Layout

| path | |
|---|---|
| `cmd/gateway` | the gateway binary |
| `cmd/extproc` | the scheduler as an Envoy external processor |
| `cmd/simbackend` | model-server simulator (KV cache, batching, prefix cache, vLLM metrics) |
| `cmd/loadgen`, `internal/workload` | replayable load generator |
| `internal/scheduler` | the four policies and the load score |
| `internal/prefix` | block hashing and the per-replica prefix index |
| `internal/backend` | replica state and the metrics scraper |
| `internal/queue` | critical-first, weighted fair queue |
| `internal/gateway` | request pipeline, spillover, budgets, admin API |
| `ui/` | the live routing dashboard (static) |
| `deploy/` | compose, Grafana, Kubernetes, Envoy |

Tests: `go test -race ./...` (unit tests for every package plus an in-process integration test that runs simulated replicas behind the gateway and checks prefix routing, shedding, spillover and queueing). CI runs the tests, a 10-second benchmark, and the image build.

## Limitations

- The simulator is calibrated to the shape of real servers (prefill vs decode, batch slowdown, KV admission, prefix caching), not to any specific GPU. Absolute numbers are simulated milliseconds; the comparisons between policies are the point.
- The prefix index is the gateway's belief about a replica's cache, not the cache itself; under eviction pressure some predicted hits miss. The benchmark reports the replicas' actual hit ratios alongside the gateway's.
- At low load `prefix_aware` concentrates each prompt family on one replica rather than spreading; that is intended (it is what makes the cache hit), and load takes over once a replica's score passes 0.8.
- Spend accounting lives in memory per gateway instance; a shared store is the obvious next step for multiple gateways.
