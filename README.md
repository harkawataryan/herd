# Herd

An OpenAI-compatible gateway that sits in front of your own model servers (vLLM, llama.cpp, Ollama) and picks which replica gets each request based on live KV-cache usage and prefix-cache affinity. When everything is full it sheds low-priority requests, queues the rest fairly across tenants, and can spill over to a hosted provider within a per-tenant budget.

I built it after a summer working on utilization-based load balancing in a service mesh and wanting to see how the same idea plays out for LLM serving, where request cost depends on prompt length and on what's already cached, and where a replica with a free connection can still be out of KV memory. Round-robin and least-connections can't see any of that.

Point anything that speaks the OpenAI chat API at it (Cursor, Open WebUI, LangChain, curl). Every routing decision shows up in a live dashboard and in Prometheus.

## Benchmark results

8 simulated replicas (32k-token KV cache, batch size 16), a workload of 20 large shared system prompts with Zipf popularity and short user turns, Poisson arrivals at 7 req/s. Same seeded trace for each policy. `bench/run.sh` reproduces it in about 40 seconds.

| policy | p50 TTFT | p95 TTFT | p99 TTFT | p99 end-to-end | prefix hits |
|---|---|---|---|---|---|
| round_robin | 1460 ms | 5080 ms | 10040 ms | 15920 ms | 0% |
| least_queue | 1040 ms | 3780 ms | 6240 ms | 11160 ms | 0% |
| kv_aware | 1120 ms | 4120 ms | 5560 ms | 11080 ms | 0% |
| prefix_aware | 740 ms | 2760 ms | 4480 ms | 10080 ms | 82% |

prefix_aware cuts p99 time-to-first-token by 55% vs round-robin at the same throughput, no shedding.

With two of the eight replicas at half capacity (`bench/hetero.sh`, 6 req/s), round-robin keeps sending the weak replicas a full share and their queues never drain:

| policy | p50 TTFT | p99 TTFT | traffic to each weak replica |
|---|---|---|---|
| round_robin | 1480 ms | 292140 ms | 12.4% |
| least_queue | 1220 ms | 23440 ms | ~5% |
| kv_aware | 1140 ms | 12560 ms | 4.4% |
| prefix_aware | 1080 ms | 13480 ms | 4.9% |

Full tables: [bench/results.md](bench/results.md), [bench/results-hetero.md](bench/results-hetero.md). How the workload and simulator are set up, and what the numbers do and don't mean: [docs/benchmarks.md](docs/benchmarks.md).

## How it works

```
clients ──HTTP/SSE──▶ gateway ─┬─▶ replica 1 (vLLM / llama.cpp / Ollama / simulator)
                      │        ├─▶ replica 2            /metrics scraped for KV and queue gauges
   tenant + criticality        └─▶ cloud provider        (priced per token, per-tenant budget)
   ▼
   saturated? ── no ──▶ policy picks a replica ──▶ streaming proxy, TTFT/TPOT measured
       │ yes
       ├─ sheddable ──▶ 503 + Retry-After
       ├─ critical  ──▶ cloud (or least-loaded replica if there's no provider)
       └─ standard  ──▶ cloud if the tenant has budget, otherwise fair queue
```

**Load signal.** Every 500ms the gateway scrapes each replica's `/metrics` for `vllm:gpu_cache_usage_perc` and `vllm:num_requests_waiting` (llama.cpp's metric names work too). Between scrapes it adds the tokens it has sent since, so the estimate stays current. Load score = `0.6 × KV estimate + 0.4 × inflight/batch`; a full batch or a non-empty server queue counts as 1.

**Prefix affinity.** Prompts are hashed in 256-char blocks, each hash chained to the previous one, which is how servers key their prefix caches. The gateway keeps an LRU index of which chains went to which replica. `prefix_aware` picks the replica with the longest match unless its load score is over 0.8, then falls back to the least loaded one. There's a small hysteresis so it doesn't flap.

**Saturation.** Requests carry `X-Criticality: critical|standard|sheddable`. Tenants are identified by API key and have a weight and a cloud budget. The queue releases critical requests first and splits freed capacity across tenants with deficit round robin.

**Spillover.** Providers are configured with per-token prices and a model mapping (e.g. `llama-3-8b → gpt-4o-mini`). Spend is tracked per tenant and stops at the budget.

**Envoy.** `cmd/extproc` runs the same scheduler as an Envoy external processor. Envoy buffers the body, asks for an endpoint, and an `ORIGINAL_DST` cluster sends the request there. Same shape as the Kubernetes Gateway API inference extension, so it can sit inside an existing mesh.

## Running it

```bash
go build -o bin/ ./cmd/...
for i in 1 2 3; do bin/simbackend -addr :900$i -name sim-$i -time-scale 0.1 & done
bin/simbackend -addr :9101 -name cloud -mode cloud -models gpt-4o-mini &
cp herd.example.yaml herd.yaml
bin/gateway -config herd.yaml        # dashboard at http://localhost:8080

curl -N localhost:8080/v1/chat/completions -H 'Authorization: Bearer demo-key' \
  -H 'Content-Type: application/json' \
  -d '{"model":"llama-3-8b","stream":true,"messages":[{"role":"user","content":"hello"}]}'
```

Responses include `X-Herd-Backend` and `X-Herd-Reason`. `/admin/backends` and `/admin/decisions` are what the dashboard reads; `/metrics` is Prometheus.

Any server with a `/v1/chat/completions` endpoint works as a backend. vLLM and `llama-server --metrics` expose the KV gauges; Ollama doesn't, so it gets routed on in-flight count only.

Other ways to run it:
- `docker compose -f deploy/compose/docker-compose.yml up --build` for 8 replicas plus Prometheus and Grafana (gateway :8080, Grafana :3000).
- `deploy/k8s/herd.yaml` has a StatefulSet for replicas, a Deployment for the gateway, and an HPA (CPU by default, `hpa-queue.yaml` for queue depth via prometheus-adapter). Written for kind; I haven't run it on a managed cluster.
- `bin/extproc -config herd.yaml` then `envoy -c deploy/envoy/envoy.yaml`.

## Running the benchmarks

```bash
bench/run.sh
bench/hetero.sh
BENCH_RPS=9 BENCH_DURATION=300s bench/run.sh
LOADGEN_ARGS="-system-chars 4000 -mix short=0.4,rag=0.4,long=0.2" bench/run.sh
```

`cmd/loadgen` replays a seeded trace at a scaled clock, so a 5-minute run takes about 15 seconds. Details in `bench/README.md`.

## Layout

| path | |
|---|---|
| `cmd/gateway` | the gateway |
| `cmd/extproc` | scheduler as an Envoy external processor |
| `cmd/simbackend` | model server simulator (KV cache, batching, prefix cache, vLLM-style metrics) |
| `cmd/loadgen`, `internal/workload` | load generator |
| `internal/scheduler` | the four policies |
| `internal/prefix` | block hashing and per-replica prefix index |
| `internal/backend` | replica state and metrics scraper |
| `internal/queue` | fair queue |
| `internal/gateway` | request pipeline, spillover, budgets, admin API |
| `ui/` | dashboard |
| `deploy/` | compose, Grafana, Kubernetes, Envoy |

`go test -race ./...` runs unit tests for each package plus an integration test that spins up simulated replicas behind the gateway in-process and checks prefix routing, shedding, spillover and queueing.

## Caveats

- The simulator models the shape of a real server (prefill vs decode, batch slowdown, KV admission, prefix cache) but isn't calibrated to any particular GPU. Compare the policies to each other, not the absolute milliseconds.
- The prefix index is the gateway's guess at what each replica has cached. Under eviction pressure some predicted hits miss; the benchmark reports the replicas' real hit ratios next to the gateway's.
- At low load, `prefix_aware` concentrates each prompt family on one replica instead of spreading. That's intentional (it's what produces the cache hits); load takes over once a replica's score passes 0.8.
- Spend tracking is in memory per gateway process. Running more than one gateway would need a shared store.
