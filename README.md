# Herd

Herd is a load balancer for people running their own LLM servers. It sits in front of a few vLLM, llama.cpp or Ollama replicas, speaks the OpenAI chat API, and decides which replica gets each request based on how full each one's KV cache is and which one has already seen the start of the prompt. When every replica is full it drops low-priority requests, queues the rest fairly between tenants, and can send overflow to a hosted provider as long as that tenant still has budget.

I started it after spending a summer on utilization-based load balancing in a service mesh. I wanted to see how the same idea works for LLM serving, where it gets harder. A request's cost depends on how long the prompt is and what's already cached, and a replica can have a free connection while being out of KV memory. Round-robin and least-connections can't see any of that.

Anything that talks to the OpenAI chat API can point at it (Cursor, Open WebUI, LangChain, plain curl), and every routing decision shows up in a live dashboard and in Prometheus.

## Results

The benchmark runs 8 simulated replicas, each with a 32k-token KV cache and a batch size of 16. The workload is 20 large shared system prompts picked with Zipf popularity, each followed by a short user turn, arriving as a Poisson process at 7 requests a second. Every policy replays the same seeded trace, and `bench/run.sh` reproduces it in about 40 seconds.

| policy | p50 TTFT | p95 TTFT | p99 TTFT | p99 end-to-end | prefix hits |
|---|---|---|---|---|---|
| round_robin | 1460 ms | 5080 ms | 10040 ms | 15920 ms | 0% |
| least_queue | 1040 ms | 3780 ms | 6240 ms | 11160 ms | 0% |
| kv_aware | 1120 ms | 4120 ms | 5560 ms | 11080 ms | 0% |
| prefix_aware | 740 ms | 2760 ms | 4480 ms | 10080 ms | 82% |

Prefix-aware routing cuts p99 time to first token by 55% compared to round-robin, at the same throughput and without shedding anything.

The gap gets much bigger when the replicas aren't equal. With two of the eight running at half capacity (`bench/hetero.sh`, 6 requests a second), round-robin keeps giving the slow replicas a full share and their queues never drain.

| policy | p50 TTFT | p99 TTFT | traffic to each slow replica |
|---|---|---|---|
| round_robin | 1480 ms | 292140 ms | 12.4% |
| least_queue | 1220 ms | 23440 ms | ~5% |
| kv_aware | 1140 ms | 12560 ms | 4.4% |
| prefix_aware | 1080 ms | 13480 ms | 4.9% |

The full tables are in [bench/results.md](bench/results.md) and [bench/results-hetero.md](bench/results-hetero.md), and [docs/benchmarks.md](docs/benchmarks.md) explains how the workload and simulator are set up and what the numbers can and can't tell you.

## How it routes

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

Every 500 ms the gateway scrapes each replica's `/metrics` for `vllm:gpu_cache_usage_perc` and `vllm:num_requests_waiting` (llama.cpp's metric names work too). Scrapes go stale quickly under load, so between them it adds up the tokens it has sent each replica since the last one. A replica's load score is `0.6 × KV estimate + 0.4 × inflight/batch`, and a full batch or a non-empty server queue counts as fully loaded.

For prefix affinity, prompts are hashed in 256-character blocks, with each block's hash chained to the one before it. That's the same way servers key their own prefix caches, so the gateway can keep an LRU index of which chains it sent where. The `prefix_aware` policy picks the replica with the longest match unless that replica's load score is over 0.8, in which case it falls back to the least loaded one. A little hysteresis keeps it from flapping between the two.

Each request can carry `X-Criticality: critical`, `standard` or `sheddable`, and tenants are identified by API key, each with a weight and a cloud budget. When the replicas are saturated, the queue releases critical requests first and splits freed capacity between tenants with deficit round robin. Cloud providers are configured with per-token prices and a model mapping (for example `llama-3-8b` to `gpt-4o-mini`), and spending stops at each tenant's budget.

The same scheduler also runs as an Envoy external processor in `cmd/extproc`. Envoy buffers the request body, asks Herd for an endpoint, and an `ORIGINAL_DST` cluster forwards the request there. That's the same shape as the Kubernetes Gateway API inference extension, so Herd can sit inside an existing mesh instead of in front of it.

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

Every response includes `X-Herd-Backend` and `X-Herd-Reason`, so you can see where it went and why. The dashboard reads `/admin/backends` and `/admin/decisions`, and `/metrics` is for Prometheus.

Any server with a `/v1/chat/completions` endpoint works as a backend. vLLM and `llama-server --metrics` expose KV cache usage. Ollama doesn't, so Herd routes Ollama replicas on in-flight count alone.

To run 8 replicas with Prometheus and Grafana, use `docker compose -f deploy/compose/docker-compose.yml up --build` (gateway on :8080, Grafana on :3000). `deploy/k8s/herd.yaml` has a StatefulSet for the replicas, a Deployment for the gateway and an HPA that scales on CPU, or on queue depth through prometheus-adapter with `hpa-queue.yaml`. I wrote it for kind and haven't tried it on a managed cluster. To run behind Envoy, start `bin/extproc -config herd.yaml` and then `envoy -c deploy/envoy/envoy.yaml`.

## Running the benchmarks

```bash
bench/run.sh
bench/hetero.sh
BENCH_RPS=9 BENCH_DURATION=300s bench/run.sh
LOADGEN_ARGS="-system-chars 4000 -mix short=0.4,rag=0.4,long=0.2" bench/run.sh
```

`cmd/loadgen` replays a seeded trace on a scaled clock, so a 5-minute run takes about 15 seconds. `bench/README.md` has the details.

## Code

The gateway is in `cmd/gateway`, with the request pipeline, spillover, budgets and admin API in `internal/gateway`. The four routing policies are in `internal/scheduler`, prefix hashing and the per-replica index in `internal/prefix`, replica state and the metrics scraper in `internal/backend`, and the fair queue in `internal/queue`. `cmd/simbackend` is the model server simulator (KV cache, batching, prefix cache, vLLM-style metrics) and `cmd/loadgen` with `internal/workload` is the load generator. The dashboard is in `ui/` and deployment files are in `deploy/`.

`go test -race ./...` runs unit tests for every package plus an integration test that starts simulated replicas behind the gateway in-process and checks prefix routing, shedding, spillover and queueing.

## What to keep in mind

The simulator models how a real server behaves (prefill versus decode, slower steps at larger batches, KV admission, the prefix cache), but it isn't calibrated to any particular GPU. Compare the policies to each other rather than reading too much into the absolute milliseconds.

The prefix index is the gateway's guess at what each replica has cached, and under eviction pressure some of the hits it predicts turn out to be misses. The benchmark reports the replicas' real hit rates next to the gateway's predictions so you can see the difference.

At low load, `prefix_aware` keeps each family of prompts on one replica instead of spreading them out. That's on purpose, since it's what produces the cache hits, and load takes over once a replica's score passes 0.8.

Spending is tracked in memory in each gateway process, so running more than one gateway would need a shared store.
