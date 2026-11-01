# Herd

An OpenAI-compatible gateway that sits in front of your own model servers (vLLM, llama.cpp, Ollama) and decides which replica gets each request. It prices every placement in one unit, prompt blocks of prefill: the blocks the replica would have to prefill because it doesn't have them cached, the blocks' worth of time the request would spend waiting for a batch slot or KV memory there, and the reuse value of the cached blocks the request would push out. Lowest total wins. When everything is full it sheds low-priority requests, queues the rest fairly across tenants, and can spill over to a hosted provider within a per-tenant budget.

I built it after a summer working on utilization-based load balancing in a service mesh and wanting to see how the same idea plays out for LLM serving, where request cost depends on prompt length and on what's already cached, and where a replica with a free connection can still be out of KV memory. Round-robin and least-connections can't see any of that, and routers that score cache hits and load separately have to be told how to trade one against the other.

Point anything that speaks the OpenAI chat API at it (Cursor, Open WebUI, LangChain, curl). Every routing decision shows up in a live dashboard and in Prometheus.

## What's different about it

**One cost, no thresholds.** Most routers compute a cache-affinity score and a load score and combine them with weights (the llm-d scheduler behind GKE Inference Gateway weights prefix hits 3 and queue depth 2) or with a cutoff (Herd's own earlier `prefix_aware` policy abandoned a cache hit once a replica's load score passed 0.8). `pack` converts everything to prefill blocks. A cache hit is worth exactly the blocks it saves; waiting for a slot on a busy replica costs exactly the blocks that could have been prefilled in that time; so a 60-block hit on a replica with a 300 ms wait beats prefilling 60 blocks elsewhere, and a 10-block hit does not.

**The gateway keeps its own ledger.** The KV-cache gauge a server exports is scraped every few hundred milliseconds and does not include requests the gateway has already sent that are still queued at the server. Herd counts the tokens of everything it has in flight per replica and treats that as a floor on the replica's KV demand. Without this, every policy that trusts the gauge piles large prompts onto a replica that is already full and finds out seconds later.

**Expected wait from the replica's own rhythm.** A replica with no free batch slot or no KV room frees a slot roughly every (mean request duration ÷ running requests), and each completion releases about a request's worth of tokens. Herd measures both per replica and turns the queue ahead of a request into seconds, then into blocks.

**Conversations and prompt families stay put.** Agent loops and chats send prompts that extend the previous one. Herd recognises a session from an `X-Session-ID` header, or infers it from the prompt prefix, and keeps it on the replica that served its last turn unless moving saves more than a few blocks. Prompts that share a leading block (one app's system prompt) form a family that is kept on as few replicas as its load allows, so the same shared prefix isn't cached on all eight replicas while conversations get evicted for lack of room.

**What a placement evicts is priced too.** Each block in a replica's cache is valued by how likely it is to be asked for again before it would have been evicted anyway: its recent request frequency against the replica's cache turnover time, plus the chance that the conversation it belongs to is coming back. A one-off document is steered to the replica whose oldest cached blocks are other one-off documents. In the benchmarks below this term buys the least of the four; the ablation is in the docs.

## Benchmark results

Eight simulated replicas plus a cloud provider, the same seeded traces for every policy, three seeds averaged. Latencies are time to first token in simulated milliseconds. `goodput` is the share of all requests answered within 2 s; `cache hit` is the share of prompt blocks the replicas themselves reported as prefix-cache hits. Two policies reproduce the documented scoring of other open-source routers so Herd can be compared on the same workload: `llmd_style` (prefix score ×3 + queue score ×2) and `dynamo_style` (blocks to prefill + blocks of active decode). They are reimplementations from those projects' public docs, not their code.

**Shared-context chat** (`bench/run.sh`): 20 large system prompts with Zipf popularity, short user turns, 7 req/s, 32k-token caches.

| policy | p50 | p95 | p99 | goodput | cache hit |
|---|---|---|---|---|---|
| round_robin | 1300 ms | 4733 ms | 9233 ms | 73.0% | 41% |
| least_queue | 1093 ms | 3440 ms | 5600 ms | 81.6% | 41% |
| kv_aware | 1133 ms | 3673 ms | 5940 ms | 80.2% | 41% |
| prefix_aware | 313 ms | 2600 ms | 4800 ms | 90.7% | 71% |
| llmd_style | 440 ms | 4000 ms | 9767 ms | 87.4% | 76% |
| dynamo_style | 753 ms | 2407 ms | 3460 ms | 91.5% | 61% |
| pack | 353 ms | 2587 ms | 4287 ms | 90.1% | 74% |

**Two weak replicas** (`bench/hetero.sh`): same, with replicas 7 and 8 at half capacity.

| policy | p50 | p95 | p99 | goodput | traffic to each weak replica |
|---|---|---|---|---|---|
| round_robin | 1707 ms | 168.6 s | 293.2 s | 58.9% | 12.4% |
| least_queue | 1373 ms | 8120 ms | 25.2 s | 65.6% | ~5% |
| kv_aware | 1713 ms | 8313 ms | 15.7 s | 56.8% | ~4% |
| prefix_aware | 1473 ms | 7727 ms | 18.0 s | 62.3% | 4.1% |
| llmd_style | 1013 ms | 11.1 s | 34.8 s | 70.9% | 5.5% |
| dynamo_style | 1253 ms | 644.9 s | 812.7 s | 60.4% | ~12% |
| pack | 1327 ms | 7133 ms | 17.0 s | 66.2% | 5.8% |

**Agent sessions under cache pressure** (`bench/agents.sh`): conversations that grow turn by turn with 12 s of think time between turns, mixed with one-off RAG requests, on 16k-token caches.

| policy | p50 | p95 | p99 | p99 agent turns | goodput | cache hit |
|---|---|---|---|---|---|---|
| round_robin | 767 ms | 3567 ms | 6927 ms | 6413 ms | 88.2% | 25% |
| least_queue | 740 ms | 3307 ms | 6573 ms | 6520 ms | 89.7% | 25% |
| kv_aware | 733 ms | 3587 ms | 6647 ms | 6553 ms | 89.8% | 26% |
| prefix_aware | 460 ms | 3040 ms | 5920 ms | 5413 ms | 90.5% | 46% |
| llmd_style | 433 ms | 3060 ms | 5680 ms | 5400 ms | 91.0% | 47% |
| dynamo_style | 573 ms | 2727 ms | 5073 ms | 5187 ms | 92.5% | 38% |
| pack | 447 ms | 2680 ms | 4767 ms | 4633 ms | 93.0% | 46% |

On the workload it was built for, `pack` has the lowest p95 and p99 overall and for agent turns (16–20% under `prefix_aware` and `llmd_style`), and the highest goodput.

How to read all three: `pack` is first or second on every tail metric in every scenario, and it is the only policy that is. Each of the others has a workload it is good at and one where it falls apart: `llmd_style` has the best cache reuse but its p99 doubles on the chat workload and triples with weak replicas, because its queue score is a rank, not a wait; `dynamo_style` has the best p99 on the chat workload and collapses with weak replicas, because decode blocks say nothing about a replica being slow; `prefix_aware` is strong on chat and loses conversations under cache pressure. Full tables with throughput, shedding and spillover: [bench/results.md](bench/results.md), [bench/results-hetero.md](bench/results-hetero.md), [bench/results-agents.md](bench/results-agents.md). Workloads, what each policy can see, the eviction-pricing ablation and the caveats: [docs/benchmarks.md](docs/benchmarks.md).

## How it works

```
clients ──HTTP/SSE──▶ gateway ─┬─▶ replica 1 (vLLM / llama.cpp / Ollama / simulator)
                      │        ├─▶ replica 2            /metrics scraped for KV and queue gauges
   tenant, criticality,        └─▶ cloud provider        (priced per token, per-tenant budget)
   session
   ▼
   saturated? ── no ──▶ policy prices each replica ──▶ streaming proxy, TTFT/TPOT measured
       │ yes
       ├─ sheddable ──▶ 503 + Retry-After
       ├─ critical  ──▶ cloud (or least-loaded replica if there's no provider)
       └─ standard  ──▶ cloud if the tenant has budget, otherwise fair queue
```

**Load signal.** Every 500 ms the gateway scrapes each replica's `/metrics` for `vllm:gpu_cache_usage_perc` and `vllm:num_requests_waiting` (llama.cpp's metric names work too). Between scrapes it adds the tokens it has sent since, and it never lets the estimate drop below its own ledger of in-flight tokens. Per replica it also keeps running averages of request duration, tokens per request and time to first token.

**Prefix index.** Prompts are hashed in 256-char blocks, each hash chained to the previous one, which is how servers key their prefix caches. The gateway keeps a per-replica LRU index of the chains it has sent, sized to the replica's KV cache, refreshed when a request is dispatched and again when its first token arrives (which is when the server actually inserts the prompt). The load generator checks this index against what the replicas report per request; the mean error is under two blocks.

**The pack policy.** For each candidate replica: `cost = blocks to prefill + expected wait in blocks + value of blocks evicted + value of own blocks × risk of losing them before the next turn`. The expected wait is zero when a batch slot and KV room are free, otherwise completions needed × seconds per completion, converted at the configured `prefill_tps`. Block value is `1 − e^(−requests per half-life × survival fraction)`, combined with the session's return probability, capped at one re-prefill per block. A conversation's last replica, then the family's oldest member, wins if within `pack_slack_blocks` of the cheapest. The other policies (`round_robin`, `least_queue`, `kv_aware`, `prefix_aware`, `llmd_style`, `dynamo_style`) are kept for comparison and selectable with `-policy`.

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
  -H 'Content-Type: application/json' -H 'X-Session-ID: demo-1' \
  -d '{"model":"llama-3-8b","stream":true,"messages":[{"role":"user","content":"hello"}]}'
```

Responses include `X-Herd-Backend`, `X-Herd-Reason`, `X-Herd-Prefix-Blocks` and, for conversations, `X-Herd-Session`. `/admin/backends` and `/admin/decisions` are what the dashboard reads, with the per-replica costs and waits behind every decision; `/metrics` is Prometheus.

Any server with a `/v1/chat/completions` endpoint works as a backend. vLLM and `llama-server --metrics` expose the KV gauges; Ollama doesn't, so it gets routed on the gateway's own ledger and in-flight count. Set `prefill_tps` in the config to your servers' prompt throughput so waits are priced in the right units.

Other ways to run it:
- `docker compose -f deploy/compose/docker-compose.yml up --build` for 8 replicas plus Prometheus and Grafana (gateway :8080, Grafana :3000).
- `deploy/k8s/herd.yaml` has a StatefulSet for replicas, a Deployment for the gateway, and an HPA (CPU by default, `hpa-queue.yaml` for queue depth via prometheus-adapter). Written for kind; I haven't run it on a managed cluster.
- `bin/extproc -config herd.yaml` then `envoy -c deploy/envoy/envoy.yaml`.

## Running the benchmarks

```bash
bench/run.sh
bench/hetero.sh
bench/agents.sh
BENCH_SEEDS="1 2 3" bench/agents.sh
POLICIES="prefix_aware pack" EXTRA_CONFIG="pack_slack_blocks: 16" bench/agents.sh
```

`cmd/loadgen` replays a seeded trace at a scaled clock, so a 3-minute run takes about 10 seconds; agent sessions are replayed as dependent chains, each turn sent only after the previous one is answered. Details in `bench/README.md`.

## Layout

| path | |
|---|---|
| `cmd/gateway` | the gateway |
| `cmd/extproc` | scheduler as an Envoy external processor |
| `cmd/simbackend` | model server simulator (KV cache, batching, prefix cache, vLLM-style metrics) |
| `cmd/loadgen`, `internal/workload` | load generator, including agent sessions |
| `internal/scheduler` | the policies; `pack.go` is the default, `WaitBlocks` the wait model |
| `internal/value` | block reuse values and session tracking |
| `internal/prefix` | block hashing, per-replica prefix index, eviction prediction |
| `internal/backend` | replica state, token ledger, metrics scraper |
| `internal/queue` | fair queue |
| `internal/gateway` | request pipeline, spillover, budgets, admin API |
| `ui/` | dashboard |
| `deploy/` | compose, Grafana, Kubernetes, Envoy |

`go test -race ./...` runs unit tests for each package plus an integration test that spins up simulated replicas behind the gateway in-process and checks prefix routing, session affinity, shedding, spillover and queueing.

## Caveats

- The simulator models the shape of a real server (prefill vs decode, batch slowdown, KV admission, prefix cache) but isn't calibrated to any particular GPU, and it doesn't slow prefill down while decode is busy. Compare the policies to each other, not the absolute milliseconds.
- `llmd_style` and `dynamo_style` are the documented default scorers of those projects, reimplemented here. Both projects have options the defaults don't enable, and the real thing would be the comparison to run next, on real model servers.
- Seed-to-seed variance is about the size of pack's edge on the agents workload. It is a consistent edge across seeds, not a large one.
- The eviction-value term needs a popularity half-life and a default think time (`value_half_life_ms`, `session_gap_ms`); the session gap is learned per conversation after its second turn.
- Spend tracking is in memory per gateway process. Running more than one gateway would need a shared store.
