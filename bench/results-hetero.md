# Herd benchmark (hetero)

Workload: rps=7 duration=120s seed=1 time_scale=0.05 replicas=8 kv=32768 loadgen_args='-system-chars 20000 -system-prompts 20 -mix short=0.7,rag=0.3,long=0'. Latencies are in simulated milliseconds; goodput is the share of all requests answered with TTFT under 2 s; cache hit is the share of prompt blocks the servers reported as prefix-cache hits. Generated 2026-11-01T07:22:24Z.

| policy | p50 TTFT ms | p95 TTFT ms | p99 TTFT ms | p99 agent TTFT ms | goodput % | req/s | tokens/s | shed % | spill % | cache hit % |
|---|---|---|---|---|---|---|---|---|---|---|
| round_robin | 1707 | 168573 | 293247 | - | 58.9 | 6.70 | 554 | 0.0 | 0.4 | 37.6 |
| least_queue | 1373 | 8120 | 25220 | - | 65.6 | 6.59 | 545 | 1.7 | 6.6 | 39.1 |
| kv_aware | 1713 | 8313 | 15727 | - | 56.8 | 6.57 | 543 | 2.0 | 6.2 | 39.1 |
| prefix_aware | 1473 | 7727 | 18040 | - | 62.3 | 6.64 | 549 | 1.0 | 4.9 | 53.3 |
| llmd_style | 1013 | 11120 | 34793 | - | 70.9 | 6.66 | 551 | 0.7 | 2.5 | 74.3 |
| dynamo_style | 1253 | 644947 | 812727 | - | 60.4 | 6.70 | 555 | 0.0 | 0.0 | 53.1 |
| pack | 1327 | 7133 | 17027 | - | 66.2 | 6.67 | 553 | 0.4 | 2.0 | 65.3 |

Each row is the mean over 3 seeds.
