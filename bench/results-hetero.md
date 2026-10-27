# Herd benchmark (hetero)

Workload: rps=6 duration=120s seed=1 time_scale=0.05 replicas=8 loadgen_args='-system-chars 20000 -system-prompts 20 -mix short=0.7,rag=0.3,long=0'. Latencies are in simulated milliseconds. Generated 2026-10-27T07:21:27Z.

| policy | p50 TTFT ms | p95 TTFT ms | p99 TTFT ms | p99 e2e ms | req/s | tokens/s | shed % | spill % | prefix hit % |
|---|---|---|---|---|---|---|---|---|---|
| round_robin | 1480 | 168060 | 292140 | 300700 | 5.78 | 486 | 0.0 | 0.0 | 0.0 |
| least_queue | 1220 | 7660 | 23440 | 29240 | 5.76 | 484 | 0.4 | 1.4 | 0.0 |
| kv_aware | 1140 | 5560 | 12560 | 18340 | 5.74 | 483 | 0.7 | 1.7 | 0.0 |
| prefix_aware | 1080 | 5400 | 13480 | 19620 | 5.78 | 486 | 0.0 | 0.6 | 71.6 |
