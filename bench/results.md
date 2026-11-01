# Herd benchmark (homogeneous)

Workload: rps=7 duration=120s seed=1 time_scale=0.05 replicas=8 loadgen_args='-system-chars 20000 -system-prompts 20 -mix short=0.7,rag=0.3,long=0'. Latencies are in simulated milliseconds. Generated 2026-10-27T07:19:41Z.

| policy | p50 TTFT ms | p95 TTFT ms | p99 TTFT ms | p99 e2e ms | req/s | tokens/s | shed % | spill % | prefix hit % |
|---|---|---|---|---|---|---|---|---|---|
| round_robin | 1460 | 5080 | 10040 | 15920 | 6.70 | 560 | 0.0 | 0.0 | 0.0 |
| least_queue | 1040 | 3780 | 6240 | 11160 | 6.70 | 560 | 0.0 | 0.0 | 0.0 |
| kv_aware | 1120 | 4120 | 5560 | 11080 | 6.69 | 559 | 0.1 | 0.2 | 0.0 |
| prefix_aware | 740 | 2760 | 4480 | 10080 | 6.70 | 560 | 0.0 | 0.0 | 81.8 |
