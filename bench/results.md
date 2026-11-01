# Herd benchmark (homogeneous)

Workload: rps=7 duration=120s seed=1 time_scale=0.05 replicas=8 kv=32768 loadgen_args='-system-chars 20000 -system-prompts 20 -mix short=0.7,rag=0.3,long=0'. Latencies are in simulated milliseconds; goodput is the share of all requests answered with TTFT under 2 s; cache hit is the share of prompt blocks the servers reported as prefix-cache hits. Generated 2026-11-01T07:16:58Z.

| policy | p50 TTFT ms | p95 TTFT ms | p99 TTFT ms | p99 agent TTFT ms | goodput % | req/s | tokens/s | shed % | spill % | cache hit % |
|---|---|---|---|---|---|---|---|---|---|---|
| round_robin | 1300 | 4733 | 9233 | - | 73.0 | 6.70 | 555 | 0.0 | 0.0 | 40.9 |
| least_queue | 1093 | 3440 | 5600 | - | 81.6 | 6.69 | 554 | 0.1 | 0.3 | 40.6 |
| kv_aware | 1133 | 3673 | 5940 | - | 80.2 | 6.70 | 555 | 0.0 | 0.0 | 40.5 |
| prefix_aware | 313 | 2600 | 4800 | - | 90.7 | 6.70 | 554 | 0.0 | 0.1 | 71.3 |
| llmd_style | 440 | 4000 | 9767 | - | 87.4 | 6.70 | 555 | 0.0 | 0.0 | 76.3 |
| dynamo_style | 753 | 2407 | 3460 | - | 91.5 | 6.70 | 555 | 0.0 | 0.1 | 60.7 |
| pack | 353 | 2587 | 4287 | - | 90.1 | 6.70 | 554 | 0.0 | 0.2 | 74.1 |

Each row is the mean over 3 seeds.
