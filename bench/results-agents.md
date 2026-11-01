# Herd benchmark (agents)

Workload: rps=1.8 duration=180s seed=1 time_scale=0.05 replicas=8 kv=16384 loadgen_args='-system-chars 6000 -system-prompts 16 -mix short=0.2,rag=0.4,agent=0.4 -agent-turns 6 -agent-gap 12s -agent-tool-chars 2000'. Latencies are in simulated milliseconds; goodput is the share of all requests answered with TTFT under 2 s; cache hit is the share of prompt blocks the servers reported as prefix-cache hits. Generated 2026-11-01T07:33:16Z.

| policy | p50 TTFT ms | p95 TTFT ms | p99 TTFT ms | p99 agent TTFT ms | goodput % | req/s | tokens/s | shed % | spill % | cache hit % |
|---|---|---|---|---|---|---|---|---|---|---|
| round_robin | 767 | 3567 | 6927 | 6413 | 88.2 | 5.00 | 616 | 0.3 | 0.5 | 24.9 |
| least_queue | 740 | 3307 | 6573 | 6520 | 89.7 | 5.01 | 617 | 0.2 | 0.6 | 24.9 |
| kv_aware | 733 | 3587 | 6647 | 6553 | 89.8 | 5.01 | 617 | 0.2 | 0.3 | 26.0 |
| prefix_aware | 460 | 3040 | 5920 | 5413 | 90.5 | 5.01 | 617 | 0.1 | 0.3 | 45.5 |
| llmd_style | 433 | 3060 | 5680 | 5400 | 91.0 | 5.01 | 618 | 0.1 | 0.1 | 47.3 |
| dynamo_style | 573 | 2727 | 5073 | 5187 | 92.5 | 5.00 | 616 | 0.4 | 1.0 | 38.0 |
| pack | 447 | 2680 | 4767 | 4633 | 93.0 | 5.01 | 617 | 0.2 | 0.6 | 45.5 |

Each row is the mean over 3 seeds.
