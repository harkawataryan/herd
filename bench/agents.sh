#!/usr/bin/env bash
# The cache-pressure comparison: agent sessions (prompts that grow turn by
# turn, with think time between turns) and one-off RAG requests sharing the
# same replicas, with a KV cache small enough that something has to be
# evicted. This is where a router that knows what it evicts should win.
# Writes bench/results-agents.md.
#
#   BENCH_RPS=5 bench/agents.sh
# Arrivals per second; 40% of them start a ~6-turn session, so the request
# rate is about 2.8x this. Replicas get half the usual KV cache. Agents
# think for 12 s on average between turns (a tool call, a person typing),
# which is longer than the cache takes to turn over at this load.
export BENCH_RPS="${BENCH_RPS:-1.8}"
export BENCH_DURATION="${BENCH_DURATION:-180s}"
export SIM_KV="${SIM_KV:-16384}"
export LOADGEN_ARGS="${AGENT_LOADGEN_ARGS:--system-chars 6000 -system-prompts 16 -mix short=0.2,rag=0.4,agent=0.4 -agent-turns 6 -agent-gap 12s -agent-tool-chars 2000}"
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"

build
start_sims
cfg=$(write_config)
for p in $POLICIES; do
  echo "== $p (agents)"
  run_policy "$p" "$cfg" agents
done
write_table agents "$BENCH_DIR/results-agents.md"
