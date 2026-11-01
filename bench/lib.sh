#!/usr/bin/env bash
# Shared helpers for the benchmark scripts. Sourced, not executed.
set -euo pipefail

BENCH_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$BENCH_DIR/.." && pwd)"
BIN="$BENCH_DIR/bin"
RESULTS="${BENCH_RESULTS:-$BENCH_DIR/results}"

# Knobs (override with env vars).
BENCH_RPS="${BENCH_RPS:-7}"             # simulated requests/second, ~85% of measured capacity
BENCH_DURATION="${BENCH_DURATION:-120s}" # simulated seconds of traffic per policy
BENCH_SEED="${BENCH_SEED:-1}"
TIME_SCALE="${TIME_SCALE:-0.05}"
# Default workload: shared-context chat, i.e. a pool of large system prompts
# (RAG context, few-shot examples) with short user turns, the shape where
# prefix caching and KV pressure both matter. Override with LOADGEN_ARGS.
LOADGEN_ARGS="${LOADGEN_ARGS:--system-chars 20000 -system-prompts 20 -mix short=0.7,rag=0.3,long=0}"         # sims and loadgen run 20x faster than real time
GATEWAY_PORT="${GATEWAY_PORT:-18080}"
SIM_BASE="${SIM_BASE:-19000}"       # replicas listen on SIM_BASE+1 .. SIM_BASE+8
CLOUD_PORT="${CLOUD_PORT:-19101}"
POLICIES="${POLICIES:-round_robin least_queue kv_aware prefix_aware}"
REPLICAS=8

PIDS=()
cleanup() {
  for pid in "${PIDS[@]:-}"; do kill "$pid" 2>/dev/null || true; done
  wait 2>/dev/null || true
}
trap cleanup EXIT INT TERM

build() {
  mkdir -p "$BIN" "$RESULTS"
  (cd "$ROOT" && go build -o "$BIN/gateway" ./cmd/gateway && go build -o "$BIN/simbackend" ./cmd/simbackend && go build -o "$BIN/loadgen" ./cmd/loadgen)
}

# Ports must be free: a stray gateway or sim on them would silently answer
# the health checks and the benchmark would measure the wrong thing.
assert_free() { # port
  if curl -s --max-time 1 "http://127.0.0.1:$1/healthz" >/dev/null 2>&1; then
    echo "port $1 is already serving something; set GATEWAY_PORT/SIM_BASE/CLOUD_PORT to free ports" >&2
    exit 1
  fi
}

wait_for() { # url
  for _ in $(seq 1 100); do
    if curl -sf "$1" >/dev/null 2>&1; then return 0; fi
    sleep 0.1
  done
  echo "timed out waiting for $1" >&2
  return 1
}

# start_sims [hetero]: 8 local replicas on 9001-9008 plus a cloud-mode
# provider on 9101. With "hetero", replicas 7 and 8 have half the capacity.
start_sims() {
  local hetero="${1:-}"
  assert_free "$GATEWAY_PORT"; assert_free "$CLOUD_PORT"
  for i in $(seq 1 $REPLICAS); do assert_free $((SIM_BASE + i)); done
  for i in $(seq 1 $REPLICAS); do
    local kv=32768 prefill=4000 decode=40
    if [[ "$hetero" == "hetero" && $i -ge 7 ]]; then kv=16384; prefill=2000; decode=20; fi
    "$BIN/simbackend" -addr "127.0.0.1:$((SIM_BASE + i))" -name "sim-$i" -kv-capacity $kv -prefill-tps $prefill -decode-tps $decode \
      -time-scale "$TIME_SCALE" -seed $i >"$RESULTS/sim-$i.log" 2>&1 &
    PIDS+=($!)
  done
  "$BIN/simbackend" -addr "127.0.0.1:$CLOUD_PORT" -name cloud -mode cloud -models gpt-4o-mini -time-scale "$TIME_SCALE" -decode-tps 60 >"$RESULTS/cloud.log" 2>&1 &
  PIDS+=($!)
  for i in $(seq 1 $REPLICAS); do wait_for "http://127.0.0.1:$((SIM_BASE + i))/healthz"; done
  wait_for "http://127.0.0.1:$CLOUD_PORT/healthz"
}

# write_config [hetero]: the gateway config for the bench topology. The
# gateway's kv_capacity must match the sims so its KV estimate is calibrated.
write_config() {
  local hetero="${1:-}" cfg="$BENCH_DIR/herd.bench.yaml"
  {
    echo "listen: \":$GATEWAY_PORT\""
    echo "policy: prefix_aware"
    # 50 ms wall = 1 simulated second at time-scale 0.05: fresh enough for routing.
    echo "scrape_interval_ms: 50"
    echo "saturation_threshold: 0.9"
    echo "default_max_tokens: 128"
    echo "backends:"
    for i in $(seq 1 $REPLICAS); do
      local kv=32768
      if [[ "$hetero" == "hetero" && $i -ge 7 ]]; then kv=16384; fi
      printf '  - {name: sim-%d, url: http://127.0.0.1:%d, models: [llama-3-8b], kv_capacity: %d, max_batch: 16}\n' $i $((SIM_BASE + i)) $kv
    done
    cat <<EOF
providers:
  - name: cloud
    url: http://127.0.0.1:$CLOUD_PORT
    models: [gpt-4o-mini]
    model_map: {llama-3-8b: gpt-4o-mini}
    price_in_per_m: 0.15
    price_out_per_m: 0.60
tenants:
  - {name: alpha, api_key: alpha-key, weight: 3, budget_usd: 2.0}
  - {name: beta,  api_key: beta-key,  weight: 2, budget_usd: 1.0}
  - {name: gamma, api_key: gamma-key, weight: 1, budget_usd: 0.0}
EOF
  } >"$cfg"
  echo "$cfg"
}

# run_policy <policy> <config> <tag>: start a gateway, replay the workload, stop it.
run_policy() {
  local policy="$1" cfg="$2" tag="$3"
  "$BIN/gateway" -config "$cfg" -policy "$policy" -listen "127.0.0.1:$GATEWAY_PORT" >"$RESULTS/gateway-$tag-$policy.log" 2>&1 &
  local gw=$!
  wait_for "http://127.0.0.1:$GATEWAY_PORT/healthz"
  # Let the first scrape land so load-aware policies start with real gauges.
  sleep 0.3
  "$BIN/loadgen" -url "http://127.0.0.1:$GATEWAY_PORT" -rps "$BENCH_RPS" -duration "$BENCH_DURATION" -seed "$BENCH_SEED" \
    -time-scale "$TIME_SCALE" -policy-label "$policy" -out "$RESULTS/$tag-$policy.json" $LOADGEN_ARGS \
    2>"$RESULTS/loadgen-$tag-$policy.log" | tee "$RESULTS/$tag-$policy.md"
  kill $gw 2>/dev/null || true
  wait $gw 2>/dev/null || true
}

# write_table <tag> <outfile>: one row per policy from the saved markdown rows.
write_table() {
  local tag="$1" out="$2"
  {
    echo "# Herd benchmark ($tag)"
    echo
    echo "Workload: rps=$BENCH_RPS duration=$BENCH_DURATION seed=$BENCH_SEED time_scale=$TIME_SCALE replicas=$REPLICAS loadgen_args='$LOADGEN_ARGS'. Latencies are in simulated milliseconds. Generated $(date -u +%Y-%m-%dT%H:%M:%SZ)."
    echo
    local first=1
    for p in $POLICIES; do
      if [[ $first -eq 1 ]]; then head -2 "$RESULTS/$tag-$p.md"; first=0; fi
      tail -1 "$RESULTS/$tag-$p.md"
    done
  } >"$out"
  echo
  cat "$out"
}
