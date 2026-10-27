#!/usr/bin/env bash
# Compares every scheduling policy on the identical workload against 8
# identical simulated replicas plus a cloud provider. Writes bench/results.md.
#
#   BENCH_DURATION=60s BENCH_RPS=10 bench/run.sh
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"

build
start_sims
cfg=$(write_config)
for p in $POLICIES; do
  echo "== $p"
  run_policy "$p" "$cfg" homogeneous
done
write_table homogeneous "$BENCH_DIR/results.md"
