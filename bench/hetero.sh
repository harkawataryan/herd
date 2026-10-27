#!/usr/bin/env bash
# Same comparison with two of the eight replicas at half capacity (half the
# KV cache, half the prefill and decode rates). Static balancing keeps
# sending them an equal share; live signals do not. Writes bench/results-hetero.md.
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"

build
start_sims hetero
cfg=$(write_config hetero)
for p in $POLICIES; do
  echo "== $p (heterogeneous)"
  run_policy "$p" "$cfg" hetero
done
write_table hetero "$BENCH_DIR/results-hetero.md"
