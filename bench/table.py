#!/usr/bin/env python3
"""Averages the per-seed loadgen reports of each policy into one table row.

usage: table.py RESULTS_DIR TAG POLICY...
"""
import glob
import json
import statistics
import sys

results, tag, policies = sys.argv[1], sys.argv[2], sys.argv[3:]
cols = [
    ("p50 TTFT ms", lambda r: r["ttft_ms"]["p50"], "%.0f"),
    ("p95 TTFT ms", lambda r: r["ttft_ms"]["p95"], "%.0f"),
    ("p99 TTFT ms", lambda r: r["ttft_ms"]["p99"], "%.0f"),
    ("p99 agent TTFT ms", lambda r: r["p99_ttft_ms_by_class"].get("agent"), "%.0f"),
    ("goodput %", lambda r: r["goodput_pct"], "%.1f"),
    ("req/s", lambda r: r["req_per_sec"], "%.2f"),
    ("tokens/s", lambda r: r["tokens_per_sec"], "%.0f"),
    ("shed %", lambda r: r["shed_pct"], "%.1f"),
    ("spill %", lambda r: r["spill_pct"], "%.1f"),
    ("cache hit %", lambda r: r["hit_block_pct"], "%.1f"),
]
print("| policy | " + " | ".join(c[0] for c in cols) + " |")
print("|" + "---|" * (len(cols) + 1))
seeds = 0
for p in policies:
    files = [f for f in sorted(glob.glob(f"{results}/{tag}-{p}-[0-9]*.json")) if not f.endswith(".decisions.json")]
    reports = [json.load(open(f)) for f in files]
    if not reports:
        continue
    seeds = len(reports)
    cells = []
    for _, get, fmt in cols:
        vals = [v for v in (get(r) for r in reports) if v is not None]
        cells.append(fmt % statistics.mean(vals) if vals else "-")
    print(f"| {p} | " + " | ".join(cells) + " |")
if seeds > 1:
    print(f"\nEach row is the mean over {seeds} seeds.")
