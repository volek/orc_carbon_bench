#!/usr/bin/env bash
# -----------------------------------------------------------------------------
# Concurrency / QPS helper for AUDEI interactive path (epk_eq_14d).
#
# Defaults target AUDEI load profile: ~9 TPS / 18 QPS (2× headroom).
# Each "client" is one Spark (or Hive) benchmark job of a single scenario.
#
#   ENGINE=spark ./scripts/run-concurrency.sh
#   CONCURRENCY_LEVELS="9 18" SCENARIO=epk_eq_14d ./scripts/run-concurrency.sh
#   ENGINE=hive_llap HIVE_PROFILE=h4 CONCURRENCY_LEVELS="9 18" ./scripts/run-concurrency.sh
#
# Legacy wide fan-out: CONCURRENCY_LEVELS="1 5 10 25 50"
# -----------------------------------------------------------------------------
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
BASE="${BASE:-hdfs:///user/hdfs_migration_user/orc_test}"
LAYOUT="${LAYOUT:-best_orc}"
ENGINE="${ENGINE:-spark}"
SCENARIO="${SCENARIO:-epk_eq_14d}"
CONCURRENCY_LEVELS="${CONCURRENCY_LEVELS:-9 18}"
CACHE_STATE="${CACHE_STATE:-warm}"
TARGET_SIZE_TB="${TARGET_SIZE_TB:-0.02}"
SLA_THRESHOLD_MS="${SLA_THRESHOLD_MS:-3000}"
ARCHIVE_SLA_THRESHOLD_MS="${ARCHIVE_SLA_THRESHOLD_MS:-120000}"
RESULT_DIR="${RESULT_DIR:-$ROOT/result/concurrency}"
mkdir -p "$RESULT_DIR"

summary_csv="$RESULT_DIR/concurrency-${ENGINE}.csv"
echo "engine,concurrency,scenario,avg_ms,p50_ms,p95_ms,p99_ms,sla_success" > "$summary_csv"

run_spark_client() {
  local out="$1"
  "$ROOT/scripts/submit-spark32.sh" -- \
    --mode=benchmark \
    --base-path="$BASE" \
    --layout-id="$LAYOUT" \
    --engine=spark \
    --cache-state="$CACHE_STATE" \
    --sla-threshold-ms="$SLA_THRESHOLD_MS" \
    --archive-sla-threshold-ms="$ARCHIVE_SLA_THRESHOLD_MS" \
    --target-size-tb="$TARGET_SIZE_TB" \
    --benchmark-scenarios="$SCENARIO" \
    --benchmark-warmup-runs=0 \
    --benchmark-repeat-runs=1 \
    --clear-cache-between-runs=false \
    --benchmark-dataset-label="conc" \
    >"$out" 2>&1 || true
}

run_hive_client() {
  local out="$1"
  PROFILE="${HIVE_PROFILE:-h4}" \
    BASE="$BASE" LAYOUT="$LAYOUT" SUITE=audei \
    BENCHMARK_WARMUP_RUNS=0 BENCHMARK_REPEAT_RUNS=1 \
    "$ROOT/scripts/hive/run-hive-factor.sh" "$PROFILE" >"$out" 2>&1 || true
}

for conc in $CONCURRENCY_LEVELS; do
  echo "=== concurrency=$conc engine=$ENGINE scenario=$SCENARIO ==="
  pids=()
  outs=()
  for ((i = 0; i < conc; i++)); do
    out="$RESULT_DIR/${ENGINE}-c${conc}-w${i}.log"
    outs+=("$out")
    if [[ "$ENGINE" == "spark" ]]; then
      run_spark_client "$out" &
    else
      run_hive_client "$out" &
    fi
    pids+=("$!")
  done
  for pid in "${pids[@]}"; do
    wait "$pid" || true
  done

  durations_file="$RESULT_DIR/${ENGINE}-c${conc}-durations.txt"
  : > "$durations_file"
  for out in "${outs[@]}"; do
    grep -oE 'durationMs=[0-9]+' "$out" 2>/dev/null | head -1 | cut -d= -f2 >> "$durations_file" || true
    grep -oE ',[0-9]+,' "$out" 2>/dev/null | head -1 | tr -d ',' >> "$durations_file" || true
  done

  if [[ ! -s "$durations_file" ]]; then
    echo "$ENGINE,$conc,$SCENARIO,,,,, " >> "$summary_csv"
    continue
  fi

  python3 - "$durations_file" "$ENGINE" "$conc" "$SCENARIO" "$summary_csv" "$SLA_THRESHOLD_MS" <<'PY'
import sys, statistics
path, engine, conc, scenario, summary, sla = sys.argv[1:7]
sla = int(sla)
vals = []
with open(path) as f:
    for line in f:
        line=line.strip()
        if line.isdigit():
            vals.append(int(line))
if not vals:
    open(summary,'a').write(f"{engine},{conc},{scenario},,,,,\n")
    sys.exit(0)
vals.sort()
def pct(p):
    if not vals: return ''
    k = min(len(vals)-1, max(0, int(round(p*(len(vals)-1)))))
    return vals[k]
avg = statistics.mean(vals)
p50, p95, p99 = pct(0.50), pct(0.95), pct(0.99)
sla_ok = sum(1 for v in vals if v <= sla) / len(vals)
open(summary,'a').write(f"{engine},{conc},{scenario},{avg:.2f},{p50},{p95},{p99},{sla_ok:.4f}\n")
PY
done

echo "Concurrency summary: $summary_csv"
echo "AUDEI targets: ~9 TPS / 18 QPS on scenario=$SCENARIO (interactive SLA ${SLA_THRESHOLD_MS} ms)."
