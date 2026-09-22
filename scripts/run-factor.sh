#!/usr/bin/env bash
# -----------------------------------------------------------------------------
# Factor experiment runner: generate → validate → benchmark → report for one layout.
#
# Usage:
#   ./scripts/run-factor.sh --layout=b0
#   LAYOUT=d1 TARGET_SIZE_TB=0.02 CACHE_STATE=cold ./scripts/run-factor.sh
#
# Known layouts (see --help): b0, d0, d1, d2, e1, e2, e3, f0, f1, f2, f3,
#   g001, g005, h5k, h10k, h20k, h50k, i64, i128, i256, jsnappy, jzstd, jzlib,
#   k32, k256, k1024, best_orc
#
# Env: BASE, SEED, TARGET_SIZE_TB, BENCHMARK_WARMUP_RUNS, BENCHMARK_REPEAT_RUNS,
#      CACHE_STATE, ENGINE, SLA_THRESHOLD_MS, ARCHIVE_SLA_THRESHOLD_MS, SCENARIOS,
#      SKIP_GENERATE, SKIP_VALIDATE
#
# Dataset sizes (this cluster): Smoke 0.01 (~10GB) | S 0.02 (~20GB) | M 0.05 (~50GB) | L 0.1 (~100GB max).
# HDFS free ≈ 700 GB — do not keep many layouts at once.
# Default SCENARIOS=audei for SLA-oriented factor runs; use SCENARIOS=doc for legacy Q1–Q10.
# -----------------------------------------------------------------------------
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
BASE="${BASE:-hdfs:///user/hdfs_migration_user/orc_test}"
SEED="${SEED:-42}"
TARGET_SIZE_TB="${TARGET_SIZE_TB:-0.02}"
BENCHMARK_WARMUP_RUNS="${BENCHMARK_WARMUP_RUNS:-3}"
BENCHMARK_REPEAT_RUNS="${BENCHMARK_REPEAT_RUNS:-5}"
CACHE_STATE="${CACHE_STATE:-cold}"
ENGINE="${ENGINE:-spark}"
SLA_THRESHOLD_MS="${SLA_THRESHOLD_MS:-3000}"
ARCHIVE_SLA_THRESHOLD_MS="${ARCHIVE_SLA_THRESHOLD_MS:-120000}"
SCENARIOS="${SCENARIOS:-audei}"
SKIP_GENERATE="${SKIP_GENERATE:-0}"
SKIP_VALIDATE="${SKIP_VALIDATE:-0}"
CLEAR_CACHE="${CLEAR_CACHE:-true}"

LAYOUT=""
EXTRA_APP_ARGS=()

usage() {
  sed -n '2,22p' "$0" | sed 's/^# \{0,1\}//'
  exit 0
}

for arg in "$@"; do
  case "$arg" in
    --help|-h) usage ;;
    --layout=*) LAYOUT="${arg#*=}" ;;
    --skip-generate) SKIP_GENERATE=1 ;;
    --skip-validate) SKIP_VALIDATE=1 ;;
    --*) EXTRA_APP_ARGS+=("$arg") ;;
    *)
      if [[ -z "$LAYOUT" ]]; then LAYOUT="$arg"; else EXTRA_APP_ARGS+=("$arg"); fi
      ;;
  esac
done

LAYOUT="${LAYOUT:-${LAYOUT_ID:-b0}}"
LAYOUT="$(echo "$LAYOUT" | tr '[:upper:]' '[:lower:]')"

if awk "BEGIN { exit !($TARGET_SIZE_TB > 0.1) }"; then
  echo "Refusing TARGET_SIZE_TB=$TARGET_SIZE_TB (>0.1). Max dataset on this cluster is 100 GB." >&2
  exit 1
fi
# Defaults for B0 ORC baseline
PARTITION_BY="event_year,event_month,event_day"
BLOOM_COLUMNS="none"
BLOOM_FPP="0.05"
SORT_COLUMNS="none"
STRIDE="10000"
STRIPE_MB="64"
COMPRESSION="snappy"
TARGET_FILE_SIZE_MB="384"
SPARK_PUSHDOWN="true"
SPARK_VECTORIZED="true"
SPARK_AQE="true"
SPARK_DPP="true"
SPARK_CBO="true"

apply_layout() {
  case "$1" in
    b0)
      BLOOM_COLUMNS="none"
      SORT_COLUMNS="none"
      ;;
    d0)
      PARTITION_BY="none"
      BLOOM_COLUMNS="none"
      ;;
    d1)
      PARTITION_BY="event_year,event_month,event_day"
      BLOOM_COLUMNS="none"
      ;;
    d2)
      PARTITION_BY="event_year,event_month,event_day,event_hour"
      BLOOM_COLUMNS="none"
      ;;
    e1)
      SORT_COLUMNS="epk_id"
      BLOOM_COLUMNS="none"
      ;;
    e2)
      SORT_COLUMNS="event_id"
      BLOOM_COLUMNS="none"
      ;;
    e3)
      SORT_COLUMNS="event_ts,epk_id"
      BLOOM_COLUMNS="none"
      ;;
    f0)
      BLOOM_COLUMNS="none"
      ;;
    f1)
      BLOOM_COLUMNS="epk_id,event_id"
      ;;
    f2)
      BLOOM_COLUMNS="module,name"
      ;;
    f3)
      BLOOM_COLUMNS="epk_id,event_id,module,name,channel_type,state"
      ;;
    g005)
      BLOOM_COLUMNS="epk_id,event_id"
      BLOOM_FPP="0.05"
      ;;
    g001)
      BLOOM_COLUMNS="epk_id,event_id"
      BLOOM_FPP="0.01"
      ;;
    g0001)
      BLOOM_COLUMNS="epk_id,event_id"
      BLOOM_FPP="0.001"
      ;;
    h5k)  STRIDE="5000" ;;
    h10k) STRIDE="10000" ;;
    h20k) STRIDE="20000" ;;
    h50k) STRIDE="50000" ;;
    i64)  STRIPE_MB="64" ;;
    i128) STRIPE_MB="128" ;;
    i256) STRIPE_MB="256" ;;
    jsnappy) COMPRESSION="snappy" ;;
    jzstd)   COMPRESSION="zstd" ;;
    jzlib)   COMPRESSION="zlib" ;;
    k32)   TARGET_FILE_SIZE_MB="32" ;;
    k256)  TARGET_FILE_SIZE_MB="256" ;;
    k1024) TARGET_FILE_SIZE_MB="1024" ;;
    best_orc)
      # Placeholder profile; override via EXTRA_APP_ARGS after S-scale winners.
      PARTITION_BY="event_year,event_month,event_day"
      SORT_COLUMNS="epk_id"
      BLOOM_COLUMNS="epk_id"
      BLOOM_FPP="0.01"
      STRIDE="10000"
      STRIPE_MB="64"
      COMPRESSION="snappy"
      TARGET_FILE_SIZE_MB="384"
      ;;
    s0)
      # Spark baseline exec toggles on existing BEST_ORC data
      SKIP_GENERATE=1
      SPARK_PUSHDOWN="false"
      SPARK_VECTORIZED="false"
      SPARK_AQE="false"
      SPARK_DPP="false"
      SPARK_CBO="false"
      ;;
    s1)
      SKIP_GENERATE=1
      SPARK_PUSHDOWN="true"
      SPARK_VECTORIZED="true"
      SPARK_AQE="true"
      SPARK_DPP="true"
      SPARK_CBO="true"
      ;;
    *)
      echo "Unknown layout: $1" >&2
      exit 1
      ;;
  esac
}

apply_layout "$LAYOUT"

# s0/s1 re-use best_orc files but label results separately
ORC_LAYOUT="$LAYOUT"
if [[ "$LAYOUT" == "s0" || "$LAYOUT" == "s1" ]]; then
  ORC_LAYOUT="best_orc"
fi

echo "Factor layout=$LAYOUT orc_layout=$ORC_LAYOUT BASE=$BASE TARGET_SIZE_TB=$TARGET_SIZE_TB CACHE_STATE=$CACHE_STATE SCENARIOS=$SCENARIOS"

submit() {
  "$ROOT/scripts/submit-spark32.sh" -- "$@"
}

COMMON=(
  --base-path="$BASE"
  --layout-id="$ORC_LAYOUT"
  --seed="$SEED"
  --target-size-tb="$TARGET_SIZE_TB"
  --engine="$ENGINE"
  --cache-state="$CACHE_STATE"
  --sla-threshold-ms="$SLA_THRESHOLD_MS"
  --archive-sla-threshold-ms="$ARCHIVE_SLA_THRESHOLD_MS"
  --partition-by="$PARTITION_BY"
  --orc-bloom-filter-columns="$BLOOM_COLUMNS"
  --orc-bloom-filter-fpp="$BLOOM_FPP"
  --orc-sort-columns="$SORT_COLUMNS"
  --orc-row-index-stride="$STRIDE"
  --orc-stripe-size-mb="$STRIPE_MB"
  --orc-compression="$COMPRESSION"
  --target-file-size-mb="$TARGET_FILE_SIZE_MB"
  --spark-orc-filter-pushdown="$SPARK_PUSHDOWN"
  --spark-orc-vectorized="$SPARK_VECTORIZED"
  --spark-aqe="$SPARK_AQE"
  --spark-dpp="$SPARK_DPP"
  --spark-cbo="$SPARK_CBO"
  --benchmark-dataset-label="$LAYOUT"
)

if [[ "$SKIP_GENERATE" != "1" ]]; then
  submit --mode=generate "${COMMON[@]}" "${EXTRA_APP_ARGS[@]+"${EXTRA_APP_ARGS[@]}"}" \
    2>&1 | tee "factor-${LAYOUT}-generate.log"
fi

if [[ "$SKIP_VALIDATE" != "1" ]]; then
  submit --mode=validate "${COMMON[@]}" "${EXTRA_APP_ARGS[@]+"${EXTRA_APP_ARGS[@]}"}" \
    2>&1 | tee "factor-${LAYOUT}-validate.log"
fi

# For s0/s1, still tag layout_id in metrics as s0/s1 while reading best_orc paths.
BENCH_LAYOUT_ARGS=("${COMMON[@]}")
if [[ "$LAYOUT" == "s0" || "$LAYOUT" == "s1" ]]; then
  BENCH_LAYOUT_ARGS=(
    --base-path="$BASE"
    --layout-id="best_orc"
    --seed="$SEED"
    --target-size-tb="$TARGET_SIZE_TB"
    --engine="$ENGINE"
    --cache-state="$CACHE_STATE"
    --sla-threshold-ms="$SLA_THRESHOLD_MS"
    --archive-sla-threshold-ms="$ARCHIVE_SLA_THRESHOLD_MS"
    --benchmark-dataset-label="$LAYOUT"
    --spark-orc-filter-pushdown="$SPARK_PUSHDOWN"
    --spark-orc-vectorized="$SPARK_VECTORIZED"
    --spark-aqe="$SPARK_AQE"
    --spark-dpp="$SPARK_DPP"
    --spark-cbo="$SPARK_CBO"
    --orc-bloom-filter-columns="$BLOOM_COLUMNS"
  )
fi

submit \
  --mode=benchmark \
  "${BENCH_LAYOUT_ARGS[@]}" \
  --benchmark-scenarios="$SCENARIOS" \
  --benchmark-warmup-runs="$BENCHMARK_WARMUP_RUNS" \
  --benchmark-repeat-runs="$BENCHMARK_REPEAT_RUNS" \
  --benchmark-timestamp-window-days=30 \
  --clear-cache-between-runs="$CLEAR_CACHE" \
  "${EXTRA_APP_ARGS[@]+"${EXTRA_APP_ARGS[@]}"}" \
  2>&1 | tee "factor-${LAYOUT}-benchmark-${CACHE_STATE}.log"

submit \
  --mode=report \
  --base-path="$BASE" \
  --layout-id="$ORC_LAYOUT" \
  --report-name="factor-${LAYOUT}-${CACHE_STATE}" \
  2>&1 | tee "factor-${LAYOUT}-report.log"

echo "Factor $LAYOUT done. Summary under $BASE/layouts/$ORC_LAYOUT/reports/summary/"
