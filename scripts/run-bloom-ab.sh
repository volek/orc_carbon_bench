#!/usr/bin/env bash
# -----------------------------------------------------------------------------
# Bloom A/B: generate nobloom + bloom datasets, validate, benchmark both, report.
#
#   ./scripts/run-bloom-ab.sh
#   TARGET_SIZE_TB=0.1 BASE=hdfs:///user/.../orc_test_pilot ./scripts/run-bloom-ab.sh
#
# Env: BASE, SEED, TARGET_SIZE_TB, BENCHMARK_REPEAT_RUNS, NUM_EXECUTORS, EXECUTOR_MEMORY
# -----------------------------------------------------------------------------
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
BASE="${BASE:-hdfs:///user/hdfs_migration_user/orc_test_pilot}"
SEED="${SEED:-42}"
TARGET_SIZE_TB="${TARGET_SIZE_TB:-0.1}"
BENCHMARK_REPEAT_RUNS="${BENCHMARK_REPEAT_RUNS:-5}"
BENCHMARK_WARMUP_RUNS="${BENCHMARK_WARMUP_RUNS:-1}"
BLOOM_COLUMNS="event_id,user_id,product_id,campaign_id"

ORC_NOBLOOM="$BASE/orc"
ORC_BLOOM="$BASE/orc_bloom"
REPORTS_BENCHMARK_NOBLOOM="$BASE/reports/raw/benchmark_nobloom"
REPORTS_BENCHMARK_BLOOM="$BASE/reports/raw/benchmark_bloom"
REPORTS_VALIDATION_NOBLOOM="$BASE/reports/raw/validation_nobloom"
REPORTS_VALIDATION_BLOOM="$BASE/reports/raw/validation_bloom"

echo "Bloom A/B BASE=$BASE TARGET_SIZE_TB=$TARGET_SIZE_TB SEED=$SEED repeats=$BENCHMARK_REPEAT_RUNS"

submit() {
  "$ROOT/scripts/submit-spark32.sh" -- "$@"
}

# 1. generate nobloom
submit \
  --mode=generate \
  --base-path="$BASE" \
  --orc-path="$ORC_NOBLOOM" \
  --target-size-tb="$TARGET_SIZE_TB" \
  --seed="$SEED" \
  --orc-bloom-filter-columns=none \
  2>&1 | tee bloom-ab-generate-nobloom.log

# 2. generate bloom
submit \
  --mode=generate \
  --base-path="$BASE" \
  --orc-path="$ORC_BLOOM" \
  --target-size-tb="$TARGET_SIZE_TB" \
  --seed="$SEED" \
  --orc-bloom-filter-columns="$BLOOM_COLUMNS" \
  2>&1 | tee bloom-ab-generate-bloom.log

# 3. validate nobloom
submit \
  --mode=validate \
  --base-path="$BASE" \
  --orc-path="$ORC_NOBLOOM" \
  --reports-validation-path="$REPORTS_VALIDATION_NOBLOOM" \
  --seed="$SEED" \
  --orc-bloom-filter-columns=none \
  2>&1 | tee bloom-ab-validate-nobloom.log

# 4. validate bloom
submit \
  --mode=validate \
  --base-path="$BASE" \
  --orc-path="$ORC_BLOOM" \
  --reports-validation-path="$REPORTS_VALIDATION_BLOOM" \
  --seed="$SEED" \
  --orc-bloom-filter-columns="$BLOOM_COLUMNS" \
  2>&1 | tee bloom-ab-validate-bloom.log

# 5. benchmark nobloom
submit \
  --mode=benchmark \
  --base-path="$BASE" \
  --orc-path="$ORC_NOBLOOM" \
  --reports-benchmark-path="$REPORTS_BENCHMARK_NOBLOOM" \
  --benchmark-dataset-label=nobloom \
  --seed="$SEED" \
  --benchmark-scenarios=all \
  --benchmark-warmup-runs="$BENCHMARK_WARMUP_RUNS" \
  --benchmark-repeat-runs="$BENCHMARK_REPEAT_RUNS" \
  --benchmark-timestamp-window-days=30 \
  --clear-cache-between-runs=true \
  2>&1 | tee bloom-ab-benchmark-nobloom.log

# 6. benchmark bloom
submit \
  --mode=benchmark \
  --base-path="$BASE" \
  --orc-path="$ORC_BLOOM" \
  --reports-benchmark-path="$REPORTS_BENCHMARK_BLOOM" \
  --benchmark-dataset-label=bloom \
  --seed="$SEED" \
  --benchmark-scenarios=all \
  --benchmark-warmup-runs="$BENCHMARK_WARMUP_RUNS" \
  --benchmark-repeat-runs="$BENCHMARK_REPEAT_RUNS" \
  --benchmark-timestamp-window-days=30 \
  --clear-cache-between-runs=true \
  2>&1 | tee bloom-ab-benchmark-bloom.log

# 7. combined report
submit \
  --mode=report \
  --base-path="$BASE" \
  --report-name=bloom-ab-report \
  2>&1 | tee bloom-ab-report.log

echo "Bloom A/B submitted. Expect sections Benchmark Summary + Bloom filter comparison in $BASE/reports/summary/bloom-ab-report.md"
