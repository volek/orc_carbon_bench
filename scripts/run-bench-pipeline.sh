#!/usr/bin/env bash
# -----------------------------------------------------------------------------
# Повторный прогон без generate: validate → benchmark (3+ runs) → report.
# Используйте, когда ORC уже лежит в $BASE/orc (после успешного generate).
#
#   ./scripts/run-bench-pipeline.sh
#   BENCHMARK_REPEAT_RUNS=5 ./scripts/run-bench-pipeline.sh
#
# Переменные: BASE, SEED, BENCHMARK_REPEAT_RUNS, BENCHMARK_WARMUP_RUNS,
#             BENCHMARK_TIMESTAMP_WINDOW_DAYS, BENCHMARK_SCENARIOS
# -----------------------------------------------------------------------------
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
BASE="${BASE:-hdfs:///user/hdfs_migration_user/orc_test}"
SEED="${SEED:-42}"
BENCHMARK_REPEAT_RUNS="${BENCHMARK_REPEAT_RUNS:-3}"
BENCHMARK_WARMUP_RUNS="${BENCHMARK_WARMUP_RUNS:-1}"
BENCHMARK_TIMESTAMP_WINDOW_DAYS="${BENCHMARK_TIMESTAMP_WINDOW_DAYS:-30}"
BENCHMARK_SCENARIOS="${BENCHMARK_SCENARIOS:-all}"

echo "Bench pipeline BASE=$BASE SEED=$SEED scenarios=$BENCHMARK_SCENARIOS"
echo "repeats=$BENCHMARK_REPEAT_RUNS warmup=$BENCHMARK_WARMUP_RUNS windowDays=$BENCHMARK_TIMESTAMP_WINDOW_DAYS"

"$ROOT/scripts/submit-spark32.sh" -- \
  --mode=validate \
  --base-path="$BASE" \
  --seed="$SEED" \
  2>&1 | tee bench-validate.log

"$ROOT/scripts/submit-spark32.sh" -- \
  --mode=benchmark \
  --base-path="$BASE" \
  --seed="$SEED" \
  --benchmark-scenarios="$BENCHMARK_SCENARIOS" \
  --benchmark-warmup-runs="$BENCHMARK_WARMUP_RUNS" \
  --benchmark-repeat-runs="$BENCHMARK_REPEAT_RUNS" \
  --benchmark-timestamp-window-days="$BENCHMARK_TIMESTAMP_WINDOW_DAYS" \
  --clear-cache-between-runs=true \
  2>&1 | tee bench-benchmark.log

"$ROOT/scripts/submit-spark32.sh" -- \
  --mode=report \
  --base-path="$BASE" \
  2>&1 | tee bench-report.log

echo "Done. Pull summary:"
echo "  hdfs dfs -get $BASE/reports/summary ./summary"
echo "Check: Validation PASS/FAIL, runs>=$BENCHMARK_REPEAT_RUNS, avg_bytes_read vs full_scan."
