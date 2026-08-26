#!/usr/bin/env bash
# -----------------------------------------------------------------------------
# Короткий smoke на edge: generate → validate → benchmark → report (Spark 3.2 ORC).
#
# Запуск:
#   ./scripts/run-smoke.sh
#   BASE=hdfs:///user/hdfs_migration_user/orc_test TARGET_SIZE_TB=0.01 ./scripts/run-smoke.sh
#
# Переменные окружения:
#   BASE                         корневой HDFS-путь эксперимента
#                                [hdfs:///user/hdfs_migration_user/orc_test]
#   TARGET_SIZE_TB               объём generate в ТБ [0.01]
#   SEED                         seed генератора и фильтров [42]
#   BENCHMARK_REPEAT_RUNS        измеряемые повторы [3]
#   BENCHMARK_WARMUP_RUNS        прогрев [1]
#   BENCHMARK_TIMESTAMP_WINDOW_DAYS  окно filter_timestamp_range в днях [30]
#
# Логи шагов: smoke-generate.log, smoke-validate.log, smoke-benchmark.log, smoke-report.log
#
# Carbon A/B в этом репозитории не поддерживается (out of scope).
# Validation пишется в reports/raw/validation/, benchmark — в reports/raw/benchmark/
# (раньше overwrite benchmark мог стереть validation).
# -----------------------------------------------------------------------------
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
BASE="${BASE:-hdfs:///user/hdfs_migration_user/orc_test}"
TARGET_SIZE_TB="${TARGET_SIZE_TB:-0.01}"
SEED="${SEED:-42}"
BENCHMARK_REPEAT_RUNS="${BENCHMARK_REPEAT_RUNS:-3}"
BENCHMARK_WARMUP_RUNS="${BENCHMARK_WARMUP_RUNS:-1}"
BENCHMARK_TIMESTAMP_WINDOW_DAYS="${BENCHMARK_TIMESTAMP_WINDOW_DAYS:-30}"

echo "Using BASE=$BASE TARGET_SIZE_TB=$TARGET_SIZE_TB SEED=$SEED"
echo "Benchmark repeats=$BENCHMARK_REPEAT_RUNS warmup=$BENCHMARK_WARMUP_RUNS timestampWindowDays=$BENCHMARK_TIMESTAMP_WINDOW_DAYS"

"$ROOT/scripts/submit-spark32.sh" -- \
  --mode=generate \
  --base-path="$BASE" \
  --target-size-tb="$TARGET_SIZE_TB" \
  --seed="$SEED" \
  2>&1 | tee smoke-generate.log

"$ROOT/scripts/submit-spark32.sh" -- \
  --mode=validate \
  --base-path="$BASE" \
  --seed="$SEED" \
  2>&1 | tee smoke-validate.log

"$ROOT/scripts/submit-spark32.sh" -- \
  --mode=benchmark \
  --base-path="$BASE" \
  --seed="$SEED" \
  --benchmark-scenarios=all \
  --benchmark-warmup-runs="$BENCHMARK_WARMUP_RUNS" \
  --benchmark-repeat-runs="$BENCHMARK_REPEAT_RUNS" \
  --benchmark-timestamp-window-days="$BENCHMARK_TIMESTAMP_WINDOW_DAYS" \
  2>&1 | tee smoke-benchmark.log

"$ROOT/scripts/submit-spark32.sh" -- \
  --mode=report \
  --base-path="$BASE" \
  2>&1 | tee smoke-report.log

echo "Smoke pipeline submitted. Check YARN for SUCCEEDED and $BASE/reports/summary/"
echo "Expect Validation section populated and avg_bytes_read in Benchmark Summary."
