#!/usr/bin/env bash
# -----------------------------------------------------------------------------
# Короткий smoke на edge: generate → validate → benchmark → report (Spark 3.2 ORC).
#
# Запуск:
#   ./scripts/run-smoke.sh
#   BASE=hdfs:///user/hdfs_migration_user/orc_test TARGET_SIZE_TB=0.01 ./scripts/run-smoke.sh
#
# Переменные окружения:
#   BASE            корневой HDFS-путь эксперимента
#                   [hdfs:///user/hdfs_migration_user/orc_test]
#   TARGET_SIZE_TB  объём generate в ТБ [0.01]
#   SEED            seed генератора [42]
#
# Логи шагов пишутся в текущий каталог: smoke-generate.log, smoke-validate.log, ...
# -----------------------------------------------------------------------------
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
BASE="${BASE:-hdfs:///user/hdfs_migration_user/orc_test}"
TARGET_SIZE_TB="${TARGET_SIZE_TB:-0.01}"
SEED="${SEED:-42}"

echo "Using BASE=$BASE TARGET_SIZE_TB=$TARGET_SIZE_TB"

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
  --benchmark-warmup-runs=1 \
  --benchmark-repeat-runs=1 \
  2>&1 | tee smoke-benchmark.log

"$ROOT/scripts/submit-spark32.sh" -- \
  --mode=report \
  --base-path="$BASE" \
  2>&1 | tee smoke-report.log

echo "Smoke pipeline submitted. Check YARN for SUCCEEDED and $BASE/reports/summary/"
