#!/usr/bin/env bash
# -----------------------------------------------------------------------------
# Запуск ORC-референса через кластерный Spark 3.2 (YARN cluster).
# Только ORC: --mode=benchmark --formats=orc (и опционально generate ORC).
# Для CarbonData этот скрипт не использовать — нужен submit-spark31.sh.
#
# Запуск:
#   ./scripts/submit-spark32.sh -- --mode=benchmark --formats=orc --base-path="$BASE"
#   ./scripts/submit-spark32.sh --driver-memory 8g --num-executors 16 -- \
#       --mode=benchmark --formats=orc --base-path="$BASE" --seed=42
#
# Аргументы:
#   до "--"     флаги spark-submit (--driver-memory, --num-executors, ...)
#   после "--"  аргументы приложения (--mode=..., --base-path=..., ...)
#   без "--"    все аргументы считаются аргументами приложения
#
# Переменные окружения:
#   JAR32         fat JAR spark32 [build/libs/orc-carbon-bench-spark32-all.jar]
#   SPARK_SUBMIT  команда submit кластерного Spark 3.2 [spark-submit]
# -----------------------------------------------------------------------------
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
JAR="${JAR32:-$ROOT/build/libs/orc-carbon-bench-spark32-all.jar}"
SPARK_SUBMIT="${SPARK_SUBMIT:-spark-submit}"

if [[ ! -f "$JAR" ]]; then
  echo "Missing $JAR. Build with: ./gradlew :app-spark32:build" >&2
  exit 1
fi

SPARK_ARGS=()
APP_ARGS=()
seen_separator=0
has_separator=0
for arg in "$@"; do
  if [[ "$arg" == "--" ]]; then
    has_separator=1
    seen_separator=1
    continue
  fi
  if [[ $seen_separator -eq 1 ]]; then
    APP_ARGS+=("$arg")
  else
    SPARK_ARGS+=("$arg")
  fi
done
if [[ $has_separator -eq 0 ]]; then
  APP_ARGS=("${SPARK_ARGS[@]}")
  SPARK_ARGS=()
fi

exec "$SPARK_SUBMIT" \
  --master yarn \
  --deploy-mode cluster \
  "${SPARK_ARGS[@]}" \
  --class ru.sber.orcbench.AppMain \
  "$JAR" \
  "${APP_ARGS[@]}"
