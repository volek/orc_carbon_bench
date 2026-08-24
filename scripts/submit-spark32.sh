#!/usr/bin/env bash
# Submit ORC-only jobs with the cluster Spark 3.2. Do not use this script for CarbonData.
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
