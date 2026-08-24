#!/usr/bin/env bash
# Smoke pipeline on the edge node: Spark 3.1.1 Carbon+ORC, then Spark 3.2 ORC reference, then report.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
BASE="${BASE:-hdfs://dev1-abyss-sdp2-ambari-02.opsmon.sbt:50470/bench/orc-carbon}"
TARGET_SIZE_TB="${TARGET_SIZE_TB:-0.01}"
SEED="${SEED:-42}"

echo "Using BASE=$BASE TARGET_SIZE_TB=$TARGET_SIZE_TB"

"$ROOT/scripts/submit-spark31.sh" -- \
  --mode=generate \
  --base-path="$BASE" \
  --target-size-tb="$TARGET_SIZE_TB" \
  --seed="$SEED" \
  --output-formats=orc,carbon \
  --enable-bloom-index=true \
  --enable-lucene-index=true \
  2>&1 | tee smoke-generate.log

"$ROOT/scripts/submit-spark31.sh" -- \
  --mode=validate \
  --base-path="$BASE" \
  --seed="$SEED" \
  2>&1 | tee smoke-validate.log

"$ROOT/scripts/submit-spark31.sh" -- \
  --mode=benchmark \
  --base-path="$BASE" \
  --seed="$SEED" \
  --benchmark-scenarios=all \
  --benchmark-warmup-runs=1 \
  --benchmark-repeat-runs=1 \
  2>&1 | tee smoke-benchmark.log

"$ROOT/scripts/submit-spark31.sh" -- \
  --mode=index-experiment \
  --base-path="$BASE" \
  --seed="$SEED" \
  --index-profiles=baseline,bloom,lucene,bloom_lucene \
  --rebuild-indexes=true \
  --benchmark-warmup-runs=1 \
  --benchmark-repeat-runs=1 \
  2>&1 | tee smoke-index.log

"$ROOT/scripts/submit-spark32.sh" -- \
  --mode=benchmark \
  --base-path="$BASE" \
  --formats=orc \
  --seed="$SEED" \
  --benchmark-scenarios=all \
  --benchmark-warmup-runs=1 \
  --benchmark-repeat-runs=1 \
  2>&1 | tee smoke-benchmark-spark32.log

"$ROOT/scripts/submit-spark31.sh" -- \
  --mode=report \
  --base-path="$BASE" \
  2>&1 | tee smoke-report.log

echo "Smoke pipeline submitted. Check YARN for SUCCEEDED and $BASE/reports/summary/"
