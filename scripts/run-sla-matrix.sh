#!/usr/bin/env bash
# -----------------------------------------------------------------------------
# Final SLA matrix report helper (P10): aggregate Spark + Hive reports on Dataset L.
#
#   Dataset L max = 0.5 TB (~500 GB). HDFS free space ~700 GB — clean layouts first.
#   TARGET_SIZE_TB=0.5 ./scripts/run-sla-matrix.sh
# Assumes layouts best_orc (+ optional hive ingest) already benchmarked.
# -----------------------------------------------------------------------------
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
BASE="${BASE:-hdfs:///user/hdfs_migration_user/orc_test}"
TARGET_SIZE_TB="${TARGET_SIZE_TB:-0.5}"
CACHE_STATES="${CACHE_STATES:-cold warm}"
export BASE TARGET_SIZE_TB SKIP_GENERATE=1

if awk "BEGIN { exit !($TARGET_SIZE_TB > 0.5) }"; then
  echo "Refusing TARGET_SIZE_TB=$TARGET_SIZE_TB (>0.5). Max dataset on this cluster is 500 GB." >&2
  exit 1
fi

for state in $CACHE_STATES; do
  echo "=== SLA matrix spark best_orc cache=$state ==="
  CACHE_STATE="$state" "$ROOT/scripts/run-factor.sh" --layout=s1 --skip-generate --skip-validate || true
done

for profile in h0 h1 h2 h3 h4; do
  echo "=== SLA matrix hive $profile ==="
  "$ROOT/scripts/hive/run-hive-factor.sh" "$profile" || true
done

"$ROOT/scripts/submit-spark32.sh" -- \
  --mode=report \
  --base-path="$BASE" \
  --report-name="sla-matrix-final" \
  2>&1 | tee sla-matrix-report.log

echo "Final SLA report: $BASE/reports/summary/sla-matrix-final.md (and layouts/*/reports/summary)"
