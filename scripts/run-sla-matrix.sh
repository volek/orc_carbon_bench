#!/usr/bin/env bash
# -----------------------------------------------------------------------------
# Final SLA matrix on Dataset L: AUDEI interactive suite (Spark + Hive) + report.
#
#   Dataset L max = 0.1 TB (~100 GB). Clean layouts first.
#   TARGET_SIZE_TB=0.1 ./scripts/run-sla-matrix.sh
#   SCENARIOS=audei CACHE_STATES="cold warm" ./scripts/run-sla-matrix.sh
#   Also runs ST archive suite once on warm if RUN_ST=1.
#
# Assumes best_orc already generated on L (or set SKIP_GENERATE=0 to regenerate).
# -----------------------------------------------------------------------------
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
BASE="${BASE:-hdfs:///user/hdfs_migration_user/orc_test}"
TARGET_SIZE_TB="${TARGET_SIZE_TB:-0.1}"
CACHE_STATES="${CACHE_STATES:-cold warm}"
SCENARIOS="${SCENARIOS:-audei}"
SLA_THRESHOLD_MS="${SLA_THRESHOLD_MS:-3000}"
ARCHIVE_SLA_THRESHOLD_MS="${ARCHIVE_SLA_THRESHOLD_MS:-120000}"
RUN_ST="${RUN_ST:-0}"
export BASE TARGET_SIZE_TB SKIP_GENERATE="${SKIP_GENERATE:-1}" SCENARIOS
export SLA_THRESHOLD_MS ARCHIVE_SLA_THRESHOLD_MS

if awk "BEGIN { exit !($TARGET_SIZE_TB > 0.1) }"; then
  echo "Refusing TARGET_SIZE_TB=$TARGET_SIZE_TB (>0.1). Max dataset on this cluster is 100 GB." >&2
  exit 1
fi

for state in $CACHE_STATES; do
  echo "=== SLA matrix spark best_orc/s1 cache=$state scenarios=$SCENARIOS ==="
  CACHE_STATE="$state" "$ROOT/scripts/run-factor.sh" --layout=s1 --skip-generate --skip-validate || true
done

if [[ "$RUN_ST" == "1" ]]; then
  echo "=== SLA matrix ST archive suite (warm) ==="
  SCENARIOS=st CACHE_STATE=warm \
    "$ROOT/scripts/run-factor.sh" --layout=s1 --skip-generate --skip-validate || true
fi

for profile in h0 h1 h2 h3 h4; do
  echo "=== SLA matrix hive $profile suite=$SCENARIOS ==="
  SUITE="$SCENARIOS" "$ROOT/scripts/hive/run-hive-factor.sh" "$profile" || true
done

"$ROOT/scripts/submit-spark32.sh" -- \
  --mode=report \
  --base-path="$BASE" \
  --report-name="sla-matrix-final" \
  2>&1 | tee sla-matrix-report.log

echo "Final SLA report: $BASE/reports/summary/sla-matrix-final.md"
