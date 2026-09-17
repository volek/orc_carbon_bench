#!/usr/bin/env bash
# -----------------------------------------------------------------------------
# Layout factor sweep for P3–P6 on Dataset S (default TARGET_SIZE_TB=0.1 ≈ 100 GB).
# Runs one layout at a time; winners should be folded into best_orc manually.
# Max single dataset: 0.5 TB (500 GB); HDFS free ≈ 700 GB — sweep by group, delete losers.
#
#   TARGET_SIZE_TB=0.1 ./scripts/run-layout-sweep.sh
#   SWEEP=partition ./scripts/run-layout-sweep.sh
# -----------------------------------------------------------------------------
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
BASE="${BASE:-hdfs:///user/hdfs_migration_user/orc_test}"
TARGET_SIZE_TB="${TARGET_SIZE_TB:-0.1}"
CACHE_STATE="${CACHE_STATE:-cold}"
SWEEP="${SWEEP:-all}"
export BASE TARGET_SIZE_TB CACHE_STATE

if awk "BEGIN { exit !($TARGET_SIZE_TB > 0.5) }"; then
  echo "Refusing TARGET_SIZE_TB=$TARGET_SIZE_TB (>0.5). Max dataset on this cluster is 500 GB." >&2
  exit 1
fi

case "$SWEEP" in
  baseline)   LAYOUTS=(b0) ;;
  partition)  LAYOUTS=(d0 d1 d2) ;;
  sort)       LAYOUTS=(e1 e2 e3) ;;
  bloom)      LAYOUTS=(f0 f1 f2 f3) ;;
  fpp)        LAYOUTS=(g005 g001 g0001) ;;
  stride)     LAYOUTS=(h5k h10k h20k h50k) ;;
  stripe)     LAYOUTS=(i64 i128 i256) ;;
  compression) LAYOUTS=(jsnappy jzstd jzlib) ;;
  files)      LAYOUTS=(k32 k256 k1024) ;;
  all)        LAYOUTS=(b0 d0 d1 d2 e1 e2 e3 f0 f1 f2 f3 g005 g001 h5k h10k i64 i128 jsnappy jzstd k256) ;;
  *) echo "Unknown SWEEP=$SWEEP" >&2; exit 1 ;;
esac

for layout in "${LAYOUTS[@]}"; do
  echo "=== Layout sweep: $layout ==="
  "$ROOT/scripts/run-factor.sh" --layout="$layout"
done

# Combined report from base (discovers layouts/*/reports)
"$ROOT/scripts/submit-spark32.sh" -- \
  --mode=report \
  --base-path="$BASE" \
  --report-name="layout-sweep-${SWEEP}" \
  2>&1 | tee "layout-sweep-${SWEEP}-report.log"

echo "Layout sweep $SWEEP complete."
