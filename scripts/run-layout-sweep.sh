#!/usr/bin/env bash
# -----------------------------------------------------------------------------
# Layout factor sweep — AUDEI-priority groups first (partition / sort / bloom),
# then secondary (fpp / stride / stripe / compression / files).
#
# Default TARGET_SIZE_TB=0.02 ≈ Dataset S (~20 GB). Max 0.1 TB (~100 GB).
#
#   TARGET_SIZE_TB=0.02 ./scripts/run-layout-sweep.sh
#   SWEEP=audei ./scripts/run-layout-sweep.sh    # partition+sort+bloom only
#   SWEEP=partition ./scripts/run-layout-sweep.sh
#   SCENARIOS=audei CACHE_STATE=cold ./scripts/run-layout-sweep.sh
# -----------------------------------------------------------------------------
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
BASE="${BASE:-hdfs:///user/hdfs_migration_user/orc_test}"
TARGET_SIZE_TB="${TARGET_SIZE_TB:-0.02}"
CACHE_STATE="${CACHE_STATE:-cold}"
SCENARIOS="${SCENARIOS:-audei}"
SWEEP="${SWEEP:-audei}"
export BASE TARGET_SIZE_TB CACHE_STATE SCENARIOS

if awk "BEGIN { exit !($TARGET_SIZE_TB > 0.1) }"; then
  echo "Refusing TARGET_SIZE_TB=$TARGET_SIZE_TB (>0.1). Max dataset on this cluster is 100 GB." >&2
  exit 1
fi

case "$SWEEP" in
  baseline)    LAYOUTS=(b0) ;;
  # AUDEI priority: day partition, sort by epk_id, bloom on epk_id
  partition)   LAYOUTS=(d0 d1 d2) ;;
  sort)        LAYOUTS=(e1 e2 e3) ;;
  bloom)       LAYOUTS=(f0 f1 f2 f3) ;;
  audei)       LAYOUTS=(b0 d0 d1 d2 e1 e2 e3 f0 f1 f2 f3) ;;
  fpp)         LAYOUTS=(g005 g001 g0001) ;;
  stride)      LAYOUTS=(h5k h10k h20k h50k) ;;
  stripe)      LAYOUTS=(i64 i128 i256) ;;
  compression) LAYOUTS=(jsnappy jzstd jzlib) ;;
  files)       LAYOUTS=(k32 k256 k1024) ;;
  secondary)   LAYOUTS=(g005 g001 h5k h10k i64 i128 jsnappy jzstd k256) ;;
  all)         LAYOUTS=(b0 d0 d1 d2 e1 e2 e3 f0 f1 f2 f3 g005 g001 h5k h10k i64 i128 jsnappy jzstd k256) ;;
  *) echo "Unknown SWEEP=$SWEEP (audei|partition|sort|bloom|secondary|all|...)" >&2; exit 1 ;;
esac

for layout in "${LAYOUTS[@]}"; do
  echo "=== Layout sweep: $layout (scenarios=$SCENARIOS) ==="
  "$ROOT/scripts/run-factor.sh" --layout="$layout"
done

"$ROOT/scripts/submit-spark32.sh" -- \
  --mode=report \
  --base-path="$BASE" \
  --report-name="layout-sweep-${SWEEP}" \
  2>&1 | tee "layout-sweep-${SWEEP}-report.log"

echo "Layout sweep $SWEEP complete. Next: fold winners into best_orc (day + sort epk_id + bloom epk_id)."
