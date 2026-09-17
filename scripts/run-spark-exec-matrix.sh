#!/usr/bin/env bash
# -----------------------------------------------------------------------------
# Spark execution matrix (P7): S0 baseline vs S1 optimized on frozen BEST_ORC.
# Also supports pushdown / vectorized / AQE / DPP / CBO one-factor toggles.
#
#   BEST_ORC must already exist (run-factor.sh --layout=best_orc once).
#
#   ./scripts/run-spark-exec-matrix.sh
# -----------------------------------------------------------------------------
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
BASE="${BASE:-hdfs:///user/hdfs_migration_user/orc_test}"
TARGET_SIZE_TB="${TARGET_SIZE_TB:-0.01}"
CACHE_STATE="${CACHE_STATE:-cold}"
export BASE TARGET_SIZE_TB CACHE_STATE
export SKIP_GENERATE=1
export SKIP_VALIDATE=1

PROFILES=(s0 s1)

for profile in "${PROFILES[@]}"; do
  echo "=== Spark exec profile $profile ==="
  "$ROOT/scripts/run-factor.sh" --layout="$profile"
done

# Optional one-factor OFF variants against S1 defaults (read best_orc)
run_toggle() {
  local name="$1"
  shift
  echo "=== Toggle $name ==="
  LAYOUT_ID=best_orc SKIP_GENERATE=1 SKIP_VALIDATE=1 \
    "$ROOT/scripts/run-factor.sh" --layout=best_orc \
    --benchmark-dataset-label="$name" \
    "$@"
}

run_toggle "c0_pushdown_off" --spark-orc-filter-pushdown=false
run_toggle "c1_pushdown_on"  --spark-orc-filter-pushdown=true
run_toggle "l0_vec_off"      --spark-orc-vectorized=false
run_toggle "l1_vec_on"       --spark-orc-vectorized=true
run_toggle "m0_aqe_off"      --spark-aqe=false
run_toggle "m1_aqe_on"       --spark-aqe=true
run_toggle "n0_dpp_off"      --spark-dpp=false
run_toggle "n1_dpp_on"       --spark-dpp=true
run_toggle "o0_cbo_off"      --spark-cbo=false
run_toggle "o1_cbo_on"       --spark-cbo=true

echo "Spark exec matrix complete."
