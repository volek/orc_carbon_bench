#!/usr/bin/env bash
# -----------------------------------------------------------------------------
# Diagnose Hive "session tax": one Beeline session, N× epk_eq_14d, parse Time taken.
#
#   PROFILE=h2 REPEATS=10 ./scripts/hive/diagnose-session-tax.sh
#
# Writes result/hive/session-tax-<profile>-<ts>.csv and prints SESSION_TAX verdict.
# -----------------------------------------------------------------------------
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
# shellcheck source=hive-lib.sh
source "$ROOT/scripts/hive/hive-lib.sh"

BASE="${BASE:-hdfs:///user/hdfs_migration_user/orc_test}"
LAYOUT="${LAYOUT:-best_orc}"
PROFILE="${PROFILE:-h2}"
BEELINE="${BEELINE:-beeline}"
HIVE_JDBC_URL="${HIVE_JDBC_URL:-}"
REPEATS="${REPEATS:-10}"
RESULT_DIR="${RESULT_DIR:-$ROOT/result/hive}"
LOG_DIR="${LOG_DIR:-$ROOT/logs}"
TS_14D_START="${FILTER_TS_14D_START:-2024-06-01 00:00:00}"
TS_14D_END="${FILTER_TS_14D_END:-2024-06-15 00:00:00}"
EPK_ID="${FILTER_EPK_ID:-003a75de-2a9b-45bc-89c9-1f9a8ebd9b0c}"
TAX_RATIO="${TAX_RATIO:-2.0}"

mkdir -p "$RESULT_DIR" "$LOG_DIR"
TS="$(date +%Y%m%d-%H%M%S)"
OUT_CSV="$RESULT_DIR/session-tax-${PROFILE}-${TS}.csv"
BEELINE_LOG="$LOG_DIR/session-tax-${PROFILE}-${TS}.beeline.log"
SUITE_SQL="$(mktemp /tmp/orc-bench-session-tax-XXXXXX.sql)"
trap 'rm -f "$SUITE_SQL"' EXIT

hive_profile_session_init "$PROFILE" || exit 1

PART_14="$(hive_partition_predicate "$TS_14D_START" "$TS_14D_END")"
PART_CLAUSE=""
if [[ -n "$PART_14" ]]; then
  PART_CLAUSE=" AND ${PART_14}"
fi

QUERY="SELECT count(*) FROM orc_bench.events_ext WHERE event_ts >= '${TS_14D_START}' AND event_ts < '${TS_14D_END}' AND epk_id='${EPK_ID}'${PART_CLAUSE}"

{
  echo "USE orc_bench;"
  printf '%s\n' "${SESSION_INIT[@]}"
  for ((i = 0; i < REPEATS; i++)); do
    echo "SELECT 'ORC_BENCH_MARK', 'epk_eq_14d', ${i}, 'false';"
    echo "${QUERY};"
  done
} > "$SUITE_SQL"

echo "diagnose-session-tax profile=$PROFILE repeats=$REPEATS"
echo "SQL: $SUITE_SQL"
echo "Beeline log: $BEELINE_LOG"

wall_start=$(date +%s%3N)
set +e
hive_beeline_file "$SUITE_SQL" --showHeader=false --outputformat=tsv2 >"$BEELINE_LOG" 2>&1
bee_rc=$?
set -e
wall_end=$(date +%s%3N)
wall_total=$((wall_end - wall_start))

PARSE_TMP="$(mktemp /tmp/orc-bench-session-tax-parse-XXXXXX.csv)"
set +e
python3 "$ROOT/scripts/hive/parse-beeline-timings.py" "$BEELINE_LOG" --header --csv "$PARSE_TMP"
parse_rc=$?
set -e

if (( parse_rc != 0 )) || [[ ! -s "$PARSE_TMP" ]]; then
  echo "ERROR: failed to parse Time taken from $BEELINE_LOG (beeline rc=$bee_rc)" >&2
  echo "--- beeline log (tail) ---" >&2
  tail -n 60 "$BEELINE_LOG" >&2 || true
  exit 1
fi

{
  echo "run_index,time_taken_ms,wall_total_ms,profile,layout"
  # skip header of parse tmp
  tail -n +2 "$PARSE_TMP" | while IFS=, read -r scenario run_index warmup time_taken_ms; do
    echo "${run_index},${time_taken_ms},${wall_total},${PROFILE},${LAYOUT}"
  done
} > "$OUT_CSV"
rm -f "$PARSE_TMP"

python3 - "$OUT_CSV" "$TAX_RATIO" <<'PY'
import csv, statistics, sys
path, ratio = sys.argv[1], float(sys.argv[2])
rows = list(csv.DictReader(open(path)))
vals = [int(r["time_taken_ms"]) for r in rows]
if not vals:
    print("ERROR: empty timings"); sys.exit(1)
first = vals[0]
rest = vals[1:] if len(vals) > 1 else vals
med_rest = statistics.median(rest)
print("runs=%d first_ms=%d median_rest_ms=%.0f wall_total_ms=%s" % (
    len(vals), first, med_rest, rows[0].get("wall_total_ms", "")))
print("all_ms=%s" % vals)
verdict = "SESSION_TAX" if (med_rest > 0 and first >= ratio * med_rest) else "NO_SESSION_TAX"
# Also flag if all runs are huge (overhead every time)
if statistics.median(vals) > 10000 and first < ratio * med_rest:
    verdict = "HIGH_LATENCY_STABLE"
print("verdict=%s (threshold first>=%.1fx median_rest)" % (verdict, ratio))
PY

echo "Wrote $OUT_CSV"
echo "Beeline rc=$bee_rc; inspect $BEELINE_LOG if needed"
