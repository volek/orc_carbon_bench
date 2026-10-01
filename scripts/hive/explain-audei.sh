#!/usr/bin/env bash
# -----------------------------------------------------------------------------
# EXPLAIN EXTENDED for AUDEI Hive queries (partition prune checklist).
#
#   PROFILE=h2 ./scripts/hive/explain-audei.sh
# Output: result/hive/explain-<profile>-<ts>.txt
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
RESULT_DIR="${RESULT_DIR:-$ROOT/result/hive}"
EPK_ID="${FILTER_EPK_ID:-003a75de-2a9b-45bc-89c9-1f9a8ebd9b0c}"
TS_1D_START="${FILTER_TS_1D_START:-2024-06-15 00:00:00}"
TS_1D_END="${FILTER_TS_1D_END:-2024-06-16 00:00:00}"
TS_14D_START="${FILTER_TS_14D_START:-2024-06-01 00:00:00}"
TS_14D_END="${FILTER_TS_14D_END:-2024-06-15 00:00:00}"
Y="${FILTER_Y:-2024}"
M="${FILTER_M:-6}"
D="${FILTER_D:-15}"
NAME="${FILTER_NAME:-LOGON}"
STATUS="${FILTER_STATUS:-success}"
CHANNEL="${FILTER_CHANNEL:-WEB_SBOL}"

mkdir -p "$RESULT_DIR"
TS="$(date +%Y%m%d-%H%M%S)"
OUT="$RESULT_DIR/explain-${PROFILE}-${TS}.txt"
SQL="$(mktemp /tmp/orc-bench-explain-XXXXXX.sql)"
trap 'rm -f "$SQL"' EXIT

hive_profile_session_init "$PROFILE" || exit 1
PART_1D="$(hive_partition_predicate "$TS_1D_START" "$TS_1D_END")"
PART_14D="$(hive_partition_predicate "$TS_14D_START" "$TS_14D_END")"
and_part() {
  local p="$1"
  if [[ -n "$p" ]]; then echo " AND ${p}"; else echo ""; fi
}

PAGE_COLS="epk_id, event_id, event_ts, name, channel_type, state, module"

{
  echo "USE orc_bench;"
  printf '%s\n' "${SESSION_INIT[@]}"
  echo "EXPLAIN EXTENDED SELECT count(*) FROM orc_bench.events_ext WHERE event_ts >= '${TS_1D_START}' AND event_ts < '${TS_1D_END}' AND epk_id='${EPK_ID}'$(and_part "$PART_1D");"
  echo "EXPLAIN EXTENDED SELECT count(*) FROM orc_bench.events_ext WHERE event_ts >= '${TS_14D_START}' AND event_ts < '${TS_14D_END}' AND epk_id='${EPK_ID}'$(and_part "$PART_14D");"
  echo "EXPLAIN EXTENDED SELECT count(*) FROM (SELECT ${PAGE_COLS} FROM orc_bench.events_ext WHERE event_ts >= '${TS_14D_START}' AND event_ts < '${TS_14D_END}' AND epk_id='${EPK_ID}'$(and_part "$PART_14D") ORDER BY event_ts LIMIT 1000) t;"
  echo "EXPLAIN EXTENDED SELECT count(*) FROM orc_bench.events_ext WHERE event_year=${Y} AND event_month=${M} AND event_day=${D} AND name='${NAME}' AND channel_type='${CHANNEL}' AND state='${STATUS}';"
} > "$SQL"

echo "explain-audei profile=$PROFILE → $OUT"
set +e
hive_beeline_file "$SQL" --showHeader=false >"$OUT" 2>&1
rc=$?
set -e

echo "---- prune checklist (grep) ----" | tee -a "$OUT"
if grep -qiE 'partition|Pruned|Filter Operator|input paths|truncated' "$OUT"; then
  grep -iE 'partition|Pruned|Filter Operator|input paths|truncated|Stripe' "$OUT" | head -n 80 | tee -a "$OUT" || true
  echo "HINT: confirm pruned partition paths appear (not full table scan)." | tee -a "$OUT"
else
  echo "WARN: no obvious partition/prune tokens in EXPLAIN; inspect $OUT manually." | tee -a "$OUT"
fi

echo "Wrote $OUT (beeline rc=$rc)"
exit "$rc"
