#!/usr/bin/env bash
# -----------------------------------------------------------------------------
# Hive/Tez/LLAP factor runner against the same BEST_ORC external tables (P8).
#
# Profiles:
#   h0  Hive+Tez baseline (vectorization off, CBO off, no LLAP)
#   h1  + vectorization
#   h2  + vectorization + CBO
#   h3  LLAP cold
#   h4  LLAP warm
#
# Requires: beeline, Hive ≥2.0 for LLAP profiles.
#   SUITE=audei ./scripts/hive/run-hive-factor.sh h0
#   SUITE=st ./scripts/hive/run-hive-factor.sh h0
# Env: BASE, LAYOUT (default best_orc), SUITE (doc|audei|st), BEELINE, HIVE_JDBC_URL, FILTER_*
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
BASE="${BASE:-hdfs:///user/hdfs_migration_user/orc_test}"
LAYOUT="${LAYOUT:-best_orc}"
PROFILE="${1:-h0}"
BEELINE="${BEELINE:-beeline}"
HIVE_JDBC_URL="${HIVE_JDBC_URL:-}"
WARMUP="${BENCHMARK_WARMUP_RUNS:-2}"
REPEATS="${BENCHMARK_REPEAT_RUNS:-5}"
SLA_THRESHOLD_MS="${SLA_THRESHOLD_MS:-3000}"

ORC_LOCATION="$BASE/layouts/$LAYOUT/orc"
DICT_LOCATION="$BASE/layouts/$LAYOUT/dictionary"
OUT_DIR="$BASE/layouts/$LAYOUT/reports/raw/benchmark_hive_${PROFILE}"
LOCAL_CSV="$(mktemp /tmp/orc-bench-hive-XXXXXX.csv)"

Y="${FILTER_Y:-2024}"
M="${FILTER_M:-6}"
D="${FILTER_D:-15}"
EVENT_ID="${FILTER_EVENT_ID:-evt-sample}"
EPK_ID="${FILTER_EPK_ID:-003a75de-2a9b-45bc-89c9-1f9a8ebd9b0c}"
MODULE="${FILTER_MODULE:-CI02001608_sm_uko}"
NAME="${FILTER_NAME:-LOGON}"
STATUS="${FILTER_STATUS:-success}"
CHANNEL="${FILTER_CHANNEL:-WEB_SBOL}"
TS_START="${FILTER_TS_START:-2024-06-01 00:00:00}"
TS_END="${FILTER_TS_END:-2024-07-01 00:00:00}"
TS_1D_START="${FILTER_TS_1D_START:-2024-06-15 00:00:00}"
TS_1D_END="${FILTER_TS_1D_END:-2024-06-16 00:00:00}"
TS_14D_START="${FILTER_TS_14D_START:-2024-06-01 00:00:00}"
TS_14D_END="${FILTER_TS_14D_END:-2024-06-15 00:00:00}"
TS_31D_START="${FILTER_TS_31D_START:-2024-05-15 00:00:00}"
TS_31D_END="${FILTER_TS_31D_END:-2024-06-15 00:00:00}"
LIKE_TOKEN="${FILTER_LIKE_TOKEN:-audit-event}"
RLIKE="${FILTER_RLIKE:-(LOGON|FIND|ESA|LAUNCHER)}"
EPK_ID_B="${FILTER_EPK_ID_B:-003a75de-0000-4000-8000-000000000001}"
EPK_ID_C="${FILTER_EPK_ID_C:-003a75de-0000-4000-8000-000000000002}"
EPK_ID_D="${FILTER_EPK_ID_D:-003a75de-0000-4000-8000-000000000003}"
SUITE="${SUITE:-doc}"

SESSION_INIT=()
CACHE_STATE="cold"
ENGINE="hive_tez"

case "$PROFILE" in
  h0)
    SESSION_INIT+=(
      "SET hive.execution.engine=tez;"
      "SET hive.vectorized.execution.enabled=false;"
      "SET hive.cbo.enable=false;"
      "SET hive.llap.execution.mode=none;"
    )
    ;;
  h1)
    SESSION_INIT+=(
      "SET hive.execution.engine=tez;"
      "SET hive.vectorized.execution.enabled=true;"
      "SET hive.cbo.enable=false;"
      "SET hive.llap.execution.mode=none;"
    )
    ;;
  h2)
    SESSION_INIT+=(
      "SET hive.execution.engine=tez;"
      "SET hive.vectorized.execution.enabled=true;"
      "SET hive.cbo.enable=true;"
      "SET hive.llap.execution.mode=none;"
    )
    ;;
  h3)
    ENGINE="hive_llap"
    CACHE_STATE="cold"
    SESSION_INIT+=(
      "SET hive.execution.engine=tez;"
      "SET hive.vectorized.execution.enabled=true;"
      "SET hive.cbo.enable=true;"
      "SET hive.llap.execution.mode=all;"
      "SET hive.llap.io.enabled=true;"
    )
    ;;
  h4)
    ENGINE="hive_llap"
    CACHE_STATE="warm"
    SESSION_INIT+=(
      "SET hive.execution.engine=tez;"
      "SET hive.vectorized.execution.enabled=true;"
      "SET hive.cbo.enable=true;"
      "SET hive.llap.execution.mode=all;"
      "SET hive.llap.io.enabled=true;"
    )
    ;;
  *)
    echo "Unknown profile: $PROFILE (h0|h1|h2|h3|h4)" >&2
    exit 1
    ;;
esac

render_sql() {
  local file="$1"
  sed \
    -e "s|\${ORC_LOCATION}|$ORC_LOCATION|g" \
    -e "s|\${DICTIONARY_LOCATION}|$DICT_LOCATION|g" \
    -e "s|\${Y}|$Y|g" \
    -e "s|\${M}|$M|g" \
    -e "s|\${D}|$D|g" \
    -e "s|\${EVENT_ID}|$EVENT_ID|g" \
    -e "s|\${EPK_ID}|$EPK_ID|g" \
    -e "s|\${MODULE}|$MODULE|g" \
    -e "s|\${NAME}|$NAME|g" \
    -e "s|\${STATUS}|$STATUS|g" \
    -e "s|\${CHANNEL}|$CHANNEL|g" \
    -e "s|\${TS_START}|$TS_START|g" \
    -e "s|\${TS_END}|$TS_END|g" \
    -e "s|\${TS_1D_START}|$TS_1D_START|g" \
    -e "s|\${TS_1D_END}|$TS_1D_END|g" \
    -e "s|\${TS_14D_START}|$TS_14D_START|g" \
    -e "s|\${TS_14D_END}|$TS_14D_END|g" \
    -e "s|\${TS_31D_START}|$TS_31D_START|g" \
    -e "s|\${TS_31D_END}|$TS_31D_END|g" \
    -e "s|\${LIKE_TOKEN}|$LIKE_TOKEN|g" \
    -e "s|\${RLIKE}|$RLIKE|g" \
    -e "s|\${EPK_ID_B}|$EPK_ID_B|g" \
    -e "s|\${EPK_ID_C}|$EPK_ID_C|g" \
    -e "s|\${EPK_ID_D}|$EPK_ID_D|g" \
    "$file"
}

beeline_cmd() {
  if [[ -n "$HIVE_JDBC_URL" ]]; then
    "$BEELINE" -u "$HIVE_JDBC_URL" --silent=true --showHeader=false --outputformat=tsv2 -e "$1"
  else
    "$BEELINE" --silent=true --showHeader=false --outputformat=tsv2 -e "$1"
  fi
}

echo "Hive factor profile=$PROFILE suite=$SUITE engine=$ENGINE cache=$CACHE_STATE orc=$ORC_LOCATION"

# DDL once
DDL_SQL="$(render_sql "$ROOT/scripts/hive/ddl_external_orc.sql")"
beeline_cmd "$DDL_SQL" >/dev/null || true

# Init session settings
INIT_SQL="$(printf '%s\n' "${SESSION_INIT[@]}")"
beeline_cmd "$INIT_SQL" >/dev/null || true

case "$SUITE" in
  audei)
    QUERIES=(
      "epk_eq_1d:SELECT count(*) FROM events_ext WHERE event_ts >= '${TS_1D_START}' AND event_ts < '${TS_1D_END}' AND epk_id='${EPK_ID}'"
      "epk_eq_14d:SELECT count(*) FROM events_ext WHERE event_ts >= '${TS_14D_START}' AND event_ts < '${TS_14D_END}' AND epk_id='${EPK_ID}'"
      "epk_page:SELECT count(*) FROM (SELECT * FROM events_ext WHERE event_ts >= '${TS_14D_START}' AND event_ts < '${TS_14D_END}' AND epk_id='${EPK_ID}' ORDER BY event_ts LIMIT 1000) t"
      "eq_filters:SELECT count(*) FROM events_ext WHERE event_year=${Y} AND event_month=${M} AND event_day=${D} AND name='${NAME}' AND channel_type='${CHANNEL}' AND state='${STATUS}'"
      "order_by_epk_day:SELECT count(*) FROM (SELECT * FROM events_ext WHERE event_year=${Y} AND event_month=${M} AND event_day=${D} ORDER BY epk_id) t"
    )
    ;;
  st)
    QUERIES=(
      "no_filter:SELECT count(*) FROM events_ext WHERE event_ts >= '${TS_31D_START}' AND event_ts < '${TS_31D_END}'"
      "like_single:SELECT count(*) FROM events_ext WHERE event_ts >= '${TS_31D_START}' AND event_ts < '${TS_31D_END}' AND payload_json LIKE '%${LIKE_TOKEN}%'"
      "like_multi:SELECT count(*) FROM events_ext WHERE event_ts >= '${TS_31D_START}' AND event_ts < '${TS_31D_END}' AND payload_json LIKE '%${LIKE_TOKEN}%' AND payload_json LIKE '%AUDEI%' AND payload_json LIKE '%sms%' AND payload_json LIKE '%session%' AND payload_json LIKE '%pad%'"
      "like_fulltext:SELECT count(*) FROM events_ext WHERE event_ts >= '${TS_31D_START}' AND event_ts < '${TS_31D_END}' AND payload_json LIKE '%${LIKE_TOKEN}%' AND payload_json LIKE '%AUDEI%' AND payload_json LIKE '%sms%' AND payload_json LIKE '%session%' AND payload_json LIKE '%pad%' AND payload_json LIKE '%device%' AND payload_json LIKE '%confirm%' AND payload_json LIKE '%metamodel%' AND payload_json LIKE '%param%' AND payload_json LIKE '%event%'"
      "eq:SELECT count(*) FROM events_ext WHERE event_ts >= '${TS_31D_START}' AND event_ts < '${TS_31D_END}' AND epk_id='${EPK_ID}'"
      "in_list:SELECT count(*) FROM events_ext WHERE event_ts >= '${TS_31D_START}' AND event_ts < '${TS_31D_END}' AND epk_id IN ('${EPK_ID}','${EPK_ID_B}','${EPK_ID_C}','${EPK_ID_D}')"
      "rlike:SELECT count(*) FROM events_ext WHERE event_ts >= '${TS_31D_START}' AND event_ts < '${TS_31D_END}' AND payload_json RLIKE '${RLIKE}'"
    )
    ;;
  *)
    QUERIES=(
      "partition_prune:SELECT count(*) FROM events_ext WHERE event_year=${Y} AND event_month=${M} AND event_day=${D}"
      "filter_high_cardinality:SELECT count(*) FROM events_ext WHERE event_year=${Y} AND event_month=${M} AND event_day=${D} AND (epk_id='${EPK_ID}' OR event_id='${EVENT_ID}')"
      "filter_medium_cardinality:SELECT count(*) FROM events_ext WHERE event_year=${Y} AND event_month=${M} AND event_day=${D} AND (module='${MODULE}' OR name='${NAME}')"
      "filter_low_cardinality:SELECT count(*) FROM events_ext WHERE event_year=${Y} AND event_month=${M} AND event_day=${D} AND channel_type='${CHANNEL}' AND state='${STATUS}'"
      "filter_in:SELECT count(*) FROM events_ext WHERE event_year=${Y} AND event_month=${M} AND event_day=${D} AND event_id IN ('${EVENT_ID}','${EVENT_ID}-missing-a','${EVENT_ID}-missing-b')"
      "filter_timestamp_range:SELECT count(*) FROM events_ext WHERE event_ts >= '${TS_START}' AND event_ts < '${TS_END}'"
      "projection:SELECT count(event_id) FROM events_ext WHERE event_year=${Y} AND event_month=${M} AND event_day=${D}"
      "full_scan:SELECT count(*) FROM events_ext WHERE event_year=${Y} AND event_month=${M} AND event_day=${D}"
      "group_by:SELECT name, state, count(*) FROM events_ext WHERE event_year=${Y} AND event_month=${M} AND event_day=${D} GROUP BY name, state"
      "group_by_heavy:SELECT module, count(*) FROM events_ext WHERE event_year=${Y} AND event_month=${M} GROUP BY module"
      "join_dictionary:SELECT e.name, count(*) FROM events_ext e JOIN dictionary_ext d ON e.name=d.event_name WHERE e.event_year=${Y} AND e.event_month=${M} AND e.event_day=${D} AND d.event_family='featured' GROUP BY e.name"
    )
    ;;
esac

echo "scenario,duration_ms,layout_id,engine,cache_state,dataset_label,sla_ok,sla_threshold_ms,format,run_index,warmup" > "$LOCAL_CSV"

run_once() {
  local scenario="$1"
  local sql="$2"
  local warmup_flag="$3"
  local run_index="$4"
  local start end duration sla_ok
  start=$(date +%s%3N)
  beeline_cmd "${INIT_SQL}; ${sql};" >/dev/null
  end=$(date +%s%3N)
  duration=$((end - start))
  if (( duration <= SLA_THRESHOLD_MS )); then sla_ok=true; else sla_ok=false; fi
  echo "${scenario},${duration},${LAYOUT},${ENGINE},${CACHE_STATE},${PROFILE},${sla_ok},${SLA_THRESHOLD_MS},orc,${run_index},${warmup_flag}" >> "$LOCAL_CSV"
}

for entry in "${QUERIES[@]}"; do
  scenario="${entry%%:*}"
  sql="${entry#*:}"
  echo "Scenario $scenario"
  for ((i = 0; i < WARMUP; i++)); do
    run_once "$scenario" "$sql" "true" "$i"
  done
  # LLAP warm: keep cache; cold: optional service restart left to operator
  for ((i = 0; i < REPEATS; i++)); do
    run_once "$scenario" "$sql" "false" "$i"
  done
done

echo "Hive timings written to $LOCAL_CSV"
echo "Ingest into HDFS reports:"
echo "  hdfs dfs -mkdir -p $OUT_DIR"
echo "  hdfs dfs -put -f $LOCAL_CSV $OUT_DIR/hive_results.csv"
echo "  $ROOT/scripts/hive/ingest-hive-csv.sh $OUT_DIR/hive_results.csv $OUT_DIR"

# Best-effort local HDFS put + ingest when hdfs CLI is available
if command -v hdfs >/dev/null 2>&1; then
  hdfs dfs -mkdir -p "$OUT_DIR" || true
  hdfs dfs -put -f "$LOCAL_CSV" "$OUT_DIR/hive_results.csv" || true
  if [[ -x "$ROOT/scripts/hive/ingest-hive-csv.sh" ]]; then
    "$ROOT/scripts/hive/ingest-hive-csv.sh" "$OUT_DIR/hive_results.csv" "$OUT_DIR" || true
  fi
fi

echo "Hive profile $PROFILE complete."
