#!/usr/bin/env bash
# -----------------------------------------------------------------------------
# Hive/Tez/LLAP factor runner against the same BEST_ORC external tables (P8).
#
# Profiles:
#   h0   Hive+Tez baseline (vectorization off, CBO off, no LLAP)
#   h1   + vectorization + PPD/index + tez reuse
#   h2   + vectorization + CBO + stats
#   h2f  h2 + hive.fetch.task.conversion=more (A/B; not Tez/LLAP SLA)
#   h3   LLAP cold (requires live LLAP; see llap-preflight.sh)
#   h4   LLAP warm (same session warmup → measured)
#
# Session modes:
#   SESSION_MODE=persistent  (default) one Beeline; duration_ms = HS2 "Time taken"
#   SESSION_MODE=per_query   legacy: new Beeline per query; wall-clock CLI
#
#   SUITE=audei ./scripts/hive/run-hive-factor.sh h0
#   SUITE=st ./scripts/hive/run-hive-factor.sh h0
# Env: BASE, LAYOUT, SUITE, BEELINE, HIVE_JDBC_URL, FILTER_*, SESSION_MODE,
#      SKIP_LLAP_PREFLIGHT, BENCHMARK_WARMUP_RUNS, BENCHMARK_REPEAT_RUNS
# Лог: LOG_DIR → hive-factor-<profile>-<ts>.log
# -----------------------------------------------------------------------------
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
# shellcheck source=hive-lib.sh
source "$ROOT/scripts/hive/hive-lib.sh"

BASE="${BASE:-hdfs:///user/hdfs_migration_user/orc_test}"
LAYOUT="${LAYOUT:-best_orc}"
PROFILE="${1:-h0}"
BEELINE="${BEELINE:-beeline}"
HIVE_JDBC_URL="${HIVE_JDBC_URL:-}"
WARMUP="${BENCHMARK_WARMUP_RUNS:-2}"
REPEATS="${BENCHMARK_REPEAT_RUNS:-5}"
SLA_THRESHOLD_MS="${SLA_THRESHOLD_MS:-3000}"
SESSION_MODE="${SESSION_MODE:-persistent}"
LOG_DIR="${LOG_DIR:-$ROOT/logs}"
SKIP_LLAP_PREFLIGHT="${SKIP_LLAP_PREFLIGHT:-0}"

if [[ -z "${HIVE_LOG+x}" ]]; then
  mkdir -p "$LOG_DIR"
  HIVE_LOG="$LOG_DIR/hive-factor-${PROFILE}-$(date +%Y%m%d-%H%M%S).log"
fi
if [[ -n "${HIVE_LOG}" ]]; then
  mkdir -p "$(dirname "$HIVE_LOG")"
  exec > >(tee -a "$HIVE_LOG") 2>&1
  echo "Logging to $HIVE_LOG"
fi

ORC_LOCATION="$BASE/layouts/$LAYOUT/orc"
DICT_LOCATION="$BASE/layouts/$LAYOUT/dictionary"
OUT_DIR="$BASE/layouts/$LAYOUT/reports/raw/benchmark_hive_${PROFILE}"
RESULT_DIR="${RESULT_DIR:-$ROOT/result/hive}"
mkdir -p "$RESULT_DIR"
LOCAL_CSV="$RESULT_DIR/hive-${LAYOUT}-${PROFILE}-$(date +%Y%m%d-%H%M%S).csv"

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

PAGE_COLS="epk_id, event_id, event_ts, name, channel_type, state, module"
ORDER_COLS="epk_id, event_id, event_ts, name, channel_type, state, module"

hive_profile_session_init "$PROFILE" || exit 1

if [[ "$PROFILE" == "h3" || "$PROFILE" == "h4" ]]; then
  if [[ "$SKIP_LLAP_PREFLIGHT" != "1" ]]; then
    if [[ -x "$ROOT/scripts/hive/llap-preflight.sh" ]]; then
      "$ROOT/scripts/hive/llap-preflight.sh" || {
        echo "ERROR: LLAP preflight failed for profile=$PROFILE (set SKIP_LLAP_PREFLIGHT=1 to override)" >&2
        exit 1
      }
    else
      echo "WARN: llap-preflight.sh missing; continuing" >&2
    fi
  else
    echo "WARN: SKIP_LLAP_PREFLIGHT=1 — LLAP daemon not checked"
  fi
fi

PART_1D="$(hive_partition_predicate "$TS_1D_START" "$TS_1D_END")"
PART_14D="$(hive_partition_predicate "$TS_14D_START" "$TS_14D_END")"
PART_31D="$(hive_partition_predicate "$TS_31D_START" "$TS_31D_END")"
and_part() {
  local p="$1"
  if [[ -n "$p" ]]; then echo " AND ${p}"; else echo ""; fi
}

render_sql() {
  local file="$1"
  sed \
    -e "s#\${ORC_LOCATION}#$(hive_sed_repl "$ORC_LOCATION")#g" \
    -e "s#\${DICTIONARY_LOCATION}#$(hive_sed_repl "$DICT_LOCATION")#g" \
    -e "s#\${Y}#$(hive_sed_repl "$Y")#g" \
    -e "s#\${M}#$(hive_sed_repl "$M")#g" \
    -e "s#\${D}#$(hive_sed_repl "$D")#g" \
    -e "s#\${EVENT_ID}#$(hive_sed_repl "$EVENT_ID")#g" \
    -e "s#\${EPK_ID}#$(hive_sed_repl "$EPK_ID")#g" \
    -e "s#\${MODULE}#$(hive_sed_repl "$MODULE")#g" \
    -e "s#\${NAME}#$(hive_sed_repl "$NAME")#g" \
    -e "s#\${STATUS}#$(hive_sed_repl "$STATUS")#g" \
    -e "s#\${CHANNEL}#$(hive_sed_repl "$CHANNEL")#g" \
    -e "s#\${TS_START}#$(hive_sed_repl "$TS_START")#g" \
    -e "s#\${TS_END}#$(hive_sed_repl "$TS_END")#g" \
    -e "s#\${TS_1D_START}#$(hive_sed_repl "$TS_1D_START")#g" \
    -e "s#\${TS_1D_END}#$(hive_sed_repl "$TS_1D_END")#g" \
    -e "s#\${TS_14D_START}#$(hive_sed_repl "$TS_14D_START")#g" \
    -e "s#\${TS_14D_END}#$(hive_sed_repl "$TS_14D_END")#g" \
    -e "s#\${TS_31D_START}#$(hive_sed_repl "$TS_31D_START")#g" \
    -e "s#\${TS_31D_END}#$(hive_sed_repl "$TS_31D_END")#g" \
    -e "s#\${LIKE_TOKEN}#$(hive_sed_repl "$LIKE_TOKEN")#g" \
    -e "s#\${RLIKE}#$(hive_sed_repl "$RLIKE")#g" \
    -e "s#\${EPK_ID_B}#$(hive_sed_repl "$EPK_ID_B")#g" \
    -e "s#\${EPK_ID_C}#$(hive_sed_repl "$EPK_ID_C")#g" \
    -e "s#\${EPK_ID_D}#$(hive_sed_repl "$EPK_ID_D")#g" \
    "$file"
}

echo "Hive factor profile=$PROFILE suite=$SUITE engine=$ENGINE cache=$CACHE_STATE session_mode=$SESSION_MODE orc=$ORC_LOCATION"

if command -v hdfs >/dev/null 2>&1; then
  if ! hdfs dfs -test -d "$ORC_LOCATION"; then
    echo "ERROR: ORC path missing: $ORC_LOCATION (run generate/factor for layout=$LAYOUT first)" >&2
    exit 1
  fi
fi

DDL_FILE="$(mktemp /tmp/orc-bench-hive-ddl-XXXXXX.sql)"
render_sql "$ROOT/scripts/hive/ddl_external_orc.sql" > "$DDL_FILE"
echo "Running Hive DDL from $DDL_FILE (LOCATION=$ORC_LOCATION)"
if ! hive_beeline_file "$DDL_FILE" --silent=true --showHeader=false --outputformat=tsv2; then
  echo "ERROR: Hive DDL failed. Check beeline output above and ORC path $ORC_LOCATION" >&2
  rm -f "$DDL_FILE"
  exit 1
fi
rm -f "$DDL_FILE"

if ! hive_beeline_cmd "DESCRIBE orc_bench.events_ext;" --silent=true --showHeader=false --outputformat=tsv2 >/dev/null; then
  echo "ERROR: orc_bench.events_ext not found after DDL" >&2
  exit 1
fi

INIT_SQL="$(printf '%s\n' "USE orc_bench;" "${SESSION_INIT[@]}")"

case "$SUITE" in
  audei)
    QUERIES=(
      "epk_eq_1d:SELECT count(*) FROM orc_bench.events_ext WHERE event_ts >= '${TS_1D_START}' AND event_ts < '${TS_1D_END}' AND epk_id='${EPK_ID}'$(and_part "$PART_1D")"
      "epk_eq_14d:SELECT count(*) FROM orc_bench.events_ext WHERE event_ts >= '${TS_14D_START}' AND event_ts < '${TS_14D_END}' AND epk_id='${EPK_ID}'$(and_part "$PART_14D")"
      "epk_page:SELECT count(*) FROM (SELECT ${PAGE_COLS} FROM orc_bench.events_ext WHERE event_ts >= '${TS_14D_START}' AND event_ts < '${TS_14D_END}' AND epk_id='${EPK_ID}'$(and_part "$PART_14D") ORDER BY event_ts LIMIT 1000) t"
      "eq_filters:SELECT count(*) FROM orc_bench.events_ext WHERE event_year=${Y} AND event_month=${M} AND event_day=${D} AND name='${NAME}' AND channel_type='${CHANNEL}' AND state='${STATUS}'"
      "order_by_epk_day:SELECT count(*) FROM (SELECT ${ORDER_COLS} FROM orc_bench.events_ext WHERE event_year=${Y} AND event_month=${M} AND event_day=${D} ORDER BY epk_id) t"
    )
    ;;
  st)
    QUERIES=(
      "no_filter:SELECT count(*) FROM orc_bench.events_ext WHERE event_ts >= '${TS_31D_START}' AND event_ts < '${TS_31D_END}'$(and_part "$PART_31D")"
      "like_single:SELECT count(*) FROM orc_bench.events_ext WHERE event_ts >= '${TS_31D_START}' AND event_ts < '${TS_31D_END}' AND payload_json LIKE '%${LIKE_TOKEN}%'$(and_part "$PART_31D")"
      "like_multi:SELECT count(*) FROM orc_bench.events_ext WHERE event_ts >= '${TS_31D_START}' AND event_ts < '${TS_31D_END}' AND payload_json LIKE '%${LIKE_TOKEN}%' AND payload_json LIKE '%AUDEI%' AND payload_json LIKE '%sms%' AND payload_json LIKE '%session%' AND payload_json LIKE '%pad%'$(and_part "$PART_31D")"
      "like_fulltext:SELECT count(*) FROM orc_bench.events_ext WHERE event_ts >= '${TS_31D_START}' AND event_ts < '${TS_31D_END}' AND payload_json LIKE '%${LIKE_TOKEN}%' AND payload_json LIKE '%AUDEI%' AND payload_json LIKE '%sms%' AND payload_json LIKE '%session%' AND payload_json LIKE '%pad%' AND payload_json LIKE '%device%' AND payload_json LIKE '%confirm%' AND payload_json LIKE '%metamodel%' AND payload_json LIKE '%param%' AND payload_json LIKE '%event%'$(and_part "$PART_31D")"
      "eq:SELECT count(*) FROM orc_bench.events_ext WHERE event_ts >= '${TS_31D_START}' AND event_ts < '${TS_31D_END}' AND epk_id='${EPK_ID}'$(and_part "$PART_31D")"
      "in_list:SELECT count(*) FROM orc_bench.events_ext WHERE event_ts >= '${TS_31D_START}' AND event_ts < '${TS_31D_END}' AND epk_id IN ('${EPK_ID}','${EPK_ID_B}','${EPK_ID_C}','${EPK_ID_D}')$(and_part "$PART_31D")"
      "rlike:SELECT count(*) FROM orc_bench.events_ext WHERE event_ts >= '${TS_31D_START}' AND event_ts < '${TS_31D_END}' AND payload_json RLIKE '${RLIKE}'$(and_part "$PART_31D")"
    )
    ;;
  *)
    QUERIES=(
      "partition_prune:SELECT count(*) FROM orc_bench.events_ext WHERE event_year=${Y} AND event_month=${M} AND event_day=${D}"
      "filter_high_cardinality:SELECT count(*) FROM orc_bench.events_ext WHERE event_year=${Y} AND event_month=${M} AND event_day=${D} AND (epk_id='${EPK_ID}' OR event_id='${EVENT_ID}')"
      "filter_medium_cardinality:SELECT count(*) FROM orc_bench.events_ext WHERE event_year=${Y} AND event_month=${M} AND event_day=${D} AND (module='${MODULE}' OR name='${NAME}')"
      "filter_low_cardinality:SELECT count(*) FROM orc_bench.events_ext WHERE event_year=${Y} AND event_month=${M} AND event_day=${D} AND channel_type='${CHANNEL}' AND state='${STATUS}'"
      "filter_in:SELECT count(*) FROM orc_bench.events_ext WHERE event_year=${Y} AND event_month=${M} AND event_day=${D} AND event_id IN ('${EVENT_ID}','${EVENT_ID}-missing-a','${EVENT_ID}-missing-b')"
      "filter_timestamp_range:SELECT count(*) FROM orc_bench.events_ext WHERE event_ts >= '${TS_START}' AND event_ts < '${TS_END}'"
      "projection:SELECT count(event_id) FROM orc_bench.events_ext WHERE event_year=${Y} AND event_month=${M} AND event_day=${D}"
      "full_scan:SELECT count(*) FROM orc_bench.events_ext WHERE event_year=${Y} AND event_month=${M} AND event_day=${D}"
      "group_by:SELECT name, state, count(*) FROM orc_bench.events_ext WHERE event_year=${Y} AND event_month=${M} AND event_day=${D} GROUP BY name, state"
      "group_by_heavy:SELECT module, count(*) FROM orc_bench.events_ext WHERE event_year=${Y} AND event_month=${M} GROUP BY module"
      "join_dictionary:SELECT e.name, count(*) FROM orc_bench.events_ext e JOIN orc_bench.dictionary_ext d ON e.name=d.event_name WHERE e.event_year=${Y} AND e.event_month=${M} AND e.event_day=${D} AND d.event_family='featured' GROUP BY e.name"
    )
    ;;
esac

# CSV schema kept compatible with ingest-hive-csv.sh (extra metric column is OK / optional).
# Prefer core columns only for ingest safety; metric goes in a sidecar comment via echo.
echo "scenario,duration_ms,layout_id,engine,cache_state,dataset_label,sla_ok,sla_threshold_ms,format,run_index,warmup" > "$LOCAL_CSV"

append_csv_row() {
  local scenario="$1" duration="$2" warmup_flag="$3" run_index="$4"
  local sla_ok=false
  if (( duration <= SLA_THRESHOLD_MS )); then sla_ok=true; fi
  echo "${scenario},${duration},${LAYOUT},${ENGINE},${CACHE_STATE},${PROFILE},${sla_ok},${SLA_THRESHOLD_MS},orc,${run_index},${warmup_flag}" >> "$LOCAL_CSV"
}

run_once_per_query() {
  local scenario="$1"
  local sql="$2"
  local warmup_flag="$3"
  local run_index="$4"
  local start end duration
  local batch
  batch="$(mktemp /tmp/orc-bench-hive-q-XXXXXX.sql)"
  {
    printf '%s\n' "$INIT_SQL"
    printf '%s;\n' "$sql"
  } > "$batch"
  start=$(date +%s%3N)
  hive_beeline_file "$batch" --silent=true --showHeader=false --outputformat=tsv2 >/dev/null
  end=$(date +%s%3N)
  rm -f "$batch"
  duration=$((end - start))
  append_csv_row "$scenario" "$duration" "$warmup_flag" "$run_index"
}

run_persistent() {
  local suite_sql beeline_out wall_start wall_end wall_ms
  suite_sql="$(mktemp /tmp/orc-bench-hive-suite-XXXXXX.sql)"
  beeline_out="$(mktemp /tmp/orc-bench-hive-suite-XXXXXX.out)"
  {
    printf '%s\n' "$INIT_SQL"
    for entry in "${QUERIES[@]}"; do
      local scenario="${entry%%:*}"
      local sql="${entry#*:}"
      local i
      for ((i = 0; i < WARMUP; i++)); do
        echo "SELECT 'ORC_BENCH_MARK', '${scenario}', ${i}, 'true';"
        echo "${sql};"
      done
      for ((i = 0; i < REPEATS; i++)); do
        echo "SELECT 'ORC_BENCH_MARK', '${scenario}', ${i}, 'false';"
        echo "${sql};"
      done
    done
  } > "$suite_sql"

  echo "Persistent suite SQL: $suite_sql (metric=hive_time_taken)"
  wall_start=$(date +%s%3N)
  set +e
  # Need Time taken lines → do not use --silent
  hive_beeline_file "$suite_sql" --showHeader=false --outputformat=tsv2 >"$beeline_out" 2>&1
  bee_rc=$?
  set -e
  wall_end=$(date +%s%3N)
  wall_ms=$((wall_end - wall_start))
  echo "Persistent beeline rc=$bee_rc wall_suite_ms=$wall_ms"
  cat "$beeline_out"

  local parse_csv
  parse_csv="$(mktemp /tmp/orc-bench-hive-parse-XXXXXX.csv)"
  set +e
  python3 "$ROOT/scripts/hive/parse-beeline-timings.py" "$beeline_out" --header --csv "$parse_csv"
  parse_rc=$?
  set -e
  if (( parse_rc != 0 )); then
    echo "ERROR: failed to parse Time taken from persistent Beeline output" >&2
    rm -f "$suite_sql" "$beeline_out" "$parse_csv"
    exit 1
  fi

  local n=0
  local scenario run_index warmup time_taken_ms
  while IFS=, read -r scenario run_index warmup time_taken_ms; do
    [[ "$scenario" == "scenario" ]] && continue
    append_csv_row "$scenario" "$time_taken_ms" "$warmup" "$run_index"
    n=$((n + 1))
  done < "$parse_csv"
  echo "Parsed $n timed queries; wall_suite_ms=$wall_ms"
  echo "METRIC hive_time_taken (HS2 Time taken); legacy wall-clock only as wall_suite_ms=$wall_ms"
  rm -f "$suite_sql" "$beeline_out" "$parse_csv"
}

case "$SESSION_MODE" in
  persistent)
    run_persistent
    ;;
  per_query)
    echo "Legacy SESSION_MODE=per_query (wall-clock Beeline per query)"
    for entry in "${QUERIES[@]}"; do
      scenario="${entry%%:*}"
      sql="${entry#*:}"
      echo "Scenario $scenario"
      for ((i = 0; i < WARMUP; i++)); do
        run_once_per_query "$scenario" "$sql" "true" "$i"
      done
      for ((i = 0; i < REPEATS; i++)); do
        run_once_per_query "$scenario" "$sql" "false" "$i"
      done
    done
    ;;
  *)
    echo "ERROR: SESSION_MODE must be persistent|per_query (got $SESSION_MODE)" >&2
    exit 1
    ;;
esac

echo "Hive timings written to $LOCAL_CSV"
echo "Ingest into HDFS reports:"
echo "  $ROOT/scripts/hive/ingest-hive-csv.sh $LOCAL_CSV $OUT_DIR"

if [[ -x "$ROOT/scripts/hive/ingest-hive-csv.sh" ]] && command -v hdfs >/dev/null 2>&1; then
  if ! "$ROOT/scripts/hive/ingest-hive-csv.sh" "$LOCAL_CSV" "$OUT_DIR"; then
    echo "ERROR: Hive CSV→parquet ingest failed. Local CSV kept at $LOCAL_CSV" >&2
    echo "  Retry: $ROOT/scripts/hive/ingest-hive-csv.sh $LOCAL_CSV $OUT_DIR" >&2
    exit 1
  fi
elif command -v hdfs >/dev/null 2>&1; then
  echo "WARN: ingest-hive-csv.sh missing; uploading CSV only" >&2
  hdfs dfs -mkdir -p "$OUT_DIR"
  hdfs dfs -put -f "$LOCAL_CSV" "$OUT_DIR/hive_results.csv"
else
  echo "WARN: hdfs CLI missing; results only local: $LOCAL_CSV" >&2
fi

echo "Hive profile $PROFILE complete. Local CSV: $LOCAL_CSV${HIVE_LOG:+; log: $HIVE_LOG}"
