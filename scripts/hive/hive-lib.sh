#!/usr/bin/env bash
# Shared helpers for Hive factor / diagnose / explain scripts.
# shellcheck disable=SC2034

hive_sed_repl() {
  printf '%s' "$1" | sed -e 's/[\\#&]/\\&/g'
}

# Inclusive start date YYYY-MM-DD, exclusive end → Hive partition predicate.
# Uses GNU date (-d). Falls back to event_ts-only empty predicate parts if date fails.
hive_partition_predicate() {
  local start="$1" end="$2"
  local d parts=()
  local start_day end_day
  start_day="${start%% *}"
  end_day="${end%% *}"
  if ! d=$(date -d "$start_day" +%Y-%m-%d 2>/dev/null); then
    echo ""
    return 0
  fi
  while true; do
    local cur y m day
    cur=$(date -d "$d" +%Y-%m-%d)
    # stop when cur >= end_day
    if [[ "$(date -d "$cur" +%s)" -ge "$(date -d "$end_day" +%s)" ]]; then
      break
    fi
    y=$(date -d "$cur" +%Y)
    m=$(date -d "$cur" +%-m)
    day=$(date -d "$cur" +%-d)
    parts+=("(event_year=${y} AND event_month=${m} AND event_day=${day})")
    d=$(date -d "$cur + 1 day" +%Y-%m-%d)
    # safety
    if (( ${#parts[@]} > 400 )); then
      break
    fi
  done
  if (( ${#parts[@]} == 0 )); then
    echo ""
    return 0
  fi
  local joined
  joined=$(printf '%s OR ' "${parts[@]}")
  joined="${joined% OR }"
  echo "(${joined})"
}

hive_profile_session_init() {
  local profile="$1"
  SESSION_INIT=()
  CACHE_STATE="cold"
  ENGINE="hive_tez"
  case "$profile" in
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
        "SET hive.vectorized.execution.reduce.enabled=true;"
        "SET hive.optimize.ppd=true;"
        "SET hive.optimize.index.filter=true;"
        "SET hive.cbo.enable=false;"
        "SET hive.llap.execution.mode=none;"
        "SET tez.am.container.reuse.enabled=true;"
      )
      ;;
    h2)
      SESSION_INIT+=(
        "SET hive.execution.engine=tez;"
        "SET hive.vectorized.execution.enabled=true;"
        "SET hive.vectorized.execution.reduce.enabled=true;"
        "SET hive.optimize.ppd=true;"
        "SET hive.optimize.index.filter=true;"
        "SET hive.cbo.enable=true;"
        "SET hive.compute.query.using.stats=true;"
        "SET hive.llap.execution.mode=none;"
        "SET tez.am.container.reuse.enabled=true;"
      )
      ;;
    h2f)
      ENGINE="hive_tez"
      CACHE_STATE="cold"
      SESSION_INIT+=(
        "SET hive.execution.engine=tez;"
        "SET hive.vectorized.execution.enabled=true;"
        "SET hive.vectorized.execution.reduce.enabled=true;"
        "SET hive.optimize.ppd=true;"
        "SET hive.optimize.index.filter=true;"
        "SET hive.cbo.enable=true;"
        "SET hive.compute.query.using.stats=true;"
        "SET hive.fetch.task.conversion=more;"
        "SET hive.llap.execution.mode=none;"
        "SET tez.am.container.reuse.enabled=true;"
      )
      ;;
    h3)
      ENGINE="hive_llap"
      CACHE_STATE="cold"
      SESSION_INIT+=(
        "SET hive.execution.engine=tez;"
        "SET hive.vectorized.execution.enabled=true;"
        "SET hive.vectorized.execution.reduce.enabled=true;"
        "SET hive.optimize.ppd=true;"
        "SET hive.optimize.index.filter=true;"
        "SET hive.cbo.enable=true;"
        "SET hive.compute.query.using.stats=true;"
        "SET hive.llap.execution.mode=all;"
        "SET hive.llap.io.enabled=true;"
        "SET tez.am.container.reuse.enabled=true;"
      )
      ;;
    h4)
      ENGINE="hive_llap"
      CACHE_STATE="warm"
      SESSION_INIT+=(
        "SET hive.execution.engine=tez;"
        "SET hive.vectorized.execution.enabled=true;"
        "SET hive.vectorized.execution.reduce.enabled=true;"
        "SET hive.optimize.ppd=true;"
        "SET hive.optimize.index.filter=true;"
        "SET hive.cbo.enable=true;"
        "SET hive.compute.query.using.stats=true;"
        "SET hive.llap.execution.mode=all;"
        "SET hive.llap.io.enabled=true;"
        "SET tez.am.container.reuse.enabled=true;"
      )
      ;;
    *)
      echo "Unknown Hive profile: $profile (h0|h1|h2|h2f|h3|h4)" >&2
      return 1
      ;;
  esac
}

hive_beeline_file() {
  local sql_file="$1"
  shift
  local extra_opts=("$@")
  if [[ -n "${HIVE_JDBC_URL:-}" ]]; then
    "$BEELINE" -u "$HIVE_JDBC_URL" "${extra_opts[@]}" -f "$sql_file"
  else
    "$BEELINE" "${extra_opts[@]}" -f "$sql_file"
  fi
}

hive_beeline_cmd() {
  local sql="$1"
  shift
  local extra_opts=("$@")
  if [[ -n "${HIVE_JDBC_URL:-}" ]]; then
    "$BEELINE" -u "$HIVE_JDBC_URL" "${extra_opts[@]}" -e "$sql"
  else
    "$BEELINE" "${extra_opts[@]}" -e "$sql"
  fi
}
