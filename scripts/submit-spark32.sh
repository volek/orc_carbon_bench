#!/usr/bin/env bash
# -----------------------------------------------------------------------------
# Запуск ORC-бенчмарка через кластерный Spark 3.2 (YARN cluster).
#
# Запуск:
#   ./scripts/submit-spark32.sh -- --mode=benchmark --base-path="$BASE"
#   ./scripts/submit-spark32.sh --driver-memory 8g --num-executors 16 -- \
#       --mode=generate --base-path="$BASE" --target-size-tb=0.01
#
# Аргументы:
#   до "--"     флаги spark-submit (--driver-memory, --num-executors, ...)
#   после "--"  аргументы приложения (--mode=..., --base-path=..., ...)
#   без "--"    все аргументы считаются аргументами приложения
#
# Переменные окружения:
#   JAR               fat JAR [build/libs/orc-bench-all.jar]
#   SPARK_SUBMIT      команда submit кластерного Spark 3.2 [spark-submit]
#   NUM_EXECUTORS     число YARN workers/executors [16]
#   EXECUTOR_MEMORY   память executor [8g]
#   EXECUTOR_CORES    ядра на executor [4]
#   DRIVER_MEMORY             память driver [4g]
#   YARN_AM_MEMORY_OVERHEAD   overhead AM-контейнера [512m]
#
# В cluster mode явно задаёт spark.yarn.am.memory из driver-memory (иначе Spark
# наследует executor-memory и AM может занять ~9g при EXECUTOR_MEMORY=8g).
#
# По умолчанию отключает Hive/HBase delegation tokens (ORC не нуждается в Metastore/HBase;
# иначе submit зависает, если сервисы недоступны).
# -----------------------------------------------------------------------------
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
JAR="${JAR:-${JAR32:-$ROOT/build/libs/orc-bench-all.jar}}"
SPARK_SUBMIT="${SPARK_SUBMIT:-spark-submit}"
NUM_EXECUTORS="${NUM_EXECUTORS:-16}"
EXECUTOR_MEMORY="${EXECUTOR_MEMORY:-8g}"
EXECUTOR_CORES="${EXECUTOR_CORES:-4}"
DRIVER_MEMORY="${DRIVER_MEMORY:-4g}"
YARN_AM_MEMORY_OVERHEAD="${YARN_AM_MEMORY_OVERHEAD:-512m}"

if [[ ! -f "$JAR" ]]; then
  echo "Missing $JAR. Build with: ./gradlew build" >&2
  exit 1
fi

SPARK_ARGS=()
APP_ARGS=()
seen_separator=0
has_separator=0
for arg in "$@"; do
  if [[ "$arg" == "--" ]]; then
    has_separator=1
    seen_separator=1
    continue
  fi
  if [[ $seen_separator -eq 1 ]]; then
    APP_ARGS+=("$arg")
  else
    SPARK_ARGS+=("$arg")
  fi
done
if [[ $has_separator -eq 0 ]]; then
  APP_ARGS=("${SPARK_ARGS[@]}")
  SPARK_ARGS=()
fi

spark_args_has() {
  local flag="$1"
  local arg
  for arg in "${SPARK_ARGS[@]+"${SPARK_ARGS[@]}"}"; do
    if [[ "$arg" == "$flag" ]]; then
      return 0
    fi
  done
  return 1
}

spark_conf_has() {
  local key="$1"
  local arg
  for arg in "${SPARK_ARGS[@]+"${SPARK_ARGS[@]}"}"; do
    case "$arg" in
      --conf="${key}"=*)
        return 0
        ;;
    esac
  done
  return 1
}

resolve_driver_memory() {
  local arg i=0
  for arg in "${SPARK_ARGS[@]+"${SPARK_ARGS[@]}"}"; do
    case "$arg" in
      --driver-memory=*)
        echo "${arg#--driver-memory=}"
        return
        ;;
      --driver-memory)
        if [[ $((i + 1)) -lt ${#SPARK_ARGS[@]} ]]; then
          echo "${SPARK_ARGS[$((i + 1))]}"
          return
        fi
        ;;
    esac
    i=$((i + 1))
  done
  echo "$DRIVER_MEMORY"
}

DEFAULT_SPARK_ARGS=()
spark_args_has --num-executors || DEFAULT_SPARK_ARGS+=(--num-executors "$NUM_EXECUTORS")
spark_args_has --executor-memory || DEFAULT_SPARK_ARGS+=(--executor-memory "$EXECUTOR_MEMORY")
spark_args_has --executor-cores || DEFAULT_SPARK_ARGS+=(--executor-cores "$EXECUTOR_CORES")
spark_args_has --driver-memory || DEFAULT_SPARK_ARGS+=(--driver-memory "$DRIVER_MEMORY")

EFFECTIVE_DRIVER_MEMORY="$(resolve_driver_memory)"
spark_conf_has spark.yarn.am.memory || DEFAULT_SPARK_ARGS+=(--conf "spark.yarn.am.memory=${EFFECTIVE_DRIVER_MEMORY}")
spark_conf_has spark.yarn.am.memoryOverhead || DEFAULT_SPARK_ARGS+=(--conf "spark.yarn.am.memoryOverhead=${YARN_AM_MEMORY_OVERHEAD}")

echo "spark-submit resources: num-executors=${NUM_EXECUTORS} executor-memory=${EXECUTOR_MEMORY} executor-cores=${EXECUTOR_CORES} driver-memory=${EFFECTIVE_DRIVER_MEMORY} yarn-am-memory=${EFFECTIVE_DRIVER_MEMORY} yarn-am-overhead=${YARN_AM_MEMORY_OVERHEAD}" >&2

exec "$SPARK_SUBMIT" \
  --master yarn \
  --deploy-mode cluster \
  --conf spark.security.credentials.hive.enabled=false \
  --conf spark.security.credentials.hbase.enabled=false \
  "${DEFAULT_SPARK_ARGS[@]}" \
  "${SPARK_ARGS[@]}" \
  --class ru.sber.orcbench.AppMain \
  "$JAR" \
  "${APP_ARGS[@]}"
