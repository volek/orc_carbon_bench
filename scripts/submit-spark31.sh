#!/usr/bin/env bash
# -----------------------------------------------------------------------------
# Запуск orc-carbon-bench на BYOS Spark 3.1.1 (YARN cluster, существующий HDFS).
# Нужен для CarbonData, generate/validate/benchmark/index-experiment/report.
# Кластерный spark-submit 3.2 здесь не использовать.
#
# Запуск:
#   ./scripts/prepare-spark31.sh
#   ./scripts/submit-spark31.sh -- --mode=generate --base-path="$BASE"
#   ./scripts/submit-spark31.sh --driver-memory 8g --num-executors 16 -- \
#       --mode=benchmark --base-path="$BASE" --seed=42
#
# Аргументы:
#   до "--"     флаги spark-submit (--driver-memory, --num-executors, ...)
#   после "--"  аргументы приложения (--mode=..., --base-path=..., ...)
#   без "--"    все аргументы считаются аргументами приложения
#
# Переменные окружения:
#   SPARK31_HOME         каталог Spark 3.1.1 [корень_репо/dist/spark-3.1.1]
#   JAR31                fat JAR spark31 [build/libs/orc-carbon-bench-spark31-all.jar]
#   HADOOP_CONF_DIR      клиентский Hadoop conf [/etc/hadoop/conf]
#   YARN_CONF_DIR        клиентский YARN conf [тот же, что HADOOP_CONF_DIR]
#   SPARK31_VARIANT      without-hadoop — выставить SPARK_DIST_CLASSPATH
#   SPARK_DIST_CLASSPATH classpath Hadoop; если пусто — $(hadoop classpath)
# -----------------------------------------------------------------------------
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
SPARK_HOME="${SPARK31_HOME:-$ROOT/dist/spark-3.1.1}"
JAR="${JAR31:-$ROOT/build/libs/orc-carbon-bench-spark31-all.jar}"
HADOOP_CONF_DIR="${HADOOP_CONF_DIR:-/etc/hadoop/conf}"
YARN_CONF_DIR="${YARN_CONF_DIR:-$HADOOP_CONF_DIR}"

if [[ ! -x "$SPARK_HOME/bin/spark-submit" ]]; then
  echo "Spark 3.1.1 not found at $SPARK_HOME. Run ./scripts/prepare-spark31.sh first." >&2
  exit 1
fi
if [[ ! -f "$JAR" ]]; then
  echo "Missing $JAR. Build with: ./gradlew :app-spark31:build" >&2
  exit 1
fi

# Never inherit cluster Spark 3.2 conf (spark.yarn.archive / extraClassPath).
unset SPARK_CONF_DIR || true
export SPARK_HOME HADOOP_CONF_DIR YARN_CONF_DIR
if [[ "${SPARK31_VARIANT:-without-hadoop}" == "without-hadoop" && -z "${SPARK_DIST_CLASSPATH:-}" ]]; then
  if command -v hadoop >/dev/null 2>&1; then
    SPARK_DIST_CLASSPATH="$(hadoop classpath)"
    export SPARK_DIST_CLASSPATH
  else
    echo "WARNING: hadoop not on PATH; SPARK_DIST_CLASSPATH is empty" >&2
  fi
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

exec "$SPARK_HOME/bin/spark-submit" \
  --master yarn \
  --deploy-mode cluster \
  --conf spark.sql.extensions=org.apache.spark.sql.CarbonExtensions \
  --conf spark.sql.session.state.builder=org.apache.spark.sql.hive.CarbonSessionStateBuilder \
  "${SPARK_ARGS[@]}" \
  --class ru.sber.orcbench.AppMain \
  "$JAR" \
  "${APP_ARGS[@]}"
