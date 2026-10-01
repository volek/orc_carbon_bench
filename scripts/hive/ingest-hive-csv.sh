#!/usr/bin/env bash
# -----------------------------------------------------------------------------
# Upload Hive timing CSV to HDFS (retry) and convert to parquet for ReportRunner.
#
# Usage:
#   ./scripts/hive/ingest-hive-csv.sh <local-or-hdfs-csv> <hdfs-parquet-out-dir>
#
# Writes:
#   <out-dir>/          parquet dataset (_SUCCESS + part-*.parquet)
#   <out-dir>/hive_results.csv   copy of source CSV (re-put after parquet overwrite)
#
# Env:
#   HDFS_RETRIES   attempts for hdfs dfs mkdir/put/test [8]
#   HDFS_RETRY_SLEEP_SEC  pause between attempts [3]
#   SPARK_SUBMIT   [spark-submit]
# -----------------------------------------------------------------------------
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
CSV_PATH="${1:?csv path (local file or hdfs://...)}"
OUT_PATH="${2:?hdfs parquet output dir}"
HDFS_RETRIES="${HDFS_RETRIES:-8}"
HDFS_RETRY_SLEEP_SEC="${HDFS_RETRY_SLEEP_SEC:-3}"
SPARK_SUBMIT="${SPARK_SUBMIT:-spark-submit}"

hdfs_retry() {
  local attempt=1
  local rc=0
  while (( attempt <= HDFS_RETRIES )); do
    if "$@"; then
      return 0
    fi
    rc=$?
    echo "WARN: hdfs command failed (attempt $attempt/$HDFS_RETRIES, rc=$rc): $*" >&2
    sleep "$HDFS_RETRY_SLEEP_SEC"
    attempt=$((attempt + 1))
  done
  echo "ERROR: hdfs command failed after $HDFS_RETRIES attempts: $*" >&2
  return "$rc"
}

resolve_local_csv() {
  local src="$1"
  if [[ "$src" == hdfs://* || "$src" == viewfs://* ]]; then
    echo ""
    return
  fi
  if [[ "$src" == file://* ]]; then
    src="${src#file://}"
  fi
  if [[ -f "$src" ]]; then
    readlink -f "$src" 2>/dev/null || realpath "$src" 2>/dev/null || echo "$src"
    return
  fi
  echo "ERROR: local CSV not found: $src" >&2
  exit 1
}

LOCAL_CSV="$(resolve_local_csv "$CSV_PATH")"
HDFS_CSV="$OUT_PATH/hive_results.csv"

echo "Hive ingest: csv=$CSV_PATH → parquet=$OUT_PATH"

if [[ -n "$LOCAL_CSV" ]]; then
  echo "Uploading local CSV $LOCAL_CSV → $HDFS_CSV"
  hdfs_retry hdfs dfs -mkdir -p "$OUT_PATH"
  hdfs_retry hdfs dfs -put -f "$LOCAL_CSV" "$HDFS_CSV"
elif [[ "$CSV_PATH" == hdfs://* || "$CSV_PATH" == viewfs://* ]]; then
  HDFS_CSV="$CSV_PATH"
  if ! hdfs_retry hdfs dfs -test -e "$HDFS_CSV"; then
    echo "ERROR: HDFS CSV missing: $HDFS_CSV" >&2
    exit 1
  fi
  hdfs_retry hdfs dfs -mkdir -p "$OUT_PATH"
else
  echo "ERROR: unsupported CSV path: $CSV_PATH" >&2
  exit 1
fi

if ! hdfs_retry hdfs dfs -test -e "$HDFS_CSV"; then
  echo "ERROR: CSV not on HDFS after upload: $HDFS_CSV" >&2
  exit 1
fi
echo "CSV ready on HDFS: $HDFS_CSV ($(hdfs dfs -du -h "$HDFS_CSV" | awk '{print $1,$2}'))"

PY_SCRIPT="$(mktemp /tmp/orc-bench-hive-ingest-XXXXXX.py)"
cleanup() { rm -f "$PY_SCRIPT"; }
trap cleanup EXIT

cat > "$PY_SCRIPT" <<'PY'
import sys
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

csv_path, out_path = sys.argv[1], sys.argv[2]
spark = (
    SparkSession.builder.appName("orc-bench-hive-ingest")
    .config("spark.ui.enabled", "false")
    .getOrCreate()
)
try:
    raw = spark.read.option("header", "true").option("inferSchema", "true").csv(csv_path)
    if raw.rdd.isEmpty():
        raise RuntimeError("Hive CSV is empty: " + csv_path)
    enriched = (
        raw.withColumn("run_id", F.lit(str(__import__("uuid").uuid4())))
        .withColumn("rows_returned", F.lit(0))
        .withColumn("total_rows", F.lit(0))
        .withColumn("selectivity", F.lit(0.0))
        .withColumn("bytes_read", F.lit(0))
        .withColumn("records_read", F.lit(0))
        .withColumn("seed", F.lit(42))
        .withColumn("orc_path", F.lit(""))
        .withColumn("orc_bloom_columns", F.lit("none"))
        .withColumn("executed_at", F.lit(__import__("datetime").datetime.utcnow().isoformat() + "Z"))
        .withColumn("spark_version", F.lit("hive"))
        .withColumn("spark_runtime", F.lit("hive-tez-llap"))
        .withColumn("scan_ratio", F.lit(0.0))
        .withColumn("warmup", F.col("warmup").cast("boolean"))
        .withColumn("sla_ok", F.col("sla_ok").cast("boolean"))
        .withColumn("duration_ms", F.col("duration_ms").cast("long"))
        .withColumn("run_index", F.col("run_index").cast("int"))
        .withColumn("sla_threshold_ms", F.col("sla_threshold_ms").cast("long"))
    )
    measured = enriched.filter(F.col("warmup") == False)  # noqa: E712
    count = measured.count()
    if count == 0:
        raise RuntimeError("No non-warmup rows in Hive CSV: " + csv_path)
    measured.coalesce(1).write.mode("overwrite").parquet(out_path)
    print("Wrote %d measured Hive rows to %s" % (count, out_path))
finally:
    spark.stop()
PY

# Client mode: driver on edge; reads HDFS CSV (already verified) and writes parquet.
set +e
"$SPARK_SUBMIT" \
  --master yarn \
  --deploy-mode client \
  --name orc-bench-hive-ingest \
  --num-executors "${NUM_EXECUTORS:-2}" \
  --executor-memory "${EXECUTOR_MEMORY:-2g}" \
  --executor-cores "${EXECUTOR_CORES:-1}" \
  --driver-memory "${DRIVER_MEMORY:-2g}" \
  --conf spark.ui.enabled=false \
  --conf spark.security.credentials.hive.enabled=false \
  --conf spark.security.credentials.hbase.enabled=false \
  "$PY_SCRIPT" "$HDFS_CSV" "$OUT_PATH"
SUBMIT_RC=$?
set -e

if (( SUBMIT_RC != 0 )); then
  echo "WARN: pyspark ingest failed (rc=$SUBMIT_RC); trying spark-shell fallback" >&2
  SPARK_SHELL="${SPARK_SHELL:-spark-shell}"
  if ! command -v "$SPARK_SHELL" >/dev/null 2>&1; then
    echo "ERROR: spark-submit hive ingest failed and spark-shell not found" >&2
    exit "$SUBMIT_RC"
  fi
  SHELL_LOG="$(mktemp /tmp/orc-bench-hive-ingest-shell-XXXXXX.log)"
  set +e
  "$SPARK_SHELL" --conf spark.ui.enabled=false \
    --conf spark.security.credentials.hive.enabled=false \
    --conf spark.security.credentials.hbase.enabled=false \
    >"$SHELL_LOG" 2>&1 <<EOF
val csvPath = "$HDFS_CSV"
val outPath = "$OUT_PATH"
val raw = spark.read.option("header","true").option("inferSchema","true").csv(csvPath)
if (raw.head(1).isEmpty) { throw new RuntimeException("Hive CSV is empty: " + csvPath) }
val enriched = raw
  .withColumn("run_id", org.apache.spark.sql.functions.lit(java.util.UUID.randomUUID.toString))
  .withColumn("rows_returned", org.apache.spark.sql.functions.lit(0L))
  .withColumn("total_rows", org.apache.spark.sql.functions.lit(0L))
  .withColumn("selectivity", org.apache.spark.sql.functions.lit(0.0))
  .withColumn("bytes_read", org.apache.spark.sql.functions.lit(0L))
  .withColumn("records_read", org.apache.spark.sql.functions.lit(0L))
  .withColumn("seed", org.apache.spark.sql.functions.lit(42L))
  .withColumn("orc_path", org.apache.spark.sql.functions.lit(""))
  .withColumn("orc_bloom_columns", org.apache.spark.sql.functions.lit("none"))
  .withColumn("executed_at", org.apache.spark.sql.functions.lit(java.time.Instant.now.toString))
  .withColumn("spark_version", org.apache.spark.sql.functions.lit("hive"))
  .withColumn("spark_runtime", org.apache.spark.sql.functions.lit("hive-tez-llap"))
  .withColumn("scan_ratio", org.apache.spark.sql.functions.lit(0.0))
  .withColumn("warmup", org.apache.spark.sql.functions.col("warmup").cast("boolean"))
  .withColumn("sla_ok", org.apache.spark.sql.functions.col("sla_ok").cast("boolean"))
  .withColumn("duration_ms", org.apache.spark.sql.functions.col("duration_ms").cast("long"))
  .withColumn("run_index", org.apache.spark.sql.functions.col("run_index").cast("int"))
  .withColumn("sla_threshold_ms", org.apache.spark.sql.functions.col("sla_threshold_ms").cast("long"))
val measured = enriched.filter(org.apache.spark.sql.functions.col("warmup") === false)
val n = measured.count()
if (n == 0) { throw new RuntimeException("No non-warmup rows in Hive CSV: " + csvPath) }
measured.coalesce(1).write.mode("overwrite").parquet(outPath)
println("Wrote " + n + " measured Hive rows to " + outPath)
System.exit(0)
EOF
  SHELL_RC=$?
  set -e
  if (( SHELL_RC != 0 )) || grep -qE 'FileNotFoundException|ERROR [a-zA-Z.]+Exception|Caused by:.*Exception' "$SHELL_LOG"; then
    echo "ERROR: spark-shell hive ingest failed (rc=$SHELL_RC). Log: $SHELL_LOG" >&2
    tail -n 40 "$SHELL_LOG" >&2 || true
    exit 1
  fi
  rm -f "$SHELL_LOG"
fi

# overwrite parquet removes hive_results.csv — restore CSV artifact
if [[ -n "$LOCAL_CSV" ]]; then
  hdfs_retry hdfs dfs -put -f "$LOCAL_CSV" "$HDFS_CSV" || true
elif [[ "$CSV_PATH" != "$HDFS_CSV" ]]; then
  hdfs_retry hdfs dfs -cp -f "$CSV_PATH" "$HDFS_CSV" || true
fi

if ! hdfs dfs -test -e "$OUT_PATH/_SUCCESS" && ! hdfs dfs -ls "$OUT_PATH" 2>/dev/null | grep -q '\.parquet'; then
  echo "ERROR: parquet output missing under $OUT_PATH after ingest" >&2
  exit 1
fi

echo "Ingested Hive CSV → parquet $OUT_PATH (csv=$HDFS_CSV)"
