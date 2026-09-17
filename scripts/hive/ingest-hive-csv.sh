#!/usr/bin/env bash
# Convert Hive CSV timings to parquet for ReportRunner (same schema subset).
# Usage: ./scripts/hive/ingest-hive-csv.sh <hdfs-or-local-csv> <hdfs-parquet-out-dir>
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
CSV_PATH="${1:?csv path}"
OUT_PATH="${2:?parquet output dir}"

"$ROOT/scripts/submit-spark32.sh" -- \
  --mode=report \
  --base-path="${BASE:-hdfs:///user/hdfs_migration_user/orc_test}" \
  --report-name=hive-ingest-noop \
  2>/dev/null || true

# Dedicated tiny ingest via spark-sql if available; else spark-submit with inline is heavy.
# Prefer spark-shell non-interactive:
SPARK_SHELL="${SPARK_SHELL:-spark-shell}"
if ! command -v "$SPARK_SHELL" >/dev/null 2>&1; then
  echo "spark-shell not found; leave CSV at $CSV_PATH for manual ingest." >&2
  exit 0
fi

"$SPARK_SHELL" --conf spark.ui.enabled=false <<EOF
val csvPath = "$CSV_PATH"
val outPath = "$OUT_PATH"
val raw = spark.read.option("header","true").option("inferSchema","true").csv(csvPath)
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
measured.coalesce(1).write.mode("overwrite").parquet(outPath)
System.exit(0)
EOF

echo "Ingested Hive CSV → $OUT_PATH"
