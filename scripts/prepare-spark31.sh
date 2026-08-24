#!/usr/bin/env bash
# Download Apache Spark 3.1.1 (without Hadoop) into dist/spark-3.1.1 for BYOS submit.
# Does not install Spark on the Hadoop cluster.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
DIST_DIR="${SPARK31_DIST_DIR:-$ROOT/dist}"
SPARK_HOME="${SPARK31_HOME:-$DIST_DIR/spark-3.1.1}"
SPARK_VARIANT="${SPARK31_VARIANT:-without-hadoop}"
ARCHIVE_NAME="spark-3.1.1-bin-${SPARK_VARIANT}.tgz"
MIRROR="${SPARK31_MIRROR:-https://archive.apache.org/dist/spark/spark-3.1.1}"
ARCHIVE_URL="${MIRROR}/${ARCHIVE_NAME}"

mkdir -p "$DIST_DIR"
if [[ -x "$SPARK_HOME/bin/spark-submit" ]]; then
  echo "Spark 3.1.1 already present at $SPARK_HOME"
else
  echo "Downloading $ARCHIVE_URL"
  tmp="${DIST_DIR}/${ARCHIVE_NAME}"
  if command -v curl >/dev/null 2>&1; then
    curl -fL --retry 3 -o "$tmp" "$ARCHIVE_URL"
  else
    wget -O "$tmp" "$ARCHIVE_URL"
  fi
  rm -rf "$SPARK_HOME"
  tar -xzf "$tmp" -C "$DIST_DIR"
  extracted="${DIST_DIR}/spark-3.1.1-bin-${SPARK_VARIANT}"
  if [[ "$extracted" != "$SPARK_HOME" ]]; then
    mv "$extracted" "$SPARK_HOME"
  fi
  rm -f "$tmp"
  echo "Extracted Spark 3.1.1 to $SPARK_HOME"
fi

copy_hive_site() {
  local src="$1"
  if [[ -f "$src" ]]; then
    mkdir -p "$SPARK_HOME/conf"
    cp -f "$src" "$SPARK_HOME/conf/hive-site.xml"
    echo "Copied hive-site.xml from $src"
    return 0
  fi
  return 1
}

if [[ ! -f "$SPARK_HOME/conf/hive-site.xml" ]]; then
  copy_hive_site "${HIVE_CONF_DIR:-/etc/hive/conf}/hive-site.xml" \
    || copy_hive_site "${HADOOP_CONF_DIR:-/etc/hadoop/conf}/hive-site.xml" \
    || copy_hive_site "/etc/spark/conf/hive-site.xml" \
    || echo "WARNING: hive-site.xml not found. CarbonData usually needs a client hive-site.xml in $SPARK_HOME/conf/"
fi

echo
echo "Next:"
echo "  export SPARK31_HOME=$SPARK_HOME"
echo "  ./scripts/submit-spark31.sh -- --mode=generate ..."
