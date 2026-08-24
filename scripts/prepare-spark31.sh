#!/usr/bin/env bash
# Unpack bundled Apache Spark 3.1.1 (without Hadoop) for BYOS submit.
# Does not download anything and does not install Spark on the Hadoop cluster.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

DIST_DIR="${SPARK31_DIST_DIR:-$ROOT/dist}"
SPARK_HOME="${SPARK31_HOME:-$DIST_DIR/spark-3.1.1}"
SPARK_VARIANT="${SPARK31_VARIANT:-without-hadoop}"
ARCHIVE_NAME="spark-3.1.1-bin-${SPARK_VARIANT}.tgz"

find_archive() {
  local candidate
  for candidate in \
    "${SPARK31_ARCHIVE:-}" \
    "$DIST_DIR/$ARCHIVE_NAME" \
    "$ROOT/$ARCHIVE_NAME" \
    "$ROOT/build/libs/$ARCHIVE_NAME" \
    "$SCRIPT_DIR/$ARCHIVE_NAME" \
    "$PWD/$ARCHIVE_NAME" \
    "$PWD/dist/$ARCHIVE_NAME" \
    "$PWD/build/libs/$ARCHIVE_NAME"
  do
    if [[ -n "$candidate" && -f "$candidate" ]]; then
      echo "$candidate"
      return 0
    fi
  done
  return 1
}

mkdir -p "$DIST_DIR"
if [[ -x "$SPARK_HOME/bin/spark-submit" ]]; then
  echo "Spark 3.1.1 already present at $SPARK_HOME"
else
  ARCHIVE="$(find_archive || true)"
  if [[ -z "$ARCHIVE" ]]; then
    echo "Spark archive $ARCHIVE_NAME not found." >&2
    echo "Copy it from the build machine (dist/ or build/libs/) into dist/, this directory, or next to this script." >&2
    echo "Do not download Spark on the edge node." >&2
    exit 1
  fi
  echo "Extracting $ARCHIVE"
  rm -rf "$SPARK_HOME"
  tar -xzf "$ARCHIVE" -C "$DIST_DIR"
  extracted="${DIST_DIR}/spark-3.1.1-bin-${SPARK_VARIANT}"
  if [[ "$extracted" != "$SPARK_HOME" ]]; then
    mv "$extracted" "$SPARK_HOME"
  fi
  echo "Extracted Spark 3.1.1 to $SPARK_HOME"
fi

copy_conf() {
  local src="$1"
  local dest_name="$2"
  if [[ -f "$src" ]]; then
    mkdir -p "$SPARK_HOME/conf"
    cp -f "$src" "$SPARK_HOME/conf/$dest_name"
    echo "Copied $dest_name from $src"
    return 0
  fi
  return 1
}

if [[ ! -f "$SPARK_HOME/conf/hive-site.xml" ]]; then
  copy_conf "${HIVE_CONF_DIR:-/etc/hive/conf}/hive-site.xml" hive-site.xml \
    || copy_conf "${HADOOP_CONF_DIR:-/etc/hadoop/conf}/hive-site.xml" hive-site.xml \
    || copy_conf "/etc/spark/conf/hive-site.xml" hive-site.xml \
    || copy_conf "${HADOOP_CONF_DIR:-/etc/hadoop/conf}/hive-site.xml" hive-site.xml \
    || echo "WARNING: hive-site.xml not found. CarbonData usually needs a client hive-site.xml in $SPARK_HOME/conf/"
fi

echo
echo "Next:"
echo "  export SPARK31_HOME=$SPARK_HOME"
echo "  ./scripts/submit-spark31.sh -- --mode=generate ..."
