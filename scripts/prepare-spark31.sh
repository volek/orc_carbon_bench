#!/usr/bin/env bash
# -----------------------------------------------------------------------------
# Подготовка локального Apache Spark 3.1.1 (without-hadoop) на edge-ноде.
#
# Склеивает части dist/spark-3.1.1-bin-without-hadoop.tgz.part-NN (лимит GitHub
# 100 МБ), распаковывает в dist/spark-3.1.1/ и копирует клиентский hive-site.xml.
# Ничего не скачивает и не ставит Spark в Ambari / SDP.
#
# Запуск:
#   ./scripts/prepare-spark31.sh
#
# Переменные окружения:
#   SPARK31_HOME      каталог Spark после распаковки
#                     [корень_репо/dist/spark-3.1.1]
#   SPARK31_DIST_DIR  каталог с архивом и частями [корень_репо/dist]
#   SPARK31_ARCHIVE   полный путь к уже склеенному .tgz (если есть)
#   SPARK31_VARIANT   суффикс дистрибутива [without-hadoop]
#   HIVE_CONF_DIR     откуда брать hive-site.xml [/etc/hive/conf]
#   HADOOP_CONF_DIR   запасной путь к hive-site.xml [/etc/hadoop/conf]
# -----------------------------------------------------------------------------
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

DIST_DIR="${SPARK31_DIST_DIR:-$ROOT/dist}"
SPARK_HOME="${SPARK31_HOME:-$DIST_DIR/spark-3.1.1}"
SPARK_VARIANT="${SPARK31_VARIANT:-without-hadoop}"
ARCHIVE_NAME="spark-3.1.1-bin-${SPARK_VARIANT}.tgz"
PART_GLOB="${ARCHIVE_NAME}.part-*"

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

find_parts_dir() {
  local dir
  for dir in \
    "$DIST_DIR" \
    "$ROOT" \
    "$ROOT/build/libs" \
    "$SCRIPT_DIR" \
    "$PWD" \
    "$PWD/dist" \
    "$PWD/build/libs"
  do
    if compgen -G "$dir/${ARCHIVE_NAME}.part-*" > /dev/null; then
      echo "$dir"
      return 0
    fi
  done
  return 1
}

join_parts() {
  local parts_dir="$1"
  local dest="$DIST_DIR/$ARCHIVE_NAME"
  mkdir -p "$DIST_DIR"
  echo "Joining Spark archive parts from $parts_dir" >&2
  # shellcheck disable=SC2086
  cat "$parts_dir"/$PART_GLOB > "$dest"
  echo "Joined $dest ($(wc -c < "$dest") bytes)" >&2
  echo "$dest"
}

mkdir -p "$DIST_DIR"
if [[ -x "$SPARK_HOME/bin/spark-submit" ]]; then
  echo "Spark 3.1.1 already present at $SPARK_HOME"
else
  ARCHIVE="$(find_archive || true)"
  if [[ -z "$ARCHIVE" ]]; then
    PARTS_DIR="$(find_parts_dir || true)"
    if [[ -n "$PARTS_DIR" ]]; then
      ARCHIVE="$(join_parts "$PARTS_DIR")"
    fi
  fi
  if [[ -z "${ARCHIVE:-}" ]]; then
    echo "Spark archive $ARCHIVE_NAME not found (and no $ARCHIVE_NAME.part-* chunks)." >&2
    echo "Clone the repo (parts live in dist/) or copy them from the build machine." >&2
    echo "Do not download Spark on the edge node." >&2
    exit 1
  fi
  if [[ ! -f "$ARCHIVE" ]]; then
    echo "Spark archive path is not a file: $ARCHIVE" >&2
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
