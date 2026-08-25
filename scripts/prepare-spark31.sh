#!/usr/bin/env bash
# -----------------------------------------------------------------------------
# Подготовка локального Apache Spark 3.1.1 (without-hadoop) на edge-ноде.
#
# Склеивает части dist/spark-3.1.1-bin-without-hadoop.tgz.part-NN (лимит GitHub
# 100 МБ), распаковывает в dist/spark-3.1.1/, ставит Hive jars из
# dist/spark-3.1.1-hive-jars.tgz в $SPARK_HOME/jars/ (в without-hadoop
# нет spark-hive / hive-exec) и копирует клиентский hive-site.xml.
# Из BYOS-копии hive-site.xml убирает credential.provider.path на *.jceks
# (часто Permission denied у edge-пользователя). Ничего не скачивает и не ставит
# Spark в Ambari / SDP.
#
# Запуск:
#   ./scripts/prepare-spark31.sh
#
# Переменные окружения:
#   SPARK31_HOME      каталог Spark после распаковки
#                     [корень_репо/dist/spark-3.1.1]
#   SPARK31_DIST_DIR  каталог с архивом и частями [корень_репо/dist]
#   SPARK31_ARCHIVE   полный путь к уже склеенному Spark .tgz (если есть)
#   SPARK31_HIVE_ARCHIVE  полный путь к hive-jars .tgz
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
HIVE_ARCHIVE_NAME="spark-3.1.1-hive-jars.tgz"

search_paths() {
  printf '%s\n' \
    "$DIST_DIR" \
    "$ROOT" \
    "$ROOT/build/libs" \
    "$SCRIPT_DIR" \
    "$PWD" \
    "$PWD/dist" \
    "$PWD/build/libs"
}

find_file() {
  local name="$1"
  local extra="${2:-}"
  local candidate
  for candidate in \
    "$extra" \
    "$DIST_DIR/$name" \
    "$ROOT/$name" \
    "$ROOT/build/libs/$name" \
    "$SCRIPT_DIR/$name" \
    "$PWD/$name" \
    "$PWD/dist/$name" \
    "$PWD/build/libs/$name"
  do
    if [[ -n "$candidate" && -f "$candidate" ]]; then
      echo "$candidate"
      return 0
    fi
  done
  return 1
}

find_parts_dir() {
  local prefix="$1"
  local dir
  while IFS= read -r dir; do
    if compgen -G "$dir/${prefix}.part-*" > /dev/null; then
      echo "$dir"
      return 0
    fi
  done < <(search_paths)
  return 1
}

join_parts() {
  local parts_dir="$1"
  local archive_name="$2"
  local dest="$DIST_DIR/$archive_name"
  mkdir -p "$DIST_DIR"
  echo "Joining $archive_name parts from $parts_dir" >&2
  # shellcheck disable=SC2086
  cat "$parts_dir"/${archive_name}.part-* > "$dest"
  echo "Joined $dest ($(wc -c < "$dest") bytes)" >&2
  echo "$dest"
}

resolve_archive() {
  local archive_name="$1"
  local env_override="$2"
  local label="$3"
  local archive
  archive="$(find_file "$archive_name" "$env_override" || true)"
  if [[ -z "$archive" ]]; then
    local parts_dir
    parts_dir="$(find_parts_dir "$archive_name" || true)"
    if [[ -n "$parts_dir" ]]; then
      archive="$(join_parts "$parts_dir" "$archive_name")"
    fi
  fi
  if [[ -z "${archive:-}" ]]; then
    echo "$label archive $archive_name not found (and no $archive_name.part-* chunks)." >&2
    echo "Clone the repo (parts live in dist/) or copy them from the build machine." >&2
    echo "Do not download on the edge node." >&2
    exit 1
  fi
  if [[ ! -f "$archive" ]]; then
    echo "$label archive path is not a file: $archive" >&2
    exit 1
  fi
  echo "$archive"
}

mkdir -p "$DIST_DIR"
if [[ -x "$SPARK_HOME/bin/spark-submit" ]]; then
  echo "Spark 3.1.1 already present at $SPARK_HOME"
else
  ARCHIVE="$(resolve_archive "$ARCHIVE_NAME" "${SPARK31_ARCHIVE:-}" "Spark")"
  echo "Extracting $ARCHIVE"
  rm -rf "$SPARK_HOME"
  tar -xzf "$ARCHIVE" -C "$DIST_DIR"
  extracted="${DIST_DIR}/spark-3.1.1-bin-${SPARK_VARIANT}"
  if [[ "$extracted" != "$SPARK_HOME" ]]; then
    mv "$extracted" "$SPARK_HOME"
  fi
  echo "Extracted Spark 3.1.1 to $SPARK_HOME"
fi

# without-hadoop does not ship spark-hive / hive-exec; install from dist tarball.
install_hive_jars() {
  local marker="$SPARK_HOME/jars/spark-hive_2.12-3.1.1.jar"
  if [[ -f "$marker" ]]; then
    echo "Hive jars already present in $SPARK_HOME/jars"
    return 0
  fi
  local hive_archive
  hive_archive="$(find_file "$HIVE_ARCHIVE_NAME" "${SPARK31_HIVE_ARCHIVE:-}" || true)"
  if [[ -z "$hive_archive" ]]; then
    echo "Hive jars archive $HIVE_ARCHIVE_NAME not found." >&2
    echo "Clone the repo (file lives in dist/) or copy it from the build machine." >&2
    echo "Do not download Hive jars on the edge node." >&2
    exit 1
  fi
  echo "Installing Hive jars from $hive_archive into $SPARK_HOME/jars"
  mkdir -p "$SPARK_HOME/jars"
  tar -xzf "$hive_archive" -C "$SPARK_HOME/jars"
  if [[ ! -f "$marker" ]]; then
    echo "ERROR: expected $marker after extracting Hive jars archive." >&2
    echo "Rebuild on a machine with internet: ./gradlew downloadSpark31HiveJars" >&2
    exit 1
  fi
  echo "Installed Hive jars into $SPARK_HOME/jars ($(ls -1 "$SPARK_HOME/jars"/spark-hive*.jar 2>/dev/null | wc -l) spark-hive jar(s))"
}
install_hive_jars

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

# Drop jceks credential provider from BYOS hive-site only (system conf untouched).
# Otherwise Spark SecurityManager / SSLOptions fails with Permission denied on
# /usr/sdp/current/hive-client/conf/hive-site.jceks for typical edge users.
sanitize_hive_site() {
  local hive_site="$1"
  local tmp
  [[ -f "$hive_site" ]] || return 0
  if ! grep -q 'hadoop\.security\.credential\.provider\.path' "$hive_site"; then
    return 0
  fi
  tmp="$(mktemp)"
  awk '
    /<property>/ { in_prop=1; block=$0; next }
    in_prop {
      block = block "\n" $0
      if ($0 ~ /<\/property>/) {
        if (block !~ /hadoop\.security\.credential\.provider\.path/)
          print block
        in_prop=0
        next
      }
      next
    }
    { print }
  ' "$hive_site" > "$tmp"
  mv "$tmp" "$hive_site"
  echo "Removed hadoop.security.credential.provider.path from $hive_site"
}

if [[ ! -f "$SPARK_HOME/conf/hive-site.xml" ]]; then
  copy_conf "${HIVE_CONF_DIR:-/etc/hive/conf}/hive-site.xml" hive-site.xml \
    || copy_conf "${HADOOP_CONF_DIR:-/etc/hadoop/conf}/hive-site.xml" hive-site.xml \
    || copy_conf "/etc/spark/conf/hive-site.xml" hive-site.xml \
    || copy_conf "${HADOOP_CONF_DIR:-/etc/hadoop/conf}/hive-site.xml" hive-site.xml \
    || echo "WARNING: hive-site.xml not found. CarbonData usually needs a client hive-site.xml in $SPARK_HOME/conf/"
fi
sanitize_hive_site "$SPARK_HOME/conf/hive-site.xml"

echo
echo "Next:"
echo "  export SPARK31_HOME=$SPARK_HOME"
echo "  ./scripts/submit-spark31.sh -- --mode=generate ..."
