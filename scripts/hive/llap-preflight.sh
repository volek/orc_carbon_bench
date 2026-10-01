#!/usr/bin/env bash
# -----------------------------------------------------------------------------
# LLAP preflight for Hive profiles h3/h4.
# Exit 0 if LLAP looks available; non-zero otherwise.
#
#   ./scripts/hive/llap-preflight.sh
#   SKIP_LLAP_PREFLIGHT=1  — callers may skip; this script always checks.
# -----------------------------------------------------------------------------
set -euo pipefail

echo "=== LLAP preflight ==="

found=0

if command -v llapstatus >/dev/null 2>&1; then
  echo "Running: llapstatus"
  set +e
  out="$(llapstatus 2>&1)"
  rc=$?
  set -e
  echo "$out" | tail -n 40
  if (( rc == 0 )) && echo "$out" | grep -qiE 'RUNNING|instances.*[1-9]|AMState.*RUNNING'; then
    echo "OK: llapstatus reports running daemons"
    found=1
  else
    echo "WARN: llapstatus did not clearly report RUNNING (rc=$rc)"
  fi
else
  echo "INFO: llapstatus CLI not on PATH"
fi

if (( found == 0 )) && command -v yarn >/dev/null 2>&1; then
  echo "Scanning yarn application -list for LLAP..."
  set +e
  yarn_out="$(yarn application -list 2>/dev/null | grep -iE 'llap|llapdaemon' || true)"
  set -e
  if [[ -n "$yarn_out" ]]; then
    echo "$yarn_out"
    if echo "$yarn_out" | grep -qi 'RUNNING'; then
      echo "OK: found RUNNING LLAP-related YARN application"
      found=1
    fi
  else
    echo "INFO: no LLAP-named YARN apps in list"
  fi
fi

if (( found == 0 )); then
  echo "ERROR: LLAP daemon not detected. Start LLAP on the cluster before h3/h4," >&2
  echo "  or re-run with SKIP_LLAP_PREFLIGHT=1 (results will not be true LLAP)." >&2
  exit 1
fi

echo "=== LLAP preflight passed ==="
exit 0
