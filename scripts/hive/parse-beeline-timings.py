#!/usr/bin/env python3
"""Parse Beeline/HS2 output for ORC_BENCH_MARK anchors + Time taken lines.

Expected SQL pattern (persistent session):
  SELECT 'ORC_BENCH_MARK', '<scenario>', <run_index>, '<warmup>';
  <benchmark query>;

After each MARK select, Beeline emits Time taken (ignored). The next Time taken
belongs to the benchmark query and is paired with the last MARK.

Usage:
  parse-beeline-timings.py <beeline.log> [--csv out.csv]
  parse-beeline-timings.py <beeline.log> --json

CSV columns (no header unless --header):
  scenario,run_index,warmup,time_taken_ms
"""
from __future__ import print_function

import argparse
import json
import re
import sys

TIME_RE = re.compile(
    r"Time taken:\s*([0-9]+(?:\.[0-9]+)?)\s*seconds",
    re.IGNORECASE,
)
# Result row or echoed forms
MARK_RE = re.compile(
    r"ORC_BENCH_MARK[^\w]*['\"]?([A-Za-z0-9_]+)['\"]?[^\d]*(\d+)[^\w]*(true|false)",
    re.IGNORECASE,
)
MARK_TSV_RE = re.compile(
    r"^ORC_BENCH_MARK\t([A-Za-z0-9_]+)\t(\d+)\t(true|false)\s*$",
    re.IGNORECASE,
)


def parse(text):
    rows = []
    pending = None  # dict scenario, run_index, warmup
    expect_query_time = False

    for line in text.splitlines():
        raw = line.strip()
        if not raw:
            continue

        m_tsv = MARK_TSV_RE.match(raw)
        m = m_tsv or MARK_RE.search(raw)
        if m:
            pending = {
                "scenario": m.group(1),
                "run_index": int(m.group(2)),
                "warmup": m.group(3).lower(),
            }
            expect_query_time = False
            continue

        tm = TIME_RE.search(raw)
        if not tm:
            continue
        ms = int(round(float(tm.group(1)) * 1000.0))
        if pending is None:
            continue
        if not expect_query_time:
            # Time taken for the MARK select itself
            expect_query_time = True
            continue
        rows.append(
            {
                "scenario": pending["scenario"],
                "run_index": pending["run_index"],
                "warmup": pending["warmup"],
                "time_taken_ms": ms,
            }
        )
        pending = None
        expect_query_time = False

    return rows


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("log_file")
    ap.add_argument("--csv", dest="csv_path", default="")
    ap.add_argument("--header", action="store_true")
    ap.add_argument("--json", action="store_true")
    args = ap.parse_args()

    with open(args.log_file, "r", encoding="utf-8", errors="replace") as f:
        text = f.read()
    rows = parse(text)

    if args.json:
        json.dump(rows, sys.stdout, indent=2)
        sys.stdout.write("\n")
        return 0

    lines = []
    if args.header:
        lines.append("scenario,run_index,warmup,time_taken_ms")
    for r in rows:
        lines.append(
            "{},{},{},{}".format(
                r["scenario"], r["run_index"], r["warmup"], r["time_taken_ms"]
            )
        )
    out = "\n".join(lines) + ("\n" if lines else "")
    if args.csv_path:
        with open(args.csv_path, "w", encoding="utf-8") as f:
            f.write(out)
    else:
        sys.stdout.write(out)

    if not rows:
        print(
            "WARN: parse-beeline-timings: no MARK+Time taken pairs found in {}".format(
                args.log_file
            ),
            file=sys.stderr,
        )
        return 2
    return 0


if __name__ == "__main__":
    sys.exit(main())
