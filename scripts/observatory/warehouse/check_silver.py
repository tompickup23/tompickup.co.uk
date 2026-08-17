#!/usr/bin/env python3
"""Run every checks/<table>.sql against the built silver partitions.

The contract is deliberately blunt: **any row returned by any check is a
failure**. A check is written so that a clean table returns nothing at all, so
there is no threshold to argue about and no warning tier to ignore.

Placeholders available in a checks file, substituted literally and NOT through
str.format, because the checks are full of regex quantifiers like {8} and {2}
that str.format would try to read as replacement fields:
    {pq}        absolute path to the partition's part.parquet
    {snapshot}  the partition date, as a bare YYYY-MM-DD for DATE literals
    {rows}      the row count the partition manifest claims

Usage:
    check_silver.py                     # every table, latest partition each
    check_silver.py --table ch_register
    check_silver.py --all-snapshots     # every partition of every table
"""
import argparse
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import silver as SV  # noqa: E402

CHECKS = Path(__file__).resolve().parent / "checks"


def split_checks(text):
    """Split a checks file into (name, sql) on the '-- check:' markers."""
    out, name, buf = [], None, []
    for line in text.splitlines():
        if line.strip().lower().startswith("-- check:"):
            if name:
                out.append((name, "\n".join(buf)))
            name = line.split(":", 1)[1].strip()
            buf = []
        elif name is not None:
            buf.append(line)
    if name:
        out.append((name, "\n".join(buf)))
    return [(n, s.strip()) for n, s in out if s.strip()]


def partitions(table, all_snapshots):
    base = SV.silver_dir() / table
    parts = sorted(base.glob("snapshot_date=*")) if base.exists() else []
    parts = [p for p in parts if (p / "part.parquet").exists()]
    if not parts:
        return []
    return parts if all_snapshots else parts[-1:]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--table")
    ap.add_argument("--all-snapshots", action="store_true")
    args = ap.parse_args()

    files = sorted(CHECKS.glob("*.sql"))
    if args.table:
        files = [f for f in files if f.stem == args.table]
        if not files:
            raise SystemExit(f"no checks file for table {args.table}")

    con, dv = SV.connect()
    SV.log(f"duckdb {dv}, silver root {SV.silver_dir()}")

    total = failures = skipped = 0
    for f in files:
        table = f.stem
        parts = partitions(table, args.all_snapshots)
        if not parts:
            SV.log(f"SKIP {table}: no built partition")
            skipped += 1
            continue
        checks = split_checks(f.read_text())
        for part in parts:
            snap = part.name.split("=", 1)[1]
            pq = part / "part.parquet"
            manifest = json.loads((part / "manifest.json").read_text())
            SV.log(f"{table} snapshot_date={snap} "
                   f"({manifest['rows']:,} rows, {len(checks)} checks)")
            for name, sql in checks:
                body = (sql.replace("{pq}", str(pq))
                           .replace("{snapshot}", snap)
                           .replace("{rows}", str(manifest["rows"])))
                total += 1
                try:
                    rows = con.execute(body).fetchall()
                except Exception as e:
                    failures += 1
                    print(f"  FAIL [{name}] check did not run: {e}")
                    continue
                if rows:
                    failures += 1
                    print(f"  FAIL [{name}] {len(rows)} row(s) returned:")
                    for r in rows[:5]:
                        print(f"        {r}")
                else:
                    print(f"  ok   [{name}]")

    con.close()
    print()
    SV.log(f"{total} checks run, {failures} failed, {skipped} tables skipped")
    if failures:
        print("SILVER CHECKS FAILED")
        return 1
    if skipped and not args.table:
        print("SILVER CHECKS GREEN, but some tables were not built")
        return 0
    print("SILVER CHECKS GREEN")
    return 0


if __name__ == "__main__":
    sys.exit(main())
