#!/usr/bin/env python3
"""V-R1 on a synthetic bronze tree: the one-off accounts backfill passes.

`ch_accounts_api_backfill` is a single capture from 26 July 2026 that nothing
reruns (registry notes in sources.py). Under its old 45-day fail budget it
would stop every monthly run from October 2026. This test builds a throwaway
bronze tree on the vps host axis, with every consumed source landed and fresh
except the backfill, which keeps its real snapshot date, and pins the clock to
8 November 2026, the next monthly run the cron would make.

It asserts, one behaviour per check:

  1. every registry source has a budget row (no unbudgeted exit);
  2. the backfill is in `oneoff` mode, 105 days old, status ok;
  3. no consumed source is FAIL, MISSING or UNMEASURABLE, so staleness.py's
     own gate would pass;
  4. under the OLD row, the same tree FAILS on the backfill (so the change is
     what makes 3 pass, not the fixture);
  5. if pointblank is importable, the V-R1 suite itself reaches no level above
     ok on the same tree, and
  6. under the OLD row it reaches error, which fails the Snakemake pointblank
     rule (no --warn-only there). 5 and 6 are skipped, and say so, where
     pointblank is not installed;
  7. with the backfill's partition removed it is MISSING: a oneoff source that
     a builder reads still fails when absent (DATA-INTEGRITY s11.2).

Run: python3 test_staleness.py   (exit 0 = green, prints each assertion)
     uv run --no-project --with pointblank --with pandas --with duckdb \\
         python test_staleness.py    (includes check 6)
"""
import datetime as _dt
import json
import shutil
import sys
import tempfile
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import sources as S  # noqa: E402
import staleness as ST  # noqa: E402

AS_OF = _dt.date(2026, 11, 8)
FRESH = "2026-11-01"
BACKFILL = "ch_accounts_api_backfill"
BACKFILL_SNAP = "2026-07-26"
HOST = "vps"

failures = []


def check(label, ok, detail=""):
    print(f"  {'ok  ' if ok else 'FAIL'} {label}" + (f": {detail}" if detail else ""))
    if not ok:
        failures.append(label)


def land(bronze, sid, snap):
    part = bronze / f"source={sid}" / f"snapshot_date={snap}"
    part.mkdir(parents=True, exist_ok=True)
    (part / "manifest.json").write_text(json.dumps({
        "source": sid, "snapshotDate": snap, "asAt": None,
        "retrievedAt": snap, "fileCount": 1, "totalBytes": 1,
    }))


def consumed_bad(rows):
    return [r["source"] for r in rows
            if r["consumed"] and r["status"] in ("FAIL", "MISSING",
                                                 "UNMEASURABLE")]


def main():
    tmp = Path(tempfile.mkdtemp(prefix="test_staleness_"))
    bronze = tmp / "bronze"
    real_host, real_bronze = S.host, S.bronze_dir
    S.host = lambda: HOST
    S.bronze_dir = lambda h=None: bronze
    try:
        on_host = {s["id"] for s in S.SOURCES if HOST in s.get("hosts", [])}
        for sid in sorted(ST.CONSUMED & on_host):
            land(bronze, sid, BACKFILL_SNAP if sid == BACKFILL else FRESH)

        rows, unbudgeted, _, _ = ST.collect(host=HOST, as_of=AS_OF)
        check("1 every registry source has a budget row", not unbudgeted,
              str(unbudgeted))
        bf = next(r for r in rows if r["source"] == BACKFILL)
        check("2 backfill is oneoff, 105 days, ok",
              bf["mode"] == "oneoff" and bf["ageDays"] == 105
              and bf["status"] == "ok",
              f"mode {bf['mode']}, age {bf['ageDays']}, status {bf['status']}")
        bad = consumed_bad(rows)
        check("3 no consumed source FAIL, MISSING or UNMEASURABLE", not bad,
              str(bad))

        new_row = ST.BUDGETS[BACKFILL]
        ST.BUDGETS[BACKFILL] = ("fail", 45, "live API",
                                "CH API (dossier status checks)")
        try:
            old_rows, _, _, _ = ST.collect(host=HOST, as_of=AS_OF)
        finally:
            ST.BUDGETS[BACKFILL] = new_row
        old_bf = next(r for r in old_rows if r["source"] == BACKFILL)
        check("4 under the old 45-day fail row the same tree FAILS",
              old_bf["status"] == "FAIL", f"status {old_bf['status']}")

        try:
            import pointblank as pb
            import pointblank_suite as PS
        except ImportError as e:
            print(f"  skip 5 and 6, pointblank V-R1 suite: {e}")
        else:
            v, info = PS.suite_staleness(pb, AS_OF)
            levels = [s["level"] for s in PS.summarise("V-R1", v)]
            check("5 pointblank V-R1 suite all ok",
                  bool(levels) and all(lv == "ok" for lv in levels),
                  f"{len(levels)} steps over {info['sources']} fail-mode "
                  f"consumed sources: {levels}")
            ST.BUDGETS[BACKFILL] = ("fail", 45, "live API",
                                    "CH API (dossier status checks)")
            try:
                v, info = PS.suite_staleness(pb, AS_OF)
            finally:
                ST.BUDGETS[BACKFILL] = new_row
            levels = [s["level"] for s in PS.summarise("V-R1", v)]
            check("6 under the old row pointblank V-R1 reaches error",
                  "error" in levels or "critical" in levels,
                  f"{info['sources']} fail-mode consumed sources: {levels}")

        shutil.rmtree(bronze / f"source={BACKFILL}")
        rows, _, _, _ = ST.collect(host=HOST, as_of=AS_OF)
        bf = next(r for r in rows if r["source"] == BACKFILL)
        check("7 backfill absent is MISSING", bf["status"] == "MISSING",
              f"status {bf['status']}")
    finally:
        S.host, S.bronze_dir = real_host, real_bronze
        shutil.rmtree(tmp, ignore_errors=True)

    print()
    if failures:
        print(f"test_staleness FAILED: {failures}")
        return 1
    print("test_staleness GREEN")
    return 0


if __name__ == "__main__":
    sys.exit(main())
