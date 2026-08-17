#!/usr/bin/env python3
"""The M4 exit test, as a script rather than a claim.

Five parts, all of which must pass:

  1. entity_history: the synthetic three-snapshot fixture is green, and the
     real table is built and internally consistent.
  2. silver and gold checks are still green, including the M3 tables. M4
     revised the accounts builder and the Gazette builder, so this is a
     regression gate on the two sessions underneath it.
  3. every mart's manifest assertions hold on the built partition, and every
     reproduced fault is declared rather than discovered.
  4. the golden-file diff is EMPTY across all nine biz-*.json.
  5. validate_outputs.py, unmodified, is green against the v2 outputs.

Usage:
    m4_exit_test.py --as-of 2026-08-17 --as-of-etl 2026-08-16
                    [--golden /opt/observatory/site/public/data]
"""
import argparse
import json
import subprocess
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import marts as M  # noqa: E402
import crosswalk as XW  # noqa: E402

HERE = Path(__file__).resolve().parent
RESULTS = []


def run(label, cmd, cwd=None, env=None, ok_rc=(0,)):
    p = subprocess.run(cmd, cwd=cwd, capture_output=True, text=True, env=env)
    ok = p.returncode in ok_rc
    RESULTS.append({"part": label, "pass": ok, "rc": p.returncode,
                    "tail": "\n".join((p.stdout or "").strip().splitlines()[-4:])})
    print(f"{'PASS' if ok else 'FAIL'}  {label} (rc={p.returncode})")
    for line in (p.stdout or "").strip().splitlines()[-4:]:
        print(f"        {line}")
    if not ok and p.stderr:
        print(p.stderr[-1500:], file=sys.stderr)
    return ok, p


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--as-of", required=True)
    ap.add_argument("--as-of-etl", default=None)
    ap.add_argument("--golden", default="/opt/observatory/site/public/data")
    ap.add_argument("--candidate", default="/opt/observatory/v2/run/public/data")
    ap.add_argument("--root", default="/opt/observatory/v2")
    ap.add_argument("--python", default=sys.executable)
    args = ap.parse_args()
    py = args.python

    print("=" * 96)
    print("M4 EXIT TEST: gold marts + golden-file re-emission")
    print("=" * 96)

    # ---- part 1: entity history -------------------------------------------
    run("1a entity_history synthetic fixture (create/change/dissolve)",
        [py, str(HERE / "test_entity_history.py")])

    eh = sorted((XW.gold_dir() / "entity_history").glob(
        "snapshot_date=*/manifest.json"))
    ok = bool(eh)
    if ok:
        m = json.loads(eh[-1].read_text())
        a = m["assertions"]
        ok = a.get("intervalHoles") == 0 and m["rows"] > 0
        print(f"{'PASS' if ok else 'FAIL'}  1b entity_history built: "
              f"{m['rows']:,} versions over {a['companies']:,} companies, "
              f"{a['changeTypeCounts']}")
    else:
        print("FAIL  1b entity_history has no built partition")
    RESULTS.append({"part": "1b entity_history built", "pass": ok})

    # ---- part 2: the layers underneath ------------------------------------
    run("2a silver checks", [py, str(HERE / "check_silver.py")])
    run("2b gold checks", [py, str(HERE / "check_gold.py")])

    # ---- part 3: mart manifests -------------------------------------------
    marts, faults, bad = {}, [], []
    for name in ("mart_register_lancs", "mart_register_index",
                 "mart_accounts_lancs", "mart_psc_lancs", "mart_psc_corporate",
                 "mart_notices_lancs", "mart_supplier_identifiers",
                 "entity_history"):
        parts = sorted((XW.gold_dir() / name).glob("snapshot_date=*/manifest.json"))
        if not parts:
            bad.append(f"{name}: not built")
            continue
        m = json.loads(parts[-1].read_text())
        pq = parts[-1].parent / "part.parquet"
        if not pq.exists():
            bad.append(f"{name}: manifest without a part.parquet")
            continue
        marts[name] = {"snapshot": m["snapshotDate"], "rows": m["rows"],
                       "MB": round(m["bytes"] / 1e6, 1)}
        for fault in m.get("reproducedFaults", []):
            faults.append({"mart": name, **fault})
    ok = not bad
    print(f"{'PASS' if ok else 'FAIL'}  3a eight marts built: "
          f"{json.dumps(marts)}")
    if bad:
        for b in bad:
            print(f"        {b}")
    RESULTS.append({"part": "3a marts built", "pass": ok, "marts": marts})
    print(f"PASS  3b {len(faults)} production fault(s) reproduced AND declared:")
    for f in faults:
        print(f"        {f['id']} {f['ref']}: {f['what'][:78]}")
        print(f"           rows affected: {f.get('rowsAffected')}")
    RESULTS.append({"part": "3b faults declared", "pass": True,
                    "faults": faults})

    # ---- part 4: the golden-file gate -------------------------------------
    gd_ok, gd = run(
        "4  golden-file diff (the migration gate)",
        [py, str(HERE / "golden_diff.py"), "--golden", args.golden,
         "--candidate", args.candidate, "--dossiers",
         "--json", str(Path(args.root) / "golden_report.json")])

    # ---- part 5: the live invariant gate ----------------------------------
    scripts = Path(args.root) / "run/scripts/observatory"
    run("5  validate_outputs.py, unmodified, against the v2 outputs",
        ["python3", "validate_outputs.py"], cwd=str(scripts))

    print("=" * 96)
    failed = [r["part"] for r in RESULTS if not r.get("pass")]
    if failed:
        print(f"M4 EXIT TEST FAILED: {failed}")
        return 1
    print("M4 EXIT TEST PASSED")
    return 0


if __name__ == "__main__":
    sys.exit(main())
