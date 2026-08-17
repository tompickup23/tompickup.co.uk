#!/usr/bin/env python3
"""The M5 exit test, as a script rather than a claim.

Seven parts. The plan's M5 gate is "one clean cron cycle on the new driver,
rollback flag proven, THEN delete the old path". This session runs the cycle in
SHADOW and does not cut the cron over, so parts 6 and 7 test the reversibility
property directly rather than by having flipped it: the old path is provably
untouched, and the new driver provably does nothing on a second run, which is
the property that makes a resumed or repeated cron run safe.

  1. the DAG resolves and the dry run names the rules it would execute
  2. every rule of the cycle completed, with a marker carrying its run id
  3. the layer gates are green (silver, gold, manifests)
  4. the report gates are green (staleness V-R1, pointblank V-R1/V-R2,
     schemas V-T3/V-R3/V-L1)
  5. the golden-file gate still holds: the site notices nothing
  6. pipelineGitSha is wired: no manifest built in this run carries a null
  7. idempotence and reversibility: a second `snakemake monthly` executes
     nothing, and monthly_refresh.sh plus the cron entry are byte-unchanged

Usage:
    m5_exit_test.py --shadow /opt/observatory/m5 \\
                    --golden /opt/observatory/site/public/data \\
                    --as-of 2026-08-17 --as-of-etl 2026-08-16
"""
import argparse
import datetime as _dt
import hashlib
import json
import subprocess
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import driver as D  # noqa: E402

HERE = Path(__file__).resolve().parent
RESULTS = []

# Every marker a full `monthly` cycle must leave behind, with the rule that
# owns it. `linkage_pairs` is absent on purpose: the Splink tier is off by
# default and its output never reaches the crosswalk.
EXPECTED_MARKERS = [
    ("bronze/bronze.done", "bronze"),
    ("silver/ch_register.done", "silver_ch_register"),
    ("silver/ch_psc.done", "silver_ch_psc"),
    ("silver/ch_accounts.done", "silver_ch_accounts"),
    ("silver/ch_strikeoff.done", "silver_ch_strikeoff"),
    ("silver/gazette_notices.done", "silver_gazette"),
    ("gate/silver_checks.done", "silver_checks"),
    ("gold/crosswalk_edges.done", "gold_crosswalk"),
    ("gold/entity.done", "gold_entities"),
    ("gold/entity_history.done", "gold_entity_history"),
    ("gate/gold_checks.done", "gold_checks"),
    ("gold/marts.done", "marts"),
    ("assemble/site_json.done", "assemble"),
    ("report/staleness.done", "staleness"),
    ("gate/pointblank.done", "pointblank"),
    ("gate/schemas.done", "schemas"),
    ("gate/manifests.done", "manifests"),
]


def record(part, ok, detail=""):
    RESULTS.append({"part": part, "pass": bool(ok), "detail": detail})
    print(f"{'PASS' if ok else 'FAIL'}  {part}")
    if detail:
        for line in str(detail).splitlines()[:6]:
            print(f"        {line}")
    return ok


def sha256(p):
    h = hashlib.sha256()
    with open(p, "rb") as f:
        for c in iter(lambda: f.read(1 << 20), b""):
            h.update(c)
    return h.hexdigest()


def snakemake_bin():
    """The snakemake beside the interpreter running this, not whatever is on
    PATH. A string replace on sys.executable turns `.../bin/python3` into
    `.../bin/snakemake3`, which does not exist."""
    import shutil
    cand = Path(sys.executable).parent / "snakemake"
    return str(cand) if cand.exists() else (shutil.which("snakemake") or "snakemake")


def snakemake(root, targets, snakefile, config, flags=(), cores=4):
    """Targets BEFORE --config. snakemake's --config consumes every following
    argument as a name=value pair, so a target after it is read as config and
    the run dies on `Unparsable value: 'monthly'`."""
    cmd = [snakemake_bin(), "-s", str(snakefile), "--cores", str(cores)]
    cmd += list(targets) + list(flags) + ["--config"] + list(config)
    return subprocess.run(cmd, cwd=str(root), capture_output=True, text=True)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--root", default=None)
    ap.add_argument("--shadow", default="/opt/observatory/m5")
    ap.add_argument("--site", default="/opt/observatory/site")
    ap.add_argument("--golden", default="/opt/observatory/site/public/data")
    ap.add_argument("--as-of", required=True)
    ap.add_argument("--as-of-etl", default=None)
    ap.add_argument("--refresh-script", default="/opt/observatory/monthly_refresh.sh")
    ap.add_argument("--refresh-sha", default=None,
                    help="the sha256 monthly_refresh.sh had BEFORE this "
                         "session. Part 7 is only meaningful against a value "
                         "recorded before any work started.")
    ap.add_argument("--out", default=None)
    args = ap.parse_args()

    root = Path(args.root) if args.root else D.warehouse_root()
    state = root / "state"
    reports = root / "reports"
    shadow = Path(args.shadow)
    cfg = [f"as_of={args.as_of}",
           f"as_of_etl={args.as_of_etl or args.as_of}",
           f"shadow={shadow}", f"site={args.site}", f"golden={args.golden}"]

    print("=" * 96)
    print("M5 EXIT TEST: Snakemake driver, gates, schemas, provenance")
    print("=" * 96)

    # ---- part 1: the DAG resolves ----------------------------------------
    p = snakemake(root, ["monthly"], HERE / "Snakefile", cfg,
                  flags=["-n", "-q"])
    ok = p.returncode == 0
    record("1  the DAG resolves and a dry run plans cleanly",
           ok, (p.stdout or p.stderr)[-600:])

    # ---- part 2: every rule of the cycle completed ------------------------
    missing, runs = [], {}
    for rel, rule in EXPECTED_MARKERS:
        m = D.read_marker(state / rel)
        if not m:
            missing.append(rel)
            continue
        if m.get("rule") != rule:
            missing.append(f"{rel} (rule {m.get('rule')}, expected {rule})")
        runs.setdefault(m.get("runId"), []).append(rule)
    one_run = len(runs) == 1
    record(f"2  all {len(EXPECTED_MARKERS)} rules completed, one run id",
           not missing and one_run,
           f"missing: {missing}" if missing else
           f"run ids: { {k: len(v) for k, v in runs.items()} }")

    # ---- part 3: the layer gates -----------------------------------------
    sv = (reports / "check_silver.txt")
    gd = (reports / "check_gold.txt")
    sv_ok = sv.exists() and "SILVER CHECKS GREEN" in sv.read_text()
    gd_ok = gd.exists() and "GOLD CHECKS GREEN" in gd.read_text()

    def counts(path, needle):
        if not path.exists():
            return "no report"
        for line in path.read_text().splitlines():
            if needle in line:
                return line.strip()
        return "?"
    record("3a silver checks green", sv_ok, counts(sv, "checks run"))
    record("3b gold checks green", gd_ok, counts(gd, "checks run"))

    mf = reports / "manifests.json"
    m = json.loads(mf.read_text()) if mf.exists() else {}
    record("3c manifest gate green", bool(m) and not m.get("failures"),
           json.dumps(m.get("counts", {})))

    # ---- part 4: the report gates ----------------------------------------
    st = reports / "staleness.json"
    s = json.loads(st.read_text()) if st.exists() else {}
    record("4a V-R1 staleness: no source over 2x budget, none missing",
           bool(s) and s.get("counts", {}).get("fail") == 0,
           json.dumps(s.get("counts", {})))

    pbf = reports / "pointblank.json"
    pbj = json.loads(pbf.read_text()) if pbf.exists() else {}
    lv = pbj.get("levels", {})
    record("4b pointblank: no critical, no error",
           bool(pbj) and not lv.get("critical") and not lv.get("error"),
           json.dumps(lv))

    sc = reports / "schemas.json"
    scj = json.loads(sc.read_text()) if sc.exists() else {}
    hard = (scj.get("schemaErrors") or []) + (scj.get("houseStyleFindings") or []) \
        + (scj.get("vl1Findings") or [])
    record("4c schemas V-T3 and V-L1 green, V-R3 counted",
           bool(scj) and not hard and not scj.get("dossierErrorCount"),
           f"{len(scj.get('filesChecked', []))} files, "
           f"{scj.get('dossiersChecked')} dossiers, "
           f"{len(scj.get('declaredServeFaults') or [])} declared fault(s), "
           f"V-R3 pending: "
           f"{(scj.get('pendingGates') or [{}])[0].get('missingAsAt')} of "
           f"{(scj.get('pendingGates') or [{}])[0].get('sourceEntries')} "
           f"source entries carry no asAt")

    # ---- part 5: the golden-file gate ------------------------------------
    # golden_diff.py keys its report by filename, with a `_dossiers` block.
    gr = shadow / "golden_report.json"
    g = json.loads(gr.read_text()) if gr.exists() else {}
    per_file = {k: v for k, v in g.items() if not k.startswith("_")}
    total_diffs = sum(v.get("semanticDiffs", 0) for v in per_file.values())
    errored = [k for k, v in per_file.items() if v.get("error")]
    dos = g.get("_dossiers") or {}
    record("5  golden-file diff still zero: the site notices nothing",
           bool(per_file) and total_diffs == 0 and not errored
           and dos.get("differing") == 0,
           f"{len(per_file)} files, {total_diffs} semantic diff(s), "
           f"errors {errored}; dossiers {dos.get('compared')} compared, "
           f"{dos.get('differing')} differing")

    # ---- part 6: pipelineGitSha ------------------------------------------
    run_ids = set(runs)
    nulls, stamped = [], 0
    for rel, _ in EXPECTED_MARKERS:
        mk = D.read_marker(state / rel)
        if not mk:
            continue
        for name, art in (mk.get("artefacts") or {}).items():
            if not art:
                continue
            if art.get("pipelineGitSha"):
                stamped += 1
            else:
                nulls.append(f"{rel}:{name}")
    record("6  pipelineGitSha wired: every partition built this run names "
           "its own commit",
           not nulls and stamped > 0,
           f"{stamped} partition(s) stamped with "
           f"{D.pipeline_git_sha()}; nulls: {nulls}")

    # ---- part 7: idempotence and reversibility ---------------------------
    p2 = snakemake(root, ["monthly"], HERE / "Snakefile", cfg,
                   flags=["-n"])
    txt = (p2.stdout or "") + (p2.stderr or "")
    nothing = p2.returncode == 0 and ("Nothing to be done" in txt
                                      or "nothing to be done" in txt.lower())
    record("7a idempotent: a second run has nothing to do", nothing,
           txt.strip()[-300:])

    rs = Path(args.refresh_script)
    if args.refresh_sha:
        same = rs.exists() and sha256(rs) == args.refresh_sha
        record("7b reversible: monthly_refresh.sh is byte-unchanged", same,
               f"{sha256(rs) if rs.exists() else 'missing'} vs recorded "
               f"{args.refresh_sha}")
    else:
        record("7b reversible: monthly_refresh.sh sha recorded", rs.exists(),
               f"{sha256(rs) if rs.exists() else 'missing'} (no --refresh-sha "
               "given, so this records rather than asserts)")

    cron = subprocess.run(["bash", "-lc",
                           "cat /etc/cron.d/observatory-refresh 2>/dev/null "
                           "|| crontab -l 2>/dev/null | grep -i observatory "
                           "|| true"], capture_output=True, text=True)
    still_old = "monthly_refresh.sh" in cron.stdout
    record("7c reversible: the cron still calls the OLD path, so the new "
           "driver is not primary", still_old, cron.stdout.strip()[:300])

    print("=" * 96)
    failed = [r["part"] for r in RESULTS if not r["pass"]]
    report = {
        "test": "M5 exit test",
        "generated": _dt.datetime.now(_dt.timezone.utc)
                        .strftime("%Y-%m-%dT%H:%M:%SZ"),
        "runIds": sorted(x for x in run_ids if x),
        "pipelineGitSha": D.pipeline_git_sha(),
        "asOf": args.as_of, "asOfEtl": args.as_of_etl or args.as_of,
        "shadow": str(shadow), "golden": args.golden,
        "results": RESULTS,
        "passed": not failed,
    }
    out = Path(args.out) if args.out else reports / "m5_exit_test.json"
    out.write_text(json.dumps(report, indent=2) + "\n")
    print(f"written to {out}")
    if failed:
        print(f"M5 EXIT TEST FAILED: {failed}")
        return 1
    print("M5 EXIT TEST PASSED")
    return 0


if __name__ == "__main__":
    sys.exit(main())
