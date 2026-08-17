#!/usr/bin/env python3
"""The M3 exit test, as a script rather than a claim in a document.

Three things have to be true before entity resolution counts as migrated:

1. **Coverage.** The crosswalk must hold at least as many identifications as
   the production matchers it replaces. Counted per matcher, from the same
   bronze bytes the matchers themselves wrote, so the comparison cannot drift.
2. **The re-run invariant.** Running resolution twice on unchanged input must
   not move a single entity id. This is the mint-once guarantee and it is
   worthless unless it is tested rather than asserted.
3. **Gates.** check_gold.py green.

Run:  m3_exit_test.py            # coverage + invariant
      m3_exit_test.py --quick    # coverage only, skips the second resolution
"""
import argparse
import json
import subprocess
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import crosswalk as X  # noqa: E402
import silver as SV  # noqa: E402

HERE = Path(__file__).resolve().parent


def matcher_baselines():
    """What the production matchers identified, counted from bronze.

    Every snapshot of every matcher is counted, not just the latest, because
    the crosswalk deliberately carries every edition. Counting the latest only
    would set the bar below what the crosswalk actually has to clear and would
    hide the fact that the vps edition of the OCDS map is empty.
    """
    out = {}

    pound_crns = set()
    for snap, path, manifest in SV.bronze_partitions("matcher_pound"):
        f = path / "pound.json"
        if not f.exists():
            continue
        d = json.loads(f.read_text())
        for name, rec in d.get("resolved", {}).items():
            if rec.get("crn"):
                pound_crns.add((X.normalise(name), rec["crn"]))
    out["pound_set"] = pound_crns

    ocds = set()
    for snap, path, manifest in SV.bronze_partitions("matcher_ocds"):
        f = path / "ocds_supplier_ids.json"
        if not f.exists():
            continue
        d = json.loads(f.read_text())
        for name, rec in d.get("byName", {}).items():
            if rec.get("crn"):
                ocds.add((X.normalise(name), rec["crn"]))
    out["ocds_set"] = ocds

    nndr = set()
    nndr_names = set()
    for snap, path, manifest in SV.bronze_partitions("matcher_nndr"):
        f = path / "nndr_presence.json"
        if not f.exists():
            continue
        d = json.loads(f.read_text())
        for name, rec in d.get("byName", {}).items():
            nndr_names.add(X.normalise(name))
            for crn in rec.get("crns") or []:
                nndr.add((X.normalise(name), crn))
    out["nndr_set"] = nndr
    out["nndr_names_set"] = {(n,) for n in nndr_names}

    web = set()
    for snap, path, manifest in SV.bronze_partitions("matcher_websites"):
        f = path / "websites.jsonl"
        if not f.exists():
            continue
        for line in f.read_text().splitlines():
            if line.strip():
                r = json.loads(line)
                web.add((r["crn"], r["url"]))
    out["websites_set"] = web
    return out


def crosswalk_sets(con, xw_pq, rej_pq):
    """Every (normalised name, company number) the crosswalk holds or refused.

    Coverage is a SET question, not a per-matcher tally. A supplier ledger
    name that build_pound.py resolved through an award-notice identifier is
    attributed in the crosswalk to the evidence that settled it, `ocds`, not
    to the script that happened to run it. Bucketing by matcher therefore
    reports a shortfall in one bucket and a surplus in another for the same
    identification. What actually has to be true is that no identification the
    production matchers made has been lost.
    """
    landed = set(con.execute(
        f"SELECT DISTINCT source_name_norm, source_id "
        f"FROM read_parquet('{xw_pq}') WHERE scheme = 'GB-COH' "
        f"AND source_name_norm IS NOT NULL").fetchall())
    rejected = set()
    if Path(rej_pq).exists():
        rejected = set(con.execute(
            f"SELECT DISTINCT source_name_norm, source_id "
            f"FROM read_parquet('{rej_pq}') WHERE scheme = 'GB-COH' "
            f"AND reject_reason IS NOT NULL "
            f"AND source_name_norm IS NOT NULL").fetchall())
    web = set(con.execute(f"""
        SELECT DISTINCT a.source_id, b.source_id
        FROM read_parquet('{xw_pq}') a JOIN read_parquet('{xw_pq}') b
          ON a.decision_id = b.decision_id
        WHERE a.scheme = 'GB-COH' AND b.scheme = 'LBO-WEB'
    """).fetchall())
    nndr_names = set(con.execute(
        f"SELECT DISTINCT source_id FROM read_parquet('{xw_pq}') "
        f"WHERE scheme = 'LBO-NNDR'").fetchall())
    by_matcher = dict(con.execute(
        f"SELECT matcher, count(*) FROM read_parquet('{xw_pq}') "
        f"WHERE scheme = 'GB-COH' GROUP BY 1").fetchall())
    return landed, rejected, web, nndr_names, by_matcher


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--quick", action="store_true")
    args = ap.parse_args()

    con, dv = SV.connect()
    gold = X.gold_dir()
    xw_parts = sorted((gold / "crosswalk").glob("snapshot_date=*"))
    ent_parts = sorted((gold / "entity").glob("snapshot_date=*"))
    if not xw_parts or not ent_parts:
        raise SystemExit("FATAL: gold crosswalk or entity partition missing.")
    xw_pq = str(xw_parts[-1] / "part.parquet")
    ent_pq = str(ent_parts[-1] / "part.parquet")

    print("=" * 72)
    print("M3 EXIT TEST")
    print("=" * 72)

    failures = 0

    # --- 1. coverage --------------------------------------------------------
    base = matcher_baselines()
    edge_parts = sorted((gold / "crosswalk_edges").glob("snapshot_date=*"))
    rej_pq = str(edge_parts[-1] / "_rejected.parquet") if edge_parts else ""
    landed, rejected, web, nndr_names, by_matcher = crosswalk_sets(
        con, xw_pq, rej_pq)
    print("\n1. COVERAGE: no identification the production matchers made may")
    print("   be missing. Landed, or refused by the register with a reason.")
    print(f"   {'source':<14} {'production':>11} {'landed':>9} "
          f"{'refused':>8} {'lost':>6}   verdict")
    checks = [
        ("pound", base["pound_set"], landed, rejected),
        ("ocds", base["ocds_set"], landed, rejected),
        ("nndr", base["nndr_set"], landed, rejected),
        ("nndr_names", base["nndr_names_set"], nndr_names, set()),
        ("websites", base["websites_set"], web, set()),
    ]
    for label, prod, have, refused in checks:
        n_land = len(prod & have)
        n_ref = len(prod & refused)
        lost = prod - have - refused
        ok = not lost
        if not ok:
            failures += 1
        print(f"   {label:<14} {len(prod):>11,} {n_land:>9,} {n_ref:>8,} "
              f"{len(lost):>6,}   {'ok' if ok else 'LOST'}")
        for x in sorted(lost)[:3]:
            print(f"        lost: {x}")
    print("   refused = a company number the register does not hold, or holds")
    print("   for a society, a CIO or an overseas entity rather than a company")
    print(f"\n   crosswalk GB-COH edges by matcher: "
          f"{json.dumps(by_matcher, sort_keys=True)}")

    n_ent = con.execute(f"SELECT count(*) FROM read_parquet('{ent_pq}')").fetchone()[0]
    n_xw = con.execute(f"SELECT count(*) FROM read_parquet('{xw_pq}')").fetchone()[0]
    n_multi = con.execute(
        f"SELECT count(*) FROM read_parquet('{ent_pq}') WHERE scheme_count > 1"
    ).fetchone()[0]
    print(f"\n   entities {n_ent:,}, crosswalk edges {n_xw:,}, "
          f"{n_multi:,} entities carry more than one scheme")

    # --- 2. re-run invariant ------------------------------------------------
    print("\n2. RE-RUN INVARIANT: zero entity id churn on unchanged input")
    if args.quick:
        print("   skipped (--quick)")
    else:
        before = "/tmp/m3_entity_before.parquet"
        subprocess.run(["cp", ent_pq, before], check=True)
        r = subprocess.run(
            [sys.executable, str(HERE / "build_entities.py")],
            capture_output=True, text=True)
        if r.returncode != 0:
            print(f"   FAIL: second resolution errored\n{r.stderr[-800:]}")
            failures += 1
        else:
            minted = [l for l in r.stdout.splitlines() if "ids minted" in l]
            row = con.execute(f"""
                WITH a AS (SELECT entity_id, anchor_scheme, anchor_source_id
                           FROM read_parquet('{before}')),
                     b AS (SELECT entity_id, anchor_scheme, anchor_source_id
                           FROM read_parquet('{ent_pq}'))
                SELECT (SELECT count(*) FROM a),
                       (SELECT count(*) FROM b),
                       (SELECT count(*) FROM a JOIN b
                          USING (anchor_scheme, anchor_source_id)
                        WHERE a.entity_id <> b.entity_id),
                       (SELECT count(*) FROM a ANTI JOIN b
                          USING (anchor_scheme, anchor_source_id)),
                       (SELECT count(*) FROM b ANTI JOIN a
                          USING (anchor_scheme, anchor_source_id))
            """).fetchone()
            r1, r2, changed, only1, only2 = row
            ok = (r1 == r2 and changed == 0 and only1 == 0 and only2 == 0)
            if not ok:
                failures += 1
            print(f"   run 1 {r1:,} entities, run 2 {r2:,} entities")
            print(f"   changed ids {changed}, dropped {only1}, added {only2}"
                  f"   {'ok' if ok else 'CHURN DETECTED'}")
            for l in minted:
                print(f"   {l.split('] ', 1)[-1]}")

    # --- 3. gates -----------------------------------------------------------
    print("\n3. GATES: check_gold.py")
    r = subprocess.run([sys.executable, str(HERE / "check_gold.py")],
                       capture_output=True, text=True)
    tail = [l for l in r.stdout.splitlines() if "checks run" in l
            or l.startswith("GOLD CHECKS")]
    for l in tail:
        print(f"   {l.split('] ', 1)[-1]}")
    fails = [l for l in r.stdout.splitlines() if l.strip().startswith("FAIL")]
    for l in fails[:10]:
        print(f"   {l.strip()}")
    if r.returncode != 0:
        failures += 1

    # --- 4. linkage evaluation present --------------------------------------
    print("\n4. LINKAGE EVALUATION: precision and recall recorded")
    lk = sorted((gold / "linkage_pairs").glob("snapshot_date=*"))
    if not lk:
        print("   FAIL: build_linkage.py has not produced an evaluation")
        failures += 1
    else:
        ev = json.loads((lk[-1] / "linkage_evaluation.json").read_text())
        op = ev.get("operating") or {}
        print(f"   splink {ev['splinkVersion']}, threshold "
              f"{ev['operatingThreshold']}")
        print(f"   truth set {ev['truthSet']['size']:,} labelled names, "
              f"{ev['truthSet'].get('inScopeSize', 0):,} in scope")
        print(f"   in-scope precision {op.get('precisionInScope')} "
              f"recall {op.get('recallInScope')}")
        print(f"   overall precision {op.get('precision')} "
              f"recall {op.get('recall')}")
        cs = ev["clericalSample"]
        print(f"   clerical sample {cs['size']} pairs, "
              f"{cs['labelledFromHeldOutIdentifier']} labelled, "
              f"{cs['awaitingHumanReview']} awaiting human review")
        print(f"   publication gate cleared: "
              f"{ev['publicationGate']['cleared']}")

    print("\n" + "=" * 72)
    if failures:
        print(f"M3 EXIT TEST FAILED ({failures} failure(s))")
        return 1
    print("M3 EXIT TEST PASSED")
    return 0


if __name__ == "__main__":
    sys.exit(main())
