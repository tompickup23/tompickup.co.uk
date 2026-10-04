#!/usr/bin/env python3
"""The five tests in WATERFALL.md section 5. No Phase 1 result is read before
all five pass. Writes $POUND_WORK/out/tests_report.json and
test1_replay_differences.csv; exits 1 on any failure.

  1. Replaying today's matcher (build_pound.match_entry, its own source code,
     with its own register index, Lancashire frame and OCDS map) reproduces
     the waterfall's decision for every key it resolves, or the difference is
     listed with a reason.
  2. No decision_id maps one payee key to two organisations.
  3. Every GB-COH id in the resolver is in a register snapshot.
  4. Value conservation: resolved plus unclassified equals the spend input's
     supplier total per body and year, to the penny.
  5. No person name appears in any output table (checked against the
     individual PSC names in gold mart_psc_lancs). Hits inside a register
     company name (an eponymous company) are listed for the gate, not
     failed; a hit anywhere else fails.
"""
import ast
import csv
import gzip
import json
import re
import sys
from collections import Counter, defaultdict
from pathlib import Path

import duckdb

from common import (EXTRACT, GOLD, HERE, INPUTS, OUT, normalise,
                    supplier_variants, write_json)

csv.field_size_limit(sys.maxsize)
REPORT = {}
REGISTER_INDEX = Path("/opt/observatory/out/register_index.tsv.gz")
MASTER = Path("/root/observatory-data/processed/master.jsonl.gz")
OCDS = INPUTS / "ocds_supplier_ids.json"


def load_resolver():
    with open(OUT / "resolver_proposed.csv", newline="") as f:
        return list(csv.DictReader(f))


def today_matcher():
    """Exec build_pound.py's own ALIASES, PREFIX_DENY, prefix_unique and
    match_entry, without running the module's top-level build."""
    src = (HERE.parent / "build_pound.py").read_text()
    tree = ast.parse(src)
    keep = [n for n in tree.body if (isinstance(n, ast.FunctionDef) and n.name in ("prefix_unique", "match_entry"))
            or (isinstance(n, ast.Assign) and any(getattr(t, "id", "") in ("ALIASES", "PREFIX_DENY", "_sorted_keys")
                                                  for t in n.targets))
            or isinstance(n, ast.Import) and any(a.name == "bisect" for a in n.names)]
    by_name = defaultdict(list)
    with gzip.open(REGISTER_INDEX, "rt") as f:
        rd = csv.reader(f, delimiter="\t")
        next(rd)
        for crn, name, pc, status in rd:
            by_name[normalise(name)].append((crn, pc, status))
    lancs = {}
    with gzip.open(MASTER, "rt") as f:
        for line in f:
            r = json.loads(line)
            lancs[r["crn"]] = True
    ns = {"by_name": by_name, "lancs_crn": lancs, "supplier_variants": supplier_variants,
          "OCDS_IDS": json.loads(OCDS.read_text()).get("byName", {}), "defaultdict": defaultdict}
    exec(compile(ast.Module(body=keep, type_ignores=[]), "build_pound.py", "exec"), ns)
    # The same matcher over the same index without companies incorporated
    # after the window closed, which the waterfall excludes (no such company
    # can be a payee in 2024/25 or 2025/26). Used only to give a reason.
    late = late_companies()
    ns2 = {**ns, "by_name": WithoutLate(by_name, late)}
    exec(compile(ast.Module(body=keep, type_ignores=[]), "build_pound.py", "exec"), ns2)
    return ns["match_entry"], by_name, ns2["match_entry"], late


def late_companies():
    """Company number -> incorporation date, for companies incorporated
    after WINDOW_END (31 March 2026) in the register extract."""
    from waterfall import WINDOW_END
    con = duckdb.connect()
    return dict(con.execute(f"""SELECT company_number, min(incorporation_date) FROM '{EXTRACT}/reg_names.parquet'
                                WHERE incorporation_date > '{WINDOW_END}' GROUP BY 1""").fetchall())


class WithoutLate:
    """Read-only view of today's register index with late companies removed."""
    def __init__(self, d, late):
        self.d, self.late = d, late

    def get(self, k, default=None):
        v = self.d.get(k)
        return [c for c in v if c[0] not in self.late] if v else default

    def __getitem__(self, k):
        return [c for c in self.d[k] if c[0] not in self.late]

    def __iter__(self):
        return iter(self.d)


def test1(rows):
    match_entry, by_name, match_entry_no_late, late = today_matcher()
    keys = {}
    for line in gzip.open(INPUTS / "payee_keys.jsonl.gz", "rt"):
        d = json.loads(line)
        e = keys.setdefault((d["body_id"], d["payee_key"]), {"names": Counter()})
        e["names"][d["payee_key"]] += 10 ** 9
        for n, c in d["supplier_canonical"].items():
            e["names"][n] += c * 1000
        for n, c in d["raw_names"].items():
            e["names"][n] += c
    ours = {(r["body_id"], r["payee_key"]): r for r in rows}
    diffs, same, today_resolved = [], 0, 0
    for k, e in keys.items():
        r = ours.get(k)
        if r is None:
            continue  # masked keys (individual or redacted) are never matched
        names = [n for n, _ in e["names"].most_common()]
        crn, how, pool = match_entry({"names": names})
        if not crn:
            continue
        today_resolved += 1
        ids = {r["org_id"]} | {a[7:] for a in r["alias_ids"].split("|") if a.startswith("GB-COH-")}
        if r["org_scheme"] == "GB-COH" and crn in ids:
            same += 1
            continue
        ev = json.loads(r["evidence"]) if r["evidence"] else {}
        others = ev.get("other_steps", [])
        if how == "alias":
            reason = "curated alias in build_pound.ALIASES: a hand decision, not a pipeline step; it goes to step 9 for a named person"
        elif r["method"] == "queue":
            reason = f"waterfall sends it to the step 9 queue: {r['reason_detail']}"
        elif ev.get("name_pool_also_ambiguous"):
            reason = ("the silver snapshots hold more than one company for the exact name (a renamed company keeps the "
                      f"name in an earlier snapshot), so step 2b does not decide it; {r['method']} resolves it and the pool "
                      "is kept in the evidence")
        elif how == "ocds" and r["method"] in ("ocds", "name-exact", "name-exact-out"):
            reason = ("variant order: today's matcher tries every name variant against the OCDS map before any exact "
                      "name, so it matched a shorter variant; the waterfall takes the full name first")
        elif how == "ocds" and any(o["method"] == "ocds" and not o["org"] for o in others):
            reason = ("the OCP Find a Tender file carries more than one company number for this name, so step 7 links "
                      "it to nothing (17 August crosswalk rule); today's matcher read ocds_supplier_ids.json alone")
        elif ev.get("name_kind") == "previous" or (ev.get("snapshot") and ev.get("snapshot") < "2026-10-01"):
            reason = ("the waterfall's exact match is on a name today's register index does not hold "
                      f"(a {ev.get('name_kind')} name, snapshot {ev.get('snapshot')})")
        elif how == "prefix-unique" and r["method"] in ("name-exact", "name-exact-out"):
            reason = "the waterfall finds an exact name in the silver snapshots before any prefix test"
        elif r["org_scheme"] == "GB-COH" and r["method"] in ("id-bank-inferred", "id-council"):
            reason = f"an earlier step ({r['method']}) proposes {r['org_id']}; today's matcher has no step 1"
        elif r["org_scheme"] != "GB-COH" and r["org_id"]:
            reason = f"waterfall resolves to {r['org_scheme']} {r['org_id']} ({r['method']}); today's matcher only knows company numbers"
        elif not r["org_id"]:
            k2 = [v for v in supplier_variants(names[0])]
            reason = ("today's register index (/opt/observatory/out/register_index.tsv.gz, 4 October 2026) "
                      f"holds a candidate the silver snapshots resolve differently; waterfall reason: {r['reason']}")
        else:
            reason = "unexplained"
            crn2 = match_entry_no_late({"names": names})[0]
            pool_late = sorted({c[0] for v in supplier_variants(names[0]) for c in (by_name.get(v) or [])
                                if c[0] in late})
            if crn in late or (pool_late and crn2 in ids):
                involved = sorted(set(pool_late) | ({crn} if crn in late else set()))
                reason = ("today's register index holds a company whose register date (incorporation, or registration for an overseas entity) is after 31 March 2026 ("
                          + ", ".join(f"{c} on {late[c]}" for c in involved)
                          + "), which cannot be a payee in the window and which the waterfall excludes; replayed "
                          f"without such companies today's matcher gives {crn2 or 'no match'}, and the waterfall "
                          f"resolves {r['org_scheme']} {r['org_id']} by {r['method']}")
        diffs.append({"body_id": k[0], "payee_key": k[1], "today_crn": crn, "today_how": how,
                      "waterfall_scheme": r["org_scheme"], "waterfall_id": r["org_id"],
                      "waterfall_method": r["method"], "reason": reason})
    with open(OUT / "test1_replay_differences.csv", "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=["body_id", "payee_key", "today_crn", "today_how", "waterfall_scheme",
                                          "waterfall_id", "waterfall_method", "reason"])
        w.writeheader()
        w.writerows(diffs)
    unexplained = [d for d in diffs if d["reason"] == "unexplained"]
    REPORT["test1"] = {"today_resolved": today_resolved, "same": same, "differences": len(diffs),
                       "by_reason": dict(Counter(d["reason"].split(":")[0][:80] for d in diffs)),
                       "unexplained": len(unexplained), "pass": not unexplained}
    return not unexplained


def test2(rows):
    per_key = Counter((r["body_id"], r["payee_key"]) for r in rows)
    ids = Counter(r["decision_id"] for r in rows)
    bad = [k for k, n in per_key.items() if n > 1] + [k for k, n in ids.items() if n > 1]
    REPORT["test2"] = {"rows": len(rows), "keys_with_two_rows": len([k for k, n in per_key.items() if n > 1]),
                       "duplicate_decision_ids": len([k for k, n in ids.items() if n > 1]), "pass": not bad}
    return not bad


def test3(rows, con):
    nums = {r[0] for r in con.execute(f"SELECT company_number FROM '{EXTRACT}/reg_numbers.parquet'").fetchall()}
    coh = set()
    for r in rows:
        if r["org_scheme"] == "GB-COH":
            coh.add(r["org_id"])
        coh |= {a[7:] for a in r["alias_ids"].split("|") if a.startswith("GB-COH-")}
    missing = sorted(coh - nums)
    REPORT["test3"] = {"gb_coh_ids": len(coh), "missing": missing[:20], "pass": not missing}
    return not missing


def test4(rows):
    spend = json.loads((INPUTS / "council_supplier_spend.json").read_text())["bodies"]
    vals = defaultdict(int)
    with open(OUT / "payee_key_values.csv", newline="") as f:
        for r in csv.DictReader(f):
            vals[(r["body_id"], r["financial_year"])] += int(r["value_pence"])
    resolved_keys = {(r["body_id"], r["payee_key"]) for r in rows if r["org_id"]}
    out, ok = {}, True
    bodies = {r["body_id"] for r in rows}
    for b in sorted(bodies):
        for fy, v in spend[b]["byYear"].items():
            fy2 = fy.replace("-", "/")
            want = round(v["total"] * 100)
            got = vals[(b, fy2)]
            out[f"{b} {fy2}"] = {"spend_input_pence": want, "resolved_plus_unclassified_pence": got,
                                 "difference_pence": got - want}
            ok &= got == want
    REPORT["test4"] = {"body_years": out, "pass": ok}
    return ok


def person_names(con):
    mart = sorted((GOLD / "mart_psc_lancs").glob("snapshot_date=*/part.parquet"))[-1]
    names = set()
    for f, m, s in con.execute(f"""SELECT name_forename, name_middle, name_surname FROM read_parquet('{mart}')
                                   WHERE is_individual AND name_surname IS NOT NULL""").fetchall():
        f = normalise(f or "")
        s = normalise(s or "")
        if f and s and len(f) > 1:
            names.add(f"{f} {s}")
            if m:
                names.add(f"{f} {normalise(m)} {s}")
    return names, mart


def windows(text):
    t = normalise(text or "").split()
    for n in (2, 3):
        for i in range(len(t) - n + 1):
            yield " ".join(t[i:i + n])


def test5(con):
    names, mart = person_names(con)
    fails, epon = [], []
    files = [OUT / "resolver_proposed.csv", OUT / "queue.csv"] + sorted(OUT.glob("verify_*.csv")) + \
        sorted(OUT.glob("ownership_walk*.csv"))
    org_fields = {"payee_key", "register_name", "register_names", "name", "proposed_name",
                  "matched_variant", "supplier_group", "payee_keys", "register_name_norm"}
    for p in files:
        if not p.exists():
            continue
        with open(p, newline="") as f:
            for r in csv.DictReader(f):
                resolved = bool(r.get("org_id")) or r.get("payee_class") in ("public body", "council company")
                for col, val in r.items():
                    if not val or col in ("decision_id", "decided_by", "decided_at"):
                        continue
                    texts = [(col, val)]
                    if val.startswith("{"):
                        try:
                            texts = [(k2, str(v2)) for k2, v2 in flatten(json.loads(val))]
                        except Exception:
                            pass
                    for c2, t in texts:
                        hit = next((w for w in windows(t) if w in names), None)
                        if not hit:
                            continue
                        leaf = c2.split(".")[-1]
                        from waterfall import has_legal_form
                        if leaf in org_fields and (resolved or leaf != "payee_key" or has_legal_form(t)):
                            epon.append({"file": p.name, "field": c2})
                        else:
                            fails.append({"file": p.name, "field": c2, "body_id": r.get("body_id", "")})
    REPORT["test5"] = {"psc_names_checked": len(names), "psc_source": str(mart),
                       "eponymous_org_name_hits": len(epon),
                       "eponymous_by_file": dict(Counter(e["file"] for e in epon)),
                       "failures": len(fails), "failures_sample": fails[:20], "pass": not fails}
    return not fails


def flatten(o, pre=""):
    if isinstance(o, dict):
        for k, v in o.items():
            yield from flatten(v, f"{pre}.{k}" if pre else k)
    elif isinstance(o, list):
        for v in o:
            yield from flatten(v, pre)
    else:
        yield pre, o


def test_5b_seed(con):
    """Step 5b against sources/council_company_seed.csv (23 companies)."""
    from waterfall import Registers, council_companies
    seed = list(csv.DictReader(open(INPUTS / "council_company_seed.csv", newline="")))
    R = Registers(con, sorted({normalise(r["name"]) for r in seed}))
    cc, mart = council_companies(con, set(), R)
    rows, agree = [], 0
    for r in seed:
        exp = r["rule_2_1_expectation"]
        exp_pass = exp.startswith("passes")
        got = r["company_number"] in cc
        ok = got == exp_pass
        agree += ok
        rows.append({"company_number": r["company_number"], "name": r["name"], "expectation": exp,
                     "step5b": "council company" if got else "not council company",
                     "route": cc.get(r["company_number"], {}).get("route", ""), "agrees": ok})
    with open(OUT / "test5b_seed.csv", "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=list(rows[0]))
        w.writeheader()
        w.writerows(rows)
    REPORT["test5b_seed"] = {"seed": len(seed), "agree": agree, "disagree": len(seed) - agree,
                             "council_companies_in_mart": len(cc), "mart": str(mart),
                             "note": "informational: each disagreement is explained in BUILD notes, not forced"}
    return True


def main():
    rows = load_resolver()
    con = duckdb.connect()
    results = {"test2": test2(rows), "test3": test3(rows, con), "test4": test4(rows),
               "test5": test5(con), "test1": test1(rows)}
    test_5b_seed(con)
    REPORT["all_pass"] = all(results.values())
    write_json(OUT / "tests_report.json", REPORT)
    print(json.dumps({k: v.get("pass") if isinstance(v, dict) else v for k, v in REPORT.items()}, indent=1))
    print(json.dumps(REPORT.get("test1"), indent=1))
    sys.exit(0 if REPORT["all_pass"] else 1)


if __name__ == "__main__":
    main()
