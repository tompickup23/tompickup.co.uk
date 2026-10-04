#!/usr/bin/env python3
"""Hand-verification tables (WATERFALL.md s3) for Tom.

Per body and financial year, payee keys are sorted by value; the keys that
together reach 80% of the body's supplier total for that year form the
verification set. A body's set is the union over 2024/25 and 2025/26.

  verify_<body>.csv    one row per key in the body's set, ranked by value
  verify_suppliers.csv the same keys deduplicated across bodies on the
                       normalised name, so a supplier paid by several bodies
                       is reviewed once; bodies and values listed
  verify_summary.json  counts per body (keys, value covered)

Each row: the proposed organisation, method, the evidence, a link to the
register record, the ownership class per year, and empty accept, reject,
change, reviewer, reviewed_at, minutes and notes columns. Nothing in these
files is a decision: only rows a named person accepts become resolver.csv.
Individual and redacted keys appear as a hash, never by name.
"""
import argparse
import csv
import json
import sys
from collections import defaultdict

from common import OUT, YEARS, normalise, write_json

csv.field_size_limit(sys.maxsize)

LINKS = {
    "GB-COH": "https://find-and-update.company-information.service.gov.uk/company/{}",
    "GB-CHC": "https://register-of-charities.charitycommission.gov.uk/en/charity-search/-/charity-details/{}",
    "XI-LEI": "https://search.gleif.org/#/record/{}",
}
REVIEW_COLS = ["accept", "reject", "change_to_scheme", "change_to_id", "reviewer", "reviewed_at",
               "minutes", "notes"]


def link(scheme, oid, ev):
    if scheme in LINKS and oid:
        return LINKS[scheme].format(oid)
    if ev.get("provider_id"):
        return f"https://www.cqc.org.uk/provider/{ev['provider_id']}"
    return ""


def register_name(ev):
    for k in ("register_name", "register_names"):
        v = ev.get(k)
        if v:
            return v if isinstance(v, str) else "; ".join(v[:2])
    return ev.get("register_name_norm", "")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--bodies", default="")
    ap.add_argument("--share", type=float, default=0.80)
    a = ap.parse_args()
    want = set(a.bodies.split(",")) if a.bodies else None
    res = {}
    with open(OUT / "resolver_proposed.csv", newline="") as f:
        for r in csv.DictReader(f):
            if want is None or r["body_id"] in want:
                res[(r["body_id"], r["payee_key"])] = r
    vals = defaultdict(lambda: defaultdict(int))
    with open(OUT / "payee_key_values.csv", newline="") as f:
        for r in csv.DictReader(f):
            if (r["body_id"], r["payee_key_out"]) in res:
                vals[(r["body_id"], r["payee_key_out"])][r["financial_year"]] += int(r["value_pence"])
    walk = defaultdict(dict)
    for p in sorted(OUT.glob("ownership_walk*.csv")):
        with open(p, newline="") as f:
            for r in csv.DictReader(f):
                walk[r["company_number"]][r["financial_year"]] = r
    bodies = sorted({k[0] for k in res})
    summary = {}
    sup = defaultdict(lambda: {"bodies": {}, "rows": []})
    for b in bodies:
        keys = [k for k in res if k[0] == b]
        chosen = set()
        stats = {}
        for fy in YEARS:
            tot = sum(vals[k][fy] for k in keys)
            cum, n = 0, 0
            for k in sorted(keys, key=lambda k: (-vals[k][fy], k)):  # ties by key, so reruns agree
                if cum >= a.share * tot or vals[k][fy] <= 0:
                    break
                cum += vals[k][fy]
                chosen.add(k)
                n += 1
            stats[fy] = {"supplier_total_pence": tot, "keys_to_80pc": n, "value_covered_pence": cum}
        rows = []
        for k in sorted(chosen, key=lambda k: (-sum(vals[k].values()), k)):
            r = res[k]
            ev = json.loads(r["evidence"]) if r["evidence"] else {}
            crn = r["org_id"] if r["org_scheme"] == "GB-COH" else next(
                (x[7:] for x in r["alias_ids"].split("|") if x.startswith("GB-COH-")), "")
            row = {"rank": len(rows) + 1, "body_id": b, "payee_key": k[1],
                   "value_2024_25": f"{vals[k]['2024/25'] / 100:.2f}",
                   "value_2025_26": f"{vals[k]['2025/26'] / 100:.2f}",
                   "value_total": f"{sum(vals[k].values()) / 100:.2f}",
                   "payee_class": r["payee_class"], "org_scheme": r["org_scheme"], "org_id": r["org_id"],
                   "alias_ids": r["alias_ids"], "register_name": register_name(ev),
                   "step": r["step"], "method": r["method"], "confidence_provisional": r["confidence"],
                   "register_link": link(r["org_scheme"], r["org_id"], ev),
                   "reason": r["reason"], "reason_detail": r["reason_detail"],
                   "ownership_2024_25": "", "ownership_2025_26": "",
                   "evidence": r["evidence"], "decision_id": r["decision_id"]}
            for fy, col in (("2024/25", "ownership_2024_25"), ("2025/26", "ownership_2025_26")):
                w = walk.get(crn, {}).get(fy)
                if w:
                    row[col] = f"{w['class']}" + (f" ({w['where']})" if w["where"] else "") + \
                        (f"; ultimate {w['ultimate_company']}" if w["chain_length"] != "0" else "") + \
                        (f"; {w['flag_2025_26']}" if w.get("flag_2025_26") else "")
            row.update({c: "" for c in REVIEW_COLS})
            rows.append(row)
            g = normalise(k[1]) or k[1]
            s = sup[g]
            s["bodies"][b] = sum(vals[k].values())
            s["rows"].append(row)
        with open(OUT / f"verify_{b}.csv", "w", newline="") as f:
            w = csv.DictWriter(f, fieldnames=list(rows[0]) if rows else ["rank"])
            w.writeheader()
            w.writerows(rows)
        summary[b] = {"keys": len(rows), "by_year": stats}
    out = []
    for g, s in sup.items():
        props = {(r["org_scheme"], r["org_id"], r["payee_class"]) for r in s["rows"]}
        r0 = max(s["rows"], key=lambda r: float(r["value_total"]))
        out.append({"supplier_group": g, "bodies": "|".join(sorted(s["bodies"])),
                    "n_bodies": len(s["bodies"]),
                    "value_total": f"{sum(s['bodies'].values()) / 100:.2f}",
                    "payee_keys": "|".join(sorted({r["payee_key"] for r in s["rows"]})),
                    "proposals_agree": len(props) == 1,
                    **{k: r0[k] for k in ("payee_class", "org_scheme", "org_id", "alias_ids", "register_name",
                                          "step", "method", "confidence_provisional", "register_link",
                                          "reason", "reason_detail", "ownership_2024_25", "ownership_2025_26",
                                          "evidence")},
                    "decision_ids": "|".join(r["decision_id"] for r in s["rows"]),
                    **{c: "" for c in REVIEW_COLS}})
    out.sort(key=lambda r: (-float(r["value_total"]), r["supplier_group"]))
    name = "verify_suppliers.csv" if want is None else f"verify_suppliers_{'_'.join(sorted(want))}.csv"
    with open(OUT / name, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=list(out[0]) if out else ["supplier_group"])
        w.writeheader()
        w.writerows(out)
    summary["_suppliers"] = {"distinct_groups": len(out),
                             "groups_paid_by_more_than_one_body": sum(1 for r in out if r["n_bodies"] > 1),
                             "groups_where_body_proposals_differ": sum(1 for r in out if not r["proposals_agree"])}
    write_json(OUT / ("verify_summary.json" if want is None else f"verify_summary_{'_'.join(sorted(want))}.json"), summary)
    print(json.dumps(summary, indent=1))


if __name__ == "__main__":
    main()
