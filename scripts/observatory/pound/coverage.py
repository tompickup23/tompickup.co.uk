#!/usr/bin/env python3
"""Coverage per body and financial year (method rule 2.4).

For each body and year, from payee_key_values.csv, resolver_proposed.csv,
the verification tables and the spend input: the supplier total; the share
resolved by any pipeline step (and by payee class: supplier, public body,
council company, gate decision); the share hand-verified (zero until a named
person completes the verification table); the share per unclassified
reason; the separate lines the spend input discloses (County accrual-coded
payments, payments to individuals under the withheld key, exclusions); the
publication threshold and VAT basis; and spend under the threshold, stated
as unknown in size. Shares are of the supplier total, in pounds to the
penny and as a percentage to one decimal place.
"""
import csv
import json
import sys
from collections import defaultdict

from common import INPUTS, OUT, write_json

csv.field_size_limit(sys.maxsize)


def main():
    spend = json.loads((INPUTS / "council_supplier_spend.json").read_text())
    bodies = spend["bodies"]
    res = {}
    with open(OUT / "resolver_proposed.csv", newline="") as f:
        for r in csv.DictReader(f):
            res[(r["body_id"], r["payee_key"])] = r
    verified = defaultdict(set)
    for p in OUT.glob("verify_*.csv"):
        if p.name.startswith(("verify_suppliers", "verify_summary")):
            continue
        with open(p, newline="") as f:
            for r in csv.DictReader(f):
                if (r.get("accept") or "").strip() and (r.get("reviewer") or "").strip():
                    verified[r["body_id"]].add(r["payee_key"])
    agg = defaultdict(lambda: defaultdict(int))
    with open(OUT / "payee_key_values.csv", newline="") as f:
        for r in csv.DictReader(f):
            k = (r["body_id"], r["payee_key_out"])
            x = res[k]
            a = agg[(r["body_id"], r["financial_year"])]
            v = int(r["value_pence"])
            a["total"] += v
            if x["org_id"]:
                a["resolved"] += v
                a[f"class:{x['payee_class']}"] += v
                a[f"method:{x['method']}"] += v
            else:
                a[f"reason:{x['reason']}"] += v
            if r["payee_key_out"] in verified[r["body_id"]]:
                a["hand_verified"] += v
    rows, out = [], {}
    for (b, fy), a in sorted(agg.items()):
        tot = a["total"]
        e = bodies[b]
        fyk = fy.replace("/", "-")
        sep = e["separateLines"]["accrual_journal"]["byYear"].get(fyk, {"rows": 0, "value": 0})
        wh = e["withheld"]["byYear"].get(fyk, {"rows": 0, "value": 0})
        ex = e["excludedFromSuppliers"].get(fyk, {})
        d = {"body_id": b, "financial_year": fy, "supplier_total": tot / 100,
             "resolved": a["resolved"] / 100, "resolved_pct": round(100 * a["resolved"] / tot, 1) if tot else None,
             "hand_verified": a["hand_verified"] / 100,
             "hand_verified_pct": round(100 * a["hand_verified"] / tot, 1) if tot else None,
             "threshold_pounds": e["threshold"]["pounds"], "threshold_stated": e["threshold"]["stated"],
             "vat_basis": e["vatBasis"]["basis"],
             "below_threshold_spend": "not published; unknown in size",
             "rc03_accrual_payments_rows": sep["rows"], "rc03_accrual_payments_value": sep["value"],
             "withheld_individuals_rows": wh["rows"], "withheld_individuals_value": wh["value"],
             "excluded_from_suppliers": ex,
             "by_class": {k[6:]: v / 100 for k, v in a.items() if k.startswith("class:")},
             "by_method": {k[7:]: v / 100 for k, v in a.items() if k.startswith("method:")},
             "by_unclassified_reason": {k[7:]: v / 100 for k, v in a.items() if k.startswith("reason:")}}
        d["by_unclassified_reason_pct"] = {k: round(100 * v * 100 / tot, 1) for k, v in d["by_unclassified_reason"].items()} if tot else {}
        d["by_class_pct"] = {k: round(100 * v * 100 / tot, 1) for k, v in d["by_class"].items()} if tot else {}
        out[f"{b} {fy}"] = d
        rows.append(d)
    write_json(OUT / "coverage.json", {"$meta": {
        "rule": "RULES.md method rule 2.4",
        "note": "Shares of each body's supplier total (bank net less internal transfers, settlement noise and "
                "purchase cards). Hand-verified is zero until a named person completes the verification table. "
                "Spend under each body's publication threshold is not published and is unknown in size.",
        "notCovered": spend["$meta"]["notCovered"]}, "bodies": out})
    flat = ["body_id", "financial_year", "supplier_total", "resolved", "resolved_pct", "hand_verified",
            "hand_verified_pct", "threshold_pounds", "threshold_stated", "vat_basis", "below_threshold_spend",
            "rc03_accrual_payments_rows", "rc03_accrual_payments_value", "withheld_individuals_rows",
            "withheld_individuals_value"]
    reasons = sorted({k for d in rows for k in d["by_unclassified_reason"]})
    classes = sorted({k for d in rows for k in d["by_class"]})
    with open(OUT / "coverage.csv", "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(flat + [f"class:{c}" for c in classes] + [f"unclassified:{r}" for r in reasons])
        for d in rows:
            w.writerow([d[c] for c in flat] + [f"{d['by_class'].get(c, 0):.2f}" for c in classes] +
                       [f"{d['by_unclassified_reason'].get(r, 0):.2f}" for r in reasons])
    print(json.dumps({k: (v["resolved_pct"], v["by_unclassified_reason_pct"]) for k, v in out.items()}, indent=1))


if __name__ == "__main__":
    main()
