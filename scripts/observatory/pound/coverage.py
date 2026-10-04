#!/usr/bin/env python3
"""Coverage per body and financial year (method rule 2.4, RULES.md decisions
12 and 14).

For each body and year, from payee_key_values.csv, resolver_proposed.csv,
the verification tables, agent_labels.csv (when the agent review has run)
and the spend input:

  * the supplier total as the council published it, shown on three lines
    (decision 14): supplier spend; public bodies (precepts, rates shares,
    tariffs, levies, other councils, departments); council companies, the
    body's own apart from other councils';
  * on supplier spend only: the share resolved by a pipeline step, the share
    corroborated (two or more independent registers name the organisation),
    the share agent-reviewed (two independent model passes agree on same),
    the share hand-verified (0%: no person verifies rows, decision 12), and
    the share per unclassified reason;
  * the separate lines the spend input discloses (County accrual-coded
    payments, payments to individuals under the withheld key, exclusions);
    the publication threshold and VAT basis; and spend under the threshold,
    stated as unknown in size.

Pounds to the penny; percentages to one decimal place. Corroborated and
agent-reviewed are never called verification.
"""
import csv
import json
import sys
from collections import defaultdict

from common import INPUTS, OUT, write_json

csv.field_size_limit(sys.maxsize)


def line_of(x):
    """The decision 14 line a resolver row's value sits on."""
    if x["org_id"] and x["payee_class"] == "public body":
        return "public_bodies"
    if x["org_id"] and x["payee_class"] == "council company":
        ev = json.loads(x["evidence"]) if x["evidence"] else {}
        own = ev.get("council_company", {}).get("owner") == "the paying body's own company"
        return "council_companies_own" if own else "council_companies_other"
    return "supplier_spend"


def agent_reviewed_keys():
    """Payee keys both agent passes labelled same (decision 12). Empty until
    agent_review.py has run."""
    p = OUT / "agent_labels.csv"
    out = set()
    if p.exists():
        with open(p, newline="") as f:
            for r in csv.DictReader(f):
                if r["agreed_label"] == "same":
                    out.add((r["body_id"], r["payee_key"]))
    return out


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
    agent = agent_reviewed_keys()
    agg = defaultdict(lambda: defaultdict(int))
    with open(OUT / "payee_key_values.csv", newline="") as f:
        for r in csv.DictReader(f):
            k = (r["body_id"], r["payee_key_out"])
            x = res[k]
            a = agg[(r["body_id"], r["financial_year"])]
            v = int(r["value_pence"])
            a["total"] += v
            line = line_of(x)
            a[f"line:{line}"] += v
            if x["org_id"]:
                a[f"class:{x['payee_class']}"] += v
            else:
                a[f"reason:{x['reason']}"] += v
            if line != "supplier_spend":
                continue
            if x["org_id"]:
                a["resolved"] += v
                a[f"method:{x['method']}"] += v
                if x["corroborated"] == "True":
                    a["corroborated"] += v
                if k in agent:
                    a["agent_reviewed"] += v
            if r["payee_key_out"] in verified[r["body_id"]]:
                a["hand_verified"] += v
    rows, out = [], {}
    for (b, fy), a in sorted(agg.items()):
        tot = a["total"]
        ss = a["line:supplier_spend"]
        e = bodies[b]
        fyk = fy.replace("/", "-")
        sep = e["separateLines"]["accrual_journal"]["byYear"].get(fyk, {"rows": 0, "value": 0})
        wh = e["withheld"]["byYear"].get(fyk, {"rows": 0, "value": 0})
        ex = e["excludedFromSuppliers"].get(fyk, {})

        def pct(v):
            return round(100 * v / ss, 1) if ss else None
        d = {"body_id": b, "financial_year": fy, "supplier_total": tot / 100,
             "line_supplier_spend": ss / 100, "line_public_bodies": a["line:public_bodies"] / 100,
             "line_council_companies_own": a["line:council_companies_own"] / 100,
             "line_council_companies_other": a["line:council_companies_other"] / 100,
             "supplier_spend_resolved": a["resolved"] / 100, "supplier_spend_resolved_pct": pct(a["resolved"]),
             "supplier_spend_corroborated": a["corroborated"] / 100,
             "supplier_spend_corroborated_pct": pct(a["corroborated"]),
             "supplier_spend_agent_reviewed": a["agent_reviewed"] / 100,
             "supplier_spend_agent_reviewed_pct": pct(a["agent_reviewed"]),
             "hand_verified": a["hand_verified"] / 100, "hand_verified_pct": pct(a["hand_verified"]),
             "threshold_pounds": e["threshold"]["pounds"], "threshold_stated": e["threshold"]["stated"],
             "vat_basis": e["vatBasis"]["basis"],
             "below_threshold_spend": "not published; unknown in size",
             "rc03_accrual_payments_rows": sep["rows"], "rc03_accrual_payments_value": sep["value"],
             "withheld_individuals_rows": wh["rows"], "withheld_individuals_value": wh["value"],
             "excluded_from_suppliers": ex,
             "by_class": {k[6:]: v / 100 for k, v in a.items() if k.startswith("class:")},
             "by_method": {k[7:]: v / 100 for k, v in a.items() if k.startswith("method:")},
             "by_unclassified_reason": {k[7:]: v / 100 for k, v in a.items() if k.startswith("reason:")}}
        d["by_unclassified_reason_pct"] = {k: round(100 * v * 100 / ss, 1)
                                           for k, v in d["by_unclassified_reason"].items()} if ss else {}
        out[f"{b} {fy}"] = d
        rows.append(d)
    write_json(OUT / "coverage.json", {"$meta": {
        "rule": "RULES.md method rule 2.4; decisions 12 and 14",
        "note": "The supplier total is each body's published spend (bank net less internal transfers, settlement "
                "noise and purchase cards), shown on three lines: supplier spend, public bodies and council "
                "companies (the body's own apart from other councils'). Resolved, corroborated, agent-reviewed, "
                "hand-verified and unclassified shares are of supplier spend only. Hand-verified is 0%: no person "
                "verifies rows (decision 12). Corroborated (two or more independent registers name the "
                "organisation) and agent-reviewed (two independent model passes agree) are not verification. "
                "Unresolved keys stay in supplier spend whatever they are. Spend under each body's publication "
                "threshold is not published and is unknown in size.",
        "notCovered": spend["$meta"]["notCovered"]}, "bodies": out})
    flat = ["body_id", "financial_year", "supplier_total", "line_supplier_spend", "line_public_bodies",
            "line_council_companies_own", "line_council_companies_other", "supplier_spend_resolved",
            "supplier_spend_resolved_pct", "supplier_spend_corroborated", "supplier_spend_corroborated_pct",
            "supplier_spend_agent_reviewed", "supplier_spend_agent_reviewed_pct", "hand_verified",
            "hand_verified_pct", "threshold_pounds", "threshold_stated", "vat_basis", "below_threshold_spend",
            "rc03_accrual_payments_rows", "rc03_accrual_payments_value", "withheld_individuals_rows",
            "withheld_individuals_value"]
    reasons = sorted({k for d in rows for k in d["by_unclassified_reason"]})
    classes = sorted({k for d in rows for k in d["by_class"]})
    money = {c for c in flat if c in ("supplier_total", "hand_verified") or c.startswith("line_")
             or (c.startswith("supplier_spend_") and not c.endswith("_pct"))}
    with open(OUT / "coverage.csv", "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(flat + [f"class:{c}" for c in classes] + [f"unclassified:{r}" for r in reasons])
        for d in rows:
            w.writerow([f"{d[c]:.2f}" if c in money else d[c] for c in flat] +
                       [f"{d['by_class'].get(c, 0):.2f}" for c in classes] +
                       [f"{d['by_unclassified_reason'].get(r, 0):.2f}" for r in reasons])
    print(json.dumps({k: (v["supplier_spend_resolved_pct"], v["supplier_spend_corroborated_pct"],
                          v["supplier_spend_agent_reviewed_pct"]) for k, v in out.items()}, indent=1))


if __name__ == "__main__":
    main()
