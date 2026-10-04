#!/usr/bin/env python3
"""Aggregates for the draft UK Councils pages (Phase 1 step 6). Draft only:
nothing here is verified or published (RULES.md decisions 7 and 8).

Per body and financial year, in pence: the supplier total on its three
lines (supplier spend, public bodies, council companies split into the
body's own and other councils', RULES.md decision 14); on supplier spend,
the value resolved, corroborated, agent-reviewed and hand-verified (0%, no
person verifies rows, decision 12) and the value unclassified, by reason;
and, for resolved suppliers with a Companies House number, the value by the
ownership walk's class at the top of the chain (method rules 2.1 and 2.2),
with the count of companies and payee keys. Beside it, labelled as such, the
same lines if a shares band above 50% were counted as control (decision 17).
The individual class is an aggregate only: a count and a value per body,
never a company, and never a country (decision 20). No payee name, company
number or person is written: the file holds classes and sums only, so no
supplier string can reach a page from it.

Reads coverage.json (for the threshold, VAT basis and separate lines),
resolver_proposed.csv, payee_key_values.csv and ownership_walk.csv in
$POUND_WORK/out; writes pound_pages.json there.
"""
import csv
import json
import sys
from collections import defaultdict
from datetime import datetime, timezone

from common import OUT, YEARS, git_sha, sha256_file, write_json

csv.field_size_limit(sys.maxsize)

# The order classes are shown in. Fixed, never by value (no ranking).
OWNERSHIP_ORDER = [
    "individual", "foreign entity", "dispersed listed", "exempt listed", "trust or trustee", "no PSC filed",
    "no unit over 50%", "more than 50% of shares, voting rights not filed", "more than one controlling parent filed",
    "corporate parent without a register number", "corporate parent, no further PSC data", "no PSC data held",
    "cycle", "resolved, no Companies House number", "not walked",
]


def main():
    cov = json.loads((OUT / "coverage.json").read_text())
    res = {}
    with open(OUT / "resolver_proposed.csv", newline="") as f:
        for r in csv.DictReader(f):
            res[(r["body_id"], r["payee_key"])] = r
    walk, walk_sh = {}, {}
    with open(OUT / "ownership_walk.csv", newline="") as f:
        for r in csv.DictReader(f):
            walk[(r["company_number"], r["financial_year"])] = "cycle" if r["cycle"] == "True" else r["class"]
            walk_sh[(r["company_number"], r["financial_year"])] = r["class_if_shares_counted"]
    own = defaultdict(lambda: defaultdict(int))
    own_sh = defaultdict(lambda: defaultdict(int))
    counts = defaultdict(lambda: defaultdict(int))
    companies = defaultdict(lambda: defaultdict(set))
    with open(OUT / "payee_key_values.csv", newline="") as f:
        for r in csv.DictReader(f):
            x = res[(r["body_id"], r["payee_key_out"])]
            if not x["org_id"] or x["payee_class"] != "supplier":
                continue
            crn = x["org_id"] if x["org_scheme"] == "GB-COH" else next(
                (a[7:] for a in (x["alias_ids"] or "").split("|") if a.startswith("GB-COH-")), None)
            fy = r["financial_year"]
            cls = walk.get((crn, fy), "not walked") if crn else "resolved, no Companies House number"
            cls_sh = walk_sh.get((crn, fy), "not walked") if crn else "resolved, no Companies House number"
            own[(r["body_id"], fy)][cls] += int(r["value_pence"])
            own_sh[(r["body_id"], fy)][cls_sh] += int(r["value_pence"])
            counts[(r["body_id"], fy)][cls] += 1
            if crn:
                companies[(r["body_id"], fy)][cls].add(crn)
    bodies = defaultdict(lambda: {"years": {}})
    for key, d in cov["bodies"].items():
        b, fy = d["body_id"], d["financial_year"]
        pence = lambda v: round(v * 100)
        o = own[(b, fy)]
        osh = own_sh[(b, fy)]
        unknown = [c for c in list(o) + list(osh) if c not in OWNERSHIP_ORDER]
        if unknown:
            raise SystemExit(f"ownership class without a place in OWNERSHIP_ORDER: {unknown}")
        bodies[b]["years"][fy] = {
            "supplierTotalPence": pence(d["supplier_total"]),
            "linesPence": {"supplierSpend": pence(d["line_supplier_spend"]),
                           "publicBodies": pence(d["line_public_bodies"]),
                           "councilCompaniesOwn": pence(d["line_council_companies_own"]),
                           "councilCompaniesOther": pence(d["line_council_companies_other"])},
            "supplierSpendResolvedPence": pence(d["supplier_spend_resolved"]),
            "supplierSpendCorroboratedPence": pence(d["supplier_spend_corroborated"]),
            "supplierSpendAgentReviewedPence": pence(d["supplier_spend_agent_reviewed"]),
            "handVerifiedPence": pence(d["hand_verified"]),
            "thresholdPounds": d["threshold_pounds"], "thresholdStated": d["threshold_stated"],
            "vatBasis": d["vat_basis"],
            "byClassPence": {k: pence(v) for k, v in d["by_class"].items()},
            "byReasonPence": {k: pence(v) for k, v in d["by_unclassified_reason"].items()},
            "rc03AccrualPayments": {"rows": d["rc03_accrual_payments_rows"],
                                    "pence": pence(d["rc03_accrual_payments_value"])},
            "withheldIndividuals": {"rows": d["withheld_individuals_rows"],
                                    "pence": pence(d["withheld_individuals_value"])},
            # decision 20: classes and sums only; no "where" or country field exists here
            "ownershipPence": [{"class": c, "pence": o[c], "payeeKeys": counts[(b, fy)][c],
                                "companies": len(companies[(b, fy)][c])}
                               for c in OWNERSHIP_ORDER if o.get(c)],
            "ownershipIfSharesCountedPence": {
                "label": "Sensitivity, not the method: the same lines if a shares band above 50% with no voting "
                         "rights filed were counted as control (RULES.md decision 17; rule 2.1 is voting power)",
                "lines": [{"class": c, "pence": osh[c]} for c in OWNERSHIP_ORDER if osh.get(c)]},
        }
        # the ownership lines must add up to the resolved supplier class
        if sum(osh.values()) != sum(o.values()):
            raise SystemExit(f"{b} {fy}: shares sensitivity lines do not add up to the ownership lines")
        if sum(o.values()) != bodies[b]["years"][fy]["byClassPence"].get("supplier", 0):
            raise SystemExit(f"{b} {fy}: ownership lines {sum(o.values())} != supplier class "
                             f"{bodies[b]['years'][fy]['byClassPence'].get('supplier', 0)}")
    walk_man = json.loads((OUT / "ownership_manifest.json").read_text())
    out = {"$meta": {
        "status": "DRAFT. Not verified, not published. Machine proposals from the Phase 1 waterfall; "
                  "no row has been accepted by a named person.",
        "pipelineGitSha": git_sha(),
        "generatedAt": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "years": YEARS,
        # a list of {file, sha256}: a hash stored under a file name with "key" in it reads to gitleaks as a secret
        "inputs": [{"file": p, "sha256": sha256_file(OUT / p)} for p in ("coverage.json", "resolver_proposed.csv",
                                                                         "payee_key_values.csv", "ownership_walk.csv")],
        "bodsAsAt": walk_man.get("bodsAsAt"),
        "notCovered": cov["$meta"].get("notCovered"),
        "note": "Pence. The supplier total (bank net less internal transfers, settlement noise and purchase cards) "
                "is shown on three lines (decision 14); resolved, corroborated, agent-reviewed and hand-verified "
                "values are of supplier spend only. Hand-verified is nil: no person verifies rows (decision 12). "
                "Corroborated and agent-reviewed are not verification. Spend under each body's publication "
                "threshold is not published and is unknown in size. Ownership lines cover resolved suppliers only; "
                "the individual class is a count and a value, with no country (decision 20)."},
        "bodies": dict(sorted(bodies.items()))}
    write_json(OUT / "pound_pages.json", out)
    print(json.dumps({b: {fy: (y["supplierSpendResolvedPence"], y["linesPence"]["supplierSpend"])
                          for fy, y in v["years"].items()}
                      for b, v in out["bodies"].items()}, indent=1))


if __name__ == "__main__":
    main()
