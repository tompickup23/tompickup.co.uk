#!/usr/bin/env python3
"""Donations x public-money overlay.

Joins Electoral Commission donation records (NW areas, 2015 onwards, from the
AI DOGE EC ETL extract) to the council supplier universe. Two evidence tiers:
  company-number  the EC record's company number matches the supplier's
                  resolved company number (strongest)
  name            normalised donor name equals a supplier name
Everything published is a verbatim register/authority fact; the page carries
"no finding of impropriety is made" (LEGAL.md vocabulary rules).
Unions and unincorporated associations are included with their donor_status
label: council payments to unions are typically payroll/facility
relationships, stated on the page.

Writes public/data/biz-money.json
"""
import json, sys
from collections import defaultdict
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
from resolve_suppliers import normalise, classify

ROOT = Path(__file__).resolve().parent.parent.parent
PROC = Path.home() / "observatory-data/processed"
def _ec_path():
    for c in [Path.home() / "clawd/burnley-council/data/shared/ec_donations.json",
              Path.home() / "aidoge/burnley-council/data/shared/ec_donations.json",
              Path("/root/aidoge/burnley-council/data/shared/ec_donations.json")]:
        if c.exists():
            return c
    raise SystemExit("ec_donations.json not found")
EC = _ec_path()

ec = json.loads(EC.read_text())
rows = [r for rs in ec.get("donations_by_area", {}).values() for r in rs]
# EC extract can duplicate a donation across areas; dedupe on ec_ref
seen, donations = set(), []
for r in rows:
    if r.get("ec_ref") and r["ec_ref"] in seen:
        continue
    seen.add(r.get("ec_ref"))
    donations.append(r)

uni = json.loads((PROC / "supplier_universe.json").read_text())["universe"]
ukeys = {u["key"]: u for u in uni}
res = json.loads((PROC / "pound.json").read_text())["resolved"]
crn_to_key = {r["crn"]: k for k, r in res.items() if r.get("crn")}

donors = {}
for r in donations:
    dn_raw = (r.get("donor_name") or "").strip()
    dn = normalise(dn_raw)
    if not dn:
        continue
    crn = (r.get("company_number") or "").strip().upper().replace(" ", "")
    crn = crn.zfill(8) if crn.isdigit() and crn else crn
    evidence, key = None, None
    if crn and crn in crn_to_key:
        evidence, key = "company-number", crn_to_key[crn]
    elif dn in ukeys:
        evidence, key = "name", dn
    if not key:
        continue
    u = ukeys[key]
    d = donors.setdefault(key, {
        "donorName": dn_raw, "donorCrnEc": crn or None,
        "supplierNames": u["names"][:3],
        "crn": res.get(key, {}).get("crn"),
        "donorStatus": r.get("donor_status"),
        "evidence": evidence,
        "tier": res.get(key, {}).get("tier", "unclassified"),
        "supplierTotalM": round(u["total"] / 1e6, 2),
        "bodies": u["bodies"],
        "recipients": defaultdict(float), "donations": []})
    if evidence == "company-number":
        d["evidence"] = "company-number"      # upgrade if any record has it
    d["recipients"][r.get("regulated_entity") or "Unknown"] += r.get("value") or 0
    d["donations"].append({"date": r.get("accepted_date"),
                           "value": r.get("value"),
                           "party": r.get("regulated_entity"),
                           "ecRef": r.get("ec_ref")})

out_rows = []
for key, d in donors.items():
    d["recipients"] = [{"party": p, "totalValue": round(v, 2)}
                       for p, v in sorted(d["recipients"].items(),
                                          key=lambda kv: -kv[1])]
    d["donationTotal"] = round(sum(x["value"] or 0 for x in d["donations"]), 2)
    d["donationCount"] = len(d["donations"])
    d["donations"] = sorted(d["donations"], key=lambda x: x["date"] or "",
                            reverse=True)[:20]
    d["donationsShown"] = len(d["donations"])
    d["isUnion"] = "trade union" in (d["donorStatus"] or "").lower()
    out_rows.append(d)
out_rows.sort(key=lambda d: -d["supplierTotalM"])

party_totals = defaultdict(float)
for d in out_rows:
    for rr in d["recipients"]:
        party_totals[rr["party"]] += rr["totalValue"]

from datetime import date as _date
out = {
    "$meta": {
        "generated": _date.today().isoformat(),
        "sources": [
            {"name": "Electoral Commission donation register (NW extract 2015 onwards)",
             "url": "https://search.electoralcommission.org.uk/", "licence": "OGL v3"},
            {"name": "Council transparency spending, 17 Lancashire bodies",
             "licence": "OGL v3 per council"}],
        "notes": [
            "This table lists organisations that appear BOTH in the Electoral "
            "Commission donation register AND as payees in Lancashire council "
            "transparency spending. Both facts are public records. No finding "
            "of impropriety is made about any organisation listed.",
            "Payments to trade unions are typically payroll deduction and "
            "facility-time relationships, not procurement.",
            "Matches are made on company number where the EC record carries "
            "one, otherwise on exact normalised name; the evidence level is "
            "shown per row."],
    },
    "summary": {
        "donationsChecked": len(donations),
        "supplierDonors": len(out_rows),
        "byParty": [{"party": p, "totalValue": round(v, 2)}
                    for p, v in sorted(party_totals.items(), key=lambda kv: -kv[1])],
    },
    "donors": out_rows,
}
(ROOT / "public/data/biz-money.json").write_text(json.dumps(out))
print(f"biz-money.json: {len(out_rows)} supplier-donors from "
      f"{len(donations)} donations; parties: "
      f"{[(r['party'], r['totalValue']) for r in out['summary']['byParty'][:6]]}")
