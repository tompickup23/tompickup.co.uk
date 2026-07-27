#!/usr/bin/env python3
"""Company dossiers: per-company JSON for the curated dossier set.

Set = Lancashire-registered (master) companies that are any of:
  - a resolved council supplier with >= £500k over the 3-year window
  - a growth-engine candidate
  - an Innovate UK winner with >= £100k awarded
  - a 100+ employee firm in the latest filed accounts
Everything shown is a register/authority fact with its date; derived items
(tier, growth flags) carry their basis. LEGAL.md green/amber rules apply;
PSC individuals shown as name + country only (no DOB, no address).

Writes public/data/company/{crn}.json + public/data/biz-companies-index.json
"""
import gzip, json, sys
from collections import defaultdict
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
from resolve_suppliers import normalise, classify
from sic_labels import SIC2

ROOT = Path(__file__).resolve().parent.parent.parent
OUT = ROOT / "public/data/company"
OUT.mkdir(parents=True, exist_ok=True)
PROC = Path.home() / "observatory-data/processed"
VPS = Path.home() / "observatory-data/vps"
GEN = __import__("datetime").date.today().isoformat()

master = {}
with gzip.open(PROC / "master.jsonl.gz", "rt") as f:
    for line in f:
        r = json.loads(line)
        master[r["crn"]] = r

# accounts series
series = defaultdict(dict)
def lines():
    with gzip.open(VPS / "lancs_accounts.jsonl.gz", "rt") as f:
        yield from f
    bf = VPS / "lancs_accounts_backfill.jsonl"
    if bf.exists():
        yield from bf.open()
for line in lines():
    r = json.loads(line)
    series[r["crn"]][r["period_end"]] = {
        "periodEnd": r["period_end"], "employees": r.get("employees"),
        "equity": r.get("equity"), "totalAssets": r.get("total_assets"),
        "cash": r.get("cash")}

growth = {c["crn"]: c for c in json.loads((PROC / "growth.json").read_text())["candidates"]}
resolved = json.loads((PROC / "pound.json").read_text())["resolved"]
uni = json.loads((PROC / "supplier_universe.json").read_text())["universe"]
spend = json.loads((PROC / "council_supplier_spend.json").read_text())["bodies"]

DISPLAY = {"blackburn": "Blackburn with Darwen BC", "blackpool": "Blackpool BC",
           "burnley": "Burnley BC", "chorley": "Chorley BC", "fylde": "Fylde BC",
           "hyndburn": "Hyndburn BC", "lancashire_cc": "Lancashire CC",
           "lancashire_fire": "Lancashire Fire and Rescue",
           "lancashire_pcc": "Lancashire PCC", "lancaster": "Lancaster CC",
           "pendle": "Pendle BC", "preston": "Preston CC",
           "ribble_valley": "Ribble Valley BC", "rossendale": "Rossendale BC",
           "south_ribble": "South Ribble BC",
           "west_lancashire": "West Lancashire BC", "wyre": "Wyre BC"}

# reverse map: crn -> universe entries; and per-body spend by supplier name
crn_universe = defaultdict(list)
for u in uni:
    r = resolved.get(u["key"])
    if r and r.get("crn"):
        crn_universe[r["crn"]].append(u)

def payments_for(crn):
    rows = defaultdict(lambda: {"total": 0.0, "years": set()})
    for u in crn_universe.get(crn, []):
        for body in u["bodies"]:
            d = spend[body]
            for name in u["names"]:
                amt = d["suppliers"].get(name)
                if amt:
                    rows[body]["total"] += amt
                    rows[body]["years"].update(d["years"])
    return [{"body": DISPLAY[b], "totalM": round(v["total"] / 1e6, 2),
             "years": sorted(v["years"])} for b, v in
            sorted(rows.items(), key=lambda kv: -kv[1]["total"])]

# PSC individuals (name + country only) and gazette notices per crn
psc = defaultdict(list)
with gzip.open(VPS / "lancs_psc.jsonl.gz", "rt") as f:
    for line in f:
        r = json.loads(line)
        if r.get("kind") == "individual-person-with-significant-control" and not r.get("ceased_on"):
            psc[r["company_number"]].append({
                "name": r.get("name"),
                "country": r.get("country_of_residence") or None})
gaz = defaultdict(list)
for n in json.loads((PROC / "gazette_lancs.json").read_text()).get("notices", []):
    if n.get("company_number"):
        gaz[n["company_number"]].append({"date": n.get("date"),
                                         "type": n.get("type"),
                                         "uri": n.get("uri")})

# innovation per crn
inn = defaultdict(list)
for p in json.loads((PROC / "innovate_lancs.json").read_text()).get("projects", []):
    if p.get("crn"):
        inn[p["crn"].zfill(8) if p["crn"].isdigit() else p["crn"]].append(p)

# premises evidence per normalised name (LAD names, never GSS codes)
import os
def _crosswalk_path():
    for c in [os.environ.get("OBS_CROSSWALK"),
              Path.home() / "clawd/briefings/lancashire-business-observatory/geo_crosswalk.json",
              Path.home() / "aidoge/briefings/lancashire-business-observatory/geo_crosswalk.json"]:
        if c and Path(c).exists():
            return Path(c)
    raise SystemExit("geo_crosswalk.json not found; set OBS_CROSSWALK")
_xw = json.loads(_crosswalk_path().read_text())["byAuthority"]
CODE_TO_NAME = {v["ons"]: v["name"] for v in _xw.values()}
def _lad_name(v):
    return CODE_TO_NAME.get(v, v) if v else ""

fhrs_by_name = defaultdict(set)
for e in json.loads((PROC / "fhrs_lancs.json").read_text()).get("establishments", []):
    if e.get("name"):
        fhrs_by_name[normalise(e["name"])].add(_lad_name(e.get("la") or e.get("lad")))
cqc_by_name = defaultdict(set)
for e in json.loads((PROC / "cqc_lancs.json").read_text()).get("locations", []):
    for k in ("provider_name", "location_name"):
        if e.get(k):
            cqc_by_name[normalise(e[k])].add(_lad_name(e.get("lad")))

# officers seed (active appointments, name+role only per GDPR policy)
officers = {}
of = VPS / "officers_seed.jsonl"
if of.exists():
    from collections import defaultdict as _dd
    officers = _dd(list)
    for line in of.open():
        o = json.loads(line)
        officers[o["crn"]].append({"name": o.get("name"),
                                   "role": o.get("officer_role"),
                                   "appointed": o.get("appointed_on")})

# momentum (may not exist yet)
momentum = {}
mf = VPS / "momentum.jsonl"
if mf.exists():
    for line in mf.open():
        r = json.loads(line)
        momentum[r["crn"]] = r

# verified websites (verify_websites.py). Only rows that carry both a match
# method and the evidence snippet are publishable; anything else is dropped
# here rather than trusted downstream.
websites = {}
_wf = PROC / "websites.jsonl"
if _wf.exists():
    for line in _wf.open():
        try:
            r = json.loads(line)
        except Exception:
            continue
        if (r.get("matchedOn") in ("crn", "name-postcode") and r.get("evidence")
                and str(r.get("url", "")).startswith(("http://", "https://"))):
            websites[r["crn"]] = {
                "url": r["url"], "matchedOn": r["matchedOn"],
                "evidence": r["evidence"][:200],
                "evidenceUrl": r.get("evidenceUrl"),
                "checkedAt": r.get("checkedAt")}
print(f"verified websites available: {len(websites)}")

_clusters = {c["postcode"]: c for c in json.loads(
    (PROC / "clusters.json").read_text())["clusters"]}
def _cluster_note(pc):
    c = _clusters.get(pc)
    if not c:
        return None
    return (f"The registered office postcode {pc} hosts {c['companies']} "
            f"companies, {c['distressPct']} percent of them dissolved, in "
            f"liquidation or facing strike-off. Such postcodes are excluded "
            f"from the Observatory's clean rates; this notes the pattern at "
            f"the address, not anything about this company.")

# ---- curated set -----------------------------------------------------------
crns = set()
for crn, us in crn_universe.items():
    if crn in master and sum(u["total"] for u in us) >= 500_000:
        crns.add(crn)
crns |= set(growth) & set(master)
for crn, ps in inn.items():
    if crn in master and sum(p.get("award_offered") or 0 for p in ps) >= 100_000:
        crns.add(crn)
for crn, ss in series.items():
    if crn in master:
        latest = sorted(ss)[-1]
        e = ss[latest].get("employees")
        if e and e >= 100:
            crns.add(crn)
print(f"dossier set: {len(crns)} companies")

index = []
for crn in sorted(crns):
    m = master[crn]
    key = normalise(m["name"])
    r = next((resolved[normalise(u["names"][0])] for u in crn_universe.get(crn, [])
              if normalise(u["names"][0]) in resolved), None)
    ss = sorted(series.get(crn, {}).values(), key=lambda x: x["periodEnd"])
    lads = set()
    if key in fhrs_by_name:
        lads |= {l for l in fhrs_by_name[key] if l}
    if key in cqc_by_name:
        lads |= {l for l in cqc_by_name[key] if l}
    g = growth.get(crn)
    mo = momentum.get(crn)
    d = {
        "$meta": {"generated": GEN,
                  "note": "Register facts as at the snapshot dates shown; "
                          "assessments are Observatory opinion with their "
                          "basis stated. Report errors: "
                          "tom.pickup@lancashire.gov.uk"},
        "crn": crn, "name": m["name"],
        "register": {
            "status": m["status"], "companyType": m["companyType"],
            "accountsCategory": m["category"], "cic": m["cic"],
            "sic": m["sic1"] or None,
            "sicLabel": SIC2.get(m["sic2"] or "", None),
            "incorporated": m["incorporated"],
            "registeredPostcode": m["postcode"], "lad": m["lad"],
            "unitary2028": m["unitary2028"],
            "chUrl": f"https://find-and-update.company-information.service.gov.uk/company/{crn}",
            "addressClusterNote": (_cluster_note(m["postcode"])
                                   if m["cluster"] else None),
        },
        "website": websites.get(crn),
        "accountsSeries": ss[-6:],
        "payments": payments_for(crn),
        "innovation": [{"title": p.get("project_title"),
                        "year": p.get("competition_year"),
                        "type": p.get("product_type"),
                        "awardK": round((p.get("award_offered") or 0) / 1e3, 1)}
                       for p in inn.get(crn, [])],
        "gazetteNotices": gaz.get(crn, []),
        "ownership": {
            "tier": r.get("tier") if r else None,
            "chain": r.get("chain") if r else [],
            "evidence": r.get("evidence") if r else None,
            "pscIndividuals": psc.get(crn, [])[:8],
        },
        "premisesEvidence": {
            "fhrsLads": sorted(l for l in fhrs_by_name.get(key, set()) if l),
            "cqcLads": sorted(l for l in cqc_by_name.get(key, set()) if l),
        },
        "footprint": {
            "registeredLad": m["lad"], "evidenceLads": sorted(lads),
            "unitary2028": m["unitary2028"],
        },
        "growth": ({"series": g["series"], "cagrPct": g["cagrPct"],
                    "flags": g["flags"], "basis": g["basis"]} if g else None),
        "officers": (officers.get(crn) or [])[:15],
        "momentum": ({"sh01Last24m": mo["sh01_24m"],
                      "newCharges24m": mo["charges_24m"]} if mo else None),
    }
    (OUT / f"{crn}.json").write_text(json.dumps(d))
    index.append({"crn": crn, "name": m["name"], "lad": m["lad"],
                  "sic2": m["sic2"],
                  "hasPayments": bool(d["payments"]),
                  "isGrowth": bool(g),
                  "hasWebsite": crn in websites})

_pub_sites = [websites[c] for c in crns if c in websites]
_by_method = {}
for w in _pub_sites:
    _by_method[w["matchedOn"]] = _by_method.get(w["matchedOn"], 0) + 1

(ROOT / "public/data/biz-companies-index.json").write_text(json.dumps(
    {"$meta": {"generated": GEN,
               "criteria": "council supplier >= £500k over 3 years, growth "
                           "candidate, Innovate UK winner >= £100k, or 100+ "
                           "employees in latest filed accounts; Lancashire-"
                           "registered companies only",
               "websites": {
                   "verified": len(_pub_sites),
                   "ofCompanies": len(index),
                   "matchRatePct": (round(100.0 * len(_pub_sites) / len(index), 1)
                                    if index else None),
                   "byMethod": _by_method,
                   "rule": "A website is shown only where the site itself "
                           "proves the match: the company registration number "
                           "on the page, or the exact registered name with the "
                           "registered-office postcode."}},
     "companies": index}))
print(f"wrote {len(index)} dossiers + index")
