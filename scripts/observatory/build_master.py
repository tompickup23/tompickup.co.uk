#!/usr/bin/env python3
"""Master business record: Lancashire register + shell-cluster detection.

Reads  ~/observatory-data/vps/lancs_register.csv.gz
Writes ~/observatory-data/processed/master.jsonl.gz  (one line per company)
       ~/observatory-data/processed/clusters.json    (address clusters, per LAD)
       ~/observatory-data/processed/master_aggregates.json (per-LAD rollups)

Method (from research/LANCASHIRE14_COMPANY_ANALYSIS_2026-06-05.md, recomputed
fresh): an address cluster is a postcode with >= 40 companies of which >= 50%
are in distress statuses. Clean rates exclude those postcodes. Distress =
Liquidation / Proposal to Strike off / any Administration / Receiver /
Voluntary Arrangement. Wording rule (LEGAL.md): clusters are described by
observable facts only.
"""
import csv, gzip, json
from collections import Counter, defaultdict
from datetime import date
from pathlib import Path

def _crosswalk_path():
    from pathlib import Path as _P
    import os
    for c in [os.environ.get("OBS_CROSSWALK"),
              _P.home() / "clawd/briefings/lancashire-business-observatory/geo_crosswalk.json",
              _P.home() / "aidoge/briefings/lancashire-business-observatory/geo_crosswalk.json"]:
        if c and _P(c).exists():
            return _P(c)
    raise SystemExit("geo_crosswalk.json not found; set OBS_CROSSWALK")


VPS = Path.home() / "observatory-data/vps"
PROC = Path.home() / "observatory-data/processed"
CROSSWALK = _crosswalk_path()

DISTRESS = {"Active - Proposal to Strike off", "Liquidation", "In Administration",
            "ADMINISTRATION ORDER", "In Administration/Administrative Receiver",
            "In Administration/Receiver Manager", "Voluntary Arrangement",
            "Live but Receiver Manager on at least one charge"}
TODAY = date.today()

xw = json.loads(CROSSWALK.read_text())["byAuthority"]
lad_to_unitary = {v["name"]: v["newUnitary"] for v in xw.values()}

rows = []
by_pc = defaultdict(list)
with gzip.open(VPS / "lancs_register.csv.gz", "rt") as f:
    for r in csv.DictReader(f):
        pc = (r["RegAddress.PostCode"] or "").strip().upper()
        sic1 = r["SICCode.SicText_1"] or ""
        inc = r["IncorporationDate"]
        try:
            d, m, y = inc.split("/")
            age_years = round((TODAY - date(int(y), int(m), int(d))).days / 365.25, 1)
        except Exception:
            age_years = None
        rec = {
            "crn": r["CompanyNumber"], "name": r["CompanyName"],
            "postcode": pc, "lad": r["lad_name"],
            "unitary2028": lad_to_unitary.get(r["lad_name"]),
            "status": r["CompanyStatus"],
            "companyType": r["CompanyCategory"],
            "distress": r["CompanyStatus"] in DISTRESS,
            "category": r["Accounts.AccountCategory"],
            "cic": r["cic"] == "true",
            "sic1": sic1, "sic2": sic1[:2] if sic1[:2].isdigit() else None,
            "incorporated": inc, "ageYears": age_years,
        }
        rows.append(rec)
        by_pc[pc].append(rec)

# --- address clusters -------------------------------------------------------
clusters = []
cluster_pcs = set()
for pc, rs in by_pc.items():
    if not pc or len(rs) < 40:
        continue
    dis = sum(1 for r in rs if r["distress"])
    if dis / len(rs) >= 0.5:
        cluster_pcs.add(pc)
        clusters.append({
            "postcode": pc, "lad": Counter(r["lad"] for r in rs).most_common(1)[0][0],
            "companies": len(rs),
            "distressPct": round(100 * dis / len(rs)),
            "note": f"{len(rs)} companies registered at this postcode; "
                    f"{round(100 * dis / len(rs))} percent dissolved, in "
                    f"liquidation or facing strike-off. Excluded from clean rates.",
        })
clusters.sort(key=lambda c: -c["companies"])
for r in rows:
    r["cluster"] = r["postcode"] in cluster_pcs

# --- per-LAD aggregates -----------------------------------------------------
agg = {}
for r in rows:
    a = agg.setdefault(r["lad"], {
        "companies": 0, "distress": 0, "cleanCompanies": 0, "cleanDistress": 0,
        "cics": 0, "clusterExcluded": 0, "byCategory": Counter(),
        "bySic2": Counter(), "sic2New3yr": Counter(), "activeBySic2": Counter()})
    a["companies"] += 1
    a["distress"] += r["distress"]
    a["cics"] += r["cic"]
    a["byCategory"][r["category"]] += 1
    if r["cluster"]:
        a["clusterExcluded"] += 1
    else:
        a["cleanCompanies"] += 1
        a["cleanDistress"] += r["distress"]
        if r["sic2"]:
            a["bySic2"][r["sic2"]] += 1
            if not r["distress"]:
                a["activeBySic2"][r["sic2"]] += 1
            if r["ageYears"] is not None and r["ageYears"] <= 3.0:
                a["sic2New3yr"][r["sic2"]] += 1

out_agg = {}
for lad, a in agg.items():
    out_agg[lad] = {
        "companies": a["companies"],
        "distressRawPct": round(100 * a["distress"] / a["companies"], 1),
        "cleanCompanies": a["cleanCompanies"],
        "distressCleanPct": round(100 * a["cleanDistress"] / a["cleanCompanies"], 1),
        "clusterExcluded": a["clusterExcluded"],
        "cics": a["cics"],
        "byCategory": dict(a["byCategory"].most_common()),
        "topSic2": [{"sic2": s, "count": c, "new3yr": a["sic2New3yr"].get(s, 0)}
                    for s, c in a["bySic2"].most_common(15)],
    }

PROC.mkdir(parents=True, exist_ok=True)
with gzip.open(PROC / "master.jsonl.gz", "wt") as f:
    for r in rows:
        f.write(json.dumps(r) + "\n")
(PROC / "clusters.json").write_text(json.dumps(
    {"$meta": {"rule": ">=40 companies at one postcode with >=50% in distress statuses",
               "asAt": f"{TODAY.isoformat()} run, latest monthly register snapshot"},
     "clusters": clusters}, indent=1))
(PROC / "master_aggregates.json").write_text(json.dumps(
    {"$meta": {"asAt": f"{TODAY.isoformat()} run, latest monthly register snapshot", "distressStatuses": sorted(DISTRESS)},
     "byLad": out_agg}, indent=1))

print(f"master: {len(rows)} companies, {len(clusters)} clusters "
      f"({sum(c['companies'] for c in clusters)} companies excluded)")
for c in clusters[:12]:
    print(f"  {c['postcode']} ({c['lad']}): {c['companies']} cos, {c['distressPct']}%")
for lad in sorted(out_agg):
    o = out_agg[lad]
    print(f"{lad}: {o['companies']} cos, raw {o['distressRawPct']}% -> clean {o['distressCleanPct']}%")
