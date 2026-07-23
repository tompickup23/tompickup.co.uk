#!/usr/bin/env python3
"""Aggregate the public-contracts record for the tompickup /lgr/contracts page.

Two sources, two classes:
  - COUNCILS (AI DOGE procurement.json, 15 bodies)      -> class "council":
    the abolished authorities whose contracts the four new unitaries INHERIT on
    vesting day. The headline handover stats are computed from these alone.
  - OTHER PUBLIC BODIES (public_contracts.json, built by fetch_public_contracts.py)
    -> classes emergency / nhs / gov / education: the bodies that also appear on
    the /lgr/property map but are NOT reorganised. They keep their own contracts;
    they are shown as the wider public-sector procurement landscape, clearly
    separated from the inheritance story.

Every row carries the same contract-length fields (term, end date, framework,
extension option)."""
import json, os, html
from collections import Counter

DATA = "/Users/tompickup/clawd/burnley-council/data"
HERE = os.path.dirname(os.path.abspath(__file__))
OUT = HERE + "/lgr-contracts.json"
PUBLIC = HERE + "/public_contracts.json"
COUNTY = {"lancashire_cc"}
BODIES = {
    "lancashire_cc": "Lancashire County Council", "burnley": "Burnley Borough Council",
    "hyndburn": "Hyndburn Borough Council", "pendle": "Pendle Borough Council",
    "rossendale": "Rossendale Borough Council", "ribble_valley": "Ribble Valley Borough Council",
    "preston": "Preston City Council", "chorley": "Chorley Council",
    "south_ribble": "South Ribble Borough Council", "west_lancashire": "West Lancashire Borough Council",
    "fylde": "Fylde Borough Council", "wyre": "Wyre Borough Council",
    "lancaster": "Lancaster City Council", "blackburn": "Blackburn with Darwen Council",
    "blackpool": "Blackpool Council",
}
out = []
for cid, body in BODIES.items():
    p = f"{DATA}/{cid}/procurement.json"
    if not os.path.exists(p): continue
    d = json.load(open(p))
    for c in d.get("contracts", []):
        if c.get("status") != "awarded":
            continue
        val = c.get("awarded_value") or c.get("value_high") or None
        if val is not None and val <= 0: val = None
        # supplier: the feed repeats the name per award line; dedupe
        sup = c.get("awarded_supplier")
        if sup:
            parts = [p.strip() for p in html.unescape(sup).split(",") if p.strip()]
            sup = ", ".join(dict.fromkeys(parts))[:120]
        out.append({
            "buyer": body, "gc": "council",
            "grp": "county" if cid in COUNTY else "district",
            "title": html.unescape(c.get("title") or "")[:180],
            "desc": html.unescape(c.get("description") or "")[:300],
            "value": val,
            "supplier": sup or None,
            "date": c.get("awarded_date"),
            "months": c.get("contract_months"),
            "maxmo": c.get("max_term_months"),
            "end": c.get("contract_end"),
            "ext": bool(c.get("has_extension_option")),
            "framework": bool(c.get("framework")),
            "sme": c.get("awarded_to_sme"),
            "cat": html.unescape(c.get("cpv_description") or "")[:60],
            "url": c.get("url"),
        })

# --- Merge the other public bodies (emergency / nhs / gov / education) --------
GROUP_LABEL = {
    "council": "Councils (abolished on vesting day)",
    "emergency": "Police, fire & ambulance",
    "nhs": "NHS",
    "gov": "National agencies (Lancashire estate)",
    "education": "Colleges & universities",
}
pub_meta = {}
if os.path.exists(PUBLIC):
    pub = json.load(open(PUBLIC))
    pub_meta = {"counts_by_group": pub.get("counts_by_group", {}),
                "counts_by_body": pub.get("counts_by_body", {}),
                "published_from": pub.get("published_from")}
    for c in pub.get("contracts", []):
        # already awarded-only, deduped, and shaped by fetch_public_contracts.py
        out.append({
            "buyer": c["buyer"], "gc": c["grp"], "grp": c["grp"],
            "title": c.get("title") or "", "desc": c.get("desc") or "",
            "value": c.get("value"), "supplier": c.get("supplier"),
            "date": c.get("date"), "months": c.get("months"), "maxmo": c.get("maxmo"),
            "end": c.get("end"), "ext": bool(c.get("ext")), "framework": bool(c.get("framework")),
            "sme": c.get("sme"), "cat": c.get("cat") or "", "url": c.get("url"),
        })

out.sort(key=lambda x: (x["value"] or 0), reverse=True)

def stat_block(rows):
    terms = sorted(x["months"] for x in rows if x["months"])
    vals = [x["value"] for x in rows if x["value"]]
    core = [x["value"] for x in rows if x["value"] and not x["framework"]]
    return {
        "total": len(rows),
        "sum": round(sum(core)),
        "sum_incl_frameworks": round(sum(vals)),
        "valued": len(vals),
        "with_term": len(terms),
        "median_term_months": terms[len(terms) // 2] if terms else None,
        "max_term_months": terms[-1] if terms else None,
        "over_5yr": sum(1 for m in terms if m >= 60),
        "over_10yr": sum(1 for m in terms if m >= 120),
        "framework_count": sum(1 for x in rows if x["framework"]),
        "with_extension_option": sum(1 for x in rows if x["ext"]),
        "running_past_vesting_2028": sum(1 for x in rows if (x.get("end") or "") >= "2028-04-01"),
    }

councils = [x for x in out if x["gc"] == "council"]
others = [x for x in out if x["gc"] != "council"]
overall = stat_block(out)
meta = {
    "generated": "2026-07-23",
    "group_labels": GROUP_LABEL,
    "counts_by_group": {g: sum(1 for x in out if x["gc"] == g)
                        for g in GROUP_LABEL},
    # headline handover stats are COUNCILS ONLY: only they are abolished/inherited
    "council": stat_block(councils),
    "other": stat_block(others),
    "overall": overall,
    # top-level mirrors of the overall figures the page reads
    "total": overall["total"],
    "sum": overall["sum"],
    "buyers": len({x["buyer"] for x in out}),
    "suppliers": len({x["supplier"] for x in out if x["supplier"]}),
    "max_term_months": overall["max_term_months"],
    "running_past_vesting_2028": overall["running_past_vesting_2028"],
    "public_bodies": pub_meta,
    "source": "Contracts Finder: council contracts via the AI DOGE procurement ETL; "
              "police, fire, ambulance, NHS, national-agency and education contracts fetched directly. Awarded contracts 2015 to date.",
    "term_note": "Term is the published contract period. Contracts Finder does not record extension options, so terms are a floor: extensions only lengthen them.",
    "scope_note": "Only the councils are abolished by reorganisation, so only their contracts pass to the new unitaries on 1 April 2028. The other bodies keep their own contracts and are shown as the wider public-sector landscape. NHS and education coverage is partial: most of their tenders are published to Find a Tender / NHS Atamis, not Contracts Finder.",
}
json.dump({"meta": meta, "contracts": out}, open(OUT, "w"), separators=(",", ":"))
c, o = meta["council"], meta["other"]
print("TOTAL contracts:", overall["total"], "| councils", c["total"], "| other public bodies", o["total"])
print("by group:", meta["counts_by_group"])
print("COUNCILS  sum £{:,.0f} | running past vesting {}".format(c["sum"], c["running_past_vesting_2028"]))
print("OTHERS    sum £{:,.0f} | running past vesting {}".format(o["sum"], o["running_past_vesting_2028"]))
print("by group:", dict(Counter(x["grp"] for x in out)))
