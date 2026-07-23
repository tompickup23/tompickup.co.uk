#!/usr/bin/env python3
"""Aggregate AI DOGE procurement.json (14 Lancashire councils) into a single
lgr-contracts.json for the tompickup /lgr/contracts page, carrying the newly
captured contract length (term, end date, framework flag)."""
import json, os, html
from collections import Counter

DATA = "/Users/tompickup/clawd/burnley-council/data"
OUT = os.path.dirname(os.path.abspath(__file__)) + "/lgr-contracts.json"
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
            "buyer": body, "grp": "county" if cid in COUNTY else "district",
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

out.sort(key=lambda x: (x["value"] or 0), reverse=True)
terms = sorted(x["months"] for x in out if x["months"])
sup_set = {x["supplier"] for x in out if x["supplier"]}
vals = [x["value"] for x in out if x["value"]]
# Headline total excludes framework ceilings: those are call-off maximums (often
# national frameworks the council merely acceded to), not the council's own spend.
core_vals = [x["value"] for x in out if x["value"] and not x["framework"]]
meta = {
    "generated": "2026-07-23",
    "total": len(out),
    "sum": round(sum(core_vals)),
    "sum_incl_frameworks": round(sum(vals)),
    "valued": len(vals),
    "buyers": len({x["buyer"] for x in out}),
    "suppliers": len(sup_set),
    "with_term": len(terms),
    "median_term_months": terms[len(terms)//2] if terms else None,
    "max_term_months": terms[-1] if terms else None,
    "over_5yr": sum(1 for m in terms if m >= 60),
    "over_10yr": sum(1 for m in terms if m >= 120),
    "framework_count": sum(1 for x in out if x["framework"]),
    "with_extension_option": sum(1 for x in out if x["ext"]),
    "max_term_parsed": sum(1 for x in out if x.get("maxmo")),
    "running_past_vesting_2028": sum(1 for x in out if (x.get("end") or "") >= "2028-04-01"),
    "source": "Contracts Finder (via AI DOGE procurement ETL), awarded contracts 2015 to date",
    "term_note": "Term is the published contract period. Contracts Finder does not record extension options, so terms are a floor: extensions only lengthen them.",
}
json.dump({"meta": meta, "contracts": out}, open(OUT, "w"), separators=(",", ":"))
print("contracts:", len(out), "| sum £{:,.0f}".format(meta["sum"]))
print("with term:", meta["with_term"], "| median", meta["median_term_months"], "mo | max", meta["max_term_months"], "mo")
print("over 5yr:", meta["over_5yr"], "| over 10yr:", meta["over_10yr"], "| frameworks:", meta["framework_count"])
print("RUNNING PAST VESTING DAY 2028:", meta["running_past_vesting_2028"])
print("by group:", dict(Counter(x["grp"] for x in out)))
