#!/usr/bin/env python3
"""Fetch Contracts Finder awarded contracts for the NON-COUNCIL Lancashire public
bodies whose estates already appear on the /lgr/property map: the emergency
services, the NHS, national agencies and education. Councils come from the AI DOGE
procurement ETL (see build_lgr_contracts.py); this fills in everyone else, so the
public-contracts page matches the property map's coverage.

Reuses the tested search + parse logic from the AI DOGE procurement_etl module, so
every row carries the same contract-length fields (term, end date, framework flag,
extension option). Requires Python 3.11+ (procurement_etl imports datetime.UTC).

Output: public_contracts.json  ->  merged into lgr-contracts.json by build_lgr_contracts.py

Groups:
  emergency  Lancashire Constabulary/PCC, Fire & Rescue, NW Ambulance  (rich on CF)
  nhs        the Lancashire acute/community trusts + NHS Property Services
             (SPARSE on CF: trusts publish most tenders to Find a Tender / NHS
             Atamis, so this is a floor, not the full picture)
  gov        national agencies with a Lancashire estate (MoD, National Highways,
             Environment Agency, Homes England, Network Rail). CF search is national,
             so notices are kept only where a Lancashire link is present.
  education  colleges + universities + the larger academy trusts (also sparse on CF)
"""
import sys, os, json, re
from collections import Counter

ETL_DIR = "/Users/tompickup/clawd/burnley-council/scripts"
sys.path.insert(0, ETL_DIR)
from procurement_etl import search_contracts, parse_notice  # noqa: E402

OUT = os.path.join(os.path.dirname(os.path.abspath(__file__)), "public_contracts.json")
PUBLISHED_FROM = "2016-01-01"

# Lancashire relevance filter for NATIONAL bodies (gov, some education). A notice
# is kept only if its title/description/region/postcode mentions a Lancashire place.
# Whole-word place-name hints only. Postcode areas are checked separately against
# the postcode FIELD (never as substrings: "pr"/"fy"/"bb" occur inside ordinary
# words like "programme"/"notify"/"ribble" and would pass everything).
LANCS_HINTS = [
    "lancashire", "lancs", "blackburn", "blackpool", "burnley", "chorley", "clitheroe",
    "colne", "fleetwood", "fylde", "hyndburn", "accrington", "lancaster", "morecambe",
    "nelson", "ormskirk", "pendle", "preston", "ribble valley", "rossendale", "rawtenstall",
    "south ribble", "leyland", "west lancashire", "skelmersdale", "wyre", "poulton",
    "darwen", "padiham", "bacup", "longridge", "garstang", "kirkham", "penwortham",
]
_WORD = re.compile(r"[a-z]+(?: [a-z]+)?")

def lancs_relevant(p):
    blob = " ".join(str(p.get(k) or "") for k in ("title", "description", "region")).lower()
    # postcode-area check against the postcode FIELD only (not free text)
    pc = (p.get("postcode") or "").strip().upper()
    pc_area = re.match(r"([A-Z]{1,2})\d", pc)
    if pc_area and pc_area.group(1) in ("BB", "FY", "PR", "LA"):
        return True
    # whole-word match, so "preston" doesn't fire on "compression"
    return any(re.search(r"\b" + re.escape(h) + r"\b", blob) for h in LANCS_HINTS)

# id, display name, group, CF search terms, organisationName substrings to accept,
# national=True -> apply the Lancashire relevance filter.
BODIES = [
    # --- Emergency services -------------------------------------------------
    dict(id="lancashire_pcc", name="Lancashire Police (Constabulary & PCC)", group="emergency",
         terms=['"Lancashire Constabulary"', '"Police and Crime Commissioner for Lancashire"',
                '"Office of the Police and Crime Commissioner for Lancashire"'],
         match=["lancashire constabulary", "crime commissioner for lancashire", "lancashire police"]),
    dict(id="lancashire_fire", name="Lancashire Fire and Rescue Service", group="emergency",
         terms=['"Lancashire Fire and Rescue"', '"Lancashire Combined Fire Authority"'],
         match=["lancashire fire", "lancashire combined fire"]),
    dict(id="nwas", name="North West Ambulance Service", group="emergency",
         terms=['"North West Ambulance Service"'],
         match=["north west ambulance"]),
    # --- NHS (sparse on Contracts Finder) -----------------------------------
    dict(id="nhs_blackpool", name="Blackpool Teaching Hospitals NHS FT", group="nhs",
         terms=['"Blackpool Teaching Hospitals"'], match=["blackpool teaching hospitals"]),
    dict(id="nhs_eastlancs", name="East Lancashire Hospitals NHS Trust", group="nhs",
         terms=['"East Lancashire Hospitals"'], match=["east lancashire hospitals"]),
    dict(id="nhs_lancsteaching", name="Lancashire Teaching Hospitals NHS FT", group="nhs",
         terms=['"Lancashire Teaching Hospitals"'], match=["lancashire teaching hospitals"]),
    dict(id="nhs_lscft", name="Lancashire & South Cumbria NHS FT", group="nhs",
         terms=['"Lancashire and South Cumbria NHS"', '"Lancashire & South Cumbria NHS"'],
         match=["lancashire and south cumbria nhs", "lancashire & south cumbria nhs"]),
    dict(id="nhs_merseywestlancs", name="Mersey & West Lancashire Teaching Hospitals NHS Trust", group="nhs",
         terms=['"Mersey and West Lancashire Teaching Hospitals"', '"Southport and Ormskirk"'],
         match=["mersey and west lancashire", "southport and ormskirk"]),
    dict(id="nhs_lsc_icb", name="NHS Lancashire & South Cumbria ICB", group="nhs",
         terms=['"NHS Lancashire and South Cumbria Integrated Care Board"'],
         match=["lancashire and south cumbria integrated care board"]),
    # --- National agencies with a Lancashire estate (national CF -> filtered) --
    dict(id="national_highways", name="National Highways", group="gov", national=True,
         terms=['"National Highways" Lancashire', '"Highways England" Lancashire'],
         match=["national highways", "highways england"]),
    dict(id="environment_agency", name="Environment Agency", group="gov", national=True,
         terms=['"Environment Agency" Lancashire'], match=["environment agency"]),
    dict(id="homes_england", name="Homes England", group="gov", national=True,
         terms=['"Homes England" Lancashire'], match=["homes england"]),
    # --- Education (colleges / universities; sparse on CF) -------------------
    dict(id="uclan", name="University of Central Lancashire", group="education",
         terms=['"University of Central Lancashire"'], match=["university of central lancashire"]),
    dict(id="lancaster_uni", name="Lancaster University", group="education",
         terms=['"Lancaster University"'], match=["lancaster university"]),
    dict(id="blackburn_college", name="Blackburn College", group="education", national=True,
         terms=['"Blackburn College"'], match=["blackburn college"]),
    dict(id="burnley_college", name="Burnley College", group="education", national=True,
         terms=['"Burnley College"'], match=["burnley college"]),
    dict(id="preston_college", name="Preston College", group="education", national=True,
         terms=['"Preston College"', '"Preston\'s College"'], match=["preston college", "preston's college"]),
]

def fetch_body(b):
    notices = []
    for t in b["terms"]:
        notices += search_contracts(t, published_from=PUBLISHED_FROM)
    parsed = [parse_notice(n) for n in notices]
    mine = [p for p in parsed if any(m in (p["organisation"] or "").lower() for m in b["match"])]
    # de-dup by notice id
    seen, uniq = set(), []
    for p in mine:
        if p["id"] and p["id"] not in seen:
            seen.add(p["id"]); uniq.append(p)
    awarded = [p for p in uniq if p["status"] == "awarded"]
    if b.get("national"):
        awarded = [p for p in awarded if lancs_relevant(p)]
    return awarded

def to_row(p, b):
    sup = p.get("awarded_supplier")
    if sup:
        import html
        parts = [x.strip() for x in html.unescape(sup).split(",") if x.strip()]
        sup = ", ".join(dict.fromkeys(parts))[:120]
    val = p.get("awarded_value") or p.get("value_high") or None
    if val is not None and val <= 0:
        val = None
    return {
        "buyer": b["name"], "grp": b["group"], "body_id": b["id"],
        "title": (p.get("title") or "")[:180],
        "desc": (p.get("description") or "")[:300],
        "value": val,
        "supplier": sup or None,
        "date": p.get("awarded_date"),
        "months": p.get("contract_months"),
        "maxmo": p.get("max_term_months"),
        "end": p.get("contract_end"),
        "ext": bool(p.get("has_extension_option")),
        "framework": bool(p.get("framework")),
        "sme": p.get("awarded_to_sme"),
        "cat": (p.get("cpv_description") or "")[:60],
        "url": p.get("url"),
    }

def main():
    only = set(sys.argv[1:])  # optional: restrict to group names or body ids
    rows = []
    per_body = {}
    for b in BODIES:
        if only and b["group"] not in only and b["id"] not in only:
            continue
        print(f"[fetch] {b['name']} ({b['group']})...", flush=True)
        awarded = fetch_body(b)
        for p in awarded:
            rows.append(to_row(p, b))
        per_body[b["id"]] = len(awarded)
        print(f"        {len(awarded)} awarded", flush=True)

    payload = {
        "generated_from": "Contracts Finder search_notices API (via AI DOGE procurement_etl)",
        "published_from": PUBLISHED_FROM,
        "counts_by_group": dict(Counter(r["grp"] for r in rows)),
        "counts_by_body": per_body,
        "contracts": rows,
    }
    # Merge with any existing file so partial runs (by group) accumulate.
    if only and os.path.exists(OUT):
        prev = json.load(open(OUT))
        kept = [r for r in prev.get("contracts", []) if r.get("grp") not in only and r.get("body_id") not in only]
        rows = kept + rows
        payload["contracts"] = rows
        payload["counts_by_group"] = dict(Counter(r["grp"] for r in rows))
        merged_bodies = {k: v for k, v in prev.get("counts_by_body", {}).items()}
        merged_bodies.update(per_body)
        payload["counts_by_body"] = merged_bodies
    json.dump(payload, open(OUT, "w"), separators=(",", ":"))
    print(f"\nwrote {OUT}: {len(rows)} contracts")
    print("by group:", payload["counts_by_group"])

if __name__ == "__main__":
    main()
