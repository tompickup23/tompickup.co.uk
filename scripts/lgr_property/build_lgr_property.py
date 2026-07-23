#!/usr/bin/env python3
"""Build the unified Lancashire-14 public property dataset for the LGR map (v2).

Layers:
  - CCOD public estate (ccod_features.json): county, district, parish councils
    (transfer to new unitaries), plus NHS, police, government/agencies (do not).
    Precise (postcode) or locality-approximate tier per feature ('pr').
  - Schools (education) from DfE GIAS.
  - Fire + ambulance stations (curated station lists).

Feature schema: lat,lng,n(ame),o(wner),b(ody),d(istrict),u(nitary N/E/S/W),
c(ategory),t(enure F/L/''),land(0/1),m(eta),pr(ecision pc/loc).
"""
import csv, json, os
from collections import Counter
from pyproj import Transformer

SCRATCH = os.path.dirname(os.path.abspath(__file__))
DATA = "/Users/tompickup/clawd/burnley-council/data"
OUT = os.path.join(SCRATCH, "lgr-public-property.json")

UNITARY = {
    "Lancaster":"N","Preston":"N","Ribble Valley":"N",
    "Blackburn with Darwen":"E","Burnley":"E","Hyndburn":"E","Pendle":"E","Rossendale":"E",
    "Chorley":"S","South Ribble":"S","West Lancashire":"S",
    "Blackpool":"W","Fylde":"W","Wyre":"W",
}
UNITARY_NAMES = {"N":"North Lancashire","E":"East Lancashire",
                 "S":"South Lancashire","W":"West Lancashire / Fylde Coast"}
UPPER = {d: "Lancashire County Council" for d in UNITARY}
UPPER["Blackburn with Darwen"] = "Blackburn with Darwen (unitary)"
UPPER["Blackpool"] = "Blackpool (unitary)"

DISTRICT_CANON = {
    "blackburn with darwen":"Blackburn with Darwen","blackburn":"Blackburn with Darwen",
    "blackpool":"Blackpool","west lancashire":"West Lancashire","west lancs":"West Lancashire",
    "ribble valley":"Ribble Valley","south ribble":"South Ribble",
}
def canon_district(s):
    if not s: return None
    k = s.strip().lower()
    if k in DISTRICT_CANON: return DISTRICT_CANON[k]
    t = s.strip().title()
    return t if t in UNITARY else None

features = []
counts = Counter()

# --- Layer 1: CCOD public estate (councils + NHS + police + gov) ------------
def add_ccod():
    p = os.path.join(SCRATCH, "ccod_features.json")
    for f in json.load(open(p)):
        d = canon_district(f["d"])
        if not d: continue
        f["d"] = d
        features.append(f)
        counts[f["o"]] += 1

# --- Layer 2: education (GIAS) ---------------------------------------------
TR = Transformer.from_crs("EPSG:27700", "EPSG:4326", always_xy=True)
LANC_LAS = {"Lancashire","Blackburn with Darwen","Blackpool"}
KEEP_GROUP = {"Academies","Local authority maintained schools","Free Schools",
              "Special schools","Colleges","Universities"}
def add_schools():
    p = os.path.join(SCRATCH, "gias_all.csv")
    r = csv.DictReader(open(p, encoding="latin-1"))
    for row in r:
        if row["LA (name)"] not in LANC_LAS: continue
        if row["EstablishmentStatus (name)"] != "Open": continue
        if row["EstablishmentTypeGroup (name)"] not in KEEP_GROUP: continue
        try: e, n = float(row["Easting"]), float(row["Northing"])
        except (ValueError, TypeError): continue
        if not e or not n: continue
        lng, lat = TR.transform(e, n)
        la = row["LA (name)"]
        d = la if la in ("Blackburn with Darwen","Blackpool") else canon_district(row["DistrictAdministrative (name)"])
        if not d: continue
        typ = row["TypeOfEstablishment (name)"]
        maintained = row["EstablishmentTypeGroup (name)"] == "Local authority maintained schools"
        trust = row.get("Trusts (name)","").strip()
        features.append({
            "lat": round(lat,6), "lng": round(lng,6), "n": row["EstablishmentName"],
            "o": "education", "b": trust if trust else ("LA maintained" if maintained else row["EstablishmentTypeGroup (name)"]),
            "d": d, "u": UNITARY[d], "c": "education", "t": "", "land": 0,
            "m": typ + (" · LA-maintained" if maintained else " · academy/other"), "pr": "pc",
        })
        counts["education"] += 1

# --- Layer 3/4: fire + ambulance stations (curated) ------------------------
def add_emergency():
    for fn, otype, body in [("fire.json","fire","Lancashire Fire and Rescue Service"),
                            ("ambulance.json","ambulance","North West Ambulance Service")]:
        fp = os.path.join(SCRATCH, fn)
        if not os.path.exists(fp): continue
        for s in json.load(open(fp)):
            lat, lng = s.get("lat"), s.get("lng")
            if not (lat and lng): continue
            d = canon_district(s.get("district"))
            if not d: continue
            features.append({
                "lat": round(lat,6), "lng": round(lng,6), "n": s.get("name"),
                "o": otype, "b": body, "d": d, "u": UNITARY[d], "c": otype,
                "t": "", "land": 0, "m": s.get("address",""), "pr": "pc",
            })
            counts[otype] += 1

add_ccod()
add_schools()
add_emergency()

# --- #4: per-unitary inheritance summary (from the full classified council set,
#         so totals are honest even for parcels the map could not geocode) -----
def is_land_addr(a):
    a = (a or "").lower().strip()
    return a.startswith(("land","plot","site","garden","amenity","open space","car park","parking"))
def build_inheritance():
    p = os.path.join(SCRATCH, "ccod_public.csv")
    if not os.path.exists(p): return None
    inh = {u: {"name": UNITARY_NAMES[u], "titles": 0, "county": 0, "district": 0,
               "land": 0, "buildings": 0, "councils": Counter()} for u in "NESW"}
    for row in csv.DictReader(open(p)):
        if row["owner"] not in ("county", "district"): continue
        d = canon_district(row["district"])
        if not d: continue
        u = UNITARY[d]
        b = inh[u]
        b["titles"] += 1
        b[row["owner"]] += 1
        if is_land_addr(row["address"]): b["land"] += 1
        else: b["buildings"] += 1
        b["councils"][row["body"]] += 1
    out = {}
    for u, b in inh.items():
        out[u] = {"name": b["name"], "titles": b["titles"], "county": b["county"],
                  "district": b["district"], "land": b["land"], "buildings": b["buildings"],
                  "councils": [{"n": n, "c": c} for n, c in b["councils"].most_common()]}
    return out
inheritance = build_inheritance()

meta = {
    "generated": "2026-07-23",
    "title": "Publicly owned property and land across the Lancashire-14",
    "unitaries": UNITARY_NAMES,
    "unitary_of_district": UNITARY,
    "upper_of_district": UPPER,
    "counts_by_owner": dict(counts),
    "total": len(features),
    "precise": sum(1 for f in features if f.get("pr") == "pc"),
    "approx": sum(1 for f in features if f.get("pr") == "loc"),
    "transfers_note": "Only county and district (borough/city) council property passes to the new unitary councils. Town and parish councils continue, and the NHS, police, fire, ambulance and national agencies keep their own estates.",
    "sources": {
        "councils": "HM Land Registry Commercial & Corporate Ownership Data (CCOD), July 2026",
        "nhs_police_gov": "HM Land Registry CCOD, July 2026",
        "education": "DfE Get Information About Schools (GIAS), open state-funded establishments",
        "fire": "Lancashire Fire and Rescue Service station list",
        "ambulance": "North West Ambulance Service",
    },
    "coverage_note": "Council, NHS, police and government titles come from HM Land Registry (CCOD). Addressed sites are placed precisely by postcode; council land parcels without a postcode are placed by street where possible, otherwise approximately by locality and shown faded. Schools, fire and ambulance stations are placed precisely.",
    "inheritance": inheritance,
}
json.dump({"meta": meta, "features": features}, open(OUT, "w"), separators=(",", ":"))
print("total features:", len(features))
print("by owner:", dict(counts))
print("by tier:", dict(Counter(f.get("pr") for f in features)))
print("by unitary:", dict(Counter(f["u"] for f in features)))
print("bytes:", os.path.getsize(OUT))
