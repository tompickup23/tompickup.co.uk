#!/usr/bin/env python3
"""Build the unified Lancashire-14 public property dataset for the LGR map.

Layers (v1 clean layers): county (LCC estate), education (GIAS state schools),
fire + ambulance (from research agent JSON). District-council (CCOD), NHS and
council-housing-stock layers are appended in later passes.

Output: lgr-public-property.json  -> { meta, features: [...] }
Compact feature schema keeps the payload small:
  lat, lng, n(ame), o(wner type code), b(ody), d(istrict), u(nitary code),
  c(ategory), t(enure F/L/''), land(0/1), meta1 (freeform e.g. school type)
"""
import csv, json, os, sys
from collections import Counter
from pyproj import Transformer

SCRATCH = os.path.dirname(os.path.abspath(__file__))
DATA = "/Users/tompickup/clawd/burnley-council/data"
OUT = os.path.join(SCRATCH, "lgr-public-property.json")

# --- future 4-unitary model (confirmed 16 Jul 2026) -------------------------
UNITARY = {
    # North
    "Lancaster": "N", "Preston": "N", "Ribble Valley": "N",
    # East
    "Blackburn with Darwen": "E", "Burnley": "E", "Hyndburn": "E",
    "Pendle": "E", "Rossendale": "E",
    # South
    "Chorley": "S", "South Ribble": "S", "West Lancashire": "S",
    # West / Fylde Coast
    "Blackpool": "W", "Fylde": "W", "Wyre": "W",
}
UNITARY_NAMES = {"N": "North Lancashire", "E": "East Lancashire",
                 "S": "South Lancashire", "W": "West Lancashire / Fylde Coast"}
# upper tier: the 12 districts sit under LCC; the 2 unitaries are their own
UPPER = {d: "Lancashire County Council" for d in UNITARY}
UPPER["Blackburn with Darwen"] = "Blackburn with Darwen (unitary)"
UPPER["Blackpool"] = "Blackpool (unitary)"

DISTRICT_CANON = {
    # normalise variants coming from different sources
    "blackburn with darwen": "Blackburn with Darwen",
    "blackburn": "Blackburn with Darwen",
    "blackpool": "Blackpool",
    "west lancashire": "West Lancashire", "west lancs": "West Lancashire",
    "ribble valley": "Ribble Valley", "south ribble": "South Ribble",
}
def canon_district(s):
    if not s: return None
    k = s.strip().lower()
    if k in DISTRICT_CANON: return DISTRICT_CANON[k]
    t = s.strip().title()
    return t if t in UNITARY else None

features = []
counts = Counter()

# --- Layer 1: LCC estate ----------------------------------------------------
def add_lcc():
    p = os.path.join(DATA, "lancashire_cc", "property_assets.json")
    d = json.load(open(p))
    for a in d["assets"]:
        lat, lng = a.get("lat"), a.get("lng")
        if not (lat and lng): continue
        dist = canon_district(a.get("district"))
        if not dist: continue  # drop the ~296 untagged / out-of-county
        ten = a.get("ownership", "")
        t = "F" if "free" in ten.lower() else ("L" if "lease" in ten.lower() else "")
        features.append({
            "lat": round(lat, 6), "lng": round(lng, 6),
            "n": a.get("name") or a.get("address") or "LCC asset",
            "o": "county", "b": "Lancashire County Council",
            "d": dist, "u": UNITARY[dist],
            "c": a.get("category", "other"),
            "t": t, "land": 1 if a.get("land_only") else 0,
            "m": a.get("category", "").replace("_", " "),
        })
        counts["county"] += 1

# --- Layer 2: education (GIAS) ---------------------------------------------
TR = Transformer.from_crs("EPSG:27700", "EPSG:4326", always_xy=True)
LANC_LAS = {"Lancashire", "Blackburn with Darwen", "Blackpool"}
# state-funded groups we keep; drop independent + not-applicable/childrens-centre-noncoord
KEEP_GROUP = {"Academies", "Local authority maintained schools", "Free Schools",
              "Special schools", "Colleges", "Universities"}
def add_schools():
    p = os.path.join(SCRATCH, "gias_all.csv")
    r = csv.DictReader(open(p, encoding="latin-1"))
    for row in r:
        if row["LA (name)"] not in LANC_LAS: continue
        if row["EstablishmentStatus (name)"] != "Open": continue
        grp = row["EstablishmentTypeGroup (name)"]
        if grp not in KEEP_GROUP: continue
        try:
            e, n = float(row["Easting"]), float(row["Northing"])
        except (ValueError, TypeError):
            continue
        if not e or not n: continue
        lng, lat = TR.transform(e, n)
        la = row["LA (name)"]
        if la in ("Blackburn with Darwen", "Blackpool"):
            dist = la
        else:
            dist = canon_district(row["DistrictAdministrative (name)"])
        if not dist: continue
        typ = row["TypeOfEstablishment (name)"]
        maintained = grp == "Local authority maintained schools"
        trust = row.get("Trusts (name)", "").strip()
        features.append({
            "lat": round(lat, 6), "lng": round(lng, 6),
            "n": row["EstablishmentName"],
            "o": "education", "b": trust if trust else ("LA maintained" if maintained else grp),
            "d": dist, "u": UNITARY[dist],
            "c": "education",
            "t": "", "land": 0,
            "m": typ + (" · LA-maintained" if maintained else " · academy/other"),
        })
        counts["education"] += 1

# --- Layer 3/4: fire + ambulance (from agent JSON, optional) ---------------
def add_emergency():
    for fn, otype, body in [("fire.json", "fire", "Lancashire Fire and Rescue Service"),
                            ("ambulance.json", "ambulance", "North West Ambulance Service")]:
        fp = os.path.join(SCRATCH, fn)
        if not os.path.exists(fp): continue
        for s in json.load(open(fp)):
            lat, lng = s.get("lat"), s.get("lng")
            if not (lat and lng): continue
            dist = canon_district(s.get("district"))
            if not dist: continue
            features.append({
                "lat": round(lat, 6), "lng": round(lng, 6),
                "n": s.get("name"), "o": otype, "b": body,
                "d": dist, "u": UNITARY[dist], "c": otype,
                "t": "", "land": 0, "m": s.get("address", ""),
            })
            counts[otype] += 1

add_lcc()
add_schools()
add_emergency()

meta = {
    "generated": "2026-07-23",
    "title": "Publicly owned property and land across the Lancashire-14",
    "unitaries": UNITARY_NAMES,
    "unitary_of_district": UNITARY,
    "upper_of_district": UPPER,
    "counts_by_owner": dict(counts),
    "total": len(features),
    "sources": {
        "county": "Lancashire County Council Local Authority Land List (AI DOGE property_assets)",
        "education": "Get Information About Schools (GIAS), DfE, open state-funded establishments",
        "fire": "Lancashire Fire and Rescue Service station list",
        "ambulance": "North West Ambulance Service",
    },
    "coverage_note": "First release. Covers the county council estate, state-funded schools, and fire and ambulance stations. District-council titles (HM Land Registry), the wider NHS estate and retained council housing are being added in later releases.",
}
json.dump({"meta": meta, "features": features}, open(OUT, "w"), separators=(",", ":"))
print("total features:", len(features))
print("by owner:", dict(counts))
print("by unitary:", dict(Counter(f["u"] for f in features)))
print("by district:", dict(Counter(f["d"] for f in features)))
print("bytes:", os.path.getsize(OUT))
PY = None
