#!/usr/bin/env python3
"""Geocode classified CCOD public titles and emit ccod_features.json.
- postcoded titles (all owners except fire/ambulance): precise via postcodes.io bulk.
- no-postcode titles (councils only: county/district/parish): locality-level via
  postcodes.io /places, with deterministic jitter, marked approximate.
CCOD fire/ambulance dropped (curated station layers cover them). No-pc gov dropped
(linear land, a locality pin would mislead)."""
import csv, json, re, time, hashlib, math, urllib.request, urllib.parse

BBOX = (53.35, 54.35, -3.30, -1.95)  # lat_min, lat_max, lng_min, lng_max (Lancashire+margin)
UNITARY = {
    "LANCASTER":"N","PRESTON":"N","RIBBLE VALLEY":"N",
    "BLACKBURN WITH DARWEN":"E","BURNLEY":"E","HYNDBURN":"E","PENDLE":"E","ROSSENDALE":"E",
    "CHORLEY":"S","SOUTH RIBBLE":"S","WEST LANCASHIRE":"S",
    "BLACKPOOL":"W","FYLDE":"W","WYRE":"W",
}
def http_json(url, data=None):
    req = urllib.request.Request(url, data=(json.dumps(data).encode() if data else None),
                                 headers={"Content-Type":"application/json"})
    with urllib.request.urlopen(req, timeout=30) as r:
        return json.load(r)

rows = list(csv.DictReader(open("ccod_public.csv")))
import os
def load_cache(fn):
    return json.load(open(fn)) if os.path.exists(fn) else {}
def save_cache(fn, d):
    json.dump(d, open(fn,"w"))

# owner routing
KEEP_PC = {"county","district","parish","nhs","police","gov"}   # precise tier (drop fire/amb)
KEEP_LOC = {"county","district","parish"}                        # locality tier
JUNK_LOC = re.compile(r"^(at |and |adjoining|motorway|m\d\d?\b)", re.I)

# ---- 1. bulk-geocode postcodes (cached) ----
pc_ll = {k: tuple(v) for k, v in load_cache("postcode_cache.json").items()}
pcs = sorted({r["postcode"].strip().upper() for r in rows
              if r["postcode"].strip() and r["owner"] in KEEP_PC and r["postcode"].strip().upper() not in pc_ll})
for i in range(0, len(pcs), 100):
    batch = pcs[i:i+100]
    res = http_json("https://api.postcodes.io/postcodes", {"postcodes": batch})
    for item in res["result"]:
        rr = item["result"]
        if rr and rr.get("latitude"):
            pc_ll[item["query"].upper()] = (rr["latitude"], rr["longitude"])
    time.sleep(0.15)
save_cache("postcode_cache.json", {k: list(v) for k, v in pc_ll.items()})
print("postcodes in cache:", len(pc_ll))

# ---- 2. geocode unique localities (councils, no postcode) ----
def locality(addr):
    parts = [p.strip() for p in addr.split(",") if p.strip()]
    if not parts: return None
    t = re.sub(r"\(.*?\)","",parts[-1]).strip()
    t = re.sub(r"^(land|plot|site|the)\b","",t,flags=re.I).strip()
    return t or (parts[-2].strip() if len(parts)>1 else None)

locs = {}
for r in rows:
    if r["owner"] in KEEP_LOC and not r["postcode"].strip():
        l = locality(r["address"])
        if l and not JUNK_LOC.match(l): locs[l.upper()] = l

loc_ll = {k: tuple(v) for k, v in load_cache("locality_cache.json").items()}
def nominatim(q):
    url = ("https://nominatim.openstreetmap.org/search?q=" + urllib.parse.quote(q + ", Lancashire, UK")
           + "&format=json&limit=1&countrycodes=gb&viewbox=-3.30,54.35,-1.95,53.35&bounded=1")
    req = urllib.request.Request(url, headers={"User-Agent": "tompickup.co.uk LGR property map (tom.pickup@lancashire.gov.uk)"})
    with urllib.request.urlopen(req, timeout=30) as r:
        a = json.load(r)
    return (float(a[0]["lat"]), float(a[0]["lon"])) if a else None

for key, disp in locs.items():
    if key in loc_ll: continue
    ll = None
    try:  # postcodes.io /places first (fast)
        arr = (http_json("https://api.postcodes.io/places?q=" + urllib.parse.quote(disp) + "&limit=1").get("result") or [])
        if arr: ll = (arr[0]["latitude"], arr[0]["longitude"])
    except Exception: pass
    if ll and not (BBOX[0] <= ll[0] <= BBOX[1] and BBOX[2] <= ll[1] <= BBOX[3]): ll = None
    if ll is None:  # Nominatim fallback (bounded to Lancashire)
        try:
            ll = nominatim(disp); time.sleep(1.05)
        except Exception: time.sleep(1.05)
    if ll and BBOX[0] <= ll[0] <= BBOX[1] and BBOX[2] <= ll[1] <= BBOX[3]:
        loc_ll[key] = (ll[0], ll[1])
save_cache("locality_cache.json", {k: list(v) for k, v in loc_ll.items()})
print("localities in cache:", len(loc_ll), "/", len(locs))

# ---- 3. emit features ----
def jitter(title, lat, lng):
    h = int(hashlib.md5(title.encode()).hexdigest(), 16)
    ang = (h % 3600) / 3600 * 2 * math.pi
    rad = ((h >> 12) % 1000) / 1000  # 0..1
    R = 0.011 * math.sqrt(rad)       # up to ~1.2km, denser near centre
    return round(lat + R * math.cos(ang), 6), round(lng + R * math.sin(ang) * 1.5, 6)

def jitter_small(title, lat, lng):
    # ~40-90m so multiple parcels on the same street do not perfectly overlap
    h = int(hashlib.md5(title.encode()).hexdigest(), 16)
    ang = (h % 3600) / 3600 * 2 * math.pi
    R = 0.0008 * (((h >> 12) % 1000) / 1000)
    return round(lat + R * math.cos(ang), 6), round(lng + R * math.sin(ang) * 1.5, 6)

# street cache (#1): (street|town lower) -> [lat,lng]
STREET_RE = re.compile(r"([A-Z][A-Za-z'&.\-]+(?:\s+[A-Z][A-Za-z'&.\-]+){0,3}\s+"
    r"(?:Street|Road|Lane|Avenue|Drive|Close|Way|Grove|Place|Terrace|Court|Crescent|Walk|"
    r"Gardens|Square|Row|Hill|Brow|Fold|Green|Gate|Bank|Rise|View|Mount|Parade|Fields?|Meadow|"
    r"Croft|Wood|Moor|Head|Side|Bridge|Nook|Vale|Park|Barn|Field))\b")
street_ll = {}
if os.path.exists(os.path.join(SCRATCH, "street_cache.json")):
    for k, v in json.load(open(os.path.join(SCRATCH, "street_cache.json"))).items():
        if v: street_ll[k] = (v[0], v[1])
print("street combos located:", len(street_ll))

def is_land(addr):
    a = addr.lower().strip()
    return 1 if a.startswith(("land","plot","site","garden","amenity","open space","car park","parking")) else 0

feats = []
seen = set()
for r in rows:
    o = r["owner"]
    if r["title"] in seen: continue
    dist = r["district"].title().replace("With","with")
    if dist == "Blackburn with Darwen": pass
    u = UNITARY.get(r["district"].upper())
    if not u: continue
    ten = r["tenure"]
    t = "F" if ten.lower().startswith("free") else ("L" if ten.lower().startswith("lease") else "")
    tier = None; lat = lng = None
    pc = r["postcode"].strip().upper()
    if pc and o in KEEP_PC and pc in pc_ll:
        lat, lng = pc_ll[pc]; tier = "pc"
    elif not pc and o in KEEP_LOC:
        l = locality(r["address"])
        m = STREET_RE.search(r["address"])
        skey = (m.group(1).strip() + "|" + l.strip()).lower() if (m and l) else None
        if skey and skey in street_ll:                       # #1 street-level (precise)
            lat, lng = jitter_small(r["title"], *street_ll[skey]); tier = "st"
        elif l and l.upper() in loc_ll:                      # locality (approximate)
            lat, lng = jitter(r["title"], *loc_ll[l.upper()]); tier = "loc"
    if tier is None: continue
    seen.add(r["title"])
    feats.append({
        "lat": round(lat,6), "lng": round(lng,6),
        "n": r["address"][:120] or "(land parcel)",
        "o": o, "b": r["body"], "d": dist, "u": u,
        "c": o, "t": t, "land": is_land(r["address"]),
        "m": r["title"], "pr": tier,
    })

json.dump(feats, open("ccod_features.json","w"), separators=(",",":"))
from collections import Counter
print("features emitted:", len(feats))
print("by owner:", dict(Counter(f["o"] for f in feats)))
print("by tier:", dict(Counter(f["pr"] for f in feats)))
