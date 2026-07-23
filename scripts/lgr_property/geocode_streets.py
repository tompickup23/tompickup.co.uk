#!/usr/bin/env python3
"""#1 Street-level geocoding for the ~13.6k no-postcode council parcels.
Extract (street, town), geocode via Nominatim bounded to the town centroid,
validate within 3km of it. Cache -> street_cache.json. Run standalone (long)."""
import csv, re, json, os, time, math, urllib.request, urllib.parse

ROADWORDS = r"(?:Street|Road|Lane|Avenue|Drive|Close|Way|Grove|Place|Terrace|Court|Crescent|Walk|Gardens|Square|Row|Hill|Brow|Fold|Green|Gate|Bank|Rise|View|Mount|Parade|Fields?|Meadow|Croft|Wood|Moor|Head|Side|Bridge|Nook|Vale|Park|Barn|Field)"
street_re = re.compile(r"([A-Z][A-Za-z'&.\-]+(?:\s+[A-Z][A-Za-z'&.\-]+){0,3}\s+" + ROADWORDS + r")\b")
BBOX = (53.35, 54.35, -3.30, -1.95)

def locality(addr):
    parts = [p.strip() for p in addr.split(",") if p.strip()]
    if not parts: return None
    t = re.sub(r"\(.*?\)","",parts[-1]).strip()
    return re.sub(r"^(land|plot|site|the)\b","",t,flags=re.I).strip() or (parts[-2].strip() if len(parts)>1 else None)

def haversine(a, b):
    R=6371; dlat=math.radians(b[0]-a[0]); dlng=math.radians(b[1]-a[1])
    x=math.sin(dlat/2)**2+math.cos(math.radians(a[0]))*math.cos(math.radians(b[0]))*math.sin(dlng/2)**2
    return 2*R*math.asin(math.sqrt(x))

loc_ll = {k: tuple(v) for k, v in json.load(open("locality_cache.json")).items()}
cache = {}
if os.path.exists("street_cache.json"):
    cache = json.load(open("street_cache.json"))

rows = csv.DictReader(open("ccod_public.csv"))
combos = {}   # "street|town" -> town
for r in rows:
    if r["owner"] in ("county","district","parish") and not r["postcode"].strip():
        m = street_re.search(r["address"]); loc = locality(r["address"])
        if m and loc:
            combos[(m.group(1).strip() + "|" + loc.strip()).lower()] = loc.strip()

def nominatim(street, town):
    town_ll = loc_ll.get(town.upper())
    if town_ll:
        vb = f"{town_ll[1]-0.06},{town_ll[0]+0.05},{town_ll[1]+0.06},{town_ll[0]-0.05}"
    else:
        vb = "-3.30,54.35,-1.95,53.35"
    url = ("https://nominatim.openstreetmap.org/search?q=" +
           urllib.parse.quote(f"{street}, {town}, Lancashire, UK") +
           f"&format=json&limit=1&countrycodes=gb&viewbox={vb}&bounded=1")
    req = urllib.request.Request(url, headers={"User-Agent": "tompickup.co.uk LGR property map (tom.pickup@lancashire.gov.uk)"})
    with urllib.request.urlopen(req, timeout=30) as resp:
        a = json.load(resp)
    if not a: return None
    ll = (float(a[0]["lat"]), float(a[0]["lon"]))
    if not (BBOX[0] <= ll[0] <= BBOX[1] and BBOX[2] <= ll[1] <= BBOX[3]): return None
    if town_ll and haversine(ll, town_ll) > 3.0: return None   # wrong same-named street
    return ll

todo = [k for k in combos if k not in cache]
print(f"combos: {len(combos)} | cached: {len(cache)} | to do: {len(todo)}", flush=True)
for i, key in enumerate(todo):
    street, town = key.split("|", 1)
    try:
        ll = nominatim(combos[key].split("|")[0] if False else street, combos[key])
        cache[key] = list(ll) if ll else None
    except Exception:
        cache[key] = None
    time.sleep(1.1)
    if i % 100 == 0:
        json.dump(cache, open("street_cache.json","w"))
        got = sum(1 for v in cache.values() if v)
        print(f"  {i}/{len(todo)} done, {got} located", flush=True)
json.dump(cache, open("street_cache.json","w"))
got = sum(1 for v in cache.values() if v)
print(f"DONE. {got}/{len(cache)} street combos located", flush=True)
