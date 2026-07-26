#!/usr/bin/env python3
"""Harvest supplier company numbers from Contracts Finder OCDS releases.

Reads every notice_id in the 17 bodies' procurement_finder.json files,
fetches /Published/Notice/OCDS/{id}, and collects supplier parties whose
identifier scheme is GB-COH. Output: name -> company number evidence map used
by build_pound.py as a high-confidence resolution source.

Resumable: per-notice results cached in raw/ocds_cache.jsonl.
"""
import json, time, sys
from pathlib import Path
import requests

DATA = Path.home() / "clawd/burnley-council/data"
RAW = Path.home() / "observatory-data/raw"
PROC = Path.home() / "observatory-data/processed"
CACHE = RAW / "ocds_cache.jsonl"
UA = {"User-Agent": "Mozilla/5.0 (observatory data fetch; tompickup.co.uk)"}
BODIES = ["blackburn", "blackpool", "burnley", "chorley", "fylde", "hyndburn",
          "lancashire_cc", "lancashire_fire", "lancashire_pcc", "lancaster",
          "pendle", "preston", "ribble_valley", "rossendale", "south_ribble",
          "west_lancashire", "wyre"]

notice_ids = []
seen = set()
for b in BODIES:
    f = DATA / b / "procurement_finder.json"
    if not f.exists():
        continue
    for n in json.loads(f.read_text()).get("notices", []):
        nid = n.get("notice_id")
        if nid and nid not in seen:
            seen.add(nid)
            notice_ids.append(nid)

done = set()
if CACHE.exists():
    for line in CACHE.open():
        try:
            done.add(json.loads(line)["notice_id"])
        except Exception:
            pass
todo = [n for n in notice_ids if n not in done]
print(f"{len(notice_ids)} notices, {len(done)} cached, {len(todo)} to fetch")

with CACHE.open("a") as out:
    for i, nid in enumerate(todo):
        try:
            for attempt in range(4):
                r = requests.get(
                    f"https://www.contractsfinder.service.gov.uk/Published/Notice/OCDS/{nid}",
                    headers=UA, timeout=30)
                if r.status_code != 429:
                    break
                time.sleep(max(30, int(r.headers.get("Retry-After") or 30)))
            if r.status_code == 429:
                continue          # leave uncached; a future run retries
            sups = []
            if r.status_code == 200:
                d = r.json()
                rels = d.get("releases") or [d]
                for rel in rels:
                    for p in rel.get("parties", []):
                        if "supplier" not in (p.get("roles") or []):
                            continue
                        ident = p.get("identifier") or {}
                        sups.append({"name": p.get("name"),
                                     "scheme": ident.get("scheme"),
                                     "id": ident.get("id")})
            out.write(json.dumps({"notice_id": nid, "status": r.status_code,
                                  "suppliers": sups}) + "\n")
            out.flush()
        except Exception as e:
            out.write(json.dumps({"notice_id": nid, "status": "err",
                                  "error": str(e)[:100], "suppliers": []}) + "\n")
        time.sleep(1.5)
        if (i + 1) % 200 == 0:
            print(f"  {i+1}/{len(todo)}")

# aggregate
sys.path.insert(0, str(Path(__file__).parent))
from resolve_suppliers import normalise
name_ids = {}
for line in CACHE.open():
    rec = json.loads(line)
    for s in rec["suppliers"]:
        if (s.get("scheme") or "").upper().replace("-", "") == "GBCOH" and s.get("id"):
            crn = s["id"].strip().upper().replace(" ", "")
            if 6 <= len(crn) <= 8:
                key = normalise(s.get("name") or "")
                if key:
                    name_ids.setdefault(key, {}).setdefault(crn.zfill(8) if crn.isdigit() else crn, 0)
                    name_ids[key][crn.zfill(8) if crn.isdigit() else crn] += 1
out_map = {}
for key, crns in name_ids.items():
    best = max(crns.items(), key=lambda kv: kv[1])
    if best[1] * 2 >= sum(crns.values()):     # majority evidence
        out_map[key] = {"crn": best[0], "notices": best[1]}
(PROC / "ocds_supplier_ids.json").write_text(json.dumps(
    {"$meta": {"source": "Contracts Finder OCDS releases, GB-COH supplier identifiers",
               "notices_checked": len(done) + len(todo)},
     "byName": out_map}))
print(f"ocds_supplier_ids.json: {len(out_map)} name->CRN mappings")
