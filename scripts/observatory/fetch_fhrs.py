#!/usr/bin/env python3
"""fetch_fhrs.py - FSA Food Hygiene (FHRS) per-LA open-data XML for the 14
Lancashire local authorities. Trading-premises register: keep ALL establishments.

Discovers each authority's FHRS id via the Authorities API, matches to the 14
Lancs LADs by LocalAuthorityIdCode / name, then downloads each per-LA XML.
Output: ~/observatory-data/processed/fhrs_lancs.json
"""
import sys
from pathlib import Path
from lxml import etree

sys.path.insert(0, str(Path(__file__).parent))
from _common import (RAW, LANCS_14, download, get_json, meta, write_out,
                     clean_text, log)

AUTH_API = "https://api.ratings.food.gov.uk/Authorities/basic"
XML_PATTERN = "https://ratings.food.gov.uk/api/open-data-files/FHRS{id}en-GB.xml"
API_HEADERS = {"x-api-version": "2", "accept": "application/json"}

# Exact FSA authority Name -> Lancs LAD. The FHRS file id is LocalAuthorityIdCode.
LANCS_NAMES = {
    "lancaster city": "E07000121",
    "preston": "E07000123",
    "ribble valley": "E07000124",
    "fylde": "E07000119",
    "wyre": "E07000128",
    "blackpool": "E06000009",
    "blackburn": "E06000008",
    "hyndburn": "E07000120",
    "rossendale": "E07000125",
    "pendle": "E07000122",
    "burnley": "E07000117",
    "chorley": "E07000118",
    "south ribble": "E07000126",
    "west lancashire": "E07000127",
}


def discover_ids():
    data = get_json(AUTH_API, headers=API_HEADERS)
    auths = data.get("authorities", [])
    log(f"FHRS API returned {len(auths)} authorities")
    found = {}  # lad -> {id, fhrs_name}
    for a in auths:
        name = (a.get("Name") or "").strip().lower()
        key = name.replace("city of ", "").replace(" council", "").strip()
        lad = LANCS_NAMES.get(key)
        if lad and lad not in found:
            # FHRS open-data file id is LocalAuthorityIdCode (e.g. Burnley = 196)
            found[lad] = {"id": a.get("LocalAuthorityIdCode"),
                          "fhrs_name": a.get("Name")}
    return found


def parse_xml(path, la_name, lad):
    rows = []
    ctx = etree.iterparse(str(path), tag="EstablishmentDetail", recover=True)
    for _, el in ctx:
        def gt(tag):
            e = el.find(tag)
            return clean_text(e.text) if e is not None and e.text else None
        geo = el.find("Geocode")
        lat = lng = None
        if geo is not None:
            la_e = geo.find("Latitude")
            lo_e = geo.find("Longitude")
            lat = clean_text(la_e.text) if la_e is not None and la_e.text else None
            lng = clean_text(lo_e.text) if lo_e is not None and lo_e.text else None
        rows.append({
            "name": gt("BusinessName"),
            "business_type": gt("BusinessType"),
            "postcode": gt("PostCode"),
            "lat": float(lat) if lat else None,
            "lng": float(lng) if lng else None,
            "rating": gt("RatingValue"),
            "rating_date": gt("RatingDate"),
            "la": la_name,
            "lad": lad,
        })
        el.clear()
        while el.getprevious() is not None:
            del el.getparent()[0]
    return rows


def main():
    ids = discover_ids()
    missing = [LANCS_14[l] for l in LANCS_14 if l not in ids]
    log(f"matched {len(ids)}/14 authorities; missing: {missing}")

    all_rows = []
    per_la = {}
    failures = []
    for lad, info in ids.items():
        fid = info["id"]
        la_name = LANCS_14[lad]
        url = XML_PATTERN.format(id=fid)
        dest = RAW / f"fhrs_{fid}.xml"
        try:
            download(url, dest)
            rows = parse_xml(dest, la_name, lad)
            all_rows.extend(rows)
            per_la[la_name] = len(rows)
            log(f"  {la_name} (id {fid}): {len(rows)} establishments")
        except Exception as e:  # noqa
            failures.append(f"{la_name} (id {fid}): {e}")
            log(f"  FAIL {la_name}: {e}")

    m = meta(
        "https://ratings.food.gov.uk/open-data / api.ratings.food.gov.uk/Authorities",
        "Open Government Licence v3.0 (FSA FHRS open data)",
        "Per-LA open-data XML for the 14 Lancashire authorities. Trading-premises "
        "register: all establishments retained regardless of rating. "
        f"Matched {len(ids)}/14 authorities. Failures: {failures or 'none'}.",
    )
    m["authorities"] = {LANCS_14[l]: ids[l]["id"] for l in ids}
    m["per_la_counts"] = per_la
    write_out("fhrs_lancs.json", m, "establishments", all_rows)
    log(f"TOTAL fhrs: {len(all_rows)} establishments across {len(per_la)} LAs")


if __name__ == "__main__":
    main()
