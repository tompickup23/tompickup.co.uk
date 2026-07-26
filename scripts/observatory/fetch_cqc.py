#!/usr/bin/env python3
"""fetch_cqc.py - CQC care directory CSV, filtered to locations in the 14
Lancashire LAs. Output: ~/observatory-data/processed/cqc_lancs.json

The public "CQC directory" CSV is location-level: it carries a Local authority
name (upper-tier for social care) and postcode, but NOT a provider company
number. provider_company_number is therefore null here (recover via CH name
match downstream). District-level LAD is resolved from postcode via postcodes.io.
"""
import sys
import csv
import re
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
from _common import (RAW, LANCS_14, download, meta, write_out, clean_text, log,
                     fresh, get_json, resolve_postcodes, lad_for_postcode,
                     looks_lancs_pc, UA)

DIRECTORY_PAGE = "https://www.cqc.org.uk/about-us/transparency/using-cqc-data"
# CQC upper-tier LA names covering the 14 Lancs LADs.
LANCS_CQC_LA = {"lancashire", "blackpool", "blackburn with darwen"}


def find_csv_url():
    import requests
    r = requests.get(DIRECTORY_PAGE, headers={"User-Agent": UA}, timeout=60)
    r.raise_for_status()
    links = re.findall(r'href="([^"]+CQC_directory\.csv[^"]*)"', r.text, re.I)
    if not links:
        links = re.findall(r'href="([^"]+directory\.csv[^"]*)"', r.text, re.I)
    if not links:
        raise RuntimeError("CQC directory CSV link not found on page")
    url = links[0]
    if url.startswith("/"):
        url = "https://www.cqc.org.uk" + url
    return url


def header_offset(path):
    with open(path, encoding="latin-1") as f:
        for i, line in enumerate(f):
            if "Local authority" in line and "Provider name" in line:
                return i
    return 0


def main():
    url = find_csv_url()
    dest = RAW / "cqc_directory.csv"
    download(url, dest)
    off = header_offset(dest)

    prelim = []
    with open(dest, encoding="latin-1", newline="") as f:
        for _ in range(off):
            next(f)
        reader = csv.DictReader(f)
        for r in reader:
            la = (r.get("Local authority") or "").strip().lower()
            pc = (r.get("Postcode") or "").strip()
            if la not in LANCS_CQC_LA and not looks_lancs_pc(pc):
                continue
            prelim.append({
                "location_name": clean_text(r.get("Name")),
                "provider_name": clean_text(r.get("Provider name")),
                "provider_company_number": None,  # not in public directory CSV
                "postcode": pc or None,
                "cqc_la": clean_text(r.get("Local authority")),
                "location_id": (r.get("CQC Location ID (for office use only)") or "").strip() or None,
                "provider_id": (r.get("CQC Provider ID (for office use only)") or "").strip() or None,
                "service_types": clean_text(r.get("Service types")),
            })

    # Resolve postcodes to LAD; keep only rows in the 14 Lancs LADs.
    pcs = [r["postcode"] for r in prelim if r["postcode"]]
    cache = resolve_postcodes(pcs)
    rows = []
    per_lad = {}
    for r in prelim:
        res = lad_for_postcode(r["postcode"], cache)
        if not res or res["lad"] not in LANCS_14:
            continue
        r["lad"] = res["lad"]
        r.pop("cqc_la", None)
        rows.append(r)
        per_lad[res["lad"]] = per_lad.get(res["lad"], 0) + 1

    m = meta(
        DIRECTORY_PAGE + " (dated CQC_directory.csv)",
        "Open Government Licence v3.0 (CQC)",
        "CQC care directory locations resolved by postcode to the 14 Lancashire "
        "LADs. Public directory CSV is location-level and carries no provider "
        "company number, so provider_company_number is null (match to CH by "
        "provider_name downstream). CQC provider_id retained for API enrichment.")
    m["per_lad_counts"] = {LANCS_14[k]: v for k, v in per_lad.items()}
    write_out("cqc_lancs.json", m, "locations", rows)


if __name__ == "__main__":
    main()
