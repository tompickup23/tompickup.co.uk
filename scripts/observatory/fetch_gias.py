#!/usr/bin/env python3
"""fetch_gias.py - GIAS all-establishments CSV, filtered to the 14 Lancs LAs,
open establishments only.

Tries the date-stamped public download endpoint for today then recent days.
Output: ~/observatory-data/processed/gias_lancs.json
"""
import sys
import csv
import io
import datetime as dt
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
from _common import (RAW, LANCS_14, download, meta, write_out, clean_text, log,
                     fresh)

URL_PATTERN = ("https://ea-edubase-api-prod.azurewebsites.net/edubase/downloads/"
               "public/edubasealldata{ymd}.csv")

# GIAS "LA (name)" values for the 14 Lancs LADs. GIAS uses upper-tier LA names.
# Districts sit under "Lancashire"; the two unitaries are separate.
LANCS_LA_NAMES = {"Lancashire", "Blackpool", "Blackburn with Darwen"}


def fetch_csv():
    dest = RAW / "gias_alldata.csv"
    if fresh(dest):
        log(f"cache hit {dest.name}")
        return dest
    today = dt.date.today()
    for back in range(0, 6):
        d = today - dt.timedelta(days=back)
        url = URL_PATTERN.format(ymd=d.strftime("%Y%m%d"))
        try:
            download(url, dest, days=7)
            if dest.stat().st_size > 1_000_000:
                return dest
        except Exception as e:  # noqa
            log(f"  {d} not available: {e}")
    raise RuntimeError("GIAS all-data CSV not available for last 6 days")


def main():
    path = fetch_csv()
    rows = []
    per_la = {}
    # GIAS CSV is latin-1 encoded.
    with open(path, encoding="latin-1", newline="") as f:
        reader = csv.DictReader(f)
        for r in reader:
            la = (r.get("LA (name)") or "").strip()
            if la not in LANCS_LA_NAMES:
                continue
            status = (r.get("EstablishmentStatus (name)") or "").strip()
            if status.lower() != "open":
                continue
            rows.append({
                "urn": r.get("URN"),
                "name": clean_text(r.get("EstablishmentName")),
                "type_group": clean_text(r.get("EstablishmentTypeGroup (name)")),
                "la_name": la,
                "postcode": (r.get("Postcode") or "").strip() or None,
                "trust_name": clean_text(r.get("Trusts (name)")) or None,
                "trust_company_number": (r.get("Trusts (code)") or "").strip() or None,
            })
            per_la[la] = per_la.get(la, 0) + 1
    m = meta(
        "https://get-information-schools.service.gov.uk/Downloads "
        "(edubasealldata CSV)",
        "Open Government Licence v3.0",
        "All open establishments where LA (name) is Lancashire, Blackpool or "
        "Blackburn with Darwen. GIAS uses upper-tier LA, so district-level "
        "attribution requires postcode->LAD resolution downstream. "
        "trust_company_number is the GIAS Trusts (code), the academy trust "
        "company number where present.")
    m["per_la_counts"] = per_la
    write_out("gias_lancs.json", m, "establishments", rows)


if __name__ == "__main__":
    main()
