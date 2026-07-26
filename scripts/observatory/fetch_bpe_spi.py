#!/usr/bin/env python3
"""Fetch the two whole-economy calibration inputs:

1. DBT Business Population Estimates 2025 detailed tables: the North West
   registered vs unregistered split and legal-status mix (region level).
2. HMRC Personal Incomes (SPI) Table 3.14: self-employment income by
   borough/district/unitary, for the 14 Lancashire LADs.

Writes ~/observatory-data/processed/bpe_spi.json. Both are calibration
aggregates: the unregistered layer on the site is MODELLED from these and must
always be labelled as such.
"""
import json, re, sys
from pathlib import Path
import requests

OUT = Path.home() / "observatory-data/processed"
RAW = Path.home() / "observatory-data/raw"
OUT.mkdir(parents=True, exist_ok=True)
RAW.mkdir(parents=True, exist_ok=True)
UA = {"User-Agent": "Mozilla/5.0 (observatory data fetch; tompickup.co.uk)"}

def get(url, dest):
    dest = RAW / dest
    if dest.exists() and dest.stat().st_size > 1000:
        return dest
    r = requests.get(url, headers=UA, timeout=120)
    r.raise_for_status()
    dest.write_bytes(r.content)
    return dest

def find_attachment(page_url, pattern):
    html = requests.get(page_url, headers=UA, timeout=60).text
    m = re.findall(r'href="(https://assets\.publishing\.service\.gov\.uk/[^"]+)"', html)
    for u in m:
        if re.search(pattern, u, re.I):
            return u
    raise SystemExit(f"no attachment matching {pattern} on {page_url}:\n" +
                     "\n".join(m[:30]))

result = {"$meta": {"retrieved": "2026-07-26", "licence": "OGL v3",
                    "sources": []}}

# --- 1. BPE 2025 detailed tables -------------------------------------------
bpe_page = ("https://www.gov.uk/government/statistics/business-population-estimates-2025/")
# The tables live on the parent statistics page; fall back to the collection page.
try:
    url = find_attachment(
        "https://www.gov.uk/government/statistics/business-population-estimates-2025",
        r"detailed.*tables.*\.(ods|xlsx)|business-population-estimates.*tables")
except SystemExit:
    url = find_attachment(
        "https://www.gov.uk/government/collections/business-population-estimates",
        r"2025.*tables.*\.(ods|xlsx)")
f = get(url, "bpe_2025_tables" + (".ods" if url.endswith(".ods") else ".xlsx"))
result["$meta"]["sources"].append({"name": "DBT Business Population Estimates 2025",
                                   "url": url})
import pandas as pd
xl = pd.read_excel(f, sheet_name=None, engine="odf" if str(f).endswith(".ods") else None,
                   header=None)
# Find the regional table: a sheet containing "North West" rows with
# all/registered/unregistered columns. BPE detailed tables: Table 27/28 hold
# region x legal status; scan every sheet for the North West all-businesses row.
nw = {}
for sheet, df in xl.items():
    txt = df.astype(str)
    hits = txt.apply(lambda col: col.str.contains("North West", na=False)).any(axis=1)
    if not hits.any():
        continue
    for i in txt.index[hits]:
        row = df.loc[i].tolist()
        nums = [c for c in row if isinstance(c, (int, float)) and not pd.isna(c) and c > 1000]
        if len(nums) >= 3:
            nw.setdefault(sheet, []).append({"row": [str(c)[:40] for c in row[:3]],
                                             "numbers": nums[:8]})
result["bpe_raw_nw_hits"] = nw  # parsed manually below by build step; keep raw
print("BPE sheets with North West rows:", list(nw))

# --- 2. HMRC SPI table 3.14 -------------------------------------------------
spi_url = find_attachment(
    "https://www.gov.uk/government/statistics/personal-incomes-statistics-for-the-tax-year-2023-to-2024",
    r"3_12_to_3_15a.*\.(ods|xlsx)")
f2 = get(spi_url, "spi_314" + (".ods" if spi_url.endswith(".ods") else ".xlsx"))
result["$meta"]["sources"].append({"name": "HMRC Personal Incomes SPI Table 3.14 (2023-24)",
                                   "url": spi_url})
LADS = ["Burnley", "Blackburn with Darwen", "Blackpool", "Chorley", "Fylde",
        "Hyndburn", "Lancaster", "Pendle", "Preston", "Ribble Valley",
        "Rossendale", "South Ribble", "West Lancashire", "Wyre"]
xl2 = pd.read_excel(f2, sheet_name=None, engine="odf" if str(f2).endswith(".ods") else None,
                    header=None)
spi = {}
for sheet, df in xl2.items():
    txt = df.astype(str)
    for lad in LADS:
        exact = txt.apply(lambda col: col.str.fullmatch(lad + "( UA)?", na=False)).any(axis=1)
        for i in txt.index[exact]:
            row = df.loc[i].tolist()
            nums = [c for c in row if isinstance(c, (int, float)) and not pd.isna(c)]
            if len(nums) >= 4 and lad not in spi:
                spi[lad] = {"sheet": sheet, "numbers": nums}
result["spi_raw_rows"] = spi
print("SPI LAD rows found:", len(spi), "/", len(LADS))

(OUT / "bpe_spi.json").write_text(json.dumps(result))
print("written bpe_spi.json")
