#!/usr/bin/env python3
"""fetch_nomis.py - NOMIS API context pulls for Lancs-14 + NW ring + England.

a. NM_142_1 enterprises: LA x SIC section x employment size band; plus legal
   status split (sole proprietor / partnership / company / non-profit) per LA.
b. NM_141_1 local units: counts by LA (branch-presence denominator).
c. NM_189_1 BRES: employee jobs by LA x SIC section, latest year.
d. ASHE NM_99_1 (workplace) + NM_30_1 (resident): median gross weekly pay,
   full-time, latest year, per LA.

Output: ~/observatory-data/processed/nomis_context.json keyed by LAD, England
rows for benchmarking. Idempotent: caches the raw NOMIS JSON per call.
"""
import sys
import json
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
from _common import (RAW, PROC, LANCS_14, NW_RING, ENGLAND, get_json, meta,
                     write_out, log, fresh)

BASE = "https://www.nomisweb.co.uk/api/v01/dataset"

# SIC 2007 2-digit division -> section letter.
DIV_SECTION = {}
_SECTIONS = {
    "A": range(1, 4), "B": range(5, 10), "C": range(10, 34), "D": [35],
    "E": range(36, 40), "F": range(41, 44), "G": range(45, 48),
    "H": range(49, 54), "I": range(55, 57), "J": range(58, 64),
    "K": range(64, 67), "L": [68], "M": range(69, 76), "N": range(77, 83),
    "O": [84], "P": [85], "Q": range(86, 89), "R": range(90, 94),
    "S": range(94, 97), "T": range(97, 99), "U": [99],
}
SECTION_NAMES = {
    "A": "Agriculture, forestry and fishing", "B": "Mining and quarrying",
    "C": "Manufacturing", "D": "Electricity, gas, steam and air conditioning",
    "E": "Water supply, sewerage, waste", "F": "Construction",
    "G": "Wholesale and retail; repair of vehicles",
    "H": "Transportation and storage", "I": "Accommodation and food service",
    "J": "Information and communication", "K": "Financial and insurance",
    "L": "Real estate", "M": "Professional, scientific and technical",
    "N": "Administrative and support service", "O": "Public administration and defence",
    "P": "Education", "Q": "Human health and social work",
    "R": "Arts, entertainment and recreation", "S": "Other service activities",
    "T": "Households as employers", "U": "Extraterritorial organisations",
}
for sec, divs in _SECTIONS.items():
    for dv in divs:
        DIV_SECTION[dv] = sec

ALL_GEOS = {**LANCS_14, **NW_RING, **ENGLAND}
GEO_CSV = ",".join(ALL_GEOS.keys())


def cached_data(cache_name, url_path, params):
    dest = RAW / cache_name
    if fresh(dest):
        log(f"cache hit {cache_name}")
        return json.load(open(dest))
    d = get_json(f"{BASE}/{url_path}", params=params)
    json.dump(d, open(dest, "w"))
    log(f"fetched {cache_name}: {len(d.get('obs', []))} obs")
    return d


def division_type_code(dataset):
    """Find the NOMIS TypeCode for SIC 2-digit divisions in this dataset."""
    d = get_json(f"{BASE}/{dataset}/industry.def.sdmx.json")
    cl = d["structure"]["codelists"]["codelist"][0]["code"]
    for c in cl:
        anns = {a["annotationtitle"]: a["annotationtext"]
                for a in c["annotations"]["annotation"]}
        tn = str(anns.get("TypeName", ""))
        if "division" in tn.lower():
            return anns.get("TypeCode")
    return None


def div_num(desc):
    """Parse the leading 2-digit division number from a NOMIS description."""
    d = desc.strip()
    if len(d) >= 2 and d[:2].isdigit():
        return int(d[:2])
    return None


def rollup_sections(obs):
    """Aggregate division-level obs into SIC-section totals per geography."""
    # returns {geo: {section_letter: value}}, plus year
    out = {}
    year = None
    for o in obs:
        geo = o["geography"]["geogcode"]
        val = o["obs_value"]["value"]
        if val is None:
            continue
        num = div_num(o["industry"]["description"])
        if num is None:
            continue
        sec = DIV_SECTION.get(num)
        if not sec:
            continue
        year = o["time"]["value"]
        out.setdefault(geo, {}).setdefault(sec, 0)
        out[geo][sec] += val
    return out, year


def main():
    out = {g: {"name": ALL_GEOS[g], "ons": g} for g in ALL_GEOS}
    notes = []

    # --- a1. NM_142_1 enterprises by industry (division->section) x sizeband
    div_t = division_type_code("NM_142_1")
    sizebands = {"0": "total", "10": "micro_0_9", "20": "small_10_49",
                 "30": "medium_50_249", "40": "large_250plus"}
    ent_by_sec_size = {g: {} for g in ALL_GEOS}
    for sb, sblabel in sizebands.items():
        d = cached_data(
            f"nomis_142_ind_sb{sb}.json", "NM_142_1.data.json",
            {"geography": GEO_CSV, "date": "latest", "industry": f"TYPE{div_t}",
             "employment_sizeband": sb, "legal_status": "0", "measures": "20100"})
        secs, yr = rollup_sections(d.get("obs", []))
        for g, sd in secs.items():
            ent_by_sec_size.setdefault(g, {})
            ent_by_sec_size[g][sblabel] = {k: sd[k] for k in sorted(sd)}
        notes.append(f"NM_142_1 enterprises year {yr}")
    for g in ALL_GEOS:
        out[g]["enterprises_by_section_sizeband"] = ent_by_sec_size.get(g, {})

    # --- a2. NM_142_1 legal-status split (total industry, total sizeband)
    legal = {"1": "company", "2": "sole_proprietor", "3": "partnership",
             "7": "non_profit_or_mutual", "10": "private_sector_total", "0": "total"}
    d = cached_data(
        "nomis_142_legal.json", "NM_142_1.data.json",
        {"geography": GEO_CSV, "date": "latest", "industry": "37748736",
         "employment_sizeband": "0", "legal_status": ",".join(legal.keys()),
         "measures": "20100"})
    for o in d.get("obs", []):
        g = o["geography"]["geogcode"]
        ls = str(o["legal_status"]["value"])
        lbl = legal.get(ls)
        if lbl:
            out[g].setdefault("enterprises_by_legal_status", {})[lbl] = o["obs_value"]["value"]
        out[g]["enterprises_year"] = o["time"]["value"]

    # --- b. NM_141_1 local units total per LA
    d = cached_data(
        "nomis_141_localunits.json", "NM_141_1.data.json",
        {"geography": GEO_CSV, "date": "latest", "industry": "37748736",
         "employment_sizeband": "0", "legal_status": "0", "measures": "20100"})
    for o in d.get("obs", []):
        g = o["geography"]["geogcode"]
        out[g]["local_units_total"] = o["obs_value"]["value"]
        out[g]["local_units_year"] = o["time"]["value"]

    # --- c. NM_189_1 BRES employee jobs by section
    div_t2 = division_type_code("NM_189_1")
    d = cached_data(
        "nomis_189_bres.json", "NM_189_1.data.json",
        {"geography": GEO_CSV, "date": "latest", "industry": f"TYPE{div_t2}",
         "employment_status": "1", "measure": "1", "measures": "20100"})
    secs, yr = rollup_sections(d.get("obs", []))
    for g in ALL_GEOS:
        out[g]["bres_employee_jobs_by_section"] = {k: secs.get(g, {}).get(k)
                                                   for k in sorted(secs.get(g, {}))}
        out[g]["bres_employee_jobs_total"] = sum(secs.get(g, {}).values()) or None
    notes.append(f"NM_189_1 BRES employee jobs year {yr}")

    # --- d. ASHE median gross weekly pay, full-time. sex=8 (FT), item=2
    #        (Median), pay=1 (Weekly gross). Workplace NM_99_1 + Resident NM_30_1
    for ds, key in [("NM_99_1", "ashe_workplace_median_weekly_ft"),
                    ("NM_30_1", "ashe_resident_median_weekly_ft")]:
        d = cached_data(
            f"nomis_{ds.split('_')[1]}_ashe.json", f"{ds}.data.json",
            {"geography": GEO_CSV, "date": "latest", "sex": "8", "item": "2",
             "pay": "1", "measures": "20100"})
        for o in d.get("obs", []):
            g = o["geography"]["geogcode"]
            v = o["obs_value"]["value"]
            out[g][key] = v
            out[g][key + "_year"] = o["time"]["value"]
        notes.append(f"{ds} ASHE median weekly FT pay")

    m = meta(
        "https://www.nomisweb.co.uk/api/v01/dataset/ (NM_142_1, NM_141_1, NM_189_1, NM_99_1, NM_30_1)",
        "Open Government Licence v3.0 (ONS Crown Copyright via NOMIS)",
        "UK Business Counts (enterprises + local units), BRES employee jobs, and "
        "ASHE median gross weekly pay (full-time) for the 14 Lancashire LADs, the "
        "NW benchmark ring and England. SIC broad sections built by rolling up "
        "2-digit divisions. IDBR caveat: VAT/PAYE-registered businesses only. "
        "BRES jobs are open-access rounded counts. " + "; ".join(notes) + ".")
    m["section_names"] = SECTION_NAMES
    m["geography_groups"] = {
        "lancs_14": list(LANCS_14.keys()),
        "nw_ring": list(NW_RING.keys()),
        "england": list(ENGLAND.keys()),
    }
    write_out("nomis_context.json", m, "areas", out)
    log(f"nomis_context: {len(out)} geographies")


if __name__ == "__main__":
    main()
