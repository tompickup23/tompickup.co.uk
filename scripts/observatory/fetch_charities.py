#!/usr/bin/env python3
"""fetch_charities.py - Charity Commission full-register bulk extracts, filtered
to charities whose contact postcode is in Lancashire (resolved to the 14 LADs).

Streams three JSON extracts from their zips (never fully extracted - disk is
tight): charity, charity_area_of_operation, charity_annual_return_parta.

Output: ~/observatory-data/processed/charities_lancs.json
  {charity_number, name, company_number, postcode, lad, status, latest_income,
   latest_expenditure, employees, volunteers, area_of_operation}
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
from _common import (RAW, LANCS_14, meta, write_out, clean_text, log,
                     zip_json_items, resolve_postcodes, lad_for_postcode,
                     looks_lancs_pc, norm_pc)

CHARITY_ZIP = RAW / "charity.zip"
AREA_ZIP = RAW / "charity_area_of_operation.zip"
ARPARTA_ZIP = RAW / "charity_annual_return_parta.zip"

BASE_URL = ("https://ccewuksprdoneregsadata1.blob.core.windows.net/data/json/"
            "publicextract.charity.zip (+ charity_area_of_operation, "
            "charity_annual_return_parta)")


def main():
    # 1. Stream the main charity register; keep main records (linked==0) whose
    #    contact postcode looks Lancashire.
    prelim = {}  # org_number -> record
    scanned = 0
    for c in zip_json_items(CHARITY_ZIP):
        scanned += 1
        if c.get("linked_charity_number") not in (0, "0"):
            continue
        pc = c.get("charity_contact_postcode")
        if not looks_lancs_pc(pc):
            continue
        org = c.get("organisation_number")
        prelim[org] = {
            "charity_number": c.get("registered_charity_number"),
            "organisation_number": org,
            "name": clean_text(c.get("charity_name")),
            "company_number": (str(c.get("charity_company_registration_number")).strip()
                               if c.get("charity_company_registration_number") else None),
            "postcode": (pc or "").strip() or None,
            "status": clean_text(c.get("charity_registration_status")),
            "latest_income": c.get("latest_income"),
            "latest_expenditure": c.get("latest_expenditure"),
            "is_cio": bool(c.get("charity_is_cio")),
        }
    log(f"charity register: scanned {scanned}, {len(prelim)} Lancs-postcode candidates")

    # 2. Resolve postcodes -> LAD; keep only the 14 Lancs LADs.
    cache = resolve_postcodes([r["postcode"] for r in prelim.values() if r["postcode"]])
    kept = {}
    per_lad = {}
    for org, r in prelim.items():
        res = lad_for_postcode(r["postcode"], cache)
        if not res or res["lad"] not in LANCS_14:
            continue
        r["lad"] = res["lad"]
        r["area_of_operation"] = []
        r["employees"] = None       # not in current part-A extract (salary bands only)
        r["volunteers"] = None
        kept[org] = r
        per_lad[res["lad"]] = per_lad.get(res["lad"], 0) + 1
    log(f"kept {len(kept)} charities in the 14 Lancs LADs")

    kept_orgs = set(kept.keys())

    # 3. area_of_operation: collect descriptions for kept orgs.
    n = 0
    for a in zip_json_items(AREA_ZIP):
        org = a.get("organisation_number")
        if org in kept_orgs:
            desc = clean_text(a.get("geographic_area_description"))
            if desc and desc not in kept[org]["area_of_operation"]:
                kept[org]["area_of_operation"].append(desc)
                n += 1
    log(f"area_of_operation: attached {n} area rows")

    # 4. annual_return_parta: latest submitted period -> volunteers (+ employees
    #    proxy). Take latest_fin_period_submitted_ind true, else lowest order no.
    best = {}  # org -> (order_number, row)
    for ar in zip_json_items(ARPARTA_ZIP):
        org = ar.get("organisation_number")
        if org not in kept_orgs:
            continue
        order = ar.get("fin_period_order_number")
        try:
            order = int(order)
        except (TypeError, ValueError):
            order = 999
        latest = ar.get("latest_fin_period_submitted_ind")
        rank = (0 if latest in (True, "true", "True", 1) else 1, order)
        if org not in best or rank < best[org][0]:
            best[org] = (rank, ar)
    for org, (_, ar) in best.items():
        kept[org]["volunteers"] = ar.get("count_volunteers")
        # employee count is not published as a single field; capture >60k count
        over60 = ar.get("employees_salary_over_60k")
        kept[org]["employees_over_60k"] = over60
        # part-A income/expenditure fallback when register latest_* is null
        if kept[org]["latest_income"] is None:
            kept[org]["latest_income"] = ar.get("total_gross_income")
        if kept[org]["latest_expenditure"] is None:
            kept[org]["latest_expenditure"] = ar.get("total_gross_expenditure")
    log(f"annual_return_parta: attached volunteer/employee data for {len(best)} charities")

    rows = list(kept.values())
    m = meta(
        BASE_URL,
        "Open Government Licence v3.0 (Charity Commission for England and Wales)",
        "Registered charities whose contact postcode resolves to one of the 14 "
        "Lancashire LADs. Main records only (linked_charity_number=0). Financials "
        "from the register's latest_income/latest_expenditure, backfilled from "
        "annual return part A gross income/expenditure where null. Volunteers from "
        "count_volunteers. A plain employee count is NOT in the current part-A "
        "extract (only >=60k salary bands), so employees is null and "
        "employees_over_60k is provided instead. Area of operation is the charity's "
        "declared geographic remit, not trading sites.")
    m["per_lad_counts"] = {LANCS_14[k]: v for k, v in per_lad.items()}
    m["charities_with_company_number"] = sum(1 for r in rows if r["company_number"])
    write_out("charities_lancs.json", m, "charities", rows)


if __name__ == "__main__":
    main()
