#!/usr/bin/env python3
"""Import the working paper's headline results into src/data/lgr/paper.json.

Source: the deposited version on Zenodo, never a working copy.
    Pickup, T. (2026) The Financial Case for Local Government Reorganisation in Lancashire, working paper v3.2.
    Zenodo. https://doi.org/10.5281/zenodo.24208546 (record 24208546,
    file LGR_Lancashire_Zenodo_Package_v3.2.zip, md5 f9b31fe7e25e20755842f53fc9a65d3c).
    Version 3.1 (10.5281/zenodo.23016761) is unchanged on Zenodo; every figure it published for the four
    proposals is the same in version 3.2.

Every figure is copied as printed from the deposit's derived/paper_tables.md (Table 8a) and
derived/paper_figures.json. Nothing is computed here. The script fails if the zip's checksum differs,
if a Table 8a row has no plain label, or if a column is missing.

Run:  python3 scripts/lgr_paper_import.py PATH/TO/LGR_Lancashire_Zenodo_Package_v3.2.zip
      then python3 scripts/lgr_build.py
"""
import hashlib
import json
import sys
import zipfile
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
OUT = ROOT / "src" / "data" / "lgr" / "paper.json"
DOI = "10.5281/zenodo.24208546"
RECORD = "24208546"
MD5 = "f9b31fe7e25e20755842f53fc9a65d3c"
V31_DOI = "10.5281/zenodo.23016761"
PKG = "LGR_Lancashire_Zenodo_Package/"

zpath = Path(sys.argv[1])
blob = zpath.read_bytes()
if hashlib.md5(blob).hexdigest() != MD5:
    sys.exit(f"{zpath}: md5 differs from the deposited file ({MD5})")
z = zipfile.ZipFile(zpath)
names = z.namelist()
pre = PKG if (PKG + "derived/paper_tables.md") in names else ""   # git archive puts files at the root
tables = z.read(pre + "derived/paper_tables.md").decode("utf-8")
figs = json.loads(z.read(pre + "derived/paper_figures.json").decode("utf-8"))

# Table 8a, row by row, as printed. Columns are read from the header, never assumed.
i = tables.index("**Table 8a.")
lines = tables[i:].split("\n")
rows, cur, header = [], None, None
for ln in lines[1:]:
    if rows and not ln.startswith("|"):
        break          # the table ends at the first line after its rows that is not a table row
    if ln.startswith("| Treatment"):
        header = [x.strip() for x in ln.strip().strip("|").split("|")][2:]
        continue
    if not ln.startswith("|") or ln.startswith("|---"):
        continue
    c = [x.strip() for x in ln.strip().strip("|").split("|")]
    treatment, measure, vals = c[0], c[1], c[2:]
    if treatment:
        cur = {"treatment": treatment, "cells": {}}
        rows.append(cur)
    cur["cells"][measure] = dict(zip(header, vals))

CONFIGS = ["2UA", "3UA", "4UA", "5UA", "1UA", "1UA (county+12)"]   # the four proposals, then the two forms
assert header and set(CONFIGS) <= set(header), f"Table 8a columns changed: {header}"
FOUR = CONFIGS[:4]

# plain labels for the page; the paper's own label is kept beside each
PLAIN = {
    "Central: Option 2 overhead, people services estimated": "Central case",
    "Realised savings per resident for two unitaries set to Buckinghamshire's reported rate (as in Table 8d; version 3.2, F75)": "Savings scaled to what Buckinghamshire reported",
    "The same, at West and North Northamptonshire's reported rate (as in Table 8d; version 3.2, F75)": "Savings scaled to what West and North Northamptonshire reported",
    "Councillors at the two unitaries' allowance rate (like for like; version 3.2)": "Councillors paid at the two-council allowance rate",
    "Option 1 overhead (pooled merger slope)": "Pooled merger slope for the overhead (Option 1)",
    "Newton on its own scope (1% per 200,000 on placement spend)": "Newton's assumption on care placements",
    "Newton on all people-services spend (version 3's treatment)": "Newton's assumption on all people services",
    "Option 1 and Newton on placements": "Option 1 and Newton's assumption on placements",
    "Option 1 and Newton on all spend": "Option 1 and Newton's assumption on all people services",
    "SEN transport term excluded": "Without the SEN transport term",
    "Netted overhead definition (central before session 6), Option 2": "Overhead net of internal recharges (earlier definition)",
    "Netted overhead definition, Option 1": "Earlier overhead definition with Option 1",
    "Direct payments term included, 65 and over, at its 2024-25 estimate (central under rule R3a, a sensitivity row under rule R3b)": "With the direct-payments term",
    # rows the paper runs on the single councils only (n/a for the four proposals)
    "Collection and electoral line elasticities at 1 for the single councils (the lines on which they lie beyond the data; version 3.2)": "Council tax collection and election costs scaled one for one with size",
    "Adult placements, 65 and over (A1) term excluded (version 3.2, item G)": "Without the care placements term for people aged 65 and over",
    "Adult placements, 18 to 64 (A11) term excluded (version 3.2, item G)": "Without the care placements term for people aged 18 to 64",
    "All negative parts excluded (SEN transport (T1), adult placements, 65 and over (A1), adult placements, 18 to 64 (A11); version 3.2, item G)": "Without any of the three terms that count against a larger council",
    "Own calibration: each single council's realisation set so that its own realised structural saving per resident equals Buckinghamshire's reported rate, capped at the central 75% (version 3.2, F05)": "Each single council's savings scaled to Buckinghamshire's rate per resident",
    "Own calibration, the same at West and North Northamptonshire's reported rate (version 3.2, F05)": "Each single council's savings scaled to West and North Northamptonshire's rate per resident",
}
out_rows, single_rows, mc = [], [], []
pick = lambda cells, cfgs: [cells.get(k, "") for k in cfgs] if cells else None
for r in rows:
    t = r["treatment"]
    cells = r["cells"]
    if t.startswith("Monte Carlo"):
        mc.append({"paperLabel": t,
                   "pNetPositive": dict(zip(CONFIGS, pick(cells.get("probability net annual > 0"), CONFIGS))),
                   "pNpvPositive": dict(zip(CONFIGS, pick(cells.get("probability ten-year NPV > 0"), CONFIGS)))})
        continue
    if t.startswith("Option 3"):
        continue   # an upper bound only, not a treatment reported with equal prominence
    if t not in PLAIN:
        sys.exit(f"unlabelled Table 8a row: {t}")
    net, npv = cells["net annual, steady state"], cells["ten-year NPV"]
    pay = cells.get("payback (years)", {})
    if all(net.get(k) == "n/a" for k in FOUR):
        single_rows.append({"label": PLAIN[t], "paperLabel": t,
                            "netAnnual": pick(net, CONFIGS[4:]), "tenYearNpv": pick(npv, CONFIGS[4:])})
        continue
    out_rows.append({"label": PLAIN[t], "paperLabel": t,
                     "netAnnual": pick(net, CONFIGS), "tenYearNpv": pick(npv, CONFIGS),
                     "payback": pick(pay, CONFIGS)})
for r in out_rows + single_rows:
    for v in r["netAnnual"] + r["tenYearNpv"]:
        assert v and v.startswith(("£", "−£")), f"{r['paperLabel']}: unexpected cell {v!r}"

K = lambda k: figs[k]
paper = {
    "$meta": {
        "citation": "Pickup, T. (2026). The Financial Case for Local Government Reorganisation in Lancashire: "
                    "A Transparent Component Model of Four Proposals and Two Single-Council Forms, with a Postscript "
                    "on the 2026 Decisions. Working paper, v3.2. Zenodo.",
        "title": "The Financial Case for Local Government Reorganisation in Lancashire: A Transparent Component Model "
                 "of Four Proposals and Two Single-Council Forms, with a Postscript on the 2026 Decisions",
        "version": "3.2",
        "doi": DOI, "doiUrl": f"https://doi.org/{DOI}", "record": f"https://zenodo.org/records/{RECORD}",
        "file": "LGR_Lancashire_Zenodo_Package_v3.2.zip", "fileMd5": MD5, "published": "2026-11-13",
        "previousVersion": {"version": "3.1", "doi": V31_DOI, "doiUrl": f"https://doi.org/{V31_DOI}",
                            "published": "2026-09-28"},
        "tableSource": "derived/paper_tables.md, Table 8a", "figureSource": "derived/paper_figures.json",
        "peerReviewed": False,
        "note": "Working paper, not peer reviewed. Steady-state net annual position and ten-year NPV at 3.5% "
                "(2024-25 prices), for 2, 3, 4 and 5 unitaries and two single-council forms that no council "
                "proposed (one new council for the whole area, 1UA; the county council with its twelve districts, "
                "1UA (county+12)). Structural effects only: transformation savings are excluded. The model's "
                "ordering favours fewer councils by construction. The author is a Cabinet member of Lancashire "
                "County Council, which proposed two unitaries and would be the continuing authority under one of "
                "the single-council forms; see the paper's Declaration of Interests.",
    },
    "configurations": CONFIGS,
    "rows": out_rows,
    "singleCouncilRows": single_rows,
    "monteCarlo": mc,
    "headline": {
        "transitionRange": K("v31.transition.range"),
        "dupPerAuthority4UA": K("v3.dup_per_auth.4UA"),
        "dupTotal4UA": K("v3.dup_total.4UA"),
        "breakeven4UA": K("v3.breakeven.4UA"),
        "net4UA": K("v3.net.4UA"),
        "npv4UA": K("v3.npv.4UA"),
    },
}
OUT.write_text(json.dumps(paper, indent=1, ensure_ascii=False) + "\n")
print(f"{OUT.relative_to(ROOT)}: {len(out_rows)} rows, {len(single_rows)} single-council rows, {len(mc)} Monte Carlo rows")
