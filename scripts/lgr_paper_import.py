#!/usr/bin/env python3
"""Import the working paper's headline results into src/data/lgr/paper.json.

Source: the deposited version on Zenodo, never a working copy.
    Pickup, T. (2026) The Financial Case for Local Government Reorganisation in Lancashire, working paper v3.1.
    Zenodo. https://doi.org/10.5281/zenodo.23016761 (record 23016761,
    file LGR_Lancashire_Zenodo_Package_v3.1.zip, md5 ec718021205d5f70695caa59fd156bd3).

Every figure is copied as printed from the deposit's derived/paper_tables.md (Table 8a) and
derived/paper_figures.json. Nothing is computed here. The script fails if the zip's checksum differs.

Run:  python3 scripts/lgr_paper_import.py PATH/TO/LGR_Lancashire_Zenodo_Package_v3.1.zip
      then python3 scripts/lgr_build.py
"""
import hashlib
import json
import sys
import zipfile
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
OUT = ROOT / "src" / "data" / "lgr" / "paper.json"
DOI = "10.5281/zenodo.23016761"
MD5 = "ec718021205d5f70695caa59fd156bd3"
PKG = "LGR_Lancashire_Zenodo_Package/"

zpath = Path(sys.argv[1])
blob = zpath.read_bytes()
if hashlib.md5(blob).hexdigest() != MD5:
    sys.exit(f"{zpath}: md5 differs from the deposited file ({MD5})")
z = zipfile.ZipFile(zpath)
tables = z.read(PKG + "derived/paper_tables.md").decode("utf-8")
figs = json.loads(z.read(PKG + "derived/paper_figures.json").decode("utf-8"))

# Table 8a, row by row, as printed
i = tables.index("**Table 8a.")
lines = tables[i:].split("\n")
rows, cur = [], None
for ln in lines[1:]:
    if rows and not ln.startswith("|"):
        break          # the table ends at the first line after its rows that is not a table row
    if not ln.startswith("|") or ln.startswith("|---") or ln.startswith("| Treatment"):
        continue
    c = [x.strip() for x in ln.strip("|").split("|")]
    treatment, measure, vals = c[0], c[1], c[2:6]
    if treatment:
        cur = {"treatment": treatment, "cells": {}}
        rows.append(cur)
    cur["cells"][measure] = vals

# plain labels for the page; the paper's own label is kept beside each
PLAIN = {
    "Central: Option 2 overhead, people services estimated": "Central case",
    "Option 1 overhead (pooled merger slope)": "Pooled merger slope for the overhead (Option 1)",
    "Newton on its own scope (1% per 200,000 on placement spend)": "Newton's assumption on care placements",
    "Newton on all people-services spend (version 3's treatment)": "Newton's assumption on all people services",
    "Option 1 and Newton on placements": "Option 1 and Newton's assumption on placements",
    "Option 1 and Newton on all spend": "Option 1 and Newton's assumption on all people services",
    "SEN transport term excluded": "Without the SEN transport term",
    "Netted overhead definition (central before session 6), Option 2": "Overhead net of internal recharges (earlier definition)",
    "Netted overhead definition, Option 1": "Earlier overhead definition with Option 1",
    "Direct payments term included, 65 and over, at its 2024-25 estimate (central under rule R3a, a sensitivity row under rule R3b)": "With the direct-payments term",
}
out_rows, mc = [], []
for r in rows:
    t = r["treatment"]
    if t.startswith("Monte Carlo"):
        mc.append({"paperLabel": t, "pNetPositive": r["cells"].get("probability net annual > 0"),
                   "pNpvPositive": r["cells"].get("probability ten-year NPV > 0")})
        continue
    if t.startswith("Option 3"):
        continue   # an upper bound only, not a treatment reported with equal prominence
    if t not in PLAIN:
        sys.exit(f"unlabelled Table 8a row: {t}")
    out_rows.append({"label": PLAIN[t], "paperLabel": t,
                     "netAnnual": r["cells"]["net annual, steady state"],
                     "tenYearNpv": r["cells"]["ten-year NPV"],
                     "payback": r["cells"]["payback (years)"]})

K = lambda k: figs[k]
paper = {
    "$meta": {
        "citation": "Pickup, T. (2026). The Financial Case for Local Government Reorganisation in Lancashire: "
                    "A Transparent Component Model of Five Structural Proposals, with a Postscript on the 2026 "
                    "Decisions. Working paper, v3.1. Zenodo.",
        "doi": DOI, "doiUrl": f"https://doi.org/{DOI}", "record": "https://zenodo.org/records/23016761",
        "file": zpath.name, "fileMd5": MD5, "published": "2026-09-28",
        "tableSource": "derived/paper_tables.md, Table 8a", "figureSource": "derived/paper_figures.json",
        "peerReviewed": False,
        "note": "Working paper, not peer reviewed. Steady-state net annual position and ten-year NPV at 3.5% "
                "(2024-25 prices), for 2, 3, 4 and 5 unitaries. Structural effects only: transformation savings "
                "are excluded. The author is a Cabinet member of Lancashire County Council, which proposed two "
                "unitaries; see the paper's Declaration of Interests.",
    },
    "configurations": ["2UA", "3UA", "4UA", "5UA"],
    "rows": out_rows,
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
print(f"{OUT.relative_to(ROOT)}: {len(out_rows)} rows, {len(mc)} Monte Carlo rows")
