#!/usr/bin/env python3
"""build_index.py - Summarise every processed observatory output into INDEX.md.

Reads each processed JSON's $meta plus the primary payload array, and writes
~/observatory-data/processed/INDEX.md with row counts, retrieval dates, source
URLs, licences and any per-LAD breakdowns.
"""
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
from _common import PROC

# processed file -> (payload key, one-line description)
OUTPUTS = [
    ("fhrs_lancs.json", "establishments", "FSA Food Hygiene establishments (trading-premises register)"),
    ("gazette_lancs.json", "notices", "The Gazette corporate insolvency notices (Lancashire)"),
    ("nomis_context.json", "areas", "NOMIS UKBC + BRES + ASHE context (Lancs-14 + NW ring + England)"),
    ("ons_demography.json", "areas", "ONS Business Demography + high-growth business %"),
    ("innovate_lancs.json", "projects", "Innovate UK funded projects (Lancashire participations)"),
    ("charities_lancs.json", "charities", "Charity Commission register (Lancashire charities)"),
    ("mutuals_lancs.json", "societies", "FCA Mutuals Public Register (Lancashire societies)"),
    ("cqc_lancs.json", "locations", "CQC care directory locations (Lancashire)"),
    ("voa_lancs.json", "entries", "VOA 2023 rating list entries (Lancashire billing authorities)"),
    ("gias_lancs.json", "establishments", "GIAS schools/trusts (Lancashire)"),
]


def main():
    lines = ["# Lancashire Business Observatory - processed data INDEX", ""]
    lines.append("Non-Companies-House data layer. Each output is JSON with a "
                 "top-level `$meta` block (source_url, retrieved, licence, notes).")
    lines.append("")
    lines.append("| Output | Rows | Retrieved | Licence |")
    lines.append("|---|---|---|---|")
    details = []
    for fname, key, desc in OUTPUTS:
        p = PROC / fname
        if not p.exists():
            lines.append(f"| {fname} | FAILED / not produced | - | - |")
            details.append((fname, desc, None, None))
            continue
        d = json.load(open(p))
        meta = d.get("$meta", {})
        rows = d.get(key, [])
        n = len(rows) if hasattr(rows, "__len__") else 0
        lines.append(f"| {fname} | {n:,} | {meta.get('retrieved','?')} | "
                     f"{meta.get('licence','?')} |")
        details.append((fname, desc, meta, n))

    # side files
    jsonl = PROC / "gazette_corporate_all.jsonl"
    if jsonl.exists():
        nl = sum(1 for _ in open(jsonl))
        lines.append(f"| gazette_corporate_all.jsonl | {nl:,} | (join table) | OGL/Crown |")

    lines.append("")
    lines.append("## Details")
    lines.append("")
    for fname, desc, meta, n in details:
        lines.append(f"### {fname}")
        lines.append(f"{desc}")
        if meta is None:
            lines.append("- STATUS: FAILED or not produced")
            lines.append("")
            continue
        lines.append(f"- rows: {n:,}")
        lines.append(f"- source_url: {meta.get('source_url','?')}")
        lines.append(f"- retrieved: {meta.get('retrieved','?')}")
        lines.append(f"- licence: {meta.get('licence','?')}")
        pl = meta.get("per_lad_counts") or meta.get("per_la_counts")
        if pl:
            pairs = ", ".join(f"{k}: {v:,}" for k, v in sorted(pl.items()))
            lines.append(f"- per-LAD: {pairs}")
        if meta.get("notes"):
            lines.append(f"- notes: {meta['notes']}")
        lines.append("")

    out = PROC / "INDEX.md"
    out.write_text("\n".join(lines))
    print(f"wrote {out}")
    print("\n".join(lines[:20]))


if __name__ == "__main__":
    main()
