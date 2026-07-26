#!/usr/bin/env python3
"""fetch_voa.py - VOA 2023 compiled rating list (list entries) for the
Lancashire billing authorities.

Streams the ~570MB star-delimited CSV from inside its zip (never fully
extracted - disk is tight). Prefilters records by Lancashire postcode area,
learns each billing-authority code's LAD by resolving a sample of its postcodes
(a BA code maps 1:1 to a district/unitary), then keeps every hereditament whose
BA code resolves to one of the 14 Lancs LADs.

Output: ~/observatory-data/processed/voa_lancs.json
  entries: {ba_code, lad, address, postcode, description, rateable_value}
  plus a per-LAD aggregate (hereditament count + total RV, and by SCat category).
"""
import sys
from collections import defaultdict, Counter
from pathlib import Path
import zipfile

sys.path.insert(0, str(Path(__file__).parent))
from _common import (RAW, LANCS_14, meta, write_out, clean_text, log,
                     looks_lancs_pc, resolve_postcodes, lad_for_postcode)

VOA_ZIP = RAW / "voa_2023_listentries.zip"
URL = ("https://voaratinglists.blob.core.windows.net/downloads/"
       "uk-englandwales-ndr-2023-listentries-compiled-epoch-0021-baseline-csv.zip")

I_BA, I_REF, I_DESCCODE, I_DESC, I_ADDR, I_PC, I_RV = 1, 3, 4, 5, 7, 14, 17


def main():
    z = zipfile.ZipFile(VOA_ZIP)
    csv_name = [n for n in z.namelist() if "historic" not in n][0]

    records = []            # candidate rows (Lancs postcode area)
    ba_samples = defaultdict(set)
    n_total = 0
    with z.open(csv_name) as f:
        for raw in f:
            n_total += 1
            line = raw.decode("latin-1").rstrip("\r\n")
            p = line.split("*")
            if len(p) < 18:
                continue
            pc = p[I_PC].strip()
            if not looks_lancs_pc(pc):
                continue
            ba = p[I_BA].strip()
            rv = p[I_RV].strip()
            try:
                rv_val = int(rv) if rv else None
            except ValueError:
                rv_val = None
            records.append({
                "ba_code": ba,
                "address": clean_text(p[I_ADDR]),
                "postcode": pc,
                "description": clean_text(p[I_DESC]),
                "scat": p[I_DESCCODE].strip() or None,
                "rateable_value": rv_val,
            })
            if len(ba_samples[ba]) < 15:
                ba_samples[ba].add(pc)
    log(f"scanned {n_total} list entries; {len(records)} in Lancs postcode areas; "
        f"{len(ba_samples)} distinct BA codes")

    # Learn BA code -> LAD by resolving sampled postcodes (majority vote).
    all_samples = [pc for s in ba_samples.values() for pc in s]
    cache = resolve_postcodes(all_samples)
    ba_to_lad = {}
    for ba, pcs in ba_samples.items():
        votes = Counter()
        for pc in pcs:
            res = lad_for_postcode(pc, cache)
            if res:
                votes[res["lad"]] += 1
        if votes:
            lad, _ = votes.most_common(1)[0]
            if lad in LANCS_14:
                ba_to_lad[ba] = lad
    log(f"BA codes mapping to the 14 Lancs LADs: {len(ba_to_lad)}")

    entries = []
    agg = {}  # lad -> {count, total_rv, by_category:{scat:{count,total_rv}}}
    ba_codes_by_lad = defaultdict(set)
    for r in records:
        lad = ba_to_lad.get(r["ba_code"])
        if not lad:
            continue
        r["lad"] = lad
        entries.append(r)
        ba_codes_by_lad[lad].add(r["ba_code"])
        a = agg.setdefault(lad, {"hereditament_count": 0, "total_rateable_value": 0,
                                 "by_category": {}})
        a["hereditament_count"] += 1
        if r["rateable_value"]:
            a["total_rateable_value"] += r["rateable_value"]
        cat = r["scat"] or "unknown"
        c = a["by_category"].setdefault(cat, {"count": 0, "total_rateable_value": 0})
        c["count"] += 1
        if r["rateable_value"]:
            c["total_rateable_value"] += r["rateable_value"]

    m = meta(
        URL,
        "Open Government Licence v3.0 (Valuation Office Agency)",
        "2023 compiled non-domestic rating list (list entries, epoch 0021 "
        "baseline) for the 14 Lancashire billing authorities. Billing-authority "
        "code was mapped to LAD by resolving a sample of its postcodes (BA code = "
        "one district/unitary). Full per-hereditament entries retained plus a "
        "per-LAD aggregate. Rateable values are list figures; verify LAD totals "
        "against published NNDR1 before quoting as a rates base. SCat is the VOA "
        "primary-description (bulk class) code.")
    m["per_lad_aggregate"] = {
        LANCS_14[lad]: {"hereditament_count": a["hereditament_count"],
                        "total_rateable_value": a["total_rateable_value"]}
        for lad, a in agg.items()}
    m["ba_codes_by_lad"] = {LANCS_14[lad]: sorted(codes)
                            for lad, codes in ba_codes_by_lad.items()}
    m["aggregate_by_lad_category"] = {LANCS_14[lad]: a["by_category"]
                                      for lad, a in agg.items()}
    write_out("voa_lancs.json", m, "entries", entries)


if __name__ == "__main__":
    main()
