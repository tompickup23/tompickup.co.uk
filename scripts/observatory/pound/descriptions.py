#!/usr/bin/env python3
"""The council's own descriptions of each payee key, for the agent review's
evidence blocks (RULES.md decisions 12 and 13). Run on the Mac, where the
bank files are, before agent_review.py; the output is copied to
$POUND_WORK/inputs.

Reads the 28 bank files pinned in spend_input_body_year.csv (Reports
Public_Pound_work/phase1/build), refusing any whose sha256 differs from the
pin (stop rule 8). For each payee key, upper(strip(supplier_canonical or
supplier)) as the spend input writes it, keeps the three department,
service area and expenditure category combinations with the most value.
The free-text description and reference columns are never read: they can
carry personal details.

Usage: descriptions.py <spend_input_body_year.csv> <bank folder> <out.jsonl.gz>
  where the bank folder holds <body>/<file name> as in bank_file_key.
"""
import csv
import gzip
import json
import sys
from pathlib import Path

import duckdb

from common import sha256_file


def main():
    pins, banks, out = Path(sys.argv[1]), Path(sys.argv[2]), Path(sys.argv[3])
    con = duckdb.connect()
    rows = []
    for r in csv.DictReader(open(pins, newline="")):
        parts = r["bank_file_key"].split("/")
        p = banks / parts[1] / parts[-1]
        if sha256_file(p) != r["bank_file_sha256"]:
            raise SystemExit(f"{p}: sha256 differs from the pin in {pins.name}")
        rows += [(r["body"],) + t for t in con.execute(f"""
            SELECT upper(trim(coalesce(nullif(trim(supplier_canonical), ''), supplier))) AS k,
                   coalesce(department_raw, department, '') AS dept,
                   coalesce(service_area_raw, service_area, '') AS area,
                   coalesce(expenditure_category, '') AS cat,
                   round(sum(amount) * 100)::BIGINT AS pence
            FROM '{p}'
            WHERE coalesce(record_class, '') <> 'internal_transfer'
            GROUP BY ALL""").fetchall()]
    by = {}
    for body, k, dept, area, cat, pence in rows:
        if not k:
            continue
        d = by.setdefault((body, k), {})
        d[(dept, area, cat)] = d.get((dept, area, cat), 0) + pence
    with gzip.open(out, "wt") as f:
        for (body, k), d in sorted(by.items()):
            top = sorted(d.items(), key=lambda kv: (-kv[1], kv[0]))[:3]
            f.write(json.dumps({"body_id": body, "payee_key": k, "descriptions": [
                {"department": a, "service_area": b, "expenditure_category": c, "value_pence": v}
                for (a, b, c), v in top]}, ensure_ascii=False) + "\n")
    print(f"{len(by)} payee keys written to {out}")


if __name__ == "__main__":
    main()
