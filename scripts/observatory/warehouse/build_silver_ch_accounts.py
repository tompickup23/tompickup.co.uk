#!/usr/bin/env python3
"""Silver: Companies House iXBRL accounts, Lancashire series.

bronze/source=ch_accounts_ixbrl/snapshot_date=.../
    lancs_accounts.jsonl.gz     the existing extractor's output, one row per
                                company per accounting period
    accounts_progress.json      per-month filing and parse-failure ledger
  -> silver/ch_accounts/snapshot_date=.../part.parquet

This wraps the EXISTING extractor. It does not reparse iXBRL and it does not
touch the parser, per the M2 checklist. The only transformation is typing,
a record-level asAt, and the duplicate rule below.

Record-level asAt is mandatory here and nowhere else in the CH family, because
rows inside one file cover different periods (DATA-INTEGRITY s4). period_end is
that date. The file-level snapshot_date says when we captured the extract, and
the two are years apart for a company filing late.

The step-anomaly filter (single-year jumps over 6x are dropped) is deliberately
NOT applied here. Silver is as-filed. The filter is an analytical judgment and
belongs in the gold gazelle build, where it already lives and where risk-register
item 3 says it must survive the migration.

Two iXBRL artefact classes ARE handled, because they are impossible rather than
merely surprising, and an impossible value is the cheapest bug detector there is:

  * A period ending 0001-01-01. Two rows in the 2026-08-08 extract. period_end
    is the record key and its record-level asAt, so a row without a real one
    cannot be typed. Those rows go to _rejected.parquet with a reason, they are
    counted in the manifest, and they are not deleted.
  * A negative or absurd employee count. 269 rows filed a negative s411 average
    and two filed over 700,000, in a Lancashire dataset. employees_as_filed
    keeps exactly what was filed; employees is nulled where the value cannot be
    a headcount, and employees_suspect says which rows that happened to. The
    filing is preserved, the derived figure is not allowed to lie.

Usage:
    build_silver_ch_accounts.py [--snapshot 2026-08-08]
"""
import argparse
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import silver as SV  # noqa: E402

TABLE = "ch_accounts"

# The earliest period end a Companies House iXBRL filing can plausibly carry.
MIN_PERIOD_END = "1990-01-01"
# No single UK filer reports an s411 average headcount anywhere near this.
MAX_EMPLOYEES = 500000


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--snapshot")
    args = ap.parse_args()

    snap, part, manifest = SV.resolve_bronze("ch_accounts_ixbrl", args.snapshot)
    src, src_sha = SV.bronze_file(manifest, part, "lancs_accounts.jsonl.gz")
    prog_path, prog_sha = SV.bronze_file(manifest, part, "accounts_progress.json")
    progress = [json.loads(l) for l in prog_path.read_text().splitlines() if l.strip()]
    expected_records = sum(p["records"] for p in progress)
    expected_filings = sum(p["lancs_filings"] for p in progress)
    parse_fail = sum(p["parse_fail"] for p in progress)
    SV.log(f"bronze snapshot_date={snap}, {len(progress)} monthly archives, "
           f"{expected_records:,} records expected, {parse_fail} parse failures")

    con, dv = SV.connect()
    SV.log(f"duckdb {dv}")

    select = f"""
    SELECT
      crn,
      try_strptime(nullif(trim(period_end), ''), '%Y-%m-%d')::DATE AS period_end,
      try_strptime(nullif(trim(period_end), ''), '%Y-%m-%d')::DATE AS as_at,
      employees                                    AS employees_as_filed,
      CASE WHEN employees < 0 OR employees > {MAX_EMPLOYEES} THEN NULL
           ELSE employees END                      AS employees,
      (employees IS NOT NULL
       AND (employees < 0 OR employees > {MAX_EMPLOYEES})) AS employees_suspect,
      equity,
      total_assets,
      net_current,
      cash,
      turnover,
      filed_zip,
      regexp_extract(filed_zip, 'Accounts_Monthly_Data-([A-Za-z]+[0-9]{{4}})', 1)
        AS filed_month,
      DATE '{snap}'  AS snapshot_date,
      '{src_sha}'    AS source_sha256
    FROM read_json('{src}', format='newline_delimited',
                   columns={{
                     'crn': 'VARCHAR', 'period_end': 'VARCHAR',
                     'employees': 'DOUBLE', 'equity': 'DOUBLE',
                     'total_assets': 'DOUBLE', 'net_current': 'DOUBLE',
                     'cash': 'DOUBLE', 'turnover': 'DOUBLE',
                     'filed_zip': 'VARCHAR'
                   }})
    """
    keep = (f"period_end IS NOT NULL AND period_end >= DATE '{MIN_PERIOD_END}' "
            f"AND period_end <= DATE '{snap}'")
    # (crn, period_end) is the intended key, but the same period legitimately
    # reappears when a company files amended accounts in a later month. The
    # newest filing wins and the count of superseded rows is reported, never
    # silently collapsed.
    dedup = f"""
    SELECT * EXCLUDE (rn) FROM (
      SELECT *, row_number() OVER (
        PARTITION BY crn, period_end
        ORDER BY filed_zip DESC NULLS LAST) AS rn
      FROM ({select}) WHERE {keep}
    ) WHERE rn = 1
    """

    raw_rows = con.execute(f"SELECT count(*) FROM ({select})").fetchone()[0]
    SV.assert_equal("records vs extractor ledger", raw_rows, expected_records)

    out = SV.table_dir(TABLE, snap)
    rejected_sql = f"""
    SELECT *, CASE
        WHEN period_end IS NULL THEN 'period_end did not parse'
        WHEN period_end < DATE '{MIN_PERIOD_END}' THEN 'period_end before {MIN_PERIOD_END}'
        ELSE 'period_end after the snapshot' END AS reject_reason
    FROM ({select}) WHERE NOT ({keep})
    """
    rej_path = out / "_rejected.parquet"
    con.execute(f"COPY ({rejected_sql}) TO '{rej_path}' "
                "(FORMAT PARQUET, COMPRESSION ZSTD)")
    rejected = con.execute(
        f"SELECT count(*) FROM read_parquet('{rej_path}')").fetchone()[0]
    if rejected:
        for r in con.execute(
                f"SELECT crn, period_end, reject_reason FROM "
                f"read_parquet('{rej_path}') LIMIT 10").fetchall():
            SV.log(f"  rejected {r[0]} {r[1]} ({r[2]})")

    SV.log("writing parquet")
    rows, nbytes = SV.write_parquet(con, dedup, out)
    superseded = raw_rows - rejected - rows
    SV.log(f"  {rows:,} rows, {nbytes/1e6:.1f} MB, {superseded:,} superseded "
           f"filings collapsed, {rejected} rows rejected to _rejected.parquet")

    pq = out / "part.parquet"
    stats = con.execute(f"""
        SELECT count(DISTINCT crn), min(period_end), max(period_end),
               count(*) FILTER (WHERE employees IS NOT NULL),
               count(*) FILTER (WHERE period_end > current_date),
               count(*) FILTER (WHERE employees_suspect),
               max(employees)
        FROM read_parquet('{pq}')
    """).fetchone()
    SV.log(f"  {stats[0]:,} companies, periods {stats[1]} to {stats[2]}, "
           f"{stats[3]:,} rows with a usable employee figure, "
           f"{stats[5]:,} impossible headcounts nulled, max {stats[6]}")
    if stats[4]:
        raise SystemExit(f"FATAL: {stats[4]} accounting periods end in the future")

    SV.write_manifest(
        out, TABLE, snap,
        inputs=[{"layer": "bronze", "source": "ch_accounts_ixbrl",
                 "snapshotDate": snap, "file": src.name, "sha256": src_sha},
                {"layer": "bronze", "source": "ch_accounts_ixbrl",
                 "snapshotDate": snap, "file": prog_path.name,
                 "sha256": prog_sha}],
        rows=rows, nbytes=nbytes, duckdb_version=dv,
        assertions={"recordsFromExtractor": raw_rows,
                    "expectedFromProgressLedger": expected_records,
                    "rows": rows,
                    "rowsRejectedImpossiblePeriod": rejected,
                    "supersededFilingsCollapsed": superseded,
                    "companies": stats[0],
                    "monthlyArchivesParsed": len(progress),
                    "lancsFilings": expected_filings,
                    "parseFailures": parse_fail,
                    "rowsWithEmployees": stats[3],
                    "impossibleHeadcountsNulled": stats[5],
                    "maxEmployees": stats[6]},
        notes=("As filed. The step-anomaly filter stays in the gold gazelle "
               "build, not here. asAt is record-level (period_end) because "
               "rows inside one file cover different periods. Employees is the "
               "s411 average for the period, never a current headcount."))
    con.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
