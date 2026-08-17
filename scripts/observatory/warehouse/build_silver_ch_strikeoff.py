#!/usr/bin/env python3
"""Silver: companies under a proposal to strike off.

silver/ch_register/snapshot_date=.../part.parquet
  -> silver/ch_strikeoff/snapshot_date=.../part.parquet

There is no separate strike-off source. The register carries the status
"Active - Proposal to Strike off" and that is the whole evidence base, so this
table is a typed projection of the register rather than an independent parse.
Saying so plainly in the manifest is the point: a table that looks like its own
source invites a later session to treat it as corroboration of the register,
which it can never be.

The Gazette first and final notices are deliberately NOT joined in here. The
join needs entity resolution and belongs in M3 and the gold marts; a silver
table stays single-source.

What a row may assert, per DATA-INTEGRITY s3: register status "proposal to
strike off". Never imminent dissolution. Many proposals are withdrawn, and the
same register carries the withdrawal.

Usage:
    build_silver_ch_strikeoff.py [--snapshot 2026-08-01]
"""
import argparse
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import silver as SV  # noqa: E402

TABLE = "ch_strikeoff"
UPSTREAM = "ch_register"
STATUS = "Active - Proposal to Strike off"


def latest_silver(table, snapshot=None):
    base = SV.silver_dir() / table
    parts = sorted(base.glob("snapshot_date=*")) if base.exists() else []
    if not parts:
        raise SystemExit(
            f"FATAL: silver/{table} has no partition. Build it first.")
    if snapshot:
        want = base / f"snapshot_date={snapshot}"
        if not want.exists():
            raise SystemExit(f"FATAL: silver/{table} has no {snapshot}")
        chosen = want
    else:
        chosen = parts[-1]
    return chosen.name.split("=", 1)[1], chosen


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--snapshot")
    args = ap.parse_args()

    snap, updir = latest_silver(UPSTREAM, args.snapshot)
    up_pq = updir / "part.parquet"
    up_manifest = json.loads((updir / "manifest.json").read_text())
    SV.log(f"upstream silver/{UPSTREAM} snapshot_date={snap}, "
           f"{up_manifest['rows']:,} rows")

    con, dv = SV.connect()
    SV.log(f"duckdb {dv}")

    select = f"""
    SELECT
      company_number,
      company_name,
      entity_type,
      companies_act_body,
      company_category,
      company_status,
      incorporation_date,
      date_diff('day', incorporation_date, DATE '{snap}') AS age_days,
      accounts_category,
      accounts_last_made_up_date,
      accounts_next_due_date,
      (accounts_next_due_date IS NOT NULL
         AND accounts_next_due_date < DATE '{snap}')  AS accounts_overdue,
      conf_stmt_next_due_date,
      (conf_stmt_next_due_date IS NOT NULL
         AND conf_stmt_next_due_date < DATE '{snap}') AS conf_stmt_overdue,
      mort_outstanding,
      reg_postcode,
      reg_postcode_norm,
      reg_post_town,
      sic_codes,
      DATE '{snap}'  AS snapshot_date,
      DATE '{snap}'  AS as_at,
      'register-status' AS evidence_basis
    FROM read_parquet('{up_pq}')
    WHERE company_status = '{STATUS}'
    """

    out = SV.table_dir(TABLE, snap)
    SV.log("writing parquet")
    rows, nbytes = SV.write_parquet(con, select, out)
    SV.log(f"  {rows:,} rows, {nbytes/1e6:.1f} MB")

    SV.assert_equal("rows vs register proposedStrikeOff",
                    rows, up_manifest["assertions"]["proposedStrikeOff"])

    pq = out / "part.parquet"
    by_type = dict(con.execute(
        f"SELECT entity_type, count(*) FROM read_parquet('{pq}') GROUP BY 1"
    ).fetchall())
    overdue = con.execute(
        f"SELECT count(*) FROM read_parquet('{pq}') WHERE accounts_overdue"
    ).fetchone()[0]
    charged = con.execute(
        f"SELECT count(*) FROM read_parquet('{pq}') WHERE mort_outstanding > 0"
    ).fetchone()[0]

    SV.write_manifest(
        out, TABLE, snap,
        inputs=[{"layer": "silver", "table": UPSTREAM, "snapshotDate": snap,
                 "rows": up_manifest["rows"],
                 "sourceSha256": up_manifest["inputs"][0]["sha256"]}],
        rows=rows, nbytes=nbytes, duckdb_version=dv,
        assertions={"rows": rows,
                    "byEntityType": {k: v for k, v in sorted(by_type.items())},
                    "accountsOverdue": overdue,
                    "withOutstandingCharges": charged},
        notes=("A projection of the register status, not an independent "
               "source, and never corroboration of it. Asserts the register "
               "status only: a proposal is not a dissolution and many are "
               "withdrawn (DATA-INTEGRITY s3). Gazette first and final notices "
               "are joined in gold, after entity resolution, not here."))
    con.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
