#!/usr/bin/env python3
"""gold/entity_history: SCD2 over the silver register snapshots.

Reads  silver/ch_register/snapshot_date=*/part.parquet (every partition)
Writes gold/entity_history/snapshot_date=<latest>/part.parquet

What a row is: one VERSION of one company's tracked attributes, valid from the
snapshot it first appeared in until the snapshot it changed or left. The
tracked set is hashed into `row_hash`; a snapshot where the hash is unchanged
extends the open version rather than creating a new one, which is the whole
point of type-2 rather than a snapshot pile.

`change_type` values and what each one may be read as:

  baseline    the company was already on the register in the earliest snapshot
              we hold. NOT a creation event. The register goes back to 1844 and
              our history starts on 2026-07-01.
  new         absent in the previous snapshot, present in this one. For a
              company incorporated inside the window this is an incorporation;
              for one that was simply missing it is not. `incorporation_date`
              is carried on the row so the reader can tell which.
  change      present in both, tracked attributes differ.
  returned    the company was absent for at least one snapshot and came back.
              Its previous version is closed at the snapshot where it vanished,
              not at the one where it returned, because the register did not
              say it existed in between.

and separately, a boolean `gone_from_register`: the version's interval ended
because the row left the bulk file, not because anything about it changed. A
dissolved company leaves the file some months after dissolution, so `gone` is a
publication fact about the file, never a death date.

Two rules that come straight from DATA-INTEGRITY:

  * s3, CH register row: the register may assert legal existence and status. A
    version boundary is a change in what the register SAYS, not an event in the
    world. Nothing here may be captioned as a business opening or closing.
  * s4 rule 5: a source refresh gap is "no data", never "no change". If a
    snapshot is missing from silver, the gap is visible in `snapshot_seq`
    rather than papered over, and the interval is not silently extended across
    it: `snapshots_observed` counts the snapshots the version was actually seen
    in.
"""
import argparse
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import marts as M  # noqa: E402
import silver as SV  # noqa: E402

TABLE = "entity_history"

# The attributes a version is defined over. Deliberately NOT every column:
# accounts_next_due_date rolls forward on its own every year and would make
# every company change every snapshot, which would be a diff of the calendar
# rather than of the register.
TRACKED = [
    "company_name",
    "company_status",
    "company_category",
    "entity_type",
    "reg_postcode_norm",
    "accounts_category",
    "dissolution_date",
    "sic_text_1",
    "proposed_strike_off",
]


def _seq_to_date_case(parts, expr):
    """A SQL CASE mapping a snapshot sequence number back to its date."""
    arms = " ".join(f"WHEN {i} THEN DATE '{s}'" for i, (s, _) in enumerate(parts))
    return f"(CASE {expr} {arms} ELSE NULL END)"


def build(con, parts, out_dir, h=None):
    """parts = [(snapshot, parquet_path), ...] in ascending snapshot order."""
    tracked_sql = ", ".join(f"coalesce(CAST({c} AS VARCHAR), '')" for c in TRACKED)
    unions = []
    for seq, (snap, path) in enumerate(parts):
        unions.append(
            "SELECT company_number, "
            f"       DATE '{snap}' AS snapshot_date, {seq} AS snapshot_seq, "
            f"       md5(concat_ws('\\x1f', {tracked_sql})) AS row_hash, "
            + ", ".join(TRACKED) + ", "
            "       incorporation_date, companies_act_body "
            f"FROM read_parquet('{path}')")
    all_snaps = "\nUNION ALL\n".join(unions)

    # A version starts wherever the hash differs from the previous snapshot the
    # company was seen in, or where the company was not seen in the immediately
    # preceding snapshot at all (a re-appearance is a new version, not a
    # continuation: rule 4 of s4, a gap is not stability).
    sql = f"""
WITH snaps AS (
{all_snaps}
),
seq AS (
  SELECT *,
         lag(row_hash)     OVER w AS prev_hash,
         lag(snapshot_seq) OVER w AS prev_seq
  FROM snaps
  WINDOW w AS (PARTITION BY company_number ORDER BY snapshot_seq)
),
marked AS (
  SELECT *,
         CASE WHEN prev_seq IS NULL THEN 1
              WHEN prev_seq <> snapshot_seq - 1 THEN 1
              WHEN prev_hash IS DISTINCT FROM row_hash THEN 1
              ELSE 0 END AS is_new_version
  FROM seq
),
grouped AS (
  SELECT *,
         sum(is_new_version) OVER (PARTITION BY company_number
                                   ORDER BY snapshot_seq
                                   ROWS UNBOUNDED PRECEDING) AS version_no
  FROM marked
),
versions AS (
  SELECT company_number,
         version_no,
         min(snapshot_seq)  AS from_seq,
         max(snapshot_seq)  AS last_seen_seq,
         min(snapshot_date) AS valid_from,
         max(snapshot_date) AS last_seen_on,
         count(*)           AS snapshots_observed,
         any_value(row_hash) AS row_hash,
         {", ".join(f"any_value({c}) AS {c}" for c in TRACKED)},
         any_value(incorporation_date) AS incorporation_date,
         any_value(companies_act_body) AS companies_act_body
  FROM grouped
  GROUP BY company_number, version_no
),
closed AS (
  SELECT v.*,
         lead(valid_from) OVER w AS next_version_from,
         lead(from_seq)   OVER w AS next_from_seq,
         lag(last_seen_seq) OVER w AS prev_last_seen_seq
  FROM versions v
  WINDOW w AS (PARTITION BY company_number ORDER BY version_no)
),
flagged AS (
  SELECT *,
         -- the interval ended because the ROW LEFT the file, rather than
         -- because an attribute changed. True both for a terminal
         -- disappearance and for a company that later came back.
         CASE WHEN next_from_seq IS NOT NULL
                THEN last_seen_seq < next_from_seq - 1
              ELSE last_seen_seq < {len(parts) - 1} END AS gone_from_register
  FROM closed
)
SELECT company_number,
       CAST(version_no AS INTEGER) AS version_no,
       valid_from,
       -- open interval where the version is still current in the latest
       -- snapshot; closed at the snapshot after it was last seen where the row
       -- left the file; closed at the next version's snapshot otherwise.
       CASE WHEN gone_from_register THEN {_seq_to_date_case(parts, "last_seen_seq + 1")}
            WHEN next_version_from IS NOT NULL THEN next_version_from
            ELSE NULL END AS valid_to,
       CASE WHEN from_seq = 0 THEN 'baseline'
            WHEN version_no = 1 THEN 'new'
            WHEN prev_last_seen_seq < from_seq - 1 THEN 'returned'
            ELSE 'change' END AS change_type,
       gone_from_register,
       snapshots_observed,
       row_hash,
       {", ".join(TRACKED)},
       incorporation_date,
       companies_act_body,
       DATE '{parts[-1][0]}' AS snapshot_date
FROM flagged
ORDER BY company_number, version_no
"""
    rows, nbytes = M.write_parquet(con, sql, out_dir)
    return rows, nbytes


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--home", default=None)
    args = ap.parse_args()
    h = args.home

    base = SV.silver_dir(h) / "ch_register"
    parts = []
    for p in sorted(base.glob("snapshot_date=*")):
        f = p / "part.parquet"
        if f.exists():
            parts.append((p.name.split("=", 1)[1], f))
    if len(parts) < 2:
        raise SystemExit(
            f"FATAL: entity_history needs at least two register snapshots, "
            f"silver has {len(parts)}. An SCD2 table built from one snapshot "
            "is a snapshot wearing a costume.")
    M.log(f"snapshots: {[s for s, _ in parts]}")

    con, ver = M.connect()
    latest = parts[-1][0]
    out_dir = M.table_dir(TABLE, latest, h)
    rows, nbytes = build(con, parts, out_dir, h)

    q = lambda s: con.execute(s).fetchone()[0]  # noqa: E731
    f = out_dir / "part.parquet"
    counts = {
        ct: q(f"SELECT count(*) FROM read_parquet('{f}') WHERE change_type = '{ct}'")
        for ct in ("baseline", "new", "change", "returned")
    }
    counts["gone"] = q(
        f"SELECT count(*) FROM read_parquet('{f}') WHERE gone_from_register")
    companies = q(f"SELECT count(DISTINCT company_number) FROM read_parquet('{f}')")
    M.log(f"entity_history: {rows} versions over {companies} companies")
    M.log(f"  {counts}")

    # Invariant: consecutive versions of a company abut exactly, EXCEPT where
    # the row left the file in between. That gap is real data (s4 rule 5) and
    # is the one hole the table is allowed to have; any other hole means the
    # table answers "what did the register say on date X" with silence.
    holes = q(f"""
      SELECT count(*) FROM (
        SELECT company_number, version_no, valid_to, gone_from_register,
               lead(valid_from) OVER (PARTITION BY company_number
                                      ORDER BY version_no) AS nxt
        FROM read_parquet('{f}')
      ) WHERE nxt IS NOT NULL AND NOT gone_from_register
          AND valid_to IS DISTINCT FROM nxt
    """)
    SV.assert_equal("interval_contiguity_holes", holes, 0)

    inputs = [{"table": "silver/ch_register", "snapshot": s} for s, _ in parts]
    M.write_manifest(
        out_dir, TABLE, latest, rows, nbytes, ver, inputs=inputs,
        assertions={"changeTypeCounts": counts, "companies": companies,
                    "intervalHoles": holes},
        notes=("SCD2 over the tracked register attributes. 'baseline' is not a "
               "creation and 'gone' is not a dissolution: both are facts about "
               "which snapshot the row appeared in. Tracked attributes exclude "
               "the rolling due dates, which change on the calendar rather "
               "than on the register."),
        extra={"trackedAttributes": TRACKED})
    print(json.dumps({"rows": rows, "companies": companies, **counts}, indent=1))


if __name__ == "__main__":
    main()
