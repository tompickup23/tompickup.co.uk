#!/usr/bin/env python3
"""Silver: Companies House persons with significant control.

bronze/source=ch_psc_extract/snapshot_date=.../
    lancs_psc.jsonl.gz          every PSC kind for Lancashire-registered companies
    corporate_psc_all.jsonl.gz  every corporate-entity PSC nationally
    psc_summary.json            the line counts we assert against
  ->
    silver/ch_psc/snapshot_date=.../part.parquet
    silver/ch_psc_corporate/snapshot_date=.../part.parquet

Two tables rather than one, because they are two populations with a partial
overlap (a Lancashire company's corporate PSC appears in both). Unioning them
would double-count exactly the rows that matter most, which is the failure mode
already live elsewhere in the estate.

What this layer may and may not say, per DATA-INTEGRITY s3: a PSC row evidences
control or ownership as registered. It does not evidence economic activity and
the residence field is not a location of operations. The country_of_residence
and postcode columns are matching keys, never a geography.

Usage:
    build_silver_ch_psc.py [--snapshot 2026-08-16]
"""
import argparse
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import silver as SV  # noqa: E402
import entity_type as ET  # noqa: E402


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--snapshot")
    args = ap.parse_args()

    snap, part, manifest = SV.resolve_bronze("ch_psc_extract", args.snapshot)
    lancs, lancs_sha = SV.bronze_file(manifest, part, "lancs_psc.jsonl.gz")
    corp, corp_sha = SV.bronze_file(manifest, part, "corporate_psc_all.jsonl.gz")
    summary_path, summary_sha = SV.bronze_file(manifest, part, "psc_summary.json")
    summary = json.loads(summary_path.read_text())
    SV.log(f"bronze snapshot_date={snap}, extractor snapshot "
           f"{summary.get('snapshot_date')}")

    con, dv = SV.connect()
    # file_ordinal is the row's position in the extractor's output, and it is
    # load-bearing rather than cosmetic: build_pound.py calls parents[crn][0]
    # "the primary corporate parent", which is simply whichever corporate PSC
    # row the file happens to list first. Without the ordinal the warehouse
    # cannot reproduce which parent the site publishes. See the M4 finding in
    # DATA-INTEGRITY s11.12.
    con.execute("SET preserve_insertion_order=true")
    SV.log(f"duckdb {dv}")
    inputs_common = {"layer": "bronze", "source": "ch_psc_extract",
                     "snapshotDate": snap}

    # --- ch_psc: Lancashire, every kind -----------------------------------
    # (company, kind, name, ceased_on) is NOT a key and asserting it as one
    # fails on real data: a person can hold two notified interests at once, and
    # 339 people in this snapshot do. The rows differ on natures_of_control,
    # postcode or country of residence and are all genuine. What IS assertable
    # is that no complete row repeats, so the builder takes DISTINCT over every
    # value column and reports how many exact copies that removed. An exact
    # copy carries no information and can only come from a double read.
    lancs_typed = f"""
    SELECT
      company_number,
      kind,
      nullif(trim(name), '')                       AS name,
      nullif(trim(name_elements.title), '')        AS name_title,
      nullif(trim(name_elements.forename), '')     AS name_forename,
      nullif(trim(name_elements.middle_name), '')  AS name_middle,
      nullif(trim(name_elements.surname), '')      AS name_surname,
      nullif(trim(nationality), '')                AS nationality,
      nullif(trim(country_of_residence), '')       AS country_of_residence,
      nullif(trim(postcode), '')                   AS postcode,
      upper(replace(coalesce(postcode, ''), ' ', '')) AS postcode_norm,
      list_sort(natures_of_control)                AS natures_of_control,
      len(coalesce(natures_of_control, []))        AS natures_count,
      try_strptime(nullif(trim(ceased_on), ''), '%Y-%m-%d')::DATE AS ceased_on,
      (nullif(trim(ceased_on), '') IS NULL)        AS active,
      (kind LIKE 'individual-%')                   AS is_individual,
      (kind LIKE 'corporate-entity-%')             AS is_corporate,
      (kind LIKE '%beneficial-owner')              AS is_beneficial_owner,
      DATE '{snap}'                                AS snapshot_date,
      '{lancs_sha}'                                AS source_sha256,
      file_ordinal
    FROM (SELECT *, row_number() OVER () AS file_ordinal
          FROM read_json('{lancs}', format='newline_delimited',
                   columns={{
                     'company_number': 'VARCHAR', 'kind': 'VARCHAR',
                     'name': 'VARCHAR',
                     'name_elements': 'STRUCT(forename VARCHAR, middle_name VARCHAR, surname VARCHAR, title VARCHAR)',
                     'nationality': 'VARCHAR', 'country_of_residence': 'VARCHAR',
                     'postcode': 'VARCHAR', 'natures_of_control': 'VARCHAR[]',
                     'ceased_on': 'VARCHAR'
                   }}))
    """
    # DISTINCT over every value column, keeping the EARLIEST position each
    # distinct row appeared at. An exact duplicate carries no information and
    # can only come from a double read, but its position does.
    lancs_sql = (f"SELECT * EXCLUDE (file_ordinal), "
                 f"min(file_ordinal) AS file_ordinal "
                 f"FROM ({lancs_typed}) GROUP BY ALL ORDER BY file_ordinal")
    raw_l = con.execute(f"SELECT count(*) FROM ({lancs_typed})").fetchone()[0]
    SV.assert_equal("lancs_psc rows as extracted", raw_l, summary["lancs_psc"])

    out_l = SV.table_dir("ch_psc", snap)
    SV.log("writing ch_psc")
    rows_l, bytes_l = SV.write_parquet(con, lancs_sql, out_l)
    exact_copies = raw_l - rows_l
    SV.log(f"  {rows_l:,} rows, {bytes_l/1e6:.1f} MB, {exact_copies:,} exact "
           "duplicate rows removed")

    pq_l = out_l / "part.parquet"
    kinds_l = dict(con.execute(
        f"SELECT kind, count(*) FROM read_parquet('{pq_l}') GROUP BY 1").fetchall())
    multi_interest = con.execute(f"""
        SELECT count(*) FROM (
          SELECT company_number, kind, name FROM read_parquet('{pq_l}')
          GROUP BY 1, 2, 3 HAVING count(*) > 1)
    """).fetchone()[0]
    SV.log(f"  {multi_interest:,} people hold more than one notified interest "
           "in the same company")
    SV.write_manifest(
        out_l, "ch_psc", snap,
        inputs=[dict(inputs_common, file=lancs.name, sha256=lancs_sha),
                dict(inputs_common, file=summary_path.name, sha256=summary_sha)],
        rows=rows_l, nbytes=bytes_l, duckdb_version=dv,
        assertions={"rowsAsExtracted": raw_l,
                    "expectedFromExtractorSummary": summary["lancs_psc"],
                    "rows": rows_l,
                    "exactDuplicateRowsRemoved": exact_copies,
                    "multipleInterestsSamePerson": multi_interest,
                    "byKind": {k: v for k, v in sorted(kinds_l.items())},
                    "active": con.execute(
                        f"SELECT count(*) FROM read_parquet('{pq_l}') WHERE active"
                    ).fetchone()[0]},
        notes=("Every PSC kind for Lancashire-registered companies. Control as "
               "registered only: not activity, and country_of_residence is not "
               "a location of operations (DATA-INTEGRITY s3)."),
        extra={"extractorSnapshotDate": summary.get("snapshot_date")})

    # --- ch_psc_corporate: national corporate-entity PSC ------------------
    corp_sql = f"""
    SELECT
      company_number,
      nullif(trim(name), '')                        AS psc_name,
      nullif(trim(registration_number), '')         AS registration_number,
      upper(replace(coalesce(trim(registration_number), ''), ' ', ''))
        AS registration_number_norm,
      regexp_matches(
        upper(replace(coalesce(trim(registration_number), ''), ' ', '')),
        '{ET.CRN_SHAPE_RE}')                        AS registration_is_crn_shaped,
      (regexp_matches(
         upper(replace(coalesce(trim(registration_number), ''), ' ', '')),
         '{ET.CRN_SHAPE_RE}')
       AND upper(replace(coalesce(trim(registration_number), ''), ' ', ''))
           = company_number)                        AS self_reference,
      nullif(trim(country_registered), '')          AS country_registered,
      nullif(trim(legal_form), '')                  AS legal_form,
      nullif(trim(postcode), '')                    AS postcode,
      upper(replace(coalesce(postcode, ''), ' ', '')) AS postcode_norm,
      try_strptime(nullif(trim(ceased_on), ''), '%Y-%m-%d')::DATE AS ceased_on,
      (nullif(trim(ceased_on), '') IS NULL)         AS active,
      DATE '{snap}'                                 AS snapshot_date,
      '{corp_sha}'                                  AS source_sha256,
      file_ordinal
    FROM (SELECT *, row_number() OVER () AS file_ordinal
          FROM read_json('{corp}', format='newline_delimited',
                   columns={{
                     'company_number': 'VARCHAR', 'name': 'VARCHAR',
                     'registration_number': 'VARCHAR',
                     'country_registered': 'VARCHAR', 'legal_form': 'VARCHAR',
                     'postcode': 'VARCHAR', 'ceased_on': 'VARCHAR'
                   }}))
    ORDER BY file_ordinal
    """
    out_c = SV.table_dir("ch_psc_corporate", snap)
    SV.log("writing ch_psc_corporate")
    rows_c, bytes_c = SV.write_parquet(con, corp_sql, out_c)
    SV.log(f"  {rows_c:,} rows, {bytes_c/1e6:.1f} MB")
    SV.assert_equal("corporate_psc rows", rows_c, summary["corporate_psc_all"])

    pq_c = out_c / "part.parquet"
    crn_ok = con.execute(
        f"SELECT count(*) FROM read_parquet('{pq_c}') "
        "WHERE registration_is_crn_shaped").fetchone()[0]
    selfref = con.execute(
        f"SELECT count(*) FROM read_parquet('{pq_c}') WHERE self_reference"
    ).fetchone()[0]
    SV.log(f"  registration_number is CRN-shaped in {crn_ok:,} of {rows_c:,} "
           f"rows ({100.0*crn_ok/rows_c:.1f} percent)")
    SV.log(f"  {selfref:,} rows name the controlled company as its own PSC")
    SV.write_manifest(
        out_c, "ch_psc_corporate", snap,
        inputs=[dict(inputs_common, file=corp.name, sha256=corp_sha),
                dict(inputs_common, file=summary_path.name, sha256=summary_sha)],
        rows=rows_c, nbytes=bytes_c, duckdb_version=dv,
        assertions={"rows": rows_c,
                    "expectedFromExtractorSummary": summary["corporate_psc_all"],
                    "registrationIsCrnShaped": crn_ok,
                    "selfReferencingEdges": selfref},
        notes=("National corporate-entity PSC rows, the group-structure input "
               "for M3. registration_is_crn_shaped is a SHAPE test and nothing "
               "more: a registered society number matches the same pattern, so "
               "the flag narrows the join candidates and never proves a "
               "company (gate V-T4). self_reference marks rows where a company "
               "is filed as its own PSC, which is a source error, kept and "
               "flagged rather than dropped."),
        extra={"extractorSnapshotDate": summary.get("snapshot_date")})

    con.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
