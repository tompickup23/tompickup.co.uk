#!/usr/bin/env python3
"""Silver: The Gazette notices, Lancashire candidate set.

bronze/source=gazette_notices/snapshot_date=.../
    gazette_corporate_all.jsonl   candidate summaries from the geo circles
    gazette_lancs.json            the resolved, postcode-confirmed subset
  -> silver/gazette_notices/snapshot_date=.../part.parquet

**The category filter is not optional and it is not the fetcher's.** The fetcher
asks the feed for category-code=24 (corporate insolvency), but the feed does not
honour that parameter when the location parameters are present, so the candidate
file also contains category 25 personal insolvency, category 29 deceased estates
and category 16 planning notices, all naming private individuals. Personal
insolvency is excluded entirely by legal rule. This builder therefore drops
every notice whose code is outside category 24 by explicit rule and asserts the
dropped counts, exactly like the Companies House prefix exclusions. Nothing
downstream may re-admit them.

What a row may assert, per DATA-INTEGRITY s3: the notice fact, verbatim and
dated. Not that a company is insolvent beyond what the notice says, and nothing
whatsoever about a person.

Usage:
    build_silver_gazette.py [--snapshot 2026-08-16]
"""
import argparse
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import silver as SV  # noqa: E402
import entity_type as ET  # noqa: E402

TABLE = "gazette_notices"
KEEP_CATEGORY = "24"          # corporate insolvency, including strike-off notices
PERSONAL_CATEGORY = "25"      # personal insolvency: excluded entirely, legal rule

# Strike-off notice codes inside category 24. First and final Gazette notices,
# voluntary and compulsory. Kept as an explicit list so a new code lands as
# "not a strike-off" rather than being swept in by a range guess.
STRIKE_OFF_CODES = ("2431", "2432", "2433", "2441", "2443", "2450", "2452",
                    "2454", "2461")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--snapshot")
    args = ap.parse_args()

    snap, part, manifest = SV.resolve_bronze("gazette_notices", args.snapshot)
    cand, cand_sha = SV.bronze_file(manifest, part, "gazette_corporate_all.jsonl")
    lancs_path, lancs_sha = SV.bronze_file(manifest, part, "gazette_lancs.json")
    lancs = json.loads(lancs_path.read_text())
    resolved_ids = {str(n["notice_id"]) for n in lancs["notices"]}
    SV.log(f"bronze snapshot_date={snap}, {len(resolved_ids)} resolved "
           "Lancashire notices in the published subset")

    con, dv = SV.connect()
    # The published notices file is in candidate-file order and build_site_json
    # dedupes on (company_number, type, date) keeping the first it meets, so
    # the order decides which notice_id and URI get published.
    con.execute("SET preserve_insertion_order=true")
    SV.log(f"duckdb {dv}")

    read = f"""
    read_json('{cand}', format='newline_delimited', columns={{
      'notice_id': 'VARCHAR', 'company_name': 'VARCHAR',
      'notice_code': 'VARCHAR', 'date': 'VARCHAR', 'uri': 'VARCHAR',
      'matched_circle': 'VARCHAR', 'company_number': 'VARCHAR',
      'reg_office_postcode': 'VARCHAR', 'insolvency_type': 'VARCHAR'
    }})
    """
    con.execute("CREATE VIEW cand AS SELECT *, row_number() OVER () "
                f"AS file_ordinal FROM {read}")
    total = con.execute("SELECT count(*) FROM cand").fetchone()[0]

    dropped = dict(con.execute("""
        SELECT substr(coalesce(notice_code, '__'), 1, 2) AS cat, count(*)
        FROM cand
        WHERE substr(coalesce(notice_code, '__'), 1, 2) <> '24'
        GROUP BY 1 ORDER BY 2 DESC
    """).fetchall())
    personal = dropped.get(PERSONAL_CATEGORY, 0)
    SV.log(f"  {total:,} candidates, dropping {sum(dropped.values()):,} outside "
           f"category 24: {dropped}")
    SV.log(f"  of those, {personal:,} are category 25 personal insolvency, "
           "excluded by legal rule")

    # Deduplicate: overlapping geo circles return the same notice more than
    # once. The circle that found it is kept as a list rather than picking one,
    # so the provenance is not quietly narrowed.
    codes = ", ".join(f"'{c}'" for c in STRIKE_OFF_CODES)
    ids = ", ".join(f"'{i}'" for i in sorted(resolved_ids)) or "''"
    select = f"""
    SELECT
      notice_id,
      any_value(nullif(trim(company_name), ''))     AS company_name,
      any_value(notice_code)                        AS notice_code,
      substr(any_value(notice_code), 1, 2)          AS notice_category,
      try_strptime(any_value(date), '%Y-%m-%d')::DATE AS notice_date,
      try_strptime(any_value(date), '%Y-%m-%d')::DATE AS as_at,
      any_value(uri)                                AS uri,
      list_sort(list_distinct(list(matched_circle))) AS matched_circles,
      any_value(nullif(trim(company_number), ''))   AS company_number,
      -- The number EXACTLY as The Gazette published it. 15 of the 10,644
      -- candidate notices carry a trailing space, and the live site joins the
      -- register on the raw string, so those notices silently fail to match a
      -- company we hold and render with the notice's own LAD and no sector.
      -- That is a live fault (M4 finding, DATA-INTEGRITY s11.10). Silver holds
      -- both: the trimmed number is the one to join on, the raw one is what
      -- lets the gold mart reproduce the published edition.
      any_value(nullif(company_number, ''))         AS company_number_raw,
      regexp_matches(upper(replace(coalesce(any_value(trim(company_number)), ''),
                                   ' ', '')), '{ET.CRN_SHAPE_RE}')
        AS company_number_is_crn,
      any_value(nullif(trim(reg_office_postcode), '')) AS reg_office_postcode,
      any_value(nullif(trim(insolvency_type), ''))  AS insolvency_type,
      (any_value(notice_code) IN ({codes}))         AS is_strike_off,
      (notice_id IN ({ids}))                        AS in_published_lancs_subset,
      -- the earliest position this notice appeared at in the candidate file.
      -- gazette_lancs.json is written in that order and build_site_json dedupes
      -- on (company_number, type, date) keeping the first it meets, so the
      -- order decides which notice_id and URI reach the site.
      min(file_ordinal)                             AS file_ordinal,
      DATE '{snap}'                                 AS snapshot_date,
      '{cand_sha}'                                  AS source_sha256
    FROM cand
    WHERE substr(coalesce(notice_code, '__'), 1, 2) = '{KEEP_CATEGORY}'
    GROUP BY notice_id
    ORDER BY min(file_ordinal)
    """

    out = SV.table_dir(TABLE, snap)
    SV.log("writing parquet")
    rows, nbytes = SV.write_parquet(con, select, out)
    SV.log(f"  {rows:,} rows, {nbytes/1e6:.1f} MB")

    pq = out / "part.parquet"
    leaked = con.execute(f"""
        SELECT count(*) FROM read_parquet('{pq}') WHERE notice_category <> '24'
    """).fetchone()[0]
    if leaked:
        raise SystemExit(
            f"FATAL: {leaked} notices outside category 24 reached silver. "
            "Personal insolvency is excluded entirely by legal rule.")

    strike = con.execute(
        f"SELECT count(*) FROM read_parquet('{pq}') WHERE is_strike_off").fetchone()[0]
    with_crn = con.execute(
        f"SELECT count(*) FROM read_parquet('{pq}') WHERE company_number_is_crn"
    ).fetchone()[0]
    published_hit = con.execute(
        f"SELECT count(*) FROM read_parquet('{pq}') WHERE in_published_lancs_subset"
    ).fetchone()[0]
    SV.assert_equal("published Lancashire subset reachable from silver",
                    published_hit, len(resolved_ids))

    SV.write_manifest(
        out, TABLE, snap,
        inputs=[{"layer": "bronze", "source": "gazette_notices",
                 "snapshotDate": snap, "file": cand.name, "sha256": cand_sha},
                {"layer": "bronze", "source": "gazette_notices",
                 "snapshotDate": snap, "file": lancs_path.name,
                 "sha256": lancs_sha}],
        rows=rows, nbytes=nbytes, duckdb_version=dv,
        assertions={"candidateRows": total,
                    "rows": rows,
                    "droppedByCategory": dropped,
                    "droppedPersonalInsolvency": personal,
                    "duplicatesCollapsed": total - sum(dropped.values()) - rows,
                    "strikeOffNotices": strike,
                    "withParsableCrn": with_crn,
                    "inPublishedLancsSubset": published_hit},
        notes=("Category 24 only. The feed ignores category-code=24 when the "
               "location parameters are present, so the candidate file carries "
               "personal insolvency, deceased estates and planning notices; "
               "they are dropped here by explicit code rule and the counts are "
               "asserted. A row asserts the notice fact and nothing further."))
    con.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
