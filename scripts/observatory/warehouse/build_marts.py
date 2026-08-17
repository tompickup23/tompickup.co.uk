#!/usr/bin/env python3
"""M4: the gold marts the site is re-emitted from.

Each mart stands in for one step of the OLD fetch/ETL layer. Nothing here
aggregates: build_master, build_growth, build_pound, build_dossiers,
build_overlay, build_site_json and build_diff all run unchanged on top, which
is the only way the golden-file test measures the warehouse rather than my
typing.

| mart                     | replaces                        | reads          |
|--------------------------|---------------------------------|----------------|
| mart_register_lancs      | refresh_register.py, Lancs half | silver+bronze  |
| mart_register_index      | refresh_register.py, index half | silver         |
| mart_accounts_lancs      | etl_accounts.py output          | silver         |
| mart_psc_lancs           | refresh_psc.py, Lancs half      | silver         |
| mart_psc_corporate       | refresh_psc.py, corporate half  | silver         |
| mart_notices_lancs       | fetch_gazette.py, filter half   | silver+bronze  |
| mart_supplier_identifiers| fetch_ocds_ids.py               | gold crosswalk |

**These marts reproduce the PRODUCTION basis.** Each one that carries a known
fault says so in a comment naming its DATA-INTEGRITY section and records it in
`reproducedFaults` in its manifest; a fault that has been fixed moves to
`clearedFaults` in the same manifest, with the commit and the count it moved,
so the register never loses an entry.

STILL REPRODUCED:

  F1 (s7.1)  the Lancashire register frame counts overseas establishments and
             overseas entities as companies. The frame is "every register row
             whose registered-office postcode is in one of the 14 LADs", with
             no entity-type filter, so 8 FC and 3 OE rows are inside the
             103,468. DATA-INTEGRITY says headline company counts exclude both.
  F4 (s9.1)  the register summary counts 5,695,466 UK companies, which is a
             line count: one record spans two lines and one line is blank. The
             record count is 5,695,465. Not published, so not load-bearing, but
             the index the supplier matcher reads has the same extra blank row.

CLEARED, each recorded in `clearedFaults` on the mart that carried it:

  F2 (s11.2) the OCDS supplier identifier map was empty on the host that runs
             the monthly cron. fetch_ocds_ids.py now talks to the Contracts
             Finder API directly and the projection emits the crosswalk's full
             set, so the two paths carry the same identifications.
  F3 (s11.9) the accounts series published the ORIGINAL filing rather than the
             restatement, because the extractor appended its 13-month backfill
             in reverse chronological order and the consumers kept the last
             line. Both consumers now resolve a period by filing date.
  F5 (s11.10) published Gazette notices carried a company number with a
             trailing space and the site joined the register on the raw
             string. The projection emits the trimmed number silver derives.
  F6 (s11.12) build_pound.py called parents[crn][0] "the primary corporate
             parent", which was whichever row the extract listed first. It now
             applies a stated selection rule that reads no file position.

Row ORDER is still part of what a mart carries: the register frame feeds a
truncated published list and the accounts stream is read as a stream, which is
why `file_ordinal` exists on the accounts, PSC and notices marts and why the
register mart is written in the CH bulk file's own name order. Preserving it is
what lets a pre-fix edition be reproduced on demand.

Usage:
    build_marts.py [--only mart_register_lancs] [--snapshot 2026-08-01]
"""
import argparse
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import marts as M  # noqa: E402
import silver as SV  # noqa: E402
import crosswalk as XW  # noqa: E402

BUILDERS = {}


def mart(name):
    def deco(fn):
        BUILDERS[name] = fn
        return fn
    return deco


# Snapshot pins, set from --pin on the command line. The clock is an input
# (DATA-INTEGRITY s12.4) and so is every snapshot: reproducing a published
# edition means reading the partitions that edition read, not the newest ones
# that happen to be on disk. M5 found this the honest way, when a second
# gazette snapshot landed between the M4 golden run and the first driver cycle
# and the marts silently moved to it.
_PINS = {}


def _silver(table, snapshot=None, h=None):
    """The silver partition for a table: the pin, else the named snapshot,
    else the latest."""
    snapshot = snapshot or _PINS.get(table)
    base = SV.silver_dir(h) / table
    parts = sorted(p for p in base.glob("snapshot_date=*")
                   if (p / "part.parquet").exists())
    if not parts:
        raise SystemExit(f"FATAL: silver has no partition for {table}")
    if snapshot:
        want = base / f"snapshot_date={snapshot}"
        if want not in parts:
            raise SystemExit(
                f"FATAL: silver/{table} has no snapshot_date={snapshot}; "
                f"present {[p.name for p in parts]}")
        chosen = want
    else:
        chosen = parts[-1]
    return chosen.name.split("=", 1)[1], chosen / "part.parquet"


# --------------------------------------------------------------- register ---

@mart("mart_register_lancs")
def build_register_lancs(con, h, args):
    """The Lancashire register frame the site is built on.

    Verbatim reproduction of refresh_register.py: every register row whose
    registered-office postcode resolves through ONSPD to one of the 14 target
    LADs, in every status, with no entity-type filter.

    REPRODUCED FAULT F1 (DATA-INTEGRITY s7.1): no entity-type filter means the
    frame counts overseas establishments (FC) and overseas entities (OE) as
    companies. `companies_act_body` is carried on every row so the fix is a
    WHERE clause, but applying it here would move a published figure.
    """
    snap, pq = _silver("ch_register", args.snapshot, h)
    onspd_sql, onspd_snap, onspd_path, onspd_files = M.onspd_lookup_sql(h)
    lads = ", ".join(f"('{k}', '{v}')" for k, v in M.TARGET_LADS.items())
    d = M.ch_date_str
    sql = f"""
    WITH pc AS ({onspd_sql}),
         target(lad_code, lad_name) AS (VALUES {lads})
    SELECT r.company_number,
           r.company_name,
           r.reg_postcode                       AS reg_postcode,
           t.lad_code,
           t.lad_name,
           r.company_category,
           r.company_status,
           {d('r.dissolution_date')}            AS dissolution_date,
           {d('r.incorporation_date')}          AS incorporation_date,
           coalesce(r.accounts_category, '')    AS accounts_category,
           {d('r.accounts_next_due_date')}      AS accounts_next_due_date,
           {d('r.accounts_last_made_up_date')}  AS accounts_last_made_up_date,
           coalesce(r.sic_text_1, '')           AS sic_text_1,
           coalesce(r.sic_text_2, '')           AS sic_text_2,
           coalesce(r.sic_text_3, '')           AS sic_text_3,
           coalesce(r.sic_text_4, '')           AS sic_text_4,
           -- production tests the CATEGORY string case-insensitively; s7.3
           -- says category is the only route to a CIC because no number
           -- prefix distinguishes one.
           lower(coalesce(r.company_category, ''))
             LIKE '%community interest company%' AS is_cic,
           r.entity_type,
           r.companies_act_body,
           r.number_prefix,
           r.snapshot_date,
           r.source_sha256
    FROM read_parquet('{pq}') r
    JOIN pc  ON pc.postcode_norm = r.reg_postcode_norm
    JOIN target t ON t.lad_code = pc.lad_code
    -- The CH bulk file is sorted by CompanyName in codepoint order and the
    -- production ETL streams it, so master.jsonl.gz inherits that order. It is
    -- load-bearing: biz-watch.json publishes the FIRST 1,000 strike-off
    -- proposals it meets, so the order decides which thousand. Verified on the
    -- published edition: 103,468 rows, name-ordered, zero duplicate names, so
    -- the ordering is total and reproducible without a file ordinal.
    ORDER BY r.company_name
    """
    out = M.table_dir("mart_register_lancs", snap, h)
    rows, nbytes = M.write_parquet(con, sql, out)
    f = out / "part.parquet"
    q = lambda s: con.execute(s).fetchone()[0]  # noqa: E731
    cics = q(f"SELECT count(*) FROM read_parquet('{f}') WHERE is_cic")
    non_ca = q(f"SELECT count(*) FROM read_parquet('{f}') "
               "WHERE NOT companies_act_body")
    by_type = dict(con.execute(
        f"SELECT entity_type, count(*) FROM read_parquet('{f}') "
        "GROUP BY 1 ORDER BY 2 DESC").fetchall())
    M.log(f"  {rows:,} rows, {cics} CICs, {non_ca} not Companies Act bodies")
    M.log(f"  entity types: {by_type}")

    # The site's own register_summary.json is the production baseline. If the
    # mart and the live ETL disagree on the frame, everything downstream is
    # noise, so this is asserted before anything else runs.
    baseline = _production_register_summary()
    if baseline:
        SV.assert_equal("lancs frame vs production register_summary",
                        rows, baseline["lancs_companies"])
        SV.assert_equal("CIC count vs production register_summary",
                        cics, baseline["cic_count"])
        for lad, n in baseline["per_lad"].items():
            got = q(f"SELECT count(*) FROM read_parquet('{f}') "
                    f"WHERE lad_name = '{lad}'")
            SV.assert_equal(f"per-LAD {lad}", got, n)

    return dict(
        table="mart_register_lancs", snapshot=snap, rows=rows, nbytes=nbytes,
        inputs=[{"layer": "silver", "table": "ch_register", "snapshot": snap},
                {"layer": "bronze", "source": "onspd", "snapshot": onspd_snap,
                 "files": onspd_files}],
        assertions={"rows": rows, "cics": cics,
                    "notCompaniesActBodies": non_ca, "entityTypes": by_type},
        reproduced_faults=[
            {"id": "F1", "ref": "DATA-INTEGRITY s7.1",
             "what": "the Lancashire frame has no entity-type filter, so "
                     "overseas establishments and overseas entities are "
                     "counted as companies",
             "rowsAffected": non_ca,
             "fixIsOneClause": "WHERE companies_act_body"}],
        notes=("The production Lancashire frame: postcode to LAD through "
               "ONSPD, 14 target LADs, every status, no entity-type filter. "
               "This is the 103,468 basis, not the 131,961 postcode-prefix "
               "one; which is right is F1's question (s11.8) and neither is "
               "publishable as a company count until then."))


def _production_register_summary():
    for p in (Path("/opt/observatory/out/register_summary.json"),
              Path.home() / "observatory-data/vps/register_summary.json"):
        if p.exists():
            return json.loads(p.read_text())
    return None


@mart("mart_register_index")
def build_register_index(con, h, args):
    """The national slim name index the supplier matcher searches.

    REPRODUCED FAULT F4 (DATA-INTEGRITY s9.1): production writes one row per
    CSV LINE, so the blank line 3,004,677 becomes an all-empty index row. Silver
    holds records, not lines, so the mart is one row short of production. The
    empty row is unreachable: build_pound only ever looks up normalised
    supplier names, which are never empty, and prefix_unique requires a key of
    at least 12 characters. The difference is recorded rather than faked.
    """
    snap, pq = _silver("ch_register", args.snapshot, h)
    lancs_pq = M.mart_dir(h) / "mart_register_lancs" / f"snapshot_date={snap}" \
        / "part.parquet"
    sql = f"""
    SELECT r.company_number,
           -- tabs and newlines are spaces in the TSV, exactly as production
           -- writes it, because the consumer splits on tabs with no quoting.
           replace(replace(r.company_name, chr(9), ' '), chr(10), ' ')
             AS company_name,
           coalesce(r.reg_postcode, '') AS reg_postcode,
           r.company_status,
           (l.company_number IS NOT NULL) AS in_lancs_frame,
           r.snapshot_date
    FROM read_parquet('{pq}') r
    LEFT JOIN read_parquet('{lancs_pq}') l USING (company_number)
    """
    out = M.table_dir("mart_register_index", snap, h)
    rows, nbytes = M.write_parquet(con, sql, out)
    baseline = _production_register_summary()
    line_count = baseline["total_uk_companies"] if baseline else None
    M.log(f"  {rows:,} records ({line_count} lines in the production index)")
    return dict(
        table="mart_register_index", snapshot=snap, rows=rows, nbytes=nbytes,
        inputs=[{"layer": "silver", "table": "ch_register", "snapshot": snap}],
        assertions={"records": rows, "productionLineCount": line_count},
        reproduced_faults=[
            {"id": "F4", "ref": "DATA-INTEGRITY s9.1",
             "what": "the production index writes one row per CSV line, so a "
                     "blank line becomes an empty index row and the count is "
                     "5,695,466 rather than 5,695,465 records",
             "rowsAffected": (line_count - rows) if line_count else None,
             "reachable": False}],
        notes="The national name index. Every register row, records not lines.")


# --------------------------------------------------------------- accounts ---

@mart("mart_accounts_lancs")
def build_accounts_lancs(con, h, args):
    """EVERY filing, in the order the site reads them, with both winners flagged.

    The mart holds one row per FILING, not per period, for a reason that only
    showed up in the golden diff: the two consumers resolve a repeated period
    DIFFERENTLY. build_growth.py writes `series[crn][pe] = employees` only when
    employees is not null, so a later filing with a null headcount does not
    overwrite an earlier real one; build_dossiers.py overwrites the whole
    record every line, nulls included. One pre-resolved row per period cannot
    satisfy both, so the mart keeps the stream and lets each consumer resolve
    it exactly as it does today. That is also what DATA-INTEGRITY 10.4 rule 4
    asks for: every edition is kept.

    F3 (DATA-INTEGRITY s11.9) CLEARED. build_growth.py and build_dossiers.py
    kept the LAST line for a (crn, period_end), and the extractor appended its
    13-month backfill in reverse chronological order, so for those periods the
    site published the ORIGINAL filing rather than the restatement. Both now
    resolve a period through accounts_rules.resolve_latest, which picks the
    filing made most recently. `site_winner` still marks what the old rule
    picked and `latest_winner` what the new one picks, because the difference
    between them is the measured size of the fix.

    The two rows silver rejects for an impossible period (0001-01-01, s9.6) are
    unioned back in here, flagged, because the live pipeline reads them and
    they change two companies' growth series.
    """
    snap, pq = _silver("ch_accounts", None, h)
    parts = [f"SELECT *, false AS impossible_period FROM read_parquet('{pq}')"]
    sup = pq.parent / "_superseded.parquet"
    if sup.exists():
        parts.append("SELECT *, false AS impossible_period FROM "
                     f"read_parquet('{sup}')")
    rej = pq.parent / "_rejected.parquet"
    if rej.exists():
        parts.append("SELECT * EXCLUDE (reject_reason), true AS "
                     f"impossible_period FROM read_parquet('{rej}')")
    all_sql = "\nUNION ALL BY NAME\n".join(parts)
    sql = f"""
    WITH all_filings AS (
{all_sql}
    ),
    ranked AS (
      SELECT *,
             -- the site's rule: last line in the concatenated stream wins
             row_number() OVER (PARTITION BY crn, period_end
                                ORDER BY file_ordinal DESC) AS site_rank,
             -- the correct rule: chronologically latest filing wins
             row_number() OVER (PARTITION BY crn, period_end
                                ORDER BY filed_month_date DESC NULLS LAST,
                                         file_ordinal DESC) AS correct_rank,
             count(*) OVER (PARTITION BY crn, period_end) AS filings_for_period
      FROM all_filings
    )
    SELECT * EXCLUDE (site_rank, correct_rank),
           (site_rank = 1)   AS site_winner,
           (correct_rank = 1) AS latest_winner
    FROM ranked
    ORDER BY file_ordinal
    """
    out = M.table_dir("mart_accounts_lancs", snap, h)
    rows, nbytes = M.write_parquet(con, sql, out)
    f = out / "part.parquet"
    q = lambda s: con.execute(s).fetchone()[0]  # noqa: E731
    periods = q(f"SELECT count(*) FROM read_parquet('{f}') WHERE site_winner")
    differs = q(f"SELECT count(*) FROM read_parquet('{f}') "
                "WHERE site_winner AND NOT latest_winner")
    emp_differs = q(f"""
        SELECT count(*) FROM (
          SELECT crn, period_end,
                 max(employees_as_filed) FILTER (WHERE site_winner)   AS a,
                 max(employees_as_filed) FILTER (WHERE latest_winner) AS b
          FROM read_parquet('{f}') GROUP BY 1, 2)
        WHERE a IS DISTINCT FROM b""")
    impossible = q(f"SELECT count(*) FROM read_parquet('{f}') "
                   "WHERE impossible_period")
    M.log(f"  {rows:,} filings over {periods:,} periods, {differs:,} periods "
          f"resolve to a filing that is not the latest, {emp_differs:,} of "
          f"those carry a different headcount, {impossible} impossible periods")
    return dict(
        table="mart_accounts_lancs", snapshot=snap, rows=rows, nbytes=nbytes,
        inputs=[{"layer": "silver", "table": "ch_accounts", "snapshot": snap}],
        assertions={"filings": rows, "periods": periods,
                    "notTheLatestFiling": differs,
                    "differentEmployeeFigure": emp_differs,
                    "impossiblePeriods": impossible},
        cleared_faults=[
            {"id": "F3", "ref": "DATA-INTEGRITY s11.9",
             "what": "the site kept the last line rather than the latest "
                     "filing, and the extract is in reverse chronological "
                     "order, so the original filing usually won",
             "fix": "scripts/observatory/accounts_rules.resolve_latest picks "
                    "the filing made most recently, by filed_zip vintage with "
                    "stream position as tie-break, and both accounts consumers "
                    "call it",
             "periodsMovedToADifferentFiling": differs,
             "periodsWithADifferentEmployeeFigure": emp_differs,
             "clearedIn": "fix/observatory-faults-f2-f7"}],
        notes=("Employee figures are the s411 average for the PERIOD and are "
               "never a current headcount. employees_as_filed is exactly what "
               "was parsed; employees is nulled where it cannot be a "
               "headcount (s9.6). The projection emits employees_as_filed "
               "because that is what the live pipeline reads."))


# -------------------------------------------------------------------- PSC ---

@mart("mart_psc_lancs")
def build_psc_lancs(con, h, args):
    snap, pq = _silver("ch_psc", None, h)
    sql = f"""
    SELECT company_number, kind, name, name_title, name_forename,
           name_middle, name_surname, nationality, country_of_residence,
           postcode, natures_of_control, ceased_on, active, is_individual,
           file_ordinal, snapshot_date
    FROM read_parquet('{pq}')
    -- the extractor's own row order. build_dossiers publishes the first eight
    -- individuals it meets for a company, so the order decides which eight.
    ORDER BY file_ordinal
    """
    out = M.table_dir("mart_psc_lancs", snap, h)
    rows, nbytes = M.write_parquet(con, sql, out)
    M.log(f"  {rows:,} notified interests")
    return dict(
        table="mart_psc_lancs", snapshot=snap, rows=rows, nbytes=nbytes,
        inputs=[{"layer": "silver", "table": "ch_psc", "snapshot": snap}],
        assertions={"rows": rows},
        notes=("A row is a notified INTEREST, not a person. 3,009 people in "
               "this snapshot hold more than one (s9.7), so any count has to "
               "say which it means. Published PSC facts are name and country "
               "only: no DOB, no address."))


@mart("mart_psc_corporate")
def build_psc_corporate(con, h, args):
    snap, pq = _silver("ch_psc_corporate", None, h)
    sql = f"""
    SELECT company_number, psc_name, registration_number,
           registration_is_crn_shaped, country_registered, legal_form,
           postcode, ceased_on, active, file_ordinal, snapshot_date
    FROM read_parquet('{pq}')
    -- F6 (DATA-INTEGRITY s11.12) CLEARED: build_pound.py used to call
    -- parents[crn][0] "the primary corporate parent", which was whichever row
    -- this file happened to list first. It now applies a stated rule that
    -- reads no file position. The order is still preserved, because it is what
    -- lets a pre-fix edition be reproduced and because build_dossiers reads
    -- the first eight individual PSCs positionally.
    ORDER BY file_ordinal
    """
    out = M.table_dir("mart_psc_corporate", snap, h)
    rows, nbytes = M.write_parquet(con, sql, out)
    f = out / "part.parquet"
    multi = con.execute(
        f"SELECT count(*) FROM (SELECT company_number FROM read_parquet('{f}') "
        "WHERE active GROUP BY 1 HAVING count(*) > 1)").fetchone()[0]
    M.log(f"  {rows:,} corporate control edges, {multi:,} companies with more "
          "than one live corporate PSC (F6 territory)")
    return dict(
        table="mart_psc_corporate", snapshot=snap, rows=rows, nbytes=nbytes,
        inputs=[{"layer": "silver", "table": "ch_psc_corporate",
                 "snapshot": snap}],
        assertions={"rows": rows,
                    "companiesWithMoreThanOneLiveCorporatePsc": multi},
        cleared_faults=[
            {"id": "F6", "ref": "DATA-INTEGRITY s11.12",
             "what": "the site named parents[crn][0] as the primary corporate "
                     "parent, which is whichever row this file lists first, so "
                     "a company with two corporate PSCs got an arbitrary "
                     "published ownership chain",
             "fix": "build_pound.primary_parent applies a stated rule: a PSC "
                    "giving a registered company number first, then the lowest "
                    "company number, then the name in codepoint order. Nothing "
                    "in it reads file position",
             "companiesWithMoreThanOneLiveCorporatePsc": multi,
             "clearedIn": "fix/observatory-faults-f2-f7"}],
        notes=("registration_is_crn_shaped is a SHAPE test and never proof of "
               "a company: a society number can be CRN-shaped (s9.4). Row "
               "order is preserved because the site depends on it."))


# ---------------------------------------------------------------- notices ---

@mart("mart_notices_lancs")
def build_notices_lancs(con, h, args):
    """The 271 published Lancashire corporate insolvency notices.

    The LAD on a notice comes from the postcodes.io cache the fetcher used, not
    from ONSPD, so the cache is read from bronze rather than re-derived. F1
    replaces it with the real location hierarchy; reproducing the published
    edition means using the lookup the published edition used.

    F5 (DATA-INTEGRITY s11.10) CLEARED. Published notices carried a company
    number with a TRAILING SPACE exactly as The Gazette published it, and the
    site joined the register on that raw string, so a notice about a company we
    hold rendered with the notice's own postcode-derived LAD and no sector.
    `company_number` is the trimmed number that joins and is what the
    projection now emits; `company_number_raw` stays as the verbatim record of
    what the publisher wrote, which is the evidence a reader would need.
    """
    snap, pq = _silver("gazette_notices", None, h)
    cache_snap, cache_path, cache_manifest = SV.resolve_bronze(
        "postcode_lad_cache", None, h)
    cache_file, cache_sha = SV.bronze_file(
        cache_manifest, cache_path, "_postcode_lad_cache.json")
    sql = f"""
    WITH cache AS (
      SELECT unnest(map_keys(j))   AS postcode_norm,
             unnest(map_values(j)) AS v
      FROM (SELECT * FROM read_json('{cache_file}',
                                    columns={{'j': 'MAP(VARCHAR, JSON)'}}))
    )
    SELECT n.notice_id,
           n.insolvency_type            AS notice_type,
           n.company_name,
           n.company_number,
           n.company_number_raw,
           CAST(n.notice_date AS VARCHAR) AS notice_date,
           n.uri,
           json_extract_string(c.v, '$.lad') AS lad_code,
           n.reg_office_postcode        AS postcode,
           n.notice_code,
           n.notice_category,
           n.is_strike_off,
           n.file_ordinal,
           n.snapshot_date
    FROM read_parquet('{pq}') n
    LEFT JOIN cache c
      ON c.postcode_norm = upper(replace(n.reg_office_postcode, ' ', ''))
    WHERE n.in_published_lancs_subset
    -- candidate-file order, which is the order the published file is in
    ORDER BY n.file_ordinal
    """
    out = M.table_dir("mart_notices_lancs", snap, h)
    rows, nbytes = M.write_parquet(con, sql, out)
    f = out / "part.parquet"
    q = lambda s: con.execute(s).fetchone()[0]  # noqa: E731
    no_lad = q(f"SELECT count(*) FROM read_parquet('{f}') WHERE lad_code IS NULL")
    untrimmed = q(f"SELECT count(*) FROM read_parquet('{f}') "
                  "WHERE company_number_raw IS DISTINCT FROM company_number")
    off_cat = q(f"SELECT count(*) FROM read_parquet('{f}') "
                "WHERE notice_category <> '24'")
    SV.assert_equal("notices outside category 24", off_cat, 0)
    SV.assert_equal("notices with no LAD", no_lad, 0)
    M.log(f"  {rows} published notices, all category 24, all placed, "
          f"{untrimmed} carrying an untrimmed company number")
    return dict(
        table="mart_notices_lancs", snapshot=snap, rows=rows, nbytes=nbytes,
        inputs=[{"layer": "silver", "table": "gazette_notices",
                 "snapshot": snap},
                {"layer": "bronze", "source": "postcode_lad_cache",
                 "snapshot": cache_snap, "sha256": cache_sha}],
        assertions={"rows": rows, "outsideCategory24": off_cat,
                    "unplaced": no_lad,
                    "untrimmedCompanyNumbers": untrimmed},
        cleared_faults=[
            {"id": "F5", "ref": "DATA-INTEGRITY s11.10",
             "what": "the site joined the register on the raw Gazette company "
                     "number, so a notice whose number carried a trailing "
                     "space rendered as though the company were unknown to us",
             "fix": "the projection emits company_number, the trimmed form "
                    "silver derives, and fetch_gazette.py trims at the point "
                    "the publisher's text arrives, so both paths join clean. "
                    "company_number_raw stays in the mart as the verbatim "
                    "record of what was published",
             "noticesWithAnUntrimmedNumber": untrimmed,
             "clearedIn": "fix/observatory-faults-f2-f7"}],
        notes=("Category 24 only. The fetcher's own cache still holds 1,150 "
               "category 25 personal insolvency notices naming individuals "
               "(s9.5); they have never reached a published file and the "
               "silver builder drops them by explicit rule. A notice is the "
               "notice fact, verbatim and dated, and nothing more."))


# ---------------------------------------------------- supplier identifiers ---

@mart("mart_supplier_identifiers")
def build_supplier_identifiers(con, h, args):
    """Company numbers a THIRD PARTY published against a supplier name.

    Sourced from the crosswalk rather than recomputed, which is the whole
    argument of DATA-INTEGRITY s11.2 in one table: a decision recorded as an
    edge survives its input going missing.

    F2 (s11.2) CLEARED. The live pipeline's OCDS map was empty on vps-main,
    because fetch_ocds_ids.py read Contracts Finder releases from a path in
    another repository that existed only on the Mac. The repaired fetcher talks
    to the API directly and the projection emits every identification the
    crosswalk holds. `in_production_edition` stays as the reconciliation
    column: it says which identifications the legacy path is also producing, so
    the two paths can be shown to agree rather than assumed to.
    """
    edges = sorted((XW.gold_dir(h) / "crosswalk_edges").glob(
        "snapshot_date=*/part.parquet"))
    if not edges:
        raise SystemExit("FATAL: no gold/crosswalk_edges; run build_crosswalk.py")
    edge_pq = edges[-1]
    snap = edge_pq.parent.name.split("=", 1)[1]
    prod = _production_ocds_names()
    prod_sql = ("(" + ", ".join(f"'{n}'" for n in sorted(prod)) + ")"
                if prod else "('\\x00 none')")
    sql = f"""
    SELECT decision_id,
           source_name_norm     AS supplier_key,
           source_name          AS supplier_name,
           max(source_id) FILTER (WHERE scheme = 'GB-COH') AS company_number,
           any_value(evidence)  AS evidence,
           any_value(evidence_class) AS evidence_class,
           any_value(confidence)     AS confidence,
           list_sort(list(DISTINCT source_snapshot)) AS source_snapshots,
           (source_name_norm IN {prod_sql}) AS in_production_edition
    FROM read_parquet('{edge_pq}')
    WHERE matcher = 'ocds'
    GROUP BY decision_id, source_name_norm, source_name
    HAVING max(source_id) FILTER (WHERE scheme = 'GB-COH') IS NOT NULL
    ORDER BY supplier_key
    """
    out = M.table_dir("mart_supplier_identifiers", snap, h)
    rows, nbytes = M.write_parquet(con, sql, out)
    f = out / "part.parquet"
    in_prod = con.execute(
        f"SELECT count(*) FROM read_parquet('{f}') "
        "WHERE in_production_edition").fetchone()[0]
    M.log(f"  {rows} OCDS identifications in the crosswalk, {in_prod} of them "
          f"also on the legacy path, {rows - in_prod} carried by the crosswalk "
          "alone")
    return dict(
        table="mart_supplier_identifiers", snapshot=snap, rows=rows,
        nbytes=nbytes,
        inputs=[{"layer": "gold", "table": "crosswalk_edges",
                 "snapshot": snap}],
        assertions={"identifications": rows,
                    "alsoOnTheLegacyPath": in_prod,
                    "carriedByTheCrosswalkAlone": rows - in_prod},
        cleared_faults=[
            {"id": "F2", "ref": "DATA-INTEGRITY s11.2",
             "what": "fetch_ocds_ids.py read a path that existed only on the "
                     "Mac, so the monthly cron produced an empty map and the "
                     "identifications looked like an honest zero",
             "fix": "fetch_ocds_ids.py talks to the Contracts Finder API "
                    "directly (f73ce55) and the projection emits the "
                    "crosswalk's full set rather than filtering to the "
                    "identifications the pre-fix edition happened to have",
             "identifications": rows,
             "alsoPresentOnTheLegacyPath": in_prod,
             "clearedIn": "fix/observatory-faults-f2-f7"}],
        notes=("An identifier-observed edge: a buyer wrote the company number "
               "on an award notice and we read it. Confidence 1.0, no score."))


def _production_ocds_names():
    """The byName keys the LEGACY path produces, read from its own output.

    Before the fetcher was repaired this was empty on vps-main and the gap was
    F2. It is now the reconciliation set: the mart records which of its
    identifications the legacy path also has, which is how the two paths are
    shown to agree instead of being assumed to.
    """
    for p in (Path("/root/observatory-data/processed/ocds_supplier_ids.json"),
              Path.home() / "observatory-data/processed/ocds_supplier_ids.json"):
        if p.exists():
            return set(json.loads(p.read_text()).get("byName", {}))
    return set()


# ------------------------------------------------------------------- main ---

ORDER = ["mart_register_lancs", "mart_register_index", "mart_accounts_lancs",
         "mart_psc_lancs", "mart_psc_corporate", "mart_notices_lancs",
         "mart_supplier_identifiers"]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--only", action="append", default=None)
    ap.add_argument("--snapshot", default=None,
                    help="register snapshot_date; default latest")
    ap.add_argument("--pin", action="append", default=[],
                    metavar="TABLE=YYYY-MM-DD",
                    help="pin a silver table to one snapshot, repeatable. "
                         "Reproducing a published edition means reading the "
                         "partitions that edition read; without this a newer "
                         "snapshot silently becomes the input and a "
                         "golden-file diff measures the feed rather than the "
                         "warehouse.")
    ap.add_argument("--home", default=None)
    args = ap.parse_args()
    for spec in args.pin:
        table, _, snap = spec.partition("=")
        if not snap:
            raise SystemExit(f"FATAL: --pin wants TABLE=YYYY-MM-DD, got {spec!r}")
        _PINS[table] = snap
    if _PINS:
        M.log(f"snapshot pins: {_PINS}")
    h = args.home

    con, dv = M.connect()
    # Row ORDER is load-bearing in two marts (the register frame feeds a
    # truncated published list, the accounts stream feeds a last-line-wins
    # rule), so insertion order is preserved on write. silver.connect() turns
    # it off for throughput; a mart that reorders its own output would be
    # reproducing a different edition.
    con.execute("SET preserve_insertion_order=true")
    M.log(f"duckdb {dv}")
    summary = {}
    for name in ORDER:
        if args.only and name not in args.only:
            continue
        M.log(f"building {name}")
        res = BUILDERS[name](con, h, args)
        out = M.table_dir(res["table"], res["snapshot"], h)
        M.write_manifest(
            out, res["table"], res["snapshot"], res["rows"], res["nbytes"], dv,
            inputs=res.get("inputs"), assertions=res.get("assertions"),
            notes=res.get("notes"),
            reproduced_faults=res.get("reproduced_faults"),
            cleared_faults=res.get("cleared_faults"))
        summary[res["table"]] = {"snapshot": res["snapshot"],
                                 "rows": res["rows"],
                                 "MB": round(res["nbytes"] / 1e6, 1)}
    print(json.dumps(summary, indent=1))


if __name__ == "__main__":
    main()
