#!/usr/bin/env python3
"""Silver: Companies House whole-register snapshot.

bronze/source=ch_register/snapshot_date=YYYY-MM-DD/BasicCompanyDataAsOneFile-*.zip
  -> silver/ch_register/snapshot_date=YYYY-MM-DD/part.parquet

National, every row, typed, with entityType stamped per DATA-INTEGRITY s2 and
the non-company families flagged out of the company count by explicit prefix
rule (entity_type.py carries the rule and the evidence).

Three shape traps this builder handles, all of them found the hard way:

  * The CSV header carries LEADING SPACES on 15 of its 55 column names
    (" CompanyNumber", " RegAddress.AddressLine2", " ConfStmtLastMadeUpDate"
    and the previous-name pairs). Reading by header name silently returns NULL
    for those columns. We therefore ignore the header entirely and supply our
    own names positionally.
  * The 2026-08-01 edition contains an embedded newline inside a quoted field
    (LIONS BOXING ORGANIZATION..., CSV lines 3,004,675 to 3,004,676) followed
    by one genuinely blank line. A reader that counts lines rather than records
    reports 5,695,466. The true record count is 5,695,465, which is what
    DuckDB, and P0's independent parse, both return. That is the whole of the
    "one row differs and is not material" note in DATA-INTEGRITY s7: it is a
    blank line, and it is now explained rather than tolerated.
  * Dates are DD/MM/YYYY, and a wrong locale guess silently swaps day and
    month for the first twelve days of every month.

Usage:
    build_silver_ch_register.py [--snapshot 2026-08-01] [--keep-csv]
"""
import argparse
import shutil
import sys
import zipfile
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import silver as SV  # noqa: E402
import entity_type as ET  # noqa: E402

TABLE = "ch_register"

# Positional names for the 55 columns, in file order, header discarded.
COLS = [
    "CompanyName", "CompanyNumber", "RegAddress_CareOf", "RegAddress_POBox",
    "RegAddress_AddressLine1", "RegAddress_AddressLine2", "RegAddress_PostTown",
    "RegAddress_County", "RegAddress_Country", "RegAddress_PostCode",
    "CompanyCategory", "CompanyStatus", "CountryOfOrigin", "DissolutionDate",
    "IncorporationDate", "Accounts_AccountRefDay", "Accounts_AccountRefMonth",
    "Accounts_NextDueDate", "Accounts_LastMadeUpDate", "Accounts_AccountCategory",
    "Returns_NextDueDate", "Returns_LastMadeUpDate", "Mortgages_NumMortCharges",
    "Mortgages_NumMortOutstanding", "Mortgages_NumMortPartSatisfied",
    "Mortgages_NumMortSatisfied", "SICCode_SicText_1", "SICCode_SicText_2",
    "SICCode_SicText_3", "SICCode_SicText_4", "LimitedPartnerships_NumGenPartners",
    "LimitedPartnerships_NumLimPartners", "URI",
]
for _i in range(1, 11):
    COLS += [f"PreviousName_{_i}_CONDATE", f"PreviousName_{_i}_CompanyName"]
COLS += ["ConfStmtNextDueDate", "ConfStmtLastMadeUpDate"]

# National counts verified against the 2026-08-01 edition on 17 Aug 2026.
# These are hard assertions: a change means the source changed shape or our
# rule is wrong, and either way the build must stop rather than publish a
# different number quietly.
EXPECTED = {
    "2026-08-01": {
        "raw_rows": 5695465,      # records, not lines. See the docstring.
        "rows": 5695465,
        "cio": 48032,             # CE 40,160 + CS 7,872
        "registered_society": 10950,   # IP 6,277 + RS 3,758 + SP 636 + NO 146 + NP 133
        "overseas_establishment": 13824,
        "overseas_entity": 30199,
        "cic": 44464,
        "llp": 50620,
        "lp": 60915,
        "other_corporate_body": 3799,
        "proposed_strike_off": 388951,
    },
}


def d(col):
    """CH dates are DD/MM/YYYY. try_strptime returns NULL rather than raising
    on a blank or malformed value, which is what we want for a bulk file."""
    return f"try_strptime(nullif(trim({col}), ''), '%d/%m/%Y')::DATE"


def i(col):
    return f"try_cast(nullif(trim({col}), '') AS INTEGER)"


def s(col):
    return f"nullif(trim({col}), '')"


def sic(col):
    """SIC columns read '01110 - Growing of cereals'. Keep the code separately."""
    return f"regexp_extract(nullif(trim({col}), ''), '^([0-9]+)', 1)"


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--snapshot")
    ap.add_argument("--keep-csv", action="store_true",
                    help="leave the decompressed CSV in place for debugging")
    args = ap.parse_args()

    snap, part, manifest = SV.resolve_bronze("ch_register", args.snapshot)
    zpath, zsha = SV.bronze_file(manifest, part, ".zip")
    SV.log(f"bronze snapshot_date={snap} {zpath.name}")

    con, dv = SV.connect()
    SV.log(f"duckdb {dv}")

    work = SV.silver_dir() / "_work"
    work.mkdir(parents=True, exist_ok=True)
    zf = zipfile.ZipFile(zpath)
    member = zf.namelist()[0]
    csv_path = work / member
    if not csv_path.exists():
        SV.log(f"decompressing {member}")
        with zf.open(member) as src, open(csv_path, "wb") as dst:
            shutil.copyfileobj(src, dst, 1 << 22)
    SV.log(f"csv {csv_path.stat().st_size/1e9:.2f} GB")

    names = "[" + ", ".join(f"'{c}'" for c in COLS) + "]"
    # parallel=false is required, not a tuning choice: the parallel reader
    # refuses this file outright because a quoted field spans a buffer boundary.
    # Single-threaded costs a couple of minutes on 2.8 GB and is the only way
    # to read every row.
    read = (
        f"read_csv('{csv_path}', header=false, skip=1, names={names}, "
        "all_varchar=true, null_padding=true, quote='\"', escape='\"', "
        "strict_mode=false, parallel=false)"
    )
    con.execute(f"CREATE VIEW raw AS SELECT * FROM {read}")

    raw_rows = con.execute("SELECT count(*) FROM raw").fetchone()[0]
    exp = EXPECTED.get(snap)
    if exp:
        SV.assert_equal("raw_rows", raw_rows, exp["raw_rows"])
    else:
        SV.log(f"  no expectation block for snapshot {snap}; raw_rows={raw_rows}")

    dropped = con.execute(
        "SELECT count(*) FROM raw WHERE nullif(trim(CompanyNumber), '') IS NULL"
    ).fetchone()[0]
    SV.log(f"  rows with no company number (dropped): {dropped}")

    # An unclassified two-letter prefix must stop the build. Silently letting
    # it fall through to 'ltd' is how a register of societies becomes a count
    # of companies.
    unknown = con.execute(f"""
        SELECT DISTINCT substr(trim(CompanyNumber), 1, 2) AS p
        FROM raw
        WHERE nullif(trim(CompanyNumber), '') IS NOT NULL
          AND NOT {ET.known_prefixes_sql('trim(CompanyNumber)')}
    """).fetchall()
    if unknown:
        raise SystemExit(
            f"FATAL: unclassified company-number prefixes {[u[0] for u in unknown]}. "
            "Add them to entity_type.PREFIX_CLASSES with their legal form; do "
            "not let them fall through to 'ltd'.")

    select = f"""
    SELECT
      trim(CompanyNumber)                          AS company_number,
      substr(trim(CompanyNumber), 1, 2)            AS number_prefix_raw,
      CASE WHEN regexp_matches(substr(trim(CompanyNumber), 1, 2), '^[A-Z]{{2}}$')
           THEN substr(trim(CompanyNumber), 1, 2) END AS number_prefix,
      {s('CompanyName')}                           AS company_name,
      {s('CompanyCategory')}                       AS company_category,
      {s('CompanyStatus')}                         AS company_status,
      {s('CountryOfOrigin')}                       AS country_of_origin,
      {d('IncorporationDate')}                     AS incorporation_date,
      {d('DissolutionDate')}                       AS dissolution_date,
      {s('RegAddress_CareOf')}                     AS reg_care_of,
      {s('RegAddress_POBox')}                      AS reg_po_box,
      {s('RegAddress_AddressLine1')}               AS reg_address_line1,
      {s('RegAddress_AddressLine2')}               AS reg_address_line2,
      {s('RegAddress_PostTown')}                   AS reg_post_town,
      {s('RegAddress_County')}                     AS reg_county,
      {s('RegAddress_Country')}                    AS reg_country,
      {s('RegAddress_PostCode')}                   AS reg_postcode,
      upper(replace(coalesce({s('RegAddress_PostCode')}, ''), ' ', ''))
        AS reg_postcode_norm,
      {i('Accounts_AccountRefDay')}                AS accounts_ref_day,
      {i('Accounts_AccountRefMonth')}              AS accounts_ref_month,
      {d('Accounts_NextDueDate')}                  AS accounts_next_due_date,
      {d('Accounts_LastMadeUpDate')}               AS accounts_last_made_up_date,
      {s('Accounts_AccountCategory')}              AS accounts_category,
      {d('Returns_NextDueDate')}                   AS returns_next_due_date,
      {d('Returns_LastMadeUpDate')}                AS returns_last_made_up_date,
      {d('ConfStmtNextDueDate')}                   AS conf_stmt_next_due_date,
      {d('ConfStmtLastMadeUpDate')}                AS conf_stmt_last_made_up_date,
      {i('Mortgages_NumMortCharges')}              AS mort_charges,
      {i('Mortgages_NumMortOutstanding')}          AS mort_outstanding,
      {i('Mortgages_NumMortPartSatisfied')}        AS mort_part_satisfied,
      {i('Mortgages_NumMortSatisfied')}            AS mort_satisfied,
      {s('SICCode_SicText_1')}                     AS sic_text_1,
      {s('SICCode_SicText_2')}                     AS sic_text_2,
      {s('SICCode_SicText_3')}                     AS sic_text_3,
      {s('SICCode_SicText_4')}                     AS sic_text_4,
      list_filter([{sic('SICCode_SicText_1')}, {sic('SICCode_SicText_2')},
                   {sic('SICCode_SicText_3')}, {sic('SICCode_SicText_4')}],
                  x -> x IS NOT NULL AND x <> '')  AS sic_codes,
      {i('LimitedPartnerships_NumGenPartners')}    AS lp_general_partners,
      {i('LimitedPartnerships_NumLimPartners')}    AS lp_limited_partners,
      {s('URI')}                                   AS ch_uri,
      list_filter([
        {", ".join("{'condate': " + d(f'PreviousName_{n}_CONDATE')
                   + ", 'name': " + s(f'PreviousName_{n}_CompanyName') + "}"
                   for n in range(1, 11))}
      ], x -> x.name IS NOT NULL)                  AS previous_names,
      {ET.prefix_case_sql('trim(CompanyNumber)')}  AS entity_type,
      ({s('CompanyCategory')} = '{ET.CIC_CATEGORY}') AS is_cic,
      ({s('CompanyStatus')} = 'Active - Proposal to Strike off')
        AS proposed_strike_off,
      DATE '{snap}'                                AS snapshot_date,
      '{zsha}'                                     AS source_sha256
    FROM raw
    WHERE nullif(trim(CompanyNumber), '') IS NOT NULL
    """
    # companies_act_body has to be layered on top: it reads entity_type, which
    # does not exist until the projection above has run.
    select = (f"SELECT *, {ET.companies_act_case_sql('entity_type')} "
              f"AS companies_act_body FROM ({select})")

    out = SV.table_dir(TABLE, snap)
    SV.log("writing parquet")
    rows, nbytes = SV.write_parquet(con, select, out)
    SV.log(f"  {rows:,} rows, {nbytes/1e6:.1f} MB")

    pq = out / "part.parquet"
    counts = dict(con.execute(f"""
        SELECT entity_type, count(*) FROM read_parquet('{pq}') GROUP BY 1
    """).fetchall())
    cic = con.execute(
        f"SELECT count(*) FROM read_parquet('{pq}') WHERE is_cic").fetchone()[0]
    strike = con.execute(
        f"SELECT count(*) FROM read_parquet('{pq}') WHERE proposed_strike_off"
    ).fetchone()[0]
    excluded = con.execute(f"""
        SELECT count(*) FROM read_parquet('{pq}') WHERE NOT companies_act_body
    """).fetchone()[0]

    # Category-per-prefix contract: a CIO filed under an LLP prefix, or a
    # society filed under a company prefix, must fail rather than land.
    for pfx, allowed in sorted(ET.PREFIX_EXPECTED_CATEGORIES.items()):
        got = set(x[0] for x in con.execute(f"""
            SELECT DISTINCT company_category FROM read_parquet('{pq}')
            WHERE number_prefix = '{pfx}'
        """).fetchall())
        if got - allowed:
            raise SystemExit(
                f"FATAL: prefix {pfx} carries unexpected CompanyCategory "
                f"{sorted(got - allowed)}. Expected {sorted(allowed)}.")

    assertions = {
        "rawRows": raw_rows,
        "rowsDroppedNoCompanyNumber": dropped,
        "rows": rows,
        "byEntityType": {k: v for k, v in sorted(counts.items())},
        "cicRows": cic,
        "proposedStrikeOff": strike,
        "excludedFromCompanyCount": excluded,
        "companiesActBodies": rows - excluded,
    }
    if exp:
        SV.assert_equal("rows", rows, exp["rows"])
        SV.assert_equal("cio", counts.get("cio", 0), exp["cio"])
        SV.assert_equal("registered_society",
                        counts.get("registered-society", 0),
                        exp["registered_society"])
        SV.assert_equal("overseas_establishment",
                        counts.get("overseas-establishment", 0),
                        exp["overseas_establishment"])
        SV.assert_equal("overseas_entity", counts.get("overseas-entity", 0),
                        exp["overseas_entity"])
        SV.assert_equal("cic", cic, exp["cic"])
        SV.assert_equal("cic_entity_type", counts.get("cic", 0), exp["cic"])
        SV.assert_equal("llp", counts.get("llp", 0), exp["llp"])
        SV.assert_equal("lp", counts.get("lp", 0), exp["lp"])
        SV.assert_equal("other_corporate_body",
                        counts.get("other-corporate-body", 0),
                        exp["other_corporate_body"])
        SV.assert_equal("proposed_strike_off", strike, exp["proposed_strike_off"])

    SV.write_manifest(
        out, TABLE, snap,
        inputs=[{"layer": "bronze", "source": "ch_register",
                 "snapshotDate": snap, "file": zpath.name, "sha256": zsha}],
        rows=rows, nbytes=nbytes, duckdb_version=dv, assertions=assertions,
        notes=(
            "National register snapshot. companies_act_body=false marks CIOs, "
            "Scottish CIOs, registered societies, overseas establishments, "
            "overseas entities and other corporate bodies, which are real "
            "registrations but not companies and are out of any company count "
            "(DATA-INTEGRITY s7.1 and s7.8). entityType comes from the number "
            "prefix first and CompanyCategory only for the CIC and plc splits."
        ))

    con.close()
    if not args.keep_csv:
        csv_path.unlink()
        SV.log("removed working csv")
    return 0


if __name__ == "__main__":
    sys.exit(main())
