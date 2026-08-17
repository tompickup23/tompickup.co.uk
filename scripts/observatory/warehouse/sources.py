"""Bronze source registry for the Lancashire Business Observatory warehouse.

One entry per register we fetch. This is the declarative map from "a file on
disk" to "(source, snapshot_date, asAt, licence)". Nothing in the warehouse
guesses provenance: if a file is not matched by an entry here it goes to
bronze/_quarantine/ and the build says so.

Two dates, per DATA-INTEGRITY s4, and they are not the same thing:
  snapshot_date  the transaction-time axis. When WE fetched it. This is the
                 hive partition key, because it is the axis that makes
                 as-retrieved queries unfalsifiable.
  as_at          the source's own reference date (list date, period end,
                 edition). Carried inside the manifest, never a partition key.
                 None where the source does not state one; never invented.

Hosts: a source lives where it was fetched. Multi-GB payloads stay put and only
their manifests are mirrored to Drive, so build_bronze.py only processes entries
whose host matches the machine it is running on.
"""
import os
import re
import datetime as _dt
from pathlib import Path

MAC_ROOT = Path(os.path.expanduser("~/observatory-data"))
VPS_ROOT = Path("/opt/observatory")
# vps-main carries BOTH trees: /opt/observatory for the CH-scale work, and a
# ~/observatory-data mirror that the monthly cron refetches into. The two are
# different vintages of the same sources under identical filenames (the Mac
# holds the 26-27 Jul fetch, vps the 8 Aug refetch), which is precisely why
# snapshot_date is a partition key and not a filename convention.
VPS_HOME = Path("/root/observatory-data")

ROOTS = {"mac": [MAC_ROOT], "vps": [VPS_ROOT, VPS_HOME]}

OGL = "Open Government Licence v3.0"


def host():
    """mac or vps. /opt/observatory decides, because vps-main ALSO carries a
    ~/observatory-data tree (a partial mirror of the Mac layout), so presence of
    the home tree proves nothing. The Mac has no /opt/observatory."""
    if VPS_ROOT.exists():
        return "vps"
    if MAC_ROOT.exists():
        return "mac"
    raise SystemExit("no observatory data root found on this machine")


def roots(h=None):
    """Every data root to search on this host, in priority order."""
    return [r for r in ROOTS[h or host()] if r.exists()]


def root(h=None):
    """The primary root: where bronze lives and where unclaimed scans start."""
    return ROOTS[h or host()][0]


def bronze_dir(h=None):
    return root(h) / "bronze"


def _from_name(pattern, group=1):
    """snapshot/as-at resolver: pull a date out of the filename itself."""
    rx = re.compile(pattern)

    def f(p):
        m = rx.search(p.name)
        return m.group(group) if m else None
    return f


def _mtime(p):
    """Fallback snapshot_date: the file's own mtime, in UTC date form.

    Honest for our purposes because every raw file here was written by a
    fetcher at retrieval time and none have been edited since. check_bronze.py
    freezes the value into the manifest, so this is read exactly once per file.
    """
    return _dt.datetime.utcfromtimestamp(p.stat().st_mtime).strftime("%Y-%m-%d")


# --- The registry ---------------------------------------------------------
# glob is relative to the host's data root. as_at may be a literal string, a
# callable taking the Path, or None where the source states no reference date.

SOURCES = [
    # --- Companies House family (vps: the bulk files are 0.5GB each) -------
    dict(
        id="ch_register",
        name="Companies House BasicCompanyDataAsOneFile",
        hosts=["vps"],
        globs=[
            "work/BasicCompanyDataAsOneFile-*.zip",
            # The 2026-08-01 snapshot our current outputs are actually built
            # from lives in the England Raw Data archive tree, not ours. An
            # absolute glob is allowed so bronze stays self-contained rather
            # than depending on another project's directory staying put.
            "/opt/doge-archive/reference/01 Companies House company data/"
            "BasicCompanyDataAsOneFile-*.zip",
        ],
        snapshot_date=_from_name(r"(\d{4}-\d{2}-\d{2})"),
        as_at=_from_name(r"(\d{4}-\d{2}-\d{2})"),
        licence=OGL,
        source_url="https://download.companieshouse.gov.uk/en_output.html",
        notes=(
            "Monthly whole-register snapshot. Contains rows that are NOT "
            "Companies Act companies: 40,160 CIO (CE), 7,872 Scottish CIO (CS) "
            "and 10,937 society rows (IP/RS/SP/NO/NP), all with blank "
            "postcodes. BR overseas-establishment rows do not exist in this "
            "file; FC does (13,824). See DATA-INTEGRITY s7.1 and s7.8."
        ),
    ),
    dict(
        id="ch_psc",
        name="Companies House PSC snapshot",
        hosts=["vps"],
        globs=["work/psc/*.zip", "work/psc/*.txt", "work/psc/*.jsonl*"],
        snapshot_date=_mtime,
        as_at=None,
        licence=OGL,
        source_url="https://download.companieshouse.gov.uk/en_pscdata.html",
        notes="Persons with significant control snapshot.",
    ),
    dict(
        id="onspd",
        name="ONS Postcode Directory",
        hosts=["vps"],
        globs=["work/ONSPD_*.zip", "onspd/*.zip", "onspd/*.csv"],
        snapshot_date=_mtime,
        as_at=_from_name(r"ONSPD_([A-Z]{3}_\d{4})"),
        licence=OGL + " (plus Royal Mail and OS caveats)",
        source_url="https://geoportal.statistics.gov.uk/",
        notes=(
            "as_at is the ONS edition tag (for example MAY_2026), not a "
            "calendar date, because that is what the source states. "
            "Terminated postcodes are retained for matching historic register "
            "addresses and must never geocode a current-presence claim "
            "(DATA-INTEGRITY s4 vintage rule 4)."
        ),
    ),

    # --- Local fetchers ----------------------------------------------------
    dict(
        id="fhrs",
        name="FSA Food Hygiene Rating Scheme, per-authority XML",
        hosts=["mac", "vps"],
        globs=["raw/fhrs_*.xml"],
        snapshot_date=_mtime,
        as_at=None,
        licence=OGL + " (FSA FHRS open data)",
        source_url="https://ratings.food.gov.uk/open-data",
        notes="14 Lancashire authorities. Premises evidence, not health of a business.",
    ),
    dict(
        id="charity_commission",
        name="Charity Commission for England and Wales register extract",
        hosts=["mac", "vps"],
        globs=["raw/charity*.zip"],
        snapshot_date=_mtime,
        as_at=None,
        licence=OGL,
        source_url="https://register-of-charities.charitycommission.gov.uk/register/full-register-download",
        notes=(
            "Daily bulk. Carries removed charities with reason codes, and the "
            "CIO flag. Excepted and exempt charities are absent by law, which "
            "is correct rather than missing."
        ),
    ),
    dict(
        id="cqc",
        name="CQC care directory",
        hosts=["mac", "vps"],
        globs=["raw/cqc_directory.csv"],
        snapshot_date=_mtime,
        as_at=None,
        licence=OGL,
        source_url="https://www.cqc.org.uk/about-us/transparency/using-cqc-data",
        notes="Premises evidence for the location hierarchy.",
    ),
    dict(
        id="gias",
        name="Get Information About Schools, all data",
        hosts=["mac", "vps"],
        globs=["raw/gias_alldata.csv"],
        snapshot_date=_mtime,
        as_at=None,
        licence=OGL,
        source_url="https://get-information-schools.service.gov.uk/",
        notes="Academy trusts are exempt charities: absent from the CC register by design.",
    ),
    dict(
        id="voa_2023_list",
        name="VOA 2023 rating list entries",
        hosts=["mac", "vps"],
        globs=["raw/voa_*listentries*.zip"],
        snapshot_date=_mtime,
        as_at="2023 list",
        licence=OGL,
        source_url="https://voaratinglists.blob.core.windows.net/html/rlidata.htm",
        notes=(
            "The 2023 list is OGL. The 2026 list bulk is NOT OGL (restricted, "
            "pass-through terms) and nothing derived from it ships before the "
            "LEGAL.md entry and the solicitor question. Do not let a 2026 file "
            "land in this partition."
        ),
    ),
    dict(
        id="nndr",
        name="Council NNDR ratepayer files",
        hosts=["mac", "vps"],
        globs=["raw/nndr/*"],
        snapshot_date=_mtime,
        as_at=None,
        licence="per-council, varies; presence evidence only, no republication",
        source_url="council transparency pages",
        notes=(
            "12 of 14 councils publish. Hyndburn withholds, Wyre publishes "
            "nameless. Evidence-only: ratepayer identities are not republished."
        ),
    ),
    dict(
        id="innovate_uk",
        name="Innovate UK funded projects bulk",
        hosts=["mac", "vps"],
        globs=["raw/IUK-*.xlsx"],
        snapshot_date=_mtime,
        as_at=_from_name(r"IUK-(\d{6})-", 1),
        licence=OGL + " (UKRI)",
        source_url="https://www.ukri.org/publications/innovate-uk-funded-projects-since-2004/",
        notes="as_at is the UKRI publication stamp in the filename, DDMMYY.",
    ),
    dict(
        id="ons_business_demography",
        name="ONS Business Demography reference tables",
        hosts=["mac", "vps"],
        globs=["raw/business_demography_*.xlsx"],
        snapshot_date=_mtime,
        as_at="2024 edition, published 2025-11-20",
        licence=OGL,
        source_url="https://www.ons.gov.uk/businessindustryandtrade/business/activitysizeandlocation/datasets/businessdemographyreferencetable",
        notes=(
            "LAD tables cover 2019 to 2024 in this edition. There is NO "
            "broad-industry by LAD cross-tab: industry is UK level only. "
            "Counts are control-rounded to the nearest 5. Depth beyond 2019 "
            "needs chained archived editions back to 2012."
        ),
    ),
    dict(
        id="bpe",
        name="DBT Business Population Estimates",
        hosts=["mac", "vps"],
        globs=["raw/bpe_*_tables.xlsx"],
        snapshot_date=_mtime,
        as_at=_from_name(r"bpe_(\d{4})_"),
        licence=OGL,
        source_url="https://www.gov.uk/government/collections/business-population-estimates",
        notes=(
            "Region level only. The unregistered slice is a MODEL, never a "
            "count, and never decomposes to named level."
        ),
    ),
    dict(
        id="hmrc_spi",
        name="HMRC Survey of Personal Incomes table 3.14",
        hosts=["mac", "vps"],
        globs=["raw/spi_*.ods"],
        snapshot_date=_mtime,
        as_at=None,
        licence=OGL,
        source_url="https://www.gov.uk/government/collections/personal-incomes-statistics",
        notes="Triples (count, mean, median) per source; LAD rows suffix ' UA' for unitaries.",
    ),
    dict(
        id="nomis",
        name="NOMIS API extracts (UKBC, BRES, ASHE)",
        hosts=["mac", "vps"],
        globs=["raw/nomis_*.json"],
        snapshot_date=_mtime,
        as_at=None,
        licence=OGL + " (ONS Crown copyright via NOMIS)",
        source_url="https://www.nomisweb.co.uk/api/v01/dataset/",
        notes=(
            "NM_142_1 and NM_141_1 business counts, NM_189_1 BRES, NM_99_1 and "
            "NM_30_1 ASHE. IDBR basis: VAT/PAYE-registered only, caption "
            "mandatory. A registered sole proprietor count is not a "
            "self-employment count."
        ),
    ),
    dict(
        id="nhsbsa_contractors",
        name="NHSBSA Contractor Details (pharmacies + dispensing GP practices)",
        hosts=["mac", "vps"],
        globs=["raw/nhsbsa_contractor_details_*.csv"],
        snapshot_date=_from_name(r"nhsbsa_contractor_details_(\d{4}-\d{2})", 1),
        as_at="2026-07 edition",
        licence=OGL,
        source_url="https://opendata.nhsbsa.net/dataset/contractor-details",
        notes=(
            "21,209 rows, monthly. Name+trading name+full address+postcode+"
            "phone for every pharmacy/appliance/dispensing-GP contractor "
            "account in England (plus Crown Dependencies). 692 rows match a "
            "Lancashire postcode area. CONTRACTOR_TYPE distinguishes single "
            "contractors, multiples (Boots, Bestway, 5+ shop chains) and "
            "practice dispensaries. Candidate A1, verified 17 Aug 2026."
        ),
    ),
    dict(
        id="nhs_payments_gp",
        name="NHS payments to general practice, England 2024-25 (practice-level)",
        hosts=["mac", "vps"],
        globs=["raw/nhs_payments_gp_2024-25_practice.csv"],
        snapshot_date=_mtime,
        as_at="2024-25 financial year",
        licence=OGL,
        source_url=(
            "https://digital.nhs.uk/data-and-information/publications/"
            "statistical/nhs-payments-to-general-practice/england-2024-25"
        ),
        notes=(
            "6,375 GP practices nationally (196 Lancashire-postcode), named "
            "practice + address + postcode + PCN + Sub ICB, with ~40 payment-"
            "category columns and Total NHS Payments to General Practice. "
            "Public money to (mostly) private GP partnerships, per the "
            "candidate note. Candidate A2, verified 17 Aug 2026."
        ),
    ),
    dict(
        id="gambling_commission",
        name="Gambling Commission public register (business licence data)",
        hosts=["mac", "vps"],
        globs=[
            "raw/gambling_commission_businesses.csv",
            "raw/gambling_commission_licences.csv",
        ],
        snapshot_date=_mtime,
        as_at=None,
        licence=OGL,
        source_url="https://www.gamblingcommission.gov.uk/public-register/businesses/download",
        notes=(
            "OPERATOR licences (remote/non-remote betting, bingo, casino, "
            "arcade, lottery), NOT premises addresses: businesses.csv is "
            "Account Number + Licence Account Name only (2,656 rows), "
            "licences.csv adds licence number/status/type/activity/dates "
            "(4,518 rows). Premises-level betting shop/arcade addresses are "
            "NOT in this register (that is council licensing, SOURCES-"
            "CANDIDATES section A item 15, not attempted this session). "
            "Name-only join, no CRN in this extract. Candidate A4, verified "
            "17 Aug 2026."
        ),
    ),
    dict(
        id="coops_uk_open_data",
        name="Co-operatives UK open dataset (organisations)",
        hosts=["mac", "vps"],
        globs=["raw/coops_uk_open_data_*.csv"],
        snapshot_date=_from_name(r"coops_uk_open_data_(\d{4}-\d{2})", 1),
        as_at="2026-07 edition",
        licence="Open Data Commons Attribution License (ODC-BY v1.0), NOT OGL",
        source_url="https://www.uk.coop/resources/open-data",
        notes=(
            "1,941 co-operatives nationally (19 Lancashire LAD-matched). "
            "Registered Number + Registrar (Companies House or FCA) gives a "
            "clean CRN/society-number split (83 CH, 754 FCA nationally). "
            "Unusually rich geography: LAD, ward, LSOA, MSOA, parish and "
            "constituency codes AND names already resolved, plus SIC codes "
            "at 3 levels. Licence is ODC-BY v1.0, the first non-OGL/non-CC0 "
            "licence in either verification sweep: read the full terms "
            "before republication, attribution wording differs from OGL's. "
            "Candidate B1, verified 17 Aug 2026."
        ),
    ),
    dict(
        id="hmrc_casc",
        name="HMRC Community Amateur Sports Club (CASC) register",
        hosts=["mac", "vps"],
        globs=["raw/hmrc_casc_register.ods"],
        snapshot_date=_mtime,
        as_at=None,
        licence=OGL,
        source_url=(
            "https://www.gov.uk/government/publications/"
            "community-amateur-sports-clubs-casc-registered-with-hmrc--2"
        ),
        notes=(
            "8,126 clubs nationally (229 Lancashire-postcode). Organisation "
            "Name + address + postcode only, no registration number of any "
            "kind: name-only join throughout. Grassroots sports clubs "
            "invisible to the CH/CC register spine, per the candidate note. "
            "Candidate B2, verified 17 Aug 2026."
        ),
    ),
    dict(
        id="census_ts066_lsoa",
        name="Census 2021 TS066 economic activity status, LSOA",
        hosts=["mac", "vps"],
        globs=["raw/census_ts066_lsoa_2021.csv.gz"],
        snapshot_date=_mtime,
        as_at="Census Day, 21 March 2021",
        licence=OGL,
        source_url="https://www.nomisweb.co.uk/output/census/2021/census2021-ts066.zip",
        notes=(
            "35,672 LSOA rows, wide format (one column per economic-activity "
            "category, already broken out: employee/self-employed with and "
            "without employees, part-time/full-time, unemployed, "
            "economically inactive by reason). NOT available at LSOA via the "
            "ONS flexible-table API (dataset TS066 there is LTLA-only, "
            "confirmed this session: 'keywords: ltla' in its own metadata); "
            "the LSOA cut only exists in Nomis's separate bulk census output "
            "zip, which also contains OA/MSOA/LTLA/UTLA/region/country files "
            "not landed here. Static 2021 data, matches the existing house "
            "caveat: 'Sub-district growth exists ONLY via our firm-level "
            "iXBRL gazelle series' - this file is a population/labour-market "
            "denominator, not a business count, and self-employment here is "
            "a headcount of PEOPLE not registered businesses (compare "
            "carefully against the bpe/hmrc_spi sources, which are the "
            "business-registration side of the same self-employment "
            "question)."
        ),
    ),
    dict(
        id="insolvencies_by_la",
        name="Individual insolvencies by location, 2015 to 2025",
        hosts=["mac", "vps"],
        globs=[
            "raw/insolvencies_by_location_2015-2025.xlsx",
            "raw/insolvencies_by_ward_2015-2025.csv",
        ],
        snapshot_date=_mtime,
        as_at="2025 edition",
        licence=OGL,
        source_url=(
            "https://www.gov.uk/government/statistics/"
            "individual-insolvencies-by-location-age-and-gender-"
            "england-and-wales-2025"
        ),
        notes=(
            "Insolvency Service annual publication. insolvencies_by_ward "
            "csv: 30,278 rows, ward_code/ward_name/local_authority_code/"
            "local_authority_name/insolvency_type (Bankruptcy/DRO/IVA) x "
            "annual counts 2015-2025; 1,124 rows match a Lancashire LA name. "
            "These are AGGREGATE ward-level counts, not named-individual "
            "records (already aggregated by the Insolvency Service itself, "
            "not a re-aggregation of case-level data). DATA-INTEGRITY s3: "
            "MAY assert aggregate rates per LA/ward; MUST NOT assert "
            "anything about a person and MUST NEVER join toward the named "
            "Individual Insolvency Register (LEGAL.md red zone)."
        ),
    ),
    dict(
        id="ofsted_childcare",
        name="Ofsted childcare providers + children's social care providers",
        hosts=["mac", "vps"],
        globs=[
            "raw/ofsted_childcare_providers_*.csv",
            "raw/ofsted_childrens_social_care_*.ods",
        ],
        snapshot_date=_mtime,
        as_at="childcare: 30 June 2026 edition; social care: 2026 edition (year to 31 March 2026)",
        licence=OGL,
        source_url=(
            "https://www.gov.uk/government/statistical-data-sets/"
            "childcare-providers-and-inspections-management-information + "
            "https://www.gov.uk/government/statistics/"
            "childrens-social-care-in-england-2026"
        ),
        notes=(
            "Two related but distinct DfE/Ofsted provider registers. "
            "ofsted_childcare_providers_2026-06-30.csv: 60,009 rows "
            "(childminders, nurseries, out-of-school clubs), LA field "
            "present, but CHILDMINDER ADDRESSES ARE REDACTED (31,959 rows), "
            "matching the SOURCES-DELTA caveat exactly. "
            "ofsted_childrens_social_care_2026.ods: multi-sheet workbook, "
            "Provider_level_at_31_Mar_2026 sheet has 6,704 provider rows "
            "(children's homes, fostering agencies etc), URN + type + "
            "registration + inspection outcome columns, ADDRESSES REDACTED "
            "for children's homes (safeguarding). DATA-INTEGRITY s3: FHRS-"
            "style premises evidence rows may assert registration/inspection "
            "facts, never a wellbeing or safety judgement beyond the stated "
            "Ofsted rating."
        ),
    ),
    dict(
        id="grantnav_lancs",
        name="360Giving GrantNav, Lancashire grants",
        hosts=["mac", "vps"],
        globs=["raw/grantnav_lancashire_*.csv.gz"],
        snapshot_date=_from_name(r"grantnav_lancashire_(\d{4}-\d{2}-\d{2})"),
        as_at=None,
        licence="varies per row, see notes",
        source_url="https://grantnav.threesixtygiving.org/",
        notes=(
            "20,854 grants filtered server-side to "
            "additional_data.GNBestCountyName = Lancashire (the 'Best "
            "Available' location facet, same semantics as the site's other "
            "location fields). Fetched via the site's own Custom CSV export "
            "(recommended fields) from inside a real browser session: the "
            "search UI sits behind a JS proof-of-work challenge that blocks "
            "plain HTTP clients, so this file cannot be re-fetched by a "
            "simple script without a real browser. Licence is PER-ROW, not "
            "uniform: mix of OGL v3, CC-BY 4.0, CC-BY-SA 4.0 and CC0 "
            "depending on the funder ('License' column on every row) - read "
            "that column before republishing any individual grant. 5,294 "
            "rows (25%) carry a Company Number (CRN), 4,954 (24%) a Charity "
            "Number: materially better join-key coverage than the "
            "lottery_grants source, which has neither. Distinct dataset from "
            "lottery_grants (this is 360Giving's own aggregation across ALL "
            "participating funders, Lottery distributors included as a "
            "subset); identifiers on Lottery-sourced rows should be "
            "360G-prefixed and crosswalk to the lottery_grants source's own "
            "'identifier' field."
        ),
    ),
    dict(
        id="rsh_registered_providers",
        name="RSH registered providers of social housing",
        hosts=["mac", "vps"],
        globs=["raw/rsh_registered_providers_*.xlsx"],
        snapshot_date=_from_name(r"rsh_registered_providers_(\d{4}-\d{2}-\d{2})"),
        as_at="24 July 2026 edition",
        licence=OGL,
        source_url="https://www.gov.uk/government/publications/registered-providers-of-social-housing",
        notes=(
            "1,580 providers, updated monthly. Columns: Organisation name, "
            "Registration number, Registration date, Designation "
            "(profit/non-profit), Corporate form, Notes. NO address or "
            "postcode column, so any join to a Lancashire subset is "
            "NAME-ONLY (probabilistic tier). Registration number is the "
            "RSH's own scheme (C/L/LH prefixes), not a CRN."
        ),
    ),
    dict(
        id="payment_practices",
        name="Payment practices reporting (large companies, CRN-bearing)",
        hosts=["mac", "vps"],
        globs=["raw/payment_practices_*.csv.gz"],
        snapshot_date=_from_name(r"payment_practices_(\d{4}-\d{2}-\d{2})"),
        as_at=None,
        licence=OGL,
        source_url="https://check-payment-practices.service.gov.uk/export/csv/",
        notes=(
            "353,347 rows, whole-file export, continuous filing. 'Company "
            "number' column is a CRN, deterministic join to the CH register "
            "spine. Statutory self-reported filings under the Reporting on "
            "Payment Practices and Performance Regulations 2017 (large "
            "companies/LLPs only, so this is a size-filtered slice, not "
            "whole-economy). Per DATA-INTEGRITY s3: MAY assert the firm's own "
            "statutory filing figures; MUST NOT assert our verification of "
            "them, must be captioned 'as filed' (self-reported, unaudited)."
        ),
    ),
    dict(
        id="nndr3_outturn",
        name="NNDR3 outturn 2025 to 2026 (national non-domestic rates collected by councils)",
        hosts=["mac", "vps"],
        globs=[
            "raw/nndr3_2025-26_consolidated_values.csv",
            "raw/nndr3_2025-26_metadata.csv",
        ],
        snapshot_date=_mtime,
        as_at="2025-26 outturn, published 2026-07-22",
        licence=OGL,
        source_url=(
            "https://www.gov.uk/government/statistics/"
            "national-non-domestic-rates-collected-by-councils-in-"
            "england-2025-to-2026"
        ),
        notes=(
            "301 rows, one per English billing authority plus totals, 2025-26 "
            "outturn (the ACTUAL year, not the forecast edition). Carries "
            "SBRR (small business rate relief, sbrr_* columns) and other "
            "relief columns per council, the NNDR3 reliefs signal this "
            "session's brief asked for. All 14 Lancashire LADs present under "
            "an 'Ecode' column that is NOT the ONS GSS code scheme (for "
            "example E2333 for Burnley, not E07000117): a lookup table "
            "against geo_crosswalk.json is needed before joining, not a "
            "direct code match. DATA-INTEGRITY s3: NNDR3 reliefs may assert "
            "reliefs granted per COUNCIL, aggregate; never per-firm relief."
        ),
    ),
    dict(
        id="oscr_register",
        name="OSCR Scottish Charity Register, full download",
        hosts=["mac", "vps"],
        globs=["raw/oscr_register_*.csv.gz"],
        snapshot_date=_from_name(r"oscr_register_(\d{4}-\d{2}-\d{2})"),
        as_at=_from_name(r"oscr_register_(\d{4}-\d{2}-\d{2})"),
        licence=OGL + " (OSCR, with attribution required, see notes)",
        source_url=(
            "https://www.oscr.org.uk/about-charities/search-the-register/"
            "download-the-scottish-charity-register/"
        ),
        notes=(
            "Full Scottish Charity Register, 24,983 rows, updated daily. Not "
            "pre-filtered to cross-border charities: any charity registered "
            "in Scotland appears here regardless of English presence, so "
            "cross-border (CCEW+OSCR dual-registered) charities are a SUBSET "
            "to be identified downstream by name+number match against the "
            "existing charities_lancs.json, not a separate download. "
            "DATA-INTEGRITY s2 dedup rule applies: one entity, two "
            "registers, dedupe on CRN first, charity no second, name+postcode "
            "last. Attribution required verbatim: 'Contains information from "
            "the Scottish Charity Register supplied by the Office of the "
            "Scottish Charity Regulator and licensed under the Open "
            "Government Licence v.3.0.' Terms also forbid direct marketing "
            "use and forbid presenting a derived list as an alternative "
            "'Scottish Charity Register'."
        ),
    ),
    dict(
        id="charity_derived",
        name="Charity Commission derived tables: removed, merged, CIO flags",
        hosts=["mac", "vps"],
        globs=[
            "raw/charity_removed.csv",
            "raw/charity_cio_flags.csv",
            "raw/charity_merged_register.csv",
        ],
        snapshot_date=_mtime,
        as_at=None,
        licence=OGL,
        source_url=(
            "https://register-of-charities.charitycommission.gov.uk/register/"
            "full-register-download (event_history, charity extracts) + "
            "https://www.gov.uk/government/publications/"
            "register-of-merged-charities"
        ),
        notes=(
            "Three derived tables extracted from the CC bulk plus one "
            "separate gov.uk publication. charity_removed.csv: 217,798 "
            "event_type=Removed rows from publicextract.charity_event_history "
            "(NOT in the main charity.zip already in bronze), carrying the "
            "removal REASON text (for example 'Does not operate') that the "
            "main charity extract does not expose; unsurfaced dissolution "
            "signal per DATA-INTEGRITY s5/E. charity_cio_flags.csv: "
            "charity_type + charity_is_cio + cio_is_dissolved for all 397,671 "
            "scanned charity register rows, 44,527 flagged CIO; a CIO is NOT "
            "a Companies Act company (DATA-INTEGRITY s2). "
            "charity_merged_register.csv: gov.uk 'Register of merged "
            "charities', July 2026 edition, 5,833 transferor/transferee "
            "pairs with vesting/transfer/registration dates, a SEPARATE "
            "publication not part of the daily bulk. National scope "
            "throughout, not yet filtered to Lancashire."
        ),
    ),
    dict(
        id="lottery_grants",
        name="National Lottery grants (DCMS), full pull",
        hosts=["mac", "vps"],
        globs=["raw/lottery_grants_full.jsonl"],
        snapshot_date=_mtime,
        as_at=None,
        licence=OGL + " (Crown copyright, confirmed 14 Aug 2026)",
        source_url="https://nationallottery.dcms.gov.uk/api/v1/grants/",
        notes=(
            "693,657 grants, GBP 48,181,641,876 total, 1994 to date, one row "
            "per line (JSONL). No recipient postcode, charity number or CRN: "
            "recipient join is name-only (probabilistic tier). CRITICAL: the "
            "location field (ward/uk_constituency/local_authority/region) is "
            "benefit-area where known with a fallback to the AWARDING body's "
            "HQ, 'often... London' per DCMS's own About page. Per-area totals "
            "built from this file are contaminated by an unmeasurable amount "
            "and may not publish without that caption. DCMS states this is "
            "NOT official statistics and is not manually validated: "
            "distributing bodies self-upload. See DATA-INTEGRITY s7.2."
        ),
    ),
    dict(
        id="voa_stock",
        name="VOA non-domestic rating stock of properties, March 2026",
        hosts=["mac", "vps"],
        globs=[
            "raw/voa_stock_of_properties_2026.zip",
            "raw/voa_stock_scat_la_2026.zip",
        ],
        snapshot_date=_mtime,
        as_at="March 2026 list",
        licence=OGL,
        source_url=(
            "https://www.gov.uk/government/statistics/"
            "non-domestic-rating-stock-of-properties-march-2026"
        ),
        notes=(
            "The aggregate publication, NOT the 2026 rating list itself (that "
            "is a separate, NOT-OGL source, see the voa_2023_list entry's "
            "warning and DATA-INTEGRITY s5). Counts and RV by SCat (sector "
            "category, hundreds of property-type columns) at local-authority "
            "level, suppressed as [c] below a disclosure threshold. 15 rows "
            "match a Lancashire LAD name in the SCat-by-LA file (14 LADs, one "
            "substring collision to check when parsed). Gives premises "
            "counts and RV, never occupation or who trades there "
            "(DATA-INTEGRITY s3 VOA rating list row)."
        ),
    ),
    dict(
        id="gleif_lei",
        name="GLEIF LEI golden copy, UK slice",
        hosts=["mac", "vps"],
        globs=["raw/gleif_lei_uk_slice.csv.gz"],
        snapshot_date=_mtime,
        as_at="2026-08-17 golden copy edition",
        licence="CC0",
        source_url=(
            "https://goldencopy.gleif.org/storage/golden-copy-files/2026/08/17/"
            "1264745/20260817-0000-gleif-goldencopy-lei2-golden-copy.csv.zip"
        ),
        notes=(
            "Filtered from the global LEI2 golden copy (3,403,779 rows, "
            "476MB zipped) down to rows whose "
            "Entity.RegistrationAuthority.RegistrationAuthorityID is "
            "RA000585, RA000586 or RA000587 (Companies House E&W / Scotland / "
            "NI): 117,318 rows. The unfiltered global file was not kept, disk "
            "space on the Mac does not hold it. Entity.RegistrationAuthority."
            "RegistrationAuthorityEntityID carries the CRN for these rows, the "
            "join key. Significance is capital-markets touchpoint only, per "
            "DATA-INTEGRITY s3."
        ),
    ),
    dict(
        id="els_high_growth",
        name="ONS Explore Local Statistics: high-growth businesses indicator",
        hosts=["mac", "vps"],
        globs=["raw/els_high_growth_businesses.csv"],
        snapshot_date=_mtime,
        as_at=None,
        licence=OGL,
        source_url=(
            "https://www.ons.gov.uk/explore-local-statistics/api/v1/data.csv"
            "?indicator=high-growth-businesses&time=all"
        ),
        notes=(
            "Percentage of businesses with average employment growth >20%/yr "
            "over a 3-year period, per ONS local indicators definition. All "
            "periods 2019 to 2023, all LADs plus England and Lancashire county "
            "aggregate rows. LAD floor only, matches the existing gazelle-engine "
            "caveat: no official sub-LAD growth stat exists."
        ),
    ),
    dict(
        id="fsa_approved_establishments",
        name="FSA approved food establishments (meat/dairy/fish/egg plants)",
        hosts=["mac", "vps"],
        globs=["raw/fsa_approved_establishments_*.csv"],
        snapshot_date=_from_name(r"fsa_approved_establishments_(\d{4}-\d{2}-\d{2})"),
        as_at="1 August 2026 edition",
        licence=OGL,
        source_url="https://ckan.publishing.service.gov.uk/dataset/approved-food-establishments",
        notes=(
            "5,336 rows nationally (177 match a Lancashire GeographicLocalAuthority "
            "value), monthly editions. AppNo + TradingName + full address + Postcode, "
            "plus ~50 Part A/B activity-section flag columns (meat, dairy, fish, eggs, "
            "gelatine, collagen etc) and species/establishment-type flags (Cutting "
            "Plant, Cold Store, Slaughterhouse, Factory Vessel...). Distinct from FHRS "
            "(already in bronze): this is the EU/UK approval-number register for "
            "high-risk food premises, which FHRS ratings do not cover. Candidate A7, "
            "verified 17 Aug 2026; the page's own resource list was checked via the "
            "CKAN API to confirm this is the live August 2026 edition, not a stale "
            "cached filename."
        ),
    ),
    dict(
        id="mmo_fishing_vessels",
        name="MMO UK fishing vessel lists (under-10m and over-10m)",
        hosts=["mac", "vps"],
        globs=[
            "raw/mmo_under10m_vessel_list_*.xlsx",
            "raw/mmo_over10m_vessel_list_*.xlsx",
        ],
        snapshot_date=_from_name(r"vessel_list_(\d{4}-\d{2})"),
        as_at="1 August 2026 edition",
        licence=OGL,
        source_url=(
            "https://www.gov.uk/government/statistical-data-sets/"
            "vessel-lists-10-metres-and-under + "
            "https://www.gov.uk/government/statistical-data-sets/"
            "vessel-lists-over-10-metres"
        ),
        notes=(
            "Two monthly workbooks, unioned as one source: under-10m (3,517 rows, "
            "55 Fleetwood-administrative-port) and over-10m (1,000 rows, 20 "
            "Fleetwood). Administrative port, home port, PLN, vessel name, RSS "
            "number, licence number, PO membership, length/tonnage/engine power, "
            "year built, hull material, licence category, shellfish/scallop licence "
            "flags. Duplicate rows exist per vessel where multiple licence "
            "categories apply (stated in the workbook's own notes row). "
            "Administrative/home port fields are the Lancashire join surface "
            "(Fleetwood). Candidate A8, verified 17 Aug 2026."
        ),
    ),
    dict(
        id="heritage_fund_awards",
        name="National Lottery Heritage Fund grant awards (own 360Giving export)",
        hosts=["mac", "vps"],
        globs=["raw/heritage_fund_nlhf_360data_*.csv"],
        snapshot_date=_mtime,
        as_at="edition to 30 June 2026",
        licence=OGL,
        source_url="https://www.heritagefund.org.uk/about/insight/open-data",
        notes=(
            "Three CSVs covering 1995-2004, 2004-2013 and 2013-2026 (49,041 rows "
            "total, 1,167 Lancashire-name-matched on Recipient Org:Location:Name or "
            "Beneficiary Location:Name). NOT redundant with the existing "
            "lottery_grants source (DCMS API): this is the Heritage Fund's own "
            "360Giving export and carries BOTH Recipient Org:Location AND "
            "Beneficiary Location as separate geographic-code fields, which the "
            "main DCMS API does not split out, plus Charity Number and Company "
            "Number columns the DCMS API lacks entirely. Identifiers are "
            "360G-NLHF-prefixed and should crosswalk to matching rows inside the "
            "lottery_grants source's own 'identifier' field where both exist. "
            "Candidate B4, verified 17 Aug 2026."
        ),
    ),
    dict(
        id="sport_england_grants",
        name="Sport England grant awards, April 2009 to March 2022 (360Giving)",
        hosts=["mac", "vps"],
        globs=["raw/sport_england_grants_*.xlsx"],
        snapshot_date=_mtime,
        as_at="April 2009 to March 2022 edition",
        licence=OGL + " (attribution string required, see notes)",
        source_url="https://www.sportengland.org/our-work/open-data/",
        notes=(
            "35,916 rows nationally, not yet Lancashire-filtered. Recipient Org "
            "Postal Code AND Latitude/Longitude present (richer geography than "
            "most 360Giving exports), plus up to 4 Beneficiary Location fields "
            "with geographic codes. Coverage stops at March 2022: no newer edition "
            "was found on the open-data page as at 17 Aug 2026, so treat as a "
            "closed historical window, not a live feed, until a refreshed file "
            "appears. Mandatory attribution string quoted verbatim on the source "
            "page: 'Contains public sector grants data from Sport England, <year "
            "of publication>, licensed under the Open Government Licence v3.0, "
            "accessed on <DD/MM/YY>.' Candidate B5 (grants half only; the Active "
            "Places facility register is CC BY 4.0 on an ArcGIS Hub platform with "
            "no single-file/API export, not attempted this session). Verified 17 "
            "Aug 2026."
        ),
    ),
    dict(
        id="gro_places_of_worship",
        name="GRO certified places of worship (HM Passport Office)",
        hosts=["mac", "vps"],
        globs=["raw/gro_places_of_worship_*.ods"],
        snapshot_date=_mtime,
        as_at="18 March 2015 edition",
        licence=OGL,
        source_url=(
            "https://www.data.gov.uk/dataset/143fdbae-daae-45b1-b970-061dcaaf109f/"
            "places-of-worship"
        ),
        notes=(
            "Places of worship registered for marriages under the Marriage Act. "
            "STALE: the file is the 18 March 2015 edition and no newer resource "
            "was found on data.gov.uk after a direct search; any registrations "
            "since 2015 (new-build mosques, gurdwaras, churches etc) are absent "
            "and this must ship with a hard vintage caption, never as a current "
            "count. Candidate B7, verified 17 Aug 2026."
        ),
    ),
    dict(
        id="lancs_community_foundation_grants",
        name="Community Foundation for Lancashire and Merseyside grant awards (360Giving)",
        hosts=["mac", "vps"],
        globs=["raw/lancs_community_foundation_grants_*.xlsx"],
        snapshot_date=_from_name(r"lancs_community_foundation_grants_(\d{4}-\d{2})"),
        as_at="quarterly editions 2022-23 through 2025-26",
        licence="Creative Commons Attribution 4.0 International (CC BY 4.0), NOT OGL",
        source_url="https://lancsfoundation.org.uk/grants-awarded",
        notes=(
            "Four annual 360Giving workbooks (2022-23 through 2025-26), 1,702 "
            "grants total, not yet Lancashire-vs-Merseyside split (the foundation "
            "covers both counties jointly; location fields need parsing to "
            "isolate Lancashire). Grants to individuals and direct donations are "
            "deliberately excluded from the published data (the foundation's own "
            "privacy policy, stated on the source page). Licence is CC BY 4.0, "
            "quoted verbatim on the page: 'This work is licensed under the "
            "Creative Commons Attribution 4.0 International License... the data "
            "must [be attributed]'; NOT OGL, a step beyond the usual licence check "
            "this session otherwise used (same class as coops_uk_open_data). "
            "Candidate B9, verified 17 Aug 2026."
        ),
    ),
    dict(
        id="givefood_foodbanks",
        name="Give Food UK food bank locations (Trussell + independent + IFAN)",
        hosts=["mac", "vps"],
        globs=["raw/givefood_foodbanks.csv"],
        snapshot_date=_mtime,
        as_at=None,
        licence="no formal open licence; good-faith reuse with attribution, see notes",
        source_url="https://www.givefood.org.uk/api/",
        notes=(
            "4,639 rows (name, address, postcode, ward, district, parliamentary "
            "constituency + MP, charity number where registered, network: "
            "Trussell/Independent/IFAN, closed flag), maintained by Give Food "
            "(registered charity 1188192) and mirrored to a public GitHub repo "
            "this session fetched from. No stated open licence: the source's own "
            "'Rules' page says only 'Do good with the data, and credit us with a "
            "link when you use it,' plus a request not to reorder/modify item "
            "lists where present. Treat as good-faith reuse with mandatory "
            "attribution to Give Food, never claim OGL or any formal licence "
            "class for this source. SAFEGUARDING CAVEAT: these are addresses of a "
            "vulnerable-people-facing service; apply the same presentation care "
            "as other such layers, no added precision beyond what the food banks "
            "themselves publish, and check for any delisting requests before "
            "publication. Candidate B10, verified 17 Aug 2026; IFAN's own site is "
            "a postcode-lookup tool only with no bulk export (BLOCKED-scrape-"
            "only, not attempted directly, this file already merges IFAN-network "
            "entries via Give Food's own 'network' column)."
        ),
    ),
    dict(
        id="hmrc_nmw_naming_round23",
        name="HMRC National Minimum Wage naming scheme, Round 23 (March 2026)",
        hosts=["mac", "vps"],
        globs=["raw/hmrc_nmw_naming_round23.xlsx"],
        snapshot_date=_mtime,
        as_at="Round 23, published 19 March 2026",
        licence=OGL,
        source_url=(
            "https://www.gov.uk/government/news/"
            "hundreds-of-employers-handed-penalties-for-illegally-underpaying-workers"
        ),
        notes=(
            "388 named employers, one round only (historical rounds are separate "
            "editions not pulled this session). Employer/Company name, Trading "
            "Name, Partial Postcode, Region, Local Authority, Constituency, LPC "
            "Low Paying Sector, ONS SIC sector/division/activity, Arrears, Number "
            "of Workers, arrears period. 14 rows match a Lancashire Local "
            "Authority. NAMED-EMPLOYER REGISTER: DATA-INTEGRITY / LEGAL.md amber, "
            "register-verbatim facts only, ingest for internal evidence, nothing "
            "publishes without the solicitor-cleared wording pattern. Candidate "
            "A14b, verified 17 Aug 2026. The companion HMRC deliberate tax "
            "defaulters list (A14a) is OGL but HTML-table-only with no bulk file, "
            "not ingested this session."
        ),
    ),
    dict(
        id="ocds",
        name="OCDS contract notices cache (supplier identifiers)",
        hosts=["mac", "vps"],
        globs=["raw/ocds_cache.jsonl"],
        snapshot_date=_mtime,
        as_at=None,
        licence=OGL,
        source_url="https://www.find-tender.service.gov.uk/",
        notes=(
            "Supplier identifier scheme is GB-COH, which is why these 329 "
            "mappings become deterministic crosswalk edges in M3."
        ),
    ),

    # --- Companies House derived captures (M2) -----------------------------
    # These three are NOT raw source files and are marked so. Companies House
    # deletes the PSC snapshot and the monthly accounts archives from its own
    # download index on a short retention, and our fetchers delete the zips
    # after parsing to keep the disk inside its guard. The extractor output is
    # therefore the ONLY immutable capture of those editions that exists
    # anywhere, which is exactly the audit-trail job bronze does. Registering
    # them here keeps the M2 rule intact (silver reads bronze, never a live
    # fetch) instead of pointing silver at a mutable working directory.
    # capture="derived-extract" marks the distinction so no later session
    # mistakes them for as-published bytes.
    dict(
        id="ch_psc_extract",
        name="Companies House PSC snapshot, parsed extract",
        hosts=["vps"],
        capture="derived-extract",
        globs=[
            "/opt/observatory/out/corporate_psc_all.jsonl.gz",
            "/opt/observatory/out/lancs_psc.jsonl.gz",
            "/opt/observatory/out/psc_summary.json",
        ],
        snapshot_date=_mtime,
        as_at=None,
        licence=OGL,
        source_url="https://download.companieshouse.gov.uk/en_pscdata.html",
        notes=(
            "Parsed from the daily PSC snapshot by refresh_psc.py, which "
            "deletes the source zip after use. lancs_psc holds every PSC kind "
            "for Lancashire-registered companies; corporate_psc_all holds "
            "every corporate-entity PSC nationally, which is the group "
            "structure input. psc_summary.json carries the line counts the "
            "silver checks assert against."
        ),
    ),
    dict(
        id="ch_accounts_ixbrl",
        name="Companies House iXBRL accounts, parsed extract",
        hosts=["vps"],
        capture="derived-extract",
        globs=[
            "/opt/observatory/out/lancs_accounts.jsonl.gz",
            "/opt/observatory/out/accounts_progress.json",
        ],
        snapshot_date=_mtime,
        as_at=None,
        licence=OGL,
        source_url="https://download.companieshouse.gov.uk/en_accountsdata.html",
        notes=(
            "Parsed from the monthly Accounts_Monthly_Data archives by "
            "etl_accounts.py. Companies House keeps only 13 monthly archives "
            "online, so an unparsed month is gone for good. No file-level "
            "asAt: the reference date is record-level (period_end), per "
            "DATA-INTEGRITY s4. accounts_progress.json is the per-month "
            "filing and parse-failure ledger."
        ),
    ),
    dict(
        id="postcode_lad_cache",
        name="Postcode to local authority cache (postcodes.io lookups)",
        hosts=["vps", "mac"],
        capture="derived-extract",
        globs=["processed/_postcode_lad_cache.json"],
        snapshot_date=_mtime,
        as_at=None,
        licence=OGL,
        source_url="https://postcodes.io/",
        notes=(
            "_common.resolve_postcodes accumulates postcodes.io answers here "
            "and every fetcher that places a record by postcode reads it, "
            "including fetch_gazette, which is why a Gazette notice's LAD "
            "cannot be reproduced from ONSPD alone. Landed in bronze in M4 so "
            "the notices mart has an immutable, hash-pinned copy of the exact "
            "lookup the published edition used. F1 replaces this cache with "
            "the ONSPD plus OS Open UPRN location hierarchy; until then it is "
            "the geocoding layer and it is a cache, not a register."
        ),
    ),
    dict(
        id="ch_accounts_api_backfill",
        name="Companies House accounts, API backfill extract",
        hosts=["vps"],
        capture="derived-extract",
        globs=[
            "/opt/observatory/out/lancs_accounts_backfill.jsonl",
            "/opt/observatory/out/backfill_progress.json",
        ],
        snapshot_date=_mtime,
        as_at=None,
        licence=OGL,
        source_url="https://api.company-information.service.gov.uk/",
        notes=(
            "backfill_accounts.py pulls filings the monthly archives never "
            "carried, straight from the CH REST API, and writes them with "
            "filed_zip='api-backfill'. Registered separately from "
            "ch_accounts_ixbrl because it is a different capture with a "
            "different provenance, and because the site's consumers read it "
            "AFTER the archive extract, which is what decides which filing "
            "wins for a period. Landed in bronze in M4: it was missing from "
            "the M2 registry, so silver under-covered the accounts the site "
            "actually publishes from by 1,377 rows."
        ),
    ),
    dict(
        id="gazette_notices",
        name="The Gazette corporate insolvency notices, Lancashire candidates",
        hosts=["vps"],
        capture="derived-extract",
        globs=[
            "/root/observatory-data/processed/gazette_corporate_all.jsonl",
            "/root/observatory-data/processed/gazette_lancs.json",
        ],
        snapshot_date=_mtime,
        as_at=None,
        licence=OGL + " / Crown copyright (The Gazette)",
        source_url=(
            "https://www.thegazette.co.uk/all-notices/notice/data.feed"
            "?category-code=24"
        ),
        notes=(
            "Candidate set gathered by fetch_gazette.py over postcode-centroid "
            "circles. The feed does NOT honour category-code=24 when the "
            "location parameters are present, so the geo search returns "
            "category 25 personal insolvency, 29 deceased estates and 16 "
            "planning notices too. fetch_gazette.py drops everything outside "
            "category 24 before writing the candidate file, and the silver "
            "builder repeats the rule as a second gate and asserts the dropped "
            "counts, which are zero once a post-fix candidate file is snapshotted."
        ),
    ),

    # --- Entity resolution inputs (M3) -------------------------------------
    # The scheme axis for the crosswalk. Vendored rather than fetched at build
    # time so a crosswalk edge minted today still resolves its scheme code in
    # five years, and pinned by hash so a silently republished codelist cannot
    # change what GB-COH means underneath an edge already written.
    dict(
        id="org_id_guide",
        name="org-id.guide organisation identifier codelist",
        hosts=["mac", "vps"],
        globs=["raw/org-id-guide-codelist-*.json"],
        snapshot_date=_from_name(r"codelist-(\d{4}-\d{2}-\d{2})"),
        as_at="2026-07-31",
        licence="Creative Commons Attribution (org-id.guide)",
        source_url="https://org-id.guide/download.json",
        notes=(
            "565 identifier lists, 25 of them GB. The codelist OCDS, BODS and "
            "360Giving all normatively reference, which is why our crosswalk "
            "uses its scheme codes rather than inventing an id vocabulary. "
            "asAt is the register repository commit the published codelist "
            "was generated from, github.com/org-id/register at "
            "f500b45f6c2a6f8b7c54eddbfb7628e73e3f01c7 dated 2026-07-31. "
            "Schemes this project needs and the codelist provides: GB-COH, "
            "GB-CHC, GB-SC, GB-NIC, GB-MPR, GB-NHS, GB-EDU, GB-UKPRN, "
            "GB-CASC. Sources with no org-id list (NNDR ratepayer rolls, "
            "council supplier ledgers, verified websites, FHRS) carry LOCAL "
            "scheme codes that are labelled local in the crosswalk and are "
            "never presented as standard codes."
        ),
    ),

    # The four production matchers whose decisions M3 migrates into crosswalk
    # edges. Each is our own output, not a publisher's bytes, so all four are
    # derived-extract captures: registering them is what lets the crosswalk
    # builder read a frozen, hashed copy instead of a working directory that
    # the next monthly run will overwrite.
    dict(
        id="matcher_pound",
        name="Pound council spend supplier resolution output",
        hosts=["vps", "mac"],
        capture="derived-extract",
        globs=[
            "processed/pound.json",
            "processed/supplier_universe.json",
            "processed/pound_review_queue.json",
        ],
        snapshot_date=_mtime,
        as_at=None,
        licence="Derived from council transparency data (OGL v3.0)",
        source_url="https://tompickup.co.uk/observatory/",
        notes=(
            "build_pound.py resolves council supplier ledger names to "
            "companies using an ALIASES canonicalisation table and an "
            "OVERRIDES map of hand-verified supplier-to-CRN decisions. Those "
            "decisions are the highest-precision tier we hold and they become "
            "deterministic crosswalk edges in M3."
        ),
    ),
    dict(
        id="matcher_websites",
        name="Verified company website matcher output",
        hosts=["vps", "mac"],
        capture="derived-extract",
        globs=[
            "processed/websites.jsonl",
            "processed/websites_summary.json",
            "processed/website_seeds.json",
        ],
        snapshot_date=_mtime,
        as_at=None,
        licence="Derived, mixed sources",
        source_url="https://tompickup.co.uk/observatory/",
        notes=(
            "verify_websites.py fetches a candidate domain and accepts it "
            "only on hard on-page evidence, principally the company number "
            "printed in the footer. It is the zero-false-positive tier and "
            "becomes deterministic crosswalk edges to a LOCAL website scheme."
        ),
    ),
    dict(
        id="matcher_nndr",
        name="NNDR ratepayer to company matcher output",
        hosts=["vps", "mac"],
        capture="derived-extract",
        globs=["processed/nndr_presence.json"],
        snapshot_date=_mtime,
        as_at=None,
        licence="Derived from council NNDR publications (OGL v3.0)",
        source_url="https://tompickup.co.uk/observatory/",
        notes=(
            "build_nndr.py matches published ratepayer names to companies. "
            "Hyndburn withholds names and Wyre publishes premises without "
            "them, so absence of a match is never absence of a premises "
            "(DATA-INTEGRITY s3)."
        ),
    ),
    dict(
        id="matcher_ocds",
        name="OCDS supplier organisation identifier mappings",
        hosts=["vps", "mac"],
        capture="derived-extract",
        globs=["processed/ocds_supplier_ids.json"],
        snapshot_date=_mtime,
        as_at=None,
        licence=OGL,
        source_url="https://www.find-tender.service.gov.uk/",
        notes=(
            "fetch_ocds_ids.py reads the organisation identifier block that "
            "Find a Tender and Contracts Finder publish against award "
            "notices. Those blocks already carry org-id scheme codes, which "
            "is why they drop into the crosswalk unchanged rather than being "
            "re-derived."
        ),
    ),
]

BY_ID = {s["id"]: s for s in SOURCES}


def resolve(value, path):
    """as_at / snapshot_date may be a literal, a callable, or None."""
    return value(path) if callable(value) else value


def for_host(h=None):
    h = h or host()
    return [s for s in SOURCES if h in s["hosts"]]
