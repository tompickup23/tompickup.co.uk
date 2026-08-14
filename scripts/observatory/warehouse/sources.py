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
]

BY_ID = {s["id"]: s for s in SOURCES}


def resolve(value, path):
    """as_at / snapshot_date may be a literal, a callable, or None."""
    return value(path) if callable(value) else value


def for_host(h=None):
    h = h or host()
    return [s for s in SOURCES if h in s["hosts"]]
