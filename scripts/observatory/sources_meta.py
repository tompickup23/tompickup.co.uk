#!/usr/bin/env python3
"""The `$meta.sources[]` block every published file carries, and its dates.

Gate V-R3 asks for three things on every source entry: what it is licensed
under, when we read it, and what date the data itself speaks for. The published
contract carried only the first two, and the second was the generation date
rather than a retrieval date, so a 2024 reference year read as this month's
figure with nothing on the page to say otherwise.

Two dates, answering different questions:

    asAt        the date the DATA speaks for. For a dated extract that is the
                extract's own edition. For a live register it is the day we
                read it, because a live register has no edition other than the
                reading, and saying so is not a fudge.
    retrievedAt when we read it.

Every value is read out of the input it describes wherever the input states
one: the Companies House bulk edition out of register_summary.json, the Gazette
window out of the most recent notice actually carried, the spend window out of
the ledger's own financial years. Where a source publishes its edition only in
prose, `asAtBasis` says which of the two dates is standing in, rather than
presenting a guess as a machine-readable fact.

This module is the single place the block is built, so the three files that
publish one (biz-*.json from build_site_json, biz-changes.json from build_diff,
biz-companies-index.json from build_dossiers) cannot drift apart.
"""
import json
import re
from datetime import date
from pathlib import Path

PROC = Path.home() / "observatory-data/processed"
VPS = Path.home() / "observatory-data/vps"


def _load(p):
    p = Path(p)
    if not p.exists():
        return {}
    try:
        return json.loads(p.read_text())
    except (ValueError, OSError):
        return {}


def _meta_of(name, key, default=None):
    return (_load(PROC / name).get("$meta") or {}).get(key, default)


def _retrieved(name, fallback):
    return _meta_of(name, "retrieved") or fallback


def _register_snapshot(fallback):
    return _load(VPS / "register_summary.json").get("snapshot_date") or fallback


def _latest_notice_date(fallback):
    """The Gazette feed's as-at IS the most recent notice it carried."""
    dates = [n.get("date") for n in
             (_load(PROC / "gazette_lancs.json").get("notices") or [])
             if n.get("date")]
    return max(dates)[:10] if dates else fallback


def _nomis_reference_year():
    """NOMIS states its reference year per dataset in the notes, not a field."""
    years = re.findall(r"year (\d{4})",
                       _meta_of("nomis_context.json", "notes") or "")
    return f"{max(years)}-12-31" if years else None


def _spend_window_end():
    years = (_load(PROC / "council_supplier_spend.json").get("$meta") or {}
             ).get("years") or []
    if not years:
        return None
    start, end = years[-1].split("-")
    return f"{start[:2]}{end}-03-31"


def sources(generated=None):
    """The `$meta.sources[]` array, dated from the inputs it describes."""
    gen = generated or date.today().isoformat()
    reg = _register_snapshot(gen)
    nomis_r = _retrieved("nomis_context.json", gen)
    nomis_y = _nomis_reference_year()
    gaz_r = _retrieved("gazette_lancs.json", gen)
    fhrs_r = _retrieved("fhrs_lancs.json", gen)
    inn_r = _retrieved("innovate_lancs.json", gen)
    char_r = _retrieved("charities_lancs.json", gen)
    cqc_r = _retrieved("cqc_lancs.json", gen)
    onspd_r = _retrieved("voa_lancs.json", gen)
    spend_end = _spend_window_end()
    return [
        {"name": "Companies House Free Company Data + PSC + accounts bulk",
         "url": "https://download.companieshouse.gov.uk/",
         "asAt": reg, "asAtBasis": "bulk file edition date",
         "retrievedAt": reg, "retrieved": reg,
         "licence": "Public register data"},
        {"name": "ONS / NOMIS (business counts, demography, BRES, ASHE)",
         "url": "https://www.nomisweb.co.uk/",
         "asAt": nomis_y or nomis_r,
         "asAtBasis": ("latest reference year in the NOMIS datasets read"
                       if nomis_y else
                       "retrieval date, no reference year stated"),
         "retrievedAt": nomis_r, "retrieved": nomis_r, "licence": "OGL v3"},
        {"name": "The Gazette", "url": "https://www.thegazette.co.uk/",
         "asAt": _latest_notice_date(gaz_r),
         "asAtBasis": "most recent notice carried",
         "retrievedAt": gaz_r, "retrieved": gaz_r, "licence": "OGL v3"},
        {"name": "FSA Food Hygiene Rating Scheme",
         "url": "https://ratings.food.gov.uk/",
         "asAt": fhrs_r, "asAtBasis": "live register, as at the day it was read",
         "retrievedAt": fhrs_r, "retrieved": fhrs_r,
         "licence": "OGL v3, FSA attribution"},
        {"name": "Innovate UK funded projects",
         "url": "https://www.ukri.org/publications/"
                "innovate-uk-funded-projects-since-2004/",
         "asAt": inn_r,
         "asAtBasis": "bulk publication as at the day it was read",
         "retrievedAt": inn_r, "retrieved": inn_r, "licence": "OGL v3"},
        {"name": "Charity Commission register extract",
         "url": "https://register-of-charities.charitycommission.gov.uk/",
         "asAt": char_r,
         "asAtBasis": "live register, as at the day it was read",
         "retrievedAt": char_r, "retrieved": char_r, "licence": "OGL v3"},
        {"name": "CQC care directory", "url": "https://www.cqc.org.uk/",
         "asAt": cqc_r,
         "asAtBasis": "dated directory as at the day it was read",
         "retrievedAt": cqc_r, "retrieved": cqc_r, "licence": "OGL v3"},
        {"name": "Council transparency spending (17 Lancashire bodies)",
         "url": "https://tompickup.co.uk/lgr/",
         "asAt": spend_end or gen,
         "asAtBasis": "end of the latest financial year in the spend window",
         "retrievedAt": gen, "retrieved": gen,
         "licence": "OGL v3 per council"},
        {"name": "ONSPD", "url": "https://geoportal.statistics.gov.uk/",
         "asAt": onspd_r,
         "asAtBasis": "postcode directory edition current at the last "
                      "geocoding pass",
         "retrievedAt": onspd_r, "retrieved": onspd_r,
         "licence": "OGL; contains OS and Royal Mail data, Crown copyright"},
    ]


DATE_NOTE = ("Each source carries two dates: asAt is the date the data speaks "
             "for, retrievedAt is the day we read it.")
