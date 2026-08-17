#!/usr/bin/env python3
"""Gate V-R1: per-source staleness budgets, measured off bronze.

DATA-INTEGRITY s4 gives a budget table in prose. This is that table as code,
plus the two things prose cannot do: it names every source the registry knows
about, and it says which axis each age was measured on.

**Which date is the age measured against.** s4 defines `asAt` as the source's
own reference date and `retrievedAt` as when we fetched it. Staleness is a
statement about the world, not about our disk, so the age is measured against
asAt wherever the source states one. Where it does not (or states something
that is not a calendar date, like the ONSPD edition tag `MAY_2026` or the
org-id.guide register commit), the age falls back to the snapshot date and the
report says `measuredOn: snapshotDate` so nobody reads a fetch date as a
publication date.

**Four modes, because "stale" does not mean one thing.**

  fail    a live or monthly feed. WARN at budget, FAIL at 2x budget. This is
          V-R1 proper.
  watch   an annual-ish statistic. There is no failure age: a 2024 figure is
          not wrong in 2026, it is a 2024 figure. The obligation is the visible
          year label (s4 vintage rule 3), which is a schema gate, not a clock.
  vintage a source with no successor edition (the 2015 places-of-worship file,
          the 2021 census, a one-off naming round). No budget; the vintage
          label is mandatory on any use.
  pinned  pinned to an upstream commit or a superseded list on purpose. Ageing
          is the intent, not a defect.

A source in `fail` mode with no bronze partition at all on this host is a
FAILURE, not a zero. That distinction is the whole lesson of the OCDS incident
(DATA-INTEGRITY s11.2): an empty input produced an empty result that looked
like an honest zero and no gate fired for weeks.

Usage:
    staleness.py                       # report + exit 1 on any FAIL
    staleness.py --report-only         # always exit 0
    staleness.py --out /path/staleness.json
    staleness.py --as-of 2026-08-17    # pin the clock (the clock is an input)
"""
import argparse
import datetime as _dt
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import sources as S  # noqa: E402
import driver as D  # noqa: E402

# (mode, budgetDays, cadence, rulebookRow)
#
# rulebookRow names the s4 line the budget comes from. Where s4 has no line for
# a source, the row reads "not in s4" and the nearest class is used, stated
# rather than implied: an invented budget that looks official is worse than an
# honest analogy.
BUDGETS = {
    # --- Companies House family ---------------------------------------
    "ch_register": ("fail", 45, "monthly", "CH snapshot, PSC"),
    "ch_psc": ("fail", 45, "monthly", "CH snapshot, PSC"),
    "ch_psc_extract": ("fail", 45, "monthly", "CH snapshot, PSC (derived extract)"),
    "ch_accounts_ixbrl": ("fail", 45, "monthly", "CH snapshot, PSC (accounts archive)"),
    "ch_accounts_api_backfill": ("fail", 45, "live API", "CH API (dossier status checks)"),
    "gazette_notices": ("fail", 45, "monthly feed", "Gazette, strike-offs"),
    # --- geography ------------------------------------------------------
    "onspd": ("fail", 120, "quarterly", "ONSPD / Code-Point"),
    "postcode_lad_cache": ("pinned", None, "derived cache",
                           "not in s4; a postcodes.io lookup cache that cannot "
                           "be re-derived, pinned by construction"),
    # --- premises and regulators ---------------------------------------
    "fhrs": ("fail", 60, "rolling", "FHRS"),
    "cqc": ("fail", 60, "rolling", "not in s4; FHRS class (rolling register)"),
    "gias": ("fail", 60, "rolling", "not in s4; FHRS class (rolling register)"),
    "nhsbsa_contractors": ("fail", 30, "nightly", "ODS"),
    "gambling_commission": ("fail", 45, "live register", "EA waste carriers class"),
    "fsa_approved_establishments": ("fail", 120, "quarterly",
                                    "not in s4; ONSPD class (quarterly file)"),
    "mmo_fishing_vessels": ("fail", 60, "monthly",
                            "not in s4; Innovate UK class (~monthly file)"),
    "ofsted_childcare": ("fail", 120, "quarterly",
                         "not in s4; ONSPD class (quarterly file)"),
    "givefood_foodbanks": ("fail", 60, "rolling",
                           "not in s4; FHRS class (rolling register)"),
    # --- VCSE ------------------------------------------------------------
    "charity_commission": ("fail", 30, "daily", "CC bulk, OSCR"),
    "charity_derived": ("fail", 30, "daily", "CC bulk, OSCR (derived tables)"),
    "oscr_register": ("fail", 30, "daily", "CC bulk, OSCR"),
    "rsh_registered_providers": ("fail", 60, "monthly-ish",
                                 "not in s4; FCA mutuals class, tightened "
                                 "because the file republishes monthly"),
    "hmrc_casc": ("watch", None, "rolling list",
                  "not in s4; a cumulative register with no edition date"),
    # --- money in ---------------------------------------------------------
    "innovate_uk": ("fail", 60, "~monthly", "Innovate UK xlsx"),
    "gleif_lei": ("fail", 30, "daily", "not in s4; ODS class (daily file)"),
    "payment_practices": ("fail", 60, "rolling filings",
                          "not in s4; FHRS class (rolling filings)"),
    "ocds": ("fail", 45, "rolling", "not in s4; Gazette class (rolling feed)"),
    "lottery_grants": ("watch", None, "rolling", "not in s4; a cumulative grant "
                       "series, award dates carry their own year"),
    "grantnav_lancs": ("watch", None, "rolling", "as lottery_grants"),
    "heritage_fund_awards": ("watch", None, "rolling", "as lottery_grants"),
    "sport_england_grants": ("vintage", None, "closed series",
                             "April 2009 to March 2022 only; no later edition "
                             "exists, so the vintage label is mandatory"),
    "lancs_community_foundation_grants": ("watch", None, "quarterly editions",
                                          "as lottery_grants"),
    # --- statistics --------------------------------------------------------
    "nomis": ("watch", None, "annual-ish",
              "NOMIS/ONS/HMRC/NNDR3/insolvency stats"),
    "ons_business_demography": ("watch", None, "annual",
                                "NOMIS/ONS/HMRC/NNDR3/insolvency stats"),
    "els_high_growth": ("watch", None, "annual",
                        "NOMIS/ONS/HMRC/NNDR3/insolvency stats"),
    "hmrc_spi": ("watch", None, "annual",
                 "NOMIS/ONS/HMRC/NNDR3/insolvency stats"),
    "bpe": ("watch", None, "annual",
            "NOMIS/ONS/HMRC/NNDR3/insolvency stats"),
    "insolvencies_by_la": ("watch", None, "annual",
                           "NOMIS/ONS/HMRC/NNDR3/insolvency stats"),
    "nndr3_outturn": ("watch", None, "annual",
                      "NOMIS/ONS/HMRC/NNDR3/insolvency stats"),
    "nhs_payments_gp": ("watch", None, "annual",
                        "NOMIS/ONS/HMRC/NNDR3/insolvency stats"),
    "coops_uk_open_data": ("watch", None, "annual",
                           "NOMIS/ONS/HMRC/NNDR3/insolvency stats"),
    "voa_stock": ("watch", None, "annual",
                  "NOMIS/ONS/HMRC/NNDR3/insolvency stats"),
    "nndr": ("watch", None, "ad hoc per billing authority",
             "not in s4; 12 of 14 councils publish on their own cadence"),
    "census_ts066_lsoa": ("vintage", None, "2021 census",
                          "a census is a fixed reference date, not a feed"),
    "gro_places_of_worship": ("vintage", None, "18 March 2015 edition",
                              "no newer file exists on data.gov.uk"),
    "hmrc_nmw_naming_round23": ("vintage", None, "one naming round",
                                "a dated round, not a series"),
    "voa_2023_list": ("pinned", None, "superseded list",
                      "the 2023 rating list, superseded by the 2026 list. Held "
                      "as the historic basis; ageing is the point"),
    # --- our own derived captures -----------------------------------------
    "org_id_guide": ("pinned", None, "upstream commit",
                     "vendored at a pinned register commit; a moving copy "
                     "would silently re-key the crosswalk"),
    "matcher_pound": ("fail", 45, "monthly", "our own monthly matcher output"),
    "matcher_websites": ("fail", 45, "monthly", "our own monthly matcher output"),
    "matcher_nndr": ("fail", 45, "monthly", "our own monthly matcher output"),
    "matcher_ocds": ("fail", 45, "monthly", "our own monthly matcher output"),
}

# The sources a builder ACTUALLY reads today, taken from every resolve_bronze
# and bronze_partitions call in the warehouse. This is the gate axis, and it is
# separate from the budget on purpose.
#
# The registry holds 51 sources; 40 of them are landed bronze awaiting their
# F-phase, and most of those were ingested on the Mac and have never been
# copied to vps-main. Failing a monthly run on vps because a Sport England
# grant file is not on that host would be a gate that cries wolf until somebody
# turns it off, so:
#
#   consumed     absent or stale is a FAILURE. A builder reads it; a missing
#                input is the OCDS incident waiting to happen (s11.2).
#   not consumed absent is `not-landed` and reported with the hosts the
#                registry says it lives on. Staleness is still COMPUTED and
#                still shown, so wiring it in later inherits a gate that
#                already tells the truth rather than one switched on blind.
#
# Adding a source to a builder without adding it here makes the gate silently
# weaker, so check_consumed() re-derives this set from the code and fails if
# the two disagree.
CONSUMED = {
    "ch_register", "ch_psc_extract", "ch_accounts_ixbrl",
    "ch_accounts_api_backfill", "gazette_notices", "onspd", "org_id_guide",
    "matcher_pound", "matcher_ocds", "matcher_nndr", "matcher_websites",
    # A Gazette notice's LAD comes from postcodes.io, not ONSPD, so this cache
    # cannot be re-derived if it is lost. It is consumed and it is pinned.
    "postcode_lad_cache",
}


def check_consumed():
    """Re-derive CONSUMED from the builders. Drift is a finding, not a nit."""
    import re as _re
    here = Path(__file__).resolve().parent
    found = set()
    pat = _re.compile(r"(?:resolve_bronze|bronze_partitions)\(\s*[\"']"
                      r"([a-z0-9_]+)[\"']")
    for p in sorted(here.glob("*.py")):
        if p.name == "staleness.py":
            continue
        found |= set(pat.findall(p.read_text()))
    return sorted(found - CONSUMED), sorted(CONSUMED - found)


# Gate V-L1's machine-readable half: sources whose licence is not OGL and which
# therefore may not reach a published surface without the stated block. The
# wording lives in LEGAL.md; this is only the list of ids that trigger it.
RESTRICTED_LICENCE = {
    "voa_2026_list": "NOT OGL, restricted pass-through terms; solicitor gate "
                     "before any derived output ships (DATA-INTEGRITY s5)",
    "coops_uk_open_data": "ODC-BY, not OGL: attribution and share-alike terms "
                          "read before republication",
    "lancs_community_foundation_grants": "CC BY 4.0, not OGL: attribution "
                                         "required",
    "givefood_foodbanks": "no formal open licence; good-faith reuse with "
                          "attribution, safeguarding-adjacent presentation "
                          "caveat",
    "sport_england_grants": "OGL with a mandatory attribution string",
    "hmrc_nmw_naming_round23": "OGL, but a named-employer register held for "
                               "internal evidence only per LEGAL.md amber",
}


def parse_date(v):
    """(date, grain) for a reference value, or (None, None). Never a guess.

    Three grains are real in this registry and all three are honest reference
    dates, so all three are read rather than one being privileged:

      day    `2026-08-01`, a dated edition.
      month  `2026-08`, a monthly edition. Read as the first of the month,
             which is the earliest day it can refer to, so the age it produces
             is a LOWER bound on how stale the source is. Erring towards fresh
             would be the wrong direction for a staleness gate; erring towards
             the start of the period is the conservative reading of "how old is
             this at worst" for a budget that fires on age.
      fy     `2025-26`, a UK financial year. Read as 1 April of the start year.

    ONSPD states `MAY_2026` and org-id.guide states a commit. Both are real
    as-at values for their sources and neither is any of the three, so they
    return (None, None) and the caller falls back to the snapshot axis and says
    so in `measuredOn`.
    """
    if not v or not isinstance(v, str):
        return None, None
    v = v.strip()
    try:
        return _dt.date.fromisoformat(v[:10]), "day"
    except ValueError:
        pass
    if len(v) == 7 and v[4] == "-" and v[:4].isdigit() and v[5:].isdigit():
        year, tail = int(v[:4]), int(v[5:])
        # `2025-26` is the financial year starting 2025, not month 26. The
        # discriminator is the successor-year test, not the range 1 to 12,
        # because `2025-04` is genuinely ambiguous between April 2025 and the
        # 2025-04 financial year and only the first reading is ever used here.
        if tail == (year + 1) % 100 and tail > 12:
            return _dt.date(year, 4, 1), "financialYear"
        if 1 <= tail <= 12:
            return _dt.date(year, tail, 1), "month"
    return None, None


def collect(host=None, as_of=None):
    host = host or S.host()
    today = as_of or _dt.date.today()
    rows = []
    known = {s["id"] for s in S.SOURCES}
    for src in S.SOURCES:
        sid = src["id"]
        if host not in src.get("hosts", []):
            continue
        mode, budget, cadence, rulebook = BUDGETS.get(
            sid, ("fail", 45, "unknown", "NOT IN THE BUDGET TABLE"))
        parts = []
        base = S.bronze_dir(host) / f"source={sid}"
        if base.exists():
            for p in sorted(base.glob("snapshot_date=*")):
                mf = p / "manifest.json"
                if mf.exists():
                    parts.append((p.name.split("=", 1)[1],
                                  json.loads(mf.read_text())))
        row = {
            "source": sid,
            "name": src.get("name"),
            "consumed": sid in CONSUMED,
            "registryHosts": src.get("hosts", []),
            "mode": mode,
            "budgetDays": budget,
            "cadence": cadence,
            "rulebookRow": rulebook,
            "partitions": len(parts),
            "licence": src.get("licence"),
            "restricted": RESTRICTED_LICENCE.get(sid),
        }
        if not parts:
            if sid in CONSUMED and mode == "fail":
                status, note = "MISSING", (
                    "a builder reads this source and it has no bronze "
                    "partition on this host. An absent input is not a zero "
                    "(DATA-INTEGRITY s11.2).")
            else:
                status, note = "not-landed", (
                    "landed bronze awaiting its F-phase; no builder reads it "
                    "yet. Registry says it lives on "
                    f"{src.get('hosts')}, and this host is {host}.")
            row.update(status=status, ageDays=None, measuredOn=None, asAt=None,
                       snapshotDate=None, retrievedAt=None, note=note)
            rows.append(row)
            continue
        snap, m = parts[-1]
        as_at, grain = parse_date(m.get("asAt"))
        if as_at:
            ref, measured_on = as_at, "asAt"
        else:
            ref, grain = parse_date(snap)
            measured_on = "snapshotDate" if ref else None
        age = (today - ref).days if ref else None
        status = "ok"
        if mode == "fail":
            if age is None:
                # An age that cannot be computed is not a fresh source. This is
                # the same shape of hole as the empty OCDS map: a check that
                # silently passes on a value it could not read.
                status = "UNMEASURABLE"
            elif budget and age >= 2 * budget:
                status = "FAIL"
            elif budget and age >= budget:
                status = "warn"
        row.update(status=status, ageDays=age, measuredOn=measured_on,
                   referenceGrain=grain,
                   asAt=m.get("asAt"), snapshotDate=snap,
                   retrievedAt=m.get("retrievedAt"),
                   files=m.get("fileCount"), bytes=m.get("totalBytes"),
                   capture=m.get("capture", "as-published"))
        rows.append(row)
    unbudgeted = sorted(known - set(BUDGETS))
    return rows, unbudgeted, host, today


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", default=None)
    ap.add_argument("--host", default=None)
    ap.add_argument("--as-of", default=None,
                    help="pin the clock, YYYY-MM-DD. The clock is an input.")
    ap.add_argument("--report-only", action="store_true",
                    help="never exit non-zero; for a first look at a new host")
    args = ap.parse_args()

    as_of = _dt.date.fromisoformat(args.as_of) if args.as_of else None
    rows, unbudgeted, host, today = collect(args.host, as_of)

    bad = ("FAIL", "MISSING", "UNMEASURABLE")
    fails = [r for r in rows if r["status"] in bad and r["consumed"]]
    not_wired = [r for r in rows if r["status"] in bad and not r["consumed"]]
    warns = [r for r in rows if r["status"] == "warn"]
    drift_extra, drift_missing = check_consumed()
    report = {
        "gate": "V-R1",
        "generated": _dt.datetime.now(_dt.timezone.utc)
                        .strftime("%Y-%m-%dT%H:%M:%SZ"),
        "asOf": today.isoformat(),
        "host": host,
        "runId": D.run_id(),
        "pipelineGitSha": D.pipeline_git_sha(),
        "counts": {"sources": len(rows),
                   "consumed": sum(1 for r in rows if r["consumed"]),
                   "ok": sum(1 for r in rows if r["status"] == "ok"),
                   "warn": len(warns), "fail": len(fails),
                   "notLanded": sum(1 for r in rows
                                    if r["status"] == "not-landed"),
                   "staleButNotWired": len(not_wired)},
        "consumedSources": sorted(CONSUMED),
        "notWiredFindings": [{"source": r["source"], "status": r["status"],
                              "registryHosts": r["registryHosts"]}
                             for r in not_wired],
        "consumedSetDrift": {"readByCodeButNotListed": drift_extra,
                             "listedButNotReadByCode": drift_missing},
        "unbudgetedSourceIds": unbudgeted,
        "restrictedLicenceSources": sorted(
            r["source"] for r in rows if r.get("restricted")),
        "sources": sorted(rows, key=lambda r: (r["status"] != "FAIL",
                                               r["status"] != "MISSING",
                                               r["status"] != "UNMEASURABLE",
                                               r["status"] != "warn",
                                               r["source"])),
    }

    width = max(len(r["source"]) for r in rows) if rows else 10
    for r in report["sources"]:
        age = "-" if r["ageDays"] is None else f"{r['ageDays']:>4}d"
        bud = "-" if not r["budgetDays"] else f"{r['budgetDays']}d"
        print(f"  {r['status']:<10} {'*' if r['consumed'] else ' '} "
              f"{r['source']:<{width}} {age} budget {bud:<5} "
              f"mode {r['mode']:<7} on {r['measuredOn'] or '-'}")
    print()
    c = report["counts"]
    print(f"V-R1 staleness on {host} as of {today}: {c['fail']} FAIL and "
          f"{c['warn']} warn across {c['consumed']} consumed sources "
          f"(marked *). Of the {c['sources']} registry sources here, "
          f"{c['ok']} are landed and inside budget, {c['notLanded']} have no "
          f"partition on this host and no builder reading them, and "
          f"{c['staleButNotWired']} are landed, stale and not yet wired.")
    if unbudgeted:
        print(f"  {len(unbudgeted)} registry source(s) carry no budget row: "
              f"{unbudgeted}")

    out = Path(args.out) if args.out else D.report_dir() / "staleness.json"
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(report, indent=2) + "\n")
    print(f"  written to {out}")

    if args.report_only:
        return 0
    if unbudgeted:
        print("V-R1 FAILED: a source with no budget row cannot be judged "
              "stale or fresh. Add it to BUDGETS.")
        return 1
    if drift_extra:
        print(f"V-R1 FAILED: {drift_extra} are read by a builder but are not "
              "in CONSUMED, so the gate is weaker than the pipeline.")
        return 1
    if fails:
        print(f"V-R1 FAILED: {[r['source'] for r in fails]}")
        return 1
    print("V-R1 GREEN")
    return 0


if __name__ == "__main__":
    sys.exit(main())
