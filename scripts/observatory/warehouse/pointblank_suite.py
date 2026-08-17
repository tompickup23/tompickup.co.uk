#!/usr/bin/env python3
"""The pointblank tier: silver to gold, with a warn level the SQL gates refuse.

`check_silver.py` and `check_gold.py` are deliberately absolute: any row
returned by any check fails the build, and that is right for the things they
test, which are all rules with no legitimate exceptions. A society number in a
CRN column is never acceptable at any rate.

This suite exists for the class the SQL gates cannot express: a proportion that
is normal at 0.2 percent and alarming at 5 percent. 271 implausible headcounts
in 151,106 filings is the iXBRL extractor behaving as it always has; the same
check firing on a tenth of the table means something broke upstream. Absolute
gates cannot hold both of those thoughts, so those checks live here with
`Thresholds(warning, error, critical)` and the run reports which level each
step reached.

Three suites, because they answer different questions:

  R1  staleness. The bronze budget table (staleness.py) validated as a frame,
      so warn-at-budget and fail-at-2x-budget become thresholds rather than
      an if-statement. This is the pointblank home the plan gives V-R1.
  R2  joined-vintage labels. DATA-INTEGRITY s4 rule 1: a figure joining
      sources more than one budget class apart must render both dates. The
      published files carry sibling year fields (`birthsYear`,
      `selfEmploymentIncomeYear`, `innovationYearRange`); this asserts each
      flagged composite field HAS its label, which is the mechanical half of a
      rule whose other half is editorial.
  Q   table quality. The marts and the gold entity layer, with the tolerance
      bands the SQL gates refuse to carry.

Failure policy: `critical` on any step fails the run. `error` fails the run
unless --warn-only. `warning` never fails and always shows in the report,
because a warn tier nobody reads is decoration.

Usage:
    pointblank_suite.py [--out-html reports/pointblank.html]
                        [--out-json reports/pointblank.json]
                        [--serve /opt/observatory/m5/run/public/data]
                        [--as-of 2026-08-17] [--warn-only]
"""
import argparse
import datetime as _dt
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import silver as SV  # noqa: E402
import crosswalk as XW  # noqa: E402
import marts as M  # noqa: E402
import driver as D  # noqa: E402
import staleness as ST  # noqa: E402

# DATA-INTEGRITY s2, verbatim. A mart carrying an entityType outside this list
# is a typing failure, which is gate V-T1 seen from the gold side.
ENTITY_TYPES = {
    "ltd", "plc", "llp", "lp", "cic", "charitable-company", "cio",
    "charity-unincorporated", "registered-society", "credit-union",
    "building-society", "friendly-society", "overseas-establishment",
    "overseas-entity", "sole-trader-named", "partnership-named",
    "unregistered-modelled", "school", "academy-trust", "nhs-org",
    "council-owned", "public-body", "parish", "other-corporate-body",
}

# s4 rule 1 in machine-readable form: field -> the sibling that must carry its
# vintage. The pairs are exactly the composites the published contract already
# labels, so this gate protects a property the files have rather than demanding
# a new one.
VINTAGE_PAIRS = [
    ("areas", "birthsLatest", "birthsYear"),
    ("areas", "deathsLatest", "birthsYear"),
    ("unitaries", "birthsLatest", "birthsYear"),
    ("unitaries", "deathsLatest", "birthsYear"),
]
AREA_VINTAGE_PAIRS = [
    ("wholeEconomy", "selfEmploymentIncomeM", "selfEmploymentIncomeYear"),
]


def declared(pb, population, headroom=1):
    """Per-step thresholds sized to a DECLARED fault.

    A fault we have measured, written up and chosen not to fix yet should not
    fail a build every night, and it should not pass silently either. So the
    step keeps asserting the correct rule, and its thresholds are set so that
    the KNOWN population warns and one more row than the known population is an
    error. The gate then fires on the day the fault grows, which is the only
    new information it could carry.

    `population` is the count from DATA-INTEGRITY, not a round number, so
    re-deriving it after a fix is a one-line diff rather than an argument about
    what the tolerance was meant to be.

    ABSOLUTE row counts, not fractions. pointblank reads a threshold below 1 as
    a fraction of the table and a value of 1 or more as a number of rows, so a
    fraction computed as (population + 1) / n silently becomes "one row" the
    moment the population approaches the table size. F2 found that the hard
    way: its declared population IS every row in the table, and the fraction
    form turned a warn-only step into a critical failure.
    """
    return pb.Thresholds(warning=1,
                         error=population + headroom,
                         critical=population * 2 + headroom)


def flags(df, *cols):
    """Booleans as 0/1 integers.

    pointblank refuses a comparison on a boolean column outright
    (`TypeError: Column 'x' is a boolean.`), which is a defensible choice for a
    library aimed at data profiling and an awkward one here, because most of
    what this suite asserts IS a flag: does the row sit in the frame, does the
    caption exist, is this a Companies Act body. Casting to 0/1 keeps the
    assertion readable as "every row is 1" rather than pushing each flag into a
    bespoke expression.
    """
    for c in cols:
        if c in df.columns:
            df[c] = df[c].astype("Int8")
    return df


def load(con, pq, cols="*", where="", order=""):
    sql = f"SELECT {cols} FROM read_parquet('{pq}')"
    if where:
        sql += f" WHERE {where}"
    if order:
        sql += f" ORDER BY {order}"
    return con.execute(sql).df()


def latest(base, table):
    p = D.latest_partition(base, table)
    return (p / "part.parquet") if p else None


def suite_staleness(pb, as_of):
    """V-R1 as a validation rather than an if-statement.

    `ageRatio` is age divided by budget, so the thresholds map exactly onto the
    rulebook: warn at 1x budget, fail at 2x. Only `fail`-mode sources that a
    builder actually READS are in the frame: a `watch` or `vintage` source has
    no failure age, and a source landed in bronze awaiting its F-phase has no
    consumer to fail. Both would manufacture failures the rulebook does not
    contain, and a gate that cries wolf gets switched off.
    """
    import pandas as pd
    rows, unbudgeted, host, today = ST.collect(as_of=as_of)
    live = [r for r in rows if r["mode"] == "fail" and r["consumed"]]
    df = pd.DataFrame([{
        "source": r["source"],
        "ageDays": r["ageDays"],
        "budgetDays": r["budgetDays"],
        "ageRatio": (None if r["ageDays"] is None or not r["budgetDays"]
                     else round(r["ageDays"] / r["budgetDays"], 3)),
        "present": r["partitions"] > 0,
        "measuredOn": r["measuredOn"] or "unmeasurable",
    } for r in live])
    flags(df, "present")
    v = (pb.Validate(
            data=df, tbl_name="staleness (V-R1)",
            label="V-R1 staleness budgets, DATA-INTEGRITY s4",
            # 1/len is "one source", so a single breach trips the level. The
            # budget already carries the tolerance; a second tolerance on top
            # of it would be tolerance applied twice.
            thresholds=pb.Thresholds(warning=1 / max(len(df), 1),
                                     error=1 / max(len(df), 1),
                                     critical=0.25))
         .col_vals_eq(columns="present", value=1)
         .col_vals_not_null(columns="ageRatio")
         .col_vals_le(columns="ageRatio", value=1.0, na_pass=False)
         .col_vals_le(columns="ageRatio", value=2.0, na_pass=False)
         .interrogate())
    return v, {"sources": len(df), "unbudgeted": unbudgeted, "host": host,
               "asOf": today.isoformat()}


def suite_vintage(pb, serve_dir):
    """V-R2: every flagged composite figure has its vintage label beside it."""
    import pandas as pd
    serve = Path(serve_dir)
    ov = serve / "biz-overview.json"
    ar = serve / "biz-areas.json"
    recs = []
    if ov.exists():
        d = json.loads(ov.read_text())
        for coll, field, label in VINTAGE_PAIRS:
            for a in d.get(coll, []):
                recs.append({
                    "file": "biz-overview.json", "area": a.get("slug"),
                    "field": field,
                    "hasValue": a.get(field) is not None,
                    # A label is only required where the figure exists. A null
                    # figure with no year is missing data, not an unlabelled
                    # join, and conflating them would fail the gate on absence.
                    "labelled": a.get(field) is None or a.get(label) is not None,
                })
    if ar.exists():
        d = json.loads(ar.read_text())
        for slug, body in d.items():
            if slug.startswith("$") or not isinstance(body, dict):
                continue
            for block, field, label in AREA_VINTAGE_PAIRS:
                b = body.get(block) or {}
                recs.append({
                    "file": "biz-areas.json", "area": slug, "field": field,
                    "hasValue": b.get(field) is not None,
                    "labelled": b.get(field) is None or b.get(label) is not None,
                })
    if not recs:
        return None, {"skipped": "no serve directory to read"}
    df = flags(pd.DataFrame(recs), "labelled", "hasValue")
    v = (pb.Validate(
            data=df, tbl_name="joined vintage labels (V-R2)",
            label="V-R2 joined-vintage labels, DATA-INTEGRITY s4 rule 1",
            thresholds=pb.Thresholds(warning=1 / len(df), error=1 / len(df),
                                     critical=0.10))
         # F7 CLEARED: the four 2028 unitary rollups used to carry an HMRC
         # income figure with no year label. The rule is asserted at full
         # strength now, so a rollup that drops a caption again fails here
         # rather than warning.
         .col_vals_eq(columns="labelled", value=1)
         .interrogate())
    return v, {"fields": len(df)}


def suite_marts(pb, con, as_of):
    """Table quality on the marts and the gold entity layer."""
    out = []
    gold = XW.gold_dir()

    pq = latest(gold, "mart_register_lancs")
    if pq:
        # Date comparisons are done in SQL, not in the validation. The marts
        # keep dates in the form the consumer reads them, which for the
        # register frame is the CH bulk file's own DD/MM/YYYY string and for a
        # Gazette notice is an ISO string, and pointblank refuses an ordering
        # comparison on a string column outright. Deriving the flag in SQL puts
        # the parsing where the format is known instead of guessing at it.
        df = load(con, pq,
                  "company_number, company_name, reg_postcode, "
                  "lad_code, entity_type, company_status, companies_act_body, "
                  f"coalesce(try_strptime(incorporation_date, '%d/%m/%Y') "
                  f"         > TIMESTAMP '{as_of} 00:00:00', false) "
                  "  AS incorporated_in_future")
        flags(df, "companies_act_body", "incorporated_in_future")
        v = (pb.Validate(
                data=df, tbl_name="gold/mart_register_lancs",
                label="the production Lancashire frame (103,468 basis)",
                thresholds=pb.Thresholds(warning=1 / len(df), error=0.001,
                                         critical=0.01))
             # Every CH registered number is exactly eight uppercase
             # alphanumerics (DATA-INTEGRITY s9.3). The narrower
             # two-letters-plus-six-digits test rejects 99 real companies.
             .col_vals_regex(columns="company_number", pattern=r"^[0-9A-Z]{8}$")
             .col_vals_not_null(columns="company_name")
             .col_vals_not_null(columns="reg_postcode")
             .col_vals_in_set(columns="lad_code", set=sorted(M.TARGET_LADS))
             .col_vals_in_set(columns="entity_type", set=sorted(ENTITY_TYPES))
             .col_vals_eq(columns="incorporated_in_future", value=0)
             .rows_distinct(columns_subset=["company_number"])
             # F1 is DECLARED, so it is a warn here rather than a pass. 11 of
             # 103,468 rows are overseas establishments or entities counted as
             # companies. A silent pass would hide a declared fault; a hard
             # fail would block a build over a fault we have chosen not to fix
             # yet. The warn tier is exactly the right shape for a known,
             # bounded, signed-off defect.
             .col_vals_eq(columns="companies_act_body", value=1,
                          thresholds=declared(pb, 11))
             .interrogate())
        out.append(("mart_register_lancs", v))

    pq = latest(gold, "mart_accounts_lancs")
    if pq:
        # The mart records which filing each rule picks rather than a single
        # "differs" flag, so F3's population is the disagreement between them:
        # the row the site publishes is not the chronologically latest one.
        df = load(con, pq,
                  "crn, period_end, employees_as_filed, employees, "
                  "file_ordinal, "
                  "(site_winner AND NOT latest_winner) "
                  "  AS differs_from_latest_filing, "
                  f"(period_end > DATE '{as_of}' "
                  "  OR period_end < DATE '1900-01-01') AS period_implausible,",
                  # latest_winner, not site_winner: the consumers now resolve a
                  # period to the filing made most recently, so this is the set
                  # they publish and the set the rules below have to hold over.
                  where="latest_winner")
        flags(df, "differs_from_latest_filing", "period_implausible")
        v = (pb.Validate(
                data=df, tbl_name="gold/mart_accounts_lancs",
                label="filed accounts, as the site reads them",
                thresholds=pb.Thresholds(warning=0.002, error=0.01,
                                         critical=0.05))
             .col_vals_regex(columns="crn", pattern=r"^[0-9A-Z]{8}$")
             .col_vals_eq(columns="period_implausible", value=0)
             # Two bands, and the difference between them is the point.
             # employees_as_filed is what the extractor wrote and is allowed to
             # be wrong: 271 rows outside the band is the known iXBRL
             # scale-and-sign artefact (s9.6), a tenth of the table outside it
             # is a broken extractor, and that difference is a threshold. The
             # derived employees column is what a consumer publishes and is not
             # allowed to be wrong at any rate, so it carries the silver bounds
             # with no tolerance at all.
             .col_vals_between(columns="employees_as_filed", left=0,
                               right=200000, na_pass=True)
             .col_vals_between(columns="employees", left=0, right=500000,
                               na_pass=True,
                               thresholds=pb.Thresholds(warning=1, error=1,
                                                        critical=1))
             .col_vals_not_null(columns="file_ordinal")
             # F3 CLEARED, and there is deliberately no step asserting it
             # here. The correction is a change of BASIS, not a row-level
             # property: this validation now runs over latest_winner, which is
             # what the consumers publish, where before it ran over
             # site_winner. Asserting "the published row is the latest filing"
             # against a set defined as the latest filings would be a
             # tautology. The size of the correction is counted where it can be
             # counted honestly, in the mart manifest's own assertions
             # (notTheLatestFiling, differentEmployeeFigure) and in the
             # per-fault diff of the published edition.
             .interrogate())
        out.append(("mart_accounts_lancs", v))

    pq = latest(gold, "mart_notices_lancs")
    if pq:
        # Both derived columns are computed in SQL rather than as a regex in
        # the validation, because a negative lookahead is not portable across
        # the backends pointblank can sit on and a silently unsupported pattern
        # would pass every row.
        df = load(con, pq,
                  "notice_id, notice_type, company_number, "
                  "company_number_raw, notice_date, uri, "
                  "length(company_number_raw) <> length(trim(company_number_raw))"
                  "  AS trailing_space, "
                  "notice_type ILIKE '%petition%' AS is_petition, "
                  f"coalesce(try_strptime(notice_date, '%Y-%m-%d') "
                  f"         > TIMESTAMP '{as_of} 00:00:00', true) "
                  "  AS notice_date_implausible")
        flags(df, "trailing_space", "is_petition", "notice_date_implausible")
        v = (pb.Validate(
                data=df, tbl_name="gold/mart_notices_lancs",
                label="Gazette corporate insolvency notices, category 24 only",
                thresholds=pb.Thresholds(warning=0.01, error=0.05,
                                         critical=0.10))
             .col_vals_regex(columns="uri",
                             pattern=r"^https://www\.thegazette\.co\.uk/")
             .col_vals_not_null(columns="notice_id")
             .col_vals_eq(columns="notice_date_implausible", value=0)
             # The legal blocker, mirrored from validate_outputs.py so it fires
             # in the warehouse rather than only at the last gate before deploy.
             .col_vals_eq(columns="is_petition", value=0)
             # F5 CLEARED: the projection emits the trimmed number and the
             # fetcher trims at the point the publisher's text arrives, so a
             # published notice carrying an untrimmed number is now a failure
             # rather than a known population.
             .col_vals_eq(columns="trailing_space", value=0)
             .interrogate())
        out.append(("mart_notices_lancs", v))

    pq = latest(gold, "mart_supplier_identifiers")
    if pq:
        df = load(con, pq, "supplier_key, company_number, evidence, "
                           "in_production_edition")
        flags(df, "in_production_edition")
        v = (pb.Validate(
                data=df, tbl_name="gold/mart_supplier_identifiers",
                label="OCDS supplier identifications, from the crosswalk",
                thresholds=pb.Thresholds(warning=0.01, error=0.05,
                                         critical=0.20))
             .col_vals_regex(columns="company_number", pattern=r"^[0-9A-Z]{8}$")
             .col_vals_not_null(columns="supplier_key")
             # decision_id, NOT supplier_key. 574 dated decisions cover 339
             # distinct supplier names, because every EDITION of every matcher
             # is kept (s10.4 rule 4) and a name verified twice is two dated
             # facts. supplier_key repeats by design. Worth recording while
             # looking: all 339 names resolve to exactly one company number
             # each, so the projection collapsing on the name is harmless here
             # even though it reads position as meaning.
             .rows_distinct(columns_subset=["decision_id"])
             # F2 CLEARED. in_production_edition is now the reconciliation
             # column: it says which of these identifications the LEGACY path
             # is also producing, which is how the two paths are shown to agree
             # rather than assumed to. A row the crosswalk holds and the
             # repaired fetcher does not is worth a warning and not a failure,
             # because the crosswalk keeps every edition of every matcher and
             # a buyer can withdraw a notice.
             .col_vals_eq(columns="in_production_edition", value=1,
                          thresholds=pb.Thresholds(warning=1, error=0.25,
                                                   critical=0.50))
             .interrogate())
        out.append(("mart_supplier_identifiers", v))

    pq = latest(gold, "entity")
    if pq:
        df = load(con, pq, "entity_id, anchor_scheme, anchor_source_id, name")
        v = (pb.Validate(
                data=df, tbl_name="gold/entity",
                label="one row per organisation, mint-once ULID",
                thresholds=pb.Thresholds(warning=1 / len(df), error=0.0005,
                                         critical=0.005))
             .col_vals_regex(columns="entity_id",
                             pattern=r"^[0-9A-HJKMNP-TV-Z]{26}$")
             .rows_distinct(columns_subset=["entity_id"])
             .col_vals_not_null(columns="anchor_id")
             .col_vals_in_set(columns="anchor_scheme",
                              set=["GB-COH", "GB-CHC", "GB-SC", "GB-NIC",
                                   "GB-MPR", "GB-NHS", "GB-EDU", "GB-UKPRN",
                                   "LBO-NNDR", "LBO-SUPPLIER", "LBO-WEB"])
             .interrogate())
        out.append(("entity", v))

    pq = latest(gold, "crosswalk")
    if pq:
        df = load(con, pq, "entity_id, scheme, source_id, confidence, method, "
                           "evidence_class")
        v = (pb.Validate(
                data=df, tbl_name="gold/crosswalk",
                label="identifier belongs to entity, on this authority",
                thresholds=pb.Thresholds(warning=1 / len(df), error=0.0005,
                                         critical=0.005))
             .col_vals_between(columns="confidence", left=0, right=1)
             # The publication gate of DATA-INTEGRITY s10.3, as a table check:
             # nothing probabilistic is in the crosswalk. If a Splink edge ever
             # lands here it is a critical failure, not a warning.
             .col_vals_eq(columns="method", value="deterministic")
             .col_vals_in_set(columns="evidence_class",
                              set=["identifier-observed", "name-rule"])
             .col_vals_not_null(columns="entity_id")
             .interrogate())
        out.append(("crosswalk", v))
    return out


def render(pb, validations, out_html, meta):
    """One HTML page, one section per validation."""
    parts = [
        "<meta charset='utf-8'>",
        "<title>Observatory pointblank report</title>",
        "<style>body{font-family:system-ui,sans-serif;margin:2rem;max-width:"
        "1200px}h1{font-size:1.4rem}h2{font-size:1.05rem;margin-top:2.5rem}"
        "pre{background:#f4f4f4;padding:.75rem;overflow-x:auto}</style>",
        "<h1>Lancashire Business Observatory: silver to gold validation</h1>",
        f"<pre>{json.dumps(meta, indent=1)}</pre>",
    ]
    for name, v in validations:
        parts.append(f"<h2>{name}</h2>")
        try:
            parts.append(v.get_tabular_report().as_raw_html())
        except Exception as e:
            parts.append(f"<p>report could not be rendered: {e}</p>")
    Path(out_html).parent.mkdir(parents=True, exist_ok=True)
    Path(out_html).write_text("\n".join(parts))


def summarise(name, v):
    rep = json.loads(v.get_json_report())
    steps = []
    for s in rep:
        level = ("critical" if s.get("critical") else
                 "error" if s.get("error") else
                 "warning" if s.get("warning") else "ok")
        steps.append({
            "suite": name, "step": s.get("i"),
            "assertion": s.get("assertion_type"), "column": s.get("column"),
            "n": s.get("n"), "nFailed": s.get("n_failed"),
            "fFailed": s.get("f_failed"), "level": level,
        })
    return steps


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--out-html", default=None)
    ap.add_argument("--out-json", default=None)
    ap.add_argument("--serve", default=None,
                    help="a public/data directory to run the V-R2 label gate "
                         "over. Omitted means the gate is skipped, not passed.")
    ap.add_argument("--as-of", default=None)
    ap.add_argument("--warn-only", action="store_true",
                    help="report an error level without failing the run")
    args = ap.parse_args()

    import pointblank as pb
    as_of = (_dt.date.fromisoformat(args.as_of) if args.as_of
             else _dt.date.today())
    con, dv = SV.connect()
    D.log(f"pointblank {pb.__version__}, duckdb {dv}, as of {as_of}")

    validations, extras = [], {}
    v, info = suite_staleness(pb, as_of)
    validations.append(("V-R1 staleness budgets", v))
    extras["staleness"] = info

    if args.serve:
        v, info = suite_vintage(pb, args.serve)
        if v is not None:
            validations.append(("V-R2 joined-vintage labels", v))
        extras["vintage"] = info
    else:
        extras["vintage"] = {"skipped": "no --serve directory given; the gate "
                                        "did not run and did not pass"}

    validations.extend(suite_marts(pb, con, as_of))
    con.close()

    steps = []
    for name, v in validations:
        steps.extend(summarise(name, v))

    levels = {lvl: sum(1 for s in steps if s["level"] == lvl)
              for lvl in ("ok", "warning", "error", "critical")}
    report = {
        "suite": "pointblank silver to gold",
        "gates": ["V-R1", "V-R2"],
        "runId": D.run_id(),
        "pipelineGitSha": D.pipeline_git_sha(),
        "generated": _dt.datetime.now(_dt.timezone.utc)
                        .strftime("%Y-%m-%dT%H:%M:%SZ"),
        "asOf": as_of.isoformat(),
        "pointblankVersion": pb.__version__,
        "levels": levels,
        "extras": extras,
        "steps": steps,
    }

    out_json = Path(args.out_json) if args.out_json else \
        D.report_dir() / "pointblank.json"
    out_json.parent.mkdir(parents=True, exist_ok=True)
    out_json.write_text(json.dumps(report, indent=2) + "\n")

    out_html = Path(args.out_html) if args.out_html else \
        D.report_dir() / "pointblank.html"
    render(pb, validations, out_html, {k: v for k, v in report.items()
                                       if k != "steps"})

    for s in steps:
        if s["level"] != "ok":
            print(f"  {s['level'].upper():<8} {s['suite']}: "
                  f"{s['assertion']}({s['column']}) "
                  f"{s['nFailed']} of {s['n']} failed "
                  f"({(s['fFailed'] or 0) * 100:.2f}%)")
    print()
    print(f"pointblank: {levels['ok']} ok, {levels['warning']} warning, "
          f"{levels['error']} error, {levels['critical']} critical "
          f"across {len(steps)} steps")
    print(f"  {out_json}")
    print(f"  {out_html}")

    if levels["critical"]:
        print("POINTBLANK FAILED at critical")
        return 1
    if levels["error"] and not args.warn_only:
        print("POINTBLANK FAILED at error")
        return 1
    print("POINTBLANK GREEN" + (" (warnings present)" if levels["warning"]
                                else ""))
    return 0


if __name__ == "__main__":
    sys.exit(main())
