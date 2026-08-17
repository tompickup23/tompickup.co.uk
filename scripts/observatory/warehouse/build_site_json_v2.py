#!/usr/bin/env python3
"""M4: re-emit every biz-*.json from the gold marts, in shadow.

The design decision worth understanding before reading the code:

    NOT ONE LINE OF AGGREGATION LOGIC IS REIMPLEMENTED HERE.

build_master, build_growth, resolve_suppliers, build_pound, build_overlay,
build_dossiers, build_site_json and build_diff are executed UNCHANGED, from a
hash-verified copy of the site checkout, against a shadow data tree whose CH
inputs are projected out of the gold marts instead of out of the old fetch/ETL
layer. A forked re-implementation would have produced a golden-file test that
measured whether I can copy code. This one measures the warehouse, which is
the thing M4 is a gate on.

Three controls make the comparison honest:

  1. **The clock is pinned.** build_master derives ageYears from date.today()
     and build_site_json stamps $meta.generated and the 90-day window from it.
     A re-run on a different day is a different computation, so --as-of pins
     the date through a sitecustomize shim rather than by editing any script.
  2. **The scripts are hash-verified.** Every file copied into the shadow run
     is sha256-compared against the checkout it came from, and the runner
     aborts on any mismatch. There is no maintained duplicate to drift.
  3. **Pass-through inputs are COPIED, never symlinked.** The chain writes
     nine files into processed/, and a symlink would have written them through
     into the live tree. Copies cost 42 MB and cannot do that.

Layers that have no silver builder yet are passed through unchanged and listed
in PASSTHROUGH with the phase that lands them. That is the honest statement of
how far the migration has got: the whole Companies House family plus the
Gazette comes out of the warehouse, and NOMIS, ONS demography, FHRS, CQC, VOA,
GIAS, charities, mutuals, Innovate UK, BPE, the council spend ledger, the NNDR
presence layer and the verified-websites layer do not, yet.

Usage:
    build_site_json_v2.py --as-of 2026-08-17 [--root /opt/observatory/v2]
                          [--reproduce-ocds-fault]
"""
import argparse
import gzip
import hashlib
import json
import os
import shutil
import subprocess
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import marts as M  # noqa: E402
import crosswalk as XW  # noqa: E402

# Everything the chain needs that the warehouse does not yet produce. The
# comment on each is the phase that ends the pass-through, so this list is also
# the migration's own to-do.
PASSTHROUGH = {
    "processed/_postcode_lad_cache.json": "F1 geocoding",
    "processed/nomis_context.json": "F6 longitudinal",
    "processed/ons_demography.json": "F6 longitudinal",
    "processed/fhrs_lancs.json": "F2 sole-trader layer",
    "processed/cqc_lancs.json": "F2 sole-trader layer",
    "processed/voa_lancs.json": "F3 rates, licence-gated",
    "processed/gias_lancs.json": "F4 VCSE",
    "processed/charities_lancs.json": "F4 VCSE",
    "processed/mutuals_lancs.json": "F4 VCSE",
    "processed/innovate_lancs.json": "F5 signals",
    "processed/bpe_spi.json": "F2 sole-trader layer",
    "processed/council_supplier_spend.json": "council spend ETL, separate repo",
    "processed/nndr_presence.json": "F3 rates",
    "processed/websites.jsonl": "stays a fetcher; the crosswalk holds the edges",
    "processed/website_seeds.json": "stays a fetcher",
    "processed/websites_summary.json": "stays a fetcher",
    "vps/momentum.jsonl": "F5 signals",
    "vps/officers_seed.jsonl": "not migrated; officers are a GDPR-limited layer",
}

# The chain, in the order monthly_refresh.sh runs it, each step tagged with
# WHICH clock it ran on. build_dossiers runs twice in production, before and
# after the website verification pass; the second run is what produces the
# published index, so both are kept.
#
# The tag exists because a monthly run takes hours and the 17 Aug edition
# STRADDLED MIDNIGHT UTC: build_master through build_overlay ran on the 16th
# (master.jsonl.gz 23:51, pound.json 23:54, biz-money.json $meta.generated
# 2026-08-16) and the rest ran on the 17th (biz-overview.json $meta.generated
# 2026-08-17). One pinned date for the whole chain shifts every ageYears
# threshold by a day, which is worth about 30 sector new3yr counts and two
# gazelle candidates. The clock is an input; it gets the same treatment as
# any other input.
CHAIN = [("build_master", "etl"), ("build_growth", "etl"),
         ("resolve_suppliers", "etl"), ("build_pound", "etl"),
         ("build_overlay", "etl"), ("build_dossiers", "etl"),
         ("build_dossiers", "site"), ("build_site_json", "site"),
         ("build_diff", "site")]

SHIM = '''"""Pin date.today() for a reproducible re-emission (M4 golden-file test).

Imported automatically by the interpreter because this directory is on
PYTHONPATH. Without it, build_master computes ageYears against the wall clock
and build_site_json stamps a different $meta.generated, so a golden-file diff
would be measuring the calendar.
"""
import os
_pin = os.environ.get("OBS_V2_PIN_DATE")
if _pin:
    import datetime as _d
    _y, _m, _dd = (int(x) for x in _pin.split("-"))

    class _Date(_d.date):
        @classmethod
        def today(cls):
            return cls(_y, _m, _dd)

    class _DateTime(_d.datetime):
        @classmethod
        def today(cls):
            return cls(_y, _m, _dd)

        @classmethod
        def now(cls, tz=None):
            return cls(_y, _m, _dd, tzinfo=tz)

    _d.date = _Date
    _d.datetime = _DateTime
'''


def sha256(p):
    h = hashlib.sha256()
    with open(p, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def stage_scripts(src_scripts, dst_scripts):
    """Copy the assemblers and verify every byte. No maintained duplicate."""
    if dst_scripts.exists():
        shutil.rmtree(dst_scripts)
    shutil.copytree(src_scripts, dst_scripts,
                    ignore=shutil.ignore_patterns("__pycache__", "warehouse"))
    n = 0
    for s in sorted(src_scripts.rglob("*.py")):
        if "__pycache__" in s.parts or "warehouse" in s.parts:
            continue
        d = dst_scripts / s.relative_to(src_scripts)
        if not d.exists() or sha256(s) != sha256(d):
            raise SystemExit(f"FATAL: staged copy of {s.name} does not match "
                             "the checkout. The shadow run must be the same "
                             "code, byte for byte.")
        n += 1
    return n


def project_register(con, root, snap, h):
    """mart_register_lancs -> vps/lancs_register.csv.gz (production layout)."""
    pq = M.mart_dir(h) / "mart_register_lancs" / f"snapshot_date={snap}" / "part.parquet"
    out = root / "home/observatory-data/vps/lancs_register.csv"
    con.execute(f"""
      COPY (SELECT company_number   AS "CompanyNumber",
                   company_name     AS "CompanyName",
                   reg_postcode     AS "RegAddress.PostCode",
                   lad_code, lad_name,
                   company_category AS "CompanyCategory",
                   company_status   AS "CompanyStatus",
                   dissolution_date AS "DissolutionDate",
                   incorporation_date AS "IncorporationDate",
                   accounts_category AS "Accounts.AccountCategory",
                   accounts_next_due_date AS "Accounts.NextDueDate",
                   accounts_last_made_up_date AS "Accounts.LastMadeUpDate",
                   sic_text_1 AS "SICCode.SicText_1",
                   sic_text_2 AS "SICCode.SicText_2",
                   sic_text_3 AS "SICCode.SicText_3",
                   sic_text_4 AS "SICCode.SicText_4",
                   CASE WHEN is_cic THEN 'true' ELSE 'false' END AS cic
            FROM read_parquet('{pq}')
            -- the CH bulk file's own order; see the mart for why it matters
            ORDER BY company_name)
      TO '{out}' (FORMAT CSV, HEADER true)""")
    with open(out, "rb") as fi, gzip.open(str(out) + ".gz", "wb") as fo:
        shutil.copyfileobj(fi, fo)
    return out.with_suffix(".csv.gz")


def project_register_index(con, root, snap, h):
    """mart_register_index -> vps/register_index.tsv.gz.

    Written by hand rather than with a CSV writer: the consumer reads it with
    csv.reader(delimiter='\\t'), production wrote it with a bare f-string, and a
    writer that quoted a name containing a double quote would change what the
    consumer parses.
    """
    pq = M.mart_dir(h) / "mart_register_index" / f"snapshot_date={snap}" / "part.parquet"
    out = root / "home/observatory-data/vps/register_index.tsv.gz"
    n = 0
    with gzip.open(out, "wt", encoding="utf-8") as f:
        f.write("CompanyNumber\tCompanyName\tPostCode\tCompanyStatus\n")
        cur = con.execute(
            "SELECT company_number, company_name, reg_postcode, company_status "
            f"FROM read_parquet('{pq}')")
        while True:
            batch = cur.fetchmany(200000)
            if not batch:
                break
            f.write("".join(f"{a}\t{b}\t{c}\t{d}\n" for a, b, c, d in batch))
            n += len(batch)
    return out, n


def project_jsonl_gz(con, sql, out_path, keys):
    n = 0
    with gzip.open(out_path, "wt", encoding="utf-8") as f:
        cur = con.execute(sql)
        while True:
            batch = cur.fetchmany(100000)
            if not batch:
                break
            for row in batch:
                f.write(json.dumps(dict(zip(keys, row)), ensure_ascii=False) + "\n")
            n += len(batch)
    return n


def project_all(root, reproduce_ocds_fault, h):
    con, dv = M.connect()
    # See build_marts.main: the projections reproduce input FILES, and the
    # order of the rows in them is part of what the consumers read.
    con.execute("SET preserve_insertion_order=true")
    V = root / "home/observatory-data/vps"
    P = root / "home/observatory-data/processed"
    for d in (V, P, root / "home/observatory-data/archive"):
        d.mkdir(parents=True, exist_ok=True)

    def snap_of(table):
        parts = sorted((M.mart_dir(h) / table).glob("snapshot_date=*"))
        if not parts:
            raise SystemExit(f"FATAL: no mart {table}; run build_marts.py")
        return parts[-1].name.split("=", 1)[1]

    report = {}

    rsnap = snap_of("mart_register_lancs")
    project_register(con, root, rsnap, h)
    M.log(f"  vps/lancs_register.csv.gz from mart_register_lancs ({rsnap})")
    _, n = project_register_index(con, root, rsnap, h)
    M.log(f"  vps/register_index.tsv.gz from mart_register_index ({n:,} rows)")
    report["register"] = {"snapshot": rsnap, "indexRows": n}

    asnap = snap_of("mart_accounts_lancs")
    apq = M.mart_dir(h) / "mart_accounts_lancs" / f"snapshot_date={asnap}" / "part.parquet"
    # employees_as_filed, not the cleaned employees column: the live consumers
    # read exactly what the extractor wrote, and build_growth applies its own
    # plausibility filters afterwards. Silver's correction is deliberately not
    # applied here (REPRODUCED FAULT F3 territory, DATA-INTEGRITY s9.6/s11.9).
    # The backfill is NOT emitted as a second file: the mart has already
    # applied the site's last-line-wins rule, so one row per period is
    # equivalent for every consumer, all of which key on (crn, period_end).
    n = project_jsonl_gz(
        con,
        "SELECT crn, CAST(period_end AS VARCHAR), employees_as_filed, equity, "
        f"total_assets, net_current, cash, turnover, filed_zip "
        # EVERY filing in the order the site reads them, not one row per
        # period. build_growth keeps the last NON-NULL employee figure and
        # build_dossiers keeps the last row including its nulls, so a
        # pre-resolved row cannot satisfy both consumers.
        f"FROM read_parquet('{apq}') ORDER BY file_ordinal",
        V / "lancs_accounts.jsonl.gz",
        ["crn", "period_end", "employees", "equity", "total_assets",
         "net_current", "cash", "turnover", "filed_zip"])
    M.log(f"  vps/lancs_accounts.jsonl.gz from mart_accounts_lancs ({n:,})")
    report["accounts"] = {"snapshot": asnap, "rows": n}

    psnap = snap_of("mart_psc_lancs")
    ppq = M.mart_dir(h) / "mart_psc_lancs" / f"snapshot_date={psnap}" / "part.parquet"
    n = project_jsonl_gz(
        con,
        "SELECT company_number, kind, name, nationality, country_of_residence, "
        "postcode, natures_of_control, "
        "CASE WHEN ceased_on IS NULL THEN '' ELSE CAST(ceased_on AS VARCHAR) END "
        f"FROM read_parquet('{ppq}') ORDER BY file_ordinal",
        V / "lancs_psc.jsonl.gz",
        ["company_number", "kind", "name", "nationality",
         "country_of_residence", "postcode", "natures_of_control", "ceased_on"])
    M.log(f"  vps/lancs_psc.jsonl.gz from mart_psc_lancs ({n:,})")

    csnap = snap_of("mart_psc_corporate")
    cpq = M.mart_dir(h) / "mart_psc_corporate" / f"snapshot_date={csnap}" / "part.parquet"
    n = project_jsonl_gz(
        con,
        "SELECT company_number, psc_name, registration_number, "
        "country_registered, legal_form, postcode, "
        "CASE WHEN ceased_on IS NULL THEN '' ELSE CAST(ceased_on AS VARCHAR) END "
        f"FROM read_parquet('{cpq}') ORDER BY file_ordinal",
        V / "corporate_psc_all.jsonl.gz",
        ["company_number", "name", "registration_number", "country_registered",
         "legal_form", "postcode", "ceased_on"])
    M.log(f"  vps/corporate_psc_all.jsonl.gz from mart_psc_corporate ({n:,})")
    report["psc"] = {"lancs": psnap, "corporate": csnap}

    nsnap = snap_of("mart_notices_lancs")
    npq = M.mart_dir(h) / "mart_notices_lancs" / f"snapshot_date={nsnap}" / "part.parquet"
    notices = [
        {"notice_id": r[0], "type": r[1], "company_name": r[2],
         "company_number": r[3], "date": r[4], "uri": r[5], "lad": r[6],
         "postcode": r[7]}
        for r in con.execute(
            # company_number, the trimmed one silver derived, not
            # company_number_raw. The Gazette publishes some numbers with a
            # trailing space and the site joins the register on this string,
            # so the raw form loses the company (DATA-INTEGRITY s11.10).
            "SELECT notice_id, notice_type, company_name, company_number, "
            f"notice_date, uri, lad_code, postcode FROM read_parquet('{npq}') "
            "ORDER BY file_ordinal").fetchall()]
    (P / "gazette_lancs.json").write_text(json.dumps(
        {"$meta": {"source": "gold/mart_notices_lancs",
                   "asAt": nsnap,
                   "licence": "Open Government Licence v3.0 / Crown copyright "
                              "(The Gazette)",
                   "notes": "Corporate insolvency notices only (category 24). "
                            "Fair and accurate extracts, each linked to its "
                            "own notice."},
         "notices": notices}))
    M.log(f"  processed/gazette_lancs.json from mart_notices_lancs ({len(notices)})")
    report["notices"] = {"snapshot": nsnap, "rows": len(notices)}

    ssnap = snap_of("mart_supplier_identifiers")
    spq = M.mart_dir(h) / "mart_supplier_identifiers" / f"snapshot_date={ssnap}" / "part.parquet"
    # The crosswalk's full set is what the projection emits. F2 is cleared:
    # fetch_ocds_ids.py talks to the Contracts Finder API directly now instead
    # of reading a path that existed only on the Mac, so the legacy path
    # produces these same identifications and the two paths agree.
    # --reproduce-ocds-fault restores the empty map the pre-fix editions
    # carried, which is the only way to reproduce one of those editions.
    # NOT is_ambiguous, always. This dict is keyed on the supplier NAME, so a
    # name that award notices attach to two company numbers would resolve to
    # whichever row sorted last: a coin toss published as an identification.
    # Ambiguity is never a merger (DATA-INTEGRITY s11.5), so those names
    # resolve to no company. The filter is outside the fault flag because it
    # is not part of the fault: reproducing a pre-fix edition must not
    # reproduce a bug we never shipped.
    where = (" WHERE NOT is_ambiguous AND in_production_edition"
             if reproduce_ocds_fault else " WHERE NOT is_ambiguous")
    rows = con.execute(
        "SELECT supplier_key, company_number, evidence "
        f"FROM read_parquet('{spq}'){where}").fetchall()
    by_name = {r[0]: {"crn": r[1], "evidence": r[2]} for r in rows}
    total = con.execute(
        f"SELECT count(*) FROM read_parquet('{spq}')").fetchone()[0]
    refused = con.execute(
        f"SELECT count(DISTINCT supplier_key) FROM read_parquet('{spq}') "
        "WHERE is_ambiguous").fetchone()[0]
    (P / "ocds_supplier_ids.json").write_text(json.dumps(
        {"byName": by_name,
         "source": "gold/mart_supplier_identifiers",
         "faultReproduced": ("DATA-INTEGRITY s11.2" if reproduce_ocds_fault
                             else None)}))
    M.log(f"  processed/ocds_supplier_ids.json: {len(by_name)} of {total} "
          f"identifications ("
          f"{'fault reproduced' if reproduce_ocds_fault else 'full crosswalk set'}"
          f"), {refused} ambiguous name(s) refused")
    report["ocds"] = {"emitted": len(by_name), "inCrosswalk": total,
                      "ambiguousNamesRefused": refused,
                      "faultReproduced": reproduce_ocds_fault}

    con.close()
    return report


def copy_passthrough(src_home, root):
    n = 0
    for rel, why in PASSTHROUGH.items():
        s = Path(src_home) / "observatory-data" / rel
        if not s.exists():
            M.log(f"  MISSING passthrough {rel} ({why})")
            continue
        d = root / "home/observatory-data" / rel
        d.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(s, d)
        n += 1
    return n


def run_chain(root, scripts, dates, crosswalk_json, python="python3", only=None):
    env = dict(os.environ)
    env["HOME"] = str(root / "home")
    env["PYTHONPATH"] = str(root / "shim")
    env["OBS_CROSSWALK"] = str(crosswalk_json)
    results = []
    for step, clock in (only or CHAIN):
        env["OBS_V2_PIN_DATE"] = dates[clock]
        t0 = time.time()
        p = subprocess.run([python, f"{step}.py"], cwd=str(scripts),
                           env=env, capture_output=True, text=True)
        tail = "\n".join((p.stdout or "").strip().splitlines()[-3:])
        M.log(f"  {step} [{clock}={dates[clock]}]: rc={p.returncode} in {time.time()-t0:.0f}s")
        if p.returncode != 0:
            print(p.stdout[-4000:])
            print(p.stderr[-4000:], file=sys.stderr)
            raise SystemExit(f"FATAL: {step} failed in the shadow run")
        results.append({"step": step, "clock": dates[clock],
                        "seconds": round(time.time() - t0, 1),
                        "tail": tail})
    return results


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--as-of", required=True,
                    help="the date the SITE-JSON half of the edition was "
                         "stamped with, YYYY-MM-DD")
    ap.add_argument("--as-of-etl", default=None,
                    help="the date the ETL half ran on, where a run straddled "
                         "midnight. Defaults to --as-of.")
    ap.add_argument("--root", default="/opt/observatory/v2")
    ap.add_argument("--site", default="/opt/observatory/site")
    ap.add_argument("--src-home", default=str(Path.home()))
    ap.add_argument("--reproduce-ocds-fault", action="store_true",
                    help="emit the empty OCDS map the pre-fix editions carried, for reproducing one of those editions")
    ap.add_argument("--home", default=None, help="warehouse root override")
    ap.add_argument("--skip-project", action="store_true")
    ap.add_argument("--python", default="python3",
                    help="the interpreter production runs the chain with")
    args = ap.parse_args()

    root = Path(args.root)
    site = Path(args.site)
    scripts = root / "run/scripts/observatory"
    root.mkdir(parents=True, exist_ok=True)

    (root / "shim").mkdir(exist_ok=True)
    (root / "shim/sitecustomize.py").write_text(SHIM)

    n = stage_scripts(site / "scripts/observatory", scripts)
    M.log(f"staged {n} assembler scripts, all sha256-verified")
    (root / "run/public/data/company").mkdir(parents=True, exist_ok=True)

    if not args.skip_project:
        M.log("projecting marts into the shadow tree")
        report = project_all(root, args.reproduce_ocds_fault, args.home)
        M.log(f"copied {copy_passthrough(args.src_home, root)} pass-through "
              f"inputs of {len(PASSTHROUGH)}")
    else:
        report = {}

    xw = os.environ.get("OBS_CROSSWALK") or (
        Path("/root/aidoge/briefings/lancashire-business-observatory/"
             "geo_crosswalk.json"))
    dates = {"site": args.as_of, "etl": args.as_of_etl or args.as_of}
    M.log(f"running the unchanged chain, clocks pinned to {dates}")
    steps = run_chain(root, scripts, dates, xw, args.python)

    out = root / "run/public/data"
    files = sorted(p.name for p in out.glob("biz-*.json"))
    M.log(f"v2 emitted {len(files)} biz files: {files}")
    (root / "v2_run.json").write_text(json.dumps(
        {"asOf": args.as_of, "asOfEtl": dates["etl"],
         "reproduceOcdsFault": args.reproduce_ocds_fault,
         "projection": report,
         "steps": steps, "files": files,
         "dossiers": len(list((out / "company").glob("*.json")))}, indent=1))
    print(json.dumps({"files": files, "root": str(out)}, indent=1))


if __name__ == "__main__":
    main()
