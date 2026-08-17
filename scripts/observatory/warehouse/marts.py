"""Shared plumbing for the gold MART layer (M4).

A mart is a gold table shaped for one consumer. The consumer here is the
existing site pipeline, and the whole point of M4 is that the site cannot tell
the difference, so the marts are deliberately shaped like the files the old
fetch/ETL layer used to hand over:

    gold/mart_<name>/snapshot_date=YYYY-MM-DD/part.parquet
    gold/mart_<name>/snapshot_date=YYYY-MM-DD/manifest.json

Three rules that are not obvious and are easy to get wrong:

1. **A mart replaces an ETL step, never an aggregation step.** The marts below
   stand in for refresh_register.py, the PSC and accounts extractors and the
   Gazette fetcher. Everything downstream of them (build_master, build_growth,
   build_pound, build_dossiers, build_site_json, build_diff) runs UNCHANGED in
   M4. Re-implementing an aggregation would make the golden-file test a test of
   my typing rather than a test of the warehouse.

2. **The marts reproduce the PRODUCTION basis, not the correct one.** Where the
   live pipeline has a known fault, the mart reproduces it and says so in a
   comment naming the DATA-INTEGRITY section. The golden-file gate is "the site
   notices nothing"; fixing a live figure is a separate task with its own
   sign-off, because the figures are published.

3. **The clock is an input.** build_master and build_site_json call
   date.today(), so a re-emission on a different day is not the same
   computation. The v2 runner pins the date to the edition being reproduced.
"""
import datetime as _dt
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import sources as S  # noqa: E402
import silver as SV  # noqa: E402
import crosswalk as XW  # noqa: E402

log = SV.log
connect = SV.connect
assert_equal = SV.assert_equal


# The 14 billing/registration authorities the site treats as Lancashire.
# Verbatim from refresh_register.py TARGET_LADS; this is the production scope
# and M4 does not touch it. The 131,961-vs-103,468 question is F1's (11.8).
TARGET_LADS = {
    "E07000121": "Lancaster", "E07000123": "Preston",
    "E07000124": "Ribble Valley", "E07000119": "Fylde",
    "E07000128": "Wyre", "E06000009": "Blackpool",
    "E06000008": "Blackburn with Darwen", "E07000120": "Hyndburn",
    "E07000125": "Rossendale", "E07000122": "Pendle",
    "E07000117": "Burnley", "E07000118": "Chorley",
    "E07000126": "South Ribble", "E07000127": "West Lancashire",
}

# ONSPD postcode areas the production ETL loads. Anything outside these eight
# areas cannot be matched to a Lancashire LAD, by construction.
ONSPD_AREAS = ["BB", "PR", "FY", "LA", "L", "OL", "WN", "BL"]


def mart_dir(h=None):
    return XW.gold_dir(h)


def table_dir(table, snapshot, h=None):
    d = mart_dir(h) / table / f"snapshot_date={snapshot}"
    d.mkdir(parents=True, exist_ok=True)
    return d


def write_manifest(out_dir, table, snapshot, rows, nbytes, duckdb_version,
                   inputs=None, assertions=None, notes=None,
                   reproduced_faults=None, cleared_faults=None, extra=None):
    """Gold-mart manifest.

    `reproducedFaults` is a first-class field rather than a note. A mart that
    knowingly carries a live fault has to say which one, in machine-readable
    form, or the next session has no way of telling a bug from a decision.

    `clearedFaults` is where an entry goes when it is fixed, with the commit
    that fixed it and the count it moved. A fault that simply disappears from
    the register leaves a reader unable to tell a fix from an oversight, and
    leaves the next diff in published figures unexplained.
    """
    m = {
        "table": table,
        "layer": "gold",
        "kind": "mart",
        "snapshotDate": snapshot,
        "rows": rows,
        "bytes": nbytes,
        "inputs": inputs or [],
        "duckdbVersion": duckdb_version,
        "pipelineGitSha": SV.git_sha(),
        "builtOnHost": S.host(),
        "builtAt": _dt.datetime.now(_dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "assertions": assertions or {},
        "reproducedFaults": reproduced_faults or [],
        "clearedFaults": cleared_faults or [],
        "notes": notes,
    }
    if extra:
        m.update(extra)
    with open(out_dir / "manifest.json", "w") as f:
        json.dump(m, f, indent=2)
        f.write("\n")
    return m


def write_parquet(con, select_sql, out_dir):
    return SV.write_parquet(con, select_sql, out_dir)


def onspd_lookup_sql(h=None):
    """A DuckDB relation of (postcode_norm, lad_code) from BRONZE ONSPD.

    Production reads the eight extracted area CSVs from /opt/observatory/onspd.
    Bronze holds the same eight files under source=onspd, so this is the same
    lookup read from an immutable, hash-verified copy instead of a working
    directory.
    """
    parts = SV.bronze_partitions("onspd", h)
    if not parts:
        raise SystemExit("FATAL: no bronze partition for source=onspd")
    # The partition that holds the extracted per-area CSVs, not the zip.
    chosen = None
    for snap, path, manifest in parts:
        names = [e["name"] for e in manifest["files"]]
        if any(n.endswith(".csv") for n in names):
            chosen = (snap, path, manifest)
    if not chosen:
        raise SystemExit(
            "FATAL: bronze source=onspd has no extracted per-area CSVs. "
            "The zip partition alone cannot be read without unpacking, and a "
            "mart never unpacks into a working directory.")
    snap, path, manifest = chosen
    files = sorted(e["name"] for e in manifest["files"]
                   if e["name"].endswith(".csv"))
    missing = [a for a in ONSPD_AREAS
               if not any(f.endswith(f"_UK_{a}.csv") for f in files)]
    if missing:
        raise SystemExit(
            f"FATAL: bronze ONSPD partition {snap} is missing areas {missing}. "
            "A partial lookup silently shrinks the Lancashire frame.")
    globs = ", ".join(f"'{path / f}'" for f in files)
    sql = (
        "SELECT DISTINCT upper(replace(trim(pcds), ' ', '')) AS postcode_norm, "
        "       lad25cd AS lad_code "
        f"FROM read_csv([{globs}], header=true, all_varchar=true, "
        "               ignore_errors=true) "
        "WHERE pcds IS NOT NULL AND pcds <> '' "
        "  AND lad25cd IS NOT NULL AND lad25cd <> ''")
    return sql, snap, path, files


def ch_date_str(col):
    """CH bulk dates are DD/MM/YYYY strings and build_master splits on '/'.

    Silver stores them as DATE, so the projection has to put them back in the
    register's own format. A NULL becomes the empty string, which is what the
    CSV carries and what g() returns.
    """
    return (f"CASE WHEN {col} IS NULL THEN '' "
            f"ELSE strftime({col}, '%d/%m/%Y') END")
