"""Shared plumbing for the silver layer.

Silver is one typed Parquet table per source per snapshot:

    silver/<source>/snapshot_date=YYYY-MM-DD/part.parquet
    silver/<source>/snapshot_date=YYYY-MM-DD/manifest.json

Rules this module enforces so no individual builder has to remember them:

  * silver is built from BRONZE only. resolve_bronze() will not look anywhere
    else, so a builder physically cannot read a live fetch or a working file.
  * every table carries snapshot_date and source_sha256 columns, so a row can
    always be traced back to the exact bronze bytes it came from.
  * the manifest records the duckdb version, the input partition, the input
    hashes and the row count, which is what the checks then assert against.
  * a build is atomic: write part.parquet.tmp, fsync, rename. A killed build
    leaves the previous edition intact rather than a half-written file.
"""
import datetime as _dt
import json
import os
import subprocess
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import sources as S  # noqa: E402


def log(msg):
    print(f"[{_dt.datetime.now().strftime('%H:%M:%S')}] {msg}", flush=True)


def silver_dir(h=None):
    return S.root(h) / "silver"


def connect():
    """DuckDB, version-pinned per the locked decision (>=1.4, <1.5)."""
    import duckdb
    v = duckdb.__version__
    major_minor = tuple(int(x) for x in v.split(".")[:2])
    if not ((1, 4) <= major_minor < (1, 5)):
        raise SystemExit(
            f"FATAL: duckdb {v} is outside the pinned range >=1.4,<1.5. "
            "The pin is a locked decision, not a preference.")
    con = duckdb.connect()
    con.execute("PRAGMA threads=4")
    con.execute("SET preserve_insertion_order=false")
    return con, v


def bronze_partitions(source_id, h=None):
    """Every snapshot_date partition present in bronze for a source, sorted."""
    base = S.bronze_dir(h) / f"source={source_id}"
    if not base.exists():
        return []
    out = []
    for p in sorted(base.glob("snapshot_date=*")):
        mf = p / "manifest.json"
        if mf.exists():
            out.append((p.name.split("=", 1)[1], p, json.loads(mf.read_text())))
    return out


def resolve_bronze(source_id, snapshot=None, h=None):
    """One bronze partition: the named snapshot, else the latest one.

    Raises rather than falling back to a working directory. A missing bronze
    partition is a build failure, never a reason to read somewhere else.
    """
    parts = bronze_partitions(source_id, h)
    if not parts:
        raise SystemExit(
            f"FATAL: no bronze partition for source={source_id}. "
            "Run build_bronze.py first; silver never reads a live fetch.")
    if snapshot:
        for snap, path, manifest in parts:
            if snap == snapshot:
                return snap, path, manifest
        raise SystemExit(
            f"FATAL: bronze has no snapshot_date={snapshot} for {source_id}. "
            f"Present: {[p[0] for p in parts]}")
    return parts[-1]


def bronze_file(manifest, path, suffix):
    """The one file in a partition whose name ends with suffix."""
    hits = [e for e in manifest["files"] if e["name"].endswith(suffix)]
    if len(hits) != 1:
        raise SystemExit(
            f"FATAL: expected exactly one '{suffix}' in {path}, found "
            f"{[e['name'] for e in hits]}")
    return path / hits[0]["name"], hits[0]["sha256"]


def git_sha():
    try:
        return subprocess.check_output(
            ["git", "rev-parse", "HEAD"], cwd=str(Path(__file__).resolve().parent),
            stderr=subprocess.DEVNULL).decode().strip()
    except Exception:
        return None


def table_dir(table, snapshot, h=None):
    d = silver_dir(h) / table / f"snapshot_date={snapshot}"
    d.mkdir(parents=True, exist_ok=True)
    return d


def write_parquet(con, select_sql, out_dir):
    """COPY a SELECT to part.parquet atomically. Returns (rows, bytes)."""
    final = out_dir / "part.parquet"
    tmp = out_dir / "part.parquet.tmp"
    if tmp.exists():
        tmp.unlink()
    con.execute(
        f"COPY ({select_sql}) TO '{tmp}' "
        "(FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE 200000)")
    fd = os.open(str(tmp), os.O_RDONLY)
    os.fsync(fd)
    os.close(fd)
    tmp.rename(final)
    rows = con.execute(
        f"SELECT count(*) FROM read_parquet('{final}')").fetchone()[0]
    return rows, final.stat().st_size


def write_manifest(out_dir, table, snapshot, inputs, rows, nbytes, duckdb_version,
                   assertions=None, notes=None, extra=None):
    m = {
        "table": table,
        "layer": "silver",
        "snapshotDate": snapshot,
        "rows": rows,
        "bytes": nbytes,
        "inputs": inputs,
        "duckdbVersion": duckdb_version,
        "pipelineGitSha": git_sha(),
        "builtOnHost": S.host(),
        "builtAt": _dt.datetime.now(_dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "assertions": assertions or {},
        "notes": notes,
    }
    if extra:
        m.update(extra)
    with open(out_dir / "manifest.json", "w") as f:
        json.dump(m, f, indent=2)
        f.write("\n")
    return m


def assert_equal(name, got, expected, hard=True):
    """A named count assertion. Hard ones abort the build.

    Exclusion counts are asserted rather than assumed because the blank-postcode
    accident that keeps our current counts clean is not a rule and will not
    survive a builder that filters on anything else (DATA-INTEGRITY s7.8).
    """
    ok = got == expected
    mark = "ok" if ok else "MISMATCH"
    log(f"  assert {name}: {got} (expected {expected}) {mark}")
    if not ok and hard:
        raise SystemExit(
            f"FATAL assertion {name}: got {got}, expected {expected}. "
            "Either the source changed shape or the rule is wrong. "
            "Do not relax the assertion without establishing which.")
    return ok
