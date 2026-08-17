"""Formalise the existing raw cache into the immutable bronze layer.

    bronze/source=<id>/snapshot_date=YYYY-MM-DD/<file>
    bronze/source=<id>/snapshot_date=YYYY-MM-DD/manifest.json
    bronze/_quarantine/<relative path>        anything with no registry entry

COPIES, never moves. The originals stay exactly where the current pipeline
expects them, so this step cannot break the live build. Deleting originals is a
separate decision that only happens after check_bronze.py has run green twice.

Bronze is immutable by rule: if a file already exists in a partition with a
different hash, that is a build FAILURE, not an overwrite. Re-fetching a source
means a new snapshot_date, never a rewrite of an old one.

Usage:
    python3 build_bronze.py                 # copy + manifest everything for this host
    python3 build_bronze.py --dry-run       # say what would happen, touch nothing
    python3 build_bronze.py --source fhrs   # one source
"""
import argparse
import datetime as _dt
import hashlib
import json
import shutil
import subprocess
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import sources as S  # noqa: E402

CHUNK = 1 << 20


def log(msg):
    print(f"[{_dt.datetime.now().strftime('%H:%M:%S')}] {msg}", flush=True)


def sha256(path):
    h = hashlib.sha256()
    with open(path, "rb") as f:
        while True:
            b = f.read(CHUNK)
            if not b:
                break
            h.update(b)
    return h.hexdigest()


def git_sha():
    """See driver.pipeline_git_sha: the deploy stamp comes first, because
    /opt/observatory/warehouse is an rsync target and not a git checkout."""
    try:
        import driver as _D
        return _D.pipeline_git_sha()
    except Exception:
        pass
    try:
        return subprocess.check_output(
            ["git", "rev-parse", "HEAD"], cwd=str(Path(__file__).resolve().parent),
            stderr=subprocess.DEVNULL).decode().strip()
    except Exception:
        return None


def plan(host):
    """Map every file under the data root to a source, or to quarantine."""
    claimed = {}
    for src in S.for_host(host):
        for g in src["globs"]:
            if g.startswith("/"):
                base, pattern = Path(g).parent, Path(g).name
                found = sorted(base.glob(pattern)) if base.exists() else []
            else:
                found = [p for r in S.roots(host) for p in sorted(r.glob(g))]
            for p in found:
                if p.is_dir() or not p.exists():
                    continue
                if p in claimed:
                    raise SystemExit(
                        f"FATAL: {p} claimed by both {claimed[p][0]['id']} and {src['id']}")
                snap = S.resolve(src["snapshot_date"], p)
                if not snap:
                    raise SystemExit(f"FATAL: no snapshot_date resolved for {p}")
                claimed[p] = (src, snap)
    return claimed


def scan_unclaimed(host, claimed):
    """Raw-layer files with no registry entry. Derived layers are not bronze."""
    raw_dirs = ["raw", "work", "onspd"]
    out = []
    for data_root in S.roots(host):
        for d in raw_dirs:
            base = data_root / d
            if not base.exists():
                continue
            for p in sorted(base.rglob("*")):
                if p.is_file() and p not in claimed and not p.name.endswith(".part"):
                    out.append(p)
    return out


# How recently a file may have been written and still be landed. A fetcher
# still appending to its output produces a TORN capture: bronze records a
# partial file, hashes it, and the immutability guard then refuses to correct
# it, because a torn capture and a legitimately changed source look identical
# from the outside.
#
# Found in M5, live: a concurrent session was running the repaired
# fetch_ocds_ids.py while a bronze build ran, and 84 KB of a file that reached
# 160 KB ten minutes later was landed as though it were the whole thing. The
# guard did its job on the second pass and refused the overwrite, which is how
# the torn capture became visible at all.
#
# 180 seconds is chosen against the fetchers we have: they write their output
# in one pass at the end of a run, so a file untouched for three minutes is
# finished. A long-running append-mode writer would need the size-stability
# check below, which is why both are here rather than only the age one.
SETTLE_SECONDS = 180


def unsettled(p, now=None):
    """True if this file looks like it is still being written.

    Two independent tests, because each misses a different case: mtime age
    catches a writer that has stalled, and a size re-read catches a writer that
    is appending steadily enough to keep its mtime fresh.
    """
    now = now or _dt.datetime.now(_dt.timezone.utc).timestamp()
    st = p.stat()
    age = now - st.st_mtime
    if age < SETTLE_SECONDS:
        return f"modified {age:.0f}s ago, under the {SETTLE_SECONDS}s settle window"
    size1 = st.st_size
    time.sleep(0.4)
    if p.stat().st_size != size1:
        return "size changed while being read"
    return None


def write_partition(src, snap, files, bronze, dry, gsha):
    part = bronze / f"source={src['id']}" / f"snapshot_date={snap}"
    manifest_path = part / "manifest.json"
    entries = []
    for p in files:
        dest = part / p.name
        digest = sha256(p)
        if dest.exists():
            existing = sha256(dest)
            if existing != digest:
                raise SystemExit(
                    f"FATAL: bronze is immutable and {dest} already exists with a "
                    f"different hash.\n  on disk {existing}\n  incoming {digest}\n"
                    f"A changed source means a NEW snapshot_date, never an overwrite.")
            log(f"  = {p.name} already in bronze, hash matches")
        elif dry:
            log(f"  + would copy {p.name} ({p.stat().st_size/1e6:.1f} MB)")
        else:
            part.mkdir(parents=True, exist_ok=True)
            tmp = dest.with_suffix(dest.suffix + ".part")
            shutil.copy2(p, tmp)
            tmp.rename(dest)
            log(f"  + {p.name} ({p.stat().st_size/1e6:.1f} MB)")
        entries.append({
            "name": p.name,
            "sha256": digest,
            "bytes": p.stat().st_size,
            "sourcePath": str(p),
        })

    # retrievedAt is the file's own mtime, NEVER the partition key. Those
    # coincide only for sources partitioned by fetch date. Where the source
    # states its own edition date in the filename (the CH monthly bulk), the
    # partition key is that published date, because the same publication is
    # immutable by definition and re-fetching it must land in the same
    # partition rather than manufacturing a duplicate. Its retrievedAt is
    # genuinely later, and saying otherwise would be a lie in the audit trail.
    retrieved = min(
        _dt.datetime.utcfromtimestamp(p.stat().st_mtime) for p in files
    ).strftime("%Y-%m-%d") if files else None

    manifest = {
        "source": src["id"],
        "sourceName": src["name"],
        "snapshotDate": snap,
        "asAt": S.resolve(src["as_at"], files[0]) if files else None,
        "retrievedAt": retrieved,
        "licence": src["licence"],
        # as-published = the bytes the publisher served. derived-extract = our
        # own parse of a source whose raw bytes are gone (Companies House
        # retention). Both are immutable once landed; only one is evidence of
        # what the publisher actually said, so the distinction is recorded.
        "capture": src.get("capture", "as-published"),
        "sourceUrl": src["source_url"],
        "notes": src["notes"],
        "hosts": src["hosts"],
        "builtOnHost": S.host(),
        "files": entries,
        "fileCount": len(entries),
        "totalBytes": sum(e["bytes"] for e in entries),
        "manifestWrittenAt": _dt.datetime.now(_dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "pipelineGitSha": gsha,
    }
    if dry:
        log(f"  ~ would write {manifest_path}")
        return manifest
    part.mkdir(parents=True, exist_ok=True)
    with open(manifest_path, "w") as f:
        json.dump(manifest, f, indent=2)
        f.write("\n")
    return manifest


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--source", help="only this source id")
    ap.add_argument("--host", choices=["mac", "vps"], help="override host detection")
    args = ap.parse_args()

    host = args.host or S.host()
    bronze = S.bronze_dir(host)
    log(f"host={host} bronze={bronze}")

    claimed = plan(host)
    by_part = {}
    for p, (src, snap) in claimed.items():
        if args.source and src["id"] != args.source:
            continue
        by_part.setdefault((src["id"], snap), []).append(p)

    if not by_part:
        log("nothing to do for this host/source")
        return 0

    gsha = git_sha()
    total_files = total_bytes = 0
    deferred = []
    for (sid, snap), files in sorted(by_part.items()):
        src = S.BY_ID[sid]
        log(f"source={sid} snapshot_date={snap} ({len(files)} files)")
        # A partition is landed whole or not at all. If ANY file in it is
        # still being written, the whole partition waits for the next run:
        # half a partition recorded as complete is exactly the failure the
        # immutability guard then makes permanent.
        busy = [(p.name, why) for p in sorted(files)
                if (why := unsettled(p)) and p.exists()]
        if busy:
            for name, why in busy:
                log(f"  ! {name} deferred: {why}")
            deferred.append({"source": sid, "snapshot": snap,
                             "files": [b[0] for b in busy]})
            continue
        m = write_partition(src, snap, sorted(files), bronze, args.dry_run, gsha)
        total_files += m["fileCount"]
        total_bytes += m["totalBytes"]
    if deferred:
        log(f"DEFERRED {len(deferred)} partition(s), still being written: "
            f"{[d['source'] for d in deferred]}. Not an error: bronze lands a "
            "file when it has stopped moving, and the next run picks it up.")

    # Quarantine is a report, not a dumping ground: we copy nothing there
    # silently. An unclaimed file is a registry gap and must be named.
    unclaimed = scan_unclaimed(host, claimed)
    if unclaimed:
        log(f"UNCLAIMED ({len(unclaimed)}) - add a registry entry or quarantine:")
        for p in unclaimed[:40]:
            log(f"  ? {p}")
        if not args.dry_run:
            q = bronze / "_quarantine"
            q.mkdir(parents=True, exist_ok=True)
            with open(q / "unclaimed.json", "w") as f:
                json.dump({
                    "writtenAt": _dt.datetime.now(_dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
                    "host": host,
                    "paths": [str(p) for p in unclaimed],
                }, f, indent=2)
                f.write("\n")
    log(f"DONE: {len(by_part)} partitions, {total_files} files, "
        f"{total_bytes/1e6:.1f} MB, {len(unclaimed)} unclaimed")
    return 0


if __name__ == "__main__":
    sys.exit(main())
