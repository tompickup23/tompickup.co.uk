"""Verify the bronze layer against its manifests. Nonzero exit on any mismatch.

This is the integrity gate for the immutable layer. It re-hashes every file in
every partition and compares against the manifest that was written beside it, so
silent corruption, a truncated copy or an edited "raw" file all fail loudly.

Checks per partition:
  1. manifest.json exists and parses
  2. every file listed in the manifest is present
  3. every present file (except manifest.json) is listed in the manifest
  4. sha256 matches
  5. byte count matches
  6. the manifest carries asAt, retrievedAt and licence (DATA-INTEGRITY V-R3)

Usage:
    python3 check_bronze.py            # all partitions on this host
    python3 check_bronze.py --quick    # sizes only, no re-hash
    python3 check_bronze.py --json     # machine-readable report on stdout
"""
import argparse
import datetime as _dt
import hashlib
import json
import sys
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


def check_partition(part, quick):
    fails = []
    mpath = part / "manifest.json"
    if not mpath.exists():
        return [f"{part}: no manifest.json"], 0, 0
    try:
        m = json.loads(mpath.read_text())
    except Exception as e:
        return [f"{mpath}: unparseable ({e})"], 0, 0

    for field in ("licence", "retrievedAt"):
        if not m.get(field):
            fails.append(f"{part}: manifest missing {field} (V-R3)")
    if "asAt" not in m:
        fails.append(f"{part}: manifest has no asAt key (V-R3; null is allowed, absent is not)")

    listed = {e["name"]: e for e in m.get("files", [])}
    on_disk = {p.name for p in part.iterdir() if p.is_file() and p.name != "manifest.json"}

    for name in sorted(set(listed) - on_disk):
        fails.append(f"{part}/{name}: listed in manifest, missing on disk")
    for name in sorted(on_disk - set(listed)):
        fails.append(f"{part}/{name}: present on disk, absent from manifest")

    checked = 0
    for name in sorted(set(listed) & on_disk):
        p = part / name
        e = listed[name]
        size = p.stat().st_size
        if size != e["bytes"]:
            fails.append(f"{p}: size {size} != manifest {e['bytes']}")
            continue
        if not quick:
            d = sha256(p)
            if d != e["sha256"]:
                fails.append(f"{p}: sha256 {d[:16]} != manifest {e['sha256'][:16]}")
                continue
        checked += 1
    return fails, checked, m.get("totalBytes", 0)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--quick", action="store_true", help="sizes only, skip hashing")
    ap.add_argument("--json", action="store_true", dest="as_json")
    ap.add_argument("--host", choices=["mac", "vps"])
    args = ap.parse_args()

    host = args.host or S.host()
    bronze = S.bronze_dir(host)
    if not bronze.exists():
        log(f"no bronze layer at {bronze}")
        return 1

    parts = sorted(p for p in bronze.glob("source=*/snapshot_date=*") if p.is_dir())
    all_fails, files_ok, bytes_ok = [], 0, 0
    for part in parts:
        fails, checked, nbytes = check_partition(part, args.quick)
        all_fails += fails
        files_ok += checked
        bytes_ok += nbytes
        mark = "FAIL" if fails else "ok"
        rel = part.relative_to(bronze)
        log(f"{mark:>4}  {rel} ({checked} files)")

    report = {
        "host": host,
        "bronze": str(bronze),
        "checkedAt": _dt.datetime.now(_dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "partitions": len(parts),
        "filesVerified": files_ok,
        "bytesVerified": bytes_ok,
        "mode": "quick" if args.quick else "full-hash",
        "failures": all_fails,
    }
    if args.as_json:
        print(json.dumps(report, indent=2))
    else:
        log(f"{len(parts)} partitions, {files_ok} files verified, "
            f"{bytes_ok/1e6:.1f} MB, {len(all_fails)} failures")
        for f in all_fails:
            log(f"  FAIL {f}")
    return 1 if all_fails else 0


if __name__ == "__main__":
    sys.exit(main())
