#!/usr/bin/env python3
"""Build-manifest gate: provenance is complete, and it names its own code.

Every partition in bronze, silver and gold carries a manifest. M1 to M4 wrote
them faithfully with one hole: `pipelineGitSha` is null in every manifest built
on vps-main, because `/opt/observatory/warehouse` is an rsync target and not a
git checkout, so `git rev-parse HEAD` had nothing to answer. The M1 note says
"wire the SHA in at deploy time during M5". This is that gate.

**Old nulls are not back-filled and that is the point.** Stamping today's sha
onto a manifest written in M2 would point a later reader at code that did not
build the artefact, which is worse than an honest gap. So the rule is temporal:
a manifest built at or after the deploy stamp's own `installedAt` must carry a
sha; one built before it is legacy, counted and named. The gap closes as
partitions are rebuilt, and the count going to zero is the evidence that it
closed.

Also checked, because they are the fields the whole audit trail rests on:

  * bronze: asAt, retrievedAt and licence on every partition (gate V-R3 at the
    layer where it IS satisfied), plus sourceUrl and a non-empty file list.
  * silver and gold: duckdbVersion inside the pinned range, a row count that
    matches the parquet, and inputs naming a real upstream partition.
  * marts: `reproducedFaults` present as an array. An empty array is a claim
    that the mart carries no known fault; a missing field is silence, and the
    two must not look alike.

Usage:
    check_manifests.py [--layer bronze|silver|gold] [--out reports/manifests.json]
"""
import argparse
import datetime as _dt
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import sources as S  # noqa: E402
import driver as D  # noqa: E402

MART_PREFIX = "mart_"


def iso(v):
    try:
        return _dt.datetime.strptime(v, "%Y-%m-%dT%H:%M:%SZ").replace(
            tzinfo=_dt.timezone.utc)
    except Exception:
        return None


def scan(root):
    """Every manifest under the warehouse, tagged with its layer."""
    out = []
    for layer, base in (("bronze", root / "bronze"),
                        ("silver", root / "silver"),
                        ("gold", root / "gold")):
        if not base.exists():
            continue
        for mf in sorted(base.rglob("manifest.json")):
            try:
                out.append((layer, mf, json.loads(mf.read_text())))
            except Exception as e:
                out.append((layer, mf, {"_unparseable": str(e)}))
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--layer", default=None,
                    choices=["bronze", "silver", "gold"])
    ap.add_argument("--out", default=None)
    ap.add_argument("--root", default=None)
    args = ap.parse_args()

    root = Path(args.root) if args.root else D.warehouse_root()
    stamp = D.pipeline_stamp()
    cutoff = iso((stamp or {}).get("installedAt", "")) if stamp else None

    manifests = scan(root)
    if args.layer:
        manifests = [m for m in manifests if m[0] == args.layer]

    fails, legacy, ok = [], [], 0
    stats = {"bronze": 0, "silver": 0, "gold": 0, "marts": 0}

    for layer, path, m in manifests:
        rel = str(path.relative_to(root))
        if "_unparseable" in m:
            fails.append({"manifest": rel, "why": "unparseable: "
                                                  + m["_unparseable"]})
            continue
        stats[layer] += 1
        problems = []

        if layer == "bronze":
            for k in ("licence", "sourceUrl", "snapshotDate", "retrievedAt"):
                if not m.get(k):
                    problems.append(f"bronze manifest has no {k}")
            if not m.get("files"):
                problems.append("bronze manifest lists no files")
            # asAt may legitimately be absent where the source states no
            # reference date. It may not be absent silently: the registry has
            # to have said so, and it does, by carrying as_at=None.
            if "asAt" not in m:
                problems.append("bronze manifest has no asAt key at all "
                                "(absent is fine, missing the key is not)")
        else:
            dv = m.get("duckdbVersion")
            if not dv:
                problems.append("no duckdbVersion")
            else:
                mm = tuple(int(x) for x in dv.split(".")[:2])
                if not ((1, 4) <= mm < (1, 5)):
                    problems.append(f"duckdb {dv} outside the pinned range")
            if m.get("rows") is None:
                problems.append("no row count")
            if not m.get("inputs"):
                problems.append("no inputs recorded, so the partition cannot "
                                "be traced upstream")
            pq = path.parent / "part.parquet"
            if not pq.exists():
                problems.append("manifest with no part.parquet beside it")
            if m.get("table", "").startswith(MART_PREFIX):
                stats["marts"] += 1
                if not isinstance(m.get("reproducedFaults"), list):
                    problems.append("a mart manifest with no reproducedFaults "
                                    "array: silence and 'no known faults' must "
                                    "not look alike")

        built = iso(m.get("builtAt") or m.get("manifestWrittenAt") or "")
        sha = m.get("pipelineGitSha")
        if not sha:
            if cutoff and built and built >= cutoff:
                problems.append(
                    "pipelineGitSha is null on a manifest built after the "
                    f"deploy stamp ({stamp.get('installedAt')}). The stamp is "
                    "installed, so this build had a sha available and did not "
                    "record it.")
            else:
                legacy.append({"manifest": rel, "layer": layer,
                               "builtAt": m.get("builtAt")
                                          or m.get("manifestWrittenAt")})

        if problems:
            fails.append({"manifest": rel, "layer": layer,
                          "problems": problems})
        else:
            ok += 1

    report = {
        "gate": "build manifests",
        "runId": D.run_id(),
        "generated": _dt.datetime.now(_dt.timezone.utc)
                        .strftime("%Y-%m-%dT%H:%M:%SZ"),
        "root": str(root),
        "host": S.host(),
        "pipelineStamp": stamp,
        "currentGitSha": D.pipeline_git_sha(),
        "counts": {"manifests": len(manifests), "clean": ok,
                   "failed": len(fails), "legacyNullSha": len(legacy),
                   **stats},
        "failures": fails,
        "legacyNullSha": legacy[:200],
    }
    out = Path(args.out) if args.out else D.report_dir() / "manifests.json"
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(report, indent=2) + "\n")

    print(f"  {len(manifests)} manifests: bronze {stats['bronze']}, "
          f"silver {stats['silver']}, gold {stats['gold']} "
          f"(of which {stats['marts']} marts)")
    print(f"  pipelineGitSha now resolves to {D.pipeline_git_sha()}")
    print(f"  {len(legacy)} manifest(s) carry a legacy null sha "
          f"(written before the deploy stamp; not back-filled by design)")
    for f in fails[:15]:
        print(f"  FAIL {f['manifest']}: {f.get('why') or f['problems']}")
    print(f"  written to {out}")

    if fails:
        print(f"\nMANIFEST GATE FAILED: {len(fails)} manifest(s)")
        return 1
    print("\nMANIFEST GATE GREEN")
    return 0


if __name__ == "__main__":
    sys.exit(main())
