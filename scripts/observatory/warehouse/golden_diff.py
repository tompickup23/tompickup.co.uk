#!/usr/bin/env python3
"""The M4 golden-file test: does the site notice the migration?

LOCKED semantics from the handoff: normalise both sides (sort keys, sort arrays
on their id field, null out $meta.generated) and diff. Zero semantic diff is
the gate. Byte identity is NOT required and is not tested.

What "normalise" means here, precisely, because a vague normaliser can hide a
real difference:

  * dict keys are sorted recursively;
  * every list is sorted by the canonical JSON of its elements, which is an
    id-agnostic total order and therefore stronger than sorting on a named id
    field: it needs no per-array configuration and cannot pick the wrong key;
  * run-date fields are nulled, and every one of them is REPORTED separately
    before it is nulled, so a normaliser can never quietly absorb a date that
    should have matched. The fields are $meta.generated, $meta.retrieved and
    $meta.sources[].retrieved, plus any $meta.asAt that embeds the run date.

Because list order is normalised away, a pure re-ordering is not a failure. It
is still worth knowing about, so it is counted and reported as ORDER-ONLY.

Usage:
    golden_diff.py --golden <dir> --candidate <dir> [--json report.json]
"""
import argparse
import json
import sys
from pathlib import Path

RUN_DATE_PATHS = [
    ("$meta", "generated"),
    ("$meta", "retrieved"),
]
MAX_SHOWN = 12


def canon(x):
    """Recursively canonical form: sorted keys, sorted lists."""
    if isinstance(x, dict):
        return {k: canon(x[k]) for k in sorted(x)}
    if isinstance(x, list):
        items = [canon(v) for v in x]
        return sorted(items, key=lambda v: json.dumps(v, sort_keys=True,
                                                      ensure_ascii=False))
    return x


def strip_run_dates(doc):
    """Null the run-date fields and return what was there before."""
    found = {}
    meta = doc.get("$meta") if isinstance(doc, dict) else None
    if isinstance(meta, dict):
        for key in ("generated", "retrieved", "asAt"):
            if key in meta:
                found[f"$meta.{key}"] = meta[key]
                meta[key] = None
        srcs = meta.get("sources")
        if isinstance(srcs, list):
            seen = set()
            for s in srcs:
                if isinstance(s, dict) and "retrieved" in s:
                    seen.add(s["retrieved"])
                    s["retrieved"] = None
            if seen:
                found["$meta.sources[].retrieved"] = sorted(
                    v for v in seen if v is not None)
    return found


def walk(a, b, path="", out=None, limit=400):
    out = [] if out is None else out
    if len(out) >= limit:
        return out
    if type(a) is not type(b) and not (
            isinstance(a, (int, float)) and isinstance(b, (int, float))):
        out.append((path, f"type {type(a).__name__}", f"type {type(b).__name__}"))
        return out
    if isinstance(a, dict):
        for k in sorted(set(a) | set(b)):
            if k not in a:
                out.append((f"{path}.{k}", "<absent>", _short(b[k])))
            elif k not in b:
                out.append((f"{path}.{k}", _short(a[k]), "<absent>"))
            else:
                walk(a[k], b[k], f"{path}.{k}", out, limit)
    elif isinstance(a, list):
        if len(a) != len(b):
            out.append((f"{path}[]", f"{len(a)} items", f"{len(b)} items"))
            sa = {json.dumps(v, sort_keys=True) for v in a}
            sb = {json.dumps(v, sort_keys=True) for v in b}
            for extra in sorted(sa - sb)[:3]:
                out.append((f"{path}[] golden-only", _short(extra), ""))
            for extra in sorted(sb - sa)[:3]:
                out.append((f"{path}[] candidate-only", "", _short(extra)))
            return out
        for i, (x, y) in enumerate(zip(a, b)):
            walk(x, y, f"{path}[{i}]", out, limit)
    elif a != b:
        out.append((path, _short(a), _short(b)))
    return out


def _short(v, n=160):
    s = v if isinstance(v, str) else json.dumps(v, ensure_ascii=False)
    return s if len(s) <= n else s[:n] + "..."


def compare(golden, candidate):
    g = json.loads(Path(golden).read_text())
    c = json.loads(Path(candidate).read_text())
    g_dates = strip_run_dates(g)
    c_dates = strip_run_dates(c)
    raw_order_match = json.dumps(g, sort_keys=True) == json.dumps(c, sort_keys=True)
    gc, cc = canon(g), canon(c)
    diffs = walk(gc, cc)
    return {
        "semanticDiffs": len(diffs),
        "diffs": diffs[:MAX_SHOWN],
        "truncated": max(0, len(diffs) - MAX_SHOWN),
        "orderOnly": bool(not diffs and not raw_order_match),
        "runDates": {"golden": g_dates, "candidate": c_dates,
                     "match": g_dates == c_dates},
    }


def compare_dossiers(golden_dir, cand_dir):
    """Not part of the gate, but 1,000 files is the strongest single signal."""
    g = {p.stem: p for p in Path(golden_dir).glob("*.json")}
    c = {p.stem: p for p in Path(cand_dir).glob("*.json")}
    both = sorted(set(g) & set(c))
    differing = []
    for crn in both:
        a = json.loads(g[crn].read_text())
        b = json.loads(c[crn].read_text())
        strip_run_dates(a)
        strip_run_dates(b)
        if json.dumps(canon(a), sort_keys=True) != json.dumps(canon(b), sort_keys=True):
            differing.append(crn)
    return {"golden": len(g), "candidate": len(c),
            "goldenOnly": sorted(set(g) - set(c))[:20],
            "candidateOnly": sorted(set(c) - set(g))[:20],
            "compared": len(both), "differing": len(differing),
            "differingSample": differing[:20]}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--golden", required=True)
    ap.add_argument("--candidate", required=True)
    ap.add_argument("--json", default=None)
    ap.add_argument("--dossiers", action="store_true")
    args = ap.parse_args()

    gdir, cdir = Path(args.golden), Path(args.candidate)
    names = sorted({p.name for p in gdir.glob("biz-*.json")} |
                   {p.name for p in cdir.glob("biz-*.json")})
    report, total = {}, 0
    print(f"{'file':32} {'result':>10}  detail")
    print("-" * 96)
    for name in names:
        g, c = gdir / name, cdir / name
        if not g.exists() or not c.exists():
            report[name] = {"error": "missing on "
                            + ("golden" if not g.exists() else "candidate")}
            print(f"{name:32} {'MISSING':>10}  {report[name]['error']}")
            total += 1
            continue
        r = compare(g, c)
        report[name] = r
        total += r["semanticDiffs"]
        if r["semanticDiffs"]:
            verdict = "DIFF"
            detail = f"{r['semanticDiffs']} differing paths"
        elif r["orderOnly"]:
            verdict = "order-only"
            detail = "identical as a multiset; list order differs"
        else:
            verdict = "identical"
            detail = ""
        if not r["runDates"]["match"]:
            detail += ("  [run dates differ: golden "
                       f"{r['runDates']['golden'].get('$meta.generated')} vs "
                       f"candidate {r['runDates']['candidate'].get('$meta.generated')}"
                       ", normalised out]")
        print(f"{name:32} {verdict:>10}  {detail}")
        for path, a, b in r["diffs"]:
            print(f"    {path}\n      golden    {a}\n      candidate {b}")
        if r["truncated"]:
            print(f"    ... and {r['truncated']} more")

    if args.dossiers:
        d = compare_dossiers(gdir / "company", cdir / "company")
        report["_dossiers"] = d
        print(f"\ndossiers: {d['golden']} golden, {d['candidate']} candidate, "
              f"{d['compared']} compared, {d['differing']} differing")
        if d["differing"]:
            print(f"  sample: {d['differingSample']}")

    print(f"\nTOTAL semantic diffs across {len(names)} files: {total}")
    if args.json:
        Path(args.json).write_text(json.dumps(report, indent=1))
    return 1 if total else 0


if __name__ == "__main__":
    sys.exit(main())
