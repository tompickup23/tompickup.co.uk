#!/usr/bin/env python3
"""Write a Snakemake rule's completion marker.

A tiny CLI rather than a Python heredoc inside the Snakefile, for one reason
that costs an hour to learn the hard way: Snakemake runs every `shell:` block
through `str.format`, so a brace in embedded Python has to be doubled, and a
missed pair fails at runtime inside a shell string with an unhelpful message.
A marker is the DAG's dependency edge; it should not be the fragile part.

Usage:
    mark.py --out state/silver/ch_register.done --rule silver_ch_register \\
            --table silver:ch_register --table silver:ch_psc_corporate \\
            --extra '{"gates": ["V-T1"]}' \\
            --from-json /opt/observatory/m5/v2_run.json:asOf,files,dossiers
"""
import argparse
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import driver as D  # noqa: E402


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", required=True)
    ap.add_argument("--rule", required=True)
    ap.add_argument("--root", default=None)
    ap.add_argument("--table", action="append", default=[],
                    help="layer:table, repeatable. Records the partition each "
                         "named table now points at, with its manifest hash.")
    ap.add_argument("--extra", default=None, help="a JSON object to merge in")
    ap.add_argument("--from-json", default=None,
                    help="path:key1,key2 - lift fields out of a report the "
                         "rule just wrote, so the marker carries the numbers "
                         "and not only the fact that something ran")
    ap.add_argument("--count-lines", default=None,
                    help="name=path, records a line count (the entity id "
                         "registry is the case this exists for)")
    args = ap.parse_args()

    root = Path(args.root) if args.root else D.warehouse_root()
    artefacts = {}
    for spec in args.table:
        layer, _, table = spec.partition(":")
        artefacts[table] = D.describe_partition(
            D.latest_partition(root / layer, table))

    extra = json.loads(args.extra) if args.extra else {}
    if args.from_json:
        path, _, keys = args.from_json.partition(":")
        try:
            d = json.loads(Path(path).read_text())
            for k in [k for k in keys.split(",") if k]:
                extra[k] = d.get(k)
        except Exception as e:
            extra["fromJsonError"] = f"{path}: {e}"
    if args.count_lines:
        name, _, path = args.count_lines.partition("=")
        try:
            with open(path) as f:
                extra[name] = sum(1 for _ in f)
        except Exception as e:
            extra[name] = f"unreadable: {e}"

    m = D.write_marker(args.out, args.rule, artefacts, extra)
    rows = {k: (v or {}).get("rows") for k, v in artefacts.items()}
    print(f"  marker {Path(args.out).name}: run {m['runId']}, "
          f"sha {m['pipelineGitSha']}, tables {rows}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
