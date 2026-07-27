#!/usr/bin/env python3
"""Print a random sample of verified websites for hand inspection.

The acceptance gate for this layer is human: open the sample, confirm each
site really is that company. One wrong match means the rule gets tightened and
the crawl reruns, so this is run after every rule change.

Usage: python3 sample_websites.py [N] [--seed S]
"""
import argparse
import json
import random
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
from _common import PROC


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("n", nargs="?", type=int, default=25)
    ap.add_argument("--seed", type=int, default=0)
    ap.add_argument("--file", default=str(PROC / "websites.jsonl"))
    ap.add_argument("--json", action="store_true")
    args = ap.parse_args()

    rows = [json.loads(l) for l in Path(args.file).open() if l.strip()]
    random.seed(args.seed)
    samp = random.sample(rows, min(args.n, len(rows)))
    if args.json:
        print(json.dumps(samp, ensure_ascii=False, indent=1))
        return
    for i, r in enumerate(samp, 1):
        print(f"{i:>2}. {r['crn']}  {r['name']}")
        print(f"    {r['url']}   [{r['matchedOn']} via {r.get('candidateSource')}]")
        print(f"    evidence: {r['evidence'][:180]}")
        print()
    print(f"{len(samp)} of {len(rows)} verified rows sampled (seed {args.seed})")


if __name__ == "__main__":
    main()
