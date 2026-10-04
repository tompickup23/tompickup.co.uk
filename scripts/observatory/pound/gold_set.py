#!/usr/bin/env python3
"""Draw the clerical gold set (Reports Public_Pound_work/phase0/GOLD_SET.md).

Population: payee keys resolved by a pipeline step (1 to 7; step 8 is not
built) that are outside their body's hand-verified 80% set. Strata: method
group, then two value bands split at the stratum's median value per key.
Size per stratum n = n0 / (1 + (n0 - 1) / N), rounded up, the whole stratum
when N is smaller; n0 = 48 for identifier and register-specific methods,
139 for exact name, prefix and OCDS or GGIS. Each value band takes half of
n (rounded up, capped at the band). Simple random sampling, seeded. Keys
matched only through a waterfall variant (W1, W1b or W2, flagged in the
evidence) form their own stratum, whatever their method (RULES.md decision
18), with n0 = 139.

Recall sample: 200 keys ending at step 10 with reason no-candidate or
ambiguous, outside the verified set: 100 drawn with probability
proportional to value (systematic PPS from a seeded random start), 100 by
simple random sampling.

gold_set_manifest.json (seed, population sha256, stratum sizes) is written
BEFORE gold_set_pairs.csv, and the pairs file carries no label: labelling is
for a named reviewer (GOLD_SET.md s3). The three Splink strata are absent.
"""
import argparse
import csv
import hashlib
import json
import math
import random
import sys
from collections import defaultdict

from common import OUT, git_sha, sha256_file, write_json

csv.field_size_limit(sys.maxsize)

GROUPS = {"id-bank-inferred": ("step 1 and 1b, identifier", 48), "id-council": ("step 1 and 1b, identifier", 48),
          "name-postcode": ("step 2, name and postcode", 48),
          "name-exact": ("step 2b, exact name", 139), "name-exact-out": ("step 2b, exact name", 139),
          "name-prefix": ("step 2c, prefix", 139),
          "charity": ("step 3, charity", 48), "cqc-provider": ("step 4, CQC provider", 48),
          "public-body": ("step 5, public body", 48), "public-body-superseded": ("step 5, public body", 48),
          "gleif": ("step 6, GLEIF", 48),
          "ocds": ("step 7, OCDS and GGIS", 139), "ggis": ("step 7, OCDS and GGIS", 139)}
VARIANT = ("variant only (W1, W1b, W2)", 139)
N0 = dict(list(GROUPS.values()) + [VARIANT])


def size(n0, N):
    if N <= 0:
        return 0
    return min(N, math.ceil(n0 / (1 + (n0 - 1) / N)))


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--seed", type=int, default=20261004)
    a = ap.parse_args()
    verified = set()
    for p in OUT.glob("verify_*.csv"):
        if p.name.startswith("verify_suppliers") or p.name.startswith("verify_summary"):
            continue
        with open(p, newline="") as f:
            for r in csv.DictReader(f):
                verified.add((r["body_id"], r["payee_key"]))
    vals = defaultdict(int)
    with open(OUT / "payee_key_values.csv", newline="") as f:
        for r in csv.DictReader(f):
            vals[(r["body_id"], r["payee_key_out"])] += int(r["value_pence"])
    pop, recall_pop = [], []
    with open(OUT / "resolver_proposed.csv", newline="") as f:
        for r in csv.DictReader(f):
            k = (r["body_id"], r["payee_key"])
            if k in verified:
                continue
            if r["method"] in GROUPS:
                ev = json.loads(r["evidence"]) if r["evidence"] else {}
                st = VARIANT[0] if ev.get("w1_variant") or ev.get("w2_variant") else GROUPS[r["method"]][0]
                pop.append({"decision_id": r["decision_id"], "body_id": k[0], "payee_key": k[1],
                            "method": r["method"], "stratum": st,
                            "value_pence": vals[k], "org_scheme": r["org_scheme"], "org_id": r["org_id"]})
            elif r["reason"] in ("no-candidate", "ambiguous"):
                recall_pop.append({"decision_id": r["decision_id"], "body_id": k[0], "payee_key": k[1],
                                   "reason": r["reason"], "value_pence": vals[k]})
    pop.sort(key=lambda r: r["decision_id"])
    recall_pop.sort(key=lambda r: r["decision_id"])
    popf, recf = OUT / "gold_population.csv", OUT / "recall_population.csv"
    for path, rows in ((popf, pop), (recf, recall_pop)):
        with open(path, "w", newline="") as f:
            w = csv.DictWriter(f, fieldnames=list(rows[0]) if rows else ["decision_id"])
            w.writeheader()
            w.writerows(rows)
    by = defaultdict(list)
    for r in pop:
        by[r["stratum"]].append(r)
    plan = {}
    rng = random.Random(a.seed)
    draws = []
    for st in sorted(by):
        rows = sorted(by[st], key=lambda r: (r["value_pence"], r["decision_id"]))
        n0 = N0[st]
        N = len(rows)
        n = size(n0, N)
        med = rows[(N - 1) // 2]["value_pence"] if N else 0
        low = [r for r in rows if r["value_pence"] <= med]
        high = [r for r in rows if r["value_pence"] > med]
        nb = math.ceil(n / 2)
        take_low, take_high = min(len(low), nb), min(len(high), nb)
        # top up from the other band when one band is smaller than its half
        short = n - take_low - take_high
        if short > 0:
            if len(low) > take_low:
                take_low = min(len(low), take_low + short)
            elif len(high) > take_high:
                take_high = min(len(high), take_high + short)
        plan[st] = {"population": N, "n0": n0, "target_n": n, "median_value_pence": med,
                    "low_band": {"population": len(low), "drawn": take_low},
                    "high_band": {"population": len(high), "drawn": take_high}}
        for band, rs, t in (("low", low, take_low), ("high", high, take_high)):
            for r in rng.sample(rs, t):
                draws.append({**r, "value_band": band})
    # recall: 100 PPS (systematic, positive values only) and 100 SRS
    pos = [r for r in recall_pop if r["value_pence"] > 0]
    tot = sum(r["value_pence"] for r in pos)
    pps = []
    if pos and tot:
        step = tot / min(100, len(pos))
        start = rng.random() * step
        targets = [start + i * step for i in range(min(100, len(pos)))]
        cum, i = 0, 0
        for r in pos:
            cum += r["value_pence"]
            while i < len(targets) and targets[i] < cum:
                pps.append(r)
                i += 1
    srs = rng.sample(recall_pop, min(100, len(recall_pop)))
    man = {"seed": a.seed, "pipelineGitSha": git_sha(),
           "population_file": popf.name, "population_sha256": sha256_file(popf),
           "recall_population_file": recf.name, "recall_population_sha256": sha256_file(recf),
           "resolver_sha256": sha256_file(OUT / "resolver_proposed.csv"),
           "strata": plan, "pairs_total": len(draws),
           "recall": {"population": len(recall_pop), "pps_drawn": len(pps), "pps_distinct": len({r["decision_id"] for r in pps}),
                      "srs_drawn": len(srs)},
           "splink_strata": "absent: step 8 not built in Phase 1 (RULES.md decision 10)",
           "step9_second_review": "not drawn: no step 9 decision has been accepted yet",
           "note": "Written before any pair is shown. Labels are same, different or cannot tell, by a named reviewer."}
    write_json(OUT / "gold_set_manifest.json", man)
    fields = ["draw_no", "stratum", "value_band", "decision_id", "body_id", "payee_key", "method",
              "org_scheme", "org_id", "value_pence", "label", "reviewer", "labelled_at", "evidence_used"]
    with open(OUT / "gold_set_pairs.csv", "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader()
        for i, r in enumerate(draws, 1):
            w.writerow({"draw_no": i, **{k: r[k] for k in fields if k in r}, "label": "", "reviewer": "",
                        "labelled_at": "", "evidence_used": ""})
    with open(OUT / "recall_sample.csv", "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=["draw", "decision_id", "body_id", "payee_key", "reason", "value_pence",
                                          "finding", "reviewer", "searched_at", "notes"])
        w.writeheader()
        for kind, rs in (("pps", pps), ("srs", srs)):
            for r in rs:
                w.writerow({"draw": kind, **{k: r[k] for k in ("decision_id", "body_id", "payee_key", "reason", "value_pence")},
                            "finding": "", "reviewer": "", "searched_at": "", "notes": ""})
    print(json.dumps(man, indent=1))


if __name__ == "__main__":
    main()
