#!/usr/bin/env python3
"""Measures that need no person (RULES.md decisions 12 and 13; method rules
2.5 and 2.7). Every figure here is labelled agent-labelled or as an
estimate; none is clerical precision or verification.

  * Agent-labelled precision on the gold set, per stratum: rows with a Wilson
    95% interval (pairs both passes labelled same, of pairs labelled same or
    different; "cannot tell", including every disagreement, is counted and
    left out of the denominator); the value bands combined with population
    weights; and pounds, by the ratio estimator within each value band,
    combined with population value weights, with a seeded bootstrap interval
    (2,000 resamples within bands). Then all strata combined the same way.
  * The agreement rate between the two passes, by item set.
  * Recall as an estimate, against the registers searched: missed keys are
    recall-sample keys both passes matched to the same candidate record.
    Rows from the simple random draw, pounds from the value-proportional draw;
    resolved keys and value are those outside the 80% sets, the frame the
    recall population was drawn from.
  * Corroborated and agent-reviewed shares of supplier spend per body and year
    (from coverage.json), and the two together.
  * Owner-level concentration per body and year on resolved supplier spend
    (method rule 2.7): HHI on the 0 to 10,000 scale and the top owner's share.
    The owner is the ownership walk's unit at the top of the chain (the
    company itself when nobody controls it); a company whose controller is an
    individual is its own unit, because persons are never linked across
    companies (rule 1.2). A resolved organisation with no Companies House
    number is its own unit. Owners with a net negative total count as nil.
  * The Scottish Government invoice-address definition (rule 2.3): not
    computable, because no bank file carries an address.

Writes agent_measures.json, precision.csv and concentration.csv in
$POUND_WORK/out.
"""
import csv
import json
import math
import random
import sys
from collections import defaultdict

from common import OUT, YEARS, git_sha, sha256_file, write_json

csv.field_size_limit(sys.maxsize)
Z = 1.96
SEED = 20261004


def wilson(k, n):
    if n == 0:
        return None, None, None
    p = k / n
    d = 1 + Z * Z / n
    c = (p + Z * Z / (2 * n)) / d
    h = Z * math.sqrt(p * (1 - p) / n + Z * Z / (4 * n * n)) / d
    return round(p, 4), round(max(0.0, c - h), 4), round(min(1.0, c + h), 4)


def weighted(bands, weight_key):
    """Combine per-band estimates with population weights. bands: list of
    dicts with same, labelled, v_same, v_lab, N, V. Returns rows and pounds."""
    num_r = den_r = num_v = den_v = 0.0
    for b in bands:
        if b["labelled"]:
            num_r += b["N"] * b["same"] / b["labelled"]
            den_r += b["N"]
        if b["v_lab"] > 0:
            num_v += b["V"] * b["v_same"] / b["v_lab"]
            den_v += b["V"]
    return (num_r / den_r if den_r else None), (num_v / den_v if den_v else None)


def bootstrap(bands, rng, reps=2000):
    """Pound precision interval: resample labelled pairs within each band."""
    est = []
    for _ in range(reps):
        bs = []
        for b in bands:
            pts = b["pairs"]
            if not pts:
                bs.append({**b, "labelled": 0, "v_lab": 0})
                continue
            s = [pts[rng.randrange(len(pts))] for _ in pts]
            bs.append({**b, "same": sum(1 for x in s if x[0]), "labelled": len(s),
                       "v_same": sum(x[1] for x in s if x[0]), "v_lab": sum(x[1] for x in s)})
        est.append(weighted(bs, "V")[1])
    est = sorted(e for e in est if e is not None)
    if not est:
        return None, None
    return round(est[int(0.025 * len(est))], 4), round(est[int(0.975 * len(est)) - 1], 4)


def main():
    if not (OUT / "agent_labels.csv").exists():
        return main_without_labels()
    labels = list(csv.DictReader(open(OUT / "agent_labels.csv", newline="")))
    man = json.loads((OUT / "gold_set_manifest.json").read_text())
    rng = random.Random(SEED)
    out = {"$meta": {"pipelineGitSha": git_sha(),
                     "status": "Agent-labelled: two independent model passes, no person labelled any row "
                               "(RULES.md decisions 12 and 13). Never clerical precision or verification.",
                     "inputs": [{"file": n, "sha256": sha256_file(OUT / n)} for n in
                                ("agent_labels.csv", "gold_set_manifest.json", "gold_population.csv",
                                 "recall_population.csv", "recall_sample.csv", "resolver_proposed.csv",
                                 "payee_key_values.csv", "coverage.json", "ownership_walk.csv")],
                     "agentReview": json.loads((OUT / "agent_review_manifest.json").read_text()).get("usage")}}
    # ---- agreement
    agree = {}
    for s in ("gold", "recall", "verify"):
        rs = [r for r in labels if r["item_set"] == s and r["agreed_label"] != "not reviewed"]
        same = sum(1 for r in rs if r["pass_a_label"] == r["pass_b_label"] and
                   (s != "recall" or r["pass_a_label"] != "same" or r["pass_a_records"] == r["pass_b_records"]))
        agree[s] = {"reviewed": len(rs), "passes_agree": same, "agreement_rate": wilson(same, len(rs)),
                    "agreed": {k: sum(1 for r in rs if r["agreed_label"] == k)
                               for k in ("same", "different", "cannot tell")},
                    "not_reviewed": sum(1 for r in labels if r["item_set"] == s and r["agreed_label"] == "not reviewed")}
    out["agreement"] = agree
    # ---- precision on the gold set
    pop = list(csv.DictReader(open(OUT / "gold_population.csv", newline="")))
    lab = {(r["body_id"], r["payee_key"]): r for r in labels if r["item_set"] == "gold"}
    pairs = list(csv.DictReader(open(OUT / "gold_set_pairs.csv", newline="")))
    strata = {}
    for st, plan in man["strata"].items():
        med = plan["median_value_pence"]
        bands = []
        for band in ("low", "high"):
            members = [p for p in pop if p["stratum"] == st and
                       ((int(p["value_pence"]) <= med) == (band == "low"))]
            drawn = [p for p in pairs if p["stratum"] == st and p["value_band"] == band]
            pts, ct, nr = [], 0, 0
            for p in drawn:
                a = lab.get((p["body_id"], p["payee_key"]), {}).get("agreed_label", "not reviewed")
                if a in ("same", "different"):
                    pts.append((a == "same", max(0, int(p["value_pence"]))))
                elif a == "cannot tell":
                    ct += 1
                else:
                    nr += 1
            bands.append({"band": band, "N": len(members), "V": sum(max(0, int(p["value_pence"])) for p in members),
                          "drawn": len(drawn), "cannot_tell": ct, "not_reviewed": nr, "pairs": pts,
                          "same": sum(1 for x in pts if x[0]), "labelled": len(pts),
                          "v_same": sum(x[1] for x in pts if x[0]), "v_lab": sum(x[1] for x in pts)})
        k = sum(b["same"] for b in bands)
        n = sum(b["labelled"] for b in bands)
        wr, wv = weighted(bands, "V")
        lo, hi = bootstrap(bands, rng)
        strata[st] = {"population": plan["population"], "drawn": sum(b["drawn"] for b in bands),
                      "labelled_same_or_different": n, "agent_same": k,
                      "cannot_tell": sum(b["cannot_tell"] for b in bands),
                      "not_reviewed": sum(b["not_reviewed"] for b in bands),
                      "row_precision_wilson": wilson(k, n),
                      "row_precision_population_weighted": round(wr, 4) if wr is not None else None,
                      "pound_precision": round(wv, 4) if wv is not None else None,
                      "pound_precision_bootstrap_95": [lo, hi], "_bands": bands}
    allb = [b for s in strata.values() for b in s["_bands"]]
    wr, wv = weighted(allb, "V")
    lo, hi = bootstrap(allb, rng)
    out["precision"] = {"label": "agent-labelled precision (no person labelled these pairs)",
                        "by_stratum": {s: {k: v for k, v in d.items() if k != "_bands"} for s, d in strata.items()},
                        "all_strata": {"row_precision_population_weighted": round(wr, 4) if wr is not None else None,
                                       "pound_precision": round(wv, 4) if wv is not None else None,
                                       "pound_precision_bootstrap_95": [lo, hi]}}
    with open(OUT / "precision.csv", "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["stratum", "population", "drawn", "labelled_same_or_different", "agent_same", "cannot_tell",
                    "not_reviewed", "row_precision", "row_wilson_low", "row_wilson_high",
                    "row_precision_population_weighted", "pound_precision", "pound_bootstrap_low",
                    "pound_bootstrap_high"])
        for s, d in sorted(strata.items()):
            p, l_, h_ = d["row_precision_wilson"]
            w.writerow([s, d["population"], d["drawn"], d["labelled_same_or_different"], d["agent_same"],
                        d["cannot_tell"], d["not_reviewed"], p, l_, h_, d["row_precision_population_weighted"],
                        d["pound_precision"], *d["pound_precision_bootstrap_95"]])
    # ---- recall (an estimate)
    rpop = list(csv.DictReader(open(OUT / "recall_population.csv", newline="")))
    N = len(rpop)
    Vpos = sum(max(0, int(r["value_pence"])) for r in rpop)
    # a key the blocked search found no candidate for is not missed against the
    # registers searched: it counts in the denominator as "no candidate found"
    rl = {(r["body_id"], r["payee_key"]): ("no candidate found" if r["pass_a_reason"] == "no register record to show"
                                          else r["agreed_label"]) for r in labels if r["item_set"] == "recall"}
    draws = list(csv.DictReader(open(OUT / "recall_sample.csv", newline="")))

    def tally(kind):
        ls = [rl.get((d["body_id"], d["payee_key"]), "not reviewed") for d in draws if d["draw"] == kind]
        return {"draws": len(ls), "missed": ls.count("same"), "not_in_candidates": ls.count("different"),
                "no_candidate_found": ls.count("no candidate found"), "cannot_tell": ls.count("cannot tell"),
                "not_reviewed": ls.count("not reviewed")}
    srs, pps = tally("srs"), tally("pps")
    verified = set()
    for p in OUT.glob("verify_*.csv"):
        if p.name.startswith(("verify_suppliers", "verify_summary")):
            continue
        for r in csv.DictReader(open(p, newline="")):
            verified.add((r["body_id"], r["payee_key"]))
    res = {(r["body_id"], r["payee_key"]): r for r in csv.DictReader(open(OUT / "resolver_proposed.csv", newline=""))}
    val = defaultdict(int)
    for r in csv.DictReader(open(OUT / "payee_key_values.csv", newline="")):
        val[(r["body_id"], r["payee_key_out"])] += int(r["value_pence"])
    R_keys = [k for k, r in res.items() if r["org_id"] and k not in verified]
    R_n, R_v = len(R_keys), sum(max(0, val[k]) for k in R_keys)

    def recall_est(t, scale, resolved):
        n = t["missed"] + t["not_in_candidates"] + t["no_candidate_found"]
        p, lo, hi = wilson(t["missed"], n)
        if p is None:
            return None
        m = [scale * x for x in (p, lo, hi)]
        rec = [resolved / (resolved + x) if resolved + x else None for x in m]
        return {"share_missed": [p, lo, hi], "estimated_missed": [round(x) for x in m],
                "recall": [round(rec[0], 4), round(rec[2], 4), round(rec[1], 4)]}
    out["recall"] = {"label": "recall, an estimate, against the registers searched (agent-labelled; no person searched)",
                     "population_keys": N, "population_value_pence_positive": Vpos,
                     "resolved_outside_80pc_sets": {"keys": R_n, "value_pence_positive": R_v},
                     "srs": srs, "pps": pps,
                     "rows": recall_est(srs, N, R_n), "pounds": recall_est(pps, Vpos, R_v),
                     "note": "Missed: both passes matched the key to the same candidate record from a blocked search "
                             "of the Companies House, Charity Commission, CQC, public-body and GLEIF extracts. Not missed: "
                             "both passes said none of the candidates is the payee, or the search found no candidate. "
                             "Cannot tell is left out of the denominator. Not missed is not the same as absent from "
                             "every register. Interval columns are the "
                             "point, then the low and high ends."}
    out.update(shared_measures())
    write_json(OUT / "agent_measures.json", out)
    print(json.dumps({"agreement": agree, "precision_all": out["precision"]["all_strata"],
                      "recall": {k: out["recall"][k] for k in ("rows", "pounds")}}, indent=1, default=str))


def main_without_labels():
    """Before the agent review has run: the measures that need no label."""
    out = {"$meta": {"pipelineGitSha": git_sha(),
                     "status": "Agent review not run: agent_labels.csv is absent, so agent-labelled precision, "
                               "the agreement rate and recall are not computed.",
                     "inputs": [{"file": n, "sha256": sha256_file(OUT / n)} for n in
                                ("resolver_proposed.csv", "payee_key_values.csv", "coverage.json",
                                 "ownership_walk.csv")]},
           "agreement": None, "precision": None, "recall": None}
    out.update(shared_measures())
    write_json(OUT / "agent_measures.json", out)
    print(json.dumps({k: out[k] for k in ("shares",)}, indent=1)[:2000])


def shared_measures():
    out = {}
    res = {(r["body_id"], r["payee_key"]): r for r in csv.DictReader(open(OUT / "resolver_proposed.csv", newline=""))}
    # ---- corroborated and agent-reviewed shares per body and year
    cov = json.loads((OUT / "coverage.json").read_text())["bodies"]
    out["shares"] = {k: {"supplier_spend": d["line_supplier_spend"],
                         "resolved_pct": d["supplier_spend_resolved_pct"],
                         "corroborated_pct": d["supplier_spend_corroborated_pct"],
                         "agent_reviewed_pct": d["supplier_spend_agent_reviewed_pct"],
                         "corroborated_or_agent_reviewed_pct": d["supplier_spend_corroborated_or_agent_reviewed_pct"],
                         "hand_verified_pct": d["hand_verified_pct"]} for k, d in cov.items()}
    # ---- owner-level concentration
    walk = {}
    for r in csv.DictReader(open(OUT / "ownership_walk.csv", newline="")):
        walk[(r["company_number"], r["financial_year"])] = r["ultimate_company"]
    owners = defaultdict(lambda: defaultdict(int))
    for r in csv.DictReader(open(OUT / "payee_key_values.csv", newline="")):
        x = res[(r["body_id"], r["payee_key_out"])]
        if not x["org_id"] or x["payee_class"] != "supplier":
            continue
        crn = x["org_id"] if x["org_scheme"] == "GB-COH" else next(
            (a[7:] for a in x["alias_ids"].split("|") if a.startswith("GB-COH-")), None)
        unit = f"GB-COH-{walk.get((crn, r['financial_year']), crn)}" if crn else f"{x['org_scheme']}-{x['org_id']}"
        owners[(r["body_id"], r["financial_year"])][unit] += int(r["value_pence"])
    conc = []
    for (b, fy), o in sorted(owners.items()):
        pos = {k: v for k, v in o.items() if v > 0}
        tot = sum(pos.values())
        hhi = sum((10000 * v / tot) ** 2 / 10000 for v in pos.values()) if tot else None
        top = max(pos.values()) if pos else 0
        conc.append({"body_id": b, "financial_year": fy, "owners": len(pos), "resolved_supplier_pence": tot,
                     "hhi": round(hhi) if hhi is not None else None,
                     "hhi_above_1800": hhi is not None and hhi > 1800,
                     "top_owner_share_pct": round(100 * top / tot, 1) if tot else None})
    with open(OUT / "concentration.csv", "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=list(conc[0]))
        w.writeheader()
        w.writerows(conc)
    out["concentration"] = {"rule": "method rule 2.7; HHI on the 0 to 10,000 scale, above 1,800 high (Open "
                                    "Contracting thresholds as cited in RULES.md); top-owner share reported, no R040 "
                                    "flag computed", "rows": conc}
    out["scottish_definition"] = ("Not computed: none of the 41 columns in the 28 pinned bank files carries a supplier "
                                  "or invoice address, so the Scottish Government invoice-address definition (rule 2.3) "
                                  "cannot be built from the banks.")
    return out


if __name__ == "__main__":
    main()
