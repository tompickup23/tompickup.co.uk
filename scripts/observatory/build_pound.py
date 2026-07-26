#!/usr/bin/env python3
"""Lancashire Pound stages 2+3: supplier -> company number -> ownership tier.

Reads  ~/observatory-data/processed/supplier_universe.json   (stage 1)
       ~/observatory-data/processed/council_supplier_spend.json
       ~/observatory-data/processed/master.jsonl.gz          (lancs register)
       ~/observatory-data/vps/register_index.tsv.gz          (5.7M UK companies)
       ~/observatory-data/vps/corporate_psc_all.jsonl.gz     (ownership edges)
       ~/observatory-data/vps/lancs_psc.jsonl.gz             (individual owners)
Writes ~/observatory-data/processed/pound.json
       ~/observatory-data/processed/pound_review_queue.json  (hand-check file)

Tiers (METHODS.md s2; language rules LEGAL.md):
  rooted           registered in Lancashire, ownership chain ends with
                   Lancashire-resident individuals
  tradingExternal  Lancashire-registered but externally/dispersedly owned,
                   OR non-local company with Lancashire trading evidence
  nonLocal         no Lancashire registration (trading evidence pass happens
                   in build_site_json when FHRS/CQC land)
  councilOwned     chain ends at a local authority
  unclassified     unmatched name, ambiguous match, or no ownership data
"""
import csv, gzip, json, re, sys
from collections import defaultdict
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
from resolve_suppliers import normalise, supplier_variants, PUBLIC_RE

PROC = Path.home() / "observatory-data/processed"
VPS = Path.home() / "observatory-data/vps"

print("loading master...")
lancs_crn = {}
lancs_outcodes = set()
with gzip.open(PROC / "master.jsonl.gz", "rt") as f:
    for line in f:
        r = json.loads(line)
        lancs_crn[r["crn"]] = r
        if r["postcode"]:
            lancs_outcodes.add(r["postcode"].split()[0])

print("loading register index...")
by_name = defaultdict(list)
with gzip.open(VPS / "register_index.tsv.gz", "rt") as f:
    rd = csv.reader(f, delimiter="\t")
    next(rd)
    for crn, name, pc, status in rd:
        by_name[normalise(name)].append((crn, pc, status))
print(f"  {len(by_name)} distinct normalised names")

print("loading ownership edges...")
parents = defaultdict(list)   # owned crn -> [{name, reg, country, ceased}]
with gzip.open(VPS / "corporate_psc_all.jsonl.gz", "rt") as f:
    for line in f:
        r = json.loads(line)
        if r.get("ceased_on"):
            continue
        parents[r["company_number"]].append(r)

print("loading lancs individual PSCs...")
indiv = defaultdict(list)     # lancs crn -> [{postcode, country_of_residence}]
with gzip.open(VPS / "lancs_psc.jsonl.gz", "rt") as f:
    for line in f:
        r = json.loads(line)
        if (r.get("kind") == "individual-person-with-significant-control"
                and not r.get("ceased_on")):
            indiv[r["company_number"]].append({
                "postcode": (r.get("postcode") or "").upper(),
                "cor": r.get("country_of_residence") or "",
            })
print(f"  {len(indiv)} lancs companies with live individual PSCs")

# Curated aliases: spend-side trading names -> verified company number.
# Each entry was checked against the register this session.
ALIASES = {
    "DOVEHAVEN CARE HOMES": "02078633",           # Dovehaven Residential Care Home Ltd, Southport
    "ZURICH MUNICIPAL": "00376989",               # trading name of Zurich Insurance (UK) Ltd
    "EDENRED": "10730187",                        # Edenred Corporate Payment UK Ltd
    "COMMUNITY LIGHTING PARTNERSHIP BPOOL": "06939062",
    "BN W DWEN AND BOLTON PHS2": "07385656",      # BwD and Bolton Phase 2 Ltd (PFI)
}

# Curated tier overrides where the register walk cannot see a public JV.
OVERRIDES = {
    "LOCAL PENSIONS PARTNERSHIP ADMINISTRATION": (
        "councilOwned",
        "Local Pensions Partnership Ltd is a joint venture of Lancashire "
        "County Council and the London Pensions Fund Authority."),
}

def match_entry(u):
    """-> (crn|None, how, ambiguous_pool)"""
    keys, seen = [], set()
    for raw in u["names"]:
        for k in supplier_variants(raw):
            if k not in seen:
                seen.add(k)
                keys.append(k)
    for k in keys:
        if k in ALIASES:
            return ALIASES[k], "alias", []
    amb_pool = []
    for k in keys:
        cands = by_name.get(k)
        if not cands:
            continue
        active = [c for c in cands if c[2] == "Active"]
        pool = active or cands
        lancs = [c for c in pool if c[0] in lancs_crn]
        if len(lancs) == 1:
            return lancs[0][0], "exact-lancs", []
        if len(pool) == 1:
            return pool[0][0], "exact", []
        if not amb_pool:
            amb_pool = (lancs or pool)[:6]
    if amb_pool:
        return None, "ambiguous", amb_pool
    return None, "no-match", []

def is_lancs_individuals(crn):
    """True/False/None(no data) for 'majority of individual PSCs Lancashire'."""
    ps = indiv.get(crn, [])
    if not ps:
        return None
    n_lancs = sum(1 for p in ps if p["postcode"].strip()
                  and p["postcode"].split()[0] in lancs_outcodes)
    return n_lancs * 2 >= len(ps)

def walk(crn, depth=0, seen=None):
    """-> (tier, chain[list of {name, crn, where}])"""
    seen = seen or set()
    if crn in seen or depth > 5:
        return "unclassified", []
    seen.add(crn)
    corp = parents.get(crn, [])
    if corp:
        p = corp[0]  # primary corporate parent
        pname = p.get("name") or ""
        if PUBLIC_RE.search(normalise(pname)):
            return "councilOwned", [{"name": pname, "where": "public body"}]
        preg = p.get("registration_number") or ""
        country = (p.get("country_registered") or "").lower()
        if not preg or not re.fullmatch(r"[0-9A-Za-z]{6,8}", preg) or (
                country and "england" not in country and "wales" not in country
                and "united kingdom" not in country and "scotland" not in country
                and "northern ireland" not in country):
            where = p.get("country_registered") or "unknown jurisdiction"
            return "tradingExternal", [{"name": pname, "where": where}]
        tier, chain = walk(preg.zfill(8) if preg.isdigit() else preg,
                           depth + 1, seen)
        where = "Lancashire" if preg in lancs_crn else (p.get("postcode") or "UK")
        link = [{"name": pname, "crn": preg, "where": where}]
        if tier == "unclassified" and chain == []:
            # terminal parent with no further data: locate it
            if preg in lancs_crn:
                li = is_lancs_individuals(preg)
                if li:
                    return "rooted", link
                if li is False:
                    return "tradingExternal", link
                return "unclassified", link
            return "tradingExternal", link
        return tier, link + chain
    li = is_lancs_individuals(crn)
    if li:
        return "rooted", []
    if li is False:
        return "tradingExternal", []
    # No live individual PSCs declared. Use company form at the terminal:
    m = lancs_crn.get(crn)
    ctype = (m.get("companyType") or "") if m else ""
    if m and ("Guarantee" in ctype or m.get("cic")
              or "EOT" in m["name"] or "EMPLOYEE OWN" in m["name"].upper()):
        return "rooted", [{"name": m["name"],
                           "where": "Lancashire (guarantee/community form, "
                                    "no registrable owners)"}]
    if m and "Public Limited" in ctype:
        return "tradingExternal", [{"name": m["name"],
                                    "where": "listed company, dispersed ownership"}]
    return "unclassified", []

def tier_for(crn):
    m = lancs_crn.get(crn)
    if not m:
        return "nonLocal", [], None
    tier, chain = walk(crn)
    return tier, chain, m

print("resolving universe...")
uni = json.loads((PROC / "supplier_universe.json").read_text())["universe"]
resolved, review = {}, []
match_stats = defaultdict(lambda: [0, 0.0])
for u in uni:
    crn, how, amb_pool = match_entry(u)
    if u["key"] in OVERRIDES:
        tier, basis = OVERRIDES[u["key"]]
        resolved[u["key"]] = {"crn": crn, "matchHow": "override", "tier": tier,
                              "chain": [{"name": basis, "where": "verified"}]}
        match_stats["override"][0] += 1
        match_stats["override"][1] += u["total"]
        continue
    if crn:
        tier, chain, m = tier_for(crn)
        resolved[u["key"]] = {
            "crn": crn, "matchHow": how, "tier": tier, "chain": chain,
            "lad": m["lad"] if m else None,
            "registeredPostcode": m["postcode"] if m else None,
        }
    elif amb_pool:
        # unanimity rule: if every candidate resolves to the same tier, the
        # ambiguity does not affect classification
        tiers = {tier_for(c[0])[0] for c in amb_pool}
        if len(tiers) == 1:
            how = "ambiguous-unanimous"
            resolved[u["key"]] = {"crn": None, "matchHow": how,
                                  "tier": tiers.pop(), "chain": [],
                                  "candidates": [c[0] for c in amb_pool]}
        else:
            resolved[u["key"]] = {"crn": None, "matchHow": how,
                                  "tier": "unclassified", "chain": [],
                                  "candidateTiers": sorted(tiers)}
    else:
        resolved[u["key"]] = {"crn": None, "matchHow": how,
                              "tier": "unclassified", "chain": []}
    match_stats[how][0] += 1
    match_stats[how][1] += u["total"]
    if u["total"] >= 2_000_000:
        r = resolved[u["key"]]
        review.append({"name": u["names"][0], "total": u["total"],
                       "bodies": u["bodies"], **r})

for how, (n, v) in sorted(match_stats.items(), key=lambda kv: -kv[1][1]):
    print(f"  {how}: {n} suppliers, £{v/1e6:.0f}m")

# per-council tier aggregation ------------------------------------------------
spend = json.loads((PROC / "council_supplier_spend.json").read_text())["bodies"]
from resolve_suppliers import classify
councils = {}
for body, d in spend.items():
    tiers = defaultdict(float)
    denom = 0.0
    for name, amt in d["suppliers"].items():
        c = classify(name)
        if c != "supplier":
            continue
        denom += amt
        tiers[resolved.get(normalise(name), {"tier": "unclassified"})["tier"]] += amt
    tiers["unclassified"] += d["tail"]["value"]  # tail is unresolved by design
    denom += d["tail"]["value"]
    councils[body] = {
        "privateSpend": round(denom, 2), "years": d["years"],
        "tiers": {k: round(v, 2) for k, v in tiers.items()},
    }
    top = {k: round(100 * v / denom, 1) for k, v in tiers.items() if denom}
    print(f"{body}: {top}")

(PROC / "pound.json").write_text(json.dumps({
    "$meta": {"stage": "2+3", "note": "trading-evidence refinement + review "
              "queue corrections applied in build_site_json"},
    "resolved": resolved, "councils": councils}))
(PROC / "pound_review_queue.json").write_text(json.dumps(
    {"$meta": {"floor": 2_000_000,
               "note": "every supplier >= £2m 3yr total, for hand verification"},
     "queue": sorted(review, key=lambda r: -r["total"])}, indent=1))
print(f"review queue: {len(review)} suppliers >= £2m")
