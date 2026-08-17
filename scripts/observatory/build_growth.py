#!/usr/bin/env python3
"""Gazelle / high-growth engine from filed-accounts employee series.

Reads  ~/observatory-data/vps/lancs_accounts.jsonl.gz (139k period records)
       ~/observatory-data/processed/master.jsonl.gz
Writes ~/observatory-data/processed/growth.json

Definitions (METHODS.md s1, cite ONS/Eurostat):
 - ons-definition: base employees >= 10 and employment CAGR > 20%/yr over a
   span of >= 2 years (3 accounting periods).
 - eurostat-10: same base, CAGR > 10%.
 - emerging: base 3-9 employees, CAGR >= 50%, span >= 2 years. Labelled
   separately; NOT called high-growth on the site.
 - young-company: incorporated <= 6 years before latest period end.
Cleaning: an s411 average outside 0 to 500,000 is an iXBRL artefact and is
dropped before anything else sees it (accounts_rules); employee values must be
integers >= 0 after rounding; drop companies whose series contains a >6x jump
that then reverts (typo signature); drop values > 20000; a period resolves to
the filing made most recently, so a restatement replaces the original filing
(accounts_rules.resolve_latest).
Every candidate row carries the basis string; the site frames the list as an
Observatory assessment.
"""
import gzip, json, math, sys
from collections import defaultdict
from datetime import date
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import accounts_rules as AR

VPS = Path.home() / "observatory-data/vps"
PROC = Path.home() / "observatory-data/processed"

master = {}
with gzip.open(PROC / "master.jsonl.gz", "rt") as f:
    for line in f:
        r = json.loads(line)
        master[r["crn"]] = r

series = defaultdict(dict)   # crn -> period_end -> employees
fin = defaultdict(dict)      # crn -> period_end -> {equity, total_assets}
def _account_lines():
    with gzip.open(VPS / "lancs_accounts.jsonl.gz", "rt") as f:
        yield from f
    bf = VPS / "lancs_accounts_backfill.jsonl"
    if bf.exists():
        with open(bf) as f:
            yield from f

# One filing per accounting period, chosen by filing date rather than by
# stream position. See accounts_rules for why position was the wrong rule and
# what filed_zip actually carries. The same call resolves the same stream in
# build_dossiers.py, so the two published views of a company's accounts cannot
# disagree about which filing they are showing.
resolved_accounts = AR.resolve_latest(json.loads(line) for line in _account_lines())
for crn, periods in resolved_accounts.items():
    for pe, r in periods.items():
        if r.get("employees") is not None:
            series[crn][pe] = r["employees"]
        fin[crn][pe] = {"equity": r.get("equity"), "assets": r.get("total_assets")}

def parse_d(s):
    y, m, d = s.split("-")
    return date(int(y), int(m), int(d))

candidates, stats = [], defaultdict(int)
for crn, periods in series.items():
    m = master.get(crn)
    if not m or m["status"] != "Active" or m["cluster"]:
        stats["skipped_inactive_or_cluster"] += 1
        continue
    pts = sorted(((parse_d(pe), e) for pe, e in periods.items()
                  if e is not None and 0 <= e <= 20000), key=lambda t: t[0])
    pts = [(d, round(e)) for d, e in pts if abs(e - round(e)) < 0.01]
    if len(pts) < 3:
        stats["lt3_periods"] += 1
        continue
    # typo signature: mid-value >6x neighbours then reverts
    vals = [e for _, e in pts]
    if any(vals[i] > 6 * max(1, vals[i-1]) and vals[i] > 6 * max(1, vals[i+1])
           for i in range(1, len(vals) - 1)):
        stats["typo_dropped"] += 1
        continue
    # sustained step anomaly: a single-year jump of more than 6x is almost
    # always an iXBRL tagging artefact (e.g. a micro-entity filing claiming
    # hundreds of staff); exclude pending manual verification rather than
    # publish an implausible growth rate
    if any(vals[i + 1] > 6 * max(1, vals[i]) for i in range(len(vals) - 1)):
        stats["step_anomaly_dropped"] += 1
        continue
    d0, e0 = pts[0]
    d1, e1 = pts[-1]
    span = (d1 - d0).days / 365.25
    if span < 2.0 or e0 <= 0:
        stats["short_span_or_zero_base"] += 1
        continue
    cagr = (e1 / e0) ** (1 / span) - 1
    flags = []
    if e0 >= 10 and cagr > 0.20:
        flags.append("ons-definition")
    elif e0 >= 10 and cagr > 0.10:
        flags.append("eurostat-10")
    elif 3 <= e0 <= 9 and cagr >= 0.50:
        flags.append("emerging")
    if not flags:
        stats["no_flag"] += 1
        continue
    age = m.get("ageYears")
    if age is not None and age <= 6.0:
        flags.append("young-company")
    birch = round((e1 - e0) * (e1 / e0), 1)
    f0 = fin.get(crn, {}).get(d0.isoformat()) or {}
    f1 = fin.get(crn, {}).get(d1.isoformat()) or {}
    assets_cagr = None
    if (f0.get("assets") or 0) > 10000 and (f1.get("assets") or 0) > 0:
        assets_cagr = round(100 * ((f1["assets"] / f0["assets"]) ** (1 / span) - 1), 1)
    candidates.append({
        "birchIndex": birch, "assetsCagrPct": assets_cagr,
        "crn": crn, "name": m["name"], "lad": m["lad"],
        "unitary2028": m["unitary2028"], "sic2": m["sic2"],
        "series": [{"periodEnd": d.isoformat(), "employees": e} for d, e in pts],
        "baseEmployees": e0, "latestEmployees": e1,
        "cagrPct": round(100 * cagr, 1), "spanYears": round(span, 1),
        "flags": flags,
        "basis": (f"Average employee numbers as disclosed in filed accounts "
                  f"(s411 Companies Act 2006), periods ending "
                  f"{d0.isoformat()} to {d1.isoformat()}."),
    })

# momentum signals (SH01 equity filings, new charges) from the CH API sweep
momentum = {}
mf = VPS / "momentum.jsonl"
if mf.exists():
    for line in mf.open():
        r = json.loads(line)
        momentum[r["crn"]] = r
for c in candidates:
    mo = momentum.get(c["crn"])
    if mo:
        c["momentum"] = {
            "sh01Last24m": mo.get("sh01_24m", 0),
            "sh01Latest": mo.get("sh01_latest"),
            "newCharges24m": mo.get("charges_24m", 0),
            "chargeLatest": mo.get("charge_latest"),
        }

candidates.sort(key=lambda c: (-("ons-definition" in c["flags"]),
                               -c["birchIndex"]))
out = {
    "$meta": {
        "source": "Companies House Accounts Data Product monthly archives (prior-period comparatives included) plus API backfills",
        "retrieved": __import__("datetime").date.today().isoformat(),
        "definitions": "ons-definition: >=10 employees at base, >20% annualised "
                       "employment growth over >=2 years. emerging: 3-9 base, "
                       ">=50% annualised, labelled separately. Ranked by the "
                       "Birch employment index (Et-E0)x(Et/E0), which balances "
                       "absolute and relative growth.",
        "persistenceCaveat": "The research literature finds high growth is "
                             "rarely persistent: most firms that meet the "
                             "definition in one period do not repeat it. This "
                             "list is a snapshot, not a prediction.",
        "cleaning": dict(stats),
    },
    "counts": {
        "companiesWithSeries": len(series),
        "candidates": len(candidates),
        "onsDefinition": sum(1 for c in candidates if "ons-definition" in c["flags"]),
        "eurostat10": sum(1 for c in candidates if "eurostat-10" in c["flags"]),
        "emerging": sum(1 for c in candidates if "emerging" in c["flags"]),
    },
    "candidates": candidates,
}
(PROC / "growth.json").write_text(json.dumps(out))
print(json.dumps(out["counts"], indent=1))
print("cleaning:", dict(stats))
byla = defaultdict(int)
for c in candidates:
    if "ons-definition" in c["flags"]:
        byla[c["lad"]] += 1
print("ONS-definition by LAD:", dict(sorted(byla.items(), key=lambda kv: -kv[1])))
