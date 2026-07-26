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
Cleaning: employee values must be integers >= 0 after rounding; drop companies
whose series contains a >6x jump that then reverts (typo signature); drop
values > 20000; periods deduped by period_end keeping the LATEST filing's
value (restatements win).
Every candidate row carries the basis string; the site frames the list as an
Observatory assessment.
"""
import gzip, json, math
from collections import defaultdict
from datetime import date
from pathlib import Path

VPS = Path.home() / "observatory-data/vps"
PROC = Path.home() / "observatory-data/processed"

master = {}
with gzip.open(PROC / "master.jsonl.gz", "rt") as f:
    for line in f:
        r = json.loads(line)
        master[r["crn"]] = r

series = defaultdict(dict)   # crn -> period_end -> employees
fin = defaultdict(dict)      # crn -> period_end -> {equity, total_assets}
import io
def _account_lines():
    with gzip.open(VPS / "lancs_accounts.jsonl.gz", "rt") as f:
        yield from f
    bf = VPS / "lancs_accounts_backfill.jsonl"
    if bf.exists():
        with open(bf) as f:
            yield from f

if True:
    for line in _account_lines():
        r = json.loads(line)
        pe, crn = r["period_end"], r["crn"]
        if r.get("employees") is not None:
            series[crn][pe] = r["employees"]   # later lines = later zips: keep last
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
    candidates.append({
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

candidates.sort(key=lambda c: (-("ons-definition" in c["flags"]),
                               -c["latestEmployees"] * (c["cagrPct"] / 100)))
out = {
    "$meta": {
        "source": "Companies House Accounts Data Product monthly archives (prior-period comparatives included) plus API backfills",
        "retrieved": __import__("datetime").date.today().isoformat(),
        "definitions": "ons-definition: >=10 employees at base, >20% annualised "
                       "employment growth over >=2 years. emerging: 3-9 base, "
                       ">=50% annualised, labelled separately.",
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
