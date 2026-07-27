#!/usr/bin/env python3
"""Invariant gate for the observatory's published JSON. Non-zero exit on any
violation; the monthly cron runs this AFTER build_site_json and BEFORE the
Astro build + deploy, so a bad upstream file can never ship silently.

These are the checks the launch fact-audit performed by hand, made permanent.
"""
import json, sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent.parent
PUB = ROOT / "public" / "data"
fails = []

def fail(msg):
    fails.append(msg)
    print(f"FAIL: {msg}")

def load(name):
    p = PUB / name
    if not p.exists():
        fail(f"{name} missing")
        return None
    try:
        return json.loads(p.read_text())
    except Exception as e:
        fail(f"{name} unparseable: {e}")
        return None

ov = load("biz-overview.json")
ar = load("biz-areas.json")
gr = load("biz-growth.json")
po = load("biz-pound.json")
wa = load("biz-watch.json")
mo = load("biz-money.json")
ix = load("biz-companies-index.json")

if ov:
    areas = ov.get("areas", [])
    units = ov.get("unitaries", [])
    if len(areas) != 14:
        fail(f"overview areas = {len(areas)}, want 14")
    if len(units) != 4:
        fail(f"overview unitaries = {len(units)}, want 4")
    by_slug = {a["slug"]: a for a in areas}
    for u in units:
        for k in ("companiesActive", "population", "cics"):
            got = u.get(k)
            want = sum(by_slug[m].get(k) or 0 for m in u.get("members", []))
            if got is not None and want and got != want:
                fail(f"unitary {u['slug']} {k}: {got} != member sum {want}")
    for a in areas + units:
        pop, comp, per1k = a.get("population"), a.get("companiesActive"), a.get("companiesPer1k")
        if pop and comp and per1k is not None:
            if abs(per1k - comp / pop * 1000) > 0.2:
                fail(f"{a['slug']} companiesPer1k {per1k} vs computed {comp/pop*1000:.1f}")
        for k, v in a.items():
            if k.endswith("Pct") and v is not None and not (0 <= v <= 100):
                fail(f"{a['slug']} {k} out of range: {v}")

if po:
    for c in po.get("councils", []):
        tiers = c.get("tiers", {})
        vsum = sum(t.get("valueM") or 0 for t in tiers.values())
        tot = c.get("spendTotalM") or 0
        if tot and abs(vsum - tot) > max(0.5, tot * 0.005):
            fail(f"pound {c['name']} tier sum {vsum:.1f} != total {tot:.1f}")
        psum = sum(t.get("pct") or 0 for t in tiers.values())
        if psum and abs(psum - 100) > 0.5:
            fail(f"pound {c['name']} tier pcts sum {psum:.1f}")
        cov = c.get("coveragePct")
        uncl = (tiers.get("unclassified") or {}).get("pct")
        if cov is not None and uncl is not None and abs((100 - uncl) - cov) > 0.2:
            fail(f"pound {c['name']} coverage {cov} vs 100-unclassified {100-uncl:.1f}")

if gr:
    for c in gr.get("candidates", []):
        s = c.get("series", [])
        if len(s) < 3:
            fail(f"growth {c['crn']} has <3 periods")
            break
        for f in c.get("flags", []):
            if f == "ons-definition" and c.get("baseEmployees", 0) < 10:
                fail(f"growth {c['crn']} ons-definition with base <10")

if wa:
    if any("Petition" in (n.get("type") or "") for n in wa.get("notices", [])):
        fail("watch contains winding-up petition notices (legal blocker)")
    for n in wa.get("notices", [])[:50]:
        if not n.get("uri"):
            fail(f"watch notice without source uri: {n.get('company')}")
            break

if mo:
    for d in mo.get("donors", []):
        rsum = round(sum(r["totalValue"] for r in d.get("recipients", [])), 2)
        if abs(rsum - d.get("donationTotal", 0)) > 0.02:
            fail(f"money {d['donorName']}: recipients {rsum} != total {d.get('donationTotal')}")
    n0 = (mo.get("$meta", {}).get("notes") or [""])[0]
    if "No finding of impropriety" not in n0:
        fail("money file missing no-finding-of-impropriety note in notes[0]")

if ix:
    n = len(ix.get("companies", []))
    if not (500 <= n <= 5000):
        fail(f"companies index size {n} outside sane range")

# em-dash sweep over the data files themselves
for f in PUB.glob("biz-*.json"):
    if "—" in f.read_text():
        fail(f"em-dash in {f.name}")

if fails:
    print(f"\n{len(fails)} invariant violation(s). DO NOT DEPLOY.")
    sys.exit(1)
print("all invariants hold")
