#!/usr/bin/env python3
"""Month-on-month changes: compare the current biz-*.json against the previous
archived edition and emit public/data/biz-changes.json for the /changes page.

Editions live in ~/observatory-data/archive/YYYY-MM/ (the cron archives after
each successful gated build). With no earlier edition, emits baseline mode:
the page explains the first edition and shows current-state highlights.
Everything reported is a delta between two register snapshots, stated as such.
"""
import json, sys
from datetime import date
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import sources_meta as SM

ROOT = Path(__file__).resolve().parent.parent.parent
PUB = ROOT / "public" / "data"
ARC = Path.home() / "observatory-data/archive"

def load(p):
    try:
        return json.loads(Path(p).read_text())
    except Exception:
        return None

today = date.today()
this_ed = today.strftime("%Y-%m")
editions = sorted(d.name for d in ARC.iterdir() if d.is_dir()) if ARC.exists() else []
prev_ed = next((e for e in reversed(editions) if e < this_ed), None)

cur = {n: load(PUB / f"biz-{n}.json") for n in
       ("overview", "growth", "pound", "watch")}
out = {"$meta": {"generated": today.isoformat(),
                 "edition": this_ed, "previousEdition": prev_ed,
                 # Gate V-R3: a published file states what it is drawn from
                 # and how recent that is, this one included. It used to carry
                 # no sources array at all, which read as though a diff had no
                 # inputs.
                 "sources": SM.sources(today.isoformat()),
                 "notes": [SM.DATE_NOTE],
                 "note": "Changes are differences between two monthly register "
                         "snapshots; a company appearing or leaving a list "
                         "reflects new filings, not an event on that day."}}

if not prev_ed:
    ov = cur["overview"] or {}
    gr = cur["growth"] or {}
    wa = cur["watch"] or {}
    out["mode"] = "baseline"
    out["baseline"] = {
        "companiesActive": sum(a.get("companiesActive") or 0
                               for a in ov.get("areas", [])),
        "gazelles": (gr.get("$meta") and len([c for c in gr.get("candidates", [])
                     if "ons-definition" in c.get("flags", [])])) or 0,
        "noticesListed": wa.get("summary", {}).get("noticesListed"),
        "strikeOffs": wa.get("summary", {}).get("currentStrikeOffProposals"),
    }
else:
    prev = {n: load(ARC / prev_ed / f"biz-{n}.json") for n in
            ("overview", "growth", "pound", "watch")}
    out["mode"] = "diff"

    # area count movements
    pv = {a["slug"]: a for a in (prev["overview"] or {}).get("areas", [])}
    moves = []
    for a in (cur["overview"] or {}).get("areas", []):
        p = pv.get(a["slug"])
        if not p:
            continue
        d = {"slug": a["slug"], "name": a["name"]}
        for k in ("companiesActive", "distressCleanPct", "highGrowthOfficialPct"):
            if a.get(k) is not None and p.get(k) is not None:
                d[k] = {"now": a[k], "then": p[k],
                        "delta": round(a[k] - p[k], 1)}
        moves.append(d)
    out["areaMovements"] = moves

    # gazelle list in/out
    def gz(g):
        return {c["crn"]: c for c in (g or {}).get("candidates", [])
                if "ons-definition" in c.get("flags", [])}
    cg, pg = gz(cur["growth"]), gz(prev["growth"])
    out["gazelles"] = {
        "entered": [{"crn": c, "name": cg[c]["name"], "lad": cg[c]["lad"]}
                    for c in sorted(set(cg) - set(pg))],
        "left": [{"crn": c, "name": pg[c]["name"], "lad": pg[c]["lad"]}
                 for c in sorted(set(pg) - set(cg))],
    }

    # new insolvency notices (by uri)
    def uris(w):
        return {n.get("uri"): n for n in (w or {}).get("notices", [])}
    cn, pn = uris(cur["watch"]), uris(prev["watch"])
    out["newNotices"] = [
        {"date": n.get("date"), "type": n.get("type"),
         "company": n.get("company"), "crn": n.get("crn"),
         "lad": n.get("lad"), "uri": u}
        for u, n in cn.items() if u not in pn][:100]

    # pound tier drift per council
    pvc = {c["body"]: c for c in (prev["pound"] or {}).get("councils", [])}
    drift = []
    for c in (cur["pound"] or {}).get("councils", []):
        p = pvc.get(c["body"])
        if not p:
            continue
        row = {"name": c["name"]}
        for t in ("rooted", "tradingExternal", "nonLocal", "unclassified"):
            a = (c["tiers"].get(t) or {}).get("pct")
            b = (p["tiers"].get(t) or {}).get("pct")
            if a is not None and b is not None:
                row[t] = round(a - b, 1)
        drift.append(row)
    out["tierDrift"] = drift

(PUB / "biz-changes.json").write_text(json.dumps(out))
print(f"biz-changes.json: mode={out['mode']}, edition={this_ed}, prev={prev_ed}")
