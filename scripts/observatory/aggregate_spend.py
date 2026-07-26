#!/usr/bin/env python3
"""Aggregate council transparency spend per supplier, per body, FY2023-24 to 2025-26.

Reads  ~/clawd/burnley-council/data/<body>/spending-<FY>.json (type == "spend" ONLY;
       contracts double-count by design, purchase cards are merchant-level).
Writes ~/observatory-data/processed/council_supplier_spend.json

Suppliers below the floor are rolled into a disclosed tail so coverage maths
stay honest. Nothing here is published directly; build_pound.py consumes it.
"""
import json, gc
from pathlib import Path

DATA = Path.home() / "clawd/burnley-council/data"
OUT = Path.home() / "observatory-data/processed"
OUT.mkdir(parents=True, exist_ok=True)

BODIES = ["blackburn", "blackpool", "burnley", "chorley", "fylde", "hyndburn",
          "lancashire_cc", "lancashire_fire", "lancashire_pcc", "lancaster",
          "pendle", "preston", "ribble_valley", "rossendale", "south_ribble",
          "west_lancashire", "wyre"]
YEARS = ["2023-24", "2024-25", "2025-26"]
FLOOR = 10_000.0  # per-supplier 3yr total below this rolls into the tail

result = {}
for body in BODIES:
    agg, total, ntx = {}, 0.0, 0
    years_found = []
    for fy in YEARS:
        f = DATA / body / f"spending-{fy}.json"
        if not f.exists():
            continue
        years_found.append(fy)
        recs = json.loads(f.read_text())
        for r in recs:
            if r.get("type") != "spend":
                continue
            amt = r.get("amount") or 0.0
            name = (r.get("supplier_canonical") or r.get("supplier") or "").strip()
            if not name:
                continue
            agg[name] = agg.get(name, 0.0) + amt
            total += amt
            ntx += 1
        del recs
        gc.collect()
    kept = {k: round(v, 2) for k, v in agg.items() if v >= FLOOR}
    tail_v = round(sum(v for v in agg.values() if v < FLOOR), 2)
    result[body] = {
        "years": years_found, "total": round(total, 2), "transactions": ntx,
        "suppliers": dict(sorted(kept.items(), key=lambda kv: -kv[1])),
        "tail": {"value": tail_v, "suppliers": len(agg) - len(kept)},
    }
    print(f"{body}: £{total/1e6:.1f}m over {years_found}, "
          f"{len(kept)} suppliers >= £10k (tail £{tail_v/1e6:.2f}m / {len(agg)-len(kept)})")

out = {"$meta": {
    "source": "AI DOGE per-council transparency spend files, type=spend only",
    "note": "Over £500 transparency data, not total budget. Contracts and "
            "purchase cards excluded (double-count / merchant-level).",
    "floor": FLOOR, "years": YEARS},
    "bodies": result}
(OUT / "council_supplier_spend.json").write_text(json.dumps(out))
print("written", OUT / "council_supplier_spend.json")
