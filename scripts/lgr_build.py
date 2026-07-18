#!/usr/bin/env python3
"""Build the Lancashire LGR model from the canonical dataset.

Reads  src/data/lgr/{authorities,county,decision}.json  (hand-verified, sourced)
Writes src/data/lgr/model.json         (consumed by src/pages/lgr.astro at build)
       public/data/lgr-model.json      (public data feed, same content + provenance)

Everything computed here is deterministic and traceable to the canonical files:
no figure may be introduced in this script that is not derived from them.

Apportionment bases for LCC's county-service budget:
  population : each unitary's share of LCC-served population (Blackpool and
               Blackburn with Darwen are already unitary, so no county spend
               is apportioned to them and their own budgets are added whole).
  needs      : population weighted by the health-demand score, as a first-order
               proxy for adult-social-care-driven cost distribution. Modelled,
               not a service-level costing.
"""
import json
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
DATA = ROOT / "src" / "data" / "lgr"

auth = json.loads((DATA / "authorities.json").read_text())
county = json.loads((DATA / "county.json").read_text())
decision = json.loads((DATA / "decision.json").read_text())
precedents = json.loads((DATA / "precedents.json").read_text())
cca = json.loads((DATA / "cca.json").read_text())
pensions = json.loads((DATA / "pensions.json").read_text())
government = json.loads((DATA / "government.json").read_text())
scen_in = json.loads((DATA / "scenarios.json").read_text())

A = auth["authorities"]
LCC_BUDGET = county["netBudget2627_m"]
BAND_D_COUNTY = county["bandD_county"]

WEIGHTED_METRICS = [
    "healthDemandScore", "noQuals", "empRate", "socialRentPct",
    "crimeRate", "fsmPct", "ealPct", "hmoPer1k",
]

def wavg(names, key):
    pairs = [(A[n]["metrics"][key], A[n]["population"]) for n in names
             if A[n]["metrics"].get(key) is not None]
    tw = sum(w for _, w in pairs)
    return round(sum(v * w for v, w in pairs) / tw, 1) if tw else None

all_names = list(A)
baseline = {k: wavg(all_names, k) for k in WEIGHTED_METRICS}

# LCC-served population and needs weights (districts only)
districts = [n for n in all_names if A[n]["type"] == "district"]
lcc_pop = {n: A[n]["population"] for n in districts}
LCC_SERVED = sum(lcc_pop.values())
need_w = {n: A[n]["population"] * A[n]["metrics"]["healthDemandScore"] for n in districts}
NEED_TOTAL = sum(need_w.values())

unitaries = []
for uname, u in decision["unitaries"].items():
    members = u["councils"]
    dist = [n for n in members if A[n]["type"] == "district"]
    pop = sum(A[n]["population"] for n in members)
    served = sum(A[n]["population"] for n in dist)

    app_pop = round(LCC_BUDGET * served / LCC_SERVED, 1)
    app_need = round(LCC_BUDGET * sum(need_w[n] for n in dist) / NEED_TOTAL, 1)

    owns = [{
        "council": n, "type": A[n]["type"], "m": A[n]["netBudget2627_m"],
        "basis": A[n]["budgetBasis"], "confidence": A[n]["budgetConfidence"],
        "source": A[n]["budgetSource"],
    } for n in members]
    own_total = round(sum(o["m"] for o in owns), 1)
    combined = round(app_pop + own_total, 1)
    combined_need = round(app_need + own_total, 1)

    # Balance sheet brought by the constituent councils (LCC's excluded — its
    # county-wide reserves/debt split across successors is undetermined).
    reserves = round(sum(A[n].get("reserves_m", 0) for n in members), 1)
    debt = round(sum(A[n].get("debt_m_2026", A[n].get("debt_m", 0)) for n in members), 1)

    # Band D council-element bill per constituent: district own + county precept;
    # existing unitaries pay their own all-in element (no county precept).
    bills = []
    for n in members:
        own = A[n]["bandD_own"]
        bill = round(own + (BAND_D_COUNTY if A[n]["type"] == "district" else 0), 2)
        bills.append({"council": n, "ownElement": own, "councilBill": bill,
                      "confidence": A[n]["bandDConfidence"]})
    dwell = {n: A[n]["metrics"]["chargeableDwellings"] for n in members}
    wsum = sum(dwell.values())
    band_avg = round(sum(b["councilBill"] * dwell[b["council"]] for b in bills) / wsum, 2)
    lo = min(bills, key=lambda b: b["councilBill"])
    hi = max(bills, key=lambda b: b["councilBill"])

    unitaries.append({
        "name": uname, "accent": u["accent"], "home": u.get("home", False),
        "note": u.get("note"), "councils": members,
        "population": pop, "lccServedPop": served,
        "chargeableDwellings": wsum,
        "metrics": {k: wavg(members, k) for k in WEIGHTED_METRICS},
        "countyApportioned": {"population": app_pop, "needs": app_need},
        "perCapitaCounty": round(app_pop * 1e6 / served) if served else None,
        "constituentReserves_m": reserves, "constituentDebt_m": debt,
        "owns": owns, "ownTotal": own_total,
        "combined": {"population": combined, "needs": combined_need},
        "combinedPerCapita": round(combined * 1e6 / pop),
        "bandD": {
            "bills": bills, "dwellingWeightedAvg": band_avg,
            "lowest": {"council": lo["council"], "bill": lo["councilBill"]},
            "highest": {"council": hi["council"], "bill": hi["councilBill"]},
            "spread": round(hi["councilBill"] - lo["councilBill"], 2),
        },
    })

# integrity checks — fail the build rather than publish a bad sum
assert abs(sum(x["countyApportioned"]["population"] for x in unitaries) - LCC_BUDGET) < 0.5
assert abs(sum(x["countyApportioned"]["needs"] for x in unitaries) - LCC_BUDGET) < 0.5
assert sum(x["population"] for x in unitaries) == sum(A[n]["population"] for n in all_names)

model = {
    "$meta": {
        "generated": "by scripts/lgr_build.py from the canonical dataset in src/data/lgr/",
        "authoritiesNote": auth["$meta"]["budgetNote"],
        "bandDNote": auth["$meta"]["bandDNote"],
        "metricsNote": auth["$meta"]["metricsNote"],
    },
    "decision": {k: v for k, v in decision.items() if k != "unitaries"},
    "county": county,
    "baseline": baseline,
    "lcc": {
        "budget_m": LCC_BUDGET, "servedPop": LCC_SERVED,
        "perCapita": round(LCC_BUDGET * 1e6 / LCC_SERVED),
    },
    "totalPop": sum(A[n]["population"] for n in all_names),
    "unitaries": unitaries,
    "precedents": precedents,
    "cca": cca,
    "pensions": pensions,
    "government": government,
}

# ---- Costs vs savings scenarios ----
years = scen_in["$meta"]["years"]
weights = scen_in["$meta"]["costProfile"]["weights"]
offset = scen_in["$meta"]["governmentOffset_m"]
run_rate = scen_in["fullRunRate_m"]
scen_out = []
for s in scen_in["scenarios"]:
    rate = run_rate * s["realisation"]
    series = []
    cum = 0.0
    payback = None
    for i, y in enumerate(years):
        save = rate * s["ramp"][i]
        cost = s["transitionCost_m"] * weights[i]
        if s.get("equalPay_m") and i == s.get("equalPayYear"):
            cost += s["equalPay_m"]
        if i == 0:
            cost -= offset  # government support nets off early spend
        net = save - cost
        cum += net
        if payback is None and cum > 0:
            payback = y
        series.append({"year": y, "savings_m": round(save, 1), "costs_m": round(cost, 1), "cumNet_m": round(cum, 1)})
    scen_out.append({
        "key": s["key"], "label": s["label"], "basis": s["basis"],
        "transitionCost_m": s["transitionCost_m"], "realisationPct": round(s["realisation"] * 100),
        "annualRunRate_m": round(rate, 1), "equalPay_m": s.get("equalPay_m", 0),
        "payback": payback or "beyond 2037/38",
        "tenYearNet_m": round(cum, 1),
        "series": series,
    })
model["costsVsSavings"] = {"$meta": scen_in["$meta"], "fullRunRate_m": run_rate,
                           "runRateNote": scen_in["runRateNote"], "scenarios": scen_out}

(DATA / "model.json").write_text(json.dumps(model, indent=1))

public = dict(model)
public["$meta"] = dict(model["$meta"], licence="Open data — figures traceable to the cited public sources; verify against the primary source before formal use.",
                       site="https://tompickup.co.uk/lgr/")
public["authorities"] = A
pub_path = ROOT / "public" / "data" / "lgr-model.json"
pub_path.write_text(json.dumps(public, indent=1))

print(f"model.json: {len(unitaries)} unitaries; LCC served pop {LCC_SERVED:,}")
for x in unitaries:
    print(f"  {x['name']}: pop {x['population']:,} | combined £{x['combined']['population']}m"
          f" (needs-basis £{x['combined']['needs']}m) | Band D spread £{x['bandD']['spread']}")
