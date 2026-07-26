#!/usr/bin/env python3
"""Assemble the six public/data/biz-*.json files from all processed layers.

Every number here traces to a processed file; nothing is invented. Missing
inputs degrade to nulls (pages render hyphens). House rules: no em-dashes,
derived figures framed as assessments, over-£500 caveat carried, dates on
everything.
"""
import gzip, json, re, sys
from collections import Counter, defaultdict
from pathlib import Path

def _crosswalk_path():
    from pathlib import Path as _P
    import os
    for c in [os.environ.get("OBS_CROSSWALK"),
              _P.home() / "clawd/briefings/lancashire-business-observatory/geo_crosswalk.json",
              _P.home() / "aidoge/briefings/lancashire-business-observatory/geo_crosswalk.json"]:
        if c and _P(c).exists():
            return _P(c)
    raise SystemExit("geo_crosswalk.json not found; set OBS_CROSSWALK")


sys.path.insert(0, str(Path(__file__).parent))
from resolve_suppliers import normalise, classify

ROOT = Path(__file__).resolve().parent.parent.parent   # repo root
PUB = ROOT / "public" / "data"
PROC = Path.home() / "observatory-data/processed"
VPS = Path.home() / "observatory-data/vps"
XW = json.loads((_crosswalk_path()).read_text())

from datetime import date as _date
GEN = _date.today().isoformat()

def load(p, default=None):
    p = Path(p)
    if not p.exists():
        print(f"  MISSING {p.name} (nulls will render)")
        return default
    return json.loads(p.read_text())

def slug(name):
    return re.sub(r"[^a-z0-9]+", "-", name.lower()).strip("-")

UNITARIES = ["North Lancashire", "West Lancashire", "East Lancashire", "South Lancashire"]
def uslug(u):
    return "west-lancashire-unitary" if u == "West Lancashire" else slug(u)

LADS = {v["name"]: v for v in XW["byAuthority"].values()}
members = defaultdict(list)
for n, v in LADS.items():
    members[v["newUnitary"]].append(n)

SIC2 = {
 "01": "Agriculture", "10": "Food manufacturing", "13": "Textiles",
 "16": "Wood products", "18": "Printing", "22": "Rubber and plastics",
 "23": "Non-metallic minerals", "24": "Metals", "25": "Fabricated metal products",
 "26": "Electronics", "27": "Electrical equipment", "28": "Machinery",
 "29": "Motor vehicles", "30": "Other transport equipment", "31": "Furniture",
 "32": "Other manufacturing", "33": "Repair of machinery",
 "35": "Energy", "38": "Waste and recycling", "41": "Construction of buildings",
 "42": "Civil engineering", "43": "Specialised construction",
 "45": "Motor trades", "46": "Wholesale", "47": "Retail",
 "49": "Land transport", "52": "Warehousing", "53": "Postal and courier",
 "55": "Accommodation", "56": "Food and beverage", "58": "Publishing",
 "59": "Film and media", "61": "Telecoms", "62": "IT and software",
 "63": "Information services", "64": "Finance and holding companies",
 "66": "Financial auxiliaries", "68": "Real estate", "69": "Legal and accounting",
 "70": "Head offices and consultancy", "71": "Architecture and engineering",
 "72": "Research", "73": "Advertising", "74": "Other professional",
 "75": "Veterinary", "77": "Rental and leasing", "78": "Employment services",
 "79": "Travel", "80": "Security", "81": "Buildings and landscape services",
 "82": "Office administration", "85": "Education", "86": "Human health",
 "87": "Residential care", "88": "Social work", "90": "Arts",
 "93": "Sport and recreation", "94": "Membership organisations",
 "95": "Repair of goods", "96": "Personal services",
}
def sic_label(s2):
    return SIC2.get(s2, f"SIC {s2}")

# ---------------------------------------------------------------- inputs ----
magg = load(PROC / "master_aggregates.json", {"byLad": {}})["byLad"]
clusters = load(PROC / "clusters.json", {"clusters": []})["clusters"]
growth = load(PROC / "growth.json", {"candidates": [], "counts": {}})
pound = load(PROC / "pound.json", {"resolved": {}, "councils": {}})
spend = load(PROC / "council_supplier_spend.json", {"bodies": {}})["bodies"]
uni_file = load(PROC / "supplier_universe.json", {"universe": []})
queue = load(PROC / "pound_review_queue.json", {"queue": []})["queue"]
nomis = load(PROC / "nomis_context.json", {})
onsdem = load(PROC / "ons_demography.json", {})
fhrs = load(PROC / "fhrs_lancs.json", {})
gazette = load(PROC / "gazette_lancs.json", {})
innovate = load(PROC / "innovate_lancs.json", {})
charities = load(PROC / "charities_lancs.json", {})
mutuals = load(PROC / "mutuals_lancs.json", {})
cqc = load(PROC / "cqc_lancs.json", {})
voa = load(PROC / "voa_lancs.json", {})
gias = load(PROC / "gias_lancs.json", {})

def rows_of(d, *keys):
    if not d:
        return []
    for k in keys:
        if isinstance(d, dict) and k in d:
            d = d[k]
    return d if isinstance(d, list) else []

SOURCES = [
 {"name": "Companies House Free Company Data + PSC + accounts bulk", "url": "https://download.companieshouse.gov.uk/", "retrieved": GEN, "licence": "Public register data"},
 {"name": "ONS / NOMIS (business counts, demography, BRES, ASHE)", "url": "https://www.nomisweb.co.uk/", "retrieved": GEN, "licence": "OGL v3"},
 {"name": "The Gazette", "url": "https://www.thegazette.co.uk/", "retrieved": GEN, "licence": "OGL v3"},
 {"name": "FSA Food Hygiene Rating Scheme", "url": "https://ratings.food.gov.uk/", "retrieved": GEN, "licence": "OGL v3, FSA attribution"},
 {"name": "Innovate UK funded projects", "url": "https://www.ukri.org/publications/innovate-uk-funded-projects-since-2004/", "retrieved": GEN, "licence": "OGL v3"},
 {"name": "Charity Commission register extract", "url": "https://register-of-charities.charitycommission.gov.uk/", "retrieved": GEN, "licence": "OGL v3"},
 {"name": "CQC care directory", "url": "https://www.cqc.org.uk/", "retrieved": GEN, "licence": "OGL v3"},
 {"name": "Council transparency spending (17 Lancashire bodies)", "url": "https://tompickup.co.uk/lgr/", "retrieved": GEN, "licence": "OGL v3 per council"},
 {"name": "ONSPD", "url": "https://geoportal.statistics.gov.uk/", "retrieved": GEN, "licence": "OGL; contains OS and Royal Mail data, Crown copyright"},
]
def meta(extra_notes=()):
    return {"generated": GEN + "T18:00:00Z", "methodologyVersion": "1.0",
            "sources": SOURCES,
            "notes": ["Transparency spending covers payments over £500 and is not a complete account of any body's budget.",
                      "Derived indicators are Observatory assessments from public data; see the method page."] + list(extra_notes)}

# ------------------------------------------------- per-LAD context joins ----
nomis_areas = (nomis or {}).get("areas", {})
nomis_by_name = {a["name"]: a for a in nomis_areas.values()}
def _adapt_ons(d):
    """Adapt the fetcher's counts-shaped ons_demography (areas keyed by code,
    per-year dicts) to flat per-name latest values. Rates derived from counts;
    survival = latest cohort with a 3-year percentage."""
    out = {}
    for a in (d or {}).get("areas", {}).values():
        def latest(dd):
            ys = sorted(int(y) for y in (dd or {}) if str(y).isdigit())
            return (ys[-1], (dd or {}).get(str(ys[-1]))) if ys else (None, None)
        by, births = latest(a.get("births"))
        _, deaths = latest(a.get("deaths"))
        _, active = latest(a.get("active"))
        hy, hg = latest(a.get("high_growth_pct"))
        surv, sy = None, None
        for cohort in sorted((a.get("survival") or {}), reverse=True):
            row = a["survival"][cohort]
            p = next((v for k, v in row.items() if k.startswith("3-year") and "cent" in k), None)
            if p is not None:
                surv, sy = round(p, 1), int(cohort)
                break
        out[a["name"]] = {
            "birthRate": round(100 * births / active, 1) if births and active else None,
            "birthRateYear": by, "deathRateYear": by,
            "deathRate": round(100 * deaths / active, 1) if deaths and active else None,
            "births": births, "deaths": deaths,
            "highGrowthPct": hg, "highGrowthYear": hy,
            "survival3yr": surv, "survival3yrYear": sy,
        }
    return out

ons_lad = _adapt_ons(onsdem) if (onsdem or {}).get("areas") else \
          (onsdem or {}).get("byLad", onsdem or {})
if "England" in ons_lad and not (onsdem or {}).get("england"):
    onsdem = dict(onsdem or {})
    onsdem["england"] = ons_lad["England"]
CODE_TO_NAME = {v["ons"]: v["name"] for v in XW["byAuthority"].values()}

def ctx(lad, *path):
    a = nomis_by_name.get(lad) or {}
    key = path[0] if path else None
    if key == "jobs":
        return a.get("bres_employee_jobs_total")
    if key == "medianPayWorkplace":
        return a.get("ashe_workplace_median_weekly_ft")
    if key == "enterprises":
        return (a.get("enterprises_by_legal_status") or {}).get("private_sector_total")
    if key == "legalStatus":
        ls = a.get("enterprises_by_legal_status") or {}
        m = {"companies": ls.get("company"),
             "soleProprietors": ls.get("sole_proprietor"),
             "partnerships": ls.get("partnership"),
             "nonProfit": ls.get("non_profit_or_mutual")}
        if len(path) == 2:
            return m.get(path[1])
        return m if ls else None
    if key == "sizeBands":
        sb = a.get("enterprises_by_section_sizeband") or {}
        lab = {"micro_0_9": "0-9 employees", "small_10_49": "10-49",
               "medium_50_249": "50-249", "large_250plus": "250 plus"}
        out = [{"band": lab[k], "count": sum(v.values())}
               for k, v in sb.items() if k in lab]
        return out or None
    return None

def ons_val(lad, *path):
    d = ons_lad.get(lad) or {}
    for k in path:
        d = d.get(k) if isinstance(d, dict) else None
        if d is None:
            return None
    return d

pc_cache = load(PROC / "_postcode_lad_cache.json", {}) or {}
def lad_of(row):
    """Resolve a row's LAD NAME from lad code/name or postcode."""
    lad = row.get("lad") or row.get("la") or row.get("la_name")
    if lad in CODE_TO_NAME:
        return CODE_TO_NAME[lad]
    if lad in LADS:
        return lad
    pc = (row.get("postcode") or "").replace(" ", "").upper()
    hit = pc_cache.get(pc)
    return hit["name"] if hit else None

# FHRS / CQC counts + name sets for trading evidence
fhrs_by_lad, fhrs_names = Counter(), set()
for e in rows_of(fhrs, "establishments"):
    fhrs_by_lad[lad_of(e)] += 1
    if e.get("name"):
        fhrs_names.add(normalise(e["name"]))
cqc_by_lad, cqc_names, cqc_lads_by_name = Counter(), set(), defaultdict(set)
for e in rows_of(cqc, "locations"):
    cqc_by_lad[lad_of(e)] += 1
    for k in ("provider_name", "location_name"):
        if e.get(k):
            cqc_names.add(normalise(e[k]))
            cqc_lads_by_name[normalise(e[k])].add(lad_of(e))
voa_by_lad = Counter()
voa_names = set()
for r in rows_of(voa, "entries"):
    voa_by_lad[lad_of(r)] += 1

# charities per LAD
char_rows = rows_of(charities, "charities")
char_by_lad = defaultdict(lambda: {"n": 0, "income": 0.0, "emp": 0, "vol": 0})
for c in char_rows:
    lad = lad_of(c)
    if lad in LADS:
        a = char_by_lad[lad]
        a["n"] += 1
        a["income"] += c.get("latest_income") or 0
        a["emp"] += c.get("employees") or 0
        a["vol"] += c.get("volunteers") or 0
mut_by_lad = Counter(lad_of(m) for m in rows_of(mutuals, "societies")
                     if m.get("status") != "Deregistered" and lad_of(m) in LADS)
gias_rows = rows_of(gias, "establishments")
gias_by_lad = Counter(lad_of(g) for g in gias_rows if lad_of(g) in LADS)

# innovation per LAD
inn_rows = rows_of(innovate, "projects")
inn_by_lad = defaultdict(lambda: {"projects": 0, "award": 0.0, "ktps": 0,
                                  "types": Counter()})
inn_by_year = defaultdict(lambda: {"projects": 0, "award": 0.0})
for p in inn_rows:
    lad = CODE_TO_NAME.get(p.get("lad"), p.get("lad"))
    p["lad"] = lad
    if lad not in LADS:
        continue
    a = inn_by_lad[lad]
    a["projects"] += 1
    a["award"] += p.get("award_offered") or 0
    a["types"][p.get("product_type") or "Grant"] += 1
    if "Knowledge Transfer" in (p.get("product_type") or ""):
        a["ktps"] += 1
    yr = p.get("competition_year")
    if isinstance(yr, str):
        m = re.search(r"(20\d\d)", yr)
        yr = int(m.group(1)) if m else None
    if yr:
        inn_by_year[yr]["projects"] += 1
        inn_by_year[yr]["award"] += p.get("award_offered") or 0

# gazelle counts per LAD (ons-definition only)
gaz_by_lad = Counter(c["lad"] for c in growth["candidates"]
                     if "ons-definition" in c["flags"])

# strike-offs from master
strikeoffs, so_by_lad = [], Counter()
with gzip.open(PROC / "master.jsonl.gz", "rt") as f:
    for line in f:
        r = json.loads(line)
        if r["status"] == "Active - Proposal to Strike off" and not r["cluster"]:
            so_by_lad[r["lad"]] += 1
            if len(strikeoffs) < 1000:
                strikeoffs.append({"company": r["name"], "crn": r["crn"],
                                   "lad": r["lad"], "status": r["status"],
                                   "asAt": "2026-07-01"})

# ------------------------------------------------------------ overview ------
def area_row(lad):
    a = magg.get(lad, {})
    pop = LADS[lad]["population"]
    comp = a.get("cleanCompanies")
    reg_unincorp = ctx(lad, "legalStatus", "soleProprietors")
    return {
        "slug": slug(lad), "name": lad, "geo": "current",
        "type": LADS[lad]["currentType"], "parent": LADS[lad]["currentParent"],
        "unitary2028": LADS[lad]["newUnitary"], "population": pop,
        "companiesActive": comp,
        "companiesPer1k": round(comp / pop * 1000, 1) if comp else None,
        "distressCleanPct": a.get("distressCleanPct"),
        "distressRawPct": a.get("distressRawPct"),
        "shellCompaniesExcluded": a.get("clusterExcluded"),
        "birthRatePct": round(ons_val(lad, "birthRate"), 1) if ons_val(lad, "birthRate") is not None else None,
        "deathRatePct": round(ons_val(lad, "deathRate"), 1) if ons_val(lad, "deathRate") is not None else None,
        "ratesYear": ons_val(lad, "birthRateYear"),
        "survival3yrPct": round(ons_val(lad, "survival3yr"), 1) if ons_val(lad, "survival3yr") is not None else None,
        "highGrowthOfficialPct": round(ons_val(lad, "highGrowthPct"), 1) if ons_val(lad, "highGrowthPct") is not None else None,
        "jobs": ctx(lad, "jobs"), "medianPayWorkplace": ctx(lad, "medianPayWorkplace"),
        "solePropsRegistered": reg_unincorp,
        "charities": char_by_lad[lad]["n"] or None,
        "cics": a.get("cics"),
        "topSectors": [{"sic": s["sic2"], "label": sic_label(s["sic2"]),
                        "count": s["count"]} for s in (a.get("topSic2") or [])[:3]],
    }

areas = [area_row(l) for l in sorted(LADS)]
def roll(us):
    rows = [r for r in areas if r["unitary2028"] == us]
    def s(k):
        vals = [r[k] for r in rows if r[k] is not None]
        return sum(vals) if vals else None
    def w(k):  # population-weighted mean
        vals = [(r[k], r["population"]) for r in rows if r[k] is not None]
        return round(sum(v * p for v, p in vals) / sum(p for _, p in vals), 1) if vals else None
    comp = s("companiesActive")
    pop = s("population")
    return {"slug": uslug(us), "name": us, "geo": "unitary2028",
            "members": [r["slug"] for r in rows], "population": pop,
            "companiesActive": comp,
            "companiesPer1k": round(comp / pop * 1000, 1) if comp and pop else None,
            "distressCleanPct": w("distressCleanPct"),
            "distressRawPct": w("distressRawPct"),
            "shellCompaniesExcluded": s("shellCompaniesExcluded"),
            "birthRatePct": w("birthRatePct"), "deathRatePct": w("deathRatePct"),
            "ratesYear": rows[0]["ratesYear"] if rows else None,
            "survival3yrPct": w("survival3yrPct"),
            "highGrowthOfficialPct": w("highGrowthOfficialPct"),
            "jobs": s("jobs"), "medianPayWorkplace": w("medianPayWorkplace"),
            "solePropsRegistered": s("solePropsRegistered"),
            "charities": s("charities"), "cics": s("cics"),
            "topSectors": []}

def ring_row(a):
    ls = a.get("enterprises_by_legal_status") or {}
    return {"name": a["name"],
            "companiesActive": ls.get("private_sector_total"),
            "jobs": a.get("bres_employee_jobs_total"),
            "medianPayWorkplace": a.get("ashe_workplace_median_weekly_ft"),
            "highGrowthOfficialPct": (ons_lad.get(a["name"]) or {}).get("highGrowthPct")}

ring = [ring_row(a) for a in nomis_areas.values()
        if a["name"] not in LADS and a["name"] != "England"]
eng = next((a for a in nomis_areas.values() if a["name"] == "England"), None)
overview = {"$meta": meta(),
            "areas": areas,
            "unitaries": [roll(u) for u in UNITARIES],
            "benchmarks": {"england": (ring_row(eng) if eng else {})
                           | {"highGrowthOfficialPct":
                              round(((onsdem or {}).get("england") or {}).get("highGrowthPct", 4.9), 1),
                              "birthRatePct": round(((onsdem or {}).get("england") or {}).get("birthRate", 0), 1) or None,
                              "deathRatePct": round(((onsdem or {}).get("england") or {}).get("deathRate", 0), 1) or None,
                              "survival3yrPct": round(((onsdem or {}).get("england") or {}).get("survival3yr", 0), 1) or None},
                           "nwRing": ring}}
(PUB / "biz-overview.json").write_text(json.dumps(overview))
print("biz-overview.json written")

# ------------------------------------------------------------ areas ---------
BPE_UNREG_PER_REG = 54 / 46      # BPE 2025: 46% registered, 54% unregistered
spi = load(PROC / "bpe_spi.json", {})
spi_rows = (spi or {}).get("spi_raw_rows", {})
def spi_income_m(lad):
    """SPI Table 3.14 rows run in triples per source: (count thousands, mean,
    median) for self-employment, employment, pensions, then total count.
    Self-employment total = count x mean. Sample-based; coarse for small
    districts (counts rounded to the nearest thousand)."""
    r = spi_rows.get(lad)
    if not r:
        return None
    nums = r.get("numbers") or []
    if len(nums) < 2 or nums[0] <= 0 or nums[1] < 1000:
        return None
    return round(float(nums[0]) * float(nums[1]) / 1000, 0)

areas_detail = {"$meta": meta(["Unregistered business estimates are MODELLED "
                              "from the national BPE registered share and are "
                              "not counts."])}
for lad in sorted(LADS):
    a = magg.get(lad, {})
    ent = ctx(lad, "enterprises") or a.get("cleanCompanies")
    reg_unincorp = (ctx(lad, "legalStatus", "soleProprietors") or 0) + \
                   (ctx(lad, "legalStatus", "partnerships") or 0)
    registered_total = ent if ctx(lad, "enterprises") else None
    unreg = round(registered_total * BPE_UNREG_PER_REG) if registered_total else None
    areas_detail[slug(lad)] = {
        "sectors": [{"sic2": s["sic2"], "label": sic_label(s["sic2"]),
                     "live": s["count"], "new3yr": s["new3yr"],
                     "distressCleanPct": None}
                    for s in (a.get("topSic2") or [])],
        "sizeBands": ctx(lad, "sizeBands") or [],
        "legalStatus": ctx(lad, "legalStatus") or {},
        "wholeEconomy": {
            "registeredCompanies": a.get("cleanCompanies"),
            "registeredUnincorporated": reg_unincorp or None,
            "unregisteredModelled": unreg,
            "unregisteredNote": "Modelled: national BPE registered share (46 "
                                "percent) applied to IDBR-registered count. "
                                "Not a count.",
            "selfEmploymentIncomeM": spi_income_m(lad),
            "selfEmploymentIncomeYear": "2023-24",
            "premisesFhrs": fhrs_by_lad.get(lad) or None,
            "premisesVoa": voa_by_lad.get(lad),
            "premisesCqc": cqc_by_lad.get(lad) or None,
        },
        "vcse": {"charities": char_by_lad[lad]["n"] or None,
                 "charityIncomeM": round(char_by_lad[lad]["income"] / 1e6, 1)
                                   if char_by_lad[lad]["income"] else None,
                 "charityEmployees": char_by_lad[lad]["emp"] or None,
                 "charityVolunteers": char_by_lad[lad]["vol"] or None,
                 "cics": a.get("cics"), "mutuals": mut_by_lad.get(lad) or None,
                 "schools": gias_by_lad.get(lad) or None, "academyTrusts": None},
        "addressClusters": [c for c in clusters if c["lad"] == lad],
        "gazelleCount": gaz_by_lad.get(lad, 0),
        "innovationAwardsM": round(inn_by_lad[lad]["award"] / 1e6, 2)
                             if lad in inn_by_lad else None,
    }
# unitary rollups for area pages
for us in UNITARIES:
    mem = [slug(m) for m in members[us]]
    merged = {"sectors": [], "sizeBands": [], "legalStatus": {},
              "wholeEconomy": {}, "vcse": {}, "addressClusters": [],
              "gazelleCount": 0, "innovationAwardsM": 0.0, "members": mem}
    sec = Counter(); new3 = Counter()
    for m in members[us]:
        d = areas_detail[slug(m)]
        for s in d["sectors"]:
            sec[s["sic2"]] += s["live"]; new3[s["sic2"]] += s["new3yr"]
        merged["addressClusters"] += d["addressClusters"]
        merged["gazelleCount"] += d["gazelleCount"]
        merged["innovationAwardsM"] += d["innovationAwardsM"] or 0
        for k, v in d["wholeEconomy"].items():
            if isinstance(v, (int, float)):
                merged["wholeEconomy"][k] = (merged["wholeEconomy"].get(k) or 0) + v
        for k, v in d["vcse"].items():
            if isinstance(v, (int, float)):
                merged["vcse"][k] = (merged["vcse"].get(k) or 0) + v
    merged["wholeEconomy"]["unregisteredNote"] = areas_detail[mem[0]]["wholeEconomy"]["unregisteredNote"]
    merged["sectors"] = [{"sic2": s, "label": sic_label(s), "live": c,
                          "new3yr": new3[s], "distressCleanPct": None}
                         for s, c in sec.most_common(15)]
    merged["innovationAwardsM"] = round(merged["innovationAwardsM"], 2)
    areas_detail[uslug(us)] = merged
(PUB / "biz-areas.json").write_text(json.dumps(areas_detail))
print("biz-areas.json written")

# ------------------------------------------------------------ watch ---------
# Primary feed: Gazette corporate insolvency notices (fair and accurate
# extracts, linked). Joined to the register for sector; geo recall is partial
# (notices are indexed on one address), stated in $meta.
master_by_crn = {}
with gzip.open(PROC / "master.jsonl.gz", "rt") as f:
    for line in f:
        r = json.loads(line)
        master_by_crn[r["crn"]] = r

def nice_type(t):
    s = re.sub(r"(?<!^)(?=[A-Z])", " ", t or "").replace("Notice", "").strip()
    return s or "Insolvency notice"

seen_n = set()
notices = []
for n in sorted(rows_of(gazette, "notices"),
                key=lambda x: x.get("date") or "", reverse=True):
    key = (n.get("company_number"), n.get("type"), n.get("date"))
    if key in seen_n:
        continue
    seen_n.add(key)
    m = master_by_crn.get(n.get("company_number") or "")
    notices.append({"date": n.get("date"), "type": nice_type(n.get("type")),
                    "company": (m["name"] if m else n.get("company_name")),
                    "crn": n.get("company_number"),
                    "lad": (m["lad"] if m else CODE_TO_NAME.get(n.get("lad"))),
                    "sic2": m["sic2"] if m else None,
                    "uri": n.get("uri")})
notices = notices[:500]
by_month = Counter()
for n in notices:
    if n["date"]:
        by_month[n["date"][:7]] += 1
watch = {"$meta": meta(["Notices are fair and accurate extracts from The "
                        "Gazette; strike-off proposals are the register "
                        "status as at the snapshot date."]),
         "summary": {"last90days": {"insolvencyNotices": len(notices),
                                    "strikeOffProposals": sum(so_by_lad.values())},
                     "byMonth": [{"month": m, "notices": c, "strikeOffs": None}
                                 for m, c in sorted(by_month.items())]},
         "notices": notices, "strikeOffs": strikeoffs,
         "strikeOffTotal": sum(so_by_lad.values()),
         "strikeOffByLad": dict(so_by_lad)}
(PUB / "biz-watch.json").write_text(json.dumps(watch))
print("biz-watch.json written")

# ------------------------------------------------------------ growth --------
bench = []
for lad in sorted(LADS):
    bench.append({"slug": slug(lad), "highGrowthPct": round(ons_val(lad, "highGrowthPct"), 1) if ons_val(lad, "highGrowthPct") is not None else None,
                  "year": ons_val(lad, "highGrowthYear") or 2024,
                  "englandPct": round(((onsdem or {}).get("england") or {}).get("highGrowthPct", 4.9), 1)})
gcands = []
for c in growth["candidates"]:
    gcands.append({"crn": c["crn"], "name": c["name"], "lad": c["lad"],
                   "sic2": c["sic2"], "unitary2028": c["unitary2028"],
                   "series": c["series"], "cagrPct": c["cagrPct"],
                   "baseEmployees": c["baseEmployees"], "flags": c["flags"],
                   "momentum": {}, "basis": c["basis"]})
growth_out = {"$meta": meta(), "officialBenchmark": bench, "candidates": gcands,
              "methodNote": "Observatory assessment from employee numbers in "
                            "filed accounts. Turnover is not public for most "
                            "small companies. See method page."}
(PUB / "biz-growth.json").write_text(json.dumps(growth_out))
print(f"biz-growth.json written ({len(gcands)} candidates)")

# ------------------------------------------------------------ pound ---------
DISPLAY = {"blackburn": "Blackburn with Darwen BC", "blackpool": "Blackpool BC",
           "burnley": "Burnley BC", "chorley": "Chorley BC", "fylde": "Fylde BC",
           "hyndburn": "Hyndburn BC", "lancashire_cc": "Lancashire CC",
           "lancashire_fire": "Lancashire Fire and Rescue",
           "lancashire_pcc": "Lancashire PCC", "lancaster": "Lancaster CC",
           "pendle": "Pendle BC", "preston": "Preston CC",
           "ribble_valley": "Ribble Valley BC", "rossendale": "Rossendale BC",
           "south_ribble": "South Ribble BC",
           "west_lancashire": "West Lancashire BC", "wyre": "Wyre BC"}
BODY_LAD = {"blackburn": "Blackburn with Darwen", "blackpool": "Blackpool",
            "burnley": "Burnley", "chorley": "Chorley", "fylde": "Fylde",
            "hyndburn": "Hyndburn", "lancaster": "Lancaster", "pendle": "Pendle",
            "preston": "Preston", "ribble_valley": "Ribble Valley",
            "rossendale": "Rossendale", "south_ribble": "South Ribble",
            "west_lancashire": "West Lancashire", "wyre": "Wyre"}

resolved = pound["resolved"]

# stage 4: trading evidence upgrades nonLocal -> tradingExternal
evidence = {}
for key, r in resolved.items():
    if r["tier"] == "nonLocal" and (key in fhrs_names or key in cqc_names):
        r["tier"] = "tradingExternal"
        src = "CQC-registered care locations" if key in cqc_names else "FHRS-registered premises"
        r["evidence"] = src
        evidence[key] = src
print(f"stage-4 evidence upgrades: {len(evidence)}")

def council_row(body):
    d = spend[body]
    tiers = defaultdict(float)
    denom = 0.0
    for name, amt in d["suppliers"].items():
        c = classify(name)
        if c == "councilOwned":
            tiers["councilOwned"] += amt
            denom += amt
            continue
        if c != "supplier":
            continue
        denom += amt
        tiers[resolved.get(normalise(name), {"tier": "unclassified"})["tier"]] += amt
    tiers["unclassified"] += d["tail"]["value"]
    denom += d["tail"]["value"]
    def t(k):
        v = tiers.get(k, 0.0)
        return {"valueM": round(v / 1e6, 1),
                "pct": round(100 * v / denom, 1) if denom else None}
    matched = denom - tiers.get("unclassified", 0)
    return {"body": body, "name": DISPLAY[body], "yearRange": "-".join(
                [d["years"][0], d["years"][-1]]) if d["years"] else None,
            "spendTotalM": round(denom / 1e6, 1),
            "coveragePct": round(100 * matched / denom, 1) if denom else None,
            "tiers": {"rooted": t("rooted"),
                      "tradingExternal": t("tradingExternal"),
                      "nonLocal": t("nonLocal"),
                      "councilOwned": t("councilOwned"),
                      "unclassified": t("unclassified")}}

council_rows = [council_row(b) for b in DISPLAY]
by_unitary = defaultdict(list)
for b, lad in BODY_LAD.items():
    by_unitary[LADS[lad]["newUnitary"]].append(b)
unit_rows = []
for us in UNITARIES:
    bs = by_unitary[us]
    agg = defaultdict(float); denom = 0.0
    for b in bs:
        r = next(c for c in council_rows if c["body"] == b)
        for k, v in r["tiers"].items():
            agg[k] += v["valueM"]
        denom += r["spendTotalM"]
    unit_rows.append({"slug": uslug(us), "name": us,
                      "members": [DISPLAY[b] for b in bs],
                      "spendTotalM": round(denom, 1),
                      "note": "Constituent district and unitary councils only. "
                              "Lancashire CC, Fire and PCC spend is county-wide "
                              "and shown separately.",
                      "tiers": {k: {"valueM": round(v, 1),
                                    "pct": round(100 * v / denom, 1) if denom else None}
                                for k, v in agg.items()}})

# top suppliers: from hand-reviewed queue, top 30 by value
top_sup = []
for q in queue[:30]:
    key = normalise(q["name"])
    r = resolved.get(key, {})
    tier = r.get("tier", q.get("tier", "unclassified"))
    chain = r.get("chain") or q.get("chain") or []
    basis_bits = []
    if r.get("lad"):
        basis_bits.append(f"Registered in {r['lad']}.")
    elif r.get("crn"):
        basis_bits.append("Registered outside Lancashire.")
    if chain:
        c0 = chain[-1]
        where = c0.get("where", "")
        if where and c0.get("name"):
            basis_bits.append(f"Ownership chain ends at {c0['name']} ({where}).")
    if r.get("evidence"):
        basis_bits.append(f"Lancashire trading evidence: {r['evidence']}.")
    if tier == "unclassified":
        basis_bits = ["Could not be matched to a single register entry; "
                      "not classified."]
    top_sup.append({"name": q["name"].title(), "crn": r.get("crn"),
                    "councils": [DISPLAY.get(b, b) for b in q["bodies"]],
                    "totalM": round(q["total"] / 1e6, 1), "tier": tier,
                    "tierBasis": " ".join(basis_bits),
                    "sitesLancs": sorted(cqc_lads_by_name.get(key, set()) - {None})
                                  if key in cqc_names else [],
                    "unitaries2028": []})

pound_out = {"$meta": meta(["Tier classification is mechanical from public "
                            "registers (method page); trading names that could "
                            "not be matched are shown as unclassified, never "
                            "guessed."]),
             "councils": council_rows, "unitaries2028": unit_rows,
             "topSuppliers": top_sup,
             "preston": {"note": "CLES and Preston City Council anchor analysis: "
                                 "spend retained in Lancashire rose from 39 "
                                 "percent (2012/13) to 79.2 percent (2016/17).",
                         "baselineLancsPct": 39, "latestLancsPct": 79.2,
                         "sourceUrl": "https://www.preston.gov.uk/article/1791/The-definitive-guide-to-the-Preston-model"}}
(PUB / "biz-pound.json").write_text(json.dumps(pound_out))
print("biz-pound.json written")

# ------------------------------------------------------------ innovation ----
inn_area = []
for lad in sorted(LADS):
    a = inn_by_lad.get(lad)
    inn_area.append({"slug": slug(lad), "projects": a["projects"] if a else 0,
                     "awardTotalM": round(a["award"] / 1e6, 2) if a else 0,
                     "ktps": a["ktps"] if a else 0,
                     "topProductTypes": [{"type": t, "n": n} for t, n in
                                         a["types"].most_common(3)] if a else []})
for us in UNITARIES:
    mem = members[us]
    inn_area.append({"slug": uslug(us),
                     "projects": sum(r["projects"] for r in inn_area
                                     if r["slug"] in [slug(m) for m in mem]),
                     "awardTotalM": round(sum(r["awardTotalM"] for r in inn_area
                                              if r["slug"] in [slug(m) for m in mem]), 2),
                     "ktps": sum(r["ktps"] for r in inn_area
                                 if r["slug"] in [slug(m) for m in mem]),
                     "topProductTypes": []})
proj_rows = [{"participant": p.get("participant"), "crn": p.get("crn"),
              "lad": p.get("lad"), "year": p.get("competition_year"),
              "productType": p.get("product_type"),
              "awardK": round((p.get("award_offered") or 0) / 1e3, 1),
              "title": p.get("project_title"), "isLead": p.get("is_lead")}
             for p in inn_rows if p.get("lad") in LADS]
inn_out = {"$meta": meta(), "byArea": inn_area,
           "byYear": [{"year": y, "projects": v["projects"],
                       "awardM": round(v["award"] / 1e6, 2)}
                      for y, v in sorted(inn_by_year.items())],
           "projects": proj_rows,
           "nwContext": (innovate or {}).get("nwContext", {})}
(PUB / "biz-innovation.json").write_text(json.dumps(inn_out))
print(f"biz-innovation.json written ({len(proj_rows)} projects)")
print("DONE")
