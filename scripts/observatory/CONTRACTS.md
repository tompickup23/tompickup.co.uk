# Lancashire Business Observatory - data contracts (v1)

The build pipeline (scripts/observatory/*.py) emits these JSON files into
public/data/. Pages under src/pages/lgr/business/ consume them client-side
(fetch), same pattern as /lgr/contracts. Every file carries:

```json
"$meta": {
  "generated": "2026-07-26T12:00:00Z",
  "methodologyVersion": "1.0",
  "sources": [{"name": "...", "url": "...", "retrieved": "YYYY-MM-DD", "licence": "..."}],
  "notes": ["..."]
}
```

House rules baked into the data: no em-dashes anywhere; derived scores are
labelled assessments with basis strings; "over £500 transparency data is not
total budget" caveat rides with any spend figure; every register fact carries
a retrieval date at file level.

Geography: every area object appears in BOTH views. `geo` field:
- current: 12 districts (type "district", parent "Lancashire CC") + 2 unitaries
- unitary2028: North Lancashire / West Lancashire / East Lancashire / South Lancashire
Slugs: kebab-case ("ribble-valley", "east-lancashire").

## biz-overview.json (index page: headline + comparator)

```json
{
  "$meta": {...},
  "areas": [
    {
      "slug": "burnley", "name": "Burnley", "geo": "current",
      "type": "district", "parent": "Lancashire CC",
      "unitary2028": "East Lancashire", "population": 96209,
      "companiesActive": 5187, "companiesPer1k": 53.9,
      "distressCleanPct": 9.2, "distressRawPct": 17.9,
      "shellCompaniesExcluded": 289,
      "birthsLatest": 610, "deathsLatest": 545, "birthsYear": 2024,
      "survival3yrPct": 37.8,
      "highGrowthOfficialPct": 3.9,
      "jobs": 41000, "medianPayWorkplace": 612.4,
      "solePropsRegistered": 890, "charities": 210, "cics": 51,
      "topSectors": [{"sic": "47", "label": "Retail", "count": 551}]
    }
  ],
  "unitaries": [ same shape, "geo": "unitary2028", "members": ["burnley", ...] ],
  "benchmarks": {
    "england": {"highGrowthOfficialPct": 4.9, "distressNote": "...", ...},
    "nwRing": [{"name": "Greater Manchester", "companiesActive": n, "highGrowthOfficialPct": n, "medianPayWorkplace": n, ...}]
  }
}
```

Nulls allowed anywhere data is missing; pages must render dashes for null.

## biz-areas.json (per-area drill page)

Keyed by slug (both geographies):
```json
{
  "$meta": {...},
  "burnley": {
    "sectors": [{"sic2": "56", "label": "Food and beverage", "live": 283,
                 "distressCleanPct": 21.2, "new3yr": 164}],
    "sizeBands": [{"band": "0-4", "count": n}],
    "legalStatus": {"companies": n, "soleProprietors": n, "partnerships": n, "nonProfit": n},
    "wholeEconomy": {
      "registeredCompanies": n, "registeredUnincorporated": n,
      "unregisteredModelled": n, "unregisteredNote": "Modelled: BPE NW ratio x APS. Not a count.",
      "selfEmploymentIncomeM": n, "selfEmploymentIncomeYear": "2023-24",
      "premisesFhrs": n, "premisesVoa": n, "premisesCqc": n
    },
    "vcse": {"charities": n, "charityIncomeM": n, "charityEmployees": n,
             "charityVolunteers": n, "cics": n, "mutuals": n,
             "schools": n, "academyTrusts": n},
    "addressClusters": [{"postcode": "BB12 8BS", "companies": 411,
      "dissolvedOrStrikingPct": 98,
      "note": "411 companies registered at this address; 98 percent dissolved or in strike-off. Excluded from clean rates."}],
    "gazelleCount": n, "innovationAwardsM": n
  }
}
```

`addressClusters.note` wording is legally constrained: observable facts only,
never purpose ("shell" banned).

## biz-watch.json (distress watch)

```json
{
  "$meta": {...},
  "summary": {"last90days": {"insolvencyNotices": n, "strikeOffProposals": n},
              "byMonth": [{"month": "2026-06", "notices": n, "strikeOffs": n}]},
  "notices": [{"date": "2026-07-14", "type": "Appointment of liquidator",
               "company": "X LTD", "crn": "01234567", "lad": "Burnley",
               "sic2": "56", "uri": "https://www.thegazette.co.uk/notice/..."}],
  "strikeOffs": [{"company": "...", "crn": "...", "lad": "...",
                  "status": "Active - Proposal to Strike off", "asAt": "2026-07-01"}]
}
```
Verbatim register facts + link. No commentary fields. Cap notices at ~500
most recent; strikeOffs at ~1000 with count of remainder in summary.

## biz-growth.json (gazelle view)

```json
{
  "$meta": {...},
  "officialBenchmark": [{"slug": "burnley", "highGrowthPct": 3.9, "year": 2024,
                         "englandPct": 4.9}],
  "candidates": [{
    "crn": "...", "name": "...", "lad": "Burnley", "sic2": "62",
    "unitary2028": "East Lancashire",
    "series": [{"periodEnd": "2023-03-31", "employees": 12},
               {"periodEnd": "2024-03-31", "employees": 19},
               {"periodEnd": "2025-03-31", "employees": 31}],
    "cagrPct": 60.8, "baseEmployees": 12,
    "flags": ["ons-definition", "young-company"],
    "momentum": {"iukGrant": true, "categoryUpgrade": false},
    "basis": "Average employee numbers as disclosed in filed accounts (s411 CA 2006), periods ending 2023-2025."
  }],
  "methodNote": "Observatory assessment from filed accounts. Not turnover-based. See method page."
}
```
Only complete >=3-period series with baseEmployees >= 10 for the ons-definition
flag; a separate "emerging" flag allowed for base 3-9 with strong growth,
clearly labelled. Outlier-cleaned (no 10x typos).

## biz-pound.json (Lancashire Pound)

```json
{
  "$meta": {...},
  "councils": [{
    "name": "Burnley BC", "yearRange": "2023/24-2025/26",
    "spendTotalM": 38.1,
    "coveragePct": 82.5,
    "tiers": {
      "rooted": {"valueM": n, "pct": n, "suppliers": n},
      "tradingExternal": {"valueM": n, "pct": n, "suppliers": n},
      "nonLocal": {"valueM": n, "pct": n, "suppliers": n},
      "unclassified": {"valueM": n, "pct": n, "suppliers": n}
    }
  }],
  "unitaries2028": [ same shape rolled up ],
  "topSuppliers": [{
    "name": "...", "crn": "...", "councils": ["Burnley BC"], "totalM": n,
    "tier": "tradingExternal",
    "tierBasis": "Trades from Lancashire premises; controlling parent X PLC is registered in London.",
    "sitesLancs": [{"lad": "Burnley", "evidence": "FHRS"}],
    "unitaries2028": ["East Lancashire"]
  }],
  "preston": {"note": "CLES/Preston comparator", "baselineLancsPct": 39,
              "latestLancsPct": 79.2, "sourceUrl": "..."}
}
```
tierBasis strings: register facts only. "Over £500" caveat in $meta.notes and
rendered on page.

## biz-innovation.json

```json
{
  "$meta": {...},
  "byArea": [{"slug": "burnley", "projects": n, "awardTotalM": n,
              "ktps": n, "topProductTypes": [{"type": "Grant", "n": n}]}],
  "byYear": [{"year": 2024, "projects": n, "awardM": n}],
  "projects": [{"participant": "...", "crn": "...", "lad": "Burnley",
                "year": 2024, "productType": "...", "awardK": n,
                "title": "...", "isLead": true}],
  "nwContext": {"nwProjects": n, "lancsShareVsPopulation": "..."}
}
```
Cap projects at the full Lancashire set (~700 rows, fine).

## Pages (all under src/pages/lgr/business/)

| Page | File | Data |
|---|---|---|
| Observatory home: headline band + area comparator with current/2028 toggle | index.astro | biz-overview.json |
| Area profile | [area].astro (static paths from crosswalk slugs incl 4 unitaries) | biz-areas.json + biz-overview.json |
| Distress watch | watch/index.astro | biz-watch.json |
| Growth engine | growth/index.astro | biz-growth.json |
| The Lancashire Pound | pound/index.astro | biz-pound.json |
| Innovation money | innovation/index.astro | biz-innovation.json |
| Method + sources + legal | method/index.astro | static content |

Shared furniture (every page): breadcrumb from /lgr/; footer block with the
disclaimer set (see LEGAL.md s7 in the clawd briefing pack); attribution lines
per source; "Report an error" mailto link (info@ style address:
tom.pickup@lancashire.gov.uk) + link to method page corrections section;
data-date labels rendered from $meta.
