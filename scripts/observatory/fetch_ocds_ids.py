#!/usr/bin/env python3
"""Harvest supplier company numbers for the 17 bodies' Contracts Finder notices.

Self-contained: discovers notice ids with the Contracts Finder V2 search API
(keyword queries per body, filtered to the body's organisation names), then
reads each notice's award records from get_published_notice. Awards whose
referenceType is COMPANIES_HOUSE carry the supplier company number, the same
value the OCDS release publishes as a GB-COH identifier (verified against the
per-notice OCDS endpoint, 17 Aug 2026). Output: name to company number
evidence map used by build_pound.py as a high-confidence resolution source.

Earlier editions read notice ids from the clawd repo's procurement_finder.json
files, a path that only exists on the Mac. On vps-main the fetcher found no
notices and silently wrote an empty map (DATA-INTEGRITY 11.2). This edition
talks to the API directly on every machine and exits non-zero rather than
writing a thin or empty map, leaving the previous output untouched.

Resumable: per-notice results cached in raw/ocds_cache.jsonl. New cache keys
are bare notice uuids; legacy keys carry a numeric suffix (uuid-NNNNNN) and
are still recognised.
"""
import datetime
import json
import re
import sys
import time
from pathlib import Path
import requests

RAW = Path.home() / "observatory-data/raw"
PROC = Path.home() / "observatory-data/processed"
CACHE = RAW / "ocds_cache.jsonl"
OUT = PROC / "ocds_supplier_ids.json"
UA = {"User-Agent": "Mozilla/5.0 (observatory data fetch; tompickup.co.uk)"}
SEARCH_URL = "https://www.contractsfinder.service.gov.uk/api/rest/2/search_notices/json"
NOTICE_URL = "https://www.contractsfinder.service.gov.uk/api/rest/2/get_published_notice/json/"
PUBLISHED_FROM = "2014-01-01T00:00:00Z"

# Failure gates. A healthy run sees thousands of notices across the 17 bodies
# (4,977 unique ids on the last known-good Mac run) and resolves hundreds of
# names. Below these floors something is broken (API change, network, an
# ignored filter), so exit non-zero and leave the previous output in place.
# An empty map must never look like an honest zero.
MIN_NOTICES = 1000
MIN_MAPPINGS = 50
MAX_FETCH_FAILURE_RATE = 0.10

# Search terms are quoted keyword queries; match names are lowercase
# substrings checked against organisationName, because keyword search also
# hits notices that merely mention a body in the description.
BODIES = {
    "blackburn": (['"Blackburn with Darwen"'],
                  ["blackburn with darwen"]),
    "blackpool": (['"Blackpool Council"', '"Blackpool Borough Council"'],
                  ["blackpool council", "blackpool borough council", "blackpool bc"]),
    "burnley": (['"Burnley Borough Council"'],
                ["burnley borough council", "burnley bc", "burnley council"]),
    "chorley": (['"Chorley Borough Council"', '"Chorley Council"'],
                ["chorley borough council", "chorley bc", "chorley council"]),
    "fylde": (['"Fylde Borough Council"', '"Fylde Council"'],
              ["fylde borough council", "fylde council", "fylde bc"]),
    "hyndburn": (['"Hyndburn Borough Council"', '"Borough of Hyndburn"'],
                 ["hyndburn borough council", "borough of hyndburn", "hyndburn bc", "hyndburn council"]),
    "lancashire_cc": (['"Lancashire County Council"'],
                      ["lancashire county council", "lancashire cc"]),
    "lancashire_fire": (['"Lancashire Combined Fire Authority"', '"Lancashire Fire and Rescue"'],
                        ["lancashire combined fire", "lancashire fire"]),
    "lancashire_pcc": (['"Crime Commissioner for Lancashire"', '"Lancashire Constabulary"'],
                       ["crime commissioner for lancashire", "lancashire constabulary"]),
    "lancaster": (['"Lancaster City Council"'],
                  ["lancaster city council", "lancaster council", "city of lancaster"]),
    "pendle": (['"Pendle Borough Council"', '"Borough of Pendle"'],
               ["pendle borough council", "borough of pendle", "pendle bc", "pendle council"]),
    "preston": (['"Preston City Council"'],
                ["preston city council", "preston council"]),
    "ribble_valley": (['"Ribble Valley Borough Council"'],
                      ["ribble valley"]),
    "rossendale": (['"Rossendale Borough Council"', '"Borough of Rossendale"'],
                   ["rossendale borough council", "borough of rossendale", "rossendale bc", "rossendale council"]),
    "south_ribble": (['"South Ribble Borough Council"'],
                     ["south ribble borough council", "south ribble bc", "south ribble council"]),
    "west_lancashire": (['"West Lancashire Borough Council"'],
                        ["west lancashire borough council", "west lancashire bc",
                         "west lancashire council", "west lancs"]),
    "wyre": (['"Wyre Council"', '"Wyre Borough Council"'],
             ["wyre council", "wyre borough council", "wyre bc"]),
}

_SUFFIXED = re.compile(
    r"^([0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12})-\d+$")


def bare_id(nid):
    """Strip the legacy numeric suffix (uuid-NNNNNN) down to the bare uuid."""
    m = _SUFFIXED.match(nid or "")
    return m.group(1).lower() if m else (nid or "").lower()


# The search API caps a response at 1000 items (maxHits) and silently
# ignores its "from" and "page" parameters: every offset returns the same
# top slice (verified 17 Aug 2026, the same silent-filter failure family as
# the Gazette category-code parameter, DATA-INTEGRITY 9.5). The only honest
# pagination is to shrink the published window until a window fits in one
# response.
MAX_HITS = 1000


def _search_window(term, dfrom, dto):
    """One search request. Returns (hitCount, items). Raises on persistent
    failure: a partial discovery must not masquerade as a complete one."""
    payload = {"searchCriteria": {"keyword": term,
                                  "publishedFrom": dfrom,
                                  "publishedTo": dto},
               "size": MAX_HITS}
    for attempt in range(5):
        try:
            r = requests.post(SEARCH_URL, json=payload, headers=UA, timeout=120)
            if r.status_code in (403, 429):
                wait = int(r.headers.get("Retry-After") or 0) or min(300, 60 * (attempt + 1))
                print(f"  search rate limited ({r.status_code}), waiting {wait}s")
                time.sleep(wait)
                continue
            r.raise_for_status()
            d = r.json()
            items = [n.get("item", n) for n in d.get("noticeList") or []]
            return d.get("hitCount") or 0, items
        except requests.RequestException as e:
            print(f"  search error (attempt {attempt + 1}): {e}")
            time.sleep(5 * (attempt + 1))
    raise RuntimeError(f"search gave up for {term} in {dfrom}..{dto}")


def _midpoint(dfrom, dto):
    f = datetime.datetime.strptime(dfrom, "%Y-%m-%dT%H:%M:%SZ")
    t = datetime.datetime.strptime(dto, "%Y-%m-%dT%H:%M:%SZ")
    return (f + (t - f) / 2).strftime("%Y-%m-%dT%H:%M:%SZ"), (t - f)


def search_notice_ids(term, dfrom, dto, ids):
    """Collect all notice ids for a keyword term into ids, splitting the
    published window until each half fits inside the response cap."""
    hits, items = _search_window(term, dfrom, dto)
    if hits > MAX_HITS:
        mid, span = _midpoint(dfrom, dto)
        if span < datetime.timedelta(hours=1):
            raise RuntimeError(f"{term}: {hits} hits inside {dfrom}..{dto}, cannot split")
        search_notice_ids(term, dfrom, mid, ids)
        search_notice_ids(term, mid, dto, ids)
        return
    if len(items) < hits:
        raise RuntimeError(f"{term}: {dfrom}..{dto} returned {len(items)} of {hits} hits")
    for it in items:
        nid = it.get("id")
        if nid:
            ids[bare_id(nid)] = it.get("organisationName") or ""
    time.sleep(1.0)


def fetch_notice(nid):
    """GET one published notice. Returns a response, or None if still rate
    limited after backoff (left uncached so a future run retries)."""
    for attempt in range(5):
        r = requests.get(NOTICE_URL + nid, headers=UA, timeout=30)
        if r.status_code in (403, 429):
            wait = int(r.headers.get("Retry-After") or 0) or min(120, 30 * (attempt + 1))
            print(f"  notice rate limited ({r.status_code}), waiting {wait}s")
            time.sleep(wait)
            continue
        return r
    return None


def award_suppliers(payload):
    """Supplier evidence rows from a notice's award records. COMPANIES_HOUSE
    references are the company number OCDS publishes as GB-COH."""
    sups = []
    for a in payload.get("awards") or []:
        name = (a.get("supplierName") or "").strip()
        if not name:
            continue
        ref_type = (a.get("referenceType") or "").upper()
        ref = (a.get("reference") or "").strip()
        if ref_type == "COMPANIES_HOUSE" and ref:
            sups.append({"name": name, "scheme": "GB-COH", "id": ref})
        else:
            sups.append({"name": name, "scheme": None, "id": None})
    return sups


def main():
    published_to = datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    # 1. Discover notice ids per body via keyword search.
    wanted = set()
    for body, (terms, match_names) in BODIES.items():
        body_ids = {}
        for term in terms:
            term_ids = {}
            search_notice_ids(term, PUBLISHED_FROM, published_to, term_ids)
            for nid, org in term_ids.items():
                if any(m in org.lower() for m in match_names):
                    body_ids[nid] = org
        print(f"{body}: {len(body_ids)} notices")
        wanted.update(body_ids)
    print(f"discovered {len(wanted)} unique notices across {len(BODIES)} bodies")
    if len(wanted) < MIN_NOTICES:
        print(f"FATAL: only {len(wanted)} notices discovered, floor is {MIN_NOTICES}; "
              f"not writing output", file=sys.stderr)
        sys.exit(1)

    # 2. Fetch uncached notices. Cached transient errors are retried.
    done = set()
    if CACHE.exists():
        for line in CACHE.open():
            try:
                rec = json.loads(line)
                if rec.get("status") != "err":
                    done.add(bare_id(rec["notice_id"]))
            except Exception:
                pass
    todo = sorted(n for n in wanted if n not in done)
    print(f"{len(done)} cached, {len(todo)} to fetch")

    failures = 0
    with CACHE.open("a") as out:
        for i, nid in enumerate(todo):
            try:
                r = fetch_notice(nid)
                if r is None:
                    failures += 1
                    continue
                sups = award_suppliers(r.json()) if r.status_code == 200 else []
                out.write(json.dumps({"notice_id": nid, "status": r.status_code,
                                      "suppliers": sups}) + "\n")
                out.flush()
            except Exception as e:
                failures += 1
                out.write(json.dumps({"notice_id": nid, "status": "err",
                                      "error": str(e)[:100], "suppliers": []}) + "\n")
                out.flush()
            time.sleep(1.0)
            if (i + 1) % 200 == 0:
                print(f"  {i + 1}/{len(todo)}")
    if todo and failures / len(todo) > MAX_FETCH_FAILURE_RATE:
        print(f"FATAL: {failures}/{len(todo)} notice fetches failed; not writing output",
              file=sys.stderr)
        sys.exit(1)

    # 3. Aggregate the cache, one record per bare notice id. When a legacy
    # suffixed record and a new bare record both exist, prefer whichever
    # carries supplier evidence so nothing double counts.
    records = {}
    for line in CACHE.open():
        try:
            rec = json.loads(line)
        except Exception:
            continue
        if rec.get("status") == "err":
            continue
        key = bare_id(rec.get("notice_id"))
        if key not in records or (rec.get("suppliers") and not records[key].get("suppliers")):
            records[key] = rec

    sys.path.insert(0, str(Path(__file__).parent))
    from resolve_suppliers import normalise
    name_ids = {}
    for rec in records.values():
        for s in rec.get("suppliers") or []:
            if (s.get("scheme") or "").upper().replace("-", "") == "GBCOH" and s.get("id"):
                crn = s["id"].strip().upper().replace(" ", "")
                if 6 <= len(crn) <= 8:
                    key = normalise(s.get("name") or "")
                    if key:
                        crn = crn.zfill(8) if crn.isdigit() else crn
                        name_ids.setdefault(key, {}).setdefault(crn, 0)
                        name_ids[key][crn] += 1
    out_map = {}
    for key, crns in name_ids.items():
        best = max(crns.items(), key=lambda kv: kv[1])
        if best[1] * 2 >= sum(crns.values()):     # majority evidence
            out_map[key] = {"crn": best[0], "notices": best[1]}

    notices_checked = len(records)
    if notices_checked < MIN_NOTICES or len(out_map) < MIN_MAPPINGS:
        print(f"FATAL: {notices_checked} notices checked, {len(out_map)} mappings; "
              f"floors are {MIN_NOTICES}/{MIN_MAPPINGS}; not writing output",
              file=sys.stderr)
        sys.exit(1)

    OUT.write_text(json.dumps(
        {"$meta": {"source": "Contracts Finder V2 API, COMPANIES_HOUSE award references "
                             "(the value OCDS publishes as GB-COH supplier identifiers)",
                   "notices_checked": notices_checked},
         "byName": out_map}))
    print(f"ocds_supplier_ids.json: {len(out_map)} name->CRN mappings "
          f"from {notices_checked} notices")


if __name__ == "__main__":
    main()
