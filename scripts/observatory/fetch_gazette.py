#!/usr/bin/env python3
"""fetch_gazette.py - The Gazette corporate insolvency notices (category 24),
2024-01-01 to now, filtered to Lancashire.

Personal insolvency (category 25) is EXCLUDED entirely (legal rule). Asking the
feed for it is not enough: the feed does NOT honour category-code when the
location parameters are present, so a geo search returns category 25 personal
insolvency, 29 deceased estates and 16 planning notices too, all naming private
individuals. Every notice outside category 24 is therefore dropped here, before
the detail fetch, so those notices are neither fetched nor cached, and the
dropped counts are logged and recorded in $meta so the feed's behaviour stays
visible. The silver builder repeats the rule as a second gate.

The Gazette summary feed carries no address or company number, and category 24
nationally is ~1M notices (it includes high-volume strike-off notices), so a
national pull is impractical. Geographic filtering in the API is weak, so this
uses a locality text-search over Lancashire place names to gather candidate
notice ids, then fetches each candidate's linked-data JSON to extract the
company number, registered-office postcode, precise insolvency type and date,
and keeps only notices whose postcode resolves to one of the 14 Lancs LADs.

Outputs:
  gazette_lancs.json          - {notice_id, type, company_name, company_number, date, uri}
  gazette_corporate_all.jsonl - the Lancashire-locality candidate set (summary
                                level) for the company-number join downstream.
"""
import sys
import json
import re
import time
import concurrent.futures as cf
from pathlib import Path
from lxml import etree

PC_RE = re.compile(r"\b[A-Z]{1,2}\d[A-Z\d]? ?\d[A-Z]{2}\b")

sys.path.insert(0, str(Path(__file__).parent))
from _common import (RAW, PROC, LANCS_14, meta, write_out, clean_text, log, UA,
                     resolve_postcodes, lad_for_postcode, norm_pc)

FEED = "https://www.thegazette.co.uk/all-notices/notice/data.feed"
START = "2024-01-01"
END = time.strftime("%Y-%m-%d")
ATOM = "{http://www.w3.org/2005/Atom}"
GZ = "{https://www.thegazette.co.uk/facets}"

KEEP_CATEGORY = "24"      # corporate insolvency, including strike-off notices
PERSONAL_CATEGORY = "25"  # personal insolvency: excluded entirely, legal rule

# Geo circles (postcode centroid + radius in miles) blanketing the 14 LADs.
# The Gazette notice feed supports location-postcode-1 + location-distance-1
# (miles). Circles overlap into neighbouring areas; the exact postcode->LAD
# filter downstream keeps only the 14 Lancs LADs, so over-reach is harmless.
GEO_CIRCLES = [
    ("LA1 1HT", 8), ("LA4 5LZ", 6),   # Lancaster, Morecambe
    ("PR1 2RL", 7), ("PR25 1DH", 6),  # Preston, Leyland/South Ribble
    ("BB7 2DD", 7),                   # Ribble Valley (Clitheroe)
    ("FY8 1AA", 6), ("FY6 7BB", 6), ("FY1 1AD", 6),  # Fylde, Wyre, Blackpool
    ("BB1 7DY", 6), ("BB5 0AA", 5), ("BB4 6HW", 5),  # Blackburn, Accrington, Rossendale
    ("BB9 0AA", 5), ("BB11 1AA", 6),  # Pendle (Nelson), Burnley
    ("PR7 1DP", 6), ("L39 2DR", 7),   # Chorley, West Lancs (Ormskirk)
]

REQ_HEADERS = {"User-Agent": UA}


def search_locality(circle):
    """Return candidate summary dicts for one (postcode, radius) circle."""
    import requests
    postcode, radius = circle
    out = []
    page = 1
    while True:
        params = {"category-code": "24", "start-publish-date": START,
                  "end-publish-date": END, "location-postcode-1": postcode,
                  "location-distance-1": str(radius),
                  "results-page-size": "100", "results-page": str(page)}
        try:
            r = requests.get(FEED, params=params, headers=REQ_HEADERS, timeout=60)
            if r.status_code != 200:
                break
            root = etree.fromstring(r.content)
        except Exception as e:  # noqa
            log(f"  {postcode}/{radius}mi p{page} error: {e}")
            break
        total_el = root.find(GZ + "total")
        total = int(total_el.text) if total_el is not None and total_el.text else 0
        entries = root.findall(ATOM + "entry")
        if not entries:
            break
        for e in entries:
            nid = e.findtext(ATOM + "id", "").rsplit("/", 1)[-1]
            out.append({
                "notice_id": nid,
                "company_name": clean_text(e.findtext(ATOM + "title")),
                "notice_code": e.findtext(GZ + "notice-code"),
                "date": (e.findtext(ATOM + "published") or "")[:10],
                "uri": f"https://www.thegazette.co.uk/notice/{nid}",
                "matched_circle": f"{postcode}/{radius}mi",
            })
        if page * 100 >= total or page >= 80:
            break
        page += 1
        time.sleep(0.1)
    return out


def _strings(obj):
    """Yield all string leaves from a nested dict/list."""
    if isinstance(obj, str):
        yield obj
    elif isinstance(obj, dict):
        for v in obj.values():
            yield from _strings(v)
    elif isinstance(obj, list):
        for v in obj:
            yield from _strings(v)


def extract_postcode(ro):
    """Robustly pull a UK postcode from a registered-office block.

    The Gazette encodes the postcode inconsistently: a postalCode string, a
    postcode URI (.../id/postcode/BB128BS), a nested {_about, label} object, or
    a list of any of these. Gather every string leaf, pull postcode-URI tails,
    and regex for the first valid postcode.
    """
    candidates = []
    for s in _strings(ro):
        if "/postcode/" in s:
            candidates.append(s.rsplit("/postcode/", 1)[-1])
        else:
            candidates.append(s)
    for c in candidates:
        m = PC_RE.search(c.upper().replace(" ", ""))
        if m:
            pc = m.group(0)
            return pc[:-3] + " " + pc[-3:]  # normalise with a single space
    return None


def fetch_detail(nid):
    """Fetch a notice's linked-data JSON; return (company_number, postcode, type)."""
    import requests
    url = f"https://www.thegazette.co.uk/notice/{nid}/data.json?view=linked-data"
    try:
        r = requests.get(url, headers={"User-Agent": UA, "Accept": "application/json"},
                         timeout=40)
        if r.status_code != 200:
            return None
        pt = r.json().get("result", {}).get("primaryTopic", {})
        types = pt.get("type", [])
        if isinstance(types, str):
            types = [types]
        ntype = None
        for t in types:
            if "insolvency#" in t:
                ntype = t.split("#", 1)[1]
                break
        about = pt.get("isAbout", {})
        if isinstance(about, list):
            about = about[0] if about else {}
        comp = about.get("company", {}) if isinstance(about, dict) else {}
        if isinstance(comp, list):
            comp = comp[0] if comp else {}
        # An identifier read out of a publisher's text is not clean until it
        # has been trimmed, and the place to do that is here, where the text
        # arrives, not in the consumer that joins on it. The Gazette publishes
        # some company numbers with a trailing space ("12541918 "); joined raw,
        # the register lookup misses and the notice renders as though we had
        # never heard of a company we hold in our own register.
        cnum = (comp.get("companyNumber") or "").strip() or None
        ro = comp.get("hasRegisteredOffice", {}) if isinstance(comp, dict) else {}
        if isinstance(ro, list):
            ro = ro[0] if ro else {}
        postcode = extract_postcode(ro) if isinstance(ro, dict) else None
        return {"company_number": cnum, "postcode": postcode, "type": ntype}
    except Exception:  # noqa
        return None


def main():
    # 1. Gather candidates across geo circles, dedupe by notice id.
    candidates = {}
    for circle in GEO_CIRCLES:
        rows = search_locality(circle)
        for r in rows:
            candidates.setdefault(r["notice_id"], r)
        log(f"  {circle[0]}/{circle[1]}mi: {len(rows)} hits (running unique {len(candidates)})")
    log(f"total unique candidate notices: {len(candidates)}")

    # 2. Drop everything outside category 24. The feed ignores category-code
    #    when the location parameters are present, so what comes back includes
    #    category 25 personal insolvency, 29 deceased estates and 16 planning
    #    notices, all naming private individuals. Personal insolvency is
    #    excluded entirely by legal rule (DATA-INTEGRITY s3, s9.5). Dropping
    #    here, before the detail fetch, means those notices are never fetched
    #    and never written to the candidate file.
    kept, dropped = {}, {}
    for nid, r in candidates.items():
        cat = (r.get("notice_code") or "__")[:2]
        if cat == KEEP_CATEGORY:
            kept[nid] = r
        else:
            dropped[cat] = dropped.get(cat, 0) + 1
    candidates = kept
    if dropped:
        by_cat = ", ".join(f"{c}:{n}" for c, n in
                           sorted(dropped.items(), key=lambda kv: -kv[1]))
        log(f"dropped {sum(dropped.values())} notices outside category "
            f"{KEEP_CATEGORY} (the feed ignores category-code alongside the "
            f"location parameters): {by_cat}")
        log(f"  of those, {dropped.get(PERSONAL_CATEGORY, 0)} were category "
            f"{PERSONAL_CATEGORY} personal insolvency, excluded by legal rule")
    log(f"category {KEEP_CATEGORY} candidates: {len(candidates)}")

    # 3. Fetch detail (company number + postcode + type) concurrently.
    ids = list(candidates.keys())
    details = {}
    with cf.ThreadPoolExecutor(max_workers=8) as ex:
        futs = {ex.submit(fetch_detail, nid): nid for nid in ids}
        done = 0
        for fut in cf.as_completed(futs):
            nid = futs[fut]
            details[nid] = fut.result()
            done += 1
            if done % 1000 == 0:
                log(f"  detail {done}/{len(ids)}")

    # 4. Save the ENRICHED candidate set as the join table (JSONL): every
    #    category 24 notice gathered, with company number + postcode + precise
    #    type. This is the table the CH company-number join runs against. The
    #    file is rewritten whole on every run, so a run with this filter in
    #    place clears any out-of-category rows a previous run left behind.
    jsonl = PROC / "gazette_corporate_all.jsonl"
    with open(jsonl, "w") as f:
        for nid, r in candidates.items():
            d = details.get(nid) or {}
            row = dict(r)
            row["company_number"] = d.get("company_number")
            row["reg_office_postcode"] = d.get("postcode")
            row["insolvency_type"] = d.get("type")
            f.write(json.dumps(row, ensure_ascii=False) + "\n")
    log(f"wrote gazette_corporate_all.jsonl: {len(candidates)} rows (enriched)")

    # 5. Resolve postcodes -> LAD, keep Lancashire.
    pcs = [d["postcode"] for d in details.values() if d and d.get("postcode")]
    cache = resolve_postcodes(pcs)

    rows = []
    per_lad = {}
    no_pc = 0
    for nid, cand in candidates.items():
        d = details.get(nid)
        if not d:
            continue
        pc = d.get("postcode")
        res = lad_for_postcode(pc, cache) if pc else None
        if not res or res["lad"] not in LANCS_14:
            if not pc:
                no_pc += 1
            continue
        rows.append({
            "notice_id": nid,
            "type": d.get("type") or ("code:" + str(cand.get("notice_code"))),
            "company_name": cand["company_name"],
            "company_number": d.get("company_number"),
            "date": cand["date"],
            "uri": cand["uri"],
            "lad": res["lad"],
            "postcode": pc,
        })
        per_lad[res["lad"]] = per_lad.get(res["lad"], 0) + 1

    m = meta(
        FEED + "?category-code=24 (corporate insolvency)",
        "Open Government Licence v3.0 / Crown copyright (The Gazette)",
        "Corporate insolvency notices only (category 24); personal insolvency "
        "(category 25) excluded entirely per legal rule. The feed does not "
        "honour category-code when the location parameters are present, so the "
        "geo search also returns categories 25, 29 (deceased estates) and 16 "
        "(planning); every notice outside category 24 is dropped by explicit "
        "code rule before the detail fetch, and the dropped counts are recorded "
        "in candidates_dropped_by_category below. Category 24 "
        "nationally is ~1M notices in the window (includes high-volume strike-off "
        "notices) and the summary feed carries no address or company number, so a "
        "national pull is impractical. Candidates were gathered via the feed's "
        "geo search (location-postcode-1 + location-distance-1) over a set of "
        "postcode-centroid circles blanketing the 14 LADs, then each candidate's "
        "linked-data JSON was fetched to extract company number, registered-office "
        "postcode and precise type; kept only where postcode resolves to a Lancs "
        f"LAD. Window {START} to {END}. Geo recall is imperfect (a notice is "
        "indexed on one address, not always the registered office), so this list "
        "under-counts and skews toward areas with prolific local practitioners; "
        "the authoritative Lancashire set comes from joining company_number to the "
        "CH spine. gazette_corporate_all.jsonl holds the category 24 candidate set "
        "enriched with company_number + reg_office_postcode + insolvency_type for "
        "that join.")
    with_cn = sum(1 for d in details.values() if d and d.get("company_number"))
    m["window"] = {"start": START, "end": END}
    m["candidate_count"] = len(candidates)
    m["candidates_dropped_by_category"] = dict(
        sorted(dropped.items(), key=lambda kv: -kv[1]))
    m["candidates_dropped_personal_insolvency"] = dropped.get(
        PERSONAL_CATEGORY, 0)
    m["candidates_with_company_number"] = with_cn
    m["per_lad_counts"] = {LANCS_14[k]: v for k, v in per_lad.items()}
    m["candidates_no_postcode_in_detail"] = no_pc
    write_out("gazette_lancs.json", m, "notices", rows)


if __name__ == "__main__":
    main()
