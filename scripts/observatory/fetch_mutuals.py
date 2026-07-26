#!/usr/bin/env python3
"""fetch_mutuals.py - FCA Mutuals Public Register, filtered to Lancashire.

The register has no bulk CSV endpoint that responds to non-browser clients (the
CSV is built client-side from search results, and the search API returns the SPA
shell to direct requests). Society detail pages, however, are server-rendered
HTML at /Search/Society/{id} and carry the society name, address, postcode,
status and registration details. This enumerates the id space (valid societies
run up to ~30,000; higher ids return an empty template), parses each page, and
keeps societies whose postcode resolves to one of the 14 Lancs LADs.

Output: ~/observatory-data/processed/mutuals_lancs.json
"""
import sys
import re
import html as _html
import concurrent.futures as cf
from pathlib import Path
import requests

sys.path.insert(0, str(Path(__file__).parent))
from _common import (LANCS_14, meta, write_out, clean_text, log, UA,
                     looks_lancs_pc, resolve_postcodes, lad_for_postcode)

BASE = "https://mutuals.fca.org.uk/Search/Society/"
MAX_ID = 30300
PC_RE = re.compile(r"\b[A-Z]{1,2}\d[A-Z\d]? ?\d[A-Z]{2}\b")

_local = None


def session():
    global _local
    if _local is None:
        _local = requests.Session()
        _local.headers.update({"User-Agent": UA})
    return _local


def parse(nid, html):
    t = re.sub(r"<script.*?</script>", " ", html, flags=re.S)
    mtitle = re.search(r"<title>(.*?)</title>", html, re.S)
    title = clean_text(_html.unescape(mtitle.group(1))) if mtitle else ""
    name = title.split(":", 1)[1].strip() if ":" in title else title
    # Address block: from the 'Address' label to the following </ tag region.
    maddr = re.search(r"Address\"?>Address\s*(.*?)<label", t, re.S)
    if not maddr:
        maddr = re.search(r">Address\s*(.*?)<label", t, re.S)
    addr = ""
    postcode = None
    if maddr:
        addr = re.sub(r"<[^>]+>", " ", maddr.group(1))
        addr = re.sub(r"\s+", " ", addr).strip()
        pcs = PC_RE.findall(addr)
        postcode = pcs[-1].strip() if pcs else None
    if not name or name == "Mutuals Public Register":
        return None
    # Flatten tags to spaces for the label/value fields.
    flat = re.sub(r"\s+", " ", _html.unescape(re.sub(r"<[^>]+>", " ", t)))
    ms = re.search(r"Status\s+(Registered|Deregistered|Dissolved|Cancelled|Converted)", flat)
    status = ms.group(1) if ms else None
    mtype = re.search(r"Registration as\s+(.+?)\s+Registration (?:Date|Act)", flat)
    soc_type = clean_text(mtype.group(1)) if mtype else None
    mdate = re.search(r"Registration Date\s+([0-9]{1,2} [A-Za-z]+ [0-9]{4})", flat)
    reg_date = mdate.group(1) if mdate else None
    return {
        "society_id": nid,
        "name": name,
        "postcode": postcode,
        "address": addr or None,
        "status": status,
        "society_type": soc_type,
        "registration_date": reg_date,
        "uri": BASE + str(nid),
    }


def fetch(nid):
    try:
        r = session().get(BASE + str(nid), timeout=30)
        if r.status_code != 200:
            return None
        rec = parse(nid, r.text)
        if rec and rec.get("postcode") and looks_lancs_pc(rec["postcode"]):
            return rec
    except Exception:  # noqa
        return None
    return None


def main():
    log(f"enumerating mutuals society ids 1..{MAX_ID}")
    candidates = []
    with cf.ThreadPoolExecutor(max_workers=16) as ex:
        futs = {ex.submit(fetch, i): i for i in range(1, MAX_ID + 1)}
        done = 0
        for fut in cf.as_completed(futs):
            done += 1
            rec = fut.result()
            if rec:
                candidates.append(rec)
            if done % 5000 == 0:
                log(f"  scanned {done}/{MAX_ID}, Lancs candidates {len(candidates)}")
    log(f"Lancs-postcode candidates: {len(candidates)}")

    cache = resolve_postcodes([c["postcode"] for c in candidates])
    rows = []
    per_lad = {}
    for c in candidates:
        res = lad_for_postcode(c["postcode"], cache)
        if not res or res["lad"] not in LANCS_14:
            continue
        c["lad"] = res["lad"]
        rows.append(c)
        per_lad[res["lad"]] = per_lad.get(res["lad"], 0) + 1

    m = meta(
        "https://mutuals.fca.org.uk/ (Search/Society detail pages)",
        "Open Government Licence / Crown copyright (FCA Mutuals Public Register)",
        "Co-operatives, community benefit societies, credit unions and working "
        "men's clubs whose registered-office postcode resolves to one of the 14 "
        "Lancashire LADs. Assembled by enumerating server-rendered society detail "
        "pages (no machine CSV endpoint responds to non-browser clients). Includes "
        "both registered and deregistered/dissolved societies; filter by status "
        "downstream. Society registration number is not reliably exposed on the "
        "detail page, so the internal society_id and page uri are provided as keys.")
    m["per_lad_counts"] = {LANCS_14[k]: v for k, v in per_lad.items()}
    m["max_id_scanned"] = MAX_ID
    write_out("mutuals_lancs.json", m, "societies", rows)


if __name__ == "__main__":
    main()
