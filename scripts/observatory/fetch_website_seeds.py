#!/usr/bin/env python3
"""Free website seeds for the verified-website layer.

Two public registers already publish a URL against an organisation, so those
are worth trying before any domain guessing:

  Charity Commission register extract  charity_contact_web, joined to a
      company on charity_company_registration_number
  UKRI Gateway to Research               organisation records carry a website;
      joined to Innovate UK winners by registered name, confirmed on postcode

A seed is only a candidate. Nothing here is published until verify_websites.py
proves the match from the site itself.

Writes ~/observatory-data/processed/website_seeds.json
"""
import json
import os
import re
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
from _common import PROC, RAW, log, meta, write_out, zip_json_items, norm_pc
from website_match import name_tokens, is_blocked_host

GTR_API = "https://gtr.ukri.org/gtr/api/organisations"
CACHE = PROC / "_gtr_org_cache.json"


def clean_url(u):
    """Normalise a register-supplied URL, or None if it is unusable."""
    if not u:
        return None
    u = str(u).strip().strip('"').strip()
    if not u or u.lower() in ("none", "n/a", "na", "-", "null"):
        return None
    u = u.split()[0]
    if not re.match(r"^https?://", u, re.I):
        if not re.match(r"^[\w.-]+\.[a-z]{2,}", u, re.I):
            return None
        u = "http://" + u
    m = re.match(r"^(https?)://([^/?#]+)(.*)$", u, re.I)
    if not m:
        return None
    host = m.group(2).lower().split("@")[-1]
    if ":" in host:
        host = host.split(":")[0]
    if not re.match(r"^[a-z0-9.-]+\.[a-z]{2,}$", host):
        return None
    if is_blocked_host(host):
        return None
    return m.group(1).lower() + "://" + host + (m.group(3) or "")


# --- charity register ------------------------------------------------------

def charity_seeds():
    z = RAW / "charity.zip"
    if not z.exists():
        log("charity.zip missing; run fetch_charities.py first")
        return {}
    out = {}
    n = 0
    for c in zip_json_items(z, "publicextract.charity.json"):
        n += 1
        crn = (c.get("charity_company_registration_number") or "").strip()
        url = clean_url(c.get("charity_contact_web"))
        if not crn or not url or crn.lower() == "none":
            continue
        crn = crn.upper()
        if crn.isdigit():
            crn = crn.zfill(8)
        out.setdefault(crn, []).append(url)
    log(f"charity register: {n} rows, {len(out)} company numbers with a website")
    return out


# --- UKRI Gateway to Research ---------------------------------------------

def _load_cache():
    if CACHE.exists():
        try:
            return json.loads(CACHE.read_text())
        except Exception:
            return {}
    return {}


def gtr_seeds(targets):
    """targets: {crn: {"name": ..., "postcode": ...}} for Innovate UK winners.

    GtR organisation records have no company number, so a record only counts
    as a seed when the registered name matches token for token, or the name
    matches loosely and the postcode agrees.
    """
    from _common import get_json
    cache = _load_cache()
    out = {}
    queried = 0
    for crn, t in sorted(targets.items()):
        toks = name_tokens(t["name"])
        if not toks:
            continue
        q = " ".join(toks[:4])
        if q not in cache:
            try:
                d = get_json(GTR_API, params={"q": q, "s": 10},
                             headers={"Accept": "application/json"}, retries=2)
                cache[q] = [
                    {"name": o.get("name"), "website": o.get("website"),
                     "reg": o.get("regNumber"),
                     "pcs": [a.get("postCode") for a in
                             ((o.get("addresses") or {}).get("address") or [])]}
                    for o in (d.get("organisation") or [])
                ]
            except Exception as e:  # noqa
                log(f"gtr query failed for {q}: {e}")
                cache[q] = []
            queried += 1
            time.sleep(0.4)
            if queried % 50 == 0:
                CACHE.write_text(json.dumps(cache))
                log(f"  gtr: {queried} queries")
        want = set(toks)
        for o in cache.get(q, []):
            url = clean_url(o.get("website"))
            if not url:
                continue
            got = set(name_tokens(o.get("name")))
            pcs = {norm_pc(p) for p in (o.get("pcs") or []) if p}
            exact = got == want
            loose = bool(got & want) and len(got & want) >= max(1, len(want) - 1)
            pc_ok = norm_pc(t.get("postcode")) in pcs
            if exact or (loose and pc_ok):
                out.setdefault(crn, [])
                if url not in out[crn]:
                    out[crn].append(url)
    CACHE.write_text(json.dumps(cache))
    log(f"gtr: {queried} live queries, {len(out)} companies with a seed")
    return out


def main():
    idx = json.loads(
        (Path(__file__).resolve().parent.parent.parent /
         "public/data/biz-companies-index.json").read_text())["companies"]
    wanted = {c["crn"] for c in idx}

    import gzip
    master = {}
    with gzip.open(PROC / "master.jsonl.gz", "rt") as f:
        for line in f:
            r = json.loads(line)
            if r["crn"] in wanted:
                master[r["crn"]] = r

    seeds = {}
    src_counts = {}

    ch = charity_seeds()
    for crn, urls in ch.items():
        if crn in wanted:
            for u in urls:
                seeds.setdefault(crn, []).append({"url": u, "source": "charity-register"})
    src_counts["charity-register"] = sum(
        1 for v in seeds.values() if any(s["source"] == "charity-register" for s in v))

    inn = json.loads((PROC / "innovate_lancs.json").read_text())["projects"]
    iuk = {}
    for p in inn:
        crn = (p.get("crn") or "").strip()
        if not crn:
            continue
        if crn.isdigit():
            crn = crn.zfill(8)
        if crn in wanted and crn in master:
            iuk[crn] = {"name": master[crn]["name"],
                        "postcode": master[crn]["postcode"]}
    log(f"innovate winners in the dossier set: {len(iuk)}")
    # GtR organisation records expose a website field but it was unpopulated
    # for all 703 organisations sampled on 27 Jul 2026, so the queries are off
    # by default. Set OBS_GTR_SEEDS=1 to re-test if UKRI backfills it.
    gt = gtr_seeds(iuk) if os.environ.get("OBS_GTR_SEEDS") else {}
    for crn, urls in gt.items():
        for u in urls:
            if not any(s["url"] == u for s in seeds.get(crn, [])):
                seeds.setdefault(crn, []).append({"url": u, "source": "gtr"})
    src_counts["gtr"] = len(gt)

    write_out(
        "website_seeds.json",
        meta("https://register-of-charities.charitycommission.gov.uk/register/full-register-download"
             " + https://gtr.ukri.org/",
             "Open Government Licence v3.0 (Charity Commission); UKRI Gateway to Research terms",
             "Register-published URLs used only as candidates. Nothing is "
             "published until verify_websites.py proves the match on the site "
             "itself."),
        "seeds", seeds,
        extra={"counts": {"companies": len(seeds), "bySource": src_counts}})
    log(f"seeds for {len(seeds)} of {len(wanted)} dossier companies")


if __name__ == "__main__":
    main()
