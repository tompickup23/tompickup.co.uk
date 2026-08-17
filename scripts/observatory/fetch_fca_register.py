#!/usr/bin/env python3
"""FCA Financial Services Register: resolve observatory companies to FRNs.

WHAT THIS IS NOT. There is no bulk download and no geographic query. The
V0.1 register API is a per-firm lookup by FRN plus a NAME-only search, tested
17 Aug 2026: `q=BB11` and `q="BB11 1"` return nothing at all, while `q=PR1`
returns STYLUX PR1 LTD, a firm with "PR1" in its NAME. The postcode shown in a
search result is display text, not an index. So a Lancashire firm cannot be
enumerated; it can only be found by name and then confirmed.

WHAT IT IS. The per-firm record carries a `Companies House Number`, which makes
the FCA a DETERMINISTIC crosswalk source rather than a fuzzy one, exactly like
the SRA (DATA-INTEGRITY s7.5). So the rule here is:

    search by name  ->  candidate FRNs  ->  fetch each firm  ->  keep ONLY
    those whose Companies House Number EQUALS the CRN we started from.

A name match alone never lands. That is not caution for its own sake: the
search is loosely ranked and `q=barclays` returns "PEAC Business Finance
Limited" as its first result, so a name-similarity tier here would publish
nonsense. Every edge this writes is `identifier-observed`: the FCA wrote the
company number down and we read it.

Two cost controls, because the search returns up to 20 candidates per name and
fetching all of them for 1,046 names would be 20,000 requests:
  1. candidates are prefiltered on the postcode the search result embeds, to
     the Lancashire outcode families, before any firm fetch;
  2. everything is paced and bounded by --max-requests.

No rate-limit headers are exposed by the service and the terms only say there
is "a maximum number of accesses within 24-hour periods", so the default pace
is deliberately slow.

Auth needs BOTH headers together: x-auth-email and x-auth-key.
"""
import argparse
import json
import os
import re
import sys
import time
import urllib.parse
import urllib.request
import datetime as _dt
from pathlib import Path

BASE = "https://register.fca.org.uk/services/V0.1"
SECRETS = Path(os.path.expanduser("~/observatory-data/secrets"))
OUT = Path(os.path.expanduser("~/observatory-data/raw/fca"))

# Postcode areas that can contain a Lancashire-14 address. BB/PR/FY/LA are the
# cores; BL/OL/WN/SK spill across the boundary and are kept as candidates
# because the CRN test below is what actually decides, not the postcode.
LANCS_AREAS = {"BB", "PR", "FY", "LA", "BL", "OL", "WN", "SK"}
POSTCODE_IN_NAME = re.compile(r"\(Postcode:\s*([A-Z]{1,2})\d", re.I)


def creds():
    email = (SECRETS / "fca.email").read_text().strip()
    key = (SECRETS / "fca.key").read_text().strip()
    return email, key


# The register 403s urllib's default User-Agent. Found the hard way on
# 17 Aug 2026: an entire sweep returned "no results" for every company,
# including one confirmed by hand to be in the register minutes earlier,
# because every request was a 403 that the caller read as an empty result.
# That is the same silent-ignored-failure family as the Gazette category-code
# bug (s9.5) and the OCDS empty map (s11.2), which is why errors are now
# COUNTED and a run that mostly failed refuses to write anything at all.
UA = "LancashireBusinessObservatory/1.0 (+https://tompickup.co.uk/lancs/business)"


def get(path, email, key, tries=3, stats=None):
    url = path if path.startswith("http") else BASE + path
    req = urllib.request.Request(url, headers={
        "x-auth-email": email, "x-auth-key": key,
        "Accept": "application/json", "User-Agent": UA})
    for attempt in range(tries):
        try:
            with urllib.request.urlopen(req, timeout=30) as r:
                body = json.loads(r.read().decode("utf-8", "replace"))
                if stats is not None:
                    stats["ok"] = stats.get("ok", 0) + 1
                return body
        except Exception as exc:  # noqa: BLE001
            if attempt == tries - 1:
                if stats is not None:
                    stats["failed"] = stats.get("failed", 0) + 1
                    stats.setdefault("firstError", str(exc))
                return {"_error": str(exc)}
            time.sleep(2 * (attempt + 1))
    return {"_error": "unreachable"}


def norm_crn(v):
    """CH numbers are exactly eight uppercase alphanumeric characters (s9.3).

    The FCA field is free text and carries short numeric forms, so it is
    zero-padded before comparison. Anything that cannot be an eight-character
    company number is refused rather than coerced.
    """
    if not v:
        return None
    s = re.sub(r"[^0-9A-Za-z]", "", str(v)).upper()
    if not s:
        return None
    if s.isdigit():
        s = s.zfill(8)
    return s if len(s) == 8 else None


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--companies", required=True,
                    help="biz-companies-index.json, the dossier population")
    ap.add_argument("--limit", type=int, default=0, help="first N companies only")
    ap.add_argument("--max-requests", type=int, default=4000)
    ap.add_argument("--sleep", type=float, default=0.7)
    ap.add_argument("--out", default=None)
    args = ap.parse_args()

    email, key = creds()
    companies = json.load(open(args.companies))["companies"]
    if args.limit:
        companies = companies[:args.limit]

    OUT.mkdir(parents=True, exist_ok=True)
    out_path = Path(args.out) if args.out else OUT / "fca_register_lancs.json"

    n_req = 0
    stats = {"ok": 0, "failed": 0}
    matches, searched, cand_seen, firm_fetched = [], 0, 0, 0
    unmatched_candidates = 0
    firm_cache = {}
    started = _dt.datetime.now(_dt.timezone.utc)

    for i, c in enumerate(companies):
        if n_req >= args.max_requests:
            print(f"STOP: hit --max-requests {args.max_requests}", file=sys.stderr)
            break
        crn = norm_crn(c.get("crn"))
        name = (c.get("name") or "").strip()
        if not crn or not name:
            continue

        q = urllib.parse.quote(name[:120])
        res = get(f"/Search?q={q}&type=firm", email, key, stats=stats)
        n_req += 1
        searched += 1
        time.sleep(args.sleep)
        rows = res.get("Data") or []

        for r in rows:
            disp = r.get("Name") or ""
            m = POSTCODE_IN_NAME.search(disp)
            area = m.group(1).upper() if m else None
            # No postcode shown is kept as a candidate: absence of display text
            # is not evidence of a non-Lancashire address.
            if area and area not in LANCS_AREAS:
                continue
            frn = r.get("Reference Number")
            if not frn:
                continue
            cand_seen += 1
            if n_req >= args.max_requests:
                break
            if frn not in firm_cache:
                firm_cache[frn] = get(f"/Firm/{frn}", email, key, stats=stats)
                n_req += 1
                firm_fetched += 1
                time.sleep(args.sleep)
            fd = (firm_cache[frn].get("Data") or [{}])[0]
            fca_crn = norm_crn(fd.get("Companies House Number"))
            if fca_crn and fca_crn == crn:
                matches.append({
                    "crn": crn,
                    "observatoryName": name,
                    "frn": str(frn),
                    "fcaOrganisationName": fd.get("Organisation Name"),
                    "fcaStatus": fd.get("Status"),
                    "businessType": fd.get("Business Type"),
                    "companiesHouseNumberAsPublished": fd.get("Companies House Number"),
                    "mutualSocietyNumber": fd.get("Mutual Society Number") or None,
                    "searchDisplayName": disp,
                    "evidenceClass": "identifier-observed",
                    "matchRule": "FCA-published Companies House Number equals the register CRN",
                })
                break
            unmatched_candidates += 1

        if (i + 1) % 50 == 0:
            print(f"  {i+1}/{len(companies)} searched, {len(matches)} confirmed, "
                  f"{n_req} requests", file=sys.stderr)

    payload = {
        "$meta": {
            "source": "FCA Financial Services Register, API V0.1",
            "sourceUrl": BASE,
            "generated": _dt.datetime.now(_dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
            "startedAt": started.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "population": "observatory dossier companies (biz-companies-index.json)",
            "populationCount": len(companies),
            "searched": searched,
            "candidatesAfterPostcodePrefilter": cand_seen,
            "firmRecordsFetched": firm_fetched,
            "candidatesRejectedOnCrn": unmatched_candidates,
            "confirmed": len(matches),
            "requests": n_req,
            "requestsOk": stats["ok"],
            "requestsFailed": stats["failed"],
            "matchRule": ("identifier-observed only: an FCA firm record is kept ONLY where "
                          "its published Companies House Number equals the CRN searched for. "
                          "Name similarity never lands an edge."),
            "accessNote": ("Lookup and name-search only. No bulk download and no geographic "
                           "query: postcode is not indexed (q=BB11 returns nothing), so a "
                           "Lancashire firm cannot be enumerated, only confirmed."),
            "licence": ("FCA Financial Services Register. Public register data, "
                        "re-use subject to the FCA's terms; attribution to the FCA."),
        },
        "firms": matches,
    }
    # A run that mostly failed must not be written as a result. An empty map
    # from a broken fetcher is indistinguishable from an honest zero once it
    # is on disk, which is exactly how 260 OCDS identifications went missing
    # for months (DATA-INTEGRITY s11.2).
    attempted = stats["ok"] + stats["failed"]
    if attempted and stats["failed"] / attempted > 0.10:
        print(f"REFUSING TO WRITE: {stats['failed']} of {attempted} requests failed "
              f"({stats['failed']/attempted:.0%}). First error: {stats.get('firstError')}. "
              "Previous output left in place.", file=sys.stderr)
        return 2
    if searched and stats["ok"] == 0:
        print("REFUSING TO WRITE: no request succeeded.", file=sys.stderr)
        return 2

    out_path.write_text(json.dumps(payload, indent=2) + "\n")
    print(f"wrote {out_path}: {len(matches)} confirmed from {searched} searched, "
          f"{n_req} requests")
    return 0


if __name__ == "__main__":
    sys.exit(main())
