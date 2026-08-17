#!/usr/bin/env python3
"""Companies House disqualified officers, scoped to the observatory's own officers.

SCOPE. 103,468 CRNs is far too many raw lookups and most of them are companies
we publish nothing about. The proportionate population is the officers already
named on a dossier, because those are the only people any observatory surface
could ever put a disqualification beside.

THE MATCHING PROBLEM, which is the whole reason this script is careful. The
dossier officer list carries a name and nothing else. The disqualified-officers
search is a name search. Matching those two on name alone would attach a
disqualification to whoever happens to share a name with a disqualified
director, which is the single most damaging false positive this project could
produce. So the name is never the evidence:

  stage 1  GET /company/{crn}/officers          -> officer name AND date_of_birth
  stage 2  GET /search/disqualified-officers    -> candidates by name
  stage 3  keep ONLY where the normalised name matches AND the date of birth
           matches on BOTH month and year

CH publishes month and year of birth for officers and for disqualified
officers, so stage 3 is a real test rather than a formality. A candidate that
fails it is counted and discarded, never recorded as a maybe.

Even a passing match is a CANDIDATE, not a finding. Month-year of birth plus a
name is not identity, and LEGAL.md puts registers of individuals in the amber
zone. Nothing here publishes: every record carries `publication: "BLOCKED"` and
the match basis that produced it, so a later reader cannot mistake a name-plus-
DOB agreement for a confirmed person.

RATE LIMIT. 600 requests per 5-minute window per key, and the key is shared
with the monthly refresh, which makes CH liveness calls of its own. The script
reads x-ratelimit-remain from every response and parks itself when the window
falls below --reserve, so a sweep can never be the reason the refresh gets a
429.
"""
import argparse
import base64
import json
import os
import re
import sys
import time
import urllib.parse
import urllib.request
import datetime as _dt
from pathlib import Path

API = "https://api.company-information.service.gov.uk"
SECRETS = Path(os.path.expanduser("~/observatory-data/secrets"))
OUT = Path(os.path.expanduser("~/observatory-data/raw/ch_disqualified"))

NOISE = re.compile(r"[^A-Z ]")
TITLES = {"MR", "MRS", "MS", "MISS", "DR", "SIR", "PROF", "REV", "LORD", "LADY"}


def norm_name(s):
    """Normalise a person name for comparison only.

    CH writes dossier names as "SURNAME, Forename" and search results as
    "Forename SURNAME", so both are reduced to a sorted token set with titles
    and punctuation stripped. This decides nothing on its own; it only gets a
    candidate as far as the date-of-birth test.
    """
    if not s:
        return frozenset()
    s = NOISE.sub(" ", str(s).upper().replace(",", " "))
    return frozenset(t for t in s.split() if len(t) > 1 and t not in TITLES)


class Client:
    def __init__(self, key, reserve, pace):
        self.auth = base64.b64encode(f"{key}:".encode()).decode()
        self.reserve = reserve
        self.pace = pace
        self.requests = 0
        self.parked = 0

    def get(self, path, tries=3):
        for attempt in range(tries):
            req = urllib.request.Request(
                API + path, headers={"Authorization": f"Basic {self.auth}"})
            try:
                with urllib.request.urlopen(req, timeout=30) as r:
                    self.requests += 1
                    body = json.loads(r.read().decode("utf-8", "replace"))
                    self._respect(r.headers)
                    return body
            except urllib.error.HTTPError as e:
                self.requests += 1
                if e.code == 404:
                    return None
                if e.code == 429:
                    # Should not happen given the reserve, but if the window is
                    # genuinely exhausted the only correct move is to wait it out.
                    self.parked += 1
                    time.sleep(30)
                    continue
                if attempt == tries - 1:
                    return {"_error": f"HTTP {e.code}"}
            except Exception:  # noqa: BLE001
                if attempt == tries - 1:
                    return {"_error": "unreachable"}
            time.sleep(2 * (attempt + 1))
        return {"_error": "retries exhausted"}

    def _respect(self, headers):
        time.sleep(self.pace)
        try:
            remain = int(headers.get("x-ratelimit-remain", "999"))
            reset = int(headers.get("x-ratelimit-reset", "0"))
        except ValueError:
            return
        if remain < self.reserve:
            wait = max(0, reset - int(time.time())) + 2
            print(f"  parking {wait}s: {remain} left in window, reserving "
                  f"{self.reserve} for the refresh", file=sys.stderr)
            self.parked += 1
            time.sleep(min(wait, 330))


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--companies", required=True)
    ap.add_argument("--limit", type=int, default=0)
    ap.add_argument("--max-requests", type=int, default=6000)
    ap.add_argument("--reserve", type=int, default=200,
                    help="requests left in the 5-minute window to leave for the refresh")
    ap.add_argument("--pace", type=float, default=0.35)
    ap.add_argument("--include-resigned", action="store_true")
    ap.add_argument("--out", default=None)
    args = ap.parse_args()

    key = (SECRETS / "ch_rest.key").read_text().strip()
    cl = Client(key, args.reserve, args.pace)
    companies = json.load(open(args.companies))["companies"]
    if args.limit:
        companies = companies[:args.limit]
    OUT.mkdir(parents=True, exist_ok=True)
    out_path = Path(args.out) if args.out else OUT / "disqualified_candidates.json"
    started = _dt.datetime.now(_dt.timezone.utc)

    # ---- stage 1: officers with a date of birth -------------------------
    officers = {}
    companies_done = 0
    for c in companies:
        if cl.requests >= args.max_requests:
            break
        crn = c.get("crn")
        if not crn:
            continue
        res = cl.get(f"/company/{crn}/officers?items_per_page=100")
        companies_done += 1
        if not res or "items" not in res:
            continue
        for it in res["items"]:
            if it.get("resigned_on") and not args.include_resigned:
                continue
            dob = it.get("date_of_birth") or {}
            y, m = dob.get("year"), dob.get("month")
            if not (y and m):
                continue  # no DOB means no testable match, so it is not swept
            k = (it.get("name", ""), y, m)
            officers.setdefault(k, {"name": it.get("name"), "dobYear": y,
                                    "dobMonth": m, "companies": []})
            officers[k]["companies"].append({"crn": crn, "name": c.get("name"),
                                             "role": it.get("officer_role")})
        if companies_done % 100 == 0:
            print(f"  stage 1: {companies_done}/{len(companies)} companies, "
                  f"{len(officers)} distinct officers, {cl.requests} requests",
                  file=sys.stderr)

    print(f"stage 1 done: {len(officers)} distinct officers with a DOB from "
          f"{companies_done} companies ({cl.requests} requests)", file=sys.stderr)

    # ---- stage 2 and 3: search, then test the date of birth -------------
    candidates = []
    searched = rejected_dob = rejected_name = 0
    for k, off in officers.items():
        if cl.requests >= args.max_requests:
            print("STOP: hit --max-requests", file=sys.stderr)
            break
        q = urllib.parse.quote(off["name"][:120])
        res = cl.get(f"/search/disqualified-officers?q={q}&items_per_page=50")
        searched += 1
        if not res or not res.get("items"):
            continue
        ours = norm_name(off["name"])
        for it in res["items"]:
            theirs = norm_name(it.get("title") or it.get("address_snippet"))
            if not ours or not theirs or not ours.issubset(theirs) and not theirs.issubset(ours):
                rejected_name += 1
                continue
            dob = it.get("date_of_birth")
            dy = dm = None
            if isinstance(dob, dict):
                dy, dm = dob.get("year"), dob.get("month")
            elif isinstance(dob, str):
                mm = re.match(r"(\d{4})-(\d{2})", dob)
                if mm:
                    dy, dm = int(mm.group(1)), int(mm.group(2))
            if not dy or int(dy) != int(off["dobYear"]) or int(dm) != int(off["dobMonth"]):
                rejected_dob += 1
                continue
            candidates.append({
                "officerName": off["name"],
                "dobYear": off["dobYear"], "dobMonth": off["dobMonth"],
                "observatoryCompanies": off["companies"],
                "disqualifiedRecord": {
                    "title": it.get("title"),
                    "links": it.get("links"),
                    "addressSnippet": it.get("address_snippet"),
                    "kind": it.get("kind"),
                },
                "matchBasis": "normalised name agreement AND date of birth equal on year and month",
                "evidenceClass": "candidate-not-confirmed",
                "publication": "BLOCKED",
            })
        if searched % 200 == 0:
            print(f"  stage 2: {searched}/{len(officers)} searched, "
                  f"{len(candidates)} candidates, {cl.requests} requests", file=sys.stderr)

    payload = {
        "$meta": {
            "source": "Companies House public data API, disqualified officers",
            "sourceUrl": f"{API}/search/disqualified-officers",
            "generated": _dt.datetime.now(_dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
            "startedAt": started.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "population": "officers named on observatory dossiers, active appointments only"
                          if not args.include_resigned else "officers named on observatory dossiers",
            "companiesQueried": companies_done,
            "distinctOfficersWithDob": len(officers),
            "officersSearched": searched,
            "candidatesRejectedOnName": rejected_name,
            "candidatesRejectedOnDob": rejected_dob,
            "candidates": len(candidates),
            "requests": cl.requests,
            "rateLimit": "600 requests per 5 minute window per key, measured from "
                         "x-ratelimit-limit; this run reserved "
                         f"{args.reserve} for the monthly refresh",
            "matchRule": ("A candidate requires normalised name agreement AND date of birth "
                          "equal on BOTH month and year. Officers with no published DOB are "
                          "not swept at all, because there would be nothing to test a name "
                          "against."),
            "publicationRule": ("BLOCKED. LEGAL.md amber: a register of individuals. Name plus "
                                "month and year of birth is not identity, so every row here is "
                                "a candidate for human checking and NOTHING derived from this "
                                "file may appear on any public surface, in any aggregate, or "
                                "on a company dossier."),
            "licence": "Companies House public data, Crown copyright, Open Government Licence v3.0",
        },
        "candidates": candidates,
    }
    out_path.write_text(json.dumps(payload, indent=2) + "\n")
    print(f"wrote {out_path}: {len(candidates)} candidates from {searched} officers "
          f"searched, {cl.requests} requests, {cl.parked} pauses")
    return 0


if __name__ == "__main__":
    sys.exit(main())
