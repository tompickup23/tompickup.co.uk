#!/usr/bin/env python3
"""Attach a VERIFIED website to observatory companies.

Candidates come from register seeds, from domain guesses built off the
registered and trading names, and optionally from a search API. A candidate is
only ever accepted when the site itself proves the match, per website_match.py:
the registration number on the page, or the exact registered name together
with the registered-office postcode. Everything else is rejected.

Politeness: robots.txt honoured, one request per second per host, identifying
user agent, 10 second timeouts, a host is dropped after 5 failures. Only the
homepage text of a VERIFIED site is retained, capped at 20KB, for the sector
classification layer that comes later. Nothing is kept from a rejected site.

Outputs
  ~/observatory-data/processed/websites.jsonl          one line per verified company
  ~/observatory-data/processed/websites_summary.json   run statistics
  ~/observatory-data/processed/website_text/{crn}.txt  homepage text, verified only

Usage
  python3 verify_websites.py [--limit N] [--only CRN[,CRN]] [--refresh]
                            [--workers N] [--source index|master]
"""
import argparse
import gzip
import json
import os
import re
import socket
import sys
import threading
import time
import urllib.parse
import urllib.robotparser
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
from _common import PROC, log
from website_match import (guess_domains, is_blocked_host, name_tokens,
                           page_text, path_segment_names, verify)

ROOT = Path(__file__).resolve().parent.parent.parent
OUT_JSONL = PROC / "websites.jsonl"
OUT_SUMMARY = PROC / "websites_summary.json"
TEXT_DIR = PROC / "website_text"

UA = ("LancashireObservatory/1.0 "
      "(+https://tompickup.co.uk/lancs/business/method/)")
TIMEOUT = 10
PER_HOST_DELAY = 1.0
MAX_HOST_FAILURES = 5
MAX_BYTES = 2_000_000
MAX_TEXT = 20_000
MAX_GUESSES = 16
MAX_LEGAL_PAGES = 5

LEGAL_HINTS = ("contact", "about", "terms", "privacy", "legal", "imprint",
               "impressum", "policy", "cookie", "company-information",
               "who-we-are", "t-and-c", "disclaimer", "conditions",
               "corporate", "accessibility", "modern-slavery")
STATIC_LEGAL_PATHS = ("/contact", "/contact-us", "/about", "/about-us",
                      "/terms", "/privacy", "/legal",
                      "/terms-and-conditions", "/privacy-policy")

_host_lock = threading.Lock()
_host_next = {}
_host_fail = Counter()
_robots = {}
_robots_lock = threading.Lock()
_dns_cache = {}
_dns_lock = threading.Lock()


def _session():
    import requests
    s = requests.Session()
    s.headers.update({
        "User-Agent": UA,
        "Accept": "text/html,application/xhtml+xml",
        "Accept-Language": "en-GB,en;q=0.9",
    })
    return s


_local = threading.local()


def sess():
    if not hasattr(_local, "s"):
        _local.s = _session()
    return _local.s


def resolves(host):
    with _dns_lock:
        if host in _dns_cache:
            return _dns_cache[host]
    ok = True
    try:
        socket.getaddrinfo(host, None)
    except Exception:  # noqa
        ok = False
    with _dns_lock:
        _dns_cache[host] = ok
    return ok


def throttle(host):
    """One request per second per host, whatever the worker layout."""
    while True:
        with _host_lock:
            now = time.time()
            nxt = _host_next.get(host, 0)
            if now >= nxt:
                _host_next[host] = now + PER_HOST_DELAY
                return
            wait = nxt - now
        time.sleep(min(wait, PER_HOST_DELAY))


def robots_ok(url):
    p = urllib.parse.urlsplit(url)
    key = p.scheme + "://" + p.netloc
    with _robots_lock:
        rp = _robots.get(key)
    if rp is None:
        rp = urllib.robotparser.RobotFileParser()
        rp.set_url(key + "/robots.txt")
        try:
            throttle(p.netloc)
            r = sess().get(key + "/robots.txt", timeout=TIMEOUT,
                           allow_redirects=True)
            if r.status_code == 200 and len(r.content) < 500_000:
                rp.parse(r.text.splitlines())
            else:
                rp.parse([])
        except Exception:  # noqa
            rp.parse([])
        with _robots_lock:
            _robots[key] = rp
    try:
        return rp.can_fetch(UA, url)
    except Exception:  # noqa
        return True


def fetch(url):
    """Return (final_url, text) or (None, reason)."""
    host = urllib.parse.urlsplit(url).netloc
    if _host_fail[host] >= MAX_HOST_FAILURES:
        return None, "host-failed"
    if not robots_ok(url):
        return None, "robots"
    throttle(host)
    try:
        r = sess().get(url, timeout=TIMEOUT, allow_redirects=True, stream=True)
        ctype = (r.headers.get("content-type") or "").lower()
        if r.status_code != 200:
            _host_fail[host] += 1
            return None, "http-%d" % r.status_code
        if "html" not in ctype and "text" not in ctype:
            return None, "not-html"
        body = b""
        for chunk in r.iter_content(65536):
            body += chunk
            if len(body) > MAX_BYTES:
                break
        r.close()
    except Exception as e:  # noqa
        _host_fail[host] += 1
        return None, "error:" + type(e).__name__
    enc = r.encoding or "utf-8"
    try:
        raw = body.decode(enc, "ignore")
    except Exception:  # noqa
        raw = body.decode("utf-8", "ignore")
    final = r.url
    if is_blocked_host(urllib.parse.urlsplit(final).netloc):
        return None, "blocked-host"
    return final, raw


def legal_links(base_url, raw_html):
    """Footer and nav links that a UK company puts its details behind."""
    out = []
    base = urllib.parse.urlsplit(base_url)
    for m in re.finditer(r'(?is)<a\b[^>]*href=["\']([^"\']+)["\'][^>]*>(.*?)</a>',
                         raw_html or ""):
        href, label = m.group(1), page_text(m.group(2)).lower()
        if href.startswith(("mailto:", "tel:", "#", "javascript:")):
            continue
        u = urllib.parse.urljoin(base_url, href)
        p = urllib.parse.urlsplit(u)
        if p.netloc != base.netloc or p.scheme not in ("http", "https"):
            continue
        blob = (p.path + " " + label).lower()
        if any(h in blob for h in LEGAL_HINTS):
            u = urllib.parse.urlunsplit((p.scheme, p.netloc, p.path, p.query, ""))
            if u not in out and u.rstrip("/") != base_url.rstrip("/"):
                out.append(u)
        if len(out) >= MAX_LEGAL_PAGES * 3:
            break
    return out[:MAX_LEGAL_PAGES]


def check_company(c, seeds):
    """Try every candidate for one company. First proof wins."""
    crn, name, postcode = c["crn"], c["name"], c.get("postcode")
    reasons = Counter()

    candidates = []
    for s in seeds:
        candidates.append((s["url"], s.get("source", "seed")))
    if not c.get("candidatesOnly"):
        for host in guess_domains(name, c.get("altNames", ()), limit=MAX_GUESSES):
            candidates.append(("https://" + host + "/", "domain-guess"))
    for url in c.get("searchCandidates", ()):
        candidates.append((url, "search"))

    seen_hosts = set()
    tried = 0
    for url, source in candidates:
        p = urllib.parse.urlsplit(url)
        host = p.netloc.lower()
        if not host or host in seen_hosts:
            continue
        seen_hosts.add(host)
        if is_blocked_host(host):
            reasons["blocked-host"] += 1
            continue
        target = None
        if resolves(host):
            target = url
        elif not host.startswith("www.") and resolves("www." + host):
            target = urllib.parse.urlunsplit(
                (p.scheme or "https", "www." + host, p.path or "/", p.query, ""))
        if not target:
            reasons["no-dns"] += 1
            continue
        tried += 1

        final, raw = fetch(target)
        if final is None:
            if raw == "error:SSLError" and target.startswith("https://"):
                final, raw = fetch("http://" + target[len("https://"):])
            if final is None:
                reasons[raw] += 1
                continue
        # A search result is only accepted on the registration number. The
        # name-postcode rule stays available to register-published seeds and
        # to name-derived domain guesses, where the page cannot belong to a
        # third party writing about this company.
        allow_np = source != "search"

        def _verify(t):
            return verify(t, crn, name, postcode, allow_name_postcode=allow_np)

        text = page_text(raw)
        matched, evidence = _verify(text)
        pages = [(final, raw, text)]
        if not matched:
            links = legal_links(final, raw)
            for u in STATIC_LEGAL_PATHS:
                cand = urllib.parse.urljoin(final, u)
                if cand not in links and len(links) < MAX_LEGAL_PAGES:
                    links.append(cand)
            for lu in links[:MAX_LEGAL_PAGES]:
                lf, lraw = fetch(lu)
                if lf is None:
                    continue
                ltext = page_text(lraw)
                matched, evidence = _verify(ltext)
                if matched:
                    pages.append((lf, lraw, ltext))
                    break
        if matched:
            home = pages[0]
            evidence_url = pages[-1][0]
            # Publish the bare domain, except where the organisation lives on
            # a named path of a shared host (a local Age UK sits at
            # ageuk.org.uk/<place>/); collapsing that to the root would credit
            # the national body's site to a local company.
            p0 = urllib.parse.urlsplit(home[0])
            seg = (p0.path or "/").strip("/").split("/")[0]
            if seg and path_segment_names(seg, name, c.get("altNames", ())):
                pub_url = urllib.parse.urlunsplit(
                    (p0.scheme, p0.netloc, "/" + seg + "/", "", ""))
            else:
                pub_url = _site_root(home[0])
            home_text = home[2]
            return {
                "crn": crn,
                "name": name,
                "url": pub_url,
                "matchedOn": matched,
                "evidence": evidence,
                "evidenceUrl": evidence_url,
                "landingUrl": home[0],
                "candidateSource": source,
                "checkedAt": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
                "homepageTextRef": "website_text/%s.txt" % crn,
                "_homepageText": home[2][:MAX_TEXT],
            }, reasons, tried
        reasons["no-match"] += 1
    return None, reasons, tried


def _site_root(url):
    p = urllib.parse.urlsplit(url)
    return urllib.parse.urlunsplit((p.scheme, p.netloc, "/", "", ""))


# --- candidate inputs ------------------------------------------------------

def load_alt_names():
    """Trading names councils pay, keyed by company number. A supplier is
    often paid under a trading name that makes a better domain guess than the
    registered one."""
    alt = {}
    try:
        resolved = json.loads((PROC / "pound.json").read_text())["resolved"]
        uni = json.loads((PROC / "supplier_universe.json").read_text())["universe"]
    except Exception as e:  # noqa
        log(f"alt names unavailable: {e}")
        return alt
    for u in uni:
        r = resolved.get(u["key"])
        if r and r.get("crn"):
            for n in u.get("names", [])[:3]:
                alt.setdefault(r["crn"], [])
                if n not in alt[r["crn"]] and len(alt[r["crn"]]) < 3:
                    alt[r["crn"]].append(n)
    return alt


def brave_candidates(name, town):
    """Optional search-API tier. Off unless BRAVE_API_KEY is set. Free tier is
    2,000 queries a month, which covers a monthly re-run of this set."""
    key = os.environ.get("BRAVE_API_KEY")
    if not key:
        return []
    q = " ".join(name_tokens(name)) + (" " + town if town else "")
    try:
        throttle("api.search.brave.com")
        r = sess().get("https://api.search.brave.com/res/v1/web/search",
                       params={"q": q, "count": 5, "country": "GB"},
                       headers={"X-Subscription-Token": key,
                                "Accept": "application/json"},
                       timeout=TIMEOUT)
        if r.status_code != 200:
            return []
        return [it["url"] for it in (r.json().get("web", {}).get("results") or [])
                if it.get("url")][:5]
    except Exception as e:  # noqa
        log(f"brave query failed: {e}")
        return []


def load_targets(source, only, limit):
    master = {}
    with gzip.open(PROC / "master.jsonl.gz", "rt") as f:
        for line in f:
            r = json.loads(line)
            master[r["crn"]] = r
    if source == "master":
        crns = [c for c, m in master.items() if m.get("status") == "Active"]
    else:
        idx = json.loads(
            (ROOT / "public/data/biz-companies-index.json").read_text())["companies"]
        crns = [c["crn"] for c in idx]
    if only:
        want = {x.strip() for x in only.split(",")}
        crns = [c for c in crns if c in want]
    crns = sorted(set(crns))
    if limit:
        crns = crns[:limit]
    alt = load_alt_names()
    out = []
    for crn in crns:
        m = master.get(crn)
        if not m:
            continue
        out.append({"crn": crn, "name": m["name"], "postcode": m.get("postcode"),
                    "lad": m.get("lad"), "altNames": alt.get(crn, [])})
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--limit", type=int, default=0)
    ap.add_argument("--only", default="")
    ap.add_argument("--refresh", action="store_true",
                    help="re-check companies already verified")
    ap.add_argument("--workers", type=int, default=12)
    ap.add_argument("--source", default="index", choices=("index", "master"))
    ap.add_argument("--out", default=str(OUT_JSONL))
    ap.add_argument("--candidates", default="",
                    help="JSON file of {crn: [url, ...]} extra candidates, for "
                         "feeding results from any search tool through the "
                         "same verification rule")
    ap.add_argument("--candidates-only", action="store_true",
                    help="skip domain guessing; use seeds and --candidates "
                         "only. For a top-up pass over companies whose name "
                         "permutations have already been tried and failed.")
    args = ap.parse_args()

    out_path = Path(args.out)
    TEXT_DIR.mkdir(parents=True, exist_ok=True)

    seeds_file = PROC / "website_seeds.json"
    seeds = {}
    if seeds_file.exists():
        seeds = json.loads(seeds_file.read_text()).get("seeds", {})
    log(f"seeds loaded for {len(seeds)} companies")

    existing = {}
    if out_path.exists():
        for line in out_path.open():
            try:
                r = json.loads(line)
                existing[r["crn"]] = r
            except Exception:  # noqa
                pass
    log(f"{len(existing)} previously verified")

    targets = load_targets(args.source, args.only, args.limit)
    target_set_size = len(targets)
    if not args.refresh:
        targets = [t for t in targets if t["crn"] not in existing]
    log(f"{len(targets)} companies to check, {args.workers} workers")

    # The checkpoint rewrites the whole file, so widen it on big runs (the
    # full-register extension is 100k+ companies, where every-50 would be
    # quadratic).
    checkpoint = 50 if len(targets) <= 5000 else 1000

    reasons = Counter()
    results = dict(existing) if not args.refresh else {}
    done = [0]
    tried_total = [0]
    lock = threading.Lock()
    t0 = time.time()

    extra = {}
    if args.candidates:
        extra = json.loads(Path(args.candidates).read_text())
        log(f"extra candidates supplied for {len(extra)} companies")

    def work(c):
        c["searchCandidates"] = list(extra.get(c["crn"], []))
        c["candidatesOnly"] = args.candidates_only
        if os.environ.get("BRAVE_API_KEY"):
            c["searchCandidates"] += brave_candidates(c["name"], c.get("lad"))
        try:
            hit, rs, tried = check_company(c, seeds.get(c["crn"], []))
        except Exception as e:  # noqa
            log(f"{c['crn']} crashed: {type(e).__name__}: {e}")
            return
        with lock:
            reasons.update(rs)
            tried_total[0] += tried
            done[0] += 1
            if hit:
                txt = hit.pop("_homepageText")
                (TEXT_DIR / f"{hit['crn']}.txt").write_text(txt)
                results[hit["crn"]] = hit
                log(f"MATCH {hit['crn']} {hit['matchedOn']:>13} {hit['url']}")
            if done[0] % checkpoint == 0:
                el = time.time() - t0
                log(f"  {done[0]}/{len(targets)} checked, {len(results)} verified, "
                    f"{el/60:.1f} min, {tried_total[0]} sites fetched")
                _write(out_path, results)

    from concurrent.futures import ThreadPoolExecutor
    with ThreadPoolExecutor(max_workers=args.workers) as ex:
        list(ex.map(work, targets))

    _write(out_path, results)
    elapsed = round(time.time() - t0, 1)
    by_method = Counter(r["matchedOn"] for r in results.values())
    by_source = Counter(r.get("candidateSource") for r in results.values())
    attempted = len(targets)
    summary = {
        "$meta": {
            "generated": datetime.now(timezone.utc).strftime("%Y-%m-%d"),
            "rule": "A website is published only when the site proves the "
                    "match: the company registration number on the page, or "
                    "the exact registered name with the registered-office "
                    "postcode. Everything else is rejected.",
            "userAgent": UA,
        },
        "attempted": attempted,
        "targetSet": args.source,
        "targetSetSize": target_set_size,
        "verified": len(results),
        # Rate is against the whole target set, not just the companies checked
        # on this pass. A top-up run checks only the ones still unmatched.
        "matchRatePct": (round(100.0 * len(results) / target_set_size, 1)
                         if target_set_size else None),
        "byMethod": dict(by_method),
        "byCandidateSource": dict(by_source),
        "sitesFetched": tried_total[0],
        "rejectReasons": dict(reasons.most_common()),
        "elapsedSeconds": elapsed,
    }
    OUT_SUMMARY.write_text(json.dumps(summary, indent=1, ensure_ascii=False))
    log(f"verified {len(results)}/{target_set_size} "
        f"({summary['matchRatePct']}%) in {elapsed/60:.1f} min")
    log(f"by method: {dict(by_method)}")
    log(f"top rejects: {dict(reasons.most_common(8))}")


def _write(path, results):
    tmp = path.with_suffix(".part")
    with tmp.open("w") as f:
        for crn in sorted(results):
            r = dict(results[crn])
            r.pop("_homepageText", None)
            f.write(json.dumps(r, ensure_ascii=False) + "\n")
    tmp.rename(path)


if __name__ == "__main__":
    main()
