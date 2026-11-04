#!/usr/bin/env python3
"""Tell IndexNow which URLs changed.

Google does not consume IndexNow, but Bing does, and Bing's index is the candidate
pool ChatGPT Search and Copilot draw citations from. For a site that publishes
intermittently this is the difference between a piece being retrievable the same
day and waiting on a crawl cycle.

Usage:
    python3 scripts/indexnow_ping.py https://tompickup.co.uk/news/some-article/ [...]
    python3 scripts/indexnow_ping.py --recent 3     # news articles from the last N days
    python3 scripts/indexnow_ping.py --sitemap      # every URL in the live sitemap
"""
import argparse
import json
import re
import sys
import urllib.request
from datetime import datetime, timedelta, timezone
from pathlib import Path

HOST = "tompickup.co.uk"
KEY = "6db086fa92a744fdae08491a16603960"
ENDPOINT = "https://api.indexnow.org/IndexNow"
NEWS_DIR = Path(__file__).resolve().parent.parent / "src" / "content" / "news"
SITEMAP_INDEX = f"https://{HOST}/sitemap-index.xml"
LOC_RE = re.compile(r"<loc>([^<]+)</loc>")


def recent_articles(days: int) -> list[str]:
    cutoff = datetime.now(timezone.utc) - timedelta(days=days)
    urls = []
    for md in sorted(NEWS_DIR.glob("*.md")):
        if md.name.startswith("_"):
            continue
        head = md.read_text(encoding="utf-8").split("---")
        if len(head) < 3:
            continue
        for key in ("updated", "date"):
            m = re.search(rf"^{key}:\s*(.+)$", head[1], re.M)
            if not m:
                continue
            raw = m.group(1).strip().strip("\"'")
            try:
                when = datetime.fromisoformat(raw.replace("Z", "+00:00"))
            except ValueError:
                continue
            if when.tzinfo is None:
                when = when.replace(tzinfo=timezone.utc)
            if when >= cutoff:
                urls.append(f"https://{HOST}/news/{md.stem}/")
            break
    return urls


def fetch_text(url: str, timeout: int = 60) -> str:
    req = urllib.request.Request(url, headers={"User-Agent": "tompickup-indexnow/1"})
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return resp.read().decode("utf-8", errors="replace")


def sitemap_urls() -> list[str]:
    """Every page <loc> under the live sitemap index (post-deploy surface)."""
    index_xml = fetch_text(SITEMAP_INDEX, timeout=30)
    child_maps = [
        loc.replace("&amp;", "&")
        for loc in LOC_RE.findall(index_xml)
        if loc.rstrip("/").endswith(".xml")
    ]
    if not child_maps:
        child_maps = [f"https://{HOST}/sitemap-0.xml"]
    urls: list[str] = []
    for sitemap_url in child_maps:
        body = fetch_text(sitemap_url)
        for loc in LOC_RE.findall(body):
            url = loc.replace("&amp;", "&")
            if url.rstrip("/").endswith(".xml"):
                continue
            urls.append(url)
    return urls


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("urls", nargs="*")
    ap.add_argument("--recent", type=int, metavar="DAYS",
                    help="submit news articles published or updated in the last N days")
    ap.add_argument("--sitemap", action="store_true",
                    help="submit every URL in the live sitemap (used after deploy)")
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    urls = list(args.urls)
    if args.recent:
        urls += recent_articles(args.recent)
    if args.sitemap:
        urls += sitemap_urls()
    urls = sorted(set(urls))

    if not urls:
        print("Nothing to submit.")
        return 0

    bad = [u for u in urls if not u.startswith(f"https://{HOST}/")]
    if bad:
        print(f"Refusing to submit URLs on another host: {bad}", file=sys.stderr)
        return 1

    payload = {
        "host": HOST,
        "key": KEY,
        "keyLocation": f"https://{HOST}/{KEY}.txt",
        "urlList": urls,
    }

    print(f"Submitting {len(urls)} URL(s):")
    for u in urls:
        print(f"  {u}")
    if args.dry_run:
        return 0

    req = urllib.request.Request(
        ENDPOINT,
        data=json.dumps(payload).encode(),
        headers={"Content-Type": "application/json; charset=utf-8"},
    )
    with urllib.request.urlopen(req, timeout=30) as resp:
        # 200 accepted, 202 accepted but key still validating. Both are success.
        print(f"IndexNow responded HTTP {resp.status}")
        return 0 if resp.status in (200, 202) else 1


if __name__ == "__main__":
    raise SystemExit(main())
