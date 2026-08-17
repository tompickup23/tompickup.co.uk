#!/usr/bin/env python3
"""fetch_lottery.py - National Lottery grants, full pull.

DCMS's nationallottery.dcms.gov.uk API, OGL v3.0 + Crown copyright (confirmed
14 Aug 2026, DATA-INTEGRITY s7.2). No key, unauthenticated JSON, paginated.

The location field is NOT a reliable recipient or delivery location: DCMS's
own About page says it is "usually" the benefit area, falling back to the
AWARDING BODY'S HQ where unknown, "often... London". Per-area totals built
from this file must carry that caption or not publish (DATA-INTEGRITY s3, s7.2
- binding). DCMS also states this is not official statistics and is not
manually validated.

Output: ~/observatory-data/raw/lottery_grants_full.jsonl (one grant per line,
streamed rather than held in memory: ~450MB across 693k+ rows).
"""
import json
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
from _common import RAW, UA, log
import requests

BASE = "https://nationallottery.dcms.gov.uk/api/v1/grants/"
OUT = RAW / "lottery_grants_full.jsonl"
LIMIT = 5000


def main():
    page = 1
    written = 0
    total = None
    with open(OUT, "w") as f:
        while True:
            r = requests.get(BASE, params={"limit": LIMIT, "page": page},
                              headers={"User-Agent": UA}, timeout=120)
            r.raise_for_status()
            d = r.json()
            if total is None:
                total = d["count"]
                log(f"total grants to pull: {total}, "
                    f"total_amount_awarded={d.get('total_amount_awarded')}")
            results = d.get("results", [])
            if not results:
                break
            for row in results:
                f.write(json.dumps(row, ensure_ascii=False) + "\n")
            written += len(results)
            if page % 10 == 0 or not d.get("next_page"):
                log(f"page {page}: {written}/{total} written")
            if not d.get("next_page"):
                break
            page += 1
            time.sleep(0.1)
    log(f"done: {written} grants written to {OUT}")


if __name__ == "__main__":
    main()
