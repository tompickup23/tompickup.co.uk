#!/usr/bin/env python3
"""
FixMyStreet ETL — Fetches recent street reports via the Open311 API.

Data source: FixMyStreet Open311 API (mySociety)
API: https://www.fixmystreet.com/open311/v2/
Scope: Burnley Borough (MaPit ID 2371) + Lancashire CC (MaPit ID 2230)

Reports include: potholes, fly-tipping, street lighting, graffiti, abandoned vehicles, etc.
No authentication required.

Output: public/data/fixmystreet.json
Schedule: Every 12 hours via GitHub Actions
"""

import json
import sys
import os
import time
from datetime import datetime, timezone, timedelta
from urllib.request import urlopen, Request
from urllib.parse import urlencode
from urllib.error import URLError, HTTPError

# --- Config ---
BASE_URL = "https://www.fixmystreet.com/open311/v2"

# MaPit IDs for Lancashire councils (pipe-separated for multi-council queries)
COUNCILS = {
    "burnley": {"mapit_id": "2371", "name": "Burnley Borough Council"},
    "lancashire": {"mapit_id": "2230", "name": "Lancashire County Council"},
}

# Burnley area bounding box (approx) for location-based fallback
BURNLEY_LAT = 53.789
BURNLEY_LNG = -2.248

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
DEFAULT_OUTPUT = os.path.join(SCRIPT_DIR, "..", "public", "data", "fixmystreet.json")

# How far back to look (days)
LOOKBACK_DAYS = 90


def fetch_reports(council_key: str, status: str = "open", max_requests: int = 200) -> list:
    """Fetch reports from FixMyStreet Open311 API for a given council."""
    council = COUNCILS[council_key]

    # Calculate date range
    end_date = datetime.now(timezone.utc)
    start_date = end_date - timedelta(days=LOOKBACK_DAYS)

    params = {
        "jurisdiction_id": "fixmystreet",
        "agency_responsible": council["mapit_id"],
        "status": status,
        "start_date": start_date.strftime("%Y-%m-%dT00:00:00+00:00"),
        "end_date": end_date.strftime("%Y-%m-%dT23:59:59+00:00"),
        "max_requests": max_requests,
    }

    url = f"{BASE_URL}/requests.json?{urlencode(params)}"
    req = Request(url, headers={
        "User-Agent": "TomPickup-FixMyStreet-ETL/1.0",
        "Accept": "application/json",
    })

    try:
        with urlopen(req, timeout=30) as resp:
            data = json.loads(resp.read().decode("utf-8"))
    except (URLError, HTTPError) as e:
        print(f"  ERROR fetching {council_key} ({status}): {e}", file=sys.stderr)
        return []

    # API wraps results in {"service_requests": [...]}
    if isinstance(data, dict) and "service_requests" in data:
        reports = data["service_requests"]
        print(f"  {council_key} ({status}): {len(reports)} reports")
        return reports
    elif isinstance(data, list):
        print(f"  {council_key} ({status}): {len(data)} reports")
        return data
    else:
        print(f"  {council_key} ({status}): unexpected response format", file=sys.stderr)
        return []


def parse_report(report: dict) -> dict:
    """Convert Open311 report to clean record."""
    return {
        "id": report.get("service_request_id", ""),
        "title": report.get("title", ""),
        "description": (report.get("description") or "")[:300],
        "category": report.get("service_name", ""),
        "service_code": report.get("service_code", ""),
        "status": report.get("status", ""),
        "lat": report.get("lat"),
        "lng": report.get("long"),
        "photo": report.get("media_url", ""),
        "reported": report.get("requested_datetime", ""),
        "updated": report.get("updated_datetime", ""),
        "interface": report.get("interface_used", ""),
    }


def categorize_reports(records: list) -> dict:
    """Group reports by category for summary stats."""
    cats = {}
    for r in records:
        cat = r["category"] or "Other"
        cats[cat] = cats.get(cat, 0) + 1
    return dict(sorted(cats.items(), key=lambda x: -x[1]))


def assign_ward(record: dict, ward_boundaries: list) -> str:
    """Simple point-in-polygon to assign ward. Uses ray-casting algorithm."""
    lat, lng = record.get("lat"), record.get("lng")
    if not lat or not lng:
        return ""
    try:
        lat = float(lat)
        lng = float(lng)
    except (ValueError, TypeError):
        return ""

    for ward in ward_boundaries:
        coords = ward.get("coordinates", [])
        if not coords:
            continue
        # Simple ray-casting for polygon containment
        polygon = coords if isinstance(coords[0][0], (int, float)) else coords[0]
        n = len(polygon)
        inside = False
        j = n - 1
        for i in range(n):
            xi, yi = polygon[i]  # [lng, lat] in GeoJSON
            xj, yj = polygon[j]
            if ((yi > lat) != (yj > lat)) and (lng < (xj - xi) * (lat - yi) / (yj - yi) + xi):
                inside = not inside
            j = i
        if inside:
            return ward.get("name", "")
    return ""


def main():
    output_path = sys.argv[1] if len(sys.argv) > 1 else DEFAULT_OUTPUT

    print(f"FixMyStreet ETL — {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Fetching reports from Open311 API (last {LOOKBACK_DAYS} days)...")

    t0 = time.time()

    # Load ward boundaries for ward assignment
    wards_file = os.path.join(SCRIPT_DIR, "..", "src", "data", "burnley-wards.json")
    ward_boundaries = []
    try:
        with open(wards_file, "r") as f:
            wd = json.load(f)
        for slug, ward in wd.get("wards", {}).items():
            boundary = ward.get("boundary", {})
            if boundary.get("coordinates"):
                ward_boundaries.append({
                    "name": ward.get("name", ""),
                    "slug": slug,
                    "coordinates": boundary["coordinates"],
                })
    except Exception as e:
        print(f"  Warning: Could not load ward boundaries: {e}")

    all_reports = []

    # Fetch from both councils
    for council_key in COUNCILS:
        for status in ["open", "closed"]:
            reports = fetch_reports(council_key, status=status, max_requests=200)
            for r in reports:
                parsed = parse_report(r)
                parsed["council"] = council_key
                # Assign ward
                if ward_boundaries and parsed["lat"]:
                    parsed["ward"] = assign_ward(parsed, ward_boundaries)
                else:
                    parsed["ward"] = ""
                all_reports.append(parsed)

    # Deduplicate by ID
    seen = set()
    unique = []
    for r in all_reports:
        if r["id"] not in seen:
            seen.add(r["id"])
            unique.append(r)

    all_reports = unique
    print(f"  Total unique reports: {len(all_reports)}")

    # Sort by date (most recent first)
    all_reports.sort(key=lambda r: r.get("reported", ""), reverse=True)

    # Compute stats
    open_reports = [r for r in all_reports if r["status"] == "open"]
    closed_reports = [r for r in all_reports if r["status"] == "closed"]
    by_category = categorize_reports(all_reports)

    # By ward
    by_ward = {}
    for r in all_reports:
        w = r.get("ward") or "Unknown"
        by_ward[w] = by_ward.get(w, 0) + 1
    by_ward = dict(sorted(by_ward.items(), key=lambda x: -x[1]))

    output = {
        "meta": {
            "source": "FixMyStreet Open311 API",
            "api_url": BASE_URL,
            "lookback_days": LOOKBACK_DAYS,
            "generated": datetime.now(timezone.utc).isoformat(),
            "fetch_time_ms": int((time.time() - t0) * 1000),
        },
        "stats": {
            "total": len(all_reports),
            "open": len(open_reports),
            "closed": len(closed_reports),
            "by_category": by_category,
            "by_ward": by_ward,
        },
        "reports": all_reports,
    }

    os.makedirs(os.path.dirname(os.path.abspath(output_path)), exist_ok=True)

    with open(output_path, "w") as f:
        json.dump(output, f, separators=(",", ":"))

    size_kb = os.path.getsize(output_path) / 1024
    elapsed = time.time() - t0

    print(f"\nDone in {elapsed:.1f}s")
    print(f"Output: {output_path} ({size_kb:.1f} KB)")
    print(f"Stats: {len(all_reports)} total — {len(open_reports)} open, {len(closed_reports)} closed")
    print(f"Top categories: {', '.join(list(by_category.keys())[:5])}")


if __name__ == "__main__":
    main()
