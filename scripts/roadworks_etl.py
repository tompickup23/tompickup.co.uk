#!/usr/bin/env python3
"""
Roadworks ETL — Central Lancashire Highways System (under LCC).

Fetches live roadworks data from LCC MARIO ArcGIS FeatureServer for ALL 12
Lancashire districts in a single pass. Blackpool and Blackburn with Darwen
(unitaries) manage their own highways and are NOT in MARIO.

Data source: Lancashire County Council Road Works Point layer
API: https://services-eu1.arcgis.com/9MmxkLJT84uEwsJx/arcgis/rest/services/Road_Works_Point/FeatureServer/0

Scope: All Lancashire (12 districts)
Output: burnley-council/data/lancashire_cc/roadworks.json (consumed by tompickup.co.uk + AI DOGE)

Usage:
    python3 roadworks_etl.py                  # Default output path
    python3 roadworks_etl.py /path/out.json   # Custom output path
    python3 roadworks_etl.py --district burnley  # Single district only

Schedule: Every 2 hours via cron on vps-main
"""

import json
import sys
import os
import time
import argparse
from datetime import datetime, timezone
from urllib.request import urlopen, Request
from urllib.parse import urlencode
from urllib.error import URLError, HTTPError

# --- Config ---
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
CONFIG_PATH = os.path.join(SCRIPT_DIR, "highways_config.json")

# Load config
with open(CONFIG_PATH) as f:
    CONFIG = json.load(f)

BASE_URL = CONFIG["mario"]["base_url"]
MAX_RECORDS = CONFIG["mario"]["max_records"]

# All fields we want
OUT_FIELDS = [
    "OBJECTID", "linkName", "areaName", "townName",
    "ValidFrom", "ValidTo", "CreationTime",
    "Organisation", "WorksState", "WorksCategory",
    "Severity", "Duration", "Scale",
    "NetworkManagement", "ManagementType",
    "PublicComment", "NonPublicComment",
    "Direction", "ImpactDelays", "TrafficConstriction",
    "ReferenceNo", "PermitStatus", "Urgent",
    "ImgUrl", "OneNetworkURL",
    "CauseDesc", "CauseType", "Cause",
    "RoadMaintType", "Compliance", "Probability",
]

# Severity ordering for sorting
SEVERITY_ORDER = {"high": 0, "medium": 1, "low": 2}

# Reverse lookup: MARIO townName → council_id
TOWN_TO_COUNCIL = {}
for council_id, council_cfg in CONFIG["councils"].items():
    tn = council_cfg.get("mario_town_name")
    if tn:
        TOWN_TO_COUNCIL[tn] = council_id


def fetch_roadworks(works_state: str, district_filter: str = None) -> list:
    """Fetch roadworks from ArcGIS FeatureServer for a given WorksState.

    If district_filter is None, fetches ALL Lancashire districts.
    If district_filter is a townName string, filters to that district.
    """
    if district_filter:
        where = f"townName = '{district_filter}' and (WorksState = '{works_state}')"
    else:
        # All 12 districts — no townName filter needed, MARIO only has Lancashire data
        where = f"WorksState = '{works_state}'"

    params = {
        "f": "json",
        "where": where,
        "outFields": ",".join(OUT_FIELDS),
        "returnGeometry": "true",
        "outSR": "4326",
        "resultRecordCount": MAX_RECORDS,
        "orderByFields": "linkName ASC",
    }

    url = f"{BASE_URL}?{urlencode(params)}"
    req = Request(url, headers={"User-Agent": "TomPickup-Roadworks-ETL/2.0"})

    try:
        with urlopen(req, timeout=60) as resp:
            data = json.loads(resp.read().decode("utf-8"))
    except (URLError, HTTPError) as e:
        print(f"  ERROR fetching {works_state}: {e}", file=sys.stderr)
        return []

    features = data.get("features", [])
    print(f"  {works_state}: {len(features)} records")
    return features


def parse_feature(feature: dict) -> dict:
    """Convert ArcGIS feature to clean roadworks record with district field."""
    attrs = feature.get("attributes", {})
    geom = feature.get("geometry", {})

    # Parse epoch timestamps (milliseconds)
    def parse_ts(val):
        if val and isinstance(val, (int, float)):
            return datetime.fromtimestamp(val / 1000, tz=timezone.utc).isoformat()
        return None

    # Safe string getter
    def s(key):
        v = attrs.get(key)
        return v.strip() if isinstance(v, str) else ""

    # Extract ward name from areaName (e.g., "Rosegrove with Lowerhouse Ward")
    area = s("areaName")
    ward = area.replace(" Ward", "").strip() if area else ""

    # Map townName to council_id
    town_name = s("townName")
    district = TOWN_TO_COUNCIL.get(town_name, town_name)

    return {
        "id": attrs.get("OBJECTID"),
        "road": s("linkName"),
        "ward": ward,
        "area": area,
        "district": district,
        "district_raw": town_name,
        "lat": geom.get("y"),
        "lng": geom.get("x"),
        "start_date": parse_ts(attrs.get("ValidFrom")),
        "end_date": parse_ts(attrs.get("ValidTo")),
        "created": parse_ts(attrs.get("CreationTime")),
        "operator": s("Organisation"),
        "status": s("WorksState"),
        "category": s("WorksCategory"),
        "severity": s("Severity").lower(),
        "duration": s("Duration"),
        "scale": s("Scale"),
        "restrictions": s("NetworkManagement"),
        "management_type": s("ManagementType"),
        "description": s("PublicComment"),
        "direction": s("Direction"),
        "impact": s("ImpactDelays"),
        "traffic_constriction": s("TrafficConstriction"),
        "reference": s("ReferenceNo"),
        "permit_status": s("PermitStatus"),
        "urgent": s("Urgent").lower() == "true",
        "image_url": s("ImgUrl"),
        "one_network_url": s("OneNetworkURL"),
        "cause": s("CauseDesc"),
        "cause_type": s("CauseType"),
        "road_type": s("RoadMaintType"),
    }


def compute_stats(records: list) -> dict:
    """Compute summary statistics for the roadworks data, with per-district breakdown."""
    started = [r for r in records if r["status"] == "Works started"]
    planned = [r for r in records if r["status"] == "Planned works"]

    # Count by operator
    operators = {}
    for r in records:
        op = r["operator"] or "Unknown"
        operators[op] = operators.get(op, 0) + 1

    # Count by severity
    severities = {}
    for r in records:
        sev = r["severity"] or "unknown"
        severities[sev] = severities.get(sev, 0) + 1

    # Count by ward
    wards = {}
    for r in records:
        w = r["ward"] or "Unknown"
        wards[w] = wards.get(w, 0) + 1

    # Count by restriction type
    restrictions = {}
    for r in records:
        rest = r["restrictions"] or "none"
        restrictions[rest] = restrictions.get(rest, 0) + 1

    # Per-district breakdown
    by_district = {}
    for r in records:
        d = r["district"] or "unknown"
        if d not in by_district:
            by_district[d] = {"total": 0, "works_started": 0, "planned_works": 0,
                              "high": 0, "medium": 0, "low": 0}
        by_district[d]["total"] += 1
        if r["status"] == "Works started":
            by_district[d]["works_started"] += 1
        elif r["status"] == "Planned works":
            by_district[d]["planned_works"] += 1
        sev = r["severity"] or "unknown"
        if sev in by_district[d]:
            by_district[d][sev] += 1

    return {
        "total": len(records),
        "works_started": len(started),
        "planned_works": len(planned),
        "district_count": len(by_district),
        "by_district": dict(sorted(by_district.items(), key=lambda x: -x[1]["total"])),
        "by_operator": dict(sorted(operators.items(), key=lambda x: -x[1])),
        "by_severity": severities,
        "by_ward": dict(sorted(wards.items(), key=lambda x: -x[1])),
        "by_restriction": dict(sorted(restrictions.items(), key=lambda x: -x[1])),
    }


def main():
    parser = argparse.ArgumentParser(description="Lancashire Roadworks ETL — Central LCC System")
    parser.add_argument("output", nargs="?", default=None,
                        help="Output file path (default: burnley-council/data/lancashire_cc/roadworks.json)")
    parser.add_argument("--district", type=str, default=None,
                        help="Filter to a single district (e.g. burnley, hyndburn)")
    args = parser.parse_args()

    # Resolve output path
    if args.output:
        output_path = args.output
    else:
        # Default: AI DOGE lancashire_cc data directory
        ai_doge_dir = os.path.join(SCRIPT_DIR, "..", "..", "clawd", "burnley-council", "data", "lancashire_cc")
        if os.path.isdir(ai_doge_dir):
            output_path = os.path.join(ai_doge_dir, "roadworks.json")
        else:
            # Fallback to tompickup.co.uk public/data
            output_path = os.path.join(SCRIPT_DIR, "..", "public", "data", "roadworks.json")

    # Determine district filter
    district_filter = None
    scope_label = "All Lancashire (12 districts)"
    if args.district:
        council_cfg = CONFIG["councils"].get(args.district)
        if not council_cfg:
            print(f"ERROR: Unknown district '{args.district}'. "
                  f"Available: {', '.join(CONFIG['councils'].keys())}", file=sys.stderr)
            sys.exit(1)
        district_filter = council_cfg["mario_town_name"]
        scope_label = f"{args.district.title()} only ({district_filter})"

    print(f"Roadworks ETL v2 — {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Scope: {scope_label}")
    print(f"Fetching from LCC MARIO ArcGIS...")

    t0 = time.time()

    # Fetch both states
    started_features = fetch_roadworks("Works started", district_filter)
    planned_features = fetch_roadworks("Planned works", district_filter)

    all_features = started_features + planned_features
    print(f"  Total features: {len(all_features)}")

    # Parse into clean records
    records = [parse_feature(f) for f in all_features]

    # Deduplicate — same ReferenceNo means same works notice.
    # Prefer "Works started" over "Planned works" (more current).
    # Fallback: dedup on (road, operator, start_date, end_date) for entries without ReferenceNo.
    seen_refs = {}
    seen_keys = {}
    deduped = []
    dupe_count = 0
    for r in records:
        ref = r.get("reference", "").strip()
        if ref:
            if ref in seen_refs:
                dupe_count += 1
                existing = seen_refs[ref]
                if r["status"] == "Works started" and existing["status"] != "Works started":
                    deduped = [x if x is not existing else r for x in deduped]
                    seen_refs[ref] = r
                continue
            seen_refs[ref] = r
            deduped.append(r)
        else:
            key = (r["road"], r["operator"], r.get("start_date", ""), r.get("end_date", ""))
            if key in seen_keys:
                dupe_count += 1
                continue
            seen_keys[key] = r
            deduped.append(r)

    if dupe_count:
        print(f"  Deduplicated: removed {dupe_count} duplicate entries")
    records = deduped

    # Sort: severity (high first), then district, then road name
    records.sort(key=lambda r: (SEVERITY_ORDER.get(r["severity"], 9), r["district"], r["road"]))

    # Compute stats
    stats = compute_stats(records)

    # Build output
    output = {
        "meta": {
            "source": "Lancashire County Council MARIO ArcGIS",
            "api_url": BASE_URL,
            "scope": scope_label,
            "districts_covered": list(set(r["district"] for r in records)),
            "generated": datetime.now(timezone.utc).isoformat(),
            "fetch_time_ms": int((time.time() - t0) * 1000),
            "version": "2.0",
        },
        "stats": stats,
        "roadworks": records,
    }

    # Ensure output directory exists
    os.makedirs(os.path.dirname(os.path.abspath(output_path)), exist_ok=True)

    with open(output_path, "w") as f:
        json.dump(output, f, separators=(",", ":"))

    size_kb = os.path.getsize(output_path) / 1024
    elapsed = time.time() - t0

    print(f"\nDone in {elapsed:.1f}s")
    print(f"Output: {output_path} ({size_kb:.1f} KB)")
    print(f"Stats: {stats['total']} total — {stats['works_started']} started, {stats['planned_works']} planned")
    print(f"Districts: {stats['district_count']}")
    for d, d_stats in stats["by_district"].items():
        print(f"  {d}: {d_stats['total']} works ({d_stats['works_started']} started, {d_stats['planned_works']} planned)")
    print(f"Operators: {', '.join(list(stats['by_operator'].keys())[:10])}")


if __name__ == "__main__":
    main()
