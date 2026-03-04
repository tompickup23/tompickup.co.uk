#!/usr/bin/env python3
"""
Roadworks ETL — Fetches live roadworks data from LCC MARIO ArcGIS FeatureServer.

Data source: Lancashire County Council Road Works Point layer
API: https://services-eu1.arcgis.com/9MmxkLJT84uEwsJx/arcgis/rest/services/Road_Works_Point/FeatureServer/0

Scope: Burnley District — all current and planned roadworks
Filters: Works started + Planned works
Output: public/data/roadworks.json (consumed by Leaflet map on tompickup.co.uk)

Schedule: Every 2 hours via cron on vps-main
"""

import json
import sys
import os
import time
from datetime import datetime, timezone
from urllib.request import urlopen, Request
from urllib.parse import urlencode
from urllib.error import URLError, HTTPError

# --- Config ---
BASE_URL = "https://services-eu1.arcgis.com/9MmxkLJT84uEwsJx/arcgis/rest/services/Road_Works_Point/FeatureServer/0/query"

# Burnley District filter — covers all 15 borough wards
DISTRICT_FILTER = "townName = 'Burnley District (B)'"

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

# Output path — relative to script or absolute
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
DEFAULT_OUTPUT = os.path.join(SCRIPT_DIR, "..", "public", "data", "roadworks.json")

# Severity ordering for sorting
SEVERITY_ORDER = {"high": 0, "medium": 1, "low": 2}


def fetch_roadworks(works_state: str, max_records: int = 2000) -> list:
    """Fetch roadworks from ArcGIS FeatureServer for a given WorksState."""
    where = f"{DISTRICT_FILTER} and (WorksState = '{works_state}')"

    params = {
        "f": "json",
        "where": where,
        "outFields": ",".join(OUT_FIELDS),
        "returnGeometry": "true",
        "outSR": "4326",  # WGS84 lat/lng
        "resultRecordCount": max_records,
        "orderByFields": "linkName ASC",
    }

    url = f"{BASE_URL}?{urlencode(params)}"
    req = Request(url, headers={"User-Agent": "TomPickup-Roadworks-ETL/1.0"})

    try:
        with urlopen(req, timeout=30) as resp:
            data = json.loads(resp.read().decode("utf-8"))
    except (URLError, HTTPError) as e:
        print(f"  ERROR fetching {works_state}: {e}", file=sys.stderr)
        return []

    features = data.get("features", [])
    print(f"  {works_state}: {len(features)} records")
    return features


def parse_feature(feature: dict) -> dict:
    """Convert ArcGIS feature to clean roadworks record."""
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

    return {
        "id": attrs.get("OBJECTID"),
        "road": s("linkName"),
        "ward": ward,
        "area": area,
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
    """Compute summary statistics for the roadworks data."""
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

    return {
        "total": len(records),
        "works_started": len(started),
        "planned_works": len(planned),
        "by_operator": dict(sorted(operators.items(), key=lambda x: -x[1])),
        "by_severity": severities,
        "by_ward": dict(sorted(wards.items(), key=lambda x: -x[1])),
        "by_restriction": dict(sorted(restrictions.items(), key=lambda x: -x[1])),
    }


def main():
    output_path = sys.argv[1] if len(sys.argv) > 1 else DEFAULT_OUTPUT

    print(f"Roadworks ETL — {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Fetching Burnley District roadworks from LCC MARIO...")

    t0 = time.time()

    # Fetch both states
    started_features = fetch_roadworks("Works started")
    planned_features = fetch_roadworks("Planned works")

    all_features = started_features + planned_features
    print(f"  Total features: {len(all_features)}")

    # Parse into clean records
    records = [parse_feature(f) for f in all_features]

    # Sort: severity (high first), then road name
    records.sort(key=lambda r: (SEVERITY_ORDER.get(r["severity"], 9), r["road"]))

    # Compute stats
    stats = compute_stats(records)

    # Build output
    output = {
        "meta": {
            "source": "Lancashire County Council MARIO ArcGIS",
            "api_url": BASE_URL,
            "district": "Burnley District (B)",
            "generated": datetime.now(timezone.utc).isoformat(),
            "fetch_time_ms": int((time.time() - t0) * 1000),
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
    print(f"Operators: {', '.join(stats['by_operator'].keys())}")


if __name__ == "__main__":
    main()
