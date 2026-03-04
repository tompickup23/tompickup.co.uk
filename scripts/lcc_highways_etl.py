#!/usr/bin/env python3
"""
LCC Highways ETL — Fetches highway defects, road status, street lighting from LCC ArcGIS.

Data sources (all LCC MARIO ArcGIS FeatureServer, no auth required):
- Highway Defects: potholes, surface damage, drainage
- Road Status: live road conditions
- Street Lighting: faults/outages
- Highway Surfacing: planned resurfacing schemes
- Highway Schemes: capital highway schemes

Scope: Burnley area (bounding box filter)
Output: public/data/lcc_highways.json
Schedule: Every 12 hours via GitHub Actions
"""

import json
import sys
import os
import time
from datetime import datetime, timezone
from urllib.request import urlopen, Request
from urllib.parse import urlencode
from urllib.error import URLError, HTTPError

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
DEFAULT_OUTPUT = os.path.join(SCRIPT_DIR, "..", "public", "data", "lcc_highways.json")

# Burnley District bounding box (WGS84)
BURNLEY_BBOX = {
    "xmin": -2.35,
    "ymin": 53.72,
    "xmax": -2.10,
    "ymax": 53.82,
}

# ArcGIS service endpoints (all JSON, no auth)
SERVICES = {
    # Note: gis.lancashire.gov.uk endpoints are firewall-restricted (connection refused).
    # Only services-eu1.arcgis.com (hosted ArcGIS Online) endpoints are publicly accessible.
    "road_status": {
        "url": "https://services-eu1.arcgis.com/9MmxkLJT84uEwsJx/arcgis/rest/services/Road_Status/FeatureServer/0/query",
        "name": "Road Status",
        "desc": "Live road conditions and closures across Lancashire",
    },
    "road_works_line": {
        "url": "https://services-eu1.arcgis.com/9MmxkLJT84uEwsJx/arcgis/rest/services/Road_Works/FeatureServer/0/query",
        "name": "Road Works (lines)",
        "desc": "Road works polylines showing affected road stretches",
    },
    "planning_county": {
        "url": "https://services-eu1.arcgis.com/9MmxkLJT84uEwsJx/arcgis/rest/services/Planning_Applications_County_Council/FeatureServer/0/query",
        "name": "LCC Planning Applications",
        "desc": "County-level planning applications (minerals, waste, schools, highways)",
    },
}


def fetch_arcgis(service_key: str, max_records: int = 2000) -> list:
    """Fetch features from an ArcGIS FeatureServer/MapServer with bbox filter."""
    svc = SERVICES[service_key]
    url = svc["url"]

    # Build envelope geometry for bbox filter
    envelope = json.dumps({
        "xmin": BURNLEY_BBOX["xmin"],
        "ymin": BURNLEY_BBOX["ymin"],
        "xmax": BURNLEY_BBOX["xmax"],
        "ymax": BURNLEY_BBOX["ymax"],
        "spatialReference": {"wkid": 4326},
    })

    params = {
        "f": "json",
        "where": "1=1",
        "outFields": "*",
        "returnGeometry": "true",
        "outSR": "4326",
        "geometry": envelope,
        "geometryType": "esriGeometryEnvelope",
        "spatialRel": "esriSpatialRelIntersects",
        "inSR": "4326",
        "resultRecordCount": max_records,
    }

    full_url = f"{url}?{urlencode(params)}"
    req = Request(full_url, headers={"User-Agent": "TomPickup-LCC-ETL/1.0"})

    try:
        with urlopen(req, timeout=45) as resp:
            data = json.loads(resp.read().decode("utf-8"))
    except (URLError, HTTPError) as e:
        print(f"  ERROR fetching {service_key}: {e}", file=sys.stderr)
        return []

    if "error" in data:
        err = data["error"]
        print(f"  ArcGIS error for {service_key}: {err.get('message', 'Unknown')}", file=sys.stderr)
        # Try fallback without geometry filter
        params_fallback = {
            "f": "json",
            "where": "1=1",
            "outFields": "*",
            "returnGeometry": "true",
            "outSR": "4326",
            "resultRecordCount": min(max_records, 500),
        }
        try:
            full_url = f"{url}?{urlencode(params_fallback)}"
            req = Request(full_url, headers={"User-Agent": "TomPickup-LCC-ETL/1.0"})
            with urlopen(req, timeout=45) as resp:
                data = json.loads(resp.read().decode("utf-8"))
        except Exception as e2:
            print(f"  Fallback also failed for {service_key}: {e2}", file=sys.stderr)
            return []

    features = data.get("features", [])
    print(f"  {svc['name']}: {len(features)} features")
    return features


def parse_timestamp(val):
    """Parse ArcGIS epoch timestamp (milliseconds) to ISO string."""
    if val and isinstance(val, (int, float)):
        return datetime.fromtimestamp(val / 1000, tz=timezone.utc).isoformat()
    return None


def parse_highway_defect(feature: dict) -> dict:
    """Parse a highway defect feature."""
    attrs = feature.get("attributes", {})
    geom = feature.get("geometry", {})

    return {
        "type": "defect",
        "id": attrs.get("OBJECTID"),
        "category": attrs.get("Category", attrs.get("CATEGORY", "")),
        "subcategory": attrs.get("SubCategory", attrs.get("SUBCATEGORY", "")),
        "status": attrs.get("Status", attrs.get("STATUS", "")),
        "priority": attrs.get("Priority", attrs.get("PRIORITY", "")),
        "road": attrs.get("RoadName", attrs.get("ROAD_NAME", attrs.get("Street", ""))),
        "location": attrs.get("Location", attrs.get("LOCATION", "")),
        "reported": parse_timestamp(attrs.get("LoggedDate", attrs.get("LOGGED_DATE"))),
        "completed": parse_timestamp(attrs.get("CompletedDate", attrs.get("COMPLETED_DATE"))),
        "lat": geom.get("y"),
        "lng": geom.get("x"),
    }


def parse_road_status(feature: dict) -> dict:
    """Parse a road status feature."""
    attrs = feature.get("attributes", {})
    geom = feature.get("geometry", {})

    # Handle polyline geometry (take midpoint)
    lat, lng = None, None
    if "y" in geom:
        lat, lng = geom["y"], geom["x"]
    elif "paths" in geom:
        paths = geom["paths"]
        if paths and paths[0]:
            mid = len(paths[0]) // 2
            lng, lat = paths[0][mid][0], paths[0][mid][1]

    return {
        "type": "road_status",
        "id": attrs.get("OBJECTID"),
        "road": attrs.get("RoadName", attrs.get("linkName", attrs.get("Road", ""))),
        "status": attrs.get("Status", attrs.get("WorksState", "")),
        "description": attrs.get("Description", attrs.get("PublicComment", ""))[:300] if attrs.get("Description", attrs.get("PublicComment")) else "",
        "start_date": parse_timestamp(attrs.get("StartDate", attrs.get("ValidFrom"))),
        "end_date": parse_timestamp(attrs.get("EndDate", attrs.get("ValidTo"))),
        "lat": lat,
        "lng": lng,
    }


def parse_surfacing(feature: dict) -> dict:
    """Parse a highway surfacing feature."""
    attrs = feature.get("attributes", {})
    geom = feature.get("geometry", {})

    lat, lng = None, None
    if "y" in geom:
        lat, lng = geom["y"], geom["x"]
    elif "paths" in geom:
        paths = geom["paths"]
        if paths and paths[0]:
            mid = len(paths[0]) // 2
            lng, lat = paths[0][mid][0], paths[0][mid][1]

    return {
        "type": "surfacing",
        "id": attrs.get("OBJECTID"),
        "road": attrs.get("RoadName", attrs.get("Road", attrs.get("ROAD_NAME", ""))),
        "scheme": attrs.get("Scheme", attrs.get("SchemeName", "")),
        "status": attrs.get("Status", ""),
        "treatment": attrs.get("Treatment", attrs.get("TreatmentType", "")),
        "year": attrs.get("Year", attrs.get("FinancialYear", "")),
        "length_m": attrs.get("Length", attrs.get("LENGTH")),
        "lat": lat,
        "lng": lng,
    }


def parse_street_lighting(feature: dict) -> dict:
    """Parse a street lighting feature."""
    attrs = feature.get("attributes", {})
    geom = feature.get("geometry", {})

    return {
        "type": "lighting",
        "id": attrs.get("OBJECTID"),
        "road": attrs.get("Street", attrs.get("RoadName", attrs.get("STREET", ""))),
        "unit_no": attrs.get("UnitNo", attrs.get("UNIT_NO", "")),
        "status": attrs.get("Status", attrs.get("STATUS", "")),
        "lamp_type": attrs.get("LampType", attrs.get("LAMP_TYPE", "")),
        "wattage": attrs.get("Wattage", attrs.get("WATTAGE")),
        "lat": geom.get("y"),
        "lng": geom.get("x"),
    }


PARSERS = {
    "road_status": parse_road_status,
    "road_works_line": parse_road_status,  # Same format as road status
    "planning_county": parse_road_status,  # Generic parser
}


def main():
    output_path = sys.argv[1] if len(sys.argv) > 1 else DEFAULT_OUTPUT

    print(f"LCC Highways ETL — {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Fetching from LCC MARIO ArcGIS (Burnley area)...")

    t0 = time.time()
    results = {}

    for svc_key in SERVICES:
        features = fetch_arcgis(svc_key)
        parser = PARSERS[svc_key]
        records = [parser(f) for f in features]
        # Filter to only records with valid coordinates
        records = [r for r in records if r.get("lat") and r.get("lng")]
        results[svc_key] = records

    # Summary stats
    stats = {}
    for key, records in results.items():
        stats[key] = {
            "count": len(records),
            "name": SERVICES[key]["name"],
            "description": SERVICES[key]["desc"],
        }
        # Category breakdown where applicable
        if key == "highway_defects":
            by_cat = {}
            for r in records:
                c = r.get("category") or "Unknown"
                by_cat[c] = by_cat.get(c, 0) + 1
            stats[key]["by_category"] = dict(sorted(by_cat.items(), key=lambda x: -x[1]))

            by_status = {}
            for r in records:
                s = r.get("status") or "Unknown"
                by_status[s] = by_status.get(s, 0) + 1
            stats[key]["by_status"] = dict(sorted(by_status.items(), key=lambda x: -x[1]))

    total = sum(len(v) for v in results.values())

    output = {
        "meta": {
            "source": "Lancashire County Council MARIO ArcGIS",
            "services": list(SERVICES.keys()),
            "bbox": BURNLEY_BBOX,
            "generated": datetime.now(timezone.utc).isoformat(),
            "fetch_time_ms": int((time.time() - t0) * 1000),
        },
        "stats": stats,
        "total": total,
        **results,
    }

    os.makedirs(os.path.dirname(os.path.abspath(output_path)), exist_ok=True)

    with open(output_path, "w") as f:
        json.dump(output, f, separators=(",", ":"))

    size_kb = os.path.getsize(output_path) / 1024
    elapsed = time.time() - t0

    print(f"\nDone in {elapsed:.1f}s")
    print(f"Output: {output_path} ({size_kb:.1f} KB)")
    print(f"Total: {total} features across {len(SERVICES)} services")
    for key, s in stats.items():
        print(f"  {s['name']}: {s['count']}")


if __name__ == "__main__":
    main()
