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

Features:
  - Archive: Completed/removed works saved to roadworks_archive.json
  - Analytics: Operator league tables, district volume, severity trends, duration analysis

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
import statistics
from datetime import datetime, timezone, timedelta
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


def archive_completed_works(output_path: str, new_records: list) -> int:
    """Diff new data against previous snapshot. Archive any works that disappeared."""
    archive_path = output_path.replace("roadworks.json", "roadworks_archive.json")
    now = datetime.now(timezone.utc).isoformat()

    # Load previous snapshot
    old_records = {}
    if os.path.exists(output_path):
        try:
            with open(output_path) as f:
                old_data = json.load(f)
            for r in old_data.get("roadworks", []):
                key = r.get("reference") or f"{r['road']}|{r['operator']}|{r.get('start_date','')}"
                old_records[key] = r
        except (json.JSONDecodeError, KeyError):
            pass

    if not old_records:
        return 0

    # Build set of new record keys
    new_keys = set()
    for r in new_records:
        key = r.get("reference") or f"{r['road']}|{r['operator']}|{r.get('start_date','')}"
        new_keys.add(key)

    # Find disappeared records (were in old, not in new)
    disappeared = []
    for key, r in old_records.items():
        if key not in new_keys:
            # Calculate actual duration
            actual_days = None
            if r.get("start_date"):
                try:
                    start = datetime.fromisoformat(r["start_date"])
                    actual_days = (datetime.now(timezone.utc) - start).days
                except (ValueError, TypeError):
                    pass
            r["completed_date"] = now
            r["actual_duration_days"] = actual_days
            r["archived_reason"] = "removed_from_mario"
            disappeared.append(r)

    if not disappeared:
        return 0

    # Load existing archive
    archive = {"meta": {}, "archived_works": []}
    if os.path.exists(archive_path):
        try:
            with open(archive_path) as f:
                archive = json.load(f)
        except (json.JSONDecodeError, KeyError):
            archive = {"meta": {}, "archived_works": []}

    # Deduplicate — don't re-add if already archived
    existing_keys = set()
    for r in archive.get("archived_works", []):
        key = r.get("reference") or f"{r['road']}|{r['operator']}|{r.get('start_date','')}"
        existing_keys.add(key)

    new_archived = []
    for r in disappeared:
        key = r.get("reference") or f"{r['road']}|{r['operator']}|{r.get('start_date','')}"
        if key not in existing_keys:
            new_archived.append(r)

    if not new_archived:
        return 0

    archive["archived_works"].extend(new_archived)
    archive["meta"] = {
        "archive_started": archive.get("meta", {}).get("archive_started", now[:10]),
        "last_updated": now,
        "total_archived": len(archive["archived_works"]),
    }

    with open(archive_path, "w") as f:
        json.dump(archive, f, indent=2)

    print(f"  Archived {len(new_archived)} completed/removed works → {archive_path}")
    return len(new_archived)


def compute_analytics(records: list, output_path: str) -> dict:
    """Compute rich analytics from live data + archive. Output to roadworks_analytics.json."""
    archive_path = output_path.replace("roadworks.json", "roadworks_archive.json")
    analytics_path = output_path.replace("roadworks.json", "roadworks_analytics.json")
    now = datetime.now(timezone.utc)

    # Load archive
    archived = []
    if os.path.exists(archive_path):
        try:
            with open(archive_path) as f:
                archived = json.load(f).get("archived_works", [])
        except (json.JSONDecodeError, KeyError):
            pass

    all_works = records + archived

    # --- Operator League Tables ---
    ops = {}
    for r in all_works:
        op = r.get("operator") or "Unknown"
        if op not in ops:
            ops[op] = {"operator": op, "active_works": 0, "completed_works": 0,
                       "durations": [], "overruns": 0, "total_with_dates": 0,
                       "high_severity": 0, "districts": set()}
        is_archived = "completed_date" in r
        if is_archived:
            ops[op]["completed_works"] += 1
        else:
            ops[op]["active_works"] += 1

        if r.get("severity") == "high":
            ops[op]["high_severity"] += 1
        if r.get("district"):
            ops[op]["districts"].add(r["district"])

        # Duration analysis
        if r.get("start_date") and r.get("end_date"):
            try:
                start = datetime.fromisoformat(r["start_date"])
                end = datetime.fromisoformat(r["end_date"])
                planned_days = (end - start).days
                if planned_days > 0:
                    ops[op]["durations"].append(planned_days)
                    ops[op]["total_with_dates"] += 1
                    # Overrun: archived and actual > planned
                    if is_archived and r.get("actual_duration_days"):
                        if r["actual_duration_days"] > planned_days:
                            ops[op]["overruns"] += 1
            except (ValueError, TypeError):
                pass

    operator_league = []
    for op, data in ops.items():
        total = data["active_works"] + data["completed_works"]
        avg_dur = statistics.mean(data["durations"]) if data["durations"] else None
        overrun_pct = (data["overruns"] / data["completed_works"] * 100) if data["completed_works"] > 0 else None
        high_pct = (data["high_severity"] / total * 100) if total > 0 else 0
        operator_league.append({
            "operator": op,
            "active_works": data["active_works"],
            "completed_works": data["completed_works"],
            "total_works": total,
            "avg_duration_days": round(avg_dur, 1) if avg_dur else None,
            "overrun_pct": round(overrun_pct, 1) if overrun_pct is not None else None,
            "high_severity_pct": round(high_pct, 1),
            "districts_active_in": len(data["districts"]),
        })
    operator_league.sort(key=lambda x: -x["total_works"])

    # --- District Work Volume ---
    district_volume = {}
    for r in records:
        d = r.get("district") or "unknown"
        if d not in district_volume:
            district_volume[d] = {"active": 0, "completed_30d": 0, "completed_90d": 0}
        district_volume[d]["active"] += 1

    for r in archived:
        d = r.get("district") or "unknown"
        if d not in district_volume:
            district_volume[d] = {"active": 0, "completed_30d": 0, "completed_90d": 0}
        if r.get("completed_date"):
            try:
                completed = datetime.fromisoformat(r["completed_date"])
                days_ago = (now - completed).days
                if days_ago <= 30:
                    district_volume[d]["completed_30d"] += 1
                if days_ago <= 90:
                    district_volume[d]["completed_90d"] += 1
            except (ValueError, TypeError):
                pass

    # Trend: compare 30d completion rate vs 90d
    for d, vol in district_volume.items():
        rate_30 = vol["completed_30d"]
        rate_90 = vol["completed_90d"] / 3 if vol["completed_90d"] > 0 else 0
        if rate_30 > rate_90 * 1.2:
            vol["trend"] = "increasing"
        elif rate_30 < rate_90 * 0.8:
            vol["trend"] = "decreasing"
        else:
            vol["trend"] = "stable"

    # --- Severity Trends ---
    severity_current = {"high": 0, "medium": 0, "low": 0, "unknown": 0}
    for r in records:
        sev = r.get("severity", "unknown")
        severity_current[sev] = severity_current.get(sev, 0) + 1

    severity_30d = {"high": 0, "medium": 0, "low": 0, "unknown": 0}
    for r in archived:
        if r.get("completed_date"):
            try:
                completed = datetime.fromisoformat(r["completed_date"])
                if (now - completed).days <= 30:
                    sev = r.get("severity", "unknown")
                    severity_30d[sev] = severity_30d.get(sev, 0) + 1
            except (ValueError, TypeError):
                pass

    # --- Duration Analysis ---
    durations_by_cat = {}
    all_durations = []
    for r in all_works:
        if r.get("start_date") and r.get("end_date"):
            try:
                start = datetime.fromisoformat(r["start_date"])
                end = datetime.fromisoformat(r["end_date"])
                planned = (end - start).days
                if 0 < planned < 3650:  # Sanity: < 10 years
                    cat = r.get("category", "Unknown")
                    if cat not in durations_by_cat:
                        durations_by_cat[cat] = []
                    durations_by_cat[cat].append(planned)
                    all_durations.append(planned)
            except (ValueError, TypeError):
                pass

    duration_by_category = {}
    for cat, durs in durations_by_cat.items():
        duration_by_category[cat] = {
            "count": len(durs),
            "avg": round(statistics.mean(durs), 1),
            "median": round(statistics.median(durs)),
            "max": max(durs),
        }

    # Longest running active works
    longest_running = []
    for r in records:
        if r.get("start_date"):
            try:
                start = datetime.fromisoformat(r["start_date"])
                days = (now - start).days
                if days > 90:
                    longest_running.append({
                        "road": r["road"], "district": r.get("district", ""),
                        "days": days, "operator": r.get("operator", ""),
                        "start_date": r["start_date"],
                        "category": r.get("category", ""),
                    })
            except (ValueError, TypeError):
                pass
    longest_running.sort(key=lambda x: -x["days"])
    longest_running = longest_running[:20]

    # Overdue works (past end_date but still active)
    overdue = []
    for r in records:
        if r.get("end_date"):
            try:
                end = datetime.fromisoformat(r["end_date"])
                if end < now:
                    days_overdue = (now - end).days
                    overdue.append({
                        "road": r["road"], "district": r.get("district", ""),
                        "operator": r.get("operator", ""),
                        "planned_end": r["end_date"][:10],
                        "days_overdue": days_overdue,
                        "category": r.get("category", ""),
                    })
            except (ValueError, TypeError):
                pass
    overdue.sort(key=lambda x: -x["days_overdue"])
    overdue = overdue[:30]

    analytics = {
        "meta": {
            "generated": now.isoformat(),
            "live_works": len(records),
            "archived_works": len(archived),
            "total_analysed": len(all_works),
        },
        "operator_league": operator_league,
        "district_volume": dict(sorted(district_volume.items(),
                                       key=lambda x: -x[1]["active"])),
        "severity_trends": {
            "current": severity_current,
            "completed_30d": severity_30d,
        },
        "duration_analysis": {
            "overall_avg": round(statistics.mean(all_durations), 1) if all_durations else None,
            "overall_median": round(statistics.median(all_durations)) if all_durations else None,
            "by_category": duration_by_category,
            "longest_running": longest_running,
            "overdue_works": overdue,
            "overdue_count": len(overdue),
        },
    }

    with open(analytics_path, "w") as f:
        json.dump(analytics, f, indent=2)

    size_kb = os.path.getsize(analytics_path) / 1024
    print(f"  Analytics: {analytics_path} ({size_kb:.1f} KB)")
    print(f"    Operators: {len(operator_league)}, Overdue: {len(overdue)}, Longest: {longest_running[0]['days']}d" if longest_running else "    No long-running works")
    return analytics


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

    # Archive completed works (diff against previous snapshot)
    archived_count = archive_completed_works(output_path, records)

    # Compute stats
    stats = compute_stats(records)

    # Compute analytics (operator league, district volume, severity, duration)
    analytics = compute_analytics(records, output_path)

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
