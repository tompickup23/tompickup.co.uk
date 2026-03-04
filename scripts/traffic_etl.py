#!/usr/bin/env python3
"""
Traffic Impact ETL — Builds daily traffic impact model for Burnley.

Data sources:
- DfT Road Traffic Statistics API (AADF counts, count points, hourly raw counts)
- Lancashire school term dates (hardcoded through 2028)
- Burnley FC fixtures (fixtur.es iCal feed)
- Rush hour patterns (empirical UK traffic distribution)
- Roadworks data (from roadworks.json)

Combines everything into a traffic impact model showing:
- Hourly traffic flow profiles (typical weekday vs weekend)
- Impact heatmap: hour × day showing congestion risk
- Event overlays: match days, school run times, roadworks density
- Per-road traffic volumes from DfT count points

Output: public/data/traffic.json
Schedule: Every 12 hours via GitHub Actions
"""

import json
import sys
import os
import time
import re
from datetime import datetime, timezone, timedelta, date
from urllib.request import urlopen, Request
from urllib.parse import urlencode
from urllib.error import URLError, HTTPError

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
DEFAULT_OUTPUT = os.path.join(SCRIPT_DIR, "..", "public", "data", "traffic.json")

# --- DfT Traffic API ---
DFT_BASE = "https://roadtraffic.dft.gov.uk/api"
LANCASHIRE_LA_ID = 76  # Lancashire county LA ID in DfT system


# --- UK Rush Hour Empirical Profiles (% of daily traffic by hour) ---
# Source: DfT National Road Traffic Survey typical profiles
WEEKDAY_PROFILE = {
    0: 0.8, 1: 0.5, 2: 0.4, 3: 0.4, 4: 0.7, 5: 1.8,
    6: 3.8, 7: 6.8, 8: 7.9, 9: 6.2, 10: 5.3, 11: 5.5,
    12: 5.8, 13: 5.6, 14: 5.8, 15: 6.8, 16: 7.5, 17: 7.8,
    18: 6.2, 19: 4.2, 20: 3.2, 21: 2.5, 22: 2.0, 23: 1.4,
}

WEEKEND_PROFILE = {
    0: 1.5, 1: 1.0, 2: 0.7, 3: 0.5, 4: 0.5, 5: 0.8,
    6: 1.5, 7: 2.5, 8: 4.0, 9: 5.5, 10: 6.5, 11: 7.0,
    12: 7.2, 13: 7.0, 14: 6.8, 15: 6.5, 16: 6.0, 17: 5.5,
    18: 5.0, 19: 4.5, 20: 4.0, 21: 3.5, 22: 2.8, 23: 2.2,
}

# School run overlay (additional % increase during term time)
SCHOOL_RUN_OVERLAY = {
    7: 3.0, 8: 8.0, 9: 4.0,  # Morning school run: ~8:15-8:55 peak
    14: 2.0, 15: 6.0, 16: 3.0,  # Afternoon pickup: ~3:00-3:30 peak
}

# Match day overlay for Burnley FC (additional % for 3pm KO Saturday)
MATCH_DAY_OVERLAY_3PM = {
    12: 2.0, 13: 4.0, 14: 6.0, 15: 2.0,  # Pre-match arrival
    16: 1.0, 17: 8.0, 18: 5.0, 19: 2.0,  # Post-match departure
}

MATCH_DAY_OVERLAY_EVENING = {
    17: 3.0, 18: 5.0, 19: 6.0, 20: 2.0,  # Pre-match evening
    21: 1.0, 22: 7.0, 23: 4.0,  # Post-match evening
}


# --- Lancashire School Term Dates (hardcoded through 2028) ---
SCHOOL_TERMS = [
    # 2024-25
    {"start": "2024-09-02", "end": "2024-10-25", "name": "Autumn 1 2024"},
    {"start": "2024-11-04", "end": "2024-12-20", "name": "Autumn 2 2024"},
    {"start": "2025-01-06", "end": "2025-02-14", "name": "Spring 1 2025"},
    {"start": "2025-02-24", "end": "2025-04-04", "name": "Spring 2 2025"},
    {"start": "2025-04-22", "end": "2025-05-23", "name": "Summer 1 2025"},
    {"start": "2025-06-02", "end": "2025-07-18", "name": "Summer 2 2025"},
    # 2025-26
    {"start": "2025-09-01", "end": "2025-10-24", "name": "Autumn 1 2025"},
    {"start": "2025-11-03", "end": "2025-12-19", "name": "Autumn 2 2025"},
    {"start": "2026-01-05", "end": "2026-02-13", "name": "Spring 1 2026"},
    {"start": "2026-02-23", "end": "2026-03-27", "name": "Spring 2 2026"},
    {"start": "2026-04-13", "end": "2026-05-22", "name": "Summer 1 2026"},
    {"start": "2026-06-01", "end": "2026-07-17", "name": "Summer 2 2026"},
    # 2026-27
    {"start": "2026-09-07", "end": "2026-10-23", "name": "Autumn 1 2026"},
    {"start": "2026-11-02", "end": "2026-12-18", "name": "Autumn 2 2026"},
    {"start": "2027-01-04", "end": "2027-02-12", "name": "Spring 1 2027"},
    {"start": "2027-02-22", "end": "2027-03-26", "name": "Spring 2 2027"},
    {"start": "2027-04-12", "end": "2027-05-28", "name": "Summer 1 2027"},
    {"start": "2027-06-07", "end": "2027-07-23", "name": "Summer 2 2027"},
    # 2027-28
    {"start": "2027-09-06", "end": "2027-10-22", "name": "Autumn 1 2027"},
    {"start": "2027-11-01", "end": "2027-12-17", "name": "Autumn 2 2027"},
    {"start": "2028-01-04", "end": "2028-02-18", "name": "Spring 1 2028"},
    {"start": "2028-02-28", "end": "2028-03-31", "name": "Spring 2 2028"},
    {"start": "2028-04-17", "end": "2028-05-26", "name": "Summer 1 2028"},
    {"start": "2028-06-05", "end": "2028-07-21", "name": "Summer 2 2028"},
]


def is_term_time(check_date: date) -> bool:
    """Check if a date falls within school term time."""
    for term in SCHOOL_TERMS:
        start = date.fromisoformat(term["start"])
        end = date.fromisoformat(term["end"])
        if start <= check_date <= end:
            return True
    return False


def get_current_term(check_date: date) -> str:
    """Get the current term name."""
    for term in SCHOOL_TERMS:
        start = date.fromisoformat(term["start"])
        end = date.fromisoformat(term["end"])
        if start <= check_date <= end:
            return term["name"]
    return "School Holiday"


def fetch_dft_count_points() -> list:
    """Fetch DfT traffic count points in Burnley area."""
    burnley_points = []
    page = 1

    while page <= 10:  # Safety limit
        url = f"{DFT_BASE}/count-points?filter[local_authority_id]={LANCASHIRE_LA_ID}&page[size]=200&page[number]={page}"
        req = Request(url, headers={"User-Agent": "TomPickup-Traffic-ETL/1.0", "Accept": "application/json"})

        try:
            with urlopen(req, timeout=30) as resp:
                data = json.loads(resp.read().decode("utf-8"))
        except (URLError, HTTPError) as e:
            print(f"  ERROR fetching DfT count points page {page}: {e}", file=sys.stderr)
            break

        points = data.get("data", [])
        if not points:
            break

        for p in points:
            # DfT uses flat objects, not nested attributes
            lat = p.get("latitude")
            lng = p.get("longitude")
            # Convert string coords to float
            try:
                lat = float(lat) if lat else None
                lng = float(lng) if lng else None
            except (ValueError, TypeError):
                continue

            if lat and lng and 53.72 < lat < 53.82 and -2.35 < lng < -2.10:
                burnley_points.append({
                    "id": p.get("count_point_id", p.get("id")),
                    "road": p.get("road_name", ""),
                    "road_category": p.get("road_category", ""),
                    "road_type": p.get("road_type", ""),
                    "lat": lat,
                    "lng": lng,
                    "description": (p.get("start_junction_road_name") or "") + " to " + (p.get("end_junction_road_name") or ""),
                })

        # Check if more pages
        if data.get("next_page_url"):
            page += 1
            time.sleep(0.5)  # Rate limit
        else:
            break

    print(f"  DfT count points in Burnley: {len(burnley_points)} (scanned {page} pages)")
    return burnley_points


def fetch_dft_aadf(count_point_ids: list) -> dict:
    """Fetch Annual Average Daily Flow data for Burnley count points."""
    aadf_data = {}

    for cp_id in count_point_ids[:30]:  # Limit to avoid rate limits
        url = f"{DFT_BASE}/average-annual-daily-flow?filter[count_point_id]={cp_id}&page[size]=10"
        req = Request(url, headers={"User-Agent": "TomPickup-Traffic-ETL/1.0", "Accept": "application/json"})

        try:
            with urlopen(req, timeout=15) as resp:
                data = json.loads(resp.read().decode("utf-8"))
            records = data.get("data", [])
            if records:
                # DfT uses flat objects — get most recent year
                latest = max(records, key=lambda r: r.get("year", r.get("aadf_year", 0)))
                aadf_data[str(cp_id)] = {
                    "year": latest.get("year", latest.get("aadf_year")),
                    "all_motor_vehicles": latest.get("all_motor_vehicles"),
                    "cars_and_taxis": latest.get("cars_and_taxis"),
                    "buses_and_coaches": latest.get("buses_and_coaches"),
                    "lgvs": latest.get("lgvs"),
                    "all_hgvs": latest.get("all_hgvs"),
                    "pedal_cycles": latest.get("pedal_cycles"),
                }
        except Exception:
            pass

        time.sleep(0.5)  # Rate limit

    print(f"  DfT AADF data: {len(aadf_data)} count points")
    return aadf_data


def fetch_burnley_fc_fixtures() -> list:
    """Fetch Burnley FC fixtures from fixturedownload.com JSON API."""
    # Try current and previous season URLs
    fixture_urls = [
        "https://fixturedownload.com/feed/json/epl-2025/burnley",
        "https://fixturedownload.com/feed/json/efl-championship-2025/burnley",
        "https://fixturedownload.com/feed/ical/epl-2025/burnley",  # Also returns JSON
    ]

    for url in fixture_urls:
        req = Request(url, headers={"User-Agent": "Mozilla/5.0"})
        try:
            with urlopen(req, timeout=15) as resp:
                raw = resp.read().decode("utf-8")

            data = json.loads(raw)
            if isinstance(data, list) and data:
                fixtures = parse_fixture_json(data)
                print(f"  Burnley FC fixtures: {len(fixtures)} (from fixturedownload.com)")
                return fixtures
        except json.JSONDecodeError:
            # Not JSON, try iCal parsing
            if "BEGIN:VCALENDAR" in raw:
                fixtures = parse_ical(raw)
                if fixtures:
                    print(f"  Burnley FC fixtures: {len(fixtures)} (iCal)")
                    return fixtures
        except Exception as e:
            print(f"  Fixture fetch failed ({url}): {e}", file=sys.stderr)
            continue

    print("  Warning: Could not fetch Burnley FC fixtures", file=sys.stderr)
    return []


def parse_fixture_json(matches: list) -> list:
    """Parse fixturedownload.com JSON into fixture list."""
    now = datetime.now()
    cutoff_past = (now - timedelta(days=60)).strftime("%Y-%m-%d")
    cutoff_future = (now + timedelta(days=120)).strftime("%Y-%m-%d")

    fixtures = []
    for m in matches:
        date_str = m.get("DateUtc", "")
        if not date_str:
            continue

        try:
            dt = datetime.strptime(date_str, "%Y-%m-%d %H:%M:%SZ")
        except ValueError:
            try:
                dt = datetime.strptime(date_str[:10], "%Y-%m-%d")
            except ValueError:
                continue

        d = dt.strftime("%Y-%m-%d")
        if not (cutoff_past <= d <= cutoff_future):
            continue

        home_team = m.get("HomeTeam", "")
        away_team = m.get("AwayTeam", "")
        location = m.get("Location", "")
        is_home = home_team.lower().strip() == "burnley"

        opponent = away_team if is_home else home_team
        summary = f"{home_team} vs {away_team}"

        # Score (if played)
        home_score = m.get("HomeTeamScore")
        away_score = m.get("AwayTeamScore")
        score = f"{home_score}-{away_score}" if home_score is not None and away_score is not None else None

        fixtures.append({
            "date": d,
            "time": dt.strftime("%H:%M"),
            "kickoff_hour": dt.hour,
            "opponent": opponent,
            "summary": summary,
            "location": location,
            "is_home": is_home,
            "score": score,
            "round": m.get("RoundNumber"),
        })

    return sorted(fixtures, key=lambda x: x["date"])


def parse_ical(text: str) -> list:
    """Parse iCal text into fixture list (fallback)."""
    fixtures = []
    current = {}

    for line in text.split("\n"):
        line = line.strip()
        if line == "BEGIN:VEVENT":
            current = {}
        elif line == "END:VEVENT":
            if current.get("date"):
                fixtures.append(current)
            current = {}
        elif line.startswith("DTSTART"):
            val = line.split(":", 1)[-1]
            try:
                if "T" in val:
                    dt = datetime.strptime(val.replace("Z", ""), "%Y%m%dT%H%M%S")
                    current["date"] = dt.strftime("%Y-%m-%d")
                    current["time"] = dt.strftime("%H:%M")
                    current["kickoff_hour"] = dt.hour
                else:
                    current["date"] = datetime.strptime(val, "%Y%m%d").strftime("%Y-%m-%d")
                    current["time"] = "15:00"
                    current["kickoff_hour"] = 15
            except ValueError:
                pass
        elif line.startswith("SUMMARY:"):
            current["summary"] = line[8:]
            current["opponent"] = line[8:]
        elif line.startswith("LOCATION:"):
            current["location"] = line[9:]
            current["is_home"] = "turf moor" in line[9:].lower()

    return sorted(fixtures, key=lambda x: x.get("date", ""))


def build_impact_heatmap(today: date, roadworks_count: int, fixtures: list) -> list:
    """Build 7-day × 24-hour traffic impact heatmap."""
    heatmap = []

    for day_offset in range(7):
        day = today + timedelta(days=day_offset)
        day_str = day.strftime("%Y-%m-%d")
        day_name = day.strftime("%A")
        is_weekend = day.weekday() >= 5
        in_term = is_term_time(day)

        # Check for match
        match = None
        for f in fixtures:
            if f["date"] == day_str and f.get("is_home"):
                match = f
                break

        base_profile = WEEKEND_PROFILE if is_weekend else WEEKDAY_PROFILE

        hours = []
        for hour in range(24):
            flow = base_profile[hour]

            # School run overlay (weekdays only, term time)
            school_overlay = 0
            if not is_weekend and in_term and hour in SCHOOL_RUN_OVERLAY:
                school_overlay = SCHOOL_RUN_OVERLAY[hour]
                flow += school_overlay

            # Match day overlay
            match_overlay = 0
            if match:
                ko = match.get("kickoff_hour", 15)
                if ko >= 19:
                    overlay = MATCH_DAY_OVERLAY_EVENING
                else:
                    overlay = MATCH_DAY_OVERLAY_3PM
                if hour in overlay:
                    match_overlay = overlay[hour]
                    flow += match_overlay

            # Roadworks impact (slight increase during working hours)
            rw_impact = 0
            if 7 <= hour <= 18 and roadworks_count > 0:
                rw_impact = min(roadworks_count * 0.15, 3.0)  # Cap at 3%
                flow += rw_impact

            # Determine congestion level
            if flow >= 12:
                level = "severe"
            elif flow >= 9:
                level = "high"
            elif flow >= 6:
                level = "moderate"
            elif flow >= 3:
                level = "low"
            else:
                level = "minimal"

            hours.append({
                "hour": hour,
                "flow_pct": round(flow, 1),
                "level": level,
                "school_run": school_overlay > 0,
                "match_day": match_overlay > 0,
                "roadworks_impact": round(rw_impact, 1),
            })

        heatmap.append({
            "date": day_str,
            "day": day_name,
            "is_weekend": is_weekend,
            "in_term": in_term,
            "match": match,
            "peak_hour": max(hours, key=lambda h: h["flow_pct"])["hour"],
            "peak_flow": max(hours, key=lambda h: h["flow_pct"])["flow_pct"],
            "hours": hours,
        })

    return heatmap


def main():
    output_path = sys.argv[1] if len(sys.argv) > 1 else DEFAULT_OUTPUT

    print(f"Traffic Impact ETL — {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    t0 = time.time()
    today = date.today()

    # 1. DfT count points
    print("\n1. Fetching DfT traffic count points...")
    count_points = fetch_dft_count_points()

    # 2. AADF data for count points
    aadf_data = {}
    if count_points:
        print("\n2. Fetching AADF data...")
        cp_ids = [cp["id"] for cp in count_points if cp.get("id")]
        aadf_data = fetch_dft_aadf(cp_ids)

    # Merge AADF into count points
    for cp in count_points:
        cp_id = str(cp.get("id", ""))
        if cp_id in aadf_data:
            cp["aadf"] = aadf_data[cp_id]

    # 3. Burnley FC fixtures
    print("\n3. Fetching Burnley FC fixtures...")
    fixtures = fetch_burnley_fc_fixtures()

    # 4. Load existing roadworks for impact calculation
    rw_path = os.path.join(SCRIPT_DIR, "..", "public", "data", "roadworks.json")
    active_roadworks = 0
    try:
        with open(rw_path) as f:
            rw = json.load(f)
        active_roadworks = rw.get("stats", {}).get("works_started", 0)
    except Exception:
        pass

    # 5. Build impact heatmap
    print("\n4. Building traffic impact heatmap...")
    heatmap = build_impact_heatmap(today, active_roadworks, fixtures)

    # 6. School term status
    term_status = {
        "in_term": is_term_time(today),
        "current_term": get_current_term(today),
        "terms": SCHOOL_TERMS,
    }

    # 7. Key road summaries
    key_roads = {}
    for cp in count_points:
        road = cp.get("road", "")
        if road and cp.get("aadf"):
            if road not in key_roads or (cp["aadf"].get("all_motor_vehicles", 0) or 0) > (key_roads[road].get("daily_vehicles", 0) or 0):
                key_roads[road] = {
                    "road": road,
                    "category": cp.get("road_category", ""),
                    "daily_vehicles": cp["aadf"].get("all_motor_vehicles"),
                    "cars": cp["aadf"].get("cars_and_taxis"),
                    "hgvs": cp["aadf"].get("all_hgvs"),
                    "buses": cp["aadf"].get("buses_and_coaches"),
                    "cycles": cp["aadf"].get("pedal_cycles"),
                    "year": cp["aadf"].get("year"),
                    "lat": cp.get("lat"),
                    "lng": cp.get("lng"),
                }

    key_roads_list = sorted(key_roads.values(), key=lambda x: -(x.get("daily_vehicles") or 0))

    # 8. Traffic flow profiles
    profiles = {
        "weekday": WEEKDAY_PROFILE,
        "weekend": WEEKEND_PROFILE,
        "school_run_overlay": SCHOOL_RUN_OVERLAY,
        "match_day_3pm": MATCH_DAY_OVERLAY_3PM,
        "match_day_evening": MATCH_DAY_OVERLAY_EVENING,
    }

    output = {
        "meta": {
            "source": "DfT Road Traffic API + fixtur.es + LCC School Terms",
            "generated": datetime.now(timezone.utc).isoformat(),
            "fetch_time_ms": int((time.time() - t0) * 1000),
            "active_roadworks": active_roadworks,
        },
        "count_points": count_points,
        "key_roads": key_roads_list[:20],
        "fixtures": fixtures,
        "school_terms": term_status,
        "heatmap": heatmap,
        "profiles": profiles,
    }

    os.makedirs(os.path.dirname(os.path.abspath(output_path)), exist_ok=True)

    with open(output_path, "w") as f:
        json.dump(output, f, separators=(",", ":"))

    size_kb = os.path.getsize(output_path) / 1024
    elapsed = time.time() - t0

    print(f"\nDone in {elapsed:.1f}s")
    print(f"Output: {output_path} ({size_kb:.1f} KB)")
    print(f"Count points: {len(count_points)}, Key roads: {len(key_roads_list)}")
    print(f"Fixtures: {len(fixtures)}, School term: {'Yes' if term_status['in_term'] else 'No'}")
    print(f"Active roadworks: {active_roadworks}")
    print(f"7-day peak: {max(h['peak_flow'] for h in heatmap):.1f}% at {max(heatmap, key=lambda h: h['peak_flow'])['day']}")


if __name__ == "__main__":
    main()
