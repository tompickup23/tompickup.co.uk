#!/usr/bin/env python3
"""
Traffic Intelligence ETL — Comprehensive traffic model for Burnley.

Data sources:
- DfT Road Traffic Statistics API (AADF counts, count points)
- OpenStreetMap Overpass API (traffic signals, pedestrian crossings)
- Lancashire school term dates (hardcoded through 2028)
- Burnley FC fixtures (fixturedownload.com JSON)
- Rush hour patterns (empirical UK traffic distribution)
- Roadworks data (from roadworks.json) — active works, TTROs
- FixMyStreet data (from fixmystreet.json) — pothole/defect reports

Congestion model outputs:
- Hourly traffic flow profiles (weekday vs weekend vs school+match)
- 7-day × 24-hour impact heatmap with congestion severity
- Junction Congestion Index (JCI) — per signalised junction scoring
- Road Corridor Congestion Score — per key road corridor
- Infrastructure map: 119 traffic signals, 100+ signal crossings from OSM
- DfT count point AADF volumes at 92 monitoring locations
- Congestion pinch points with severity ranking
- Options appraisal: scored LCC interventions

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



# --- Burnley signalised junctions (named, for congestion model) ---
# Key junctions with their approximate coordinates and characteristics
NAMED_JUNCTIONS = [
    {"name": "M65 J10 Burnley Barracks", "lat": 53.7896, "lng": -2.2860, "type": "interchange", "lanes": 4, "approaches": 6, "notes": "Two-island roundabout, 500m span, NO2 exceedance"},
    {"name": "M65 J9 Rosegrove", "lat": 53.7853, "lng": -2.2829, "type": "interchange", "lanes": 3, "approaches": 4, "notes": "Widened 2019-20, still bottleneck"},
    {"name": "M65 J11 Barracks/Brierfield", "lat": 53.8013, "lng": -2.3277, "type": "interchange", "lanes": 2, "approaches": 4, "notes": "A6114 junction"},
    {"name": "Centenary Way / Yorkshire St", "lat": 53.7925, "lng": -2.2425, "type": "signals", "lanes": 2, "approaches": 4, "notes": "Roundabout demolished 2025, new signalised junction"},
    {"name": "Manchester Rd / Red Lion St", "lat": 53.7889, "lng": -2.2387, "type": "signals", "lanes": 2, "approaches": 3, "notes": "New one-way system Feb 2026, LUF scheme"},
    {"name": "Colne Rd / Barracks Rd", "lat": 53.7943, "lng": -2.2325, "type": "signals", "lanes": 2, "approaches": 4, "notes": "Major works Jan-Aug 2026, new avg speed cameras"},
    {"name": "Todmorden Rd / Manchester Rd", "lat": 53.7868, "lng": -2.2453, "type": "signals", "lanes": 2, "approaches": 3, "notes": "Cross-Pennine route intersection"},
    {"name": "Parker Lane / Manchester Rd", "lat": 53.7899, "lng": -2.2490, "type": "signals", "lanes": 2, "approaches": 3, "notes": "Town centre approach"},
    {"name": "Accrington Rd / Rossendale Rd", "lat": 53.7855, "lng": -2.2822, "type": "signals", "lanes": 2, "approaches": 4, "notes": "Rosegrove junction, repeated utility works"},
    {"name": "Padiham Rd / Active Travel", "lat": 53.7970, "lng": -2.2911, "type": "signals", "lanes": 2, "approaches": 3, "notes": "TTRO in force, LUF improvement"},
    {"name": "Brunshaw Rd / Brownside Rd", "lat": 53.7738, "lng": -2.2564, "type": "signals", "lanes": 2, "approaches": 3, "notes": "School run congestion"},
    {"name": "A646 / A682 Centenary Way", "lat": 53.7888, "lng": -2.2619, "type": "signals", "lanes": 2, "approaches": 3, "notes": "Viaduct approach"},
    {"name": "Colne Rd / North St", "lat": 53.7976, "lng": -2.2392, "type": "signals", "lanes": 2, "approaches": 3, "notes": "Active speed camera zone"},
    {"name": "A6114 / Colne Rd", "lat": 53.8040, "lng": -2.2346, "type": "signals", "lanes": 2, "approaches": 3, "notes": "Brierfield approach"},
    {"name": "A646 Glen View / Todmorden Rd", "lat": 53.7697, "lng": -2.2255, "type": "signals", "lanes": 2, "approaches": 3, "notes": "Cliviger Gorge approach"},
]

# Burnley schools near major roads (cause school run congestion)
SCHOOL_LOCATIONS = [
    {"name": "Burnley College", "lat": 53.7930, "lng": -2.2420, "pupils": 3000, "road": "Princess Way"},
    {"name": "Unity College", "lat": 53.7857, "lng": -2.2478, "pupils": 900, "road": "Todmorden Road"},
    {"name": "Habergham Eaves Primary", "lat": 53.7820, "lng": -2.2610, "pupils": 350, "road": "Rossendale Road"},
    {"name": "St Peter's CE Primary", "lat": 53.7896, "lng": -2.2560, "pupils": 240, "road": "Church Street"},
    {"name": "Burnley Brunshaw Primary", "lat": 53.7740, "lng": -2.2530, "pupils": 400, "road": "Brunshaw Road"},
    {"name": "Padiham Green CE Primary", "lat": 53.7968, "lng": -2.3100, "pupils": 280, "road": "Padiham Road"},
    {"name": "Shuttleworth College", "lat": 53.7905, "lng": -2.2670, "pupils": 1100, "road": "Cavalry Way"},
    {"name": "Blessed Trinity RC College", "lat": 53.7845, "lng": -2.2360, "pupils": 900, "road": "Ormerod Road"},
    {"name": "Coal Clough Academy", "lat": 53.7752, "lng": -2.2702, "pupils": 750, "road": "Coal Clough Lane"},
    {"name": "Burnley Campus UCLan", "lat": 53.7910, "lng": -2.2480, "pupils": 2000, "road": "Parker Lane"},
]

# Turf Moor (Burnley FC) — match day traffic management
TURF_MOOR = {"lat": 53.7891, "lng": -2.2303, "capacity": 21944, "name": "Turf Moor"}

# --- Major projects (cannot be deferred — structural works in progress) ---
# These are tagged so the deferral engine knows not to recommend pausing them
MAJOR_PROJECTS = [
    {
        "name": "Town2Turf (Levelling Up Fund)",
        "description": "£19.9M LUF-funded town centre to Turf Moor pedestrian/cycling link. Manchester Rd one-way system, Centenary Way junction rebuild, Hammerton St improvements.",
        "roads": ["Manchester Road", "Centenary Way", "Hammerton Street", "Yorkshire Street", "Red Lion Street"],
        "lat_range": [53.786, 53.795],
        "lng_range": [-2.248, -2.230],
        "start": "2025-06",
        "end": "2027-03",
        "cannot_defer": True,
        "reason": "LUF grant-funded project with DLUHC milestones — deferral risks losing £19.9M funding",
    },
    {
        "name": "Colne Rd / Barracks Rd Average Speed Cameras",
        "description": "Major junction rebuild with new average speed camera installation. Full carriageway works.",
        "roads": ["Colne Road", "Barracks Road"],
        "lat_range": [53.793, 53.800],
        "lng_range": [-2.240, -2.225],
        "start": "2026-01",
        "end": "2026-08",
        "cannot_defer": True,
        "reason": "Safety-critical infrastructure — average speed camera installation committed",
    },
    {
        "name": "Padiham Rd Active Travel",
        "description": "LCC active travel corridor with new signal-controlled crossings and cycle infrastructure.",
        "roads": ["Padiham Road"],
        "lat_range": [53.795, 53.802],
        "lng_range": [-2.310, -2.280],
        "start": "2025-09",
        "end": "2026-06",
        "cannot_defer": True,
        "reason": "Active Travel Fund grant — deferral risks DfT clawback",
    },
]


def is_major_project_work(rw: dict):
    """Check if a roadwork is part of a known major project.

    Returns the project dict if matched, None otherwise.
    Major project works cannot be deferred — they have funding milestones.
    """
    rlat = rw.get("lat")
    rlng = rw.get("lng")
    road = (rw.get("road") or "").lower()

    if not rlat or not rlng:
        return None

    try:
        rlat = float(rlat)
        rlng = float(rlng)
    except (ValueError, TypeError):
        return None

    for proj in MAJOR_PROJECTS:
        # Check geographic bounds
        if (proj["lat_range"][0] <= rlat <= proj["lat_range"][1] and
            proj["lng_range"][0] <= rlng <= proj["lng_range"][1]):
            # Also check road name match
            for proad in proj["roads"]:
                if proad.lower() in road or road in proad.lower():
                    return proj
    return None

# Key congestion corridors — road segments for corridor scoring
CONGESTION_CORRIDORS = [
    {"name": "M65 Corridor (J8-J12)", "road": "M65", "coords": [[53.779,-2.341],[53.783,-2.314],[53.786,-2.285],[53.790,-2.258],[53.793,-2.253],[53.811,-2.253]], "base_severity": 0.3},
    {"name": "Manchester Rd (Town Centre)", "road": "A671/A682", "coords": [[53.786,-2.245],[53.789,-2.239],[53.791,-2.237],[53.793,-2.233]], "base_severity": 0.8},
    {"name": "Colne Rd (Burnley-Brierfield)", "road": "A682", "coords": [[53.793,-2.233],[53.797,-2.238],[53.804,-2.235],[53.812,-2.235],[53.818,-2.236]], "base_severity": 0.7},
    {"name": "Cavalry Way / A671", "road": "A671", "coords": [[53.790,-2.267],[53.790,-2.260],[53.789,-2.255],[53.790,-2.252]], "base_severity": 0.5},
    {"name": "Accrington Rd (to M65 J9)", "road": "A679", "coords": [[53.785,-2.283],[53.783,-2.290],[53.779,-2.300],[53.777,-2.310]], "base_severity": 0.6},
    {"name": "Todmorden Rd (Cliviger)", "road": "A646", "coords": [[53.787,-2.245],[53.784,-2.252],[53.781,-2.257],[53.774,-2.254]], "base_severity": 0.3},
    {"name": "Padiham Rd", "road": "A671", "coords": [[53.790,-2.267],[53.793,-2.278],[53.797,-2.291],[53.800,-2.312]], "base_severity": 0.5},
    {"name": "A56 (Rawtenstall Rd)", "road": "A56", "coords": [[53.775,-2.330],[53.764,-2.338],[53.749,-2.342]], "base_severity": 0.4},
]


def fetch_osm_infrastructure() -> dict:
    """Fetch traffic signals and pedestrian crossings from OpenStreetMap Overpass API."""
    bbox = "53.72,-2.36,53.83,-2.15"

    # Traffic signals
    signals_query = f'[out:json][timeout:30];node["highway"="traffic_signals"]({bbox});out;'
    signals_url = f"https://overpass-api.de/api/interpreter?data={signals_query}"

    signals = []
    try:
        req = Request(signals_url, headers={"User-Agent": "TomPickup-Traffic-ETL/1.0"})
        with urlopen(req, timeout=30) as resp:
            data = json.loads(resp.read().decode("utf-8"))
        for el in data.get("elements", []):
            signals.append({"lat": el["lat"], "lng": el["lon"], "id": el["id"]})
        print(f"  OSM traffic signals: {len(signals)}")
    except Exception as e:
        print(f"  OSM signals fetch failed: {e}", file=sys.stderr)

    time.sleep(1)  # Rate limit Overpass

    # Signal crossings
    crossings_query = f'[out:json][timeout:30];node["highway"="crossing"]["crossing"~"signal|traffic_signals"]({bbox});out;'
    crossings_url = f"https://overpass-api.de/api/interpreter?data={crossings_query}"

    crossings = []
    try:
        req = Request(crossings_url, headers={"User-Agent": "TomPickup-Traffic-ETL/1.0"})
        with urlopen(req, timeout=30) as resp:
            data = json.loads(resp.read().decode("utf-8"))
        for el in data.get("elements", []):
            tags = el.get("tags", {})
            crossings.append({
                "lat": el["lat"], "lng": el["lon"], "id": el["id"],
                "type": tags.get("crossing_ref", tags.get("crossing", "signal")),
                "bicycle": tags.get("bicycle") == "yes" or tags.get("crossing_ref") == "toucan",
            })
        print(f"  OSM signal crossings: {len(crossings)}")
    except Exception as e:
        print(f"  OSM crossings fetch failed: {e}", file=sys.stderr)

    return {"signals": signals, "crossings": crossings}


def haversine_m(lat1, lng1, lat2, lng2):
    """Haversine distance in metres."""
    from math import radians, sin, cos, sqrt, atan2
    R = 6371000
    dlat = radians(lat2 - lat1)
    dlng = radians(lng2 - lng1)
    a = sin(dlat/2)**2 + cos(radians(lat1)) * cos(radians(lat2)) * sin(dlng/2)**2
    return R * 2 * atan2(sqrt(a), sqrt(1-a))


def cluster_signals_to_junctions(signals: list, radius_m: float = 50) -> list:
    """Cluster nearby traffic signals into junctions."""
    used = set()
    clusters = []

    for i, s in enumerate(signals):
        if i in used:
            continue
        cluster = [s]
        used.add(i)
        for j, s2 in enumerate(signals):
            if j in used:
                continue
            if haversine_m(s["lat"], s["lng"], s2["lat"], s2["lng"]) < radius_m:
                cluster.append(s2)
                used.add(j)
        avg_lat = sum(n["lat"] for n in cluster) / len(cluster)
        avg_lng = sum(n["lng"] for n in cluster) / len(cluster)
        clusters.append({"lat": round(avg_lat, 6), "lng": round(avg_lng, 6), "signal_count": len(cluster)})

    return clusters


def classify_restriction(rw: dict) -> dict:
    """Classify a roadwork's actual traffic restriction impact.

    Returns dict with:
      restriction_class: 'full_closure' | 'lane_restriction' | 'minor'
      capacity_reduction: 0.0-1.0 (1.0 = fully closed)
      impact_label: human-readable impact description
    """
    tc = (rw.get("traffic_constriction") or "").lower()
    mt = (rw.get("management_type") or "").lower()
    rest = (rw.get("restrictions") or "").lower()
    impact = (rw.get("impact") or "").lower()
    severity = (rw.get("severity") or "").lower()

    # Full closure: road is blocked
    if tc == "roadblocked" or mt == "roadclosed":
        return {
            "restriction_class": "full_closure",
            "capacity_reduction": 1.0,
            "impact_label": "Road closed — all traffic diverted",
        }

    # Lane restriction: one or more lanes blocked, traffic flows with delays
    if tc == "lanesblocked" or mt == "laneclosures":
        # Temp traffic lights typically means one lane open, alternating traffic
        if rest == "temporarytrafficlights":
            return {
                "restriction_class": "lane_restriction",
                "capacity_reduction": 0.6,  # ~60% capacity loss with temp lights
                "impact_label": "Lane closed — temp traffic lights, alternating flow",
            }
        return {
            "restriction_class": "lane_restriction",
            "capacity_reduction": 0.4,
            "impact_label": "Lane closure — reduced capacity",
        }

    # Temp traffic lights without explicit lane/road blocking
    if rest == "temporarytrafficlights":
        return {
            "restriction_class": "lane_restriction",
            "capacity_reduction": 0.5,
            "impact_label": "Temporary traffic lights — alternating flow",
        }

    # Infer from impact/severity if constriction data missing
    if impact == "verylongdelays" and severity == "high":
        return {
            "restriction_class": "lane_restriction",
            "capacity_reduction": 0.4,
            "impact_label": "Significant disruption expected",
        }

    if impact in ("longdelays", "verylongdelays"):
        return {
            "restriction_class": "lane_restriction",
            "capacity_reduction": 0.3,
            "impact_label": "Lane/carriageway restriction likely",
        }

    # Minor: no explicit closure or lane block
    return {
        "restriction_class": "minor",
        "capacity_reduction": 0.1,
        "impact_label": "Minor works — footway or verge only",
    }


def build_junction_congestion_index(junctions: list, count_points: list,
                                     roadworks: list, fms_reports: list) -> list:
    """Calculate Junction Congestion Index (JCI) for each named junction.

    Now restriction-aware: full road closure near a junction scores much higher
    than a minor verge dig. Each nearby roadwork contributes based on its actual
    capacity reduction, not just a flat count.

    JCI = (traffic_volume × 0.25) + (signal_complexity × 0.10)
        + (roadworks_impact × 0.30) + (school_proximity × 0.15)
        + (defect_density × 0.10) + (crossing_frequency × 0.05)
        + (closure_severity_bonus × 0.05)

    Scale: 0-100. Higher = more congested.
    """
    scored = []
    for jn in junctions:
        jlat, jlng = jn["lat"], jn["lng"]

        # Traffic volume score: nearest DfT count point AADF
        nearest_aadf = 0
        for cp in count_points:
            if cp.get("aadf") and cp.get("lat") and cp.get("lng"):
                d = haversine_m(jlat, jlng, cp["lat"], cp["lng"])
                if d < 1000:
                    vol = cp["aadf"].get("all_motor_vehicles", 0) or 0
                    if vol > nearest_aadf:
                        nearest_aadf = vol
        vol_score = min(nearest_aadf / 600, 100)

        # Signal density (approaches/complexity)
        signal_score = min(jn.get("approaches", 3) * 15, 100)

        # Roadworks proximity — now restriction-aware
        # Each nearby roadwork contributes based on its capacity_reduction and distance
        rw_impact_total = 0.0
        nearby_closures = 0
        nearby_lane_restrictions = 0
        nearby_minor = 0
        nearby_works = []
        for rw in roadworks:
            rlat = rw.get("lat")
            rlng = rw.get("lng")
            if not rlat or not rlng:
                continue
            try:
                d = haversine_m(jlat, jlng, float(rlat), float(rlng))
            except (ValueError, TypeError):
                continue
            if d < 500:  # Within 500m of junction
                restriction = classify_restriction(rw)
                cap_red = restriction["capacity_reduction"]
                # Distance decay: closer works have more impact
                distance_weight = max(0, 1.0 - d / 500)
                rw_impact_total += cap_red * distance_weight * 100

                rc = restriction["restriction_class"]
                if rc == "full_closure":
                    nearby_closures += 1
                elif rc == "lane_restriction":
                    nearby_lane_restrictions += 1
                else:
                    nearby_minor += 1

                nearby_works.append({
                    "road": rw.get("road", ""),
                    "operator": rw.get("operator", ""),
                    "class": rc,
                    "capacity_reduction": cap_red,
                    "distance_m": int(d),
                })

        rw_score = min(rw_impact_total, 100)

        # Closure severity bonus: full closures near a junction are catastrophic
        closure_bonus = min(nearby_closures * 30 + nearby_lane_restrictions * 10, 100)

        # School proximity
        school_score = 0
        for sc in SCHOOL_LOCATIONS:
            d = haversine_m(jlat, jlng, sc["lat"], sc["lng"])
            if d < 500:
                school_score = min(school_score + sc["pupils"] / 10, 100)

        # Defect reports (FMS) within 300m
        defect_count = 0
        for r in fms_reports:
            rlat = r.get("lat")
            rlng = r.get("lng") or r.get("long")
            if rlat and rlng:
                try:
                    if haversine_m(jlat, jlng, float(rlat), float(rlng)) < 300:
                        defect_count += 1
                except (ValueError, TypeError):
                    pass
        defect_score = min(defect_count * 15, 100)

        # Crossing frequency
        crossing_score = min(jn.get("lanes", 2) * 20, 100)

        jci = round(
            vol_score * 0.25 + signal_score * 0.10 + rw_score * 0.30
            + school_score * 0.15 + defect_score * 0.10 + crossing_score * 0.05
            + closure_bonus * 0.05
        , 1)

        level = "critical" if jci >= 70 else "high" if jci >= 50 else "moderate" if jci >= 30 else "low"

        scored.append({
            **jn,
            "jci": min(jci, 100),
            "jci_level": level,
            "traffic_volume": nearest_aadf,
            "nearby_schools": sum(1 for sc in SCHOOL_LOCATIONS if haversine_m(jlat, jlng, sc["lat"], sc["lng"]) < 500),
            "nearby_defects": defect_count,
            "nearby_closures": nearby_closures,
            "nearby_lane_restrictions": nearby_lane_restrictions,
            "nearby_minor_works": nearby_minor,
            "nearby_works": nearby_works[:5],  # Top 5 for display
        })

    return sorted(scored, key=lambda x: -x["jci"])


def build_corridor_scores(corridors: list, count_points: list, roadworks_count: int) -> list:
    """Score congestion severity for each road corridor."""
    scored = []
    for cor in corridors:
        # Average volume along corridor from nearest count points
        total_vol = 0
        vol_count = 0
        for coord in cor["coords"]:
            for cp in count_points:
                if cp.get("aadf") and cp.get("lat") and cp.get("lng"):
                    d = haversine_m(coord[0], coord[1], cp["lat"], cp["lng"])
                    if d < 800:
                        vol = cp["aadf"].get("all_motor_vehicles", 0) or 0
                        if vol > 0:
                            total_vol += vol
                            vol_count += 1
                            break

        avg_vol = total_vol / max(vol_count, 1)
        vol_factor = min(avg_vol / 50000, 1.0)

        # Combine with base severity and roadworks
        rw_factor = min(roadworks_count * 0.02, 0.3)
        severity = min(cor["base_severity"] + vol_factor * 0.3 + rw_factor, 1.0)

        level = "critical" if severity >= 0.8 else "severe" if severity >= 0.6 else "high" if severity >= 0.4 else "moderate" if severity >= 0.2 else "low"

        scored.append({
            "name": cor["name"],
            "road": cor["road"],
            "coords": cor["coords"],
            "severity": round(severity, 2),
            "level": level,
            "avg_daily_vehicles": int(avg_vol),
        })

    return sorted(scored, key=lambda x: -x["severity"])


def build_options_appraisal(junctions: list, corridors: list, roadworks_count: int) -> list:
    """Build scored options appraisal for LCC congestion interventions."""
    critical_junctions = sum(1 for j in junctions if j.get("jci", 0) >= 70)
    severe_corridors = sum(1 for c in corridors if c.get("severity", 0) >= 0.6)

    options = [
        {
            "intervention": "Lane Rental Scheme Adoption",
            "description": "Charge utilities up to £2,500/day for occupying traffic-sensitive roads during peak hours. Financial incentive for off-peak/night working.",
            "legal_basis": "Traffic Management Act 2004 s32-39; Lane Rental Guidance 2025",
            "congestion_reduction_pct": 15,
            "cost": "Revenue-neutral (charges fund scheme)",
            "timeline": "12-18 months (DfT approval required)",
            "priority": "high",
            "status": "LCC actively exploring (decision expected 2026)",
            "evidence": "Kent/Surrey schemes reduced peak-hour utility works by 60-70%",
        },
        {
            "intervention": "Night Works & Off-Peak Scheduling",
            "description": "Mandate utility works on traffic-sensitive roads between 8pm-6am using permit conditions. S56 NRSWA timing directions.",
            "legal_basis": "NRSWA 1991 s56; Lancashire Permit Scheme conditions",
            "congestion_reduction_pct": 20,
            "cost": "Marginal (permit admin only)",
            "timeline": "Immediate (existing powers)",
            "priority": "critical",
            "status": "Partially implemented — not consistently enforced",
            "evidence": "TfL night works policy reduced daytime disruption by 30%+ on A-roads",
        },
        {
            "intervention": "Coordinated Phasing of Major Works",
            "description": "Refuse concurrent permits on linked corridors. Stagger Levelling Up, utility, and maintenance works sequentially not simultaneously.",
            "legal_basis": "NRSWA 1991 s59 (duty to coordinate); TMA 2004 s16 (Network Management Duty); Co-ordination Code 6th Ed 2025",
            "congestion_reduction_pct": 25,
            "cost": "£0 (coordination effort only)",
            "timeline": "Immediate",
            "priority": "critical",
            "status": f"FAILING — {roadworks_count} concurrent works causing gridlock",
            "evidence": "Feb-Mar 2026 Manchester Rd + Colne Rd + Hammerton St simultaneous closure = 'utterly unmanageable'",
        },
        {
            "intervention": "Section 58 Resurfacing Protection",
            "description": "After resurfacing a road, prohibit further excavation for up to 12 months. Prevents utilities immediately digging up new surfaces.",
            "legal_basis": "NRSWA 1991 s58, s58A (inserted by TMA 2004 s52)",
            "congestion_reduction_pct": 8,
            "cost": "£0 (regulatory power)",
            "timeline": "Immediate (existing power)",
            "priority": "moderate",
            "status": "Underused by LCC",
            "evidence": "Average cost of re-opening new road surface: £15,000-£50,000 per dig",
        },
        {
            "intervention": "Doubled FPN Enforcement",
            "description": "Issue £240 Fixed Penalty Notices for every permit breach (working without permit, overrunning, poor signing). Doubled from Jan 2026.",
            "legal_basis": "Street Works (Charges and Penalties) Regulations 2025 (SI 2025/1074), in force 5 Jan 2026",
            "congestion_reduction_pct": 5,
            "cost": "Revenue-generating",
            "timeline": "Immediate (regulations now in force)",
            "priority": "moderate",
            "status": "New powers available — enforcement rate unknown",
            "evidence": "FPNs doubled to £240 (£160 early payment). Working without permit: £500",
        },
        {
            "intervention": "Section 74 Overrun Charges (Weekends)",
            "description": "Charge utilities up to £10,000/day for overrunning works, now including weekends and bank holidays (loophole closed Jan 2026).",
            "legal_basis": "NRSWA 1991 s74; 2025 Regulations extending to all days",
            "congestion_reduction_pct": 10,
            "cost": "Revenue-generating",
            "timeline": "Immediate (new regulations in force)",
            "priority": "high",
            "status": "Weekend charging newly available from 5 Jan 2026",
            "evidence": "Maximum daily charge: £10,000 (traffic-sensitive) + £2,500 (lane rental) = £12,500/day",
        },
        {
            "intervention": "SCOOT/MOVA Adaptive Signal Control",
            "description": f"Upgrade {critical_junctions} critical junctions to adaptive signal control (SCOOT urban networks or MOVA isolated junctions). Real-time green time optimisation.",
            "legal_basis": "TMA 2004 s16 Network Management Duty",
            "congestion_reduction_pct": 12,
            "cost": "£50-80K per junction (MOVA), £200K+ for SCOOT corridor",
            "timeline": "6-18 months per junction",
            "priority": "high" if critical_junctions >= 3 else "moderate",
            "status": "Not deployed at most Burnley junctions",
            "evidence": "MOVA typically increases junction capacity by 10-15%. SCOOT reduces delays by 12% average",
        },
        {
            "intervention": "Variable Message Signs (VMS)",
            "description": "Install VMS boards on M65 approaches and key A-roads to warn of delays, suggest alternative routes, and show real-time journey times.",
            "legal_basis": "Highways Act 1980; TMA 2004 s16",
            "congestion_reduction_pct": 8,
            "cost": "£30-60K per sign (solar/connected)",
            "timeline": "3-6 months",
            "priority": "moderate",
            "status": "No permanent VMS in Burnley area",
            "evidence": "VMS reduces re-routing response time from 15min to 2min; 8-12% peak flow redistribution",
        },
        {
            "intervention": "Protected Street Designations",
            "description": "Designate M65 approaches and town centre A-roads as Protected Streets under s61 NRSWA. Refuse new utility apparatus where alternatives exist.",
            "legal_basis": "NRSWA 1991 s61",
            "congestion_reduction_pct": 5,
            "cost": "£0 (designation process)",
            "timeline": "3-6 months",
            "priority": "moderate",
            "status": "No designations currently in Burnley",
            "evidence": "All motorways automatically protected. A-roads can be designated if strategic traffic need demonstrated",
        },
    ]

    # Score each option
    for opt in options:
        impact = opt["congestion_reduction_pct"]
        if opt["priority"] == "critical":
            urgency = 3
        elif opt["priority"] == "high":
            urgency = 2
        else:
            urgency = 1
        opt["overall_score"] = round(impact * urgency / 3, 1)

    return sorted(options, key=lambda x: -x["overall_score"])


def build_deferral_recommendations(roadworks: list, junctions: list, corridors: list,
                                    today: date, fixtures: list) -> list:
    """Identify LCC-controlled works that could be deferred to ease congestion.

    Scoring criteria:
    1. Is the work LCC-controlled? (only LCC can defer its own works)
    2. Proximity to critical/high-JCI junctions
    3. Overlap with other concurrent works on the same corridor
    4. Severity of restriction (full closure >> lane restriction >> minor)
    5. School term / match day overlap
    6. Work category (emergency/urgent cannot be deferred)

    Returns recommendations sorted by deferral benefit (highest first).
    """
    today_str = today.isoformat()
    in_term = is_term_time(today)

    # Match days in next 14 days
    match_dates = set()
    for f in fixtures:
        if f.get("is_home") and f.get("date"):
            try:
                fd = date.fromisoformat(f["date"])
                if 0 <= (fd - today).days <= 14:
                    match_dates.add(f["date"])
            except ValueError:
                pass

    # Build critical junction lookup
    critical_junctions = [j for j in junctions if j.get("jci", 0) >= 50]

    recommendations = []
    for rw in roadworks:
        operator = rw.get("operator", "")

        # Only recommend deferrals for LCC's own works
        # Utilities can receive s56 timing directions (separate function)
        is_lcc = "lancashire" in operator.lower() or "lcc" in operator.lower()

        # Emergency/urgent works cannot be deferred
        category = (rw.get("category") or "").lower()
        if "emergency" in category or "urgent" in category:
            continue

        # Major project works cannot be deferred — funding milestones
        major = is_major_project_work(rw)
        if major and major.get("cannot_defer"):
            continue

        # Parse dates
        start = rw.get("start_date", "")[:10]
        end = rw.get("end_date", "")[:10]
        if not start:
            continue

        # Only consider works that haven't finished
        if end and end < today_str:
            continue

        restriction = classify_restriction(rw)
        cap_red = restriction["capacity_reduction"]
        rc = restriction["restriction_class"]

        # Skip truly minor works — not worth recommending deferral
        if cap_red < 0.2:
            continue

        rlat = rw.get("lat")
        rlng = rw.get("lng")
        if not rlat or not rlng:
            continue

        # 1. Junction impact score (0-40)
        junction_impact = 0
        affected_junctions = []
        for j in critical_junctions:
            try:
                d = haversine_m(float(rlat), float(rlng), j["lat"], j["lng"])
            except (ValueError, TypeError):
                continue
            if d < 600:
                jci = j.get("jci", 0)
                proximity_factor = max(0, 1.0 - d / 600)
                junction_impact += jci * proximity_factor * 0.5
                affected_junctions.append({
                    "name": j["name"],
                    "jci": j["jci"],
                    "distance_m": int(d),
                })
        junction_score = min(junction_impact, 40)

        # 2. Restriction severity score (0-25)
        severity_score = cap_red * 25

        # 3. Concurrent works clash score (0-20)
        concurrent_count = 0
        clashing_works = []
        for other in roadworks:
            if other.get("id") == rw.get("id"):
                continue
            olat = other.get("lat")
            olng = other.get("lng")
            if not olat or not olng:
                continue
            try:
                d = haversine_m(float(rlat), float(rlng), float(olat), float(olng))
            except (ValueError, TypeError):
                continue
            # Works within 400m are on the same corridor segment
            if d < 400:
                other_start = other.get("start_date", "")[:10]
                other_end = other.get("end_date", "")[:10]
                # Check temporal overlap
                if other_start and other_end:
                    if not (other_end < start or (end and other_start > end)):
                        concurrent_count += 1
                        other_restriction = classify_restriction(other)
                        clashing_works.append({
                            "road": other.get("road", ""),
                            "operator": other.get("operator", ""),
                            "class": other_restriction["restriction_class"],
                        })
        clash_score = min(concurrent_count * 8, 20)

        # 4. School term + match day overlap (0-15)
        timing_score = 0
        timing_flags = []
        if in_term:
            timing_score += 8
            timing_flags.append("school_term")
        # Check if works span any match day
        for md in match_dates:
            if start <= md and (not end or end >= md):
                timing_score += 7
                timing_flags.append("match_day")
                break
        timing_score = min(timing_score, 15)

        total_score = round(junction_score + severity_score + clash_score + timing_score, 1)

        if total_score < 15:
            continue  # Not worth flagging

        # Build reasoning
        reasons = []
        if junction_score >= 10:
            jnames = ", ".join(j["name"] for j in affected_junctions[:2])
            reasons.append(f"Near critical junction{'s' if len(affected_junctions) > 1 else ''}: {jnames}")
        if rc == "full_closure":
            reasons.append("Full road closure — maximum disruption")
        elif rc == "lane_restriction":
            reasons.append(restriction["impact_label"])
        if clash_score > 0:
            reasons.append(f"{concurrent_count} other works within 400m — corridor overload")
        if "school_term" in timing_flags:
            reasons.append("School term — school run traffic compounds delays")
        if "match_day" in timing_flags:
            reasons.append("Burnley FC home match nearby — match day traffic spike")

        recommendation = {
            "id": rw.get("id"),
            "road": rw.get("road", ""),
            "operator": operator,
            "is_lcc_controlled": is_lcc,
            "category": rw.get("category", ""),
            "start_date": start,
            "end_date": end,
            "restriction_class": rc,
            "capacity_reduction": cap_red,
            "impact_label": restriction["impact_label"],
            "lat": rlat,
            "lng": rlng,
            "deferral_score": total_score,
            "junction_score": round(junction_score, 1),
            "severity_score": round(severity_score, 1),
            "clash_score": round(clash_score, 1),
            "timing_score": round(timing_score, 1),
            "affected_junctions": affected_junctions[:3],
            "clashing_works": clashing_works[:4],
            "timing_flags": timing_flags,
            "reasons": reasons,
            "action": "DEFER" if is_lcc else "s56_TIMING_DIRECTION",
            "recommendation": (
                f"Defer to {'school holiday' if 'school_term' in timing_flags else 'off-peak period'} — "
                f"{'full closure' if rc == 'full_closure' else 'lane restriction'} "
                f"causing {int(cap_red * 100)}% capacity loss"
                if is_lcc else
                f"Issue s56 timing direction — require {'night works (8pm-6am)' if cap_red >= 0.5 else 'off-peak hours'} "
                f"to reduce {int(cap_red * 100)}% capacity impact"
            ),
        }
        recommendations.append(recommendation)

    return sorted(recommendations, key=lambda x: -x["deferral_score"])


def build_clash_detection(roadworks: list, corridors: list, today: date) -> list:
    """Detect concurrent works on the same road corridor that compound congestion.

    This is the s59 NRSWA 'duty to coordinate' analysis — LCC should not allow
    multiple concurrent works on the same strategic corridor.
    """
    today_str = today.isoformat()
    clashes = []

    for cor in corridors:
        cor_coords = cor["coords"]
        cor_works = []

        for rw in roadworks:
            rlat = rw.get("lat")
            rlng = rw.get("lng")
            if not rlat or not rlng:
                continue

            # Check if work is near this corridor
            min_dist = float("inf")
            for coord in cor_coords:
                try:
                    d = haversine_m(float(rlat), float(rlng), coord[0], coord[1])
                    min_dist = min(min_dist, d)
                except (ValueError, TypeError):
                    continue

            if min_dist < 300:  # Within 300m of corridor
                start = rw.get("start_date", "")[:10]
                end = rw.get("end_date", "")[:10]
                if end and end < today_str:
                    continue
                restriction = classify_restriction(rw)
                cor_works.append({
                    "road": rw.get("road", ""),
                    "operator": rw.get("operator", ""),
                    "start": start,
                    "end": end,
                    "restriction_class": restriction["restriction_class"],
                    "capacity_reduction": restriction["capacity_reduction"],
                    "impact_label": restriction["impact_label"],
                    "lat": rlat,
                    "lng": rlng,
                })

        if len(cor_works) >= 2:
            # Check for temporal overlap pairs
            overlapping_pairs = []
            for i, w1 in enumerate(cor_works):
                for w2 in cor_works[i+1:]:
                    s1, e1 = w1["start"], w1["end"]
                    s2, e2 = w2["start"], w2["end"]
                    if s1 and s2:
                        if not (e1 and e1 < s2) and not (e2 and e2 < s1):
                            overlapping_pairs.append((w1, w2))

            if overlapping_pairs:
                total_cap_reduction = sum(w["capacity_reduction"] for w in cor_works)
                severity = "critical" if total_cap_reduction >= 1.5 else "high" if total_cap_reduction >= 1.0 else "moderate"

                clashes.append({
                    "corridor": cor["name"],
                    "road": cor["road"],
                    "concurrent_works": len(cor_works),
                    "works": cor_works,
                    "overlapping_pairs": len(overlapping_pairs),
                    "total_capacity_reduction": round(total_cap_reduction, 2),
                    "severity": severity,
                    "s59_breach": total_cap_reduction >= 1.0,  # s59 duty to coordinate likely breached
                    "recommendation": (
                        f"s59 NRSWA breach likely — {len(cor_works)} concurrent works on {cor['name']} "
                        f"with combined {int(total_cap_reduction * 100)}% capacity reduction. "
                        f"LCC must stagger these works sequentially."
                        if total_cap_reduction >= 1.0 else
                        f"Monitor — {len(cor_works)} works on {cor['name']}, "
                        f"combined {int(total_cap_reduction * 100)}% capacity impact."
                    ),
                })

    return sorted(clashes, key=lambda x: -x["total_capacity_reduction"])


def build_timing_recommendations(roadworks: list, junctions: list, today: date) -> list:
    """Generate s56 timing direction recommendations for utility works.

    LCC has power under NRSWA 1991 s56 to direct utilities when they can work.
    This identifies works that should receive timing directions for off-peak/night working.
    """
    recs = []
    critical_junctions = [j for j in junctions if j.get("jci", 0) >= 50]

    for rw in roadworks:
        operator = rw.get("operator", "")
        # s56 only applies to utility works, not LCC's own
        if "lancashire" in operator.lower():
            continue

        restriction = classify_restriction(rw)
        cap_red = restriction["capacity_reduction"]
        if cap_red < 0.3:
            continue  # Minor works don't need timing directions

        rlat = rw.get("lat")
        rlng = rw.get("lng")
        if not rlat or not rlng:
            continue

        # Check junction proximity
        near_critical = False
        nearest_junction = None
        for j in critical_junctions:
            try:
                d = haversine_m(float(rlat), float(rlng), j["lat"], j["lng"])
            except (ValueError, TypeError):
                continue
            if d < 500:
                near_critical = True
                nearest_junction = j["name"]
                break

        if not near_critical and cap_red < 0.5:
            continue  # Only flag if near critical junction OR major restriction

        category = (rw.get("category") or "").lower()
        if "emergency" in category:
            continue  # Emergency works exempt from timing directions

        # Determine recommended timing
        if cap_red >= 0.6:
            timing = "Night works only (20:00-06:00)"
            urgency = "critical"
        elif cap_red >= 0.4:
            timing = "Off-peak hours (09:30-15:30 or 19:00-07:00)"
            urgency = "high"
        else:
            timing = "Avoid AM/PM peaks (not 07:30-09:30 or 16:00-18:30)"
            urgency = "moderate"

        recs.append({
            "road": rw.get("road", ""),
            "operator": operator,
            "restriction_class": restriction["restriction_class"],
            "capacity_reduction": cap_red,
            "impact_label": restriction["impact_label"],
            "near_critical_junction": nearest_junction,
            "timing_direction": timing,
            "urgency": urgency,
            "legal_basis": "NRSWA 1991 s56 — Directions as to timing of street works",
        })

    return sorted(recs, key=lambda x: -x["capacity_reduction"])


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

    print(f"Traffic Intelligence ETL — {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

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

    # 4. Load existing roadworks — full records for intelligent analysis
    rw_path = os.path.join(SCRIPT_DIR, "..", "public", "data", "roadworks.json")
    active_roadworks = 0
    roadworks_records = []
    try:
        with open(rw_path) as f:
            rw = json.load(f)
        active_roadworks = rw.get("stats", {}).get("works_started", 0)
        roadworks_records = rw.get("roadworks", [])
    except Exception:
        pass

    # 5. OSM traffic infrastructure
    print("\n4. Fetching OSM traffic infrastructure...")
    infra = fetch_osm_infrastructure()
    signal_clusters = cluster_signals_to_junctions(infra["signals"])
    print(f"  Clustered {len(infra['signals'])} signals into {len(signal_clusters)} junction groups")

    # 6. Load FixMyStreet reports for defect proximity
    fms_path = os.path.join(SCRIPT_DIR, "..", "public", "data", "fixmystreet.json")
    fms_reports = []
    try:
        with open(fms_path) as f:
            fms = json.load(f)
        fms_reports = fms.get("reports", [])
    except Exception:
        pass

    # 7. Build junction congestion index (now restriction-aware)
    print("\n5. Building Junction Congestion Index (restriction-aware)...")
    jci_results = build_junction_congestion_index(NAMED_JUNCTIONS, count_points, roadworks_records, fms_reports)
    for j in jci_results[:5]:
        closures = j.get("nearby_closures", 0)
        lanes = j.get("nearby_lane_restrictions", 0)
        print(f"  {j['name']}: JCI {j['jci']} ({j['jci_level']}) — {closures} closures, {lanes} lane restrictions nearby")

    # 8. Build corridor congestion scores
    print("\n6. Building corridor congestion scores...")
    corridor_results = build_corridor_scores(CONGESTION_CORRIDORS, count_points, active_roadworks)
    for c in corridor_results[:3]:
        print(f"  {c['name']}: severity {c['severity']} ({c['level']})")

    # 9. Deferral recommendations
    print("\n7. Building deferral recommendations...")
    deferrals = build_deferral_recommendations(roadworks_records, jci_results, corridor_results, today, fixtures)
    lcc_deferrals = [d for d in deferrals if d["is_lcc_controlled"]]
    utility_deferrals = [d for d in deferrals if not d["is_lcc_controlled"]]
    print(f"  {len(lcc_deferrals)} LCC works recommended for deferral")
    print(f"  {len(utility_deferrals)} utility works recommended for s56 timing direction")
    for d in deferrals[:3]:
        print(f"    {d['road']} ({d['operator']}): score {d['deferral_score']} — {d['restriction_class']}")

    # 10. Clash detection (s59 coordination duty)
    print("\n8. Detecting corridor clashes (s59 duty to coordinate)...")
    clashes = build_clash_detection(roadworks_records, CONGESTION_CORRIDORS, today)
    for cl in clashes:
        print(f"  {cl['corridor']}: {cl['concurrent_works']} concurrent works, {cl['severity']} severity")
    if not clashes:
        print("  No corridor clashes detected")

    # 11. s56 Timing direction recommendations
    print("\n9. Building s56 timing recommendations...")
    timing_recs = build_timing_recommendations(roadworks_records, jci_results, today)
    print(f"  {len(timing_recs)} works need timing directions")

    # 12. Restriction classification summary
    restriction_summary = {"full_closure": 0, "lane_restriction": 0, "minor": 0}
    for rw in roadworks_records:
        rc = classify_restriction(rw)["restriction_class"]
        restriction_summary[rc] += 1
    print(f"\n10. Restriction summary: {restriction_summary['full_closure']} closures, "
          f"{restriction_summary['lane_restriction']} lane restrictions, {restriction_summary['minor']} minor")

    # 13. Options appraisal
    print("\n11. Building options appraisal...")
    options = build_options_appraisal(jci_results, corridor_results, active_roadworks)
    print(f"  {len(options)} interventions scored")

    # 14. Build impact heatmap
    print("\n12. Building traffic impact heatmap...")
    heatmap = build_impact_heatmap(today, active_roadworks, fixtures)

    # 11. School term status
    term_status = {
        "in_term": is_term_time(today),
        "current_term": get_current_term(today),
        "terms": SCHOOL_TERMS,
    }

    # 12. Key road summaries
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

    # 13. Traffic flow profiles
    profiles = {
        "weekday": WEEKDAY_PROFILE,
        "weekend": WEEKEND_PROFILE,
        "school_run_overlay": SCHOOL_RUN_OVERLAY,
        "match_day_3pm": MATCH_DAY_OVERLAY_3PM,
        "match_day_evening": MATCH_DAY_OVERLAY_EVENING,
    }

    # Tag roadworks with restriction classification + major project flags
    classified_works = []
    for rw in roadworks_records:
        restriction = classify_restriction(rw)
        major = is_major_project_work(rw)
        classified_works.append({
            "id": rw.get("id"),
            "road": rw.get("road", ""),
            "operator": rw.get("operator", ""),
            "restriction_class": restriction["restriction_class"],
            "capacity_reduction": restriction["capacity_reduction"],
            "impact_label": restriction["impact_label"],
            "major_project": major["name"] if major else None,
            "cannot_defer": bool(major and major.get("cannot_defer")),
        })

    output = {
        "meta": {
            "source": "DfT Road Traffic API + OSM Overpass + LCC School Terms",
            "generated": datetime.now(timezone.utc).isoformat(),
            "fetch_time_ms": int((time.time() - t0) * 1000),
            "active_roadworks": active_roadworks,
            "restriction_summary": restriction_summary,
        },
        "count_points": count_points,
        "key_roads": key_roads_list[:20],
        "fixtures": fixtures,
        "school_terms": term_status,
        "heatmap": heatmap,
        "profiles": profiles,
        "infrastructure": {
            "traffic_signals": signal_clusters,
            "signal_crossings": infra["crossings"],
            "schools": SCHOOL_LOCATIONS,
            "turf_moor": TURF_MOOR,
        },
        "congestion_model": {
            "junctions": jci_results,
            "corridors": corridor_results,
            "options_appraisal": options,
        },
        "operational_intelligence": {
            "deferral_recommendations": deferrals[:20],
            "lcc_deferrals": lcc_deferrals[:10],
            "utility_s56_recommendations": utility_deferrals[:10],
            "corridor_clashes": clashes,
            "timing_directions": timing_recs[:15],
            "classified_works": classified_works,
            "major_projects": MAJOR_PROJECTS,
        },
    }

    os.makedirs(os.path.dirname(os.path.abspath(output_path)), exist_ok=True)

    with open(output_path, "w") as f:
        json.dump(output, f, separators=(",", ":"))

    size_kb = os.path.getsize(output_path) / 1024
    elapsed = time.time() - t0

    print(f"\nDone in {elapsed:.1f}s")
    print(f"Output: {output_path} ({size_kb:.1f} KB)")
    print(f"Infrastructure: {len(infra['signals'])} signals, {len(infra['crossings'])} crossings, {len(signal_clusters)} junctions")
    print(f"Congestion: {len(jci_results)} junctions scored, {len(corridor_results)} corridors")
    print(f"Count points: {len(count_points)}, Key roads: {len(key_roads_list)}")
    print(f"Fixtures: {len(fixtures)}, School term: {'Yes' if term_status['in_term'] else 'No'}")
    print(f"Active roadworks: {active_roadworks} ({restriction_summary['full_closure']} closures, {restriction_summary['lane_restriction']} lane restrictions)")
    print(f"Deferrals: {len(lcc_deferrals)} LCC, {len(utility_deferrals)} utility s56. Clashes: {len(clashes)} corridors")
    print(f"Options: {len(options)}, Timing recs: {len(timing_recs)}")
    if heatmap:
        print(f"7-day peak: {max(h['peak_flow'] for h in heatmap):.1f}% at {max(heatmap, key=lambda h: h['peak_flow'])['day']}")


if __name__ == "__main__":
    main()
