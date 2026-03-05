#!/usr/bin/env python3
"""
Traffic Intelligence ETL — Central Lancashire Highways System (under LCC).

Comprehensive traffic model covering all 12 Lancashire districts.
Config-driven via highways_config.json — no hardcoded values.

Data sources:
- DfT Road Traffic Statistics API (AADF counts, count points)
- OpenStreetMap Overpass API (traffic signals, pedestrian crossings)
- Lancashire school term dates (from config)
- Sport venue fixtures (all venues from config)
- Rush hour patterns (empirical UK traffic distribution from config)
- Roadworks data (from roadworks.json) — Lancashire-wide active works, TTROs
- FixMyStreet data (from fixmystreet.json) — pothole/defect reports

Congestion model outputs:
- Hourly traffic flow profiles (weekday vs weekend vs school+match)
- 7-day × 24-hour impact heatmap with congestion severity
- Junction Congestion Index (JCI) — per signalised junction scoring
- Road Corridor Congestion Score — per key road corridor
- Infrastructure map: traffic signals, signal crossings from OSM
- DfT count point AADF volumes across Lancashire
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
CONFIG_PATH = os.path.join(SCRIPT_DIR, "highways_config.json")

# --- Load config ---
with open(CONFIG_PATH) as f:
    HIGHWAYS_CONFIG = json.load(f)

# Lancashire-wide bbox from config
_lancs = HIGHWAYS_CONFIG["lancashire"]
LANCASHIRE_BBOX = _lancs["bbox"]  # {south, north, west, east}

# --- DfT Traffic API (from config) ---
DFT_BASE = HIGHWAYS_CONFIG["dft"]["base_url"]
LANCASHIRE_LA_ID = HIGHWAYS_CONFIG["dft"]["lancashire_la_id"]

# --- Traffic profiles (from config) ---
def _int_keys(d):
    """Convert string keys to int keys for profile dicts."""
    return {int(k): v for k, v in d.items()}

WEEKDAY_PROFILE = _int_keys(HIGHWAYS_CONFIG["profiles"]["weekday"])
WEEKEND_PROFILE = _int_keys(HIGHWAYS_CONFIG["profiles"]["weekend"])
SCHOOL_RUN_OVERLAY = _int_keys(HIGHWAYS_CONFIG["profiles"]["school_run_overlay"])
MATCH_DAY_OVERLAY_3PM = _int_keys(HIGHWAYS_CONFIG["profiles"]["match_day_overlay_3pm"])
MATCH_DAY_OVERLAY_EVENING = _int_keys(HIGHWAYS_CONFIG["profiles"]["match_day_overlay_evening"])

# --- School terms (from config) ---
SCHOOL_TERMS = HIGHWAYS_CONFIG["school_terms"]

# --- Aggregate all district data from config ---
ALL_SPORT_VENUES = []
ALL_MAJOR_PROJECTS = []
ALL_NAMED_JUNCTIONS_SEED = []
ALL_SCHOOLS_SEED = []
ALL_CORRIDORS_SEED = []

for _council_id, _council_cfg in HIGHWAYS_CONFIG["councils"].items():
    for v in _council_cfg.get("sport_venues", []):
        v["district"] = _council_id
        ALL_SPORT_VENUES.append(v)
    for p in _council_cfg.get("major_projects", []):
        p["district"] = _council_id
        ALL_MAJOR_PROJECTS.append(p)
    for j in _council_cfg.get("named_junctions_seed", []):
        j["district"] = _council_id
        ALL_NAMED_JUNCTIONS_SEED.append(j)
    for s in _council_cfg.get("schools_seed", []):
        s["district"] = _council_id
        ALL_SCHOOLS_SEED.append(s)
    for c in _council_cfg.get("corridors_seed", []):
        c["district"] = _council_id
        ALL_CORRIDORS_SEED.append(c)

ALL_MAJOR_EVENTS = HIGHWAYS_CONFIG.get("major_events", [])

# --- Legal keywords (from config) ---
_legal = HIGHWAYS_CONFIG.get("legal_keywords", {})


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

            if lat and lng and LANCASHIRE_BBOX["south"] < lat < LANCASHIRE_BBOX["north"] and LANCASHIRE_BBOX["west"] < lng < LANCASHIRE_BBOX["east"]:
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

    for cp_id in count_point_ids[:50]:  # Limit to avoid rate limits (raised from 30)
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


def fetch_sport_fixtures() -> list:
    """Fetch fixtures for all sport venues from config."""
    all_fixtures = []

    for venue in ALL_SPORT_VENUES:
        venue_name = venue.get("name", "Unknown")
        team = venue.get("team", "")
        urls = venue.get("fixture_urls", [])
        if not urls:
            continue

        for url in urls:
            req = Request(url, headers={"User-Agent": "Mozilla/5.0"})
            try:
                with urlopen(req, timeout=15) as resp:
                    raw = resp.read().decode("utf-8")

                data = json.loads(raw)
                if isinstance(data, list) and data:
                    fixtures = parse_fixture_json(data)
                    # Tag each fixture with venue info
                    for fix in fixtures:
                        fix["venue"] = venue_name
                        fix["venue_lat"] = venue.get("lat")
                        fix["venue_lng"] = venue.get("lng")
                        fix["venue_capacity"] = venue.get("capacity", 0)
                        fix["district"] = venue.get("district", "")
                    all_fixtures.extend(fixtures)
                    print(f"  {team} fixtures: {len(fixtures)} (from fixturedownload.com)")
                    break  # Got fixtures for this venue
            except json.JSONDecodeError:
                if "BEGIN:VCALENDAR" in raw:
                    fixtures = parse_ical(raw)
                    if fixtures:
                        for fix in fixtures:
                            fix["venue"] = venue_name
                            fix["venue_lat"] = venue.get("lat")
                            fix["venue_lng"] = venue.get("lng")
                            fix["venue_capacity"] = venue.get("capacity", 0)
                            fix["district"] = venue.get("district", "")
                        all_fixtures.extend(fixtures)
                        print(f"  {team} fixtures: {len(fixtures)} (iCal)")
                        break
            except Exception as e:
                print(f"  Fixture fetch failed for {team} ({url}): {e}", file=sys.stderr)
                continue

    if not all_fixtures:
        print("  Warning: Could not fetch any sport fixtures", file=sys.stderr)
    return sorted(all_fixtures, key=lambda x: x.get("date", ""))


def load_hardcoded_fixtures() -> list:
    """Load hardcoded fixtures from config as fallback when APIs fail."""
    hardcoded = HIGHWAYS_CONFIG.get("hardcoded_fixtures", [])
    if not hardcoded:
        return []

    today_str = date.today().isoformat()
    fixtures = []
    for f in hardcoded:
        d = f.get("date", "")
        if d < today_str:
            continue  # Skip past fixtures
        time_str = f.get("time", "15:00")
        try:
            ko_hour = int(time_str.split(":")[0])
        except (ValueError, IndexError):
            ko_hour = 15
        fixtures.append({
            "date": d,
            "time": time_str,
            "kickoff_hour": ko_hour,
            "opponent": f.get("opponent", ""),
            "summary": f"{f.get('team', '')} vs {f.get('opponent', '')}",
            "venue": f.get("venue", ""),
            "venue_lat": f.get("venue_lat"),
            "venue_lng": f.get("venue_lng"),
            "venue_capacity": f.get("capacity", 0),
            "is_home": True,  # All hardcoded fixtures are home games
            "source": "config",
        })
    return sorted(fixtures, key=lambda x: x["date"])


def get_upcoming_major_events(today: date, days_ahead: int = 90) -> list:
    """Get major events from config that are upcoming and could affect traffic."""
    cutoff = (today + timedelta(days=days_ahead)).isoformat()
    today_str = today.isoformat()
    upcoming = []
    for ev in ALL_MAJOR_EVENTS:
        dates = ev.get("dates", [])
        future_dates = [d for d in dates if today_str <= d <= cutoff]
        if future_dates:
            upcoming.append({
                "name": ev["name"],
                "dates": future_dates,
                "first_date": future_dates[0],
                "last_date": future_dates[-1],
                "lat": ev.get("lat"),
                "lng": ev.get("lng"),
                "crowd": ev.get("crowd", 0),
                "impact_radius_km": ev.get("impact_radius_km", 2),
                "roads_affected": ev.get("roads_affected", []),
                "districts": ev.get("districts", []),
                "type": ev.get("type", "event"),
                "note": ev.get("note", ""),
            })
    return sorted(upcoming, key=lambda x: x["first_date"])


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
        # Check if home team matches any of our configured venues
        is_home = any(
            home_team.lower().strip() in v.get("team", "").lower()
            for v in ALL_SPORT_VENUES
        )

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



# --- All data loaded from config ---
# Named junctions, schools, sport venues, major projects, corridors are loaded
# from highways_config.json at module level (ALL_NAMED_JUNCTIONS_SEED, etc.)
NAMED_JUNCTIONS = ALL_NAMED_JUNCTIONS_SEED
SCHOOL_LOCATIONS = ALL_SCHOOLS_SEED
MAJOR_PROJECTS = ALL_MAJOR_PROJECTS

# Bridge and structural works — legally cannot defer under s1 Highways Act 1980
BRIDGE_KEYWORDS = _legal.get("bridge_keywords", [
    "bridge", "footbridge", "viaduct", "overbridge", "underpass", "abutment",
    "parapet", "bridge deck", "bearing replacement", "structural repair",
])

# Planning obligation / development works — tied to s106/s278 agreements
PLANNING_OBLIGATION_KEYWORDS = [
    "drainage connection", "foul drainage", "off-site drainage",
    "s278", "s106", "section 278", "section 106",
    "developer contribution", "planning condition",
    "housing development", "residential development",
]

# Works-started status means physical work is underway — deferral not feasible
NON_DEFERRABLE_STATUSES = _legal.get("non_deferrable_statuses", ["Works started"])


def is_bridge_or_structural_work(rw):
    """Check if a roadwork involves bridge/structural work that cannot legally be deferred.

    Bridge works have legal obligations under s1 Highways Act 1980 (duty to maintain)
    and structural safety requirements. Mid-construction deferral is not feasible.

    Only matches on description (not road name) to avoid false positives like 'Stockbridge Road'.
    """
    desc = (rw.get("description") or "").lower()
    for kw in BRIDGE_KEYWORDS:
        if kw in desc:
            return True
    return False


def is_planning_obligation_work(rw):
    """Check if a roadwork is tied to a planning obligation (s106/s278).

    These works are required by planning conditions and cannot be deferred without
    breaching the planning agreement — the developer/contractor has a legal obligation.
    """
    desc = (rw.get("description") or "").lower()
    for kw in PLANNING_OBLIGATION_KEYWORDS:
        if kw in desc:
            return True
    return False


def is_major_project_work(rw: dict):
    """Check if a roadwork is part of a known major project.

    Detection methods (layered, any match triggers):
    1. Geographic bounds + road name match
    2. Geographic bounds + description keyword match (for side streets)
    3. Bridge/structural safety detection

    Returns the project dict if matched, None otherwise.
    Major project works cannot be deferred — they have funding milestones.
    """
    rlat = rw.get("lat")
    rlng = rw.get("lng")
    road = (rw.get("road") or "").lower()
    desc = (rw.get("description") or "").lower()
    operator = (rw.get("operator") or "").lower()

    if not rlat or not rlng:
        return None

    try:
        rlat = float(rlat)
        rlng = float(rlng)
    except (ValueError, TypeError):
        return None

    for proj in MAJOR_PROJECTS:
        # Check geographic bounds first
        if not (proj["lat_range"][0] <= rlat <= proj["lat_range"][1] and
                proj["lng_range"][0] <= rlng <= proj["lng_range"][1]):
            continue

        # Method 1: Road name match
        for proad in proj["roads"]:
            if proad.lower() in road or road in proad.lower():
                return proj

        # Method 2: Description keyword match (LCC works within bounds with LUF-style descriptions)
        if "lancashire" in operator and "description_keywords" in proj:
            keyword_hits = sum(1 for kw in proj["description_keywords"] if kw in desc)
            if keyword_hits >= 2:  # At least 2 keyword matches = likely part of programme
                return proj

    # Bridge/structural safety — cannot defer regardless of project
    if is_bridge_or_structural_work(rw):
        return {
            "name": "Bridge / Structural Safety Work",
            "description": "Bridge or structural maintenance — legal duty to maintain under s1 Highways Act 1980",
            "cannot_defer": True,
            "reason": "Structural safety — s1 Highways Act 1980 duty to maintain. Cannot defer mid-construction.",
        }

    return None

# Key congestion corridors — loaded from config (all districts)
CONGESTION_CORRIDORS = ALL_CORRIDORS_SEED


def fetch_osm_infrastructure() -> dict:
    """Fetch traffic signals and pedestrian crossings from OpenStreetMap Overpass API."""
    bbox = f"{LANCASHIRE_BBOX['south']},{LANCASHIRE_BBOX['west']},{LANCASHIRE_BBOX['north']},{LANCASHIRE_BBOX['east']}"

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


# --- Road Infrastructure Cache ---
INFRA_CACHE_PATH = os.path.join(SCRIPT_DIR, ".infra_cache.json")
INFRA_CACHE_MAX_AGE_HOURS = 24  # Re-fetch after 24 hours


def _load_infra_cache():
    """Load cached road infrastructure if fresh enough."""
    try:
        if not os.path.exists(INFRA_CACHE_PATH):
            return None
        age_hours = (time.time() - os.path.getmtime(INFRA_CACHE_PATH)) / 3600
        if age_hours > INFRA_CACHE_MAX_AGE_HOURS:
            return None
        with open(INFRA_CACHE_PATH) as f:
            cached = json.load(f)
        print(f"  Using cached road infrastructure ({age_hours:.1f}h old, {len(cached.get('traffic_signals', []))} signals)")
        return cached
    except Exception:
        return None


def _save_infra_cache(data: dict):
    """Save road infrastructure to cache file."""
    try:
        with open(INFRA_CACHE_PATH, "w") as f:
            json.dump(data, f, separators=(",", ":"))
    except Exception as e:
        print(f"  Warning: Could not save infra cache: {e}", file=sys.stderr)


def _compute_union_bbox() -> dict:
    """Compute the union bounding box across all configured councils.
    Falls back to the Lancashire-wide bbox from config."""
    councils = HIGHWAYS_CONFIG.get("councils", {})
    if not councils:
        return LANCASHIRE_BBOX

    south = min(c["bbox"]["south"] for c in councils.values() if "bbox" in c)
    north = max(c["bbox"]["north"] for c in councils.values() if "bbox" in c)
    west = min(c["bbox"]["west"] for c in councils.values() if "bbox" in c)
    east = max(c["bbox"]["east"] for c in councils.values() if "bbox" in c)
    return {"south": south, "north": north, "west": west, "east": east}


def _parse_way_center(element: dict, node_lookup: dict) -> tuple:
    """Compute the centre point of an OSM way from its node refs.
    Returns (lat, lng) or (None, None) if nodes not found."""
    nds = element.get("nodes", [])
    if not nds:
        return None, None
    lats, lngs = [], []
    for nd_id in nds:
        nd = node_lookup.get(nd_id)
        if nd:
            lats.append(nd["lat"])
            lngs.append(nd["lon"])
    if not lats:
        return None, None
    return round(sum(lats) / len(lats), 6), round(sum(lngs) / len(lngs), 6)


def fetch_road_infrastructure(bbox: dict = None) -> dict:
    """Fetch traffic lights, roundabouts, speed limits, one-way streets,
    level crossings, narrow roads, bridges, and pedestrian crossings
    from OpenStreetMap via Overpass API.

    Uses a single union query for efficiency. Results are cached to avoid
    hammering the Overpass API on repeated runs.

    Args:
        bbox: Dict with south/north/west/east. Defaults to union of all council bboxes.

    Returns:
        Dict with arrays for each infrastructure type, plus summary counts.
    """
    # Check cache first
    cached = _load_infra_cache()
    if cached is not None:
        return cached

    if bbox is None:
        bbox = _compute_union_bbox()

    bb = f"{bbox['south']},{bbox['west']},{bbox['north']},{bbox['east']}"

    # Split into node query and way query for efficiency
    # Nodes: just out; (they already have lat/lon)
    # Ways: out center; (Overpass calculates centroid, avoids massive node download)
    node_query = f"""[out:json][timeout:120];
(
  node["highway"="traffic_signals"]({bb});
  node["highway"="mini_roundabout"]({bb});
  node["railway"="level_crossing"]({bb});
  node["highway"="bus_stop"]({bb});
);
out;"""

    way_query = f"""[out:json][timeout:120];
(
  way["junction"="roundabout"]({bb});
  way["highway"~"^(primary|secondary|tertiary|trunk)$"]["maxspeed"]({bb});
  way["highway"~"^(primary|secondary|tertiary)$"]["oneway"="yes"]({bb});
  way["highway"~"^(primary|secondary|tertiary|unclassified)$"]["lanes"="1"]({bb});
  way["narrow"="yes"]({bb});
  way["maxweight"]({bb});
  way["maxheight"]({bb});
  way["bridge"="yes"]["highway"~"^(primary|secondary|tertiary|trunk|motorway)$"]({bb});
);
out center;"""

    url = "https://overpass-api.de/api/interpreter"
    headers = {
        "User-Agent": "TomPickup-Traffic-ETL/3.0",
        "Content-Type": "application/x-www-form-urlencoded",
    }
    empty_result = {
        "traffic_signals": [], "roundabouts": [], "mini_roundabouts": [],
        "speed_limits": [], "one_way_streets": [], "narrow_roads": [],
        "level_crossings": [], "weight_restrictions": [], "height_restrictions": [],
        "bridges": [], "bus_stop_count": 0,
    }

    elements = []

    # Fetch nodes (traffic signals, mini roundabouts, level crossings, bus stops)
    try:
        req = Request(url, data=f"data={node_query}".encode("utf-8"), headers=headers)
        with urlopen(req, timeout=150) as resp:
            data = json.loads(resp.read().decode("utf-8"))
        node_els = data.get("elements", [])
        print(f"  Overpass nodes: {len(node_els)} elements")
        elements.extend(node_els)
    except Exception as e:
        print(f"  ERROR fetching road infrastructure nodes from Overpass: {e}", file=sys.stderr)
        return empty_result

    # Brief pause to be polite to the API
    import time
    time.sleep(2)

    # Fetch ways (roundabouts, speed limits, one-way, narrow, bridges, restrictions)
    # Uses out center; so ways come back with center lat/lon directly
    try:
        req = Request(url, data=f"data={way_query}".encode("utf-8"), headers=headers)
        with urlopen(req, timeout=150) as resp:
            data = json.loads(resp.read().decode("utf-8"))
        way_els = data.get("elements", [])
        print(f"  Overpass ways: {len(way_els)} elements")
        elements.extend(way_els)
    except Exception as e:
        print(f"  ERROR fetching road infrastructure ways from Overpass: {e}", file=sys.stderr)
        # Still use node data if we have it
        if not elements:
            return empty_result

    print(f"  Total infrastructure elements: {len(elements)}")

    # No node_lookup needed — ways have center coords from 'out center;'

    # Classify elements
    traffic_signals = []
    roundabouts = []
    mini_roundabouts = []
    speed_limits = []
    one_way_streets = []
    narrow_roads = []
    level_crossings = []
    weight_restrictions = []
    height_restrictions = []
    bridges = []
    bus_stop_count = 0

    for el in elements:
        tags = el.get("tags", {})
        el_type = el.get("type")

        if el_type == "node":
            lat, lng = el.get("lat"), el.get("lon")
            if not lat or not lng:
                continue

            hw = tags.get("highway", "")
            rw_tag = tags.get("railway", "")

            if hw == "traffic_signals":
                traffic_signals.append({
                    "lat": round(lat, 6), "lng": round(lng, 6),
                    "id": el["id"],
                    "crossing": tags.get("crossing"),
                    "crossing_ref": tags.get("crossing_ref"),
                })
            elif hw == "mini_roundabout":
                mini_roundabouts.append({
                    "lat": round(lat, 6), "lng": round(lng, 6),
                    "id": el["id"],
                    "name": tags.get("name", ""),
                })
            elif rw_tag == "level_crossing":
                level_crossings.append({
                    "lat": round(lat, 6), "lng": round(lng, 6),
                    "id": el["id"],
                    "barrier": tags.get("crossing:barrier", tags.get("barrier", "unknown")),
                    "light": tags.get("crossing:light") == "yes",
                    "name": tags.get("name", ""),
                })
            elif hw == "bus_stop":
                bus_stop_count += 1

        elif el_type == "way":
            # 'out center;' puts center coords in el["center"]
            center = el.get("center")
            if center:
                lat, lng = center.get("lat"), center.get("lon")
            else:
                # Fallback: try node lookup (shouldn't be needed with out center)
                lat, lng = _parse_way_center(el, {})
            if lat is None:
                continue

            junction = tags.get("junction", "")
            hw = tags.get("highway", "")
            road_name = tags.get("name", tags.get("ref", ""))

            if junction == "roundabout":
                roundabouts.append({
                    "lat": lat, "lng": lng,
                    "id": el["id"],
                    "name": road_name,
                    "ref": tags.get("ref", ""),
                })
            elif tags.get("maxspeed") and hw in ("primary", "secondary", "tertiary", "trunk"):
                speed_limits.append({
                    "lat": lat, "lng": lng,
                    "id": el["id"],
                    "road": road_name,
                    "speed": tags["maxspeed"],
                    "road_class": hw,
                })
            elif tags.get("oneway") == "yes" and hw in ("primary", "secondary", "tertiary"):
                one_way_streets.append({
                    "lat": lat, "lng": lng,
                    "id": el["id"],
                    "road": road_name,
                    "road_class": hw,
                })
            elif tags.get("lanes") == "1" or tags.get("narrow") == "yes":
                narrow_roads.append({
                    "lat": lat, "lng": lng,
                    "id": el["id"],
                    "road": road_name,
                    "road_class": hw,
                    "lanes": tags.get("lanes", ""),
                    "width": tags.get("width", ""),
                })

            # Weight restrictions (can overlap with other categories)
            if tags.get("maxweight"):
                weight_restrictions.append({
                    "lat": lat, "lng": lng,
                    "id": el["id"],
                    "road": road_name,
                    "maxweight": tags["maxweight"],
                })

            # Height restrictions
            if tags.get("maxheight"):
                height_restrictions.append({
                    "lat": lat, "lng": lng,
                    "id": el["id"],
                    "road": road_name,
                    "maxheight": tags["maxheight"],
                })

            # Bridges on classified roads
            if tags.get("bridge") == "yes" and hw in ("primary", "secondary", "tertiary", "trunk", "motorway"):
                bridge_len = None
                bridges.append({
                    "lat": lat, "lng": lng,
                    "id": el["id"],
                    "road": road_name,
                    "road_class": hw,
                    "length_m": bridge_len,
                    "maxweight": tags.get("maxweight"),
                    "maxheight": tags.get("maxheight"),
                    "name": tags.get("bridge:name", tags.get("name", "")),
                })

    # Build speed limit zone counts
    speed_zone_counts = {}
    for sl in speed_limits:
        spd = sl.get("speed", "")
        # Normalise — strip "mph" suffix if present
        spd_clean = spd.replace(" mph", "").replace("mph", "").strip()
        speed_zone_counts[spd_clean] = speed_zone_counts.get(spd_clean, 0) + 1

    result = {
        "traffic_signals": traffic_signals,
        "roundabouts": roundabouts,
        "mini_roundabouts": mini_roundabouts,
        "speed_limits": speed_limits,
        "speed_limit_zones": speed_zone_counts,
        "one_way_streets": one_way_streets,
        "narrow_roads": narrow_roads,
        "level_crossings": level_crossings,
        "weight_restrictions": weight_restrictions,
        "height_restrictions": height_restrictions,
        "bridges": bridges,
        "bus_stop_count": bus_stop_count,
    }

    print(f"  Traffic signals: {len(traffic_signals)}, Roundabouts: {len(roundabouts)}, "
          f"Mini roundabouts: {len(mini_roundabouts)}")
    print(f"  Level crossings: {len(level_crossings)}, Narrow roads: {len(narrow_roads)}, "
          f"Bridges: {len(bridges)}")
    print(f"  One-way streets: {len(one_way_streets)}, Bus stops: {bus_stop_count}")
    print(f"  Weight restrictions: {len(weight_restrictions)}, Height restrictions: {len(height_restrictions)}")
    print(f"  Speed zones: {speed_zone_counts}")

    # Cache the result
    _save_infra_cache(result)

    return result


def build_infra_hotspots(road_infra: dict, roadworks: list) -> list:
    """Identify infrastructure hotspots — clusters of problematic features.

    Hotspot types:
    - signal_cluster: 3+ traffic signals within 200m (cascade delay risk)
    - narrow_bottleneck: narrow/single-lane road on classified road
    - level_crossing: each level crossing is a hotspot (severe disruption when closed)
    - bridge_restriction: weight/height restricted bridge on major road
    """
    hotspots = []

    # 1. Signal clusters — 3+ signals within 200m
    signals = road_infra.get("traffic_signals", [])
    used_signal_ids = set()
    for i, s in enumerate(signals):
        if s.get("id") in used_signal_ids:
            continue
        cluster = [s]
        for j, s2 in enumerate(signals):
            if i == j or s2.get("id") in used_signal_ids:
                continue
            if haversine_m(s["lat"], s["lng"], s2["lat"], s2["lng"]) < 200:
                cluster.append(s2)
        if len(cluster) >= 3:
            for c in cluster:
                used_signal_ids.add(c.get("id"))
            avg_lat = round(sum(c["lat"] for c in cluster) / len(cluster), 6)
            avg_lng = round(sum(c["lng"] for c in cluster) / len(cluster), 6)
            # Count nearby roadworks
            nearby_works = sum(
                1 for rw in roadworks
                if rw.get("lat") and rw.get("lng")
                and haversine_m(avg_lat, avg_lng, float(rw["lat"]), float(rw["lng"])) < 500
            )
            hotspots.append({
                "name": f"Signal cluster ({len(cluster)} signals)",
                "type": "signal_cluster",
                "lat": avg_lat, "lng": avg_lng,
                "severity": "high" if len(cluster) >= 5 else "medium",
                "detail": f"{len(cluster)} traffic signals within 200m — cascade delay risk during roadworks",
                "signal_count": len(cluster),
                "nearby_works": nearby_works,
            })

    # 2. Level crossings — each is a hotspot
    for lx in road_infra.get("level_crossings", []):
        nearby_works = sum(
            1 for rw in roadworks
            if rw.get("lat") and rw.get("lng")
            and haversine_m(lx["lat"], lx["lng"], float(rw["lat"]), float(rw["lng"])) < 500
        )
        hotspots.append({
            "name": lx.get("name") or f"Level crossing ({lx.get('barrier', 'unknown')} barrier)",
            "type": "level_crossing",
            "lat": lx["lat"], "lng": lx["lng"],
            "severity": "high",
            "detail": f"Level crossing — barrier type: {lx.get('barrier', 'unknown')}. "
                      f"Closure blocks all traffic; severe disruption if only route",
            "barrier": lx.get("barrier"),
            "nearby_works": nearby_works,
        })

    # 3. Narrow road bottlenecks on classified roads
    for nr in road_infra.get("narrow_roads", []):
        if nr.get("road_class") in ("primary", "secondary", "tertiary"):
            nearby_works = sum(
                1 for rw in roadworks
                if rw.get("lat") and rw.get("lng")
                and haversine_m(nr["lat"], nr["lng"], float(rw["lat"]), float(rw["lng"])) < 500
            )
            hotspots.append({
                "name": nr.get("road") or "Narrow road",
                "type": "narrow_bottleneck",
                "lat": nr["lat"], "lng": nr["lng"],
                "severity": "high" if nr.get("road_class") == "primary" else "medium",
                "detail": f"Single lane / narrow road on {nr.get('road_class', 'classified')} road — "
                          f"major bottleneck, no overtaking possible",
                "road_class": nr.get("road_class"),
                "nearby_works": nearby_works,
            })

    # 4. Bridges with restrictions on major roads
    for br in road_infra.get("bridges", []):
        if br.get("maxweight") or br.get("maxheight"):
            nearby_works = sum(
                1 for rw in roadworks
                if rw.get("lat") and rw.get("lng")
                and haversine_m(br["lat"], br["lng"], float(rw["lat"]), float(rw["lng"])) < 500
            )
            restrictions = []
            if br.get("maxweight"):
                restrictions.append(f"weight limit {br['maxweight']}t")
            if br.get("maxheight"):
                restrictions.append(f"height limit {br['maxheight']}m")
            hotspots.append({
                "name": br.get("name") or br.get("road") or "Bridge",
                "type": "bridge_restriction",
                "lat": br["lat"], "lng": br["lng"],
                "severity": "medium",
                "detail": f"Bridge on {br.get('road_class', 'classified')} road — {', '.join(restrictions)}",
                "road_class": br.get("road_class"),
                "length_m": br.get("length_m"),
                "nearby_works": nearby_works,
            })

    return sorted(hotspots, key=lambda x: {"high": 0, "medium": 1, "low": 2}.get(x.get("severity", "low"), 9))


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
                                     roadworks: list, fms_reports: list,
                                     road_infra: dict = None) -> list:
    """Calculate Junction Congestion Index (JCI) for each named junction.

    Now restriction-aware and infrastructure-aware: full road closure near a
    junction scores much higher than a minor verge dig. Nearby infrastructure
    (traffic signals, roundabouts, narrow roads, level crossings) adds bonus
    points reflecting real-world capacity constraints.

    JCI = (traffic_volume × 0.22) + (signal_complexity × 0.08)
        + (roadworks_impact × 0.28) + (school_proximity × 0.13)
        + (defect_density × 0.08) + (crossing_frequency × 0.05)
        + (closure_severity_bonus × 0.05) + (infra_bonus × 0.11)

    Infrastructure bonus sources:
    - Traffic signals within 100m → +5 pts (signals cause natural queuing)
    - Roundabout within 200m → +3 pts (capacity constraint)
    - Narrow road/single lane within 300m → +8 pts (major bottleneck)
    - Level crossing within 500m → +10 pts (severe disruption when closed)
    - One-way street within 200m → +2 pts (routing constraints)

    Scale: 0-100. Higher = more congested.
    """
    scored = []
    for jn in junctions:
        jlat, jlng = jn["lat"], jn["lng"]

        # Traffic volume score: nearest DfT count point AADF
        nearest_aadf = 0
        nearest_dist = float("inf")
        data_quality = "none"  # none | estimated | medium | high
        for cp in count_points:
            if cp.get("aadf") and cp.get("lat") and cp.get("lng"):
                d = haversine_m(jlat, jlng, cp["lat"], cp["lng"])
                if d < 1500:  # Expanded search radius
                    vol = cp["aadf"].get("all_motor_vehicles", 0) or 0
                    if vol > 0 and d < nearest_dist:
                        nearest_aadf = vol
                        nearest_dist = d
                        if d < 500:
                            data_quality = "high"
                        elif d < 1000:
                            data_quality = "medium"
                        else:
                            data_quality = "low"

        # B1 fix: estimate from road category when no DfT data nearby
        if nearest_aadf == 0:
            road_name = jn.get("name", "").upper()
            # Estimate AADF from road classification
            if any(road_name.startswith(p) for p in ("M6", "M55", "M61", "M65", "M66")):
                nearest_aadf = 60000  # Motorway
            elif road_name.startswith("A") and any(c.isdigit() for c in road_name[:4]):
                nearest_aadf = 15000  # A-road
            elif road_name.startswith("B") and any(c.isdigit() for c in road_name[:4]):
                nearest_aadf = 5000   # B-road
            else:
                nearest_aadf = 2000   # Unclassified
            data_quality = "estimated"

        # Confidence penalty for estimated data
        vol_score = min(nearest_aadf / 600, 100)
        if data_quality == "estimated":
            vol_score *= 0.75  # 25% penalty for estimated volumes

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

        # Infrastructure proximity bonus (from road_infra if available)
        infra_bonus_raw = 0.0
        nearby_infra_signals = 0
        nearby_infra_roundabouts = 0
        nearby_infra_narrow = 0
        nearby_infra_level_crossings = 0
        nearby_infra_oneway = 0

        if road_infra:
            # Traffic signals within 100m → +5 pts each (natural queuing)
            for sig in road_infra.get("traffic_signals", []):
                if haversine_m(jlat, jlng, sig["lat"], sig["lng"]) < 100:
                    infra_bonus_raw += 5
                    nearby_infra_signals += 1

            # Roundabouts within 200m → +3 pts each (capacity constraint)
            for rb in road_infra.get("roundabouts", []):
                if haversine_m(jlat, jlng, rb["lat"], rb["lng"]) < 200:
                    infra_bonus_raw += 3
                    nearby_infra_roundabouts += 1
            for mrb in road_infra.get("mini_roundabouts", []):
                if haversine_m(jlat, jlng, mrb["lat"], mrb["lng"]) < 200:
                    infra_bonus_raw += 2
                    nearby_infra_roundabouts += 1

            # Narrow road / single lane within 300m → +8 pts (major bottleneck)
            for nr in road_infra.get("narrow_roads", []):
                if haversine_m(jlat, jlng, nr["lat"], nr["lng"]) < 300:
                    infra_bonus_raw += 8
                    nearby_infra_narrow += 1

            # Level crossing within 500m → +10 pts (severe disruption when closed)
            for lx in road_infra.get("level_crossings", []):
                if haversine_m(jlat, jlng, lx["lat"], lx["lng"]) < 500:
                    infra_bonus_raw += 10
                    nearby_infra_level_crossings += 1

            # One-way street within 200m → +2 pts (routing constraints)
            for ow in road_infra.get("one_way_streets", []):
                if haversine_m(jlat, jlng, ow["lat"], ow["lng"]) < 200:
                    infra_bonus_raw += 2
                    nearby_infra_oneway += 1

        infra_bonus = min(infra_bonus_raw, 100)

        jci = round(
            vol_score * 0.22 + signal_score * 0.08 + rw_score * 0.28
            + school_score * 0.13 + defect_score * 0.08 + crossing_score * 0.05
            + closure_bonus * 0.05 + infra_bonus * 0.11
        , 1)

        level = "critical" if jci >= 70 else "high" if jci >= 50 else "moderate" if jci >= 30 else "low"

        scored.append({
            **jn,
            "jci": min(jci, 100),
            "jci_level": level,
            "traffic_volume": nearest_aadf,
            "data_quality": data_quality,
            "nearby_schools": sum(1 for sc in SCHOOL_LOCATIONS if haversine_m(jlat, jlng, sc["lat"], sc["lng"]) < 500),
            "nearby_defects": defect_count,
            "nearby_closures": nearby_closures,
            "nearby_lane_restrictions": nearby_lane_restrictions,
            "nearby_minor_works": nearby_minor,
            "nearby_works": nearby_works[:5],  # Top 5 for display
            "infra_signals_100m": nearby_infra_signals,
            "infra_roundabouts_200m": nearby_infra_roundabouts,
            "infra_narrow_300m": nearby_infra_narrow,
            "infra_level_crossings_500m": nearby_infra_level_crossings,
            "infra_oneway_200m": nearby_infra_oneway,
            "infra_bonus": round(infra_bonus, 1),
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


def estimate_road_traffic(road_name: str, description: str = "") -> int:
    """Estimate daily traffic volume from road name when no DfT data available.

    Returns estimated AADT (Annual Average Daily Traffic).
    Used to filter out genuinely low-traffic backstreets from high-priority recommendations.
    """
    if not road_name:
        return 500  # Unknown — conservative estimate

    rn = road_name.upper().strip()
    desc = (description or "").lower()

    # Motorway
    if rn.startswith("M") and rn[1:2].isdigit():
        return 80000

    # A-road
    if rn.startswith("A") and rn[1:2].isdigit():
        return 15000

    # B-road
    if rn.startswith("B") and rn[1:2].isdigit():
        return 5000

    # Indicators of low-traffic streets
    low_traffic_indicators = [
        "rear of", "back of", "alley", "lane", "court", "close", "mews",
        "cul-de-sac", "terrace", "grove", "place", "rise", "walk",
        "crescent", "gardens", "square", "yard", "passage",
        "footpath", "footway", "bridleway", "access road", "service road",
    ]

    road_lower = road_name.lower()
    desc_lower = desc.lower()

    for indicator in low_traffic_indicators:
        if indicator in road_lower or indicator in desc_lower:
            return 200  # Very low traffic

    # Common high-traffic road name patterns
    high_traffic = ["road", "street", "avenue", "way", "drive", "boulevard", "highway"]
    for ht in high_traffic:
        if road_lower.endswith(ht) or road_lower.endswith(ht + " "):
            return 3000  # Moderate

    return 1500  # Default residential


def build_deferral_recommendations(roadworks: list, junctions: list, corridors: list,
                                    today: date, fixtures: list, major_events: list = None,
                                    road_infra: dict = None) -> list:
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

    # Match days in next 14 days — track venue location for proximity checks
    match_events = []  # [{date, venue_lat, venue_lng, venue_capacity, venue}]
    for f in fixtures:
        if f.get("is_home") and f.get("date"):
            try:
                fd = date.fromisoformat(f["date"])
                if 0 <= (fd - today).days <= 14:
                    match_events.append({
                        "date": f["date"],
                        "venue_lat": f.get("venue_lat"),
                        "venue_lng": f.get("venue_lng"),
                        "venue_capacity": f.get("venue_capacity", 0),
                        "venue": f.get("venue", ""),
                    })
            except ValueError:
                pass

    # Major events in next 90 days — check proximity to works
    upcoming_events = major_events or []

    # Next school holiday for specific deferral target
    next_holiday_date = None
    for term in SCHOOL_TERMS:
        term_end = date.fromisoformat(term["end"])
        if term_end > today:
            next_holiday_date = (term_end + timedelta(days=1)).isoformat()
            break

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

        # Works already started cannot be deferred — road is physically dug up
        status = rw.get("status", "")
        if status in NON_DEFERRABLE_STATUSES:
            continue

        # Major project works cannot be deferred — funding milestones
        major = is_major_project_work(rw)
        if major and major.get("cannot_defer"):
            continue

        # Planning obligation works cannot be deferred — s106/s278 legal requirements
        if is_planning_obligation_work(rw):
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

        # Skip works on very low-traffic roads — not worth the political cost of deferral
        est_traffic = estimate_road_traffic(rw.get("road", ""), rw.get("description", ""))
        if est_traffic < 500 and cap_red < 0.8:
            continue  # Backstreet — no meaningful traffic impact even with lane restriction

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
        # Check if works span any match day AND are near the venue (within 2km)
        for me in match_events:
            md = me["date"]
            if start <= md and (not end or end >= md):
                # Only flag if work is within 2km of the match venue
                vlat = me.get("venue_lat")
                vlng = me.get("venue_lng")
                if vlat and vlng:
                    try:
                        venue_dist = haversine_m(float(rlat), float(rlng), vlat, vlng)
                    except (ValueError, TypeError):
                        venue_dist = 99999
                    if venue_dist > 2000:
                        continue  # Too far from venue — match day irrelevant
                # Scale impact by venue capacity (PNE/Burnley > Accrington)
                cap = me.get("venue_capacity", 5000)
                match_impact = 7 if cap >= 10000 else 4 if cap >= 5000 else 2
                timing_score += match_impact
                timing_flags.append("match_day")
                break
        # Check major event proximity
        for ev in upcoming_events:
            for ev_date in ev.get("dates", []):
                if start <= ev_date and (not end or end >= ev_date):
                    ev_lat = ev.get("lat")
                    ev_lng = ev.get("lng")
                    if ev_lat and ev_lng:
                        try:
                            ev_dist = haversine_m(float(rlat), float(rlng), ev_lat, ev_lng)
                        except (ValueError, TypeError):
                            ev_dist = 99999
                        radius = ev.get("impact_radius_km", 2) * 1000
                        if ev_dist <= radius:
                            crowd = ev.get("crowd", 0)
                            ev_impact = 7 if crowd >= 20000 else 5 if crowd >= 10000 else 3 if crowd >= 5000 else 1
                            timing_score += ev_impact
                            timing_flags.append(f"major_event:{ev['name']}")
                            break  # One event match is enough
            if f"major_event:" in str(timing_flags):
                break  # Already matched an event
        timing_score = min(timing_score, 15)

        total_score = round(junction_score + severity_score + clash_score + timing_score, 1)

        if total_score < 15:
            continue  # Not worth flagging

        # B2: Confidence scoring (0.0-1.0)
        confidence = 1.0
        confidence_flags = []

        # Check traffic data quality for affected junctions
        has_estimated_traffic = any(
            j.get("data_quality") == "estimated" for j in affected_junctions
        )
        has_no_traffic = not affected_junctions  # No junction proximity data at all
        if has_no_traffic:
            confidence -= 0.15
            confidence_flags.append("no_traffic_data")
        elif has_estimated_traffic:
            confidence -= 0.10
            confidence_flags.append("estimated_traffic_volume")

        # Auto-discovered corridors (from config seed) have less certainty
        is_on_auto_corridor = clash_score > 0  # If clashing, it's corridor-based
        if is_on_auto_corridor and not any(
            j.get("data_quality") in ("high", "medium") for j in affected_junctions
        ):
            confidence -= 0.15
            confidence_flags.append("auto_corridor_no_verified_data")

        # Inferred restriction (no explicit description)
        if not (rw.get("description") or "").strip():
            confidence -= 0.10
            confidence_flags.append("inferred_restriction")

        # School term proximity is seasonal assumption
        if "school_term" in timing_flags:
            # Check if any school is actually nearby
            has_nearby_school = False
            for sc in SCHOOL_LOCATIONS:
                try:
                    if haversine_m(float(rlat), float(rlng), sc["lat"], sc["lng"]) < 800:
                        has_nearby_school = True
                        break
                except (ValueError, TypeError):
                    pass
            if not has_nearby_school:
                confidence -= 0.10
                confidence_flags.append("school_term_no_nearby_school")

        # Infrastructure-aware confidence/flags
        infra_flags = []
        if road_infra:
            try:
                rlat_f, rlng_f = float(rlat), float(rlng)
            except (ValueError, TypeError):
                rlat_f, rlng_f = None, None

            if rlat_f is not None:
                # Level crossing nearby — high confidence flag
                for lx in road_infra.get("level_crossings", []):
                    if haversine_m(rlat_f, rlng_f, lx["lat"], lx["lng"]) < 500:
                        confidence += 0.05  # Boost confidence — measurable disruption
                        confidence_flags.append("near_level_crossing")
                        infra_flags.append("near level crossing — closure would remove sole alternative route")
                        break

                # Narrow road — high impact flag
                for nr in road_infra.get("narrow_roads", []):
                    if haversine_m(rlat_f, rlng_f, nr["lat"], nr["lng"]) < 300:
                        confidence_flags.append("narrow_road_no_parallel_route")
                        infra_flags.append("narrow / single-lane road — no overtaking, works create total blockage")
                        break

                # Multiple traffic signals nearby — cascade delay flag
                nearby_signal_count = sum(
                    1 for sig in road_infra.get("traffic_signals", [])
                    if haversine_m(rlat_f, rlng_f, sig["lat"], sig["lng"]) < 200
                )
                if nearby_signal_count >= 3:
                    confidence_flags.append("signal_cluster")
                    infra_flags.append(f"signal-controlled junction — {nearby_signal_count} signals within 200m, works will cascade delays")

        confidence = round(max(min(confidence, 1.0), 0.1), 2)

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
        # Infrastructure-aware reasons
        for iflag in infra_flags:
            reasons.append(iflag)
        if "school_term" in timing_flags:
            # Check if any school is actually nearby (for more specific text)
            has_school_nearby_for_reason = False
            for sc in SCHOOL_LOCATIONS:
                try:
                    if haversine_m(float(rlat), float(rlng), sc["lat"], sc["lng"]) < 800:
                        has_school_nearby_for_reason = True
                        reasons.append(f"School term — near {sc['name']} ({sc.get('pupils', '?')} pupils)")
                        break
                except (ValueError, TypeError):
                    pass
            if not has_school_nearby_for_reason:
                reasons.append("School term — general network congestion increased")
        if "match_day" in timing_flags:
            reasons.append("Match day — pre/post-match traffic disruption")
        # Major event reasons
        event_flags = [f for f in timing_flags if f.startswith("major_event:")]
        for ef in event_flags:
            ev_name = ef.split(":", 1)[1]
            reasons.append(f"Major event — {ev_name} creating additional traffic pressure")

        # Determine road classification for feasibility of night works
        road_name = rw.get("road", "")
        road_class = "minor"
        if road_name:
            rn = road_name.upper().strip()
            if rn.startswith("M") and rn[1:2].isdigit():
                road_class = "motorway"
            elif rn.startswith("A") and rn[1:2].isdigit():
                road_class = "a_road"
            elif rn.startswith("B") and rn[1:2].isdigit():
                road_class = "b_road"
            # Check if road name in description contains a classified road
            desc_upper = (rw.get("description") or "").upper()
            for prefix in ("A6", "A59", "A56", "A583", "A585", "A682", "A679", "A680", "A681", "A671", "A584"):
                if prefix in desc_upper:
                    road_class = "a_road"
                    break

        # Check advance notice feasibility for s56 (need >3 days notice)
        days_until_start = 0
        try:
            start_date_obj = date.fromisoformat(start)
            days_until_start = (start_date_obj - today).days
        except (ValueError, TypeError):
            pass
        s56_feasible = days_until_start > 3

        # Build recommendation text
        if is_lcc:
            # LCC deferral — specify target window
            has_nearby_school_for_text = "school_term" in timing_flags and "school_term_no_nearby_school" not in confidence_flags
            if has_nearby_school_for_text and next_holiday_date:
                defer_target = f"school holiday (from {next_holiday_date})"
            elif next_holiday_date:
                defer_target = f"off-peak period or school holiday (from {next_holiday_date})"
            else:
                defer_target = "off-peak period"

            rec_text = (
                f"Defer to {defer_target} — "
                f"{'full closure' if rc == 'full_closure' else 'lane restriction'} "
                f"causing {int(cap_red * 100)}% capacity loss"
            )
            action = "DEFER"
        elif s56_feasible:
            # Utility s56 timing direction — only recommend night works on classified roads
            if cap_red >= 0.5 and road_class in ("motorway", "a_road", "b_road"):
                timing_text = "night works (20:00-06:00)"
            elif cap_red >= 0.5:
                # Residential street — night works inappropriate due to EPA 1990 noise
                timing_text = "off-peak hours (09:30-15:30) to avoid peak traffic"
            else:
                timing_text = "off-peak hours (avoid 07:30-09:30 and 16:00-18:30)"
            rec_text = (
                f"Issue s56 timing direction — require {timing_text} "
                f"to reduce {int(cap_red * 100)}% capacity impact"
            )
            action = "s56_TIMING_DIRECTION"
        else:
            # Too late for s56 — can only monitor
            rec_text = (
                f"Monitor — works start within {days_until_start} days, "
                f"s56 timing direction no longer feasible. "
                f"Consider s74 overrun charges if works exceed permitted duration"
            )
            action = "MONITOR"
            # Reduce score — we can't actually do anything about these
            total_score = round(total_score * 0.6, 1)

        recommendation = {
            "id": rw.get("id"),
            "road": road_name,
            "operator": operator,
            "is_lcc_controlled": is_lcc,
            "category": rw.get("category", ""),
            "status": status,
            "description": (rw.get("description") or "")[:200],
            "start_date": start,
            "end_date": end,
            "restriction_class": rc,
            "capacity_reduction": cap_red,
            "impact_label": restriction["impact_label"],
            "road_class": road_class,
            "lat": rlat,
            "lng": rlng,
            "deferral_score": total_score,
            "confidence": confidence,
            "confidence_flags": confidence_flags,
            "junction_score": round(junction_score, 1),
            "severity_score": round(severity_score, 1),
            "clash_score": round(clash_score, 1),
            "timing_score": round(timing_score, 1),
            "affected_junctions": affected_junctions[:3],
            "clashing_works": clashing_works[:4],
            "timing_flags": timing_flags,
            "reasons": reasons,
            "action": action,
            "recommendation": rec_text,
            "estimated_traffic": est_traffic,
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

                is_breach = total_cap_reduction >= 1.5 or (total_cap_reduction >= 1.0 and len(cor_works) >= 3)
                is_coordination = total_cap_reduction >= 1.0
                # B4: s59 monitoring tier — developing situation that could escalate
                is_monitor = (
                    not is_breach and not is_coordination
                    and total_cap_reduction >= 0.5
                    and len(cor_works) >= 2
                )

                if is_breach:
                    rec_text = (
                        f"s59 NRSWA breach likely — {len(cor_works)} concurrent works on {cor['name']} "
                        f"with combined {int(total_cap_reduction * 100)}% capacity reduction. "
                        f"LCC should use s56 timing powers to stagger these works and avoid s59 breach."
                    )
                elif is_coordination:
                    rec_text = (
                        f"s59 coordination needed — {len(cor_works)} concurrent works on {cor['name']} "
                        f"with combined {int(total_cap_reduction * 100)}% capacity reduction. "
                        f"LCC should phase these works to reduce cumulative impact."
                    )
                elif is_monitor:
                    rec_text = (
                        f"s59 monitoring — {len(cor_works)} works on {cor['name']} "
                        f"with combined {int(total_cap_reduction * 100)}% capacity impact. "
                        f"Developing situation — could breach s59 threshold if additional works approved."
                    )
                else:
                    rec_text = (
                        f"Monitor — {len(cor_works)} works on {cor['name']}, "
                        f"combined {int(total_cap_reduction * 100)}% capacity impact."
                    )

                clashes.append({
                    "corridor": cor["name"],
                    "road": cor["road"],
                    "concurrent_works": len(cor_works),
                    "works": cor_works,
                    "overlapping_pairs": len(overlapping_pairs),
                    "total_capacity_reduction": round(total_cap_reduction, 2),
                    "severity": severity,
                    "s59_breach": is_breach,
                    "s59_coordination_needed": is_coordination,
                    "s59_monitor": is_monitor,
                    "recommendation": rec_text,
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

        # Classify road for appropriate timing direction
        road_name_td = rw.get("road", "").upper().strip()
        is_classified_road = (
            (road_name_td.startswith("A") and len(road_name_td) > 1 and road_name_td[1:2].isdigit())
            or (road_name_td.startswith("B") and len(road_name_td) > 1 and road_name_td[1:2].isdigit())
            or (road_name_td.startswith("M") and len(road_name_td) > 1 and road_name_td[1:2].isdigit())
        )

        # Determine recommended timing — only suggest night works on classified roads
        # (residential night works create EPA 1990 s80 statutory nuisance complaints)
        if cap_red >= 0.6 and is_classified_road:
            timing = "Night works only (20:00-06:00)"
            urgency = "critical"
        elif cap_red >= 0.6:
            timing = "Off-peak hours (09:30-15:30) — night works inappropriate for residential area"
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


def build_strategic_recommendations(roadworks: list, junctions: list, corridors: list,
                                     clashes: list, deferrals: list, timing_recs: list,
                                     today: date, fixtures: list, major_events: list) -> dict:
    """Build top-level strategic recommendations — what LCC should do NOW.

    This is the executive summary: prioritised actions based on ALL data,
    not dependent on timeline position.
    """
    today_str = today.isoformat()

    # 1. Immediate actions (this week)
    immediate = []

    # s59 breaches need action TODAY
    for clash in clashes:
        if clash.get("s59_breach"):
            immediate.append({
                "priority": "critical",
                "category": "s59_breach",
                "action": f"Issue s56 timing directions to stagger works on {clash['corridor']}",
                "detail": f"{clash['concurrent_works']} concurrent works creating {int(clash['total_capacity_reduction'] * 100)}% capacity loss. LCC has duty under NRSWA s59 to coordinate.",
                "legal_basis": "NRSWA 1991 s59 — Duty to coordinate",
                "corridor": clash.get("corridor", ""),
            })

    # High-scoring LCC deferrals that can still be actioned
    for d in deferrals[:10]:
        if d.get("is_lcc_controlled") and d.get("action") == "DEFER" and d.get("deferral_score", 0) >= 30:
            immediate.append({
                "priority": "high",
                "category": "lcc_deferral",
                "action": f"Defer LCC works on {d['road']}",
                "detail": d.get("recommendation", ""),
                "road": d.get("road", ""),
                "score": d.get("deferral_score", 0),
            })

    # Critical s56 timing directions
    critical_s56 = [t for t in timing_recs if t.get("urgency") == "critical"]
    for t in critical_s56[:5]:
        immediate.append({
            "priority": "high",
            "category": "s56_timing",
            "action": f"Issue s56 timing direction for {t['operator']} works on {t['road']}",
            "detail": f"{t['timing_direction']} — {int(t['capacity_reduction'] * 100)}% capacity impact" + (f" near {t['near_critical_junction']}" if t.get('near_critical_junction') else ""),
            "legal_basis": "NRSWA 1991 s56 — Directions as to timing",
            "road": t.get("road", ""),
        })

    # 2. Upcoming event preparations
    event_preps = []
    for ev in (major_events or []):
        first_date = ev.get("first_date", "")
        days_away = 0
        try:
            days_away = (date.fromisoformat(first_date) - today).days
        except (ValueError, TypeError):
            continue

        if days_away > 60:
            continue  # Too far out

        # Check if any works clash with this event
        clashing = []
        for rw in roadworks:
            start = rw.get("start_date", "")[:10]
            end = rw.get("end_date", "")[:10]
            if not start:
                continue
            for ev_date in ev.get("dates", []):
                if start <= ev_date and (not end or end >= ev_date):
                    ev_lat = ev.get("lat")
                    ev_lng = ev.get("lng")
                    rlat = rw.get("lat")
                    rlng = rw.get("lng")
                    if ev_lat and ev_lng and rlat and rlng:
                        try:
                            d = haversine_m(float(rlat), float(rlng), ev_lat, ev_lng)
                            if d < ev.get("impact_radius_km", 2) * 1000:
                                clashing.append({
                                    "road": rw.get("road", ""),
                                    "operator": rw.get("operator", ""),
                                    "distance_m": int(d),
                                })
                        except (ValueError, TypeError):
                            pass
                    break

        prep = {
            "event": ev["name"],
            "date": first_date,
            "days_away": days_away,
            "crowd": ev.get("crowd", 0),
            "districts": ev.get("districts", []),
            "roads_affected": ev.get("roads_affected", []),
            "clashing_works": len(clashing),
            "clashing_details": clashing[:5],
        }

        if clashing:
            prep["action"] = f"Reschedule {len(clashing)} works near {ev['name']} ({first_date}) — {ev.get('crowd', 0):,} expected crowd"
            prep["priority"] = "high" if ev.get("crowd", 0) >= 10000 else "medium"
        else:
            prep["action"] = f"Monitor traffic management plan for {ev['name']} ({first_date})"
            prep["priority"] = "low"

        event_preps.append(prep)

    # 3. Network-wide summary stats
    total_works = len(roadworks)
    active = sum(1 for rw in roadworks if rw.get("status") == "Works started")
    closures = sum(1 for rw in roadworks if "closure" in (rw.get("restrictions") or rw.get("management_type") or "").lower())
    lcc_works = sum(1 for rw in roadworks if "lancashire" in (rw.get("operator") or "").lower())
    utility_works = total_works - lcc_works

    # 4. Key metrics
    critical_junctions = [j for j in junctions if j.get("jci", 0) >= 70]
    stressed_corridors = [c for c in corridors if c.get("severity", 0) >= 0.6]

    # 5. Match day preparations
    match_preps = []
    for f in fixtures:
        if f.get("is_home") and f.get("date"):
            d = f["date"]
            try:
                days = (date.fromisoformat(d) - today).days
            except (ValueError, TypeError):
                continue
            if 0 < days <= 14:
                # Check for works near venue
                vlat = f.get("venue_lat")
                vlng = f.get("venue_lng")
                nearby_works = 0
                if vlat and vlng:
                    for rw in roadworks:
                        rlat = rw.get("lat")
                        rlng = rw.get("lng")
                        if rlat and rlng:
                            try:
                                dist = haversine_m(float(rlat), float(rlng), vlat, vlng)
                                if dist < 2000:
                                    start = rw.get("start_date", "")[:10]
                                    end = rw.get("end_date", "")[:10]
                                    if start <= d and (not end or end >= d):
                                        nearby_works += 1
                            except (ValueError, TypeError):
                                pass

                match_preps.append({
                    "date": d,
                    "days_away": days,
                    "venue": f.get("venue", ""),
                    "opponent": f.get("opponent", ""),
                    "capacity": f.get("venue_capacity", 0),
                    "nearby_works": nearby_works,
                    "action": f"{'Issue timing directions for' if nearby_works > 0 else 'No works near'} {f.get('venue', '')} — {f.get('opponent', '')} ({d})" + (f" — {nearby_works} works within 2km" if nearby_works else ""),
                })

    return {
        "generated": datetime.now().isoformat(),
        "summary": {
            "total_works": total_works,
            "active_works": active,
            "road_closures": closures,
            "lcc_controlled": lcc_works,
            "utility_works": utility_works,
            "critical_junctions": len(critical_junctions),
            "stressed_corridors": len(stressed_corridors),
            "s59_breaches": len([c for c in clashes if c.get("s59_breach")]),
            "actionable_deferrals": len([d for d in deferrals if d.get("action") == "DEFER" and d.get("deferral_score", 0) >= 25]),
        },
        "immediate_actions": sorted(immediate, key=lambda x: {"critical": 0, "high": 1, "medium": 2, "low": 3}.get(x.get("priority", "low"), 9)),
        "event_preparations": sorted(event_preps, key=lambda x: x.get("days_away", 999)),
        "match_preparations": match_preps,
    }


def main():
    # Parse CLI flags
    args = sys.argv[1:]
    skip_infra = "--skip-infra" in args
    args = [a for a in args if a != "--skip-infra"]
    output_path = args[0] if args else DEFAULT_OUTPUT

    print(f"Traffic Intelligence ETL v3 (Lancashire-wide) — {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    if skip_infra:
        print("  --skip-infra: Skipping road infrastructure fetch")

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

    # 3. Sport fixtures (all venues from config)
    print(f"\n3. Fetching sport fixtures ({len(ALL_SPORT_VENUES)} venues)...")
    fixtures = fetch_sport_fixtures()

    # Merge hardcoded fixtures for clubs where API failed
    api_venues = set(f.get("venue", "") for f in fixtures)
    hardcoded = load_hardcoded_fixtures()
    hardcoded_merged = 0
    for hf in hardcoded:
        if hf["venue"] not in api_venues:
            fixtures.append(hf)
            hardcoded_merged += 1
    if hardcoded_merged:
        print(f"  + {hardcoded_merged} hardcoded fixtures merged (API fallback)")
        fixtures.sort(key=lambda x: x.get("date", ""))

    # 3b. Major events
    major_events = get_upcoming_major_events(today)
    if major_events:
        print(f"  Major events upcoming: {len(major_events)}")
        for ev in major_events[:5]:
            print(f"    {ev['name']} ({ev['first_date']}, ~{ev['crowd']:,} crowd)")

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

    # 5. OSM traffic infrastructure (basic signals + crossings)
    print("\n4. Fetching OSM traffic infrastructure...")
    infra = fetch_osm_infrastructure()
    signal_clusters = cluster_signals_to_junctions(infra["signals"])
    print(f"  Clustered {len(infra['signals'])} signals into {len(signal_clusters)} junction groups")

    # 5b. Comprehensive road infrastructure from OSM
    road_infra = None
    infra_hotspots = []
    if not skip_infra:
        print("\n4b. Fetching comprehensive road infrastructure from OSM...")
        road_infra = fetch_road_infrastructure()
        if road_infra:
            infra_hotspots = build_infra_hotspots(road_infra, roadworks_records)
            print(f"  Infrastructure hotspots: {len(infra_hotspots)}")
    else:
        print("\n4b. Skipping road infrastructure (--skip-infra)")

    # 6. Load FixMyStreet reports for defect proximity
    fms_path = os.path.join(SCRIPT_DIR, "..", "public", "data", "fixmystreet.json")
    fms_reports = []
    try:
        with open(fms_path) as f:
            fms = json.load(f)
        fms_reports = fms.get("reports", [])
    except Exception:
        pass

    # 7. Build junction congestion index (now restriction-aware + infrastructure-aware)
    print("\n5. Building Junction Congestion Index (restriction + infrastructure aware)...")
    jci_results = build_junction_congestion_index(NAMED_JUNCTIONS, count_points, roadworks_records, fms_reports, road_infra)
    for j in jci_results[:5]:
        closures = j.get("nearby_closures", 0)
        lanes = j.get("nearby_lane_restrictions", 0)
        infra_b = j.get("infra_bonus", 0)
        print(f"  {j['name']}: JCI {j['jci']} ({j['jci_level']}) — {closures} closures, {lanes} lane restrictions, infra +{infra_b}")

    # 8. Build corridor congestion scores
    print("\n6. Building corridor congestion scores...")
    corridor_results = build_corridor_scores(CONGESTION_CORRIDORS, count_points, active_roadworks)
    for c in corridor_results[:3]:
        print(f"  {c['name']}: severity {c['severity']} ({c['level']})")

    # 9. Deferral recommendations (now infrastructure-aware)
    print("\n7. Building deferral recommendations...")
    deferrals = build_deferral_recommendations(roadworks_records, jci_results, corridor_results, today, fixtures, major_events, road_infra)
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
    non_deferrable_count = 0
    luf_works_count = 0
    bridge_works_count = 0
    planning_obligation_count = 0
    started_count = 0
    for rw in roadworks_records:
        restriction = classify_restriction(rw)
        major = is_major_project_work(rw)
        status = rw.get("status", "")
        is_started = status in NON_DEFERRABLE_STATUSES
        is_bridge = is_bridge_or_structural_work(rw)
        is_planning = is_planning_obligation_work(rw)

        if major and major.get("cannot_defer"):
            non_deferrable_count += 1
            if "LUF" in major.get("name", ""):
                luf_works_count += 1
        if is_bridge:
            bridge_works_count += 1
        if is_planning:
            planning_obligation_count += 1
        if is_started:
            started_count += 1

        classified_works.append({
            "id": rw.get("id"),
            "road": rw.get("road", ""),
            "operator": rw.get("operator", ""),
            "status": status,
            "description": (rw.get("description") or "")[:200],
            "restriction_class": restriction["restriction_class"],
            "capacity_reduction": restriction["capacity_reduction"],
            "impact_label": restriction["impact_label"],
            "major_project": major["name"] if major else None,
            "cannot_defer": bool(major and major.get("cannot_defer")),
            "is_bridge": is_bridge,
            "is_planning_obligation": is_planning,
            "is_started": is_started,
            "start_date": (rw.get("start_date") or "")[:10],
            "end_date": (rw.get("end_date") or "")[:10],
        })

    # Compute date ranges for validity windows
    all_starts = [rw.get("start_date", "")[:10] for rw in roadworks_records if rw.get("start_date")]
    all_ends = [rw.get("end_date", "")[:10] for rw in roadworks_records if rw.get("end_date")]
    earliest_start = min(all_starts) if all_starts else today.isoformat()
    latest_end = max(all_ends) if all_ends else today.isoformat()

    # Next school holiday for deferral target
    next_holiday_start = None
    for term in SCHOOL_TERMS:
        term_end = date.fromisoformat(term["end"])
        if term_end > today:
            # Holiday starts day after term ends
            next_holiday_start = (term_end + timedelta(days=1)).isoformat()
            break

    # B3: Data freshness — track per-source staleness
    now_utc = datetime.now(timezone.utc)
    data_freshness = {
        "dft_count_points": {
            "source": "DfT Road Traffic Statistics API",
            "records": len(count_points),
            "note": "Annual counts, last updated by DfT typically in Oct/Nov",
        },
        "dft_aadf": {
            "source": "DfT AADF Data",
            "records": len(aadf_data),
            "note": "Annual average daily flow; latest year varies by count point",
        },
        "osm_infrastructure": {
            "source": "OpenStreetMap Overpass API",
            "records": len(infra["signals"]) + len(infra["crossings"]),
            "note": "Community-maintained; updated in real-time by OSM mappers",
        },
        "roadworks": {
            "source": "LCC MARIO ArcGIS (roadworks.json)",
            "records": len(roadworks_records),
            "stale_hours": None,
            "note": "Depends on roadworks_etl.py schedule (every 2 hours)",
        },
        "sport_fixtures": {
            "source": "fixturedownload.com",
            "records": len(fixtures),
            "note": "Season fixtures; static after season start",
        },
        "school_terms": {
            "source": "highways_config.json (manual)",
            "records": len(SCHOOL_TERMS),
            "note": "Set annually; update before September each year",
        },
        "major_events": {
            "source": "highways_config.json",
            "records": len(major_events),
            "note": "Manually curated Lancashire events calendar",
        },
        "road_infrastructure": {
            "source": "OpenStreetMap Overpass API (comprehensive)",
            "records": (
                sum(len(road_infra.get(k, [])) for k in [
                    "traffic_signals", "roundabouts", "mini_roundabouts",
                    "level_crossings", "narrow_roads", "one_way_streets",
                    "bridges", "weight_restrictions", "height_restrictions",
                ]) + road_infra.get("bus_stop_count", 0)
            ) if road_infra else 0,
            "cached": road_infra is not None and os.path.exists(INFRA_CACHE_PATH),
            "note": "Cached for 24h. Traffic signals, roundabouts, level crossings, narrow roads, bridges, restrictions",
            "skipped": skip_infra,
        },
    }

    # Try to determine roadworks freshness from file mtime
    try:
        rw_mtime = os.path.getmtime(rw_path)
        rw_age_hours = (time.time() - rw_mtime) / 3600
        data_freshness["roadworks"]["stale_hours"] = round(rw_age_hours, 1)
        data_freshness["roadworks"]["stale"] = rw_age_hours > 6  # >6h = stale
    except Exception:
        pass

    # JCI data quality summary
    jci_quality = {"high": 0, "medium": 0, "low": 0, "estimated": 0, "none": 0}
    for j in jci_results:
        q = j.get("data_quality", "none")
        jci_quality[q] = jci_quality.get(q, 0) + 1

    # Build strategic recommendations — actions LCC can take NOW based on all data
    strategic_recs = build_strategic_recommendations(
        roadworks_records, jci_results, corridor_results, clashes,
        deferrals, timing_recs, today, fixtures, major_events
    )

    output = {
        "meta": {
            "source": "DfT Road Traffic API + OSM Overpass + LCC School Terms + MARIO ArcGIS",
            "generated": now_utc.isoformat(),
            "analysis_date": today.isoformat(),
            "fetch_time_ms": int((time.time() - t0) * 1000),
            "active_roadworks": active_roadworks,
            "restriction_summary": restriction_summary,
            "data_freshness": data_freshness,
            "jci_data_quality": jci_quality,
            "validity": {
                "roadworks_window": {"from": earliest_start, "to": latest_end},
                "analysis_valid_until": (today + timedelta(days=1)).isoformat(),
                "next_school_holiday": next_holiday_start,
                "in_school_term": is_term_time(today),
                "current_term": get_current_term(today),
            },
            "coverage": {
                "total_works": len(roadworks_records),
                "works_started": started_count,
                "planned_works": len(roadworks_records) - started_count,
                "luf_programme_works": luf_works_count,
                "bridge_structural_works": bridge_works_count,
                "planning_obligation_works": planning_obligation_count,
                "non_deferrable_total": non_deferrable_count + started_count + bridge_works_count + planning_obligation_count,
            },
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
            "sport_venues": ALL_SPORT_VENUES,
            "turf_moor": ALL_SPORT_VENUES[0] if ALL_SPORT_VENUES else None,  # backward compat
        },
        "road_infrastructure": {
            "summary": {
                "traffic_signals": len(road_infra.get("traffic_signals", [])) if road_infra else 0,
                "roundabouts": len(road_infra.get("roundabouts", [])) if road_infra else 0,
                "mini_roundabouts": len(road_infra.get("mini_roundabouts", [])) if road_infra else 0,
                "level_crossings": len(road_infra.get("level_crossings", [])) if road_infra else 0,
                "narrow_roads": len(road_infra.get("narrow_roads", [])) if road_infra else 0,
                "one_way_streets": len(road_infra.get("one_way_streets", [])) if road_infra else 0,
                "bridges": len(road_infra.get("bridges", [])) if road_infra else 0,
                "weight_restrictions": len(road_infra.get("weight_restrictions", [])) if road_infra else 0,
                "height_restrictions": len(road_infra.get("height_restrictions", [])) if road_infra else 0,
                "bus_stops": road_infra.get("bus_stop_count", 0) if road_infra else 0,
                "speed_limit_zones": road_infra.get("speed_limit_zones", {}) if road_infra else {},
            },
            "hotspots": infra_hotspots[:50],
            "level_crossings": road_infra.get("level_crossings", []) if road_infra else [],
            "narrow_roads": road_infra.get("narrow_roads", []) if road_infra else [],
            "bridges": road_infra.get("bridges", []) if road_infra else [],
            "weight_restrictions": road_infra.get("weight_restrictions", []) if road_infra else [],
            "height_restrictions": road_infra.get("height_restrictions", []) if road_infra else [],
        } if road_infra else None,
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
        "strategic_recommendations": strategic_recs,
        "major_events": [
            {
                "name": ev["name"],
                "dates": ev["dates"],
                "crowd": ev.get("crowd", 0),
                "districts": ev.get("districts", []),
                "type": ev.get("type", ""),
                "roads_affected": ev.get("roads_affected", []),
                "lat": ev.get("lat"),
                "lng": ev.get("lng"),
            }
            for ev in major_events
        ],
    }

    os.makedirs(os.path.dirname(os.path.abspath(output_path)), exist_ok=True)

    with open(output_path, "w") as f:
        json.dump(output, f, separators=(",", ":"))

    size_kb = os.path.getsize(output_path) / 1024
    elapsed = time.time() - t0

    print(f"\nDone in {elapsed:.1f}s")
    print(f"Output: {output_path} ({size_kb:.1f} KB)")
    print(f"Infrastructure: {len(infra['signals'])} signals, {len(infra['crossings'])} crossings, {len(signal_clusters)} junctions")
    if road_infra:
        ri = road_infra
        print(f"Road infra: {len(ri.get('traffic_signals', []))} signals, {len(ri.get('roundabouts', []))} roundabouts, "
              f"{len(ri.get('level_crossings', []))} level crossings, {len(ri.get('narrow_roads', []))} narrow, "
              f"{len(ri.get('bridges', []))} bridges, {len(infra_hotspots)} hotspots")
    print(f"Congestion: {len(jci_results)} junctions scored, {len(corridor_results)} corridors")
    print(f"Count points: {len(count_points)}, Key roads: {len(key_roads_list)}")
    print(f"Fixtures: {len(fixtures)}, School term: {'Yes' if term_status['in_term'] else 'No'}")
    print(f"Active roadworks: {active_roadworks} ({restriction_summary['full_closure']} closures, {restriction_summary['lane_restriction']} lane restrictions)")
    print(f"Non-deferrable: {non_deferrable_count} (LUF: {luf_works_count}, bridge: {bridge_works_count}, planning: {planning_obligation_count}, started: {started_count})")
    print(f"Deferrals: {len(lcc_deferrals)} LCC, {len(utility_deferrals)} utility s56. Clashes: {len(clashes)} corridors")
    print(f"Options: {len(options)}, Timing recs: {len(timing_recs)}")
    if heatmap:
        print(f"7-day peak: {max(h['peak_flow'] for h in heatmap):.1f}% at {max(heatmap, key=lambda h: h['peak_flow'])['day']}")


if __name__ == "__main__":
    main()
