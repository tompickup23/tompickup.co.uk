"""Shared helpers for the Lancashire Business Observatory fetchers.

Raw downloads cache to ~/observatory-data/raw/, normalized outputs to
~/observatory-data/processed/. Every processed output is JSON with a top-level
"$meta" block. House style: no em-dashes in any output text.
"""
import json
import os
import sys
import time
import datetime as _dt
from pathlib import Path

RAW = Path(os.path.expanduser("~/observatory-data/raw"))
PROC = Path(os.path.expanduser("~/observatory-data/processed"))
RAW.mkdir(parents=True, exist_ok=True)
PROC.mkdir(parents=True, exist_ok=True)

# --- Geography ------------------------------------------------------------
# The 14 Lancashire LADs (from geo_crosswalk.json).
LANCS_14 = {
    "E07000121": "Lancaster",
    "E07000123": "Preston",
    "E07000124": "Ribble Valley",
    "E07000119": "Fylde",
    "E07000128": "Wyre",
    "E06000009": "Blackpool",
    "E06000008": "Blackburn with Darwen",
    "E07000120": "Hyndburn",
    "E07000125": "Rossendale",
    "E07000122": "Pendle",
    "E07000117": "Burnley",
    "E07000118": "Chorley",
    "E07000126": "South Ribble",
    "E07000127": "West Lancashire",
}

# NW benchmark ring (aggregate comparators, not firm-level).
NW_RING = {
    # Greater Manchester (10)
    "E08000001": "Bolton",
    "E08000002": "Bury",
    "E08000003": "Manchester",
    "E08000004": "Oldham",
    "E08000005": "Rochdale",
    "E08000006": "Salford",
    "E08000007": "Stockport",
    "E08000008": "Tameside",
    "E08000009": "Trafford",
    "E08000010": "Wigan",
    # Liverpool City Region (6)
    "E06000006": "Halton",
    "E08000011": "Knowsley",
    "E08000012": "Liverpool",
    "E08000013": "St. Helens",
    "E08000014": "Sefton",
    "E08000015": "Wirral",
    # Cheshire & Warrington (3)
    "E06000049": "Cheshire East",
    "E06000050": "Cheshire West and Chester",
    "E06000007": "Warrington",
    # Cumbria (2 new unitaries, from Apr 2023)
    "E06000063": "Cumberland",
    "E06000064": "Westmorland and Furness",
}

ENGLAND = {"E92000001": "England"}

ALL_GEO = {**LANCS_14, **NW_RING}

# Lancashire postcode area prefixes for postcode-based filtering.
# BB, PR, FY, LA cover most; plus fringe districts from other areas.
LANCS_PC_AREAS = ["BB", "PR", "FY", "LA"]
# Fringe outcodes that fall in the 14 LADs but sit in non-Lancs postcode areas.
LANCS_FRINGE_OUTCODES = ["L39", "L40", "OL12", "OL13", "WN8", "WN6", "OL14", "M26"]

UA = "LancashireBusinessObservatory/1.0 (research; tompickup.co.uk)"


def log(msg):
    print(f"[{_dt.datetime.now().strftime('%H:%M:%S')}] {msg}", flush=True)


def fresh(path, days=7):
    """True if path exists and is younger than `days` days."""
    p = Path(path)
    if not p.exists() or p.stat().st_size == 0:
        return False
    age = time.time() - p.stat().st_mtime
    return age < days * 86400


def download(url, dest, days=7, headers=None, timeout=120, stream=True):
    """Idempotent cached download. Skips if dest exists and is <days old."""
    import requests
    dest = Path(dest)
    if fresh(dest, days):
        log(f"cache hit {dest.name} ({dest.stat().st_size/1e6:.1f} MB)")
        return dest
    # gzip/deflate only: the vps brotli decoder rejects some NOMIS br responses
    h = {"User-Agent": UA, "Accept-Encoding": "gzip, deflate"}
    if headers:
        h.update(headers)
    log(f"GET {url}")
    r = requests.get(url, headers=h, timeout=timeout, stream=stream)
    r.raise_for_status()
    tmp = dest.with_suffix(dest.suffix + ".part")
    with open(tmp, "wb") as f:
        for chunk in r.iter_content(chunk_size=1 << 16):
            if chunk:
                f.write(chunk)
    tmp.rename(dest)
    log(f"saved {dest.name} ({dest.stat().st_size/1e6:.1f} MB)")
    return dest


def get_json(url, headers=None, timeout=60, params=None, retries=3):
    import requests
    # gzip/deflate only: the vps brotli decoder rejects some NOMIS br responses
    h = {"User-Agent": UA, "Accept-Encoding": "gzip, deflate"}
    if headers:
        h.update(headers)
    last = None
    for i in range(retries):
        try:
            r = requests.get(url, headers=h, timeout=timeout, params=params)
            r.raise_for_status()
            return r.json()
        except Exception as e:  # noqa
            last = e
            log(f"retry {i+1}/{retries} {url}: {e}")
            time.sleep(2 * (i + 1))
    raise last


def zip_json_items(zip_path, member=None):
    """Stream items from a (possibly BOM-prefixed) JSON-array file inside a zip.

    Uses ijson so 500MB+ extracts never fully load into memory.
    """
    import zipfile
    import ijson
    z = zipfile.ZipFile(zip_path)
    name = member or z.namelist()[0]
    f = z.open(name)
    b = f.read(3)
    if b != b"\xef\xbb\xbf":  # no BOM: reopen from the start
        f.close()
        f = z.open(name)
    for obj in ijson.items(f, "item"):
        yield obj
    f.close()


def clean_text(s):
    """Strip em-dashes and normalise whitespace for house style."""
    if s is None:
        return None
    if not isinstance(s, str):
        return s
    return s.replace("—", "-").replace("–", "-").strip()


def meta(source_url, licence, notes=""):
    return {
        "source_url": source_url,
        "retrieved": _dt.date.today().isoformat(),
        "licence": licence,
        "notes": clean_text(notes),
    }


def write_out(name, meta_block, payload_key, rows, extra=None):
    """Write processed JSON with $meta first. Returns row count."""
    out = {"$meta": meta_block}
    if extra:
        out.update(extra)
    out[payload_key] = rows
    dest = PROC / name
    from decimal import Decimal

    def _default(o):
        if isinstance(o, Decimal):
            return int(o) if o == o.to_integral_value() else float(o)
        raise TypeError(f"not serializable: {type(o)}")

    with open(dest, "w") as f:
        json.dump(out, f, ensure_ascii=False, indent=1, default=_default)
    n = len(rows) if hasattr(rows, "__len__") else 0
    log(f"wrote {name}: {n} rows ({dest.stat().st_size/1e6:.2f} MB)")
    return n


# --- postcodes.io bulk resolver (cached) ----------------------------------
_PC_CACHE = PROC / "_postcode_lad_cache.json"


def _load_pc_cache():
    if _PC_CACHE.exists():
        try:
            return json.load(open(_PC_CACHE))
        except Exception:
            return {}
    return {}


def _save_pc_cache(c):
    json.dump(c, open(_PC_CACHE, "w"))


def norm_pc(pc):
    if not pc:
        return None
    return pc.upper().replace(" ", "").strip()


def resolve_postcodes(postcodes):
    """Bulk-resolve postcodes to LAD (ONS code) via postcodes.io. Cached.

    Returns {normalised_postcode: {"lad": code, "name": name} or None}.
    """
    import requests
    cache = _load_pc_cache()
    want = []
    for pc in postcodes:
        n = norm_pc(pc)
        if n and n not in cache:
            want.append(n)
    want = sorted(set(want))
    log(f"postcodes.io: {len(want)} new to resolve, {len(cache)} cached")
    # postcodes.io bulk accepts up to 100 per POST. It needs spaced format.
    def space_pc(n):
        # insert space before last 3 chars
        if len(n) > 3:
            return n[:-3] + " " + n[-3:]
        return n
    for i in range(0, len(want), 100):
        batch = want[i:i + 100]
        try:
            r = requests.post(
                "https://api.postcodes.io/postcodes",
                json={"postcodes": [space_pc(x) for x in batch]},
                headers={"User-Agent": UA}, timeout=60,
            )
            r.raise_for_status()
            for item in r.json().get("result", []):
                q = norm_pc(item.get("query"))
                res = item.get("result")
                if res and res.get("codes", {}).get("admin_district"):
                    cache[q] = {
                        "lad": res["codes"]["admin_district"],
                        "name": res.get("admin_district"),
                    }
                else:
                    cache[q] = None
        except Exception as e:  # noqa
            log(f"postcodes.io batch fail: {e}")
        if i and i % 2000 == 0:
            _save_pc_cache(cache)
            log(f"  resolved {i}/{len(want)}")
        time.sleep(0.05)
    _save_pc_cache(cache)
    return cache


def lad_for_postcode(pc, cache):
    n = norm_pc(pc)
    if not n:
        return None
    return cache.get(n)


def looks_lancs_pc(pc):
    """Cheap prefilter before hitting postcodes.io: is this plausibly Lancs?"""
    n = norm_pc(pc)
    if not n:
        return False
    for area in LANCS_PC_AREAS:  # BB PR FY LA - 2-letter area then a digit
        if n.startswith(area) and len(n) > len(area) and n[len(area)].isdigit():
            return True
    for oc in LANCS_FRINGE_OUTCODES:
        if n.startswith(oc):
            return True
    return False
