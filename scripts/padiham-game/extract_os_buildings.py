"""Pull OS OpenMap Local building outlines for Padiham into local game metres.

Usage: download OS OpenMap Local (ESRI Shapefile, grid square SD) from the OS Data Hub, unzip its data folder to <dir>, then
  uv run --no-project --with pyshp --with pyproj python -I scripts/padiham-game/extract_os_buildings.py <dir> scripts/padiham-game/data/os-buildings.json

Contains OS data (c) Crown copyright and database right. Open Government Licence.
"""
import json
import math
import sys

import shapefile
from pyproj import Transformer

LAT0, LON0 = 53.8020, -2.3150
MX = math.cos(math.radians(LAT0)) * 111320.0
MZ = 110540.0
BBOX = (-2.3290, 53.7960, -2.3010, 53.8080)  # lon/lat, a little wider than the OSM extract


def simplify(pts, tol):
    if len(pts) < 3:
        return pts
    a, b = pts[0], pts[-1]
    dx, dz = b[0] - a[0], b[1] - a[1]
    l = math.hypot(dx, dz) or 1e-9
    best, idx = 0, 0
    for i in range(1, len(pts) - 1):
        d = abs((pts[i][0] - a[0]) * dz - (pts[i][1] - a[1]) * dx) / l
        if d > best:
            best, idx = d, i
    if best <= tol:
        return [a, b]
    return simplify(pts[: idx + 1], tol)[:-1] + simplify(pts[idx:], tol)


def main(src, dst):
    to_bng = Transformer.from_crs(4326, 27700, always_xy=True)
    to_ll = Transformer.from_crs(27700, 4326, always_xy=True)
    e0, n0 = to_bng.transform(BBOX[0], BBOX[1])
    e1, n1 = to_bng.transform(BBOX[2], BBOX[3])
    out = []
    for name, important in (("SD_Building", False), ("SD_ImportantBuilding", True)):
        sf = shapefile.Reader(f"{src}/{name}")
        fields = [f[0] for f in sf.fields[1:]]
        for i, shp in enumerate(sf.iterShapes()):
            bx0, by0, bx1, by1 = shp.bbox
            if bx1 < e0 or bx0 > e1 or by1 < n0 or by0 > n1:
                continue
            parts = list(shp.parts) + [len(shp.points)]
            ring = shp.points[parts[0]:parts[1]]
            pts = []
            for e, n in ring:
                lon, lat = to_ll.transform(e, n)
                pts.append((round((lon - LON0) * MX, 2), round(-(lat - LAT0) * MZ, 2)))
            if pts[0] == pts[-1]:
                pts = pts[:-1]
            # simplify as an open chain split at the farthest vertex so the ring keeps its shape
            far = max(range(len(pts)), key=lambda k: (pts[k][0] - pts[0][0]) ** 2 + (pts[k][1] - pts[0][1]) ** 2)
            s = simplify(pts[: far + 1], 0.4)[:-1] + simplify(pts[far:] + [pts[0]], 0.4)[:-1]
            if len(s) < 3:
                continue
            rec = {"p": s}
            if important:
                r = dict(zip(fields, sf.record(i)))
                rec["name"] = r.get("DISTNAME") or ""
                rec["class"] = r.get("CLASSIFICA") or r.get("FEATCODE") or ""
            out.append(rec)
    json.dump(out, open(dst, "w"), separators=(",", ":"))
    print(len(out), "outlines")
    print([(r["name"], r["class"]) for r in out if "name" in r])


if __name__ == "__main__":
    main(sys.argv[1], sys.argv[2])
