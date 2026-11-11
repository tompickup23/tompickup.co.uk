"""Build game/terrain.json: ground heights and real building heights from Environment Agency LIDAR.

Usage (from the repo root, after build_world.py):
  uv run --no-project --with numpy --with tifffile --with imagecodecs --with pyproj python -I scripts/padiham-game/build_terrain.py public/games/the-padiham-job/world.json public/games/the-padiham-job/terrain.json

Source: Environment Agency, LIDAR Composite, 1 m digital terrain model (DTM) and first return digital
surface model (DSM), read over WCS with no key. (c) Environment Agency copyright and/or database right 2022.
All rights reserved. Open Government Licence.

The two GeoTIFFs (about 9 MB each) are kept in scripts/padiham-game/data/lidar/, which is not committed;
the script downloads them when they are missing.

What it writes:
  x0, z0, s, nx, nz   a grid of ground heights in game metres, nx by nz nodes, s metres apart
  h                   height at each node in centimetres above the lowest node, row by row (z, then x)
  decks               one per mapped bridge: its line, half width, and the height at each end in metres
  bh                  one entry per building in world.json, in the same order: the middle value of surface
                      minus terrain inside the outline, in metres, or null where there is too little data
Rerun this after build_world.py, because bh follows the order of the buildings there.
"""
import json
import math
import os
import sys
import time
import urllib.request

import numpy as np
import tifffile
from pyproj import Transformer, network

LAT0, LON0 = 53.8020, -2.3150
MX = math.cos(math.radians(LAT0)) * 111320.0
MZ = 110540.0
S = 12         # grid spacing, metres
MARGIN = 160   # ground mapped beyond the playable slice, metres
HERE = os.path.dirname(os.path.abspath(__file__))
CACHE = os.path.join(HERE, "data", "lidar")
WCS = "https://environment.data.gov.uk/spatialdata/{slug}/wcs?service=WCS&version=2.0.1&request=GetCoverage&CoverageId={cov}&format=image/tiff&subset=E({e0},{e1})&subset=N({n0},{n1})&SUBSETTINGCRS=http://www.opengis.net/def/crs/EPSG/0/27700"
PRODUCTS = {
    "dtm": ("lidar-composite-digital-terrain-model-dtm-1m", "13787b9a-26a4-4775-8523-806d13af58fc__Lidar_Composite_Elevation_DTM_1m"),
    "dsm": ("lidar-composite-digital-surface-model-first-return-dsm-1m", "df4e3ec3-315e-48aa-aaaf-b5ae74d7b2bb__Lidar_Composite_Elevation_FZ_DSM_1m"),
}


def fetch(kind, box):
    path = os.path.join(CACHE, f"{kind}_{box[0]}_{box[1]}_{box[2]}_{box[3]}.tif")
    if not os.path.exists(path):
        os.makedirs(CACHE, exist_ok=True)
        slug, cov = PRODUCTS[kind]
        url = WCS.format(slug=slug, cov=cov, e0=box[0], e1=box[2], n0=box[1], n1=box[3])
        for attempt in range(4):
            try:
                with urllib.request.urlopen(urllib.request.Request(url, headers={"User-Agent": "terrain-builder"}), timeout=180) as r:
                    data = r.read()
                if data[:2] not in (b"II", b"MM"):
                    raise RuntimeError("not a TIFF: " + data[:200].decode("utf-8", "replace"))
                open(path, "wb").write(data)
                break
            except Exception as err:  # the service sometimes stalls; try again
                print("retry", kind, attempt, err)
                time.sleep(5)
        else:
            raise SystemExit(f"could not download {kind}")
    with tifffile.TiffFile(path) as t:
        page = t.pages[0]
        arr = page.asarray().astype("float32")
        if "ModelTransformationTag" in page.tags:  # a 4 by 4 matrix: pixel size and the top left corner
            m = page.tags["ModelTransformationTag"].value
            sx, sy, e0, n1 = m[0], -m[5], m[3], m[7]
        else:
            sx, sy = page.tags["ModelPixelScaleTag"].value[:2]
            tp = page.tags["ModelTiepointTag"].value
            e0, n1 = tp[3], tp[4]
    arr[(arr < -1000) | (arr > 5000)] = np.nan  # the no-data value is a very large negative number
    print(kind, arr.shape, "origin", e0, n1, "pixel", sx, sy, "range", float(np.nanmin(arr)), float(np.nanmax(arr)))
    return arr, e0, n1, sx, sy


def main(world_path, out_path):
    world = json.load(open(world_path, encoding="utf-8"))
    sl = world["slice"]
    x0 = math.floor((sl[0] - MARGIN) / S) * S
    z0 = math.floor((sl[1] - MARGIN) / S) * S
    nx = math.ceil((sl[2] + MARGIN - x0) / S) + 1
    nz = math.ceil((sl[3] + MARGIN - z0) / S) + 1
    network.set_network_enabled(True)  # fetch the OSTN15 grid, so positions are right to well under a metre
    tr = Transformer.from_crs("EPSG:4326", "EPSG:27700", always_xy=True)

    def to_en(xs, zs):
        return tr.transform(LON0 + np.asarray(xs) / MX, LAT0 - np.asarray(zs) / MZ)

    cx = [x0 - S, x0 + nx * S, x0 - S, x0 + nx * S]
    cz = [z0 - S, z0 - S, z0 + nz * S, z0 + nz * S]
    ce, cn = to_en(cx, cz)
    box = (int(math.floor(min(ce) / 10) * 10 - 10), int(math.floor(min(cn) / 10) * 10 - 10), int(math.ceil(max(ce) / 10) * 10 + 10), int(math.ceil(max(cn) / 10) * 10 + 10))
    print("National Grid box", box)
    dtm, e0, n1, px, py = fetch("dtm", box)
    dsm = fetch("dsm", box)[0]

    def pix(e, n):
        col = np.clip(np.floor((np.asarray(e) - e0) / px).astype(int), 0, dtm.shape[1] - 1)
        row = np.clip(np.floor((n1 - np.asarray(n)) / py).astype(int), 0, dtm.shape[0] - 1)
        return row, col

    # Ground: the mean of the 1 m terrain cells around each node.
    gx, gz = np.meshgrid(x0 + np.arange(nx) * S, z0 + np.arange(nz) * S)
    ge, gn = to_en(gx.ravel(), gz.ravel())
    rows, cols = pix(ge, gn)
    half = S // 2
    acc = np.zeros(rows.shape, "float64")
    cnt = np.zeros(rows.shape, "float64")
    for dr in range(-half, half + 1):
        for dc in range(-half, half + 1):
            v = dtm[np.clip(rows + dr, 0, dtm.shape[0] - 1), np.clip(cols + dc, 0, dtm.shape[1] - 1)]
            ok = ~np.isnan(v)
            acc[ok] += v[ok]
            cnt[ok] += 1
    h = np.where(cnt > 0, acc / np.maximum(cnt, 1), np.nan).reshape(nz, nx)
    for _ in range(50):  # fill gaps from their neighbours
        bad = np.isnan(h)
        if not bad.any():
            break
        p = np.pad(h, 1, constant_values=np.nan)
        nb = np.stack([p[:-2, 1:-1], p[2:, 1:-1], p[1:-1, :-2], p[1:-1, 2:]])
        fill = np.nanmean(nb, axis=0)
        h[bad] = fill[bad]
    print("gaps filled, any left:", bool(np.isnan(h).any()))
    p = np.pad(h, 1, mode="edge")  # one light smoothing pass
    h = (p[:-2, 1:-1] + p[2:, 1:-1] + p[1:-1, :-2] + p[1:-1, 2:] + 4 * h) / 8

    def ground(x, z, grid):
        u = min(max((x - x0) / S, 0), nx - 1.001)
        v = min(max((z - z0) / S, 0), nz - 1.001)
        i, j = int(u), int(v)
        fu, fv = u - i, v - j
        return grid[j, i] * (1 - fu) * (1 - fv) + grid[j, i + 1] * fu * (1 - fv) + grid[j + 1, i] * (1 - fu) * fv + grid[j + 1, i + 1] * fu * fv

    # Bridges: the terrain model cuts bridges out, so the ground under one is the river bed. Each bridge is written
    # as a deck: a straight line in height between its two ends, which the game lays over the ground beneath.
    base = float(h.min())
    decks = []
    # A bridge is often mapped in several pieces. Join pieces of the same kind that meet end to end, so the deck
    # runs from bank to bank and no end sits over the water.
    chains = []
    for r in world["roads"]:
        if r.get("b"):
            chains.append({"p": [list(q) for q in r["p"]], "w": r["w"], "c": "foot" if r["c"] == "foot" else "road"})
    joined = True
    while joined:
        joined = False
        for i in range(len(chains)):
            for j in range(len(chains)):
                a, b = chains[i], chains[j]
                if i == j or a["c"] != b["c"]:
                    continue
                for pa, pb in ((a["p"], b["p"]), (a["p"], b["p"][::-1]), (a["p"][::-1], b["p"]), (a["p"][::-1], b["p"][::-1])):
                    if math.hypot(pa[-1][0] - pb[0][0], pa[-1][1] - pb[0][1]) < 0.3:
                        a["p"] = pa + pb[1:]
                        a["w"] = max(a["w"], b["w"])
                        del chains[j]
                        joined = True
                        break
                if joined:
                    break
            if joined:
                break
    for r in chains:
        pts = r["p"]

        def end_height(a, b):
            # the bank at this end: the highest of the end itself and two points a little further back along the road
            l = math.hypot(a[0] - b[0], a[1] - b[1]) or 1
            ux, uz = (a[0] - b[0]) / l, (a[1] - b[1]) / l
            return max(ground(a[0] + ux * d, a[1] + uz * d, h) for d in (0, 6, 12))

        ha, hb = end_height(pts[0], pts[1]), end_height(pts[-1], pts[-2])
        decks.append({"p": pts, "hw": round(r["w"] / 2 + (1.0 if r["c"] == "foot" else 3.4), 2), "a": round(float(ha - base), 2), "b": round(float(hb - base), 2)})
    print("grid", nx, "by", nz, "relief", round(float(h.max()) - base, 1), "m, lowest", round(base, 2), "m above datum, bridge decks", len(decks), [(d["a"], d["b"]) for d in decks])

    # Buildings: the middle value of surface minus terrain inside each outline, pulled in a little from the walls.
    bh = []
    for b in world["buildings"]:
        p = b["p"]
        cxm = sum(q[0] for q in p) / len(p)
        czm = sum(q[1] for q in p) / len(p)
        q = [(cxm + (v[0] - cxm) * 0.85, czm + (v[1] - czm) * 0.85) for v in p]
        xs = [v[0] for v in q]
        zs = [v[1] for v in q]
        sx, sz = [], []
        zz = math.floor(min(zs))
        while zz <= max(zs):
            xx = math.floor(min(xs))
            while xx <= max(xs):
                inside = False
                jn = len(q) - 1
                for i in range(len(q)):
                    if (q[i][1] > zz) != (q[jn][1] > zz) and xx < (q[jn][0] - q[i][0]) * (zz - q[i][1]) / (q[jn][1] - q[i][1]) + q[i][0]:
                        inside = not inside
                    jn = i
                if inside:
                    sx.append(xx)
                    sz.append(zz)
                xx += 1
            zz += 1
        if len(sx) < 8:
            bh.append(None)
            continue
        e, n = to_en(sx, sz)
        rr, cc = pix(e, n)
        d = dsm[rr, cc] - dtm[rr, cc]
        d = d[~np.isnan(d)]
        m = float(np.median(d)) if len(d) >= 8 else 0
        bh.append(round(m, 1) if m >= 2 else None)
    got = [v for v in bh if v is not None]
    print("building heights:", len(got), "of", len(bh), "middle value", round(float(np.median(got)), 1), "m, tallest", max(got), "m")

    out = {
        "attribution": "Contains Environment Agency LIDAR Composite data (c) Environment Agency copyright and/or database right 2022. All rights reserved.",
        "x0": x0, "z0": z0, "s": S, "nx": nx, "nz": nz,
        "h": [int(round((v - base) * 100)) for v in h.ravel()],
        "decks": decks,
        "bh": bh,
    }
    json.dump(out, open(out_path, "w", encoding="utf-8"), separators=(",", ":"))
    print("wrote", out_path, os.path.getsize(out_path), "bytes")


if __name__ == "__main__":
    main(sys.argv[1], sys.argv[2])
