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
  wk                  grid nodes raised to build a bridge approach that the water must ignore: [node, old height]
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
        decks.append({"p": pts, "hw": round(r["w"] / 2 + (1.0 if r["c"] == "foot" else 3.4), 2), "a": round(float(ha - base), 2), "b": round(float(hb - base), 2), "road": r["c"] == "road"})

    # Where a bridge forks over the water, one arm starts in mid-river and its end has no bank to measure. Give
    # that end the height of the deck it joins.
    def deck_at(d, x, z):
        cum = [0.0]
        for i in range(1, len(d["p"])):
            cum.append(cum[-1] + math.hypot(d["p"][i][0] - d["p"][i - 1][0], d["p"][i][1] - d["p"][i - 1][1]))
        best = None
        for i in range(len(d["p"]) - 1):
            a, b = d["p"][i], d["p"][i + 1]
            dx, dz = b[0] - a[0], b[1] - a[1]
            t = max(0.0, min(1.0, ((x - a[0]) * dx + (z - a[1]) * dz) / ((dx * dx + dz * dz) or 1)))
            dist = math.hypot(x - a[0] - dx * t, z - a[1] - dz * t)
            if best is None or dist < best[0]:
                best = (dist, d["a"] + (d["b"] - d["a"]) * (cum[i] + t * (cum[i + 1] - cum[i])) / (cum[-1] or 1))
        return best
    fork_ends = set()
    for di, d in enumerate(decks):
        if not d["road"]:
            continue
        for key, end in (("a", d["p"][0]), ("b", d["p"][-1])):
            for oi, o in enumerate(decks):
                if oi == di or not o["road"]:
                    continue
                # the other deck passes through this end, and not merely by sharing its own end point
                inner = any(math.hypot(q[0] - end[0], q[1] - end[1]) < 0.3 for q in o["p"][1:-1])
                if inner:
                    d[key] = round(float(deck_at(o, end[0], end[1])[1]), 2)
                    fork_ends.add((di, key))

    # The terrain model has the river bank cut away at each end of a road bridge, which left the road dropping off
    # the end of the deck into a dip. Build the approach: raise the land around each bank end towards the height
    # of the deck, falling away gently with distance. Ground in or beside the water is left alone, so the river
    # stays level.
    def in_water(x, z):
        """0 on land, 2 inside a mapped water outline, 1 on the river's drawn line where no outline covers it."""
        on_line = 0
        for wt in world["water"]:
            if "poly" in wt:
                q = wt["poly"]
                inside = False
                jn = len(q) - 1
                for i in range(len(q)):
                    if (q[i][1] > z) != (q[jn][1] > z) and x < (q[jn][0] - q[i][0]) * (z - q[i][1]) / (q[jn][1] - q[i][1]) + q[i][0]:
                        inside = not inside
                    jn = i
                if inside:
                    return 2
            else:
                for i in range(len(wt["p"]) - 1):
                    a, b = wt["p"][i], wt["p"][i + 1]
                    dx, dz = b[0] - a[0], b[1] - a[1]
                    t = max(0.0, min(1.0, ((x - a[0]) * dx + (z - a[1]) * dz) / ((dx * dx + dz * dz) or 1)))
                    if math.hypot(x - a[0] - dx * t, z - a[1] - dz * t) < wt["w"] / 2:
                        on_line = 1
        return on_line
    raised = 0
    # A node on the edge of the river's drawn line, close to a bridge end, is raised with the bank, and its old
    # height is kept for the water, which stays level beneath the new bank.
    water_keep = {}
    for di, d in enumerate(decks):
        if not d["road"]:
            continue
        for key, end, back in (("a", d["p"][0], d["p"][1]), ("b", d["p"][-1], d["p"][-2])):
            if (di, key) in fork_ends:
                continue
            top = d[key] + base
            l = math.hypot(end[0] - back[0], end[1] - back[1]) or 1
            ux, uz = (end[0] - back[0]) / l, (end[1] - back[1]) / l
            for j in range(nz):
                for i in range(nx):
                    x, z = x0 + i * S, z0 + j * S
                    vx, vz = x - end[0], z - end[1]
                    dist = math.hypot(vx, vz)
                    if dist > 34 or vx * ux + vz * uz < -4:
                        continue
                    wet = in_water(x, z)
                    if wet == 2 or (wet == 1 and dist > 16):
                        continue
                    want = top - max(0.0, dist - 10) * 0.12
                    if want > h[j, i]:
                        if wet == 1:
                            water_keep.setdefault(j * nx + i, float(h[j, i]))
                        h[j, i] = want
                        raised += 1
    print("bridge approaches: grid nodes raised", raised, "fork ends", sorted(fork_ends))

    # Where the bank could not be raised (the grid node is in the water), the road still dropped off the end of
    # the deck. Carry each deck on along the way it joins until the ground comes up to meet it, 30 m at most.
    plain = [r for r in world["roads"] if not r.get("b")]

    def next_way(at, ux, uz, used):
        best = None
        for r in plain:
            if id(r) in used:
                continue
            for pts2 in (r["p"], r["p"][::-1]):
                if math.hypot(pts2[0][0] - at[0], pts2[0][1] - at[1]) < 0.3 and len(pts2) > 1:
                    l2 = math.hypot(pts2[1][0] - pts2[0][0], pts2[1][1] - pts2[0][1]) or 1
                    dot = ((pts2[1][0] - pts2[0][0]) * ux + (pts2[1][1] - pts2[0][1]) * uz) / l2
                    if dot > 0.3 and (best is None or dot > best[0]):
                        best = (dot, r, pts2)
        return best
    carried = []
    for di, d in enumerate(decks):
        for key in ("a", "b"):
            if (di, key) in fork_ends:
                continue
            pts2 = d["p"] if key == "b" else d["p"][::-1]
            top = d[key] + base
            end, back = pts2[-1], pts2[-2]
            if ground(end[0], end[1], h) >= top - 0.3:
                continue
            l = math.hypot(end[0] - back[0], end[1] - back[1]) or 1
            ux, uz = (end[0] - back[0]) / l, (end[1] - back[1]) / l
            extra, used, at, gone, met = [], set(), end, 0.0, False
            while gone < 30 and not met:
                nw = next_way(at, ux, uz, used)
                if nw is None:
                    break
                used.add(id(nw[1]))
                line = nw[2]
                for i in range(1, len(line)):
                    a2, b2 = line[i - 1], line[i]
                    seg = math.hypot(b2[0] - a2[0], b2[1] - a2[1]) or 1
                    ux, uz = (b2[0] - a2[0]) / seg, (b2[1] - a2[1]) / seg
                    step = 1.0
                    while step < seg and gone + step < 30:
                        x, z = a2[0] + ux * step, a2[1] + uz * step
                        if ground(x, z, h) >= top - 0.15:
                            extra.append([round(x, 1), round(z, 1)])
                            met = True
                            break
                        step += 1.0
                    if met:
                        break
                    gone += seg
                    extra.append([b2[0], b2[1]])
                    at = b2
                    if gone >= 30:
                        break
            if extra:
                d["p"] = (pts2 + extra) if key == "b" else (pts2 + extra)[::-1]
                carried.append((di, key, len(extra), met))
    print("decks carried on to meet the ground (deck, end, points added, met):", carried)
    for d in decks:
        del d["road"]
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
        "wk": [[k, int(round((v - base) * 100))] for k, v in sorted(water_keep.items())],
        "bh": bh,
    }
    json.dump(out, open(out_path, "w", encoding="utf-8"), separators=(",", ":"))
    print("wrote", out_path, os.path.getsize(out_path), "bytes")


if __name__ == "__main__":
    main(sys.argv[1], sys.argv[2])
