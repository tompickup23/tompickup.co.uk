"""Convert an OpenStreetMap extract of Padiham town centre into game/world.json.

Usage (from the repo root):
  uv run --no-project python -I scripts/padiham-game/build_world.py scripts/padiham-game/data/osm-map.json public/games/the-padiham-job/world.json scripts/padiham-game/data/os-buildings.json scripts/padiham-game/data/overrides.json

Map data (c) OpenStreetMap contributors, Open Database Licence.
Coordinates are local metres: x east, z south, origin at LAT0/LON0.
"""
import json
import math
import random
import sys

LAT0, LON0 = 53.8020, -2.3150
MX = math.cos(math.radians(LAT0)) * 111320.0
MZ = 110540.0

ROAD_WIDTH = {
    "primary": 9.5, "secondary": 9, "tertiary": 8, "unclassified": 6.5,
    "residential": 6.5, "living_street": 6, "pedestrian": 6, "service": 4,
    "track": 3, "footway": 2.2, "path": 2, "cycleway": 2.6, "steps": 2,
}
FOOT = {"footway", "path", "cycleway", "steps", "track"}
GREEN = {
    ("leisure", "park"): "park", ("leisure", "pitch"): "pitch",
    ("leisure", "playground"): "park", ("leisure", "garden"): "park",
    ("landuse", "grass"): "grass", ("landuse", "meadow"): "grass",
    ("landuse", "recreation_ground"): "park", ("landuse", "cemetery"): "grass",
    ("landuse", "allotments"): "grass", ("landuse", "forest"): "wood",
    ("natural", "wood"): "wood", ("natural", "scrub"): "scrub",
    ("natural", "grassland"): "grass",
}


def r1(v):
    return round(v, 1)


def centroid(pts):
    return (sum(p[0] for p in pts) / len(pts), sum(p[1] for p in pts) / len(pts))


def in_poly(x, z, pts):
    inside = False
    j = len(pts) - 1
    for i in range(len(pts)):
        xi, zi = pts[i]
        xj, zj = pts[j]
        if (zi > z) != (zj > z) and x < (xj - xi) * (z - zi) / (zj - zi) + xi:
            inside = not inside
        j = i
    return inside


def nearest_on_seg(px, pz, a, b):
    dx, dz = b[0] - a[0], b[1] - a[1]
    l2 = dx * dx + dz * dz
    t = 0 if l2 == 0 else max(0, min(1, ((px - a[0]) * dx + (pz - a[1]) * dz) / l2))
    qx, qz = a[0] + t * dx, a[1] + t * dz
    return qx, qz, math.hypot(px - qx, pz - qz)


def main(src, dst, os_path=None, over_path=None):
    data = json.load(open(src, encoding="utf-8"))
    nodes, ways, rels = {}, [], []
    for e in data["elements"]:
        if e["type"] == "node":
            nodes[e["id"]] = ((e["lon"] - LON0) * MX, -(e["lat"] - LAT0) * MZ, e.get("tags", {}))
        elif e["type"] == "way":
            ways.append(e)
        else:
            rels.append(e)
    way_by_id = {w["id"]: w for w in ways}

    def pts_of(w):
        return [(nodes[n][0], nodes[n][1]) for n in w["nodes"] if n in nodes]

    buildings, roads, water, green, pois, trees = [], [], [], [], [], []
    rng = random.Random(7)

    def add_green(kind, pts):
        if len(pts) >= 4:
            green.append({"k": kind, "p": [[r1(x), r1(z)] for x, z in pts[:-1]]})
            if kind in ("wood", "scrub"):
                xs = [p[0] for p in pts]
                zs = [p[1] for p in pts]
                area = (max(xs) - min(xs)) * (max(zs) - min(zs))
                for _ in range(int(min(120, area / (160 if kind == "wood" else 400)))):
                    x, z = rng.uniform(min(xs), max(xs)), rng.uniform(min(zs), max(zs))
                    if in_poly(x, z, pts):
                        trees.append([r1(x), r1(z)])

    for w in ways:
        t = w.get("tags", {})
        pts = pts_of(w)
        if len(pts) < 2:
            continue
        closed = w["nodes"][0] == w["nodes"][-1] and len(pts) >= 4
        if "building" in t and t["building"] != "demolished" and closed:
            h = None
            try:
                if "height" in t:
                    h = float(t["height"].split()[0])
                elif "building:levels" in t:
                    h = float(t["building:levels"]) * 3.2
            except ValueError:
                h = None
            if h is None:
                kind = t["building"]
                base = {"church": 13, "civic": 11, "apartments": 14, "industrial": 8,
                        "retail": 7, "commercial": 8, "school": 7}.get(kind, 6.5)
                if t.get("shop") == "supermarket":
                    base = 7
                h = base + (w["id"] % 5) * 0.4
            b = {"p": [[r1(x), r1(z)] for x, z in pts[:-1]], "h": r1(h), "t": t["building"]}
            if "name" in t:
                b["n"] = t["name"]
            buildings.append(b)
        if "highway" in t and t["highway"] in ROAD_WIDTH and t.get("area") != "yes":
            hw = t["highway"]
            rd = {"p": [[r1(x), r1(z)] for x, z in pts], "w": ROAD_WIDTH[hw], "c": "foot" if hw in FOOT else hw}
            if "name" in t and hw not in FOOT and hw != "service":
                rd["n"] = t["name"]
            if t.get("bridge") == "yes":
                rd["b"] = 1
            roads.append(rd)
        if t.get("waterway") in ("river", "stream"):
            water.append({"p": [[r1(x), r1(z)] for x, z in pts], "w": 20 if t["waterway"] == "river" else 3})
        if t.get("natural") == "water" and closed:
            water.append({"poly": [[r1(x), r1(z)] for x, z in pts[:-1]]})
        for (k, v), kind in GREEN.items():
            if t.get(k) == v and closed:
                add_green(kind, pts)
                break

    for rel in rels:
        t = rel.get("tags", {})
        if t.get("type") == "multipolygon" and t.get("natural") == "wood":
            for m in rel["members"]:
                w = way_by_id.get(m["ref"])
                if m["type"] == "way" and m.get("role") == "outer" and w and w["nodes"][0] == w["nodes"][-1]:
                    add_green("wood", pts_of(w))

    for x, z, t in nodes.values():
        if t.get("natural") == "tree":
            trees.append([r1(x), r1(z)])

    # Prefer Ordnance Survey outlines where available: OSM has few buildings mapped here.
    if os_path:
        osm_b = buildings
        buildings = []
        os_all = json.load(open(os_path, encoding="utf-8"))
        plain = [o for o in os_all if "class" not in o]
        # "Important building" outlines duplicate ordinary ones: pass their class on, keep one copy.
        for o in os_all:
            if "class" not in o:
                continue
            cx, cz = centroid(o["p"])
            host = next((q for q in plain if in_poly(cx, cz, q["p"])), None)
            if host is None:
                plain.append(o)
            else:
                host["class"] = o["class"]
                host["name"] = o.get("name", "")
        for o in plain:
            pts = o["p"]
            b = {"p": [[r1(x), r1(z)] for x, z in pts], "t": "yes"}
            cls = o.get("class", "")
            if "Worship" in cls:
                b["t"] = "church"
            elif cls in ("Library", "Post Office", "Fire Station"):
                b["t"] = "civic"
            elif "Education" in cls:
                b["t"] = "school"
            if o.get("name") and o["name"] != "Unknown":
                b["n"] = o["name"]
            for ob in osm_b:
                cx, cz = centroid(ob["p"])
                if in_poly(cx, cz, pts):
                    if ob["t"] != "yes":
                        b["t"] = ob["t"]
                    if "n" in ob:
                        b["n"] = ob["n"]
            buildings.append(b)

    # Per-building details read from photos.
    if over_path:
        for ov in json.load(open(over_path, encoding="utf-8"))["buildings"]:
            hit = next((b for b in buildings if in_poly(ov["at"][0], ov["at"][1], b["p"])), None)
            if hit is None:
                print("override matched no building:", ov["name"])
                continue
            # "p" replaces the outline itself, for the few places where the outline and the road line disagree.
            for k in ("m", "f", "fh", "r", "win", "feat", "shops", "frontAt", "pub", "band", "interior", "bays", "bandFloor", "bandW", "spots", "sign", "p"):
                if k in ov:
                    hit[k] = ov[k]

    # Street furniture and crossings that are mapped as points.
    props = []
    for x, z, t in nodes.values():
        k = None
        if t.get("highway") == "crossing":
            k = "signals" if t.get("crossing") == "traffic_signals" else "zebra"
        elif t.get("highway") == "bus_stop":
            k = "bus"
        elif t.get("amenity") == "post_box":
            k = "post"
        elif t.get("highway") == "mini_roundabout":
            k = "mini"
        if k:
            props.append({"k": k, "x": r1(x), "z": r1(z)})

    # Buses: the named stops inside the play area, and for each mapped service and direction the stops it calls
    # at, in order. From OpenStreetMap route relations, which can be out of date. Limited services are left out.
    SL = [-620, -330, 480, 430]
    stop_ix, bus_stops, bus_routes = {}, [], []
    for nid, (x, z, t) in nodes.items():
        if t.get("highway") == "bus_stop" and t.get("name") and SL[0] + 6 < x < SL[2] - 6 and SL[1] + 6 < z < SL[3] - 6:
            stop_ix[nid] = len(bus_stops)
            bus_stops.append({"n": t["name"], "x": r1(x), "z": r1(z)})
    for rel in rels:
        t = rel.get("tags", {})
        if t.get("route") != "bus" or not t.get("ref") or not t.get("to") or "Limited" in t["ref"]:
            continue
        calls = [stop_ix[m["ref"]] for m in rel["members"] if m["type"] == "node" and m["ref"] in stop_ix]
        calls = [c for i, c in enumerate(calls) if c not in calls[:i]]
        if len(calls) >= 2:
            bus_routes.append({"ref": t["ref"], "to": t["to"].replace(" Bus Station", ""), "s": calls})
    bus_routes.sort(key=lambda r: (r["ref"], r["to"]))

    # Points of interest: named shops, amenities, landmarks.
    walk = [r for r in roads]

    def door(cx, cz):
        best = (cx, cz, 1e9)
        for r in walk:
            p = r["p"]
            for i in range(len(p) - 1):
                q = nearest_on_seg(cx, cz, p[i], p[i + 1])
                if q[2] < best[2]:
                    best = q
        return best

    seen = set()

    def add_poi(name, cx, cz, kind):
        if name in seen:
            return
        seen.add(name)
        dx, dz, dist = door(cx, cz)
        pois.append({"n": name, "x": r1(cx), "z": r1(cz), "dx": r1(dx), "dz": r1(dz), "k": kind})

    keys = ("shop", "amenity", "leisure", "man_made")
    for w in ways:
        t = w.get("tags", {})
        if "name" in t and "highway" not in t and (any(k in t for k in keys) or t.get("building")):
            if t.get("amenity") == "parking":
                continue
            cx, cz = centroid(pts_of(w))
            add_poi(t["name"], cx, cz, next((t[k] for k in keys if k in t), t.get("building")))
    for x, z, t in nodes.values():
        if "name" in t and any(k in t for k in ("shop", "amenity")):
            add_poi(t["name"], x, z, t.get("shop") or t.get("amenity"))

    if over_path:
        for cp in json.load(open(over_path, encoding="utf-8")).get("pois", []):
            add_poi(cp["n"], cp["x"], cp["z"], cp["k"])

    # Landmarks with no building outline (memorials, the bandstand), placed in overrides.json.
    landmarks = []
    if over_path:
        for lm in json.load(open(over_path, encoding="utf-8")).get("landmarks", []):
            landmarks.append({"k": lm["k"], "x": lm["x"], "z": lm["z"], "h": lm.get("h", 0)})

    xs = [n[0] for n in nodes.values()]
    zs = [n[1] for n in nodes.values()]
    # Riverbank trees: the Calder's banks are wooded through the town, and OpenStreetMap maps few single trees.
    # One tree about every 16 m on each bank, inside the playable area, of the wide river lines, clear of roads, bridges and buildings.
    # Their own random stream, so nothing placed earlier moves.
    brng = random.Random(11)
    for wt in water:
        if "p" not in wt or wt.get("w", 0) < 10:
            continue
        pl = wt["p"]
        carry = 0.0
        for i in range(len(pl) - 1):
            ax, az, bx, bz = pl[i][0], pl[i][1], pl[i + 1][0], pl[i + 1][1]
            seg = math.hypot(bx - ax, bz - az)
            if seg < 0.5:
                continue
            ux, uz = (bx - ax) / seg, (bz - az) / seg
            d = carry
            while d < seg:
                for side in (1, -1):
                    off = wt["w"] / 2 + 3.5 + brng.random() * 4
                    x = ax + ux * d - uz * side * off + (brng.random() - 0.5) * 3
                    z = az + uz * d + ux * side * off + (brng.random() - 0.5) * 3
                    near_road = any(nearest_on_seg(x, z, r["p"][k], r["p"][k + 1])[2] < r["w"] / 2 + 5 for r in roads for k in range(len(r["p"]) - 1))
                    near_b = any(in_poly(x, z, b["p"]) or min(math.hypot(q[0] - x, q[1] - z) for q in b["p"]) < 5 for b in buildings)
                    in_play = -660 < x < 520 and -370 < z < 470
                    in_water = any('poly' in q and in_poly(x, z, q['poly']) for q in water)
                    if in_play and not in_water and not near_road and not near_b:
                        trees.append([r1(x), r1(z)])
                d += 16
            carry = d - seg

    world = {
        "attribution": "Map data (c) OpenStreetMap contributors, ODbL. Contains OS data (c) Crown copyright and database right 2026.",
        "slice": [-620, -330, 480, 430],
        "props": props, "landmarks": landmarks, "bus": {"stops": bus_stops, "routes": bus_routes},
        "bounds": [r1((-2.3260 - LON0) * MX), r1(-(53.8065 - LAT0) * MZ), r1((-2.3040 - LON0) * MX), r1(-(53.7975 - LAT0) * MZ)],
        "buildings": buildings, "roads": roads, "water": water, "green": green,
        "trees": trees, "pois": pois,
    }
    json.dump(world, open(dst, "w", encoding="utf-8"), separators=(",", ":"), ensure_ascii=False)
    print({k: len(v) for k, v in world.items() if isinstance(v, list)})
    print(sorted((b["n"], b["t"]) for b in buildings if "n" in b))


if __name__ == "__main__":
    main(*sys.argv[1:5])
