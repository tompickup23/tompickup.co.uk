// Builds the town from world.json: ground, roads and markings, buildings with roofs and facades,
// signs, street furniture, parked cars, trees and the slice barriers.
import * as THREE from 'three';
import { terrain, groundH } from './terrain.js';
import { TILE, PART, matTile, tileUV, SHOP_VARIANTS, facadeAtlas, slateTexture, groundTexture, signTexture, closedTexture, unionTexture } from './textures.js';

// ---------- Spatial grids ----------
export const CELL = 16;
const EMPTY = [];
const key = (cx, cz) => (cx + 512) * 1024 + (cz + 512);
export class Grid {
  constructor() { this.m = new Map(); }
  add(ax, az, bx, bz, pad, item) {
    const x0 = Math.floor((Math.min(ax, bx) - pad) / CELL), x1 = Math.floor((Math.max(ax, bx) + pad) / CELL);
    const z0 = Math.floor((Math.min(az, bz) - pad) / CELL), z1 = Math.floor((Math.max(az, bz) + pad) / CELL);
    for (let cx = x0; cx <= x1; cx++) for (let cz = z0; cz <= z1; cz++) {
      const k = key(cx, cz);
      let a = this.m.get(k);
      if (!a) this.m.set(k, a = []);
      a.push(item);
    }
  }
  at(x, z) { return this.m.get(key(Math.floor(x / CELL), Math.floor(z / CELL))) || EMPTY; }
}
export const walls = new Grid();     // [ax, az, bx, bz, low]  low = 1 for things the camera can see over
export const roadGrid = new Grid();  // [ax, az, bx, bz, halfWidth, name, class, roadIndex]
export const riverGrid = new Grid(); // [ax, az, bx, bz, halfWidth]

export function distToSeg(px, pz, s) {
  const dx = s[2] - s[0], dz = s[3] - s[1];
  const l2 = dx * dx + dz * dz;
  let t = l2 ? ((px - s[0]) * dx + (pz - s[1]) * dz) / l2 : 0;
  t = t < 0 ? 0 : t > 1 ? 1 : t;
  const qx = s[0] + t * dx, qz = s[1] + t * dz;
  return [Math.hypot(px - qx, pz - qz), qx, qz];
}
export const hash = (n) => { const x = Math.sin(n * 127.1 + 11.7) * 43758.5453; return x - Math.floor(x); };
export function nearWall(x, z, r) {
  for (const s of walls.at(x, z)) if (distToSeg(x, z, s)[0] < r) return true;
  return false;
}

// ---------- Geometry batching ----------
const colCache = new Map();
const C = (hex) => { let c = colCache.get(hex); if (!c) colCache.set(hex, c = new THREE.Color(hex)); return c; };
const _v = new THREE.Vector3(), _n = new THREE.Vector3(), _m = new THREE.Matrix4(), _nm = new THREE.Matrix3(), _q = new THREE.Quaternion(), _s = new THREE.Vector3(), _p = new THREE.Vector3();
const UP = new THREE.Vector3(0, 1, 0);
export class Batch {
  constructor() { this.p = []; this.n = []; this.u = []; this.c = []; }
  tri(a, b, c, col, uv, toward) {
    let nx = (b[1] - a[1]) * (c[2] - a[2]) - (b[2] - a[2]) * (c[1] - a[1]);
    let ny = (b[2] - a[2]) * (c[0] - a[0]) - (b[0] - a[0]) * (c[2] - a[2]);
    let nz = (b[0] - a[0]) * (c[1] - a[1]) - (b[1] - a[1]) * (c[0] - a[0]);
    if (toward && nx * toward[0] + ny * toward[1] + nz * toward[2] < 0) {
      [b, c] = [c, b]; nx = -nx; ny = -ny; nz = -nz;
      if (uv) uv = [uv[0], uv[2], uv[1]];
    }
    const l = Math.hypot(nx, ny, nz) || 1;
    this.p.push(a[0], a[1], a[2], b[0], b[1], b[2], c[0], c[1], c[2]);
    for (let i = 0; i < 3; i++) { this.n.push(nx / l, ny / l, nz / l); this.c.push(col.r, col.g, col.b); }
    if (uv) this.u.push(uv[0][0], uv[0][1], uv[1][0], uv[1][1], uv[2][0], uv[2][1]); else this.u.push(0, 0, 0, 0, 0, 0);
  }
  // a, b, c, d anticlockwise seen from the front; r = [u0, v0, u1, v1] with a at (u0, v0) and c at (u1, v1)
  quad(a, b, c, d, col, r, toward) {
    r = r || [0, 0, 0, 0];
    this.tri(a, b, c, col, [[r[0], r[1]], [r[2], r[1]], [r[2], r[3]]], toward);
    this.tri(a, c, d, col, [[r[0], r[1]], [r[2], r[3]], [r[0], r[3]]], toward);
  }
  add(geo, m, col) {
    const pos = geo.attributes.position, nor = geo.attributes.normal;
    _nm.getNormalMatrix(m);
    for (let i = 0; i < pos.count; i++) {
      _v.fromBufferAttribute(pos, i).applyMatrix4(m);
      _n.fromBufferAttribute(nor, i).applyMatrix3(_nm).normalize();
      this.p.push(_v.x, _v.y, _v.z); this.n.push(_n.x, _n.y, _n.z); this.u.push(0, 0); this.c.push(col.r, col.g, col.b);
    }
  }
  // Cut every triangle along the terrain grid lines and diagonals, so each piece lies inside one flat ground triangle.
  clipToGrid() {
    const T = terrain(), s = T.s, P = [], N = [], U = [], Cc = [];
    const fns = [(q) => (q[0] - T.x0) / s, (q) => (q[2] - T.z0) / s, (q) => (q[0] - T.x0) / s - (q[2] - T.z0) / s];
    const split = (polys, f) => {
      const out = [];
      for (const poly of polys) {
        let rest = poly, lo = Infinity, hi = -Infinity;
        for (const q of poly) { const v = f(q); if (v < lo) lo = v; if (v > hi) hi = v; }
        for (let k = Math.floor(lo) + 1; k < hi && rest.length >= 3; k++) {
          const a = [], b = [];
          for (let i = 0; i < rest.length; i++) {
            const p0 = rest[i], p1 = rest[(i + 1) % rest.length], f0 = f(p0) - k, f1 = f(p1) - k;
            if (f0 <= 0) a.push(p0);
            if (f0 >= 0) b.push(p0);
            if ((f0 < 0 && f1 > 0) || (f0 > 0 && f1 < 0)) { const t = f0 / (f0 - f1), m = p0.map((v, j) => v + (p1[j] - v) * t); a.push(m); b.push(m); }
          }
          if (a.length >= 3) out.push(a);
          rest = b;
        }
        if (rest.length >= 3) out.push(rest);
      }
      return out;
    };
    for (let t = 0; t < this.p.length / 9; t++) {
      const tri = [0, 1, 2].map((k) => { const v = t * 3 + k; return [this.p[v * 3], this.p[v * 3 + 1], this.p[v * 3 + 2], this.u[v * 2], this.u[v * 2 + 1]]; });
      // Small pieces (round road joins, short dashes) are left whole and raised a touch: over so short a span the
      // ground barely bends, and cutting them all would multiply the triangle count several times over.
      let big = false;
      for (let k = 0; k < 3; k++) if (Math.hypot(tri[k][0] - tri[(k + 1) % 3][0], tri[k][2] - tri[(k + 1) % 3][2]) > 4) big = true;
      let polys = [tri];
      if (big) for (const f of fns) polys = split(polys, f);
      else for (const q of tri) q[1] += 0.015;
      const c = [this.c[t * 9], this.c[t * 9 + 1], this.c[t * 9 + 2]], n = [this.n[t * 9], this.n[t * 9 + 1], this.n[t * 9 + 2]];
      for (const poly of polys) for (let k = 1; k < poly.length - 1; k++) for (const q of [poly[0], poly[k], poly[k + 1]]) { P.push(q[0], q[1], q[2]); U.push(q[3], q[4]); Cc.push(c[0], c[1], c[2]); N.push(n[0], n[1], n[2]); }
    }
    this.p = P; this.n = N; this.u = U; this.c = Cc;
  }
  // Lift everything on to the terrain. 'plain' moves each point up by the ground height beneath it, which keeps walls
  // upright and lets a long row follow the slope of its street. 'clip' and 'water' are for flat things lying on the
  // ground: they are cut along the terrain grid first, so they sit on it exactly. 'water' stays on the ground under bridges.
  drape(mode) {
    if (!terrain() || !mode) return;
    if (mode !== 'plain') this.clipToGrid();
    const p = this.p, water = mode === 'water';
    for (let i = 0; i < p.length; i += 3) p[i + 1] += groundH(p[i], p[i + 2], water);
    if (mode !== 'plain') for (let i = 0; i < p.length; i += 9) {
      const ax = p[i + 3] - p[i], ay = p[i + 4] - p[i + 1], az = p[i + 5] - p[i + 2], bx = p[i + 6] - p[i], by = p[i + 7] - p[i + 1], bz = p[i + 8] - p[i + 2];
      const nx = ay * bz - az * by, ny = az * bx - ax * bz, nz = ax * by - ay * bx, l = Math.hypot(nx, ny, nz);
      if (l > 1e-9) for (let k = 0; k < 9; k += 3) { this.n[i + k] = nx / l; this.n[i + k + 1] = ny / l; this.n[i + k + 2] = nz / l; }
    }
  }
  // Returns a group of meshes, one per square of the town, so only the squares in view are drawn.
  mesh(material, shadows = 'both', drape = 'plain') {
    this.drape(drape);
    const p = this.p, buckets = new Map();
    for (let t = 0; t < p.length; t += 9) {
      const k = Math.floor((p[t] + p[t + 3] + p[t + 6]) / 3 / CHUNK) + ',' + Math.floor((p[t + 2] + p[t + 5] + p[t + 8]) / 3 / CHUNK);
      let b = buckets.get(k);
      if (!b) buckets.set(k, b = []);
      b.push(t / 9);
    }
    const group = new THREE.Group();
    for (const [k, tris] of buckets) {
      const n = tris.length * 3, P3 = new Float32Array(n * 3), N3 = new Float32Array(n * 3), U2 = new Float32Array(n * 2), C3 = new Float32Array(n * 3);
      tris.forEach((t, i) => {
        for (let j = 0; j < 9; j++) { P3[i * 9 + j] = p[t * 9 + j]; N3[i * 9 + j] = this.n[t * 9 + j]; C3[i * 9 + j] = this.c[t * 9 + j]; }
        for (let j = 0; j < 6; j++) U2[i * 6 + j] = this.u[t * 6 + j];
      });
      const g = new THREE.BufferGeometry();
      g.setAttribute('position', new THREE.BufferAttribute(P3, 3));
      g.setAttribute('normal', new THREE.BufferAttribute(N3, 3));
      g.setAttribute('uv', new THREE.BufferAttribute(U2, 2));
      g.setAttribute('color', new THREE.BufferAttribute(C3, 3));
      const m = new THREE.Mesh(g, material);
      m.castShadow = shadows === 'both' || shadows === 'cast';
      m.receiveShadow = shadows === 'both' || shadows === 'receive';
      const [cx, cz] = k.split(',').map(Number);
      chunks.push({ mesh: m, x: (cx + 0.5) * CHUNK, z: (cz + 0.5) * CHUNK });
      group.add(m);
    }
    return group;
  }
}
const BOX = new THREE.BoxGeometry(1, 1, 1).toNonIndexed();
const CYL = new THREE.CylinderGeometry(1, 1, 1, 8).toNonIndexed();
const CONE = new THREE.CylinderGeometry(0.15, 1, 1, 8).toNonIndexed();
const SPH = new THREE.SphereGeometry(1, 8, 6).toNonIndexed();
// Cheap shapes for things there are hundreds of: a five-sided open tube (10 triangles) for poles and chimney pots,
// and a six-sided drum (24) that serves as both wheels on one axle of a parked car.
const POLE = new THREE.CylinderGeometry(1, 1, 1, 5, 1, true).toNonIndexed();
const AXLE = new THREE.CylinderGeometry(1, 1, 1, 6).toNonIndexed();
// Town geometry is split into squares this many metres across, so the renderer can skip the parts out of view.
const CHUNK = 230;
export const chunks = []; // { mesh, x, z } for every square, for hiding the far ones
// Place a primitive in a local frame: o = {x, z, h} where local +z points along heading h.
function put(b, geo, o, lx, ly, lz, sx, sy, sz, col, tiltZ = 0) {
  const s = Math.sin(o.h), c = Math.cos(o.h);
  _p.set(o.x + lx * c + lz * s, ly, o.z - lx * s + lz * c);
  _q.setFromAxisAngle(UP, o.h);
  if (tiltZ) _q.multiply(new THREE.Quaternion().setFromAxisAngle(new THREE.Vector3(0, 0, 1), tiltZ));
  b.add(geo, _m.compose(_p, _q, _s.set(sx, sy, sz)), col);
}

// ---------- Polyline helpers ----------
function ribbon(b, p, w, y, col, discs = true) {
  const hw = w / 2, up = [0, 1, 0];
  for (let i = 0; i < p.length; i++) {
    const [ax, az] = p[i];
    if (discs) for (let k = 0; k < 8; k++) {
      const a0 = (k / 8) * Math.PI * 2, a1 = ((k + 1) / 8) * Math.PI * 2;
      b.tri([ax, y, az], [ax + Math.cos(a1) * hw, y, az + Math.sin(a1) * hw], [ax + Math.cos(a0) * hw, y, az + Math.sin(a0) * hw], col, null, up);
    }
    if (i === p.length - 1) break;
    const [bx, bz] = p[i + 1];
    const dx = bx - ax, dz = bz - az, l = Math.hypot(dx, dz) || 1;
    const nx = (-dz / l) * hw, nz = (dx / l) * hw;
    b.quad([ax + nx, y, az + nz], [bx + nx, y, bz + nz], [bx - nx, y, bz - nz], [ax - nx, y, az - nz], col, null, up);
  }
}
export function offsetLine(p, off) {
  const out = [];
  for (let i = 0; i < p.length; i++) {
    let nx = 0, nz = 0;
    for (const j of [i - 1, i]) {
      if (j < 0 || j >= p.length - 1) continue;
      const dx = p[j + 1][0] - p[j][0], dz = p[j + 1][1] - p[j][1], l = Math.hypot(dx, dz) || 1;
      nx += -dz / l; nz += dx / l;
    }
    const l = Math.hypot(nx, nz) || 1;
    out.push([p[i][0] + (nx / l) * off, p[i][1] + (nz / l) * off]);
  }
  return out;
}
function walk(p, start, stepFn, fn) {
  let next = start, acc = 0, n = 0;
  for (let i = 0; i < p.length - 1; i++) {
    const dx = p[i + 1][0] - p[i][0], dz = p[i + 1][1] - p[i][1], l = Math.hypot(dx, dz);
    if (!l) continue;
    while (next <= acc + l) {
      const t = (next - acc) / l;
      fn(p[i][0] + dx * t, p[i][1] + dz * t, dx / l, dz / l, next, n++);
      next += stepFn(n);
    }
    acc += l;
  }
  return acc;
}
const lineLength = (p) => { let t = 0; for (let i = 0; i < p.length - 1; i++) t += Math.hypot(p[i + 1][0] - p[i][0], p[i + 1][1] - p[i][1]); return t; };
function dashes(b, p, y, w, dash, gap, col) {
  walk(p, gap, () => dash + gap, (x, z, dx, dz) => {
    const nx = -dz * w / 2, nz = dx * w / 2, ex = dx * dash, ez = dz * dash;
    b.quad([x + nx, y, z + nz], [x + ex + nx, y, z + ez + nz], [x + ex - nx, y, z + ez - nz], [x - nx, y, z - nz], col, null, [0, 1, 0]);
  });
}
function fillPoly(b, p, y, col, uvScale = 0) {
  const tris = THREE.ShapeUtils.triangulateShape(p.map(([x, z]) => new THREE.Vector2(x, z)), []);
  for (const [i, j, k] of tris) {
    const uv = uvScale ? [[p[i][0] * uvScale, p[i][1] * uvScale], [p[j][0] * uvScale, p[j][1] * uvScale], [p[k][0] * uvScale, p[k][1] * uvScale]] : null;
    b.tri([p[i][0], y, p[i][1]], [p[j][0], y, p[j][1]], [p[k][0], y, p[k][1]], col, uv, [0, 1, 0]);
  }
}

// ---------- Buildings ----------
function obb(p) {
  let best = null;
  for (let i = 0; i < p.length; i++) {
    const a = p[i], c = p[(i + 1) % p.length];
    let dx = c[0] - a[0], dz = c[1] - a[1];
    const l = Math.hypot(dx, dz);
    if (l < 0.5) continue;
    dx /= l; dz /= l;
    let u0 = 1e9, u1 = -1e9, v0 = 1e9, v1 = -1e9;
    for (const q of p) { const u = q[0] * dx + q[1] * dz, v = -q[0] * dz + q[1] * dx; u0 = Math.min(u0, u); u1 = Math.max(u1, u); v0 = Math.min(v0, v); v1 = Math.max(v1, v); }
    const area = (u1 - u0) * (v1 - v0);
    if (!best || area < best.area) {
      const mu = (u0 + u1) / 2, mv = (v0 + v1) / 2;
      best = { area, cx: dx * mu - dz * mv, cz: dz * mu + dx * mv, ux: dx, uz: dz, L: u1 - u0, W: v1 - v0 };
    }
  }
  if (best && best.W > best.L) { [best.L, best.W] = [best.W, best.L]; [best.ux, best.uz] = [-best.uz, best.ux]; }
  return best;
}
function frontClass(mx, mz, nx, nz) {
  let best = null, bd = 1e9;
  for (const s of roadGrid.at(mx, mz)) {
    if (s[6] === 'foot' || s[6] === 'service') continue;
    const [d, qx, qz] = distToSeg(mx, mz, s);
    if (d > s[4] + 10 || (qx - mx) * nx + (qz - mz) * nz < 0.55 * d) continue;
    if (d < bd) { bd = d; best = s[6]; }
  }
  return best;
}
const CENTRE = [95, 140], CENTRE_R = 330;
const BIG = new Set(['Tesco', 'Lidl', 'Boyes']);

const TRIMC = '#cbbf9f', COPPER = '#6aa68c', LEAD = '#8d949c', TOWER = '#8f8468';
// Local frame helper: +z points along heading h, matching put().
const toWorld = (o, lx, y, lz) => { const s = Math.sin(o.h), c = Math.cos(o.h); return [o.x + lx * c + lz * s, y, o.z - lx * s + lz * c]; };

function building(b, bi, W, R, P, bays, ex) {
  let p = b.p.map((q) => [q[0], q[1]]);
  let A = 0;
  for (let i = 0; i < p.length; i++) { const j = (i + 1) % p.length; A += p[i][0] * p[j][1] - p[j][0] * p[i][1]; }
  A /= 2;
  if (A < 0) { p.reverse(); A = -A; }
  const o = obb(p);
  if (!o || A < 6) return;
  const central = Math.hypot(o.cx - CENTRE[0], o.cz - CENTRE[1]) < CENTRE_R;
  const h1 = hash(bi + 1), h2 = hash(bi * 3 + 7);
  const church = b.t === 'church', civic = b.t === 'civic' && !b.m, school = b.t === 'school';
  const shed = !church && !civic && !school && !b.m && (b.t === 'industrial' || BIG.has(b.n) || (A > 650 && !central));
  const small = A < 40;
  const feat = new Set(b.feat || []);
  const tall = b.win === 'tall' || civic;
  let floors = 2, floorH = 2.9, bayW = 4.6;
  if (church) { floors = 1; floorH = 8.5; bayW = 4; }
  else if (civic) { floors = 2; floorH = 4.2; }
  else if (school) { floors = 1; floorH = 4.6; }
  else if (shed) { floors = 1; floorH = A > 2000 ? 8 : 6.5; bayW = 7; }
  else if (small) { floors = 1; floorH = 2.7; }
  else if (central && A > 140 && h1 < 0.3 && !b.m) floors = 3;
  // Real height from LIDAR where there is one (b.lh): the middle value of surface minus terrain over the outline,
  // which sits about half way up the roof. Buildings with details read from photos keep those.
  if (b.lh && !church && !civic && !school && !small && !b.f && !b.fh) {
    if (shed) floorH = Math.min(12, Math.max(4, b.lh));
    else floors = Math.max(1, Math.min(4, Math.round((b.lh - 1.2) / floorH)));
  }
  if (b.f) floors = b.f;
  if (b.fh) floorH = b.fh;
  if (tall) bayW = 4.2;
  let m = b.m;
  if (!m) {
    const hm = hash(bi * 17 + 3);
    m = civic ? 'ashlar' : school ? 'brick_red' : central ? (hm < 0.12 ? 'render' : 'stone') : hm < 0.2 ? 'render' : hm < 0.34 ? 'brick_red' : 'stone';
  }
  const wallH = floors * floorH;
  const spread = m === 'stone' ? 0.24 : 0.12, k = 1 - spread + h2 * spread, warm = hash(bi * 5 + 2);
  const tint = shed ? new THREE.Color(1, 1, 1) : new THREE.Color(k, k * (0.96 + warm * 0.04), k * (0.9 + warm * 0.08));
  const T = (part) => matTile(m, part);
  const blank = tileUV(shed ? TILE.SHED : T(PART.BLANK));
  let front = null, forced = -1, pubFront = null;
  if (b.frontAt) { let bd = 1e9; for (let i = 0; i < p.length; i++) { const c = p[(i + 1) % p.length], d = Math.hypot((p[i][0] + c[0]) / 2 - b.frontAt[0], (p[i][1] + c[1]) / 2 - b.frontAt[1]); if (d < bd) { bd = d; forced = i; } } }

  for (let i = 0; i < p.length; i++) {
    const a = p[i], c = p[(i + 1) % p.length];
    let dx = c[0] - a[0], dz = c[1] - a[1];
    const L = Math.hypot(dx, dz);
    if (L < 0.2) continue;
    dx /= L; dz /= L;
    const nx = dz, nz = -dx;
    walls.add(a[0], a[1], c[0], c[1], 1.5, [a[0], a[1], c[0], c[1], 0]);
    const fr = i === forced ? 'primary' : frontClass((a[0] + c[0]) / 2, (a[1] + c[1]) / 2, nx, nz);
    if (fr) { const score = L + (fr === 'primary' ? 1000 : 0) + (i === forced ? 1e6 : 0); if (!front || score > front.score) front = { score, x: (a[0] + c[0]) / 2, z: (a[1] + c[1]) / 2, nx, nz, L }; }
    const plain = !church && !shed && !school && !small && !tall && !feat.has('firedoors');
    const shopFront = fr && plain && (b.shops !== undefined ? b.shops : (fr === 'primary' || fr === 'tertiary') && central);
    const pubEdge = b.pub && i === forced;
    const nb = pubEdge && b.bays ? b.bays : L < 3 ? 1 : Math.max(1, Math.round(L / bayW));
    const sStar = pubEdge ? Math.max(0, Math.min(L, (b.frontAt[0] - a[0]) * dx + (b.frontAt[1] - a[1]) * dz)) : 0;
    const doorJ = pubEdge ? Math.min(nb - 1, Math.floor(sStar / (L / nb))) : -1, doorS = (doorJ + 0.5) * (L / nb);
    if (pubEdge) {
      const cx = a[0] + dx * doorS, cz = a[1] + dz * doorS;
      pubFront = { x: (a[0] + c[0]) / 2, z: (a[1] + c[1]) / 2, nx, nz, L, doorLx: -(doorS - L / 2) };
      if (b.interior) ex.doors.push({ name: b.interior, x: cx + nx * 1.3, z: cz + nz * 1.3, nx, nz });
      if (b.band) ex.bands.push({ text: b.band, x: cx + nx * 0.1, z: cz + nz * 0.1, y: (b.bandFloor || floors - 1) * floorH - 0.12, nx, nz, w: Math.min(L, b.bandW || 14) * 0.92 });
    }
    for (let j = 0; j < nb; j++) {
      const s0 = (j * L) / nb, s1 = ((j + 1) * L) / nb;
      const x0 = a[0] + dx * s0, z0 = a[1] + dz * s0, x1 = a[0] + dx * s1, z1 = a[1] + dz * s1;
      const hb = hash(bi * 31 + i * 7 + j);
      let inPub = false;
      for (let f = 0; f < floors; f++) {
        let t;
        if (L < 3) t = shed ? TILE.SHED : T(PART.BLANK);
        else if (church) t = TILE.CHURCH;
        else if (shed) t = fr && hb < 0.3 ? (BIG.has(b.n) ? TILE.GLASS : TILE.SHED_DOOR) : TILE.SHED;
        else if (feat.has('firedoors')) t = fr && j < 2 ? TILE.FIRE_DOOR : T(PART.UPPER_B);
        else if (small) t = T(PART.BLANK);
        else if (pubEdge && Math.abs((s0 + s1) / 2 - doorS) < 7.5) { inPub = true; t = f === 0 ? (j === doorJ ? TILE.PUB_DOOR : TILE.PUB_WIN) : T(PART.UPPER); }
        else if (b.pub && b.shops === false) t = f === 0 ? T(hb < 0.5 ? PART.BACK : PART.BLANK) : T(hb < 0.7 ? PART.UPPER_B : PART.BLANK);
        else if (tall) t = f === 0 && fr && j === Math.floor(nb / 2) ? T(PART.TALL_DOOR) : T(PART.TALL);
        else if (school) t = T(PART.UPPER_B);
        else if (f === 0) t = fr ? (shopFront ? TILE.SHOP + Math.floor(hb * SHOP_VARIANTS) : T(hb < 0.5 ? PART.HOUSE : PART.HOUSE_B)) : T(hb < 0.55 ? PART.BACK : PART.BLANK);
        else t = fr ? T(PART.UPPER) : T(hash(hb * 99 + f) < 0.6 ? PART.UPPER_B : PART.BLANK);
        const y0 = f * floorH, y1 = y0 + floorH;
        W.quad([x1, y0, z1], [x0, y0, z0], [x0, y1, z0], [x1, y1, z1], tint, tileUV(t));
      }
      if (fr && !inPub) bays.push({ x: (x0 + x1) / 2, z: (z0 + z1) / 2, nx, nz, w: L / nb, y: floorH * 0.855, shop: shopFront, used: false });
    }
  }

  if (b.n && front) ex.fronts.set(b.n, front);

  // Roof
  const ratio = A / (o.L * o.W);
  const slate = 0.75 + hash(bi + 77) * 0.35;
  // Slate grey, or brown tile on the cricket pavilion.
  const rc = b.feat && b.feat.includes('pavilion') ? new THREE.Color(0.86, 0.56, 0.42) : new THREE.Color(slate, slate, slate * 1.04);
  const ux = o.ux, uz = o.uz, vx = -o.uz, vz = o.ux, hl = o.L / 2, hw = o.W / 2;
  const at = (ud, vd, y) => [o.cx + ux * ud + vx * vd, y, o.cz + uz * ud + vz * vd];
  const hd = Math.atan2(ux, uz);
  let kind = b.r || (shed || small ? 'flat' : o.W <= 24 && ratio > (church ? 0.45 : 0.6) ? 'gable' : 'mansard');
  if ((kind === 'gable' || kind === 'hip') && ratio < 0.5) kind = 'mansard';
  let roofTop = wallH;
  if (kind === 'gable') {
    const rh = church ? o.W * 0.55 : Math.min(4.2, Math.max(1.2, o.W * 0.3));
    roofTop = wallH + rh;
    const r1 = at(-hl - 0.25, 0, roofTop), r2 = at(hl + 0.25, 0, roofTop);
    const sl = Math.hypot(hw + 0.4, rh) / 2.4, ul = (o.L + 0.5) / 2.4;
    for (const sv of [1, -1]) R.quad(at(-hl - 0.25, sv * (hw + 0.4), wallH - 0.12), at(hl + 0.25, sv * (hw + 0.4), wallH - 0.12), r2, r1, rc, [0, 0, ul, sl], [0, 1, 0]);
    for (const su of [1, -1]) W.tri(at(su * hl, -hw, wallH), at(su * hl, hw, wallH), at(su * hl, 0, roofTop), tint, [[blank[0], blank[1]], [blank[2], blank[1]], [(blank[0] + blank[2]) / 2, blank[3]]], [ux * su, 0, uz * su]);
    if (!church && !small && o.L > 6) {
      const stoneC = C(m === 'brick_red' ? '#8a4a3a' : '#8f8068'), pot = C('#b5623c');
      for (let s = -hl + 2.3; s < hl - 1; s += 9.2) {
        const org = { x: o.cx + ux * s, z: o.cz + uz * s, h: hd };
        put(P, BOX, org, 0, roofTop + 0.35, 0, 1.0, 1.5, 0.6, stoneC);
        put(P, POLE, org, -0.25, roofTop + 1.25, 0, 0.13, 0.36, 0.13, pot);
        put(P, POLE, org, 0.25, roofTop + 1.25, 0, 0.13, 0.36, 0.13, pot);
      }
    }
  } else if (kind === 'hip') {
    const rh = Math.min(4.6, hw * 0.55), rl = Math.max(0, hl - hw);
    roofTop = wallH + rh;
    const r1 = at(-rl, 0, roofTop), r2 = at(rl, 0, roofTop), e = 0.45, y = wallH - 0.1;
    for (const sv of [1, -1]) R.quad(at(-hl - e, sv * (hw + e), y), at(hl + e, sv * (hw + e), y), r2, r1, rc, [0, 0, o.L / 2.4, Math.hypot(hw, rh) / 2.4], [0, 1, 0]);
    for (const su of [1, -1]) R.tri(at(su * (hl + e), -hw - e, y), at(su * (hl + e), hw + e, y), su > 0 ? r2 : r1, rc, [[0, 0], [o.W / 2.4, 0], [o.W / 4.8, Math.hypot(hw, rh) / 2.4]], [0, 1, 0]);
  } else if (kind === 'mansard') {
    // Slate slopes rising from every wall to a flat top: reads as a pitched roof from the street.
    const D = Math.min(5, Math.sqrt(A) * 0.26), rh = D * 0.8, n = p.length, q = [];
    for (let i = 0; i < n; i++) {
      const a = p[(i + n - 1) % n], v = p[i], c = p[(i + 1) % n];
      let d0x = v[0] - a[0], d0z = v[1] - a[1], d1x = c[0] - v[0], d1z = c[1] - v[1];
      const l0 = Math.hypot(d0x, d0z) || 1, l1 = Math.hypot(d1x, d1z) || 1;
      const n0x = -d0z / l0, n0z = d0x / l0, n1x = -d1z / l1, n1z = d1x / l1; // inward normals
      const den = Math.max(0.4, 1 + n0x * n1x + n0z * n1z);
      q.push([v[0] + ((n0x + n1x) / den) * D, v[1] + ((n0z + n1z) / den) * D]);
    }
    roofTop = wallH + rh;
    for (let i = 0; i < n; i++) {
      const j = (i + 1) % n, L = Math.hypot(p[j][0] - p[i][0], p[j][1] - p[i][1]);
      R.quad([p[i][0], wallH - 0.05, p[i][1]], [p[j][0], wallH - 0.05, p[j][1]], [q[j][0], roofTop, q[j][1]], [q[i][0], roofTop, q[i][1]], rc, [0, 0, L / 2.4, D / 2.4], [0, 1, 0]);
    }
    fillPoly(R, q, roofTop, new THREE.Color(0.4, 0.41, 0.44), 1 / 2.4);
  } else {
    fillPoly(R, p, wallH + 0.02, new THREE.Color(0.42, 0.43, 0.46), 1 / 2.4);
    const cap = feat.has('firedoors') || m === 'brick_brown' ? C('#e4e0d4') : C('#6d6f73');
    for (let i = 0; i < p.length; i++) {
      const a = p[i], c = p[(i + 1) % p.length];
      W.quad([c[0], wallH, c[1]], [a[0], wallH, a[1]], [a[0], wallH + 0.5, a[1]], [c[0], wallH + 0.5, c[1]], cap, null);
    }
    roofTop = wallH + 0.5;
  }

  // Landmark features read from photos
  const trim = C(TRIMC);
  if (front && feat.has('portico')) {
    const f = { x: front.x, z: front.z, h: Math.atan2(front.nx, front.nz) }, hC = wallH - 1.5;
    put(P, BOX, f, 0, 0.2, 1.7, 10.4, 0.4, 3.6, trim);
    for (const lx of [-3.9, -1.3, 1.3, 3.9]) { put(P, CYL, f, lx, 0.4 + hC / 2, 2.6, 0.36, hC, 0.36, trim); put(P, BOX, f, lx, 0.55, 2.6, 0.95, 0.3, 0.95, trim); put(P, BOX, f, lx, 0.4 + hC - 0.12, 2.6, 0.95, 0.24, 0.95, trim); }
    const yE = 0.4 + hC;
    put(P, BOX, f, 0, yE + 0.4, 1.6, 10.2, 0.8, 3.3, trim);
    const y0 = yE + 0.8, y1 = y0 + 2.3, w2 = 5.3;
    P.tri(toWorld(f, -w2, y0, 3.3), toWorld(f, w2, y0, 3.3), toWorld(f, 0, y1, 3.3), trim, null, [front.nx, 0, front.nz]);
    for (const s of [-1, 1]) P.quad(toWorld(f, s * w2, y0, 3.4), toWorld(f, 0, y1, 3.4), toWorld(f, 0, y1, -0.2), toWorld(f, s * w2, y0, -0.2), C(COPPER), null, [0, 1, 0]);
  }
  if (front && feat.has('planters')) {
    const f = { x: front.x, z: front.z, h: Math.atan2(front.nx, front.nz) }, cols = ['#d6402f', '#f0c330', '#e86aa0', '#f08a24', '#ffffff'];
    for (const lx of [-16, -12.5, -9, 9, 12.5, 16]) {
      if (Math.abs(lx) > front.L / 2 - 1.5) continue;
      put(P, BOX, f, lx, 0.4, 4.2, 2.6, 0.8, 0.9, C('#2f3136'));
      for (let i = 0; i < 9; i++) put(P, SPH, f, lx - 1.1 + i * 0.275, 0.92 + hash(bi + i + lx) * 0.12, 4.2 + (hash(i * 3 + lx) - 0.5) * 0.4, 0.2, 0.2, 0.2, C(cols[Math.floor(hash(i * 7 + lx * 3) * cols.length)]));
    }
  }
  if (pubFront) {
    const f = { x: pubFront.x, z: pubFront.z, h: Math.atan2(pubFront.nx, pubFront.nz) }, dl = pubFront.doorLx, half = pubFront.L / 2;
    if (feat.has('fence')) {
      // blue timber fence with picnic benches behind it, as at the Starkie Arms
      const blue = C('#1f3f6b'), wood = C('#6b4a32'), out = 2.7;
      const seg = (lx0, lz0, lx1, lz1) => {
        const len = Math.hypot(lx1 - lx0, lz1 - lz0);
        if (len < 0.3) return;
        const along = Math.abs(lx1 - lx0) > Math.abs(lz1 - lz0);
        put(P, BOX, f, (lx0 + lx1) / 2, 0.6, (lz0 + lz1) / 2, along ? len : 0.08, 1.2, along ? 0.08 : len, blue);
        const p0 = toWorld(f, lx0, 0, lz0), p1 = toWorld(f, lx1, 0, lz1);
        walls.add(p0[0], p0[2], p1[0], p1[2], 1.5, [p0[0], p0[2], p1[0], p1[2], 1]);
      };
      seg(-half, out, dl - 1.2, out); seg(dl + 1.2, out, half, out); seg(-half, 0, -half, out); seg(half, 0, half, out);
      for (const lx of [dl - 4.3, dl + 4.3]) {
        if (Math.abs(lx) > half - 1) continue;
        put(P, BOX, f, lx, 0.76, 1.35, 1.7, 0.07, 0.75, wood);
        for (const dz2 of [-0.62, 0.62]) put(P, BOX, f, lx, 0.46, 1.35 + dz2, 1.7, 0.05, 0.28, wood);
        for (const e of [-0.6, 0.6]) put(P, BOX, f, lx + e, 0.38, 1.35, 0.08, 0.76, 1.5, C('#4a4f55'));
      }
    }
    if (feat.has('pubplanters')) for (const e of [-5.3, -2.8, 2.8, 5.3]) {
      put(P, BOX, f, dl + e, 0.95, 0.45, 2.1, 0.32, 0.45, C('#2f3136'));
      for (let i = 0; i < 7; i++) put(P, SPH, f, dl + e - 0.9 + i * 0.3, 1.2, 0.45, 0.2, 0.2, 0.2, C(i % 3 ? '#4f8a4a' : '#d6402f'));
    }
    // lantern over the door
    put(P, BOX, f, dl, 2.55, 0.35, 0.3, 0.4, 0.3, C('#e9c46a'));
    put(P, BOX, f, dl, 2.8, 0.2, 0.05, 0.05, 0.4, C('#2b2b2b'));
  }
  // Four frontages dressed from reference pictures supplied by the site owner (no picture is reproduced).
  const edgeAt = (i) => { const a = p[i], c = p[(i + 1) % p.length], L = Math.hypot(c[0] - a[0], c[1] - a[1]) || 1, dx = (c[0] - a[0]) / L, dz = (c[1] - a[1]) / L; return { a, c, L, dx, dz, nx: dz, nz: -dx }; };
  // A frame on wall i, centred s metres along it: +x is the viewer's right, +z is out from the wall.
  const wallFrame = (e, s) => ({ x: e.a[0] + e.dx * s, z: e.a[1] + e.dz * s, h: Math.atan2(e.nx, e.nz) });
  if (front && feat.has('gablefront')) {
    // 57 to 65 Burnley Road: three storeys of stone over shops, a central gable, two tall white bay windows
    // and an arched doorway with a balcony at the left end.
    const f = { x: front.x, z: front.z, h: Math.atan2(front.nx, front.nz) }, st = C('#b3a07a'), white = C('#f1efe6'), glass = C('#2b3640'), SLATE = C('#474b53');
    const y0 = wallH - 0.1, y1 = wallH + 3.4, w2 = 4.6;
    P.tri(toWorld(f, -w2, y0, 0.06), toWorld(f, w2, y0, 0.06), toWorld(f, 0, y1, 0.06), st, null, [front.nx, 0, front.nz]);
    for (const sd of [-1, 1]) P.quad(toWorld(f, sd * (w2 + 0.3), y0 - 0.1, 0.4), toWorld(f, 0, y1 + 0.15, 0.4), toWorld(f, 0, y1 + 0.15, -5), toWorld(f, sd * (w2 + 0.3), y0 - 0.1, -5), SLATE, null, [0, 1, 0]);
    put(P, BOX, f, 0, wallH + 1.0, 0.1, 1.7, 1.1, 0.08, glass);
    put(P, SPH, f, 0, wallH + 1.55, 0.08, 0.85, 0.6, 0.06, glass);
    put(P, BOX, f, 0, y0 + 0.05, 0.12, w2 * 2 + 0.6, 0.22, 0.3, C(TRIMC));
    for (const lx of [-6.2, 6.2]) {
      if (Math.abs(lx) > front.L / 2 - 2) continue;
      put(P, BOX, f, lx, floorH * 2, 0.45, 3.3, floorH * 2 - 0.5, 0.9, white);
      for (const fl of [1, 2]) { put(P, BOX, f, lx, fl * floorH + floorH * 0.5, 0.91, 2.7, floorH * 0.56, 0.05, glass); for (const d of [-0.45, 0.45]) put(P, BOX, f, lx + d, fl * floorH + floorH * 0.5, 0.93, 0.07, floorH * 0.56, 0.04, white); }
    }
    const ex0 = -(front.L / 2 - 2.4);
    put(P, BOX, f, ex0, 1.55, 0.12, 2.7, 3.1, 0.24, st);
    put(P, BOX, f, ex0, 1.25, 0.2, 1.6, 2.5, 0.12, C('#1c2026'));
    put(P, SPH, f, ex0, 2.5, 0.2, 0.8, 0.55, 0.08, C('#1c2026'));
    put(P, BOX, f, ex0, floorH + 0.5, 0.55, 3.3, 0.9, 1.1, white);
    for (const lx of [-4.6, 4.6]) { put(P, BOX, f, lx, 0.55, 3.0, 1.1, 1.1, 1.1, C('#1b1c1f')); put(P, SPH, f, lx, 1.5, 3.0, 0.55, 0.6, 0.55, C('#7a3340')); }
  }
  if (forced >= 0 && feat.has('cornershop')) {
    // The corner of Church Street and Alma Street: a splayed corner painted white, dark red pilasters and
    // fascia, a wooden corner door and arched windows above. No name is shown.
    const n = p.length, white = C('#f2efe8'), red = C('#7a1f28'), glass = C('#27303a'), wood = C('#8a5a34');
    const face = (i, s0, s1, door) => {
      const e = edgeAt(i); s0 = Math.max(0, s0); s1 = Math.min(e.L, s1);
      const len = s1 - s0; if (len < 0.8) return;
      const f = wallFrame(e, (s0 + s1) / 2);
      put(P, BOX, f, 0, wallH / 2, 0.03, len, wallH, 0.1, white);
      put(P, BOX, f, 0, floorH - 0.28, 0.1, len, 0.5, 0.14, red);
      for (const d of [-1, 1]) put(P, BOX, f, d * (len / 2 - 0.16), floorH / 2, 0.1, 0.3, floorH, 0.18, red);
      const nw = Math.max(1, Math.round(len / 3.2));
      for (let k = 0; k < nw; k++) {
        const lx = -len / 2 + ((k + 0.5) * len) / nw, ww = Math.min(1.1, len / nw - 0.9);
        put(P, BOX, f, lx, floorH + (wallH - floorH) * 0.5, 0.09, ww, (wallH - floorH) * 0.5, 0.06, glass);
        put(P, SPH, f, lx, floorH + (wallH - floorH) * 0.75, 0.08, ww / 2, 0.4, 0.05, glass);
        if (door && k === Math.floor(nw / 2)) put(P, BOX, f, lx, 1.1, 0.1, 1.1, 2.2, 0.08, wood);
        else put(P, BOX, f, lx, 1.35, 0.09, Math.min(1.9, len / nw - 0.8), 1.5, 0.06, glass);
      }
    };
    face(forced, 0, 99, true);
    if (b.sign) { const e = edgeAt(forced), f = wallFrame(e, e.L / 2); ex.bands.push({ text: b.sign, plain: true, bg: '#5e1820', fg: '#f3e2b0', x: f.x + e.nx * 0.2, z: f.z + e.nz * 0.2, y: floorH - 0.28, nx: e.nx, nz: e.nz, w: Math.max(2.6, e.L * 0.95), h: 0.44 }); }
    const prev = (forced + n - 1) % n, next = (forced + 1) % n;
    face(prev, edgeAt(prev).L - 6.4, 99, false);
    face(next, 0, 6.4, false);
  }
  // Single frontages inside a longer outline: each spot picks the wall nearest its point.
  for (const sp of b.spots || []) {
    let bi2 = 0, bd = 1e9, bq = null;
    for (let i = 0; i < p.length; i++) { const c = p[(i + 1) % p.length], r = distToSeg(sp.at[0], sp.at[1], [p[i][0], p[i][1], c[0], c[1]]); if (r[0] < bd) { bd = r[0]; bi2 = i; bq = r; } }
    const e = edgeAt(bi2), f = { x: bq[1], z: bq[2], h: Math.atan2(e.nx, e.nz) };
    if (sp.k === 'black') {
      // 90 Burnley Road: a black painted shop front with a moulded cornice, white lettering and windows frosted
      // along the bottom.
      const black = C('#15161a'), warm = C('#e6d9b8'), frost = C('#f1efe9');
      put(P, BOX, f, 0, 1.8, 0.05, 7.4, 3.6, 0.14, black);
      put(P, BOX, f, 0, 3.66, 0.16, 7.7, 0.2, 0.36, black);
      for (let k = -8; k <= 8; k++) put(P, BOX, f, k * 0.45, 3.5, 0.2, 0.2, 0.12, 0.14, black);
      for (const lx of [-2.35, 2.35]) { put(P, BOX, f, lx, 1.55, 0.13, 2.6, 2.1, 0.03, warm); put(P, BOX, f, lx, 0.85, 0.15, 2.6, 0.7, 0.03, frost); }
      put(P, BOX, f, 0, 1.2, 0.13, 1.0, 2.2, 0.03, warm); put(P, BOX, f, 0, 0.7, 0.15, 1.0, 0.6, 0.03, frost);
      if (sp.sign) ex.bands.push({ text: sp.sign, plain: true, bg: '#15161a', fg: '#ffffff', x: f.x + e.nx * 0.16, z: f.z + e.nz * 0.16, y: 3.0, nx: e.nx, nz: e.nz, w: 5.2, h: 0.78 });
    } else if (sp.k === 'pub') {
      // 68 Burnley Road: a two storey stone pub front, a door in a white surround between sash windows, gold
      // lettering between the floors, a lantern and hanging baskets.
      const st = C('#a8946c'), white = C('#f1efe6'), glass = C('#27303a');
      put(P, BOX, f, 0, floorH / 2, 0.04, 11, floorH, 0.1, st);
      put(P, BOX, f, 0, 1.3, 0.1, 1.5, 2.6, 0.14, white); put(P, BOX, f, 0, 1.1, 0.16, 0.95, 2.2, 0.06, C('#1c2026'));
      for (const lx of [-3.6, 3.6]) { put(P, BOX, f, lx, 1.55, 0.1, 1.5, 1.7, 0.06, white); put(P, BOX, f, lx, 1.55, 0.13, 1.25, 1.45, 0.04, glass); }
      for (const lx of [-1.5, 1.5]) { put(P, SPH, f, lx, 2.45, 0.5, 0.3, 0.26, 0.3, C('#4f8a4a')); put(P, SPH, f, lx, 2.55, 0.5, 0.2, 0.14, 0.2, C('#d6402f')); }
      put(P, BOX, f, 0, 2.75, 0.35, 0.28, 0.38, 0.28, C('#e9c46a'));
      if (sp.sign) ex.bands.push({ text: sp.sign, x: f.x + e.nx * 0.12, z: f.z + e.nz * 0.12, y: floorH + 0.3, nx: e.nx, nz: e.nz, w: 5.5 });
    } else if (sp.k === 'horse') {
      // 42 Burnley Road: a dark brick block rising above the shops, with a white horse standing on a ledge at
      // first floor height, facing along the row.
      const dark = C('#3b302b'), white = C('#f3f1ea');
      put(P, BOX, f, 0, floorH + (wallH + 1.7 - floorH) / 2, 0.12, 7, wallH + 1.7 - floorH, 0.5, dark);
      put(P, BOX, f, 0, wallH + 1.75, 0.12, 7.3, 0.16, 0.7, C('#e4e0d4'));
      const y = floorH + 0.75, lz = 0.85;
      put(P, BOX, f, 0, y - 0.06, lz, 3.0, 0.12, 0.8, C('#d8d4c8'));
      put(P, SPH, f, 0.1, y + 1.3, lz, 0.95, 0.42, 0.3, white);
      for (const lx of [-0.55, -0.4, 0.7, 0.85]) put(P, CYL, f, lx, y + 0.5, lz + (lx === -0.4 || lx === 0.7 ? 0.13 : -0.13), 0.065, 1.0, 0.065, white);
      put(P, BOX, f, -0.86, y + 1.64, lz, 0.38, 1.1, 0.24, white, -0.6);
      put(P, BOX, f, -1.4, y + 1.98, lz, 0.6, 0.27, 0.2, white, 0.5);
      for (const dz2 of [-0.07, 0.07]) put(P, BOX, f, -1.16, y + 2.24, lz + dz2, 0.06, 0.16, 0.05, white);
      put(P, BOX, f, 1.08, y + 1.05, lz, 0.1, 0.75, 0.1, white, 0.25);
    }
  }
  if (front && feat.has('pavilion')) {
    // The cricket pavilion: white walls under a brown tiled roof, a central gable with a clock facing the
    // field, and a veranda with benches and hanging baskets.
    const f = { x: front.x, z: front.z, h: Math.atan2(front.nx, front.nz) }, white = C('#f4f2ea'), tile = C('#8a5a44'), dark = C('#23272d');
    const half = front.L / 2, y0 = wallH - 0.05, y1 = wallH + 2.6, w2 = 3.2;
    P.tri(toWorld(f, -w2, y0, 0.5), toWorld(f, w2, y0, 0.5), toWorld(f, 0, y1, 0.5), white, null, [front.nx, 0, front.nz]);
    for (const sd of [-1, 1]) P.quad(toWorld(f, sd * (w2 + 0.3), y0 - 0.1, 0.8), toWorld(f, 0, y1 + 0.15, 0.8), toWorld(f, 0, y1 + 0.15, -4), toWorld(f, sd * (w2 + 0.3), y0 - 0.1, -4), tile, null, [0, 1, 0]);
    put(P, SPH, f, 0, wallH + 1.05, 0.52, 0.5, 0.5, 0.05, C('#fbfaf5'));
    put(P, BOX, f, 0, wallH + 1.2, 0.58, 0.05, 0.36, 0.03, dark); put(P, BOX, f, 0.12, wallH + 1.05, 0.58, 0.26, 0.05, 0.03, dark);
    put(P, BOX, f, 0, wallH - 0.2, 1.5, front.L + 0.6, 0.16, 3.0, tile);
    put(P, BOX, f, 0, 0.12, 1.5, front.L + 0.4, 0.24, 3.0, C('#b9b3a4'));
    for (let lx = -half; lx <= half + 0.01; lx += front.L / 5) put(P, BOX, f, lx, wallH / 2 - 0.1, 2.85, 0.14, wallH - 0.2, 0.14, white);
    for (const lx of [-half * 0.6, half * 0.6]) { put(P, BOX, f, lx, 0.7, 2.2, 2.4, 0.08, 0.5, dark); put(P, BOX, f, lx, 1.0, 1.98, 2.4, 0.5, 0.06, dark); }
    for (const lx of [-half * 0.8, -half * 0.3, half * 0.3, half * 0.8]) put(P, SPH, f, lx, wallH - 0.75, 2.85, 0.28, 0.26, 0.28, C(Math.abs(lx) > half * 0.5 ? '#d6508a' : '#d6402f'));
    if (b.sign) ex.bands.push({ text: b.sign, plain: true, bg: '#1d3b2a', fg: '#f3e2b0', x: f.x + front.nx * 3.05, z: f.z + front.nz * 3.05, y: wallH - 0.2, nx: front.nx, nz: front.nz, w: Math.min(8, front.L * 0.6), h: 0.5 });
  }
  if (feat.has('sportshall')) {
    // The leisure centre on Park Road: low brown brick ranges under a taller grey clad hall with a band of
    // windows, and a canopy over the way in.
    const c0 = { x: o.cx, z: o.cz, h: hd }, clad = C('#9aa1a6'), dark = C('#3b4650');
    put(P, BOX, c0, 0, wallH + 2.5, o.L * 0.14, o.W * 0.78, 5, o.L * 0.58, clad);
    put(P, BOX, c0, 0, wallH + 3.7, o.L * 0.14, o.W * 0.78 + 0.12, 1.0, o.L * 0.5, dark);
    put(P, BOX, c0, 0, wallH + 5.1, o.L * 0.14, o.W * 0.78 + 0.5, 0.25, o.L * 0.58 + 0.5, C('#6d7378'));
    if (front) {
      const f = { x: front.x, z: front.z, h: Math.atan2(front.nx, front.nz) }, red = C('#7a2a2a');
      put(P, BOX, f, 0, 3.0, 1.7, 7, 0.28, 3.4, red);
      for (const lx of [-3.2, 3.2]) put(P, CYL, f, lx, 1.45, 3.1, 0.09, 2.9, 0.09, C('#4a4f55'));
    }
  }
  if (feat.has('cupola')) {
    const c0 = { x: o.cx, z: o.cz, h: front ? Math.atan2(front.nx, front.nz) : hd }, y = roofTop - 0.3;
    put(P, BOX, c0, 0, y + 0.7, 0, 2.6, 1.4, 2.6, C('#e9e6da'));
    put(P, BOX, c0, 0, y + 2.6, 0, 2.0, 2.4, 2.0, C(COPPER));
    for (const [lx, lz, sx, sz] of [[0, 1.02, 1.3, 0.06], [0, -1.02, 1.3, 0.06], [1.02, 0, 0.06, 1.3], [-1.02, 0, 0.06, 1.3]]) put(P, BOX, c0, lx, y + 2.7, lz, sx, 1.3, sz, C('#f4f1e6'));
    put(P, SPH, c0, 0, y + 3.8, 0, 1.3, 1.0, 1.3, C(COPPER));
    put(P, CYL, c0, 0, y + 5.2, 0, 0.06, 1.4, 0.06, C('#d9b44a'));
  }
  if (feat.has('turret')) {
    // rounded corner bay with a small lantern, at the sharpest corner of the outline
    let best = 0, ba = 9;
    for (let i = 0; i < p.length; i++) {
      const a = p[(i + p.length - 1) % p.length], v = p[i], c = p[(i + 1) % p.length];
      const ang = Math.acos(Math.max(-1, Math.min(1, ((a[0] - v[0]) * (c[0] - v[0]) + (a[1] - v[1]) * (c[1] - v[1])) / ((Math.hypot(a[0] - v[0], a[1] - v[1]) || 1) * (Math.hypot(c[0] - v[0], c[1] - v[1]) || 1)))));
      if (ang < ba) { ba = ang; best = i; }
    }
    const v = p[best], dl = Math.hypot(o.cx - v[0], o.cz - v[1]) || 1, t0 = { x: v[0] + ((o.cx - v[0]) / dl) * 2.4, z: v[1] + ((o.cz - v[1]) / dl) * 2.4, h: 0 };
    const ash = C('#cdb98f');
    put(P, CYL, t0, 0, (wallH * 0.68) / 2, 0, 3.1, wallH * 0.68, 3.1, ash);
    put(P, CYL, t0, 0, wallH * 0.68 + 0.3, 0, 3.3, 0.6, 3.3, trim);
    put(P, CYL, t0, 0, wallH * 0.34, 0, 3.2, 0.3, 3.2, trim);
    const l0 = { x: o.cx + (v[0] - o.cx) * 0.45, z: o.cz + (v[1] - o.cz) * 0.45, h: 0 };
    put(P, BOX, l0, 0, roofTop + 1.6, 0, 3.2, 3.2, 3.2, ash);
    put(P, CYL, l0, 0, roofTop + 4.0, 0, 1.3, 1.6, 1.3, C(LEAD));
    put(P, SPH, l0, 0, roofTop + 4.8, 0, 1.35, 1.1, 1.35, C(LEAD));
    put(P, CYL, l0, 0, roofTop + 6.4, 0, 0.05, 1.4, 0.05, C('#3a3d42'));
  }
  if (feat.has('drilltower')) {
    const t0 = { x: o.cx - ux * (hl + 2.5) - vx * (hw - 1.5), z: o.cz - uz * (hl + 2.5) - vz * (hw - 1.5), h: hd }, g = C('#4a4f55');
    for (const lx of [-1.1, 1.1]) for (const lz of [-1.1, 1.1]) put(P, BOX, t0, lx, 6.5, lz, 0.16, 13, 0.16, g);
    for (const y of [3.4, 6.6, 9.8, 13]) { put(P, BOX, t0, 0, y, 0, 2.5, 0.12, 2.5, g); for (const lz of [-1.2, 1.2]) put(P, BOX, t0, 0, y + 0.55, lz, 2.4, 0.06, 0.06, g); }
  }
  if (feat.has('tower')) {
    // St Leonard's: a south-west tower of four stages, with four clock faces, battlements, four corner turrets with
    // spirelets and four minor pinnacles (Historic England, list entry 1238303). The height is not in the listing.
    let sw = p[0];
    for (const q of p) if (q[1] - q[0] > sw[1] - sw[0]) sw = q;
    const dl = Math.hypot(o.cx - sw[0], o.cz - sw[1]) || 1;
    const t0 = { x: sw[0] + ((o.cx - sw[0]) / dl) * 4.4, z: sw[1] + ((o.cz - sw[1]) / dl) * 4.4, h: hd }, st = C(TOWER), th = wallH + 15, e = 3.3;
    const DARKW = C('#2b3038'), FACE = C('#ece8da');
    put(P, BOX, t0, 0, th / 2, 0, 6.6, th, 6.6, st);
    for (const f of [0.3, 0.55, 0.8]) put(P, BOX, t0, 0, th * f, 0, 6.9, 0.25, 6.9, trim);
    put(P, BOX, t0, 0, th + 0.4, 0, 7.1, 0.8, 7.1, st);
    const yc = th * 0.675;
    for (const [ax, az] of [[0, 1], [0, -1], [1, 0], [-1, 0]]) {
      const lx = ax * (e + 0.04), lz = az * (e + 0.04), wide = az ? 1 : 0;
      // belfry louvres in the top stage, a tall window lower down, and a clock face in a square panel
      for (const d of [-1, 1]) put(P, BOX, t0, lx + (wide ? d * 0.95 : 0), th - 2.6, lz + (wide ? 0 : d * 0.95), wide ? 1.1 : 0.06, 3, wide ? 0.06 : 1.1, DARKW);
      put(P, BOX, t0, lx, th * 0.42, lz, wide ? 1.2 : 0.06, 2.6, wide ? 0.06 : 1.2, DARKW);
      put(P, BOX, t0, lx, yc, lz, wide ? 2.3 : 0.1, 2.3, wide ? 0.1 : 2.3, trim);
      put(P, SPH, t0, lx + ax * 0.06, yc, lz + az * 0.06, wide ? 0.95 : 0.05, 0.95, wide ? 0.05 : 0.95, FACE);
      put(P, BOX, t0, lx + ax * 0.13, yc + 0.32, lz + az * 0.13, wide ? 0.09 : 0.04, 0.7, wide ? 0.04 : 0.09, DARKW);
      put(P, BOX, t0, lx + ax * 0.13 + (wide ? 0.22 : 0), yc, lz + az * 0.13 + (wide ? 0 : 0.22), wide ? 0.5 : 0.04, 0.09, wide ? 0.04 : 0.5, DARKW);
      // battlements, with a minor pinnacle in the middle of each side
      for (const d of [-1.7, 1.7]) put(P, BOX, t0, ax * 3.3 + (wide ? d : 0), th + 1.15, az * 3.3 + (wide ? 0 : d), wide ? 1.1 : 0.5, 0.7, wide ? 0.5 : 1.1, st);
      put(P, BOX, t0, ax * 3.3, th + 1.2, az * 3.3, 0.5, 0.8, 0.5, st);
      put(P, CONE, t0, ax * 3.3, th + 2.3, az * 3.3, 0.3, 1.5, 0.3, st);
    }
    for (const lx of [-3.2, 3.2]) for (const lz of [-3.2, 3.2]) {
      put(P, CYL, t0, lx, th + 1.5, lz, 0.55, 2.2, 0.55, st);
      put(P, CONE, t0, lx, th + 3.9, lz, 0.62, 2.6, 0.62, st);
    }
  }
  if (feat.has('nave') || feat.has('porch')) {
    // Work from the longest wall, which on a church is a side of the nave.
    let ei = 0, el = 0;
    for (let i = 0; i < p.length; i++) { const c = p[(i + 1) % p.length], l = Math.hypot(c[0] - p[i][0], c[1] - p[i][1]); if (l > el) { el = l; ei = i; } }
    const a = p[ei], c = p[(ei + 1) % p.length], dx = (c[0] - a[0]) / el, dz = (c[1] - a[1]) / el;
    const mx = (a[0] + c[0]) / 2, mz = (a[1] + c[1]) / 2;
    let inx = -dz, inz = dx;
    if ((o.cx - mx) * inx + (o.cz - mz) * inz < 0) { inx = -inx; inz = -inz; }
    // Depth of the body: the far wall that runs back the other way, not the transept or vestry beyond it.
    let depth = o.W;
    for (let i = 0; i < p.length; i++) {
      if (i === ei) continue;
      const q = p[i], r = p[(i + 1) % p.length], l = Math.hypot(r[0] - q[0], r[1] - q[1]);
      if (l > el * 0.4 && Math.abs(((r[0] - q[0]) * dx + (r[1] - q[1]) * dz) / l) > 0.95) depth = ((q[0] + r[0]) / 2 - mx) * inx + ((q[1] + r[1]) / 2 - mz) * inz;
    }
    const st = C(TOWER), SLATE = C('#474b53'), DARKW = C('#2b3038');
    if (feat.has('nave')) {
      // A clerestory rising above the aisle roofs, under a steeply pitched slate roof.
      const n0 = { x: mx + (inx * depth) / 2, z: mz + (inz * depth) / 2, h: Math.atan2(dx, dz) }, nw = depth * 0.24, hn = el / 2 - 0.4;
      const y0 = wallH + 3.2, y1 = y0 + nw * 1.35, ov = nw + 0.4;
      put(P, BOX, n0, 0, (wallH - 0.6 + y0) / 2, 0, nw * 2, y0 - wallH + 0.6, hn * 2, st);
      for (let lz = -hn + 2.2; lz < hn - 1; lz += 3.6) for (const sd of [-1, 1]) put(P, BOX, n0, sd * (nw + 0.02), wallH + 1.9, lz, 0.08, 1.5, 1.0, DARKW);
      for (const sd of [-1, 1]) {
        P.quad(toWorld(n0, sd * ov, y0 - 0.15, -hn - 0.3), toWorld(n0, sd * ov, y0 - 0.15, hn + 0.3), toWorld(n0, 0, y1, hn + 0.3), toWorld(n0, 0, y1, -hn - 0.3), SLATE, null, [0, 1, 0]);
        P.tri(toWorld(n0, -nw, y0, sd * hn), toWorld(n0, nw, y0, sd * hn), toWorld(n0, 0, y1 - 0.1, sd * hn), st, null, [dx * sd, 0, dz * sd]);
      }
    }
    if (feat.has('porch')) {
      // A single storey porch on the south side, clear of the tower.
      const west = a[0] < c[0] ? 1 : -1, sP = west * (0.4 * el - el / 2);
      const f = { x: mx + dx * sP, z: mz + dz * sP, h: Math.atan2(-inx, -inz) };
      put(P, BOX, f, 0, 1.7, 1.5, 4.2, 3.4, 3, st);
      put(P, BOX, f, 0, 1.25, 3.02, 1.5, 2.5, 0.06, DARKW);
      P.tri(toWorld(f, -2.3, 3.4, 3.05), toWorld(f, 2.3, 3.4, 3.05), toWorld(f, 0, 5.2, 3.05), st, null, [-inx, 0, -inz]);
      for (const sd of [-1, 1]) P.quad(toWorld(f, sd * 2.4, 3.3, 3.2), toWorld(f, 0, 5.25, 3.2), toWorld(f, 0, 5.25, -0.1), toWorld(f, sd * 2.4, 3.3, -0.1), SLATE, null, [0, 1, 0]);
      const k = [toWorld(f, -2.1, 0, 0), toWorld(f, -2.1, 0, 3), toWorld(f, 2.1, 0, 3), toWorld(f, 2.1, 0, 0)];
      for (let i = 0; i < 3; i++) walls.add(k[i][0], k[i][2], k[i + 1][0], k[i + 1][2], 1.5, [k[i][0], k[i][2], k[i + 1][0], k[i + 1][2], 1]);
    }
  }
}

// ---------- Football ground ----------
const centre = (p) => [p.reduce((s, q) => s + q[0], 0) / p.length, p.reduce((s, q) => s + q[1], 0) / p.length];
function footballGround(F, poly, scene) {
  const o = obb(poly);
  const G = new Batch(), h = Math.atan2(o.ux, o.uz), f = { x: o.cx, z: o.cz, h };
  // Local frame: z along the pitch, x across it. The mapped outline includes the run-off, so the lines sit inside it.
  const hl = o.L / 2 - 6, hw = Math.min(o.W / 2 - 5, 34), y = 0.06, WHITE = C('#f1efe6'), lw = 0.14;
  const line = (x0, z0, x1, z1) => {
    const a = toWorld(f, x0, y, z0), b = toWorld(f, x1, y, z1), dx = b[0] - a[0], dz = b[2] - a[2], l = Math.hypot(dx, dz) || 1, nx = (-dz / l) * lw, nz = (dx / l) * lw;
    F.quad([a[0] + nx, y, a[2] + nz], [b[0] + nx, y, b[2] + nz], [b[0] - nx, y, b[2] - nz], [a[0] - nx, y, a[2] - nz], WHITE, null, [0, 1, 0]);
  };
  // mown stripes
  for (let i = 0; i < 12; i++) if (i % 2) { const z0 = -hl + (i * 2 * hl) / 12, z1 = z0 + (2 * hl) / 12; const c = [[-hw, z0], [hw, z0], [hw, z1], [-hw, z1]].map(([lx, lz]) => toWorld(f, lx, 0.045, lz)); F.quad(c[0], c[1], c[2], c[3], C('#58a04c'), null, [0, 1, 0]); }
  line(-hw, -hl, hw, -hl); line(-hw, hl, hw, hl); line(-hw, -hl, -hw, hl); line(hw, -hl, hw, hl); line(-hw, 0, hw, 0);
  for (let k = 0; k < 28; k++) { const a0 = (k / 28) * 6.2832, a1 = ((k + 1) / 28) * 6.2832; line(Math.cos(a0) * 9.15, Math.sin(a0) * 9.15, Math.cos(a1) * 9.15, Math.sin(a1) * 9.15); }
  const DARK = C('#3a3f45'), LAMP = C('#f4f1d8'), BLUE = C('#1f4fa3'), GREY = C('#8d9299'), SEAT = C('#2a62c4');
  for (const e of [-1, 1]) {
    line(-20.15, e * hl, -20.15, e * (hl - 16.5)); line(20.15, e * hl, 20.15, e * (hl - 16.5)); line(-20.15, e * (hl - 16.5), 20.15, e * (hl - 16.5));
    line(-9.15, e * hl, -9.15, e * (hl - 5.5)); line(9.15, e * hl, 9.15, e * (hl - 5.5)); line(-9.15, e * (hl - 5.5), 9.15, e * (hl - 5.5));
    // goal
    for (const lx of [-3.66, 3.66]) put(G, CYL, f, lx, 1.22, e * hl, 0.06, 2.44, 0.06, WHITE);
    put(G, BOX, f, 0, 2.44, e * hl, 7.44, 0.12, 0.12, WHITE);
    put(G, BOX, f, 0, 1.2, e * (hl + 1.6), 7.3, 2.3, 0.03, C('#cfd6d2'));
    for (const lx of [-3.66, 3.66]) for (const [px, pz] of [[lx, e * hl]]) { const p0 = toWorld(f, px - 0.1, 0, pz), p1 = toWorld(f, px + 0.1, 0, pz); walls.add(p0[0], p0[2], p1[0], p1[2], 1.5, [p0[0], p0[2], p1[0], p1[2], 1]); }
    // floodlights at the corners
    for (const s of [-1, 1]) {
      const lx = s * (hw + 3.5), lz = e * (hl - 8);
      put(G, CYL, f, lx, 9, lz, 0.16, 18, 0.16, DARK);
      put(G, BOX, f, lx - s * 0.3, 18.2, lz, 0.4, 1.3, 2.6, DARK);
      for (const dz of [-0.8, 0, 0.8]) for (const dy of [-0.3, 0.3]) put(G, BOX, f, lx - s * 0.52, 18.2 + dy, lz + dz, 0.06, 0.4, 0.6, LAMP);
    }
  }
  // pitch-side rail, with gaps at the halfway line so the pitch can be walked on to
  for (const s of [-1, 1]) for (const e of [-1, 1]) {
    const x = s * (hw + 2), z0 = e * 3, z1 = e * (hl + 2);
    put(G, BOX, f, x, 1.0, (z0 + z1) / 2, 0.07, 0.07, Math.abs(z1 - z0), WHITE);
    for (let z = Math.min(z0, z1); z <= Math.max(z0, z1); z += 2.5) put(G, BOX, f, x, 0.5, z, 0.06, 1.0, 0.06, WHITE);
    const p0 = toWorld(f, x, 0, z0), p1 = toWorld(f, x, 0, z1);
    walls.add(p0[0], p0[2], p1[0], p1[2], 1.5, [p0[0], p0[2], p1[0], p1[2], 1]);
  }
  // a small covered stand on one touchline, in the club's blue
  const sx = hw + 6.5, sl = 30;
  for (let r = 0; r < 4; r++) { put(G, BOX, f, sx + r * 0.9, 0.3 + r * 0.45, 0, 0.9, 0.6 + r * 0.9, sl, GREY); put(G, BOX, f, sx + r * 0.9, 0.78 + r * 0.9, 0, 0.5, 0.36, sl - 0.6, SEAT); }
  put(G, BOX, f, sx + 3.4, 2.6, 0, 0.2, 5.2, sl, BLUE);
  for (const e of [-1, 1]) put(G, BOX, f, sx + 1.5, 2.6, e * (sl / 2), 3.9, 5.2, 0.2, BLUE);
  put(G, BOX, f, sx + 1.2, 5.3, 0, 5.2, 0.16, sl + 0.8, C('#d9dde2'));
  const c = [[sx - 0.5, -sl / 2], [sx + 3.6, -sl / 2], [sx + 3.6, sl / 2], [sx - 0.5, sl / 2]].map(([lx, lz]) => toWorld(f, lx, 0, lz));
  for (let i = 0; i < 4; i++) { const a = c[i], b = c[(i + 1) % 4]; walls.add(a[0], a[2], b[0], b[2], 1.5, [a[0], a[2], b[0], b[2], 0]); }
  const sign = new THREE.Mesh(new THREE.PlaneGeometry(9, 1.1), new THREE.MeshLambertMaterial({ map: signTexture('PADIHAM FC', '#1f4fa3', '#ffffff') }));
  const sp = toWorld(f, sx - 0.6, 4.6, 0); sign.position.set(sp[0], sp[1] + groundH(sp[0], sp[2]), sp[2]); sign.rotation.y = h - Math.PI / 2; scene.add(sign);
  scene.add(G.mesh(new THREE.MeshLambertMaterial({ vertexColors: true }), 'both', 'clip'));
  const spot = toWorld(f, 0, 0, 0);
  return { x: spot[0], z: spot[2] };
}

// ---------- Main build ----------
export function buildWorld(scene, w, lowSpec) {
  const out = { pedPaths: [], spots: [], signs: new Set() };
  const [SX0, SZ0, SX1, SZ1] = w.slice;
  const inSlice = (x, z, m = 0) => x > SX0 + m && x < SX1 - m && z > SZ0 + m && z < SZ1 - m;

  // The ground: the terrain grid itself, with one ring of long squares carrying it out level to the horizon.
  const T = terrain();
  let ground;
  if (T) {
    const xs = [-3000], zs = [-3000];
    for (let i = 0; i < T.nx; i++) xs.push(T.x0 + i * T.s);
    for (let j = 0; j < T.nz; j++) zs.push(T.z0 + j * T.s);
    xs.push(3000); zs.push(3000);
    const pos = [], uv = [], idx = [], nxv = xs.length;
    for (const z of zs) for (const x of xs) { pos.push(x, groundH(x, z, true), z); uv.push((x + 3000) / 6000, (3000 - z) / 6000); }
    for (let j = 0; j < zs.length - 1; j++) for (let i = 0; i < nxv - 1; i++) { const a = j * nxv + i, b = a + 1, d = a + nxv, c = d + 1; idx.push(a, c, b, a, d, c); }
    const gg = new THREE.BufferGeometry();
    gg.setAttribute('position', new THREE.Float32BufferAttribute(pos, 3));
    gg.setAttribute('uv', new THREE.Float32BufferAttribute(uv, 2));
    gg.setIndex(idx);
    gg.computeVertexNormals();
    ground = new THREE.Mesh(gg, new THREE.MeshLambertMaterial({ map: groundTexture() }));
  } else {
    ground = new THREE.Mesh(new THREE.PlaneGeometry(6000, 6000), new THREE.MeshLambertMaterial({ map: groundTexture() }));
    ground.rotation.x = -Math.PI / 2;
  }
  ground.receiveShadow = true;
  scene.add(ground);

  const F = new Batch(); // everything flat on the ground
  const WT = new Batch(); // water, which stays at river level under the bridges
  const GCOL = { park: '#6aa457', pitch: '#62ad55', grass: '#79a863', wood: '#55894c', scrub: '#6c9558' };
  // Town centre ground is paved yards and car parks, not grass.
  const disc = [];
  for (let i = 0; i < 40; i++) disc.push([CENTRE[0] + Math.cos((i / 40) * 6.2832) * CENTRE_R, CENTRE[1] + Math.sin((i / 40) * 6.2832) * CENTRE_R]);
  fillPoly(F, disc, 0.015, C('#a8a397'));
  for (const g of w.green) fillPoly(F, g.p, 0.03, C(GCOL[g.k] || '#79a863'));
  const WATER = C('#3f7391');
  for (const wt of w.water) {
    if (wt.poly) fillPoly(WT, wt.poly, 0.05, WATER);
    else {
      ribbon(WT, wt.p, wt.w, 0.05, WATER);
      if (wt.w >= 10) for (let i = 0; i < wt.p.length - 1; i++) riverGrid.add(wt.p[i][0], wt.p[i][1], wt.p[i + 1][0], wt.p[i + 1][1], wt.w, [wt.p[i][0], wt.p[i][1], wt.p[i + 1][0], wt.p[i + 1][1], wt.w / 2]);
    }
  }

  // Padiham FC's ground: the mapped pitch gets markings, goals, floodlights and a stand.
  const ground0 = w.pois.find((q) => q.n === 'Padiham Football Club');
  const fcPitch = ground0 && w.green.filter((g) => g.k === 'pitch').sort((a, b2) => Math.hypot(centre(a.p)[0] - ground0.x, centre(a.p)[1] - ground0.z) - Math.hypot(centre(b2.p)[0] - ground0.x, centre(b2.p)[1] - ground0.z))[0];
  if (fcPitch) out.ground = footballGround(F, fcPitch.p, scene);

  // Roads
  const drive = (r) => r.c !== 'foot' && r.c !== 'service';
  w.roads.forEach((r, ri) => {
    for (let i = 0; i < r.p.length - 1; i++) {
      const s = [r.p[i][0], r.p[i][1], r.p[i + 1][0], r.p[i + 1][1], r.w / 2, r.n || '', r.c, ri];
      roadGrid.add(s[0], s[1], s[2], s[3], r.w / 2 + 16, s);
    }
  });
  for (const r of w.roads) if (drive(r)) ribbon(F, r.p, r.w + 4.8, 0.08, C('#b9b4a8'));
  for (const r of w.roads) if (r.c === 'foot') ribbon(F, r.p, r.w, 0.1, C('#c2b596'));
  if (!lowSpec) for (const r of w.roads) if (drive(r)) ribbon(F, r.p, r.w + 0.6, 0.11, C('#7f7c75')); // the dark kerb edge: left out on a phone
  for (const r of w.roads) if (r.c !== 'foot') ribbon(F, r.p, r.w, 0.14, C(r.c === 'service' ? '#66686c' : r.c === 'primary' ? '#3d3f44' : '#494b50'));
  const WHITE = C('#e9e7df'), YELLOW = C('#e2b93b');
  // Kerb lines break where a side road joins, as they do on the street.
  // Another main road counts only when the line is well inside it (a fork), so the lines carry on where one stretch meets the next.
  const inSideRoad = (x, z, ri) => { for (const s of roadGrid.at(x, z)) { if (s[6] === 'foot' || s[7] === ri) continue; if (distToSeg(x, z, s)[0] < (s[6] === 'primary' ? s[4] - 0.9 : s[4] + 0.5)) return true; } return false; };
  const clearRuns = (p, ri) => {
    const runs = []; let run = [];
    const flush = () => { if (run.length > 1) runs.push(run); run = []; };
    for (let i = 0; i < p.length - 1; i++) {
      const dx = p[i + 1][0] - p[i][0], dz = p[i + 1][1] - p[i][1], n = Math.max(1, Math.ceil(Math.hypot(dx, dz) / 1.5));
      for (let k = i ? 1 : 0; k <= n; k++) { const q = [p[i][0] + (dx * k) / n, p[i][1] + (dz * k) / n]; if (inSideRoad(q[0], q[1], ri)) flush(); else run.push(q); }
    }
    flush();
    return runs;
  };
  w.roads.forEach((r, ri) => {
    if (r.c === 'primary' || r.c === 'tertiary') dashes(F, r.p, 0.17, 0.14, 3, 6, WHITE);
    if (r.c === 'primary') for (const side of [1, -1]) for (const off of [r.w / 2 - 0.3, r.w / 2 - 0.55]) for (const run of clearRuns(offsetLine(r.p, side * off), ri)) ribbon(F, run, 0.09, 0.17, YELLOW, false);
  });
  const roadAt = (x, z) => { let best = null, bd = 12; for (const s of roadGrid.at(x, z)) { if (s[6] === 'foot') continue; const d = distToSeg(x, z, s)[0]; if (d < bd) { bd = d; best = s; } } return best; };
  const P = new Batch(); // props
  const DARK = C('#2e3a36'), BLACK = C('#1b1d20'), ORANGE = C('#f08a24'), RED = C('#c0241f'), GLASS = C('#9fc1d1'), STONE = C('#9d8f76');
  for (const pr of w.props) {
    const s = roadAt(pr.x, pr.z);
    if (!s) continue;
    const l = Math.hypot(s[2] - s[0], s[3] - s[1]) || 1, dx = (s[2] - s[0]) / l, dz = (s[3] - s[1]) / l, hw = s[4];
    const [, qx, qz] = distToSeg(pr.x, pr.z, s);
    const o = { x: qx, z: qz, h: Math.atan2(dx, dz) };
    if (pr.k === 'zebra' || pr.k === 'signals') {
      if (pr.k === 'zebra') for (let j = -hw + 0.45; j < hw; j += 1.1) {
        const cx = qx - dz * j, cz = qz + dx * j;
        F.quad([cx - dz * 0.3 - dx * 1.4, 0.175, cz + dx * 0.3 - dz * 1.4], [cx - dz * 0.3 + dx * 1.4, 0.175, cz + dx * 0.3 + dz * 1.4], [cx + dz * 0.3 + dx * 1.4, 0.175, cz - dx * 0.3 + dz * 1.4], [cx + dz * 0.3 - dx * 1.4, 0.175, cz - dx * 0.3 - dz * 1.4], WHITE, null, [0, 1, 0]);
      } else for (const e of [-2.2, 2.2]) {
        const cx = qx + dx * e, cz = qz + dz * e;
        F.quad([cx - dz * hw - dx * 0.12, 0.175, cz + dx * hw - dz * 0.12], [cx - dz * hw + dx * 0.12, 0.175, cz + dx * hw + dz * 0.12], [cx + dz * hw + dx * 0.12, 0.175, cz - dx * hw + dz * 0.12], [cx + dz * hw - dx * 0.12, 0.175, cz - dx * hw - dz * 0.12], WHITE, null, [0, 1, 0]);
      }
      for (const side of [1, -1]) {
        const lx = side * (hw + 0.9), lz = side * 1.8;
        if (pr.k === 'zebra') {
          for (let k2 = 0; k2 < 5; k2++) put(P, CYL, o, lx, 0.3 + k2 * 0.6, lz, 0.07, 0.6, 0.07, k2 % 2 ? WHITE : BLACK);
          put(P, SPH, o, lx, 3.25, lz, 0.24, 0.24, 0.24, ORANGE);
        } else {
          put(P, CYL, o, lx, 1.6, lz, 0.07, 3.2, 0.07, DARK);
          put(P, BOX, o, lx, 3.0, lz, 0.32, 0.95, 0.3, BLACK);
          put(P, SPH, o, lx, 3.3, lz - side * 0.16, 0.1, 0.1, 0.06, RED);
          put(P, SPH, o, lx, 3.0, lz - side * 0.16, 0.1, 0.1, 0.06, C('#e2a21c'));
          put(P, SPH, o, lx, 2.7, lz - side * 0.16, 0.1, 0.1, 0.06, C('#2f9e4f'));
          // pedestrian guard rails either side of the crossing, as on Burnley Road
          const RAIL = C('#7d838a');
          for (const dir of [1, -1]) {
            for (const y of [1.02, 0.2]) put(P, BOX, o, side * (hw + 0.5), y, dir * 7, 0.05, 0.05, 7, RAIL);
            for (let e = 3.5; e <= 10.6; e += 0.35) put(P, BOX, o, side * (hw + 0.5), 0.6, dir * e, 0.03, e % 1.75 < 0.3 ? 1.1 : 0.82, 0.03, RAIL);
          }
        }
      }
    } else if (pr.k === 'mini') {
      for (const [rad, y, colr] of [[2.7, 0.172, C('#a2463c')], [1.6, 0.2, WHITE]]) for (let k2 = 0; k2 < 16; k2++) { const a0 = (k2 / 16) * 6.283, a1 = ((k2 + 1) / 16) * 6.283; F.tri([pr.x, y + 0.03, pr.z], [pr.x + Math.cos(a1) * rad, y, pr.z + Math.sin(a1) * rad], [pr.x + Math.cos(a0) * rad, y, pr.z + Math.sin(a0) * rad], colr, null, [0, 1, 0]); }
    } else {
      // which side of the road is the point on
      const side = (pr.x - qx) * -dz + (pr.z - qz) * dx >= 0 ? 1 : -1;
      const lx = side * (hw + 1.5);
      if (pr.k === 'post') { put(P, CYL, o, lx, 0.65, 0, 0.26, 1.3, 0.26, RED); put(P, CYL, o, lx, 1.36, 0, 0.3, 0.14, 0.3, RED); put(P, CYL, o, lx, 0.06, 0, 0.29, 0.12, 0.29, BLACK); }
      if (pr.k === 'bus') {
        put(P, CYL, o, lx - side * 0.5, 1.4, 2.6, 0.05, 2.8, 0.05, DARK);
        put(P, BOX, o, lx - side * 0.5, 2.6, 2.6, 0.06, 0.45, 0.5, C('#c8201c'));
        for (const z2 of [-1.5, 1.5]) put(P, BOX, o, lx + side * 0.9, 1.2, z2, 0.07, 2.4, 0.07, DARK);
        put(P, BOX, o, lx + side * 0.9, 1.25, 0, 0.05, 1.9, 3.0, GLASS);
        put(P, BOX, o, lx + side * 0.35, 2.45, 0, 1.4, 0.08, 3.3, DARK);
        put(P, BOX, o, lx + side * 0.6, 0.48, 0, 0.35, 0.06, 2.2, C('#7a5a3a'));
      }
    }
  }
  // Landmarks that have no building outline: placed in overrides.json, each with its source.
  for (const lm of w.landmarks || []) {
    const o = { x: lm.x, z: lm.z, h: lm.h || 0 };
    const solid = (r, n) => { for (let i = 0; i < n; i++) { const a0 = o.h + ((i + 0.5) / n) * 6.2832, a1 = o.h + ((i + 1.5) / n) * 6.2832, q = [lm.x + Math.sin(a0) * r, lm.z + Math.cos(a0) * r, lm.x + Math.sin(a1) * r, lm.z + Math.cos(a1) * r]; walls.add(q[0], q[1], q[2], q[3], 1.5, [...q, 1]); } };
    if (lm.k === 'wicket') {
      // A mown cricket square with a paler strip and stumps at each end. The direction is not surveyed.
      put(P, BOX, o, 0, 0.16, 0, 22, 0.04, 24, C('#7fbf63'));
      put(P, BOX, o, 0, 0.19, 0, 3.05, 0.04, 20.12, C('#cdbf8a'));
      for (const lz of [-10.06, 10.06]) { put(P, BOX, o, 0, 0.21, lz - Math.sign(lz) * 1.22, 3.6, 0.02, 0.06, C('#ffffff')); for (const lx of [-0.11, 0, 0.11]) put(P, BOX, o, lx, 0.55, lz, 0.04, 0.71, 0.04, C('#e8d9a8')); }
      continue;
    }
    if (lm.k === 'warmemorial') {
      // A Celtic wheel cross on a decorated shaft, a pedestal with a column at each corner and a gold lettered
      // tablet on each face, on a short plinth and base with a stone surround (War Memorials Online, record 153903).
      const S = C('#bdb7a8'), S2 = C('#a39c8c'), TAB = C('#8f887a'), GOLD = C('#c9a646');
      put(P, BOX, o, 0, 0.12, 0, 5.4, 0.24, 5.4, S2);
      put(P, BOX, o, 0, 0.4, 0, 3.1, 0.32, 3.1, S);
      put(P, BOX, o, 0, 0.7, 0, 2.4, 0.3, 2.4, S);
      put(P, BOX, o, 0, 1.65, 0, 1.3, 1.6, 1.3, S);
      for (const lx of [-0.74, 0.74]) for (const lz of [-0.74, 0.74]) put(P, CYL, o, lx, 1.65, lz, 0.11, 1.6, 0.11, S2);
      put(P, BOX, o, 0, 2.55, 0, 1.85, 0.2, 1.85, S);
      for (const [ax, az] of [[0, 1], [0, -1], [1, 0], [-1, 0]]) {
        put(P, BOX, o, ax * 0.66, 1.65, az * 0.66, az ? 0.9 : 0.04, 1.2, az ? 0.04 : 0.9, TAB);
        for (let i = 0; i < 6; i++) put(P, BOX, o, ax * 0.69, 2.1 - i * 0.18, az * 0.69, az ? 0.66 : 0.02, 0.05, az ? 0.02 : 0.66, GOLD);
      }
      put(P, BOX, o, 0, 2.85, 0, 0.7, 0.4, 0.55, S);
      put(P, BOX, o, 0, 4.25, 0, 0.42, 2.5, 0.3, S);
      put(P, BOX, o, 0, 4.85, 0, 1.5, 0.38, 0.28, S);
      for (let i = 0; i < 12; i++) { const an = (i / 12) * 6.2832; put(P, BOX, o, Math.cos(an) * 0.58, 4.85 + Math.sin(an) * 0.58, 0, 0.34, 0.13, 0.2, S2, an + Math.PI / 2); }
      solid(2.2, 4);
    } else if (lm.k === 'bandstand') {
      // The mapped bandstand in Memorial Park. No source describes it, so this is a plain open bandstand.
      const BASE = C('#b3aa98'), IRON = C('#2f4a3c'), ROOF = C('#5a4a42');
      put(P, CYL, o, 0, 0.3, 0, 2.5, 0.6, 2.5, BASE);
      put(P, CYL, o, 0, 0.65, 0, 2.3, 0.1, 2.3, C('#8c7a5c'));
      for (let i = 0; i < 8; i++) { const an = ((i + 0.5) / 8) * 6.2832; put(P, CYL, o, Math.sin(an) * 2.05, 2.0, Math.cos(an) * 2.05, 0.06, 2.7, 0.06, IRON); }
      put(P, CYL, o, 0, 3.4, 0, 2.75, 0.14, 2.75, IRON);
      put(P, CONE, o, 0, 4.15, 0, 2.7, 1.4, 2.7, ROOF);
      put(P, CYL, o, 0, 5.1, 0, 0.05, 0.6, 0.05, IRON);
      solid(2.5, 8);
    }
  }
  scene.add(WT.mesh(new THREE.MeshLambertMaterial({ vertexColors: true }), 'receive', 'water'));
  scene.add(F.mesh(new THREE.MeshLambertMaterial({ vertexColors: true }), 'receive', 'clip'));

  // Buildings
  const W = new Batch(), R = new Batch(), bays = [], ex = { doors: [], bands: [], fronts: new Map() };
  w.buildings.forEach((b, bi) => building(b, bi, W, R, P, bays, ex));
  out.doors = ex.doors;
  out.fronts = ex.fronts;
  for (const bd of ex.bands) {
    const m = new THREE.Mesh(new THREE.PlaneGeometry(bd.w, bd.h || 0.62), new THREE.MeshLambertMaterial({ map: bd.plain ? signTexture(bd.text, bd.bg, bd.fg) : signTexture(bd.text.split('').join(' '), '#1f3350', '#e6c877') }));
    m.position.set(bd.x, bd.y + groundH(bd.x, bd.z), bd.z); m.rotation.y = Math.atan2(bd.nx, bd.nz); scene.add(m);
    for (const poi of w.pois) if (poi.n.toUpperCase() === bd.text || (bd.plain && poi.n === bd.text)) out.signs.add(poi.n);
  }
  scene.add(W.mesh(new THREE.MeshLambertMaterial({ vertexColors: true, map: facadeAtlas() })));
  scene.add(R.mesh(new THREE.MeshLambertMaterial({ vertexColors: true, map: slateTexture(), side: THREE.DoubleSide })));

  // Signs on the nearest street front
  const SHORT = { 'Padiham Mobile Phone & Computers iFix4U': 'iFix4U', 'Padiham Green Church of England Primary School': 'Padiham Green CE Primary', "St Leonard's C of E Primary School": "St Leonard's CE Primary" };
  const SIGN_BG = ['#1f3d2b', '#6b1d1d', '#1d2f4f', '#26282c'];
  w.pois.forEach((poi, i) => {
    if (poi.k === 'park' || poi.k === 'pitch' || poi.k === 'bridge' || out.signs.has(poi.n)) return;
    // The big stores are deep sheds, so their nearest front is further from the mapped centre, and the sign is larger.
    const big = BIG.has(poi.n);
    let best = null, bd = big ? 80 : 26;
    for (const bay of bays) { if (bay.used) continue; const d = Math.hypot(bay.x - poi.x, bay.z - poi.z) + Math.hypot(bay.x - poi.dx, bay.z - poi.dz) * 0.5; if (d < bd) { bd = d; best = bay; } }
    if (!best) return;
    best.used = true;
    const sw = big ? Math.min(best.w * 1.25, 9) : Math.min(best.w * 0.94, 4.4);
    const m = new THREE.Mesh(new THREE.PlaneGeometry(sw, big ? sw / 7.5 : 0.56), new THREE.MeshLambertMaterial({ map: signTexture(SHORT[poi.n] || poi.n, SIGN_BG[i % SIGN_BG.length]) }));
    m.position.set(best.x + best.nx * 0.09, best.y + groundH(best.x, best.z), best.z + best.nz * 0.09);
    m.rotation.y = Math.atan2(best.nx, best.nz);
    scene.add(m);
    out.signs.add(poi.n);
  });

  // Bridge parapets
  for (const r of w.roads) if (r.b) for (let i = 0; i < r.p.length - 1; i++) {
    const a = r.p[i], c = r.p[i + 1], l = Math.hypot(c[0] - a[0], c[1] - a[1]);
    if (l < 1) continue;
    const dx = (c[0] - a[0]) / l, dz = (c[1] - a[1]) / l, off = r.c === 'foot' ? r.w / 2 + 0.3 : r.w / 2 + 2.6;
    const o = { x: (a[0] + c[0]) / 2, z: (a[1] + c[1]) / 2, h: Math.atan2(dx, dz) };
    // The deck slab under the road, so the bridge has sides and the river runs beneath it.
    put(P, BOX, o, 0, -0.75, 0, r.c === 'foot' ? r.w + 1.6 : r.w + 6.6, 1.3, l, STONE);
    for (const side of [1, -1]) {
      const ex = dz * side * off, ez = -dx * side * off; // the same side as the box drawn below
      // A parapet belongs on the outside of the bridge, not between the road and a footway that share the deck.
      if (w.roads.some((r2) => r2.b && r2 !== r && r2.p.some((q, j) => j < r2.p.length - 1 && distToSeg(o.x + ex, o.z + ez, [q[0], q[1], r2.p[j + 1][0], r2.p[j + 1][1]])[0] < (r2.c === 'foot' ? r2.w / 2 + 0.2 : r2.w / 2 + 2.5)))) continue;
      put(P, BOX, o, side * off, 0.6, 0, 0.4, 1.2, l, STONE);
      walls.add(a[0] + ex, a[1] + ez, c[0] + ex, c[1] + ez, 1.5, [a[0] + ex, a[1] + ez, c[0] + ex, c[1] + ez, 1]);
    }
  }

  // Lamp posts, benches and bins
  const mainRoad = (r) => ['primary', 'tertiary', 'residential', 'unclassified'].includes(r.c);
  w.roads.forEach((r, ri) => {
    if (!mainRoad(r)) return;
    walk(r.p, 8 + hash(ri) * 16, () => 32, (x, z, dx, dz, s, n) => {
      const side = n % 2 ? 1 : -1, off = r.w / 2 + 1.5;
      const px = x - dz * side * off, pz = z + dx * side * off;
      if (nearWall(px, pz, 0.7)) return;
      const o = { x: px, z: pz, h: Math.atan2(dz * side, -dx * side) }; // local +z points to the road
      put(P, POLE, o, 0, 3.6, 0, 0.085, 7.2, 0.085, DARK);
      put(P, POLE, o, 0, 0.35, 0, 0.14, 0.7, 0.14, DARK);
      put(P, BOX, o, 0, 7.15, 0.7, 0.08, 0.08, 1.5, DARK);
      put(P, BOX, o, 0, 7.08, 1.45, 0.3, 0.12, 0.6, C('#d9d6c8'));
    });
    if (r.c === 'primary') walk(r.p, 20 + hash(ri + 5) * 20, () => 46, (x, z, dx, dz, s, n) => {
      const side = n % 2 ? -1 : 1, off = r.w / 2 + 2.0;
      const px = x - dz * side * off, pz = z + dx * side * off;
      if (nearWall(px, pz, 0.9) || !inSlice(px, pz, -40)) return;
      const o = { x: px, z: pz, h: Math.atan2(dz * side, -dx * side) };
      const wood = C('#7a5a3a');
      put(P, BOX, o, 0, 0.46, 0, 1.7, 0.07, 0.46, wood);
      put(P, BOX, o, 0, 0.8, -0.22, 1.7, 0.36, 0.06, wood);
      for (const e of [-0.75, 0.75]) put(P, BOX, o, e, 0.23, 0, 0.07, 0.46, 0.42, BLACK);
      put(P, CYL, o, 1.6, 0.5, 0, 0.27, 1.0, 0.27, BLACK);
      put(P, CYL, o, 1.6, 0.86, 0, 0.275, 0.07, 0.275, C('#d9b44a'));
    });
  });

  // Union Jack bunting across the main street inside the playable area
  const U = new Batch(), WH = new THREE.Color(1, 1, 1);
  w.roads.forEach((r, ri) => {
    if (r.c !== 'primary') return;
    walk(r.p, 10 + hash(ri + 9) * 8, () => 17, (x, z, dx, dz) => {
      if (!inSlice(x, z, 6) || Math.hypot(x - CENTRE[0], z - CENTRE[1]) > 230) return;
      const half = r.w / 2 + 1.5, nF = 13;
      const pos = (t, drop = 0) => { const e = (t * 2 - 1) * half; return [x - dz * e, 6.6 - Math.sin(Math.PI * t) * 1.1 - drop, z + dx * e]; };
      for (let i = 0; i < nF; i++) {
        const t0 = (i + 0.12) / nF, t1 = (i + 0.88) / nF;
        U.quad(pos(t0, 0.42), pos(t1, 0.42), pos(t1), pos(t0), WH, [0, 0, 1, 1]);
      }
      for (let i = 0; i < 12; i++) { const a = pos(i / 12), b2 = pos((i + 1) / 12); P.quad([a[0], a[1] - 0.02, a[2]], [b2[0], b2[1] - 0.02, b2[2]], [b2[0], b2[1] + 0.02, b2[2]], [a[0], a[1] + 0.02, a[2]], C('#e9e7df'), null); }
    });
  });
  scene.add(U.mesh(new THREE.MeshLambertMaterial({ map: unionTexture(), side: THREE.DoubleSide }), 'none'));

  // Parked cars on side streets
  const CARS = ['#b3252a', '#2d4f8a', '#d9d9d6', '#3a3d42', '#8a8f96', '#1f5d4a', '#e8e4d8', '#6a2a2a'];
  let carN = 0;
  w.roads.forEach((r, ri) => {
    if (r.c !== 'residential' && r.c !== 'unclassified') return;
    const total = lineLength(r.p), side = hash(ri + 3) < 0.5 ? 1 : -1;
    if (total < 30) return;
    walk(r.p, 10 + hash(ri) * 10, (n) => 6.2 + hash(ri * 13 + n) * 24, (x, z, dx, dz, s, n) => {
      if (s > total - 10) return;
      if (lowSpec && n % 2) return; // half the parked cars on a phone
      const off = r.w / 2 - 1.05, px = x - dz * side * off, pz = z + dx * side * off;
      for (const q of roadGrid.at(px, pz)) if (q[7] !== ri && q[6] !== 'foot' && distToSeg(px, pz, q)[0] < q[4] + 4) return;
      if (nearWall(px, pz, 2.4)) return;
      const o = { x: px, z: pz, h: Math.atan2(dx * side, dz * side) + (hash(n + ri) - 0.5) * 0.05 };
      const col = C(CARS[Math.floor(hash(ri * 7 + n * 3) * CARS.length)]);
      const van = hash(ri + n * 11) < 0.12;
      put(P, BOX, o, 0, 0.62, 0, 1.72, 0.62, 4.1, col);
      put(P, BOX, o, 0, van ? 1.35 : 1.16, van ? -0.35 : -0.15, 1.56, van ? 0.95 : 0.5, van ? 3.1 : 2.1, van ? col : C('#1c2530'));
      if (!van) put(P, BOX, o, 0, 1.44, -0.15, 1.5, 0.07, 1.9, col);
      for (const wz of [-1.3, 1.3]) put(P, AXLE, o, 0, 0.32, wz, 0.32, 1.84, 0.32, BLACK, Math.PI / 2);
      put(P, BOX, o, 0, 0.7, 2.06, 1.5, 0.14, 0.04, C('#f4f0d8'));
      put(P, BOX, o, 0, 0.7, -2.06, 1.5, 0.14, 0.04, C('#a01c1c'));
      const s2 = Math.sin(o.h), c2 = Math.cos(o.h);
      const cn = [[-0.95, -2.15], [0.95, -2.15], [0.95, 2.15], [-0.95, 2.15]].map(([lx, lz]) => [px + lx * c2 + lz * s2, pz - lx * s2 + lz * c2]);
      for (let e = 0; e < 4; e++) { const a = cn[e], c = cn[(e + 1) % 4]; walls.add(a[0], a[1], c[0], c[1], 1.5, [a[0], a[1], c[0], c[1], 1]); }
      carN++;
    });
  });

  // Slice barriers where roads leave the playable area
  const closed = new THREE.MeshLambertMaterial({ map: closedTexture() });
  for (const r of w.roads) {
    if (r.c === 'foot') continue;
    for (let i = 0; i < r.p.length - 1; i++) {
      const a = r.p[i], c = r.p[i + 1];
      if (inSlice(a[0], a[1]) === inSlice(c[0], c[1])) continue;
      let lo = 0, hi = 1; // bisect to the crossing point
      const ina = inSlice(a[0], a[1]);
      for (let k2 = 0; k2 < 20; k2++) { const t = (lo + hi) / 2; if (inSlice(a[0] + (c[0] - a[0]) * t, a[1] + (c[1] - a[1]) * t) === ina) lo = t; else hi = t; }
      const x = a[0] + (c[0] - a[0]) * lo, z = a[1] + (c[1] - a[1]) * lo;
      const l = Math.hypot(c[0] - a[0], c[1] - a[1]) || 1, o = { x, z, h: Math.atan2((c[0] - a[0]) / l, (c[1] - a[1]) / l) };
      const half = r.w / 2 + (r.c === 'service' ? 0.5 : 2.4);
      for (let e = -half; e < half; e += 0.6) for (const y of [0.55, 1.0]) put(P, BOX, o, e + 0.3, y, 0, 0.6, 0.2, 0.07, Math.round((e + half) / 0.6) % 2 ? RED : WHITE);
      for (let e = -half; e <= half + 0.01; e += Math.max(1.2, (half * 2) / Math.round(half * 2 / 2.2))) put(P, BOX, o, e, 0.55, 0, 0.07, 1.1, 0.5, C('#8a8f96'));
      for (const e of [-half * 0.6, 0, half * 0.6]) { put(P, CONE, o, e, 0.3, ina ? -1 : 1, 0.2, 0.6, 0.2, ORANGE); }
      if (r.c !== 'service') for (const flip of [0, Math.PI]) {
        const m = new THREE.Mesh(new THREE.PlaneGeometry(2.4, 1.2), closed);
        m.position.set(x, 1.95 + groundH(x, z), z); m.rotation.y = o.h + flip; m.translateZ(0.06); scene.add(m);
      }
    }
  }
  scene.add(P.mesh(new THREE.MeshLambertMaterial({ vertexColors: true })));

  // Trees
  // One set of instanced meshes per square of the town, so trees out of view are skipped. On a phone each tree is
  // a plainer shape with a single crown.
  const trunkGeo = new THREE.CylinderGeometry(0.22, 0.34, 3, 5, 1, true), crownGeo = new THREE.IcosahedronGeometry(2.4, lowSpec ? 0 : 1);
  const trunkMat = new THREE.MeshLambertMaterial({ color: 0x6b4f35 }), crownMat = new THREE.MeshLambertMaterial({ flatShading: true });
  const col = new THREE.Color(), per = lowSpec ? 1 : 2, groves = new Map();
  w.trees.forEach((t, i) => { const k = Math.floor(t[0] / CHUNK) + ',' + Math.floor(t[1] / CHUNK); if (!groves.has(k)) groves.set(k, []); groves.get(k).push(i); });
  for (const [k, ids] of groves) {
    const trunks = new THREE.InstancedMesh(trunkGeo, trunkMat, ids.length), crowns = new THREE.InstancedMesh(crownGeo, crownMat, ids.length * per);
    ids.forEach((i, e) => {
      const [x, z] = w.trees[i], s = 0.7 + hash(i + 3) * 0.8, gy = groundH(x, z);
      _q.identity();
      trunks.setMatrixAt(e, _m.compose(_p.set(x, gy + 1.5 * s, z), _q, _s.set(s, s, s)));
      _q.setFromAxisAngle(UP, hash(i) * 6);
      crowns.setMatrixAt(e * per, _m.compose(_p.set(x, gy + 4.3 * s, z), _q, _s.set(s, s * 1.05, s)));
      col.setHSL(0.27 + hash(i + 1) * 0.07, 0.42, 0.26 + hash(i + 2) * 0.1);
      crowns.setColorAt(e * per, col);
      if (per === 2) {
        crowns.setMatrixAt(e * 2 + 1, _m.compose(_p.set(x + (hash(i + 9) - 0.5) * s, gy + 6.0 * s, z + (hash(i + 4) - 0.5) * s), _q, _s.set(s * 0.68, s * 0.7, s * 0.68)));
        crowns.setColorAt(e * 2 + 1, col.offsetHSL(0, 0, 0.04));
      }
    });
    trunks.castShadow = crowns.castShadow = true;
    trunks.computeBoundingSphere(); crowns.computeBoundingSphere();
    const [cx, cz] = k.split(',').map(Number);
    chunks.push({ mesh: trunks, x: (cx + 0.5) * CHUNK, z: (cz + 0.5) * CHUNK }, { mesh: crowns, x: (cx + 0.5) * CHUNK, z: (cz + 0.5) * CHUNK });
    scene.add(trunks, crowns);
  }

  // Pavement paths for pedestrians, and spots for pigeons
  w.roads.forEach((r) => {
    if (!mainRoad(r)) return;
    for (const side of [1, -1]) {
      const line = offsetLine(r.p, side * (r.w / 2 + 1.5));
      let run = [];
      const flush = () => { if (run.length > 1 && lineLength(run) > 35) { out.pedPaths.push(run); if (r.c === 'primary') out.spots.push(...spotsOn(offsetLine(run, side * 0.9))); } run = []; };
      for (const q of line) { if (inSlice(q[0], q[1], 8) && !nearWall(q[0], q[1], 0.5)) run.push(q); else flush(); }
      flush();
    }
  });
  function spotsOn(line) { const res = []; walk(line, 12, () => 40, (x, z) => { if (!nearWall(x, z, 1.2)) res.push([x, z]); }); return res; }
  out.cars = carN;
  return out;
}
