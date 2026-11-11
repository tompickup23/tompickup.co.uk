// Pub interiors. Each one is a room built far outside the map; walking into the pub door moves the player there.
// Layouts and finishes follow photos supplied by the site owner. Staff are generic figures and no drink brands appear.
import * as THREE from 'three';
import { walls } from './city.js';
import { makeFigure } from './life.js';
import { rng } from './textures.js';

const W = 13, D = 10, H = 2.9;

function tex(draw, size, rx, rz) {
  const cv = document.createElement('canvas'); cv.width = cv.height = size;
  draw(cv.getContext('2d'), size, rng(size + rx * 7));
  const t = new THREE.CanvasTexture(cv);
  t.wrapS = t.wrapT = THREE.RepeatWrapping; t.repeat.set(rx, rz); t.colorSpace = THREE.SRGBColorSpace; t.anisotropy = 4;
  return t;
}
const PATTERN = {
  tartan(g, s) {
    g.fillStyle = '#2b2634'; g.fillRect(0, 0, s, s);
    for (const [o, w, c] of [[0, 22, 'rgba(210,200,185,.55)'], [40, 8, 'rgba(120,90,140,.6)'], [64, 22, 'rgba(90,80,100,.6)'], [100, 4, 'rgba(230,220,200,.7)']]) { g.fillStyle = c; g.fillRect(o, 0, w, s); g.fillRect(0, o, s, w); }
  },
  damask(g, s, R) {
    g.fillStyle = '#23232b'; g.fillRect(0, 0, s, s);
    g.strokeStyle = 'rgba(190,185,200,.5)'; g.lineWidth = 3;
    for (let i = 0; i < 4; i++) for (let j = 0; j < 4; j++) { const x = i * 32 + 16, y = j * 32 + 16; g.beginPath(); g.arc(x, y, 9, 0.4, 5.2); g.stroke(); g.beginPath(); g.arc(x + 8, y - 6, 4, 0, 6.3); g.stroke(); }
  },
  floral(g, s, R) {
    g.fillStyle = '#3b2f52'; g.fillRect(0, 0, s, s);
    for (let i = 0; i < 46; i++) { g.fillStyle = ['#8d6aa8', '#c9b6d6', '#5a4a78', '#a8537a', '#6d7a8a'][i % 5]; g.beginPath(); g.ellipse(R() * s, R() * s, 6 + R() * 9, 3 + R() * 4, R() * 3, 0, 6.3); g.fill(); }
  },
  morris(g, s, R) {
    g.fillStyle = '#d9cbb4'; g.fillRect(0, 0, s, s);
    for (let i = 0; i < 40; i++) { g.fillStyle = ['#7a3f4a', '#8a7a5a', '#b9a58a', '#5f4a52'][i % 4]; g.beginPath(); g.ellipse(R() * s, R() * s, 7 + R() * 9, 3 + R() * 3, R() * 3, 0, 6.3); g.fill(); }
  },
  wood(g, s, R) {
    g.fillStyle = '#6b4526'; g.fillRect(0, 0, s, s);
    for (let x = 0; x < s; x += 16) { g.fillStyle = `rgba(30,15,5,${0.12 + R() * 0.2})`; g.fillRect(x, 0, 2, s); g.fillStyle = `rgba(255,220,170,${R() * 0.08})`; g.fillRect(x + 4, 0, 8, s); }
  },
  cream(g, s) { g.fillStyle = '#e6e0d2'; g.fillRect(0, 0, s, s); for (let x = 0; x < s; x += 16) { g.fillStyle = 'rgba(0,0,0,.10)'; g.fillRect(x, 0, 2, s); } },
  plum(g, s) { g.fillStyle = '#4a2c3a'; g.fillRect(0, 0, s, s); for (let i = 0; i < 8; i++) for (let j = 0; j < 8; j++) { g.fillStyle = 'rgba(255,255,255,.05)'; g.fillRect(i * 16 + 4, j * 16 + 4, 8, 8); } },
  parquet(g, s) {
    g.fillStyle = '#6a4024'; g.fillRect(0, 0, s, s);
    for (let i = 0; i < 8; i++) for (let j = 0; j < 8; j++) { g.fillStyle = (i + j) % 2 ? '#7a4c2c' : '#5a3620'; g.save(); g.translate(i * 16 + 8, j * 16 + 8); g.rotate((i + j) % 2 ? 0.785 : -0.785); g.fillRect(-10, -3.5, 20, 7); g.restore(); }
  },
  flags(g, s, R) { g.fillStyle = '#8f8a80'; g.fillRect(0, 0, s, s); for (let i = 0; i < 4; i++) for (let j = 0; j < 4; j++) { g.fillStyle = `rgb(${150 + R() * 40 | 0},${145 + R() * 40 | 0},${132 + R() * 40 | 0})`; g.fillRect(i * 32 + 1, j * 32 + 1, 30, 30); } },
  brick(g, s, R) { g.fillStyle = '#b9a898'; g.fillRect(0, 0, s, s); for (let y = 0, r = 0; y < s; y += 8, r++) for (let x = r % 2 ? -8 : 0; x < s; x += 16) { g.fillStyle = `rgb(${150 + R() * 30 | 0},${96 + R() * 20 | 0},${76 + R() * 16 | 0})`; g.fillRect(x + 1, y + 1, 14, 6); } },
};
const lam = (o) => new THREE.MeshLambertMaterial(o);
const flat = (c) => lam({ color: c });

export function buildPub(scene, cfg) {
  const g = new THREE.Group(); g.position.set(cfg.x, 0, cfg.z); scene.add(g);
  const box = (w, h, d, x, y, z, mat) => { const m = new THREE.Mesh(new THREE.BoxGeometry(w, h, d), typeof mat === 'number' ? flat(mat) : mat); m.position.set(x, y, z); g.add(m); return m; };
  const cyl = (r, h, x, y, z, mat, seg = 14) => { const m = new THREE.Mesh(new THREE.CylinderGeometry(r, r, h, seg), typeof mat === 'number' ? flat(mat) : mat); m.position.set(x, y, z); g.add(m); return m; };
  const solid = (x0, z0, x1, z1, low = 1) => {
    const c = [[x0, z0], [x1, z0], [x1, z1], [x0, z1]].map(([x, z]) => [x + cfg.x, z + cfg.z]);
    for (let i = 0; i < 4; i++) { const a = c[i], b = c[(i + 1) % 4]; walls.add(a[0], a[1], b[0], b[1], 1.5, [a[0], a[1], b[0], b[1], low]); }
  };
  const carpet = lam({ map: tex(PATTERN[cfg.carpet], 128, W / 1.3, D / 1.3) });
  const paper = (len) => lam({ map: tex(PATTERN[cfg.paper], 128, len / 1.6, 1.2) });
  const panel = (len) => lam({ map: tex(PATTERN[cfg.panel], 128, len / 1.2, 1) });
  const woodM = lam({ map: tex(PATTERN.wood, 128, 1, 1) }), dark = 0x3a2416, trim = cfg.panel === 'cream' ? 0x6b2a3a : 0x4a2c1a;

  // shell
  const floor = new THREE.Mesh(new THREE.PlaneGeometry(W, D), carpet); floor.rotation.x = -Math.PI / 2; floor.position.y = 0.3; g.add(floor);
  box(W + 1, 0.3, D + 1, 0, 0.149, 0, 0x222222);
  const ceil = box(W + 0.6, 0.2, D + 0.6, 0, H + 0.4, 0, 0xf1eee6); ceil.castShadow = true;
  const Y = 0.3; // interior floor level, clear of the outdoor ground layers
  const wall = (len, x, z, ry) => {
    const up = new THREE.Mesh(new THREE.PlaneGeometry(len, H - 1.0), paper(len)); up.position.set(x, Y + 1.0 + (H - 1.0) / 2, z); up.rotation.y = ry; g.add(up);
    const lo = new THREE.Mesh(new THREE.PlaneGeometry(len, 1.0), panel(len)); lo.position.set(x, Y + 0.5, z); lo.rotation.y = ry; g.add(lo);
    const rail = box(ry % Math.PI ? 0.06 : len, 0.07, ry % Math.PI ? len : 0.06, x, Y + 1.02, z, trim);
    const shell = box(ry % Math.PI ? 0.3 : len + 0.6, H + 0.6, ry % Math.PI ? len + 0.6 : 0.3, x + (ry === Math.PI / 2 ? -0.17 : ry === -Math.PI / 2 ? 0.17 : 0), Y + H / 2, z + (ry === 0 ? -0.17 : ry === Math.PI ? 0.17 : 0), 0x222222); shell.castShadow = true;
    return rail;
  };
  wall(W, 0, -D / 2, 0); wall(W, 0, D / 2, Math.PI); wall(D, -W / 2, 0, Math.PI / 2); wall(D, W / 2, 0, -Math.PI / 2);
  solid(-W / 2 - 1, -D / 2 - 1, W / 2 + 1, -D / 2, 0); solid(-W / 2 - 1, D / 2, W / 2 + 1, D / 2 + 1, 0); solid(-W / 2 - 1, -D / 2, -W / 2, D / 2, 0); solid(W / 2, -D / 2, W / 2 + 1, D / 2, 0);
  // partition wall with collisions, papered both sides
  const partition = (x0, z0, x1, z1) => {
    const len = Math.hypot(x1 - x0, z1 - z0), alongX = Math.abs(x1 - x0) > Math.abs(z1 - z0);
    box(alongX ? len : 0.16, H, alongX ? 0.16 : len, (x0 + x1) / 2, Y + H / 2, (z0 + z1) / 2, lam({ map: tex(PATTERN[cfg.paper], 128, len / 1.6, 1.8) }));
    box(alongX ? len : 0.2, 1.0, alongX ? 0.2 : len, (x0 + x1) / 2, Y + 0.5, (z0 + z1) / 2, panel(len));
    solid(Math.min(x0, x1) - 0.1, Math.min(z0, z1) - 0.1, Math.max(x0, x1) + 0.1, Math.max(z0, z1) + 0.1, 0);
  };
  const zone = (w, d, x, z, pattern) => { const m = new THREE.Mesh(new THREE.PlaneGeometry(w, d), lam({ map: tex(PATTERN[pattern], 128, w / 1.4, d / 1.4) })); m.rotation.x = -Math.PI / 2; m.position.set(x, Y + 0.01, z); g.add(m); };

  // furniture kit
  const chair = (x, z, ry) => {
    const c = new THREE.Group(); c.position.set(x, Y, z); c.rotation.y = ry; g.add(c);
    const part = (w, h, d, px, py, pz) => { const m = new THREE.Mesh(new THREE.BoxGeometry(w, h, d), woodM); m.position.set(px, py, pz); c.add(m); };
    part(0.42, 0.05, 0.42, 0, 0.45, 0); part(0.42, 0.5, 0.05, 0, 0.75, -0.2);
    for (const a of [-0.18, 0.18]) for (const b of [-0.18, 0.18]) part(0.04, 0.45, 0.04, a, 0.225, b);
  };
  const table = (x, z, w, d) => {
    box(w, 0.06, d, x, Y + 0.75, z, woodM); box(0.12, 0.72, 0.12, x, Y + 0.36, z, dark);
    chair(x - w / 2 - 0.35, z, Math.PI / 2); chair(x + w / 2 + 0.35, z, -Math.PI / 2);
    if (w > 1.2) { chair(x - 0.35, z - d / 2 - 0.35, 0); chair(x + 0.35, z + d / 2 + 0.35, Math.PI); }
    solid(x - w / 2 - 0.5, z - d / 2 - 0.3, x + w / 2 + 0.5, z + d / 2 + 0.3);
  };
  const roundTable = (x, z, high, stoolCol) => {
    const h = high ? 1.05 : 0.75;
    cyl(0.5, 0.05, x, Y + h, z, woodM, 20); cyl(0.07, h, x, Y + h / 2, z, dark);
    for (let i = 0; i < 3; i++) { const a = i * 2.1 + 0.5, sx = x + Math.cos(a) * 0.85, sz = z + Math.sin(a) * 0.85; cyl(0.19, 0.1, sx, Y + (high ? 0.72 : 0.46), sz, stoolCol, 12); cyl(0.04, high ? 0.7 : 0.44, sx, Y + (high ? 0.35 : 0.22), sz, dark, 6); }
    solid(x - 0.6, z - 0.6, x + 0.6, z + 0.6);
  };
  const pool = (x, z, alongX) => {
    const w = alongX ? 2.2 : 1.25, d = alongX ? 1.25 : 2.2;
    box(w, 0.5, d, x, Y + 0.57, z, 0x4a2a20); box(w - 0.22, 0.04, d - 0.22, x, Y + 0.83, z, 0x1f9a5a);
    for (const a of [-1, 1]) for (const b of [-1, 1]) box(0.14, 0.34, 0.14, x + a * (w / 2 - 0.15), Y + 0.17, z + b * (d / 2 - 0.15), 0x3a2018);
    [[0.2, 0.1, 0xd6402f], [-0.3, -0.2, 0xf0c330], [0.5, -0.25, 0xf4f1e6], [-0.6, 0.2, 0x1b1b1d]].forEach(([a, b, c]) => { const m = new THREE.Mesh(new THREE.SphereGeometry(0.03, 8, 6), flat(c)); m.position.set(x + (alongX ? a : b), Y + 0.88, z + (alongX ? b : a)); g.add(m); });
    solid(x - w / 2, z - d / 2, x + w / 2, z + d / 2);
    if (cfg.poolLamps) for (const e of [-0.6, 0, 0.6]) { const m = new THREE.Mesh(new THREE.SphereGeometry(0.22, 12, 6, 0, 6.3, 0, 1.6), flat(0x1f7a4a)); m.position.set(x + (alongX ? e : 0), Y + 2.05, z + (alongX ? 0 : e)); g.add(m); }
    return { x: x + cfg.x, z: z + cfg.z };
  };
  const bar = (x, z, len) => {
    box(len, 1.08, 0.55, x, Y + 0.54, z, cfg.barColor); box(len + 0.3, 0.06, 0.75, x, Y + 1.11, z, woodM);
    box(len, 2.3, 0.4, x, Y + 1.15, z - 2.15, dark);
    const bk = new THREE.Mesh(new THREE.PlaneGeometry(len - 0.4, 1.0), lam({ map: tex(PATTERN.brick, 128, len / 1.2, 1) })); bk.position.set(x, Y + 1.75, z - 1.94); g.add(bk);
    box(len - 0.4, 0.04, 0.2, x, Y + 1.3, z - 1.9, woodM);
    for (let i = 0; i < 12; i++) cyl(0.035, 0.26, x - len / 2 + 0.5 + i * ((len - 1) / 11), Y + 1.45, z - 1.88, [0x7a4a1e, 0x2f6b3a, 0xd9d6c8, 0x8a2a2a][i % 4], 8);
    for (let i = 0; i < 5; i++) { const px = x - 0.9 + i * 0.45; cyl(0.03, 0.34, px, Y + 1.3, z + 0.05, 0xb8bcc2, 8); const d2 = cyl(0.07, 0.02, px, Y + 1.5, z + 0.07, [0xc8553d, 0x2b4c9b, 0xe9c46a, 0x2f8a5a, 0x1b1b1d][i], 12); d2.rotation.x = Math.PI / 2; }
    solid(x - len / 2 - 0.15, z - 0.4, x + len / 2 + 0.15, z + 0.4);
    solid(x - len / 2 - 0.15, z - 2.4, x + len / 2 + 0.15, z - 0.4, 0); // nobody behind the bar but staff
    [[-0.9, 0x24262b, 0x6a3a22], [0.8, 0x4a5568, 0x3a2a1e]].forEach(([dx, jacket, hair]) => { const f = makeFigure({ jacket, hair, trousers: 0x2a2f3a }); f.group.position.set(x + dx, Y, z - 1.0); g.add(f.group); });
  };
  const tv = (x, z, ry) => { const m = box(1.5, 0.85, 0.06, x, Y + 1.95, z, 0x111214); m.rotation.y = ry; const s = new THREE.Mesh(new THREE.PlaneGeometry(1.4, 0.75), new THREE.MeshBasicMaterial({ color: 0x1d3a5c })); s.position.set(x, Y + 1.95, z); s.rotation.y = ry; s.translateZ(0.04); g.add(s); };
  const windowLight = (x, z, ry) => { const m = new THREE.Mesh(new THREE.PlaneGeometry(2.0, 1.3), new THREE.MeshBasicMaterial({ color: 0xcfe3f2 })); m.position.set(x, Y + 1.85, z); m.rotation.y = ry; m.translateZ(0.03); g.add(m); const fr = box(2.2, 0.08, 0.08, x, Y + 1.85, z, 0xf4f1e6); fr.rotation.y = ry; const fr2 = box(0.08, 1.4, 0.08, x, Y + 1.85, z, 0xf4f1e6); fr2.rotation.y = ry; };
  const lamp = (x, z) => { const l = new THREE.PointLight(0xffd9a8, 10, 11, 1.7); l.position.set(x, Y + 2.45, z); g.add(l); const s = new THREE.Mesh(new THREE.SphereGeometry(0.12, 8, 6), new THREE.MeshBasicMaterial({ color: 0xfff0c8 })); s.position.set(x, Y + 2.7, z); g.add(s); };

  // way out
  box(1.0, 2.1, 0.06, 0, Y + 1.05, D / 2 - 0.04, 0x3a2416);
  box(0.5, 0.18, 0.04, 0, Y + 2.35, D / 2 - 0.04, new THREE.MeshBasicMaterial({ color: 0x2fae5a }));
  const out = { name: cfg.name, floorY: Y, spawn: { x: cfg.x, z: cfg.z + D / 2 - 3.8 }, exit: { x: cfg.x, z: cfg.z + D / 2 - 0.6 }, pools: [], darts: null };

  if (cfg.layout === 'starkie') {
    zone(5.6, 3.4, 3.6, -0.9, 'flags');
    bar(3.7, -2.4, 4.6);
    out.pools.push(pool(-3.6, -2.7, true));
    windowLight(-3.6, -D / 2, 0); windowLight(-W / 2, 1.5, Math.PI / 2);
    tv(-W / 2 + 0.05, -2.2, Math.PI / 2);
    table(-3.6, 2.3, 1.6, 0.8); table(3.6, 2.9, 1.6, 0.8);
    roundTable(-0.4, -0.3, true, 0x5a4a66);
    for (const z of [-3.5, 0.2]) box(0.12, 0.6, 1.1, W / 2 - 0.08, Y + 0.45, z, 0xf1eee6); // radiators
    lamp(-3.6, -2.6); lamp(3.4, -0.6); lamp(0, 2.6);
  } else {
    // Flying Dutchman: bar faces the door, open area to the right, pool room on the left, small room back left
    bar(0.9, -2.4, 4.6);
    partition(-2.7, D / 2, -2.7, 3.3); partition(-2.7, 1.9, -2.7, 0.2); partition(-W / 2, 0.2, -2.7, 0.2);
    partition(-2.7, 0.2, -2.7, -0.6); partition(-2.7, -2.2, -2.7, -D / 2);
    zone(3.7, 4.7, -4.6, 2.6, 'parquet');
    out.pools.push(pool(-4.7, 2.7, false));
    // dartboard in the pool room
    box(1.0, 1.0, 0.08, -4.6, Y + 1.75, 0.34, 0x3a2416);
    const board = cyl(0.23, 0.03, -4.6, Y + 1.75, 0.4, 0x1b1b1d, 20); board.rotation.x = Math.PI / 2;
    const ring = cyl(0.17, 0.035, -4.6, Y + 1.75, 0.4, 0xd9cdb0, 20); ring.rotation.x = Math.PI / 2;
    const bull = cyl(0.03, 0.04, -4.6, Y + 1.75, 0.4, 0xc8202c, 10); bull.rotation.x = Math.PI / 2;
    out.darts = { x: cfg.x - 4.6, z: cfg.z + 1.2 };
    // fireplace with blue mantel and stove on the right wall
    for (const z of [1.3, 2.7]) box(0.22, 1.15, 0.24, W / 2 - 0.12, Y + 0.575, z, 0x1f4a6b);
    box(0.24, 0.26, 1.64, W / 2 - 0.13, Y + 1.2, 2.0, 0x1f4a6b); box(0.4, 0.06, 1.9, W / 2 - 0.2, Y + 1.36, 2.0, 0x1f4a6b);
    box(0.5, 0.62, 0.55, W / 2 - 0.42, Y + 0.33, 2.0, 0x16171a);
    const glow = new THREE.Mesh(new THREE.PlaneGeometry(0.3, 0.26), new THREE.MeshBasicMaterial({ color: 0xff8a3c })); glow.position.set(W / 2 - 0.68, Y + 0.36, 2.0); glow.rotation.y = -Math.PI / 2; g.add(glow);
    box(1.4, 0.04, 2.2, W / 2 - 0.7, Y + 0.02, 2.0, lam({ map: tex(PATTERN.flags, 128, 1, 1.5) }));
    box(0.05, 1.0, 0.85, W / 2 - 0.04, Y + 2.05, 2.0, 0xb9c4cc); // mirror
    tv(W / 2 - 0.05, 3.9, -Math.PI / 2); windowLight(3.6, D / 2, Math.PI);
    roundTable(3.6, 0.6, false, 0x7a3a3a); roundTable(4.6, 3.6, false, 0x7a3a3a); table(2.2, 3.4, 1.0, 0.7);
    table(-4.7, -2.9, 1.0, 0.7); roundTable(-4.6, -1.0, false, 0x7a3a3a); // back left room
    windowLight(-W / 2, -2.6, Math.PI / 2);
    lamp(-4.6, 2.6); lamp(3.8, 2.0); lamp(0.9, -0.6); lamp(-4.6, -2.4);
  }
  return out;
}
