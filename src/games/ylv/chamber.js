// Young Lancashire Voice: the council chamber as a room you can sit in.
// Built from shapes and drawn textures only. No photograph is used anywhere, and the
// carvings, shields and seat positions are stand-ins, not copies of the real ones.
// The page hands in the same seat plan it draws in 2D, so the plan and the room agree.
import * as THREE from 'three';
import { mergeGeometries } from 'three/examples/jsm/utils/BufferGeometryUtils.js';

const K = 0.04, CX = 300, CY = 118, STEP = 0.2, DEG = Math.PI / 180;
const W = 11, ZF = -4.9, ZB = 12.5, H = 8.6;
const R0 = 112 * K, RGAP = 29 * K;
export const OFFICE = { chair: 84, ce: 85, officer: 86, pupil: 87 };

const rng = (seed) => { let s = seed; return () => (s = (s * 16807) % 2147483647) / 2147483647; };

function tex(w, h, draw, repeat) {
  const c = document.createElement('canvas');
  c.width = w; c.height = h;
  draw(c.getContext('2d'), w, h);
  const t = new THREE.CanvasTexture(c);
  t.colorSpace = THREE.SRGBColorSpace;
  t.anisotropy = 4;
  if (repeat) { t.wrapS = t.wrapT = THREE.RepeatWrapping; t.repeat.set(repeat[0], repeat[1]); }
  return t;
}
const woodTex = (base, streak, seed) => tex(256, 256, (g, w, h) => {
  const r = rng(seed);
  g.fillStyle = base; g.fillRect(0, 0, w, h);
  for (let i = 0; i < 110; i++) {
    g.strokeStyle = streak; g.globalAlpha = 0.06 + r() * 0.2; g.lineWidth = 0.6 + r() * 2.4;
    const y = r() * h, b = (r() - 0.5) * 18;
    g.beginPath(); g.moveTo(0, y); g.bezierCurveTo(w / 3, y + b, (2 * w) / 3, y - b, w, y); g.stroke();
  }
}, [1, 1]);
const carpetTex = () => tex(96, 96, (g, w) => {
  g.fillStyle = '#8f7a70'; g.fillRect(0, 0, w, w);
  g.strokeStyle = '#cdbcae'; g.lineWidth = 5;
  [[0, 0, w, w], [0, w, w, 0], [-w / 2, w / 2, w / 2, w * 1.5], [w / 2, -w / 2, w * 1.5, w / 2], [-w / 2, w / 2, w / 2, -w / 2], [w / 2, w * 1.5, w * 1.5, w / 2]]
    .forEach(([a, b, c, d]) => { g.beginPath(); g.moveTo(a, b); g.lineTo(c, d); g.stroke(); });
  g.fillStyle = '#5f4d47';
  [[w / 2, w / 2], [0, 0], [w, 0], [0, w], [w, w]].forEach(([x, y]) => { g.beginPath(); g.moveTo(x, y - 11); g.lineTo(x + 11, y); g.lineTo(x, y + 11); g.lineTo(x - 11, y); g.fill(); });
  g.fillStyle = '#e2d5c8';
  [[w / 2, 0], [0, w / 2], [w, w / 2], [w / 2, w]].forEach(([x, y]) => { g.beginPath(); g.arc(x, y, 5, 0, 7); g.fill(); });
}, [2.2, 2.2]);
const marbleTex = () => tex(64, 256, (g, w, h) => {
  const r = rng(7);
  g.fillStyle = '#71877d'; g.fillRect(0, 0, w, h);
  for (let i = 0; i < 80; i++) {
    g.strokeStyle = r() > 0.5 ? '#3e5148' : '#b3c4bb'; g.globalAlpha = 0.15 + r() * 0.35; g.lineWidth = 0.5 + r() * 2.5;
    const x = r() * w, b = (r() - 0.5) * 14;
    g.beginPath(); g.moveTo(x, 0); g.bezierCurveTo(x + b, h / 3, x - b, (2 * h) / 3, x + b / 2, h); g.stroke();
  }
}, [2, 1]);
const windowTex = () => tex(256, 512, (g, w, h) => {
  const arch = (inset) => { g.beginPath(); g.moveTo(inset, h); g.lineTo(inset, 170); g.arc(128, 170, 128 - inset, Math.PI, 0); g.lineTo(w - inset, h); g.closePath(); };
  arch(0); g.fillStyle = '#f7f3ea'; g.fill();
  arch(16); const sky = g.createLinearGradient(0, 40, 0, h); sky.addColorStop(0, '#dbe9f1'); sky.addColorStop(1, '#f3f6f1'); g.fillStyle = sky; g.fill();
  g.save(); arch(16); g.clip();
  g.strokeStyle = '#3a352e'; g.lineWidth = 3;
  for (let x = 16; x < w; x += 37.3) { g.beginPath(); g.moveTo(x, 170); g.lineTo(x, h); g.stroke(); }
  for (let y = 214; y < h; y += 44) { g.beginPath(); g.moveTo(0, y); g.lineTo(w, y); g.stroke(); }
  for (let a = 0; a <= 8; a++) { const t = Math.PI + (a * Math.PI) / 8; g.beginPath(); g.moveTo(128, 170); g.lineTo(128 + 130 * Math.cos(t), 170 + 130 * Math.sin(t)); g.stroke(); }
  g.beginPath(); g.arc(128, 170, 78, Math.PI, 0); g.stroke();
  g.lineWidth = 9; g.beginPath(); g.moveTo(128, 40); g.lineTo(128, h); g.moveTo(0, 170); g.lineTo(w, 170); g.moveTo(0, 346); g.lineTo(w, 346); g.stroke();
  g.restore();
  g.fillStyle = '#3a352e'; g.beginPath(); g.arc(128, 196, 56, 0, 7); g.fill();
  g.fillStyle = '#86655a'; g.beginPath(); g.arc(128, 196, 46, 0, 7); g.fill();
  g.strokeStyle = '#a98b7c'; g.lineWidth = 3; g.beginPath(); g.arc(128, 196, 30, 0, 7); g.stroke();
  g.strokeStyle = '#f7f3ea'; g.lineWidth = 16; arch(8); g.stroke();
});
const keyTex = (rep) => tex(48, 48, (g) => {
  g.fillStyle = '#f1ead8'; g.fillRect(0, 0, 48, 48);
  g.strokeStyle = '#b4ad9b'; g.lineWidth = 4; g.lineJoin = 'miter';
  g.beginPath(); g.moveTo(0, 40); g.lineTo(6, 40); g.lineTo(6, 10); g.lineTo(42, 10); g.lineTo(42, 40); g.lineTo(48, 40); g.stroke();
  g.beginPath(); g.moveTo(18, 40); g.lineTo(18, 22); g.lineTo(30, 22); g.lineTo(30, 32); g.stroke();
  g.fillStyle = '#c9a030'; g.fillRect(0, 0, 48, 3);
}, [rep, 1]);
const mottoTex = () => tex(512, 56, (g) => {
  g.fillStyle = '#3d2616'; g.fillRect(0, 0, 512, 56);
  g.fillStyle = '#d9bd83'; g.font = '600 27px Georgia, serif'; g.textAlign = 'center'; g.textBaseline = 'middle';
  g.fillText('I N   C O N C I L I O   C O N S I L I U M', 256, 30);
});
const platesTex = (n) => tex(12 * 64, 7 * 28, (g) => {
  g.fillStyle = '#151515'; g.fillRect(0, 0, 768, 196);
  g.fillStyle = '#fff'; g.font = '700 20px Arial, sans-serif'; g.textAlign = 'center'; g.textBaseline = 'middle';
  for (let i = 0; i < n; i++) g.fillText(String(i + 1), (i % 12) * 64 + 32, Math.floor(i / 12) * 28 + 15);
});
const clockTex = () => tex(128, 128, (g) => {
  const now = new Date();
  g.fillStyle = '#5b3a1e'; g.beginPath(); g.arc(64, 64, 64, 0, 7); g.fill();
  g.fillStyle = '#b08a55'; g.beginPath(); g.arc(64, 64, 54, 0, 7); g.fill();
  g.strokeStyle = '#f0d58a'; g.lineCap = 'round';
  for (let i = 0; i < 12; i++) { const a = (i * Math.PI) / 6; g.lineWidth = 3; g.beginPath(); g.moveTo(64 + 44 * Math.sin(a), 64 - 44 * Math.cos(a)); g.lineTo(64 + 50 * Math.sin(a), 64 - 50 * Math.cos(a)); g.stroke(); }
  const hand = (a, len, lw) => { g.lineWidth = lw; g.beginPath(); g.moveTo(64, 64); g.lineTo(64 + len * Math.sin(a), 64 - len * Math.cos(a)); g.stroke(); };
  hand(((now.getHours() % 12) + now.getMinutes() / 60) * (Math.PI / 6), 26, 5);
  hand(now.getMinutes() * (Math.PI / 30), 40, 3);
});

// A curved block: the piece between two radii and two angles. Benches, desks and tiers are all this.
function sector(r0, r1, y0, y1, a0, a1, n = 14) {
  const pos = [], uv = [];
  const P = (r, a, y) => [r * Math.cos(a), y, r * Math.sin(a)];
  const quad = (p, u) => [0, 1, 2, 0, 2, 3].forEach((k) => { pos.push(...p[k]); uv.push(...u[k]); });
  for (let i = 0; i < n; i++) {
    const p = a0 + ((a1 - a0) * i) / n, q = a0 + ((a1 - a0) * (i + 1)) / n;
    const top = [P(r0, p, y1), P(r1, p, y1), P(r1, q, y1), P(r0, q, y1)];
    quad(top, top.map((v) => [v[0], v[2]]));
    for (const r of [r0, r1]) quad([P(r, p, y0), P(r, q, y0), P(r, q, y1), P(r, p, y1)], [[p * r, y0], [q * r, y0], [q * r, y1], [p * r, y1]]);
  }
  for (const a of [a0, a1]) quad([P(r0, a, y0), P(r1, a, y0), P(r1, a, y1), P(r0, a, y1)], [[r0, y0], [r1, y0], [r1, y1], [r0, y1]]);
  const g = new THREE.BufferGeometry();
  g.setAttribute('position', new THREE.Float32BufferAttribute(pos, 3));
  g.setAttribute('uv', new THREE.Float32BufferAttribute(uv, 2));
  g.computeVertexNormals();
  return g;
}

export function createRoom(host, { plan, reduced = false }) {
  const canvas = document.createElement('canvas');
  canvas.setAttribute('aria-hidden', 'true');
  const renderer = new THREE.WebGLRenderer({ canvas, antialias: true, powerPreference: 'high-performance' });
  renderer.setPixelRatio(Math.min(window.devicePixelRatio || 1, matchMedia('(pointer: coarse)').matches ? 1.75 : 2));
  renderer.toneMapping = THREE.ACESFilmicToneMapping;
  renderer.toneMappingExposure = 1.05;
  const label = document.createElement('div');
  label.className = 'ylv-room-label';
  host.append(canvas, label);

  const scene = new THREE.Scene();
  scene.background = new THREE.Color(0x1a1512);
  const camera = new THREE.PerspectiveCamera(66, 1.6, 0.08, 60);
  camera.rotation.order = 'YXZ';
  scene.add(new THREE.HemisphereLight(0xfff3dc, 0x6a5848, 2.3), new THREE.AmbientLight(0xffffff, 0.55));
  const sun = new THREE.DirectionalLight(0xfff8ea, 1.5); sun.position.set(9, 10, 5); scene.add(sun);
  const fill = new THREE.DirectionalLight(0xdfe9f2, 0.7); fill.position.set(-9, 6, -2); scene.add(fill);

  const L = (color, map) => new THREE.MeshLambertMaterial({ color, map: map || null, side: THREE.DoubleSide });
  const mats = {
    wood: L(0xffffff, woodTex('#6e4526', '#2c1a0d', 3)),
    dark: L(0xffffff, woodTex('#42291a', '#1a0f08', 5)),
    light: L(0xffffff, woodTex('#a8763c', '#6a4520', 9)),
    leather: L(0x2f5247),
    carpet: L(0xffffff, carpetTex()),
    wall: L(0xefe6cf),
    white: L(0xf6f2e8),
    beam: L(0xb7bbbb),
    gold: L(0xd0a42c),
    brass: L(0xa88a3c),
    marble: L(0xffffff, marbleTex()),
    black: L(0x1b1b1b),
    crest: L(0x8a3b32),
    glass: L(0xdfe8ec),
  };
  const bins = {};
  const put = (m, g) => { (bins[m] ||= []).push(g.index ? g.toNonIndexed() : g); return g; };
  const box = (m, w, h, d, x, y0, z, ry = 0) => { const g = new THREE.BoxGeometry(w, h, d); if (ry) g.rotateY(ry); g.translate(x, y0 + h / 2, z); return put(m, g); };
  const cyl = (m, rt, rb, h, x, y0, z, seg = 18) => { const g = new THREE.CylinderGeometry(rt, rb, h, seg); g.translate(x, y0 + h / 2, z); return put(m, g); };
  const arch = (m, r, depth, sy, x, y, z) => { const g = new THREE.CylinderGeometry(r, r, depth, 24, 1, false, 0, Math.PI); g.rotateZ(Math.PI / 2); g.rotateY(Math.PI / 2); g.scale(1, sy, 1); g.translate(x, y, z); return put(m, g); };

  /* Seats: take the 2D plan and stand it up. */
  const seats = plan.map((p) => {
    const x = (p.x - CX) * K, z = (p.y - CY) * K, rad = Math.hypot(x, z), row = Math.round((rad - R0) / RGAP);
    return { x, z, rad, row, a: Math.atan2(z, x), fy: row * STEP, ry: Math.atan2(-x, -z) };
  });
  const rows = Math.max(...seats.map((s) => s.row)) + 1;

  /* Floor, tiers and the stepped aisle. */
  { const g = new THREE.PlaneGeometry(2 * W, ZB - ZF); g.rotateX(-Math.PI / 2); g.translate(0, 0, (ZB + ZF) / 2);
    const uv = g.attributes.uv, p = g.attributes.position; for (let i = 0; i < uv.count; i++) uv.setXY(i, p.getX(i), p.getZ(i)); put('carpet', g); }
  for (let r = 1; r < rows; r++) {
    const rin = R0 + r * RGAP - 0.62;
    put('carpet', sector(rin, 17, 0, r * STEP, 6 * DEG, 174 * DEG, 30));
    const half = 0.62 / rin;
    put('brass', sector(rin - 0.012, rin + 0.05, r * STEP - 0.035, r * STEP + 0.004, Math.PI / 2 - half, Math.PI / 2 + half, 4));
  }
  /* Benches: a desk in front, a green leather seat and back, a numbered plate and a microphone at each place. */
  for (let r = 0; r < rows; r++) {
    const rad = R0 + r * RGAP, fy = r * STEP;
    for (const [a0, a1] of [[11 * DEG, 85.5 * DEG], [94.5 * DEG, 169 * DEG]]) {
      put('wood', sector(rad - 0.62, rad - 0.56, fy, fy + 0.72, a0, a1));
      put('wood', sector(rad - 0.68, rad - 0.3, fy + 0.72, fy + 0.78, a0, a1));
      put('dark', sector(rad - 0.08, rad + 0.34, fy, fy + 0.4, a0, a1));
      put('leather', sector(rad - 0.14, rad + 0.34, fy + 0.4, fy + 0.48, a0, a1));
      put('leather', sector(rad + 0.34, rad + 0.42, fy + 0.4, fy + 1.0, a0, a1));
      put('wood', sector(rad + 0.42, rad + 0.46, fy, fy + 1.0, a0, a1));
      put('wood', sector(rad + 0.31, rad + 0.48, fy + 1.0, fy + 1.05, a0, a1));
      for (const a of [a0, a1]) {
        const m = rad - 0.1;
        box('wood', 1.16, 1.08, 0.09, m * Math.cos(a), fy, m * Math.sin(a), -a);
        box('dark', 0.34, 0.3, 0.11, (rad + 0.3) * Math.cos(a), fy + 0.86, (rad + 0.3) * Math.sin(a), -a);
      }
    }
  }
  const plateGeo = [];
  seats.forEach((s, i) => {
    const c = Math.cos(s.a), n = Math.sin(s.a), r = s.rad - 0.49;
    box('brass', 0.05, 0.006, 0.18, r * c, s.fy + 0.78, r * n, -s.a);
    cyl('black', 0.005, 0.007, 0.25, r * c, s.fy + 0.78, r * n, 6);
    cyl('black', 0.012, 0.012, 0.05, r * c, s.fy + 1.02, r * n, 8);
    const g = new THREE.PlaneGeometry(0.13, 0.052); g.rotateY(Math.PI / 2 - s.a); g.translate((s.rad - 0.297) * c, s.fy + 0.75, (s.rad - 0.297) * n);
    const u0 = (i % 12) / 12, v1 = 1 - Math.floor(i / 12) / 7, u1 = u0 + 1 / 12, v0 = v1 - 1 / 7;
    g.attributes.uv.array.set([u0, v1, u1, v1, u0, v0, u1, v0]);
    plateGeo.push(g.toNonIndexed());
  });
  scene.add(new THREE.Mesh(mergeGeometries(plateGeo), new THREE.MeshBasicMaterial({ map: platesTex(seats.length) })));

  /* The dais: the Chairman's bench, high-backed chairs and the canopy with the motto. */
  box('dark', 9.2, 0.9, 2.7, 0, 0, -3.55);
  box('carpet', 9.2, 0.02, 2.7, 0, 0.9, -3.55);
  box('wood', 8.8, 1.64, 0.1, 0, 0, -2.3);
  box('wood', 8.9, 0.05, 0.55, 0, 1.62, -2.5);
  box('dark', 8.9, 0.1, 0.14, 0, 1.5, -2.26);
  for (let i = -3; i <= 3; i++) {
    box('light', 0.92, 0.72, 0.02, i * 1.22, 0.55, -2.24);
    const g = new THREE.BoxGeometry(0.24, 0.24, 0.02); g.rotateZ(Math.PI / 4); g.translate(i * 1.22, 0.91, -2.225); put('gold', g);
  }
  for (let i = -4; i <= 4; i++) {
    const x = i * 0.96, tall = i === 0 ? 1.7 : 1.35;
    box('dark', 0.58, 0.08, 0.5, x, 1.32, -3.12);
    box('dark', 0.6, tall, 0.07, x, 1.32, -3.42);
    box('leather', 0.46, tall - 0.42, 0.03, x, 1.5, -3.375);
    arch('dark', 0.3, 0.07, 0.6, x, 1.32 + tall, -3.42);
    cyl('dark', 0.05, 0.05, 0.1, x, 1.32 + tall + 0.16, -3.42, 8);
  }
  for (const x of [-1.92, 0, 1.92]) box('black', 0.46, 0.3, 0.03, x + 0.45, 1.67, -2.62);
  for (const s of [-1, 1]) { box('dark', 0.34, 3.5, 0.3, s * 1.0, 0.9, -4.72); box('gold', 0.4, 0.14, 0.34, s * 1.0, 4.3, -4.72); }
  box('dark', 2.5, 0.42, 0.36, 0, 4.44, -4.7);
  box('wood', 1.66, 3.4, 0.08, 0, 0.9, -4.84);
  arch('dark', 1.25, 0.36, 0.5, 0, 4.86, -4.7);
  arch('wood', 1.02, 0.4, 0.5, 0, 4.86, -4.7);
  { const g = new THREE.CylinderGeometry(0.3, 0.3, 0.06, 24); g.rotateX(Math.PI / 2); g.scale(0.85, 1.1, 1); g.translate(0, 5.2, -4.48); put('gold', g);
    const c = new THREE.CylinderGeometry(0.2, 0.2, 0.07, 24); c.rotateX(Math.PI / 2); c.scale(0.85, 1.1, 1); c.translate(0, 5.2, -4.47); put('crest', c); }
  { const m = new THREE.Mesh(new THREE.PlaneGeometry(2.3, 0.25), new THREE.MeshBasicMaterial({ map: mottoTex() })); m.position.set(0, 4.65, -4.51); scene.add(m); }
  /* The table in the well. */
  { const top = new THREE.CylinderGeometry(1, 1, 0.07, 36); top.scale(1.84, 1, 0.88); top.translate(0, 0.74, 1.28); put('wood', top);
    const ap = new THREE.CylinderGeometry(1, 1, 0.14, 36); ap.scale(1.7, 1, 0.76); ap.translate(0, 0.62, 1.28); put('dark', ap);
    for (const [x, z] of [[-1.2, 0.95], [1.2, 0.95], [-1.2, 1.6], [1.2, 1.6]]) box('dark', 0.1, 0.62, 0.1, x, 0, z);
    cyl('black', 0.01, 0.01, 0.3, 0, 0.81, 1.28, 6); cyl('black', 0.07, 0.07, 0.012, 0, 0.81, 1.28, 12); box('black', 0.05, 0.03, 0.14, 0, 1.1, 1.28); }

  /* Walls, wainscot, columns, windows, the gallery and the coffered ceiling. */
  const plane = (m, w, h, x, y, z, ry) => { const g = new THREE.PlaneGeometry(w, h); g.rotateY(ry); g.translate(x, y, z); return put(m, g); };
  plane('wall', 2 * W, H, 0, H / 2, ZF, 0); plane('wall', 2 * W, H, 0, H / 2, ZB, Math.PI);
  plane('wall', ZB - ZF, H, -W, H / 2, (ZB + ZF) / 2, Math.PI / 2); plane('wall', ZB - ZF, H, W, H / 2, (ZB + ZF) / 2, -Math.PI / 2);
  { const g = new THREE.PlaneGeometry(2 * W, ZB - ZF); g.rotateX(Math.PI / 2); g.translate(0, H, (ZB + ZF) / 2); put('white', g); }
  for (const s of [-1, 1]) {
    box('wood', 0.08, 1.35, ZB - ZF, s * (W - 0.04), 0, (ZB + ZF) / 2);
    box('dark', 0.12, 0.08, ZB - ZF, s * (W - 0.06), 1.35, (ZB + ZF) / 2);
    for (const z of [-3.9, 0.9, 5.6]) {
      const x = s * (W - 0.95);
      box('black', 1.0, 0.95, 1.0, x, 0, z);
      cyl('marble', 0.47, 0.54, 0.2, x, 0.95, z); cyl('marble', 0.37, 0.41, 5.3, x, 1.15, z, 20);
      cyl('gold', 0.52, 0.37, 0.5, x, 6.45, z, 12); cyl('gold', 0.44, 0.4, 0.1, x, 6.4, z, 12); box('gold', 0.98, 0.12, 0.98, x, 6.95, z);
      box('white', 1.0, H - 7.07, 1.0, x, 7.07, z);
    }
  }
  box('wood', 2 * W, 1.35, 0.08, 0, 0, ZF + 0.04);
  for (const s of [-1, 1]) for (let i = 0; i < 3; i++) box('white', 2.6, 1.9, 0.03, s * (3.2 + i * 3.1), 2.0, ZF + 0.02);
  const winMat = new THREE.MeshBasicMaterial({ map: windowTex(), transparent: true, alphaTest: 0.5 });
  const winGeo = [];
  for (const s of [-1, 1]) for (const z of [-1.5, 3.25, 8.0]) { const g = new THREE.PlaneGeometry(3.3, 6.2); g.rotateY((-s * Math.PI) / 2); g.translate(s * (W - 0.03), 4.5, z); winGeo.push(g); }
  scene.add(new THREE.Mesh(mergeGeometries(winGeo), winMat));
  // Gallery across the back, with its panelled front, clock and brass rail.
  box('white', 2 * W, 0.3, ZB - 9.9, 0, 3.6, (ZB + 9.9) / 2);
  box('wood', 2 * W, 1.35, 0.16, 0, 3.45, 9.9);
  box('dark', 2 * W, 0.1, 0.24, 0, 4.8, 9.9); box('dark', 2 * W, 0.12, 0.22, 0, 3.4, 9.9);
  for (let i = 0; i < 8; i++) { const x = (i - 3.5) * 2.7; box('light', 2.0, 0.62, 0.02, x, 3.82, 9.81); if (i < 7 && i !== 3) { box('gold', 0.36, 0.36, 0.02, x + 1.35, 3.95, 9.81); box('crest', 0.28, 0.28, 0.02, x + 1.35, 3.99, 9.8); } }
  box('brass', 2 * W, 0.04, 0.04, 0, 5.12, 9.9);
  for (let x = -10; x <= 10; x += 2) cyl('brass', 0.02, 0.02, 0.24, x, 4.9, 9.9, 6);
  { const m = new THREE.Mesh(new THREE.CircleGeometry(0.36, 28), new THREE.MeshBasicMaterial({ map: clockTex() })); m.rotation.y = Math.PI; m.position.set(0, 4.13, 9.8); scene.add(m); }
  // Doors: one under the gallery with a curved pediment, one pair at gallery level with lattice glass.
  box('dark', 2.3, 2.7, 0.14, 0, 0.8, ZB - 0.07); box('wood', 1.7, 2.3, 0.04, 0, 0.8, ZB - 0.16); arch('dark', 1.3, 0.2, 0.42, 0, 3.5, ZB - 0.1);
  box('dark', 2.5, 2.9, 0.1, 0, 3.9, ZB - 0.05);
  for (const s of [-1, 1]) { box('wood', 1.0, 2.6, 0.04, s * 0.53, 3.9, ZB - 0.12); box('glass', 0.34, 0.5, 0.02, s * 0.53, 5.4, ZB - 0.15); box('brass', 0.05, 0.4, 0.04, s * 0.08, 5.0, ZB - 0.16); }
  // Ceiling: grey beams making coffers, a white cornice and a Greek key band.
  for (const z of [-0.55, 3.8, 8.15]) box('beam', 2 * W, 0.5, 0.55, 0, H - 0.5, z);
  for (const x of [-3.67, 3.67]) box('beam', 0.55, 0.5, ZB - ZF, x, H - 0.5, (ZB + ZF) / 2);
  box('white', 2 * W, 0.35, 0.35, 0, H - 0.35, ZF + 0.17); box('white', 2 * W, 0.35, 0.35, 0, H - 0.35, ZB - 0.17);
  for (const s of [-1, 1]) box('white', 0.35, 0.35, ZB - ZF, s * (W - 0.17), H - 0.35, (ZB + ZF) / 2);
  { const a = new THREE.MeshBasicMaterial({ map: keyTex(44) }), b = new THREE.MeshBasicMaterial({ map: keyTex(35) });
    const fr = (mat, w, x, z, ry) => { const m = new THREE.Mesh(new THREE.PlaneGeometry(w, 0.5), mat); m.position.set(x, H - 0.75, z); m.rotation.y = ry; scene.add(m); };
    fr(a, 2 * W, 0, ZF + 0.02, 0); fr(a, 2 * W, 0, ZB - 0.02, Math.PI); fr(b, ZB - ZF, -W + 0.02, (ZB + ZF) / 2, Math.PI / 2); fr(b, ZB - ZF, W - 0.02, (ZB + ZF) / 2, -Math.PI / 2); }
  const bulbs = [];
  for (const x of [-7.3, 0, 7.3]) for (const z of [-2.7, 1.6, 6.0]) {
    cyl('black', 0.012, 0.012, 1.3, x, H - 1.3, z, 6); cyl('black', 0.34, 0.34, 0.02, x, H - 1.32, z, 12);
    for (let k = 0; k < 8; k++) { const a = (k * Math.PI) / 4, bx = x + 0.46 * Math.cos(a), bz = z + 0.46 * Math.sin(a), by = H - 1.85 - (k % 2) * 0.32;
      cyl('black', 0.005, 0.005, H - 1.32 - by, bx, by, bz, 4);
      const g = new THREE.SphereGeometry(0.1, 10, 8); g.scale(1, 1.5, 1); g.translate(bx, by, bz); bulbs.push(g.toNonIndexed()); }
  }
  scene.add(new THREE.Mesh(mergeGeometries(bulbs), new THREE.MeshBasicMaterial({ color: 0xfffaf0 })));
  // The drop-down screen beside the canopy. It shows who is speaking, as the real one does.
  const scr = document.createElement('canvas'); scr.width = 640; scr.height = 360;
  const scrTex = new THREE.CanvasTexture(scr); scrTex.colorSpace = THREE.SRGBColorSpace;
  box('black', 4.3, 0.2, 0.2, -5.4, 7.15, ZF + 0.12);
  { const m = new THREE.Mesh(new THREE.PlaneGeometry(4.0, 2.25), new THREE.MeshBasicMaterial({ map: scrTex })); m.position.set(-5.4, 5.95, ZF + 0.1); scene.add(m); }
  function drawScreen(item, who) {
    const g = scr.getContext('2d');
    const bg = g.createLinearGradient(0, 0, 0, 360); bg.addColorStop(0, '#18365f'); bg.addColorStop(1, '#0d1f3a'); g.fillStyle = bg; g.fillRect(0, 0, 640, 360);
    g.fillStyle = '#9fb6d6'; g.font = '600 24px Arial, sans-serif'; g.fillText('FULL COUNCIL', 32, 50);
    g.fillStyle = '#fff'; g.font = '700 52px Georgia, serif';
    const words = String(item || '').split(' '); let line = '', y = 150;
    words.forEach((w) => { if (g.measureText(line + w).width > 570) { g.fillText(line, 32, y); y += 60; line = ''; } line += w + ' '; });
    g.fillText(line, 32, y);
    if (who) { g.fillStyle = '#fff'; g.fillRect(0, 282, 640, 56); g.fillStyle = '#18365f'; g.font = '700 30px Arial, sans-serif'; g.fillText(who, 32, 320); }
    scrTex.needsUpdate = true;
  }

  for (const k in bins) scene.add(new THREE.Mesh(mergeGeometries(bins[k]), mats[k]));

  /* People. Plain figures in quiet colours: no one is a real person and no colour stands for a party. */
  const people = seats.map((s) => ({ x: s.x, z: s.z, fy: s.fy, ry: s.ry }));
  people[OFFICE.chair] = { x: 0, z: -3.12, fy: 0.92, ry: 0 };
  people[OFFICE.ce] = { x: -1.92, z: -3.12, fy: 0.92, ry: 0 };
  people[OFFICE.officer] = { x: 1.92, z: -3.12, fy: 0.92, ry: 0 };
  people[OFFICE.pupil] = { x: -2.6, z: 1.3, fy: -0.5, ry: Math.PI, guest: true };
  const N = people.length;
  const inst = (geo, color) => { const m = new THREE.InstancedMesh(geo, new THREE.MeshLambertMaterial({ color }), N); m.frustumCulled = false; scene.add(m); return m; };
  const torsoGeo = new THREE.CapsuleGeometry(0.2, 0.3, 4, 10); torsoGeo.scale(1.08, 1, 0.68);
  const hairGeo = new THREE.SphereGeometry(0.121, 14, 10);
  const torso = inst(torsoGeo, 0xffffff), head = inst(new THREE.SphereGeometry(0.115, 14, 10), 0xffffff), hair = inst(hairGeo, 0xffffff);
  const eyeGeo = mergeGeometries([-1, 1].map((k) => new THREE.SphereGeometry(0.014, 6, 5).translate(k * 0.042, 0.018, 0.106)));
  const eyes = inst(eyeGeo, 0x1d1a18);
  const arm = inst(new THREE.BoxGeometry(0.075, 0.44, 0.075), 0xffffff), card = inst(new THREE.BoxGeometry(0.3, 0.21, 0.014), 0xffffff);
  const CLOTH = [0x2b3440, 0x3b3f46, 0x4a4036, 0x5a5f66, 0x23303a, 0x6b6257, 0x34383f, 0x7a7468, 0x4b5560];
  const SKIN = [0xf0c8a8, 0xdba47d, 0xb97b52, 0x8c5c3b, 0x6c442a, 0xe8b792];
  const HAIR = [0x2a1d14, 0x4a3524, 0x6e6e6e, 0xb9a57a, 0x151515, 0xc8c8c8, 0x7a4a2a];
  const col = new THREE.Color(), rr = rng(41);
  for (let i = 0; i < N; i++) {
    const c = people[i].guest ? 0x24509c : CLOTH[Math.floor(rr() * CLOTH.length)];
    torso.setColorAt(i, col.set(c)); arm.setColorAt(i, col.set(c));
    head.setColorAt(i, col.set(SKIN[Math.floor(rr() * SKIN.length)]));
    hair.setColorAt(i, col.set(HAIR[Math.floor(rr() * HAIR.length)]));
  }
  // The Chairman's figure is set by hand, not by the shuffle, so it does not misdescribe the office holder.
  head.setColorAt(OFFICE.chair, col.set(SKIN[0])); hair.setColorAt(OFFICE.chair, col.set(HAIR[2]));
  const legs = new THREE.Mesh(new THREE.BoxGeometry(0.34, 0.92, 0.2), new THREE.MeshLambertMaterial({ color: 0x2a2d33 })); scene.add(legs);
  const pointer = new THREE.Mesh(new THREE.ConeGeometry(0.11, 0.24, 4), new THREE.MeshBasicMaterial({ color: 0xffcf33 })); pointer.rotation.x = Math.PI; scene.add(pointer);
  const mine = new THREE.Mesh(new THREE.OctahedronGeometry(0.13), new THREE.MeshBasicMaterial({ color: 0xffcf33 })); scene.add(mine);
  const VOTE = { for: 0x2e8b3a, against: 0xc2362c };
  const dummy = new THREE.Object3D(), ZERO = new THREE.Matrix4().makeScale(0, 0, 0);

  const st = { seat: -1, speaker: null, tally: null, mode: 'over', pick: false, hover: -1 };
  function placePeople() {
    const seatedMe = st.mode === 'seat' ? st.seat : -1;
    const sp = st.speaker === 'me' ? (st.mode === 'seat' ? -1 : st.seat) : st.speaker;
    legs.visible = false;
    for (let i = 0; i < N; i++) {
      const p = people[i];
      const show = i !== seatedMe && (!p.guest || sp === i);
      const up = sp === i && i !== OFFICE.chair ? 0.44 : 0;
      const set = (mesh, lx, ly, lz, on = true) => {
        if (!show || !on) return mesh.setMatrixAt(i, ZERO);
        const s = Math.sin(p.ry), c = Math.cos(p.ry);
        dummy.position.set(p.x + lx * c + lz * s, p.fy + up + ly, p.z - lx * s + lz * c);
        dummy.rotation.set(0, p.ry, 0); dummy.updateMatrix(); mesh.setMatrixAt(i, dummy.matrix);
      };
      set(torso, 0, 0.78, 0); set(head, 0, 1.2, 0.01); set(hair, 0, 1.228, -0.03); set(eyes, 0, 1.2, 0.01);
      const v = st.tally && i < seats.length ? st.tally[i] : null;
      const hand = v === 'for' || v === 'against';
      set(arm, 0.25, 1.2, 0.04, hand); set(card, 0.25, 1.52, 0.04, hand);
      if (hand) card.setColorAt(i, col.set(VOTE[v]));
      if (show && up) { legs.visible = true; legs.position.set(p.x, p.fy + 0.46 + (p.guest ? 0.5 : 0), p.z); legs.rotation.y = p.ry; }
    }
    [torso, head, hair, eyes, arm, card].forEach((m) => { m.instanceMatrix.needsUpdate = true; });
    if (card.instanceColor) card.instanceColor.needsUpdate = true;
    const sh = sp != null && sp >= 0 ? people[sp] : null;
    pointer.visible = !!sh;
    if (sh) pointer.position.set(sh.x, sh.fy + 1.2 + (sp === OFFICE.chair ? 1.0 : 0.82), sh.z);
    const mk = st.pick ? st.hover : st.mode !== 'seat' ? st.seat : -1;
    mine.visible = mk >= 0 && mk !== sp;
    if (mk >= 0) mine.position.set(people[mk].x, people[mk].fy + 1.62, people[mk].z);
  }

  /* Camera: your own seat, or the public gallery. It turns to whoever is speaking. */
  const cam = { pos: new THREE.Vector3(0, 6.5, 11.4), yaw: 0, pitch: -0.5, tp: new THREE.Vector3(), tyaw: 0, tpitch: 0, oy: 0, op: 0, fov: 56, tfov: 56, first: true };
  const tmp = new THREE.Vector3();
  function aim() {
    let look;
    if (st.mode === 'seat' && st.seat >= 0) {
      const s = seats[st.seat], standing = st.speaker === 'me';
      cam.tp.set(s.x * (1 + 0.1 / s.rad), s.fy + 1.2 + (standing ? 0.42 : 0), s.z * (1 + 0.1 / s.rad));
      const sp = st.speaker != null && st.speaker !== 'me' ? people[st.speaker] : people[OFFICE.chair];
      look = tmp.set(sp.x, sp.fy + 1.25, sp.z);
      if (st.tally) look.set(0, 0.9, 1.5);
      cam.tfov = 68;
    } else if (st.mode === 'chair') {
      cam.tp.set(0, 3.5, -1.9); look = tmp.set(0, 1.1, 6.5); cam.tfov = camera.aspect < 1.2 ? 84 : 72;
    } else {
      cam.tp.set(0, 7.3, 10.15); look = tmp.set(0, 0.2, 4.3); cam.tfov = camera.aspect < 1.2 ? 82 : 70;
    }
    const d = look.sub(cam.tp);
    cam.tyaw = Math.atan2(-d.x, -d.z); cam.tpitch = Math.asin(d.y / d.length());
  }
  let raf = 0, until = 0, last = 0, dragging = false;
  function frame(t) {
    raf = 0;
    const dt = Math.min(0.05, (t - last) / 1000 || 0.016); last = t;
    const k = reduced || cam.first ? 1 : 1 - Math.exp(-dt * 4.5);
    cam.first = false;
    cam.pos.lerp(cam.tp, k);
    const ty = cam.tyaw + cam.oy; let dy = ty - cam.yaw; dy = Math.atan2(Math.sin(dy), Math.cos(dy));
    cam.yaw += dy * k; cam.pitch += (cam.tpitch + cam.op - cam.pitch) * k; cam.fov += (cam.tfov - cam.fov) * k;
    camera.position.copy(cam.pos); camera.rotation.set(cam.pitch, cam.yaw, 0);
    if (Math.abs(camera.fov - cam.fov) > 0.01) { camera.fov = cam.fov; camera.updateProjectionMatrix(); }
    pointer.rotation.y = mine.rotation.y = t / 700;
    renderer.render(scene, camera);
    const settled = cam.pos.distanceToSquared(cam.tp) < 1e-5 && Math.abs(dy) < 1e-3 && Math.abs(cam.tpitch + cam.op - cam.pitch) < 1e-3;
    if (t < until || dragging || !settled) raf = requestAnimationFrame(frame);
  }
  const kick = (ms = 1800) => { until = performance.now() + (reduced ? 120 : ms); if (!raf) { last = performance.now(); raf = requestAnimationFrame(frame); } };

  function resize() {
    const w = host.clientWidth, h = host.clientHeight;
    if (!w || !h) return;
    renderer.setSize(w, h, false); camera.aspect = w / h; camera.updateProjectionMatrix(); aim(); kick(300);
  }
  new ResizeObserver(resize).observe(host);

  /* Looking round (drag, or arrow keys) and choosing a seat (tap). */
  const ray = new THREE.Raycaster(), ndc = new THREE.Vector2();
  function seatAt(e) {
    const r = canvas.getBoundingClientRect();
    ndc.set(((e.clientX - r.left) / r.width) * 2 - 1, -((e.clientY - r.top) / r.height) * 2 + 1);
    ray.setFromCamera(ndc, camera);
    // Nearest seat to where the ray meets the level of the seats: forgiving for small fingers.
    let best = -1, bd = 0.75;
    seats.forEach((s, i) => {
      const t = (s.fy + 0.9 - ray.ray.origin.y) / ray.ray.direction.y;
      if (t <= 0) return;
      const d = Math.hypot(ray.ray.origin.x + ray.ray.direction.x * t - s.x, ray.ray.origin.z + ray.ray.direction.z * t - s.z);
      if (d < bd) { bd = d; best = i; }
    });
    return best;
  }
  let down = null;
  const limit = () => { const m = st.mode === 'seat' ? 2.6 : 0.6; cam.oy = Math.max(-m, Math.min(m, cam.oy)); cam.op = Math.max(-0.7, Math.min(0.7, cam.op)); };
  canvas.addEventListener('pointerdown', (e) => { down = { x: e.clientX, y: e.clientY, oy: cam.oy, op: cam.op, moved: false }; dragging = true; canvas.setPointerCapture(e.pointerId); kick(); });
  canvas.addEventListener('pointermove', (e) => {
    if (down) {
      const dx = e.clientX - down.x, dy = e.clientY - down.y;
      if (Math.hypot(dx, dy) > 6) down.moved = true;
      cam.oy = down.oy + dx * 0.005; cam.op = down.op + dy * 0.004; limit(); kick();
    } else if (st.pick) {
      const h = seatAt(e);
      if (h !== st.hover) { st.hover = h; label.textContent = h >= 0 ? `Seat ${h + 1}` : ''; canvas.style.cursor = h >= 0 ? 'pointer' : 'grab'; placePeople(); kick(600); }
    }
  });
  const end = (e) => {
    if (down && !down.moved && st.pick && e.type === 'pointerup') { const h = seatAt(e); if (h >= 0 && api.onPick) api.onPick(h); }
    down = null; dragging = false;
  };
  canvas.addEventListener('pointerup', end); canvas.addEventListener('pointercancel', end);
  host.addEventListener('keydown', (e) => {
    const m = { ArrowLeft: [0.18, 0], ArrowRight: [-0.18, 0], ArrowUp: [0, 0.1], ArrowDown: [0, -0.1] }[e.key];
    if (!m) return;
    e.preventDefault(); cam.oy += m[0]; cam.op += m[1]; limit(); kick();
  });

  const api = {
    onPick: null,
    canvas,
    set(next) {
      const moved = next.speaker !== st.speaker || next.mode !== st.mode || next.seat !== st.seat;
      Object.assign(st, next);
      if (!st.pick) { st.hover = -1; label.textContent = ''; }
      if (moved) { cam.oy = 0; cam.op = 0; }
      canvas.style.cursor = 'grab';
      drawScreen(next.item, next.who);
      placePeople(); aim(); kick();
    },
    resize,
  };
  resize();
  return api;
}
