// Procedural textures: facade atlas, slate, ground, signs. All drawn on canvas, no image files.
import * as THREE from 'three';

const T = 256, COLS = 8, ROWS = 8;
// Row 0 and 1: shopfronts and special tiles. Rows 2 to 6: one row per wall material.
export const SHOP_VARIANTS = 10;
export const TILE = { SHOP: 0, CIVIC_DOOR: 10, CHURCH: 11, SHED: 12, SHED_DOOR: 13, FIRE_DOOR: 14, GLASS: 15, PUB_DOOR: 56, PUB_WIN: 57 };
export const MATERIALS = ['stone', 'brick_red', 'brick_brown', 'render', 'ashlar'];
export const PART = { BLANK: 0, UPPER: 1, UPPER_B: 2, BACK: 3, HOUSE: 4, HOUSE_B: 5, TALL: 6, TALL_DOOR: 7 };
export const matTile = (m, part) => 16 + Math.max(0, MATERIALS.indexOf(m)) * 8 + part;

export function tileUV(i) {
  const c = i % COLS, r = Math.floor(i / COLS), e = 0.003;
  return [c / COLS + e, 1 - (r + 1) / ROWS + e, (c + 1) / COLS - e, 1 - r / ROWS - e];
}

export function rng(seed) {
  let a = seed >>> 0;
  return () => { a |= 0; a = (a + 0x6d2b79f5) | 0; let t = Math.imul(a ^ (a >>> 15), 1 | a); t = (t + Math.imul(t ^ (t >>> 7), 61 | t)) ^ t; return ((t ^ (t >>> 14)) >>> 0) / 4294967296; };
}
const rgb = (c, k = 1) => `rgb(${Math.min(255, c[0] * k) | 0},${Math.min(255, c[1] * k) | 0},${Math.min(255, c[2] * k) | 0})`;

function courses(g, R, base, course, minW, varW, mortar, spread) {
  g.fillStyle = mortar; g.fillRect(0, 0, T, T);
  for (let y = 0, row = 0; y < T; y += course, row++) {
    let x = varW ? -R() * 40 : (row % 2 ? -minW / 2 : 0);
    while (x < T) {
      const bw = minW + R() * varW;
      g.fillStyle = rgb(base, 1 - spread / 2 + R() * spread);
      g.fillRect(x + 1, y + 1, bw - 1.5, course - 1.5);
      x += bw;
    }
  }
}
function grime(g) {
  const gr = g.createLinearGradient(0, 0, 0, T);
  gr.addColorStop(0, 'rgba(40,34,28,.16)'); gr.addColorStop(0.5, 'rgba(40,34,28,0)'); gr.addColorStop(1, 'rgba(40,34,28,.14)');
  g.fillStyle = gr; g.fillRect(0, 0, T, T);
}
// Wall backgrounds. Colours are taken by eye from photos of Burnley Road (see data/photos/credits.json).
const WALL = {
  stone(g, R) { courses(g, R, [178, 156, 118], 18, 28, 34, 'rgb(112,98,76)', 0.3); grime(g); },
  brick_red(g, R) { courses(g, R, [150, 66, 50], 9, 15, 0, 'rgb(176,158,140)', 0.28); grime(g); },
  brick_brown(g, R) { courses(g, R, [124, 88, 64], 9, 15, 0, 'rgb(150,136,120)', 0.26); grime(g); },
  render(g, R) {
    g.fillStyle = '#ded6c2'; g.fillRect(0, 0, T, T);
    for (let i = 0; i < 700; i++) { g.fillStyle = `rgba(90,80,60,${R() * 0.06})`; g.fillRect(R() * T, R() * T, 3, 3); }
    g.fillStyle = 'rgba(60,54,46,.10)'; g.fillRect(0, 0, T, 6);
    grime(g);
  },
  ashlar(g, R) { courses(g, R, [206, 186, 146], 32, 64, 0, 'rgb(150,134,104)', 0.12); grime(g); },
};
const STONE_TRIM = '#d6c9a8';
function glass(g, x, y, w, h) {
  const gr = g.createLinearGradient(x, y, x + w * 0.4, y + h);
  gr.addColorStop(0, '#6d879e'); gr.addColorStop(0.45, '#3a4a5c'); gr.addColorStop(1, '#222b36');
  g.fillStyle = gr; g.fillRect(x, y, w, h);
  g.fillStyle = 'rgba(255,255,255,.10)';
  g.beginPath(); g.moveTo(x + w * 0.15, y); g.lineTo(x + w * 0.45, y); g.lineTo(x + w * 0.1, y + h); g.lineTo(x, y + h); g.lineTo(x, y + h * 0.5); g.fill();
}
// style: 'stone' = full stone surround, 'brick' = stone lintel and sill only, 'plain' = frame only
function win(g, x, y, w, h, o = {}) {
  const st = o.style || 'stone';
  if (st === 'stone') { g.fillStyle = STONE_TRIM; g.fillRect(x - 7, y - 11, w + 14, h + 18); }
  if (st === 'brick') { g.fillStyle = STONE_TRIM; g.fillRect(x - 9, y - 13, w + 18, 12); }
  g.fillStyle = '#ddd1b4'; g.fillRect(x - 11, y + h + 4, w + 22, 7);
  g.fillStyle = 'rgba(0,0,0,.25)'; g.fillRect(x - 11, y + h + 11, w + 22, 3);
  glass(g, x, y, w, h);
  if (o.curtain) { g.fillStyle = 'rgba(235,225,205,.5)'; g.fillRect(x, y, w * 0.22, h); g.fillRect(x + w * 0.78, y, w * 0.22, h); }
  g.strokeStyle = '#f3efe4'; g.lineWidth = 5; g.strokeRect(x + 2, y + 2, w - 4, h - 4);
  const cols = o.cols || (w > 40 ? 2 : 1), rows = o.rows || 2;
  g.lineWidth = o.cols ? 2.5 : 3.5; g.beginPath();
  for (let i = 1; i < rows; i++) { g.moveTo(x, y + (h * i) / rows); g.lineTo(x + w, y + (h * i) / rows); }
  for (let i = 1; i < cols; i++) { g.moveTo(x + (w * i) / cols, y); g.lineTo(x + (w * i) / cols, y + h); }
  g.stroke();
}
function door(g, x, y, w, h, col, st = 'stone') {
  if (st !== 'plain') { g.fillStyle = STONE_TRIM; g.fillRect(x - 8, y - 10, w + 16, h + 10); }
  glass(g, x, y, w, 24);
  g.fillStyle = col; g.fillRect(x, y + 28, w, h - 28);
  g.strokeStyle = 'rgba(0,0,0,.35)'; g.lineWidth = 2;
  const pw = (w - 18) / 2, ph = (h - 28 - 24) / 2;
  for (let i = 0; i < 2; i++) for (let j = 0; j < 2; j++) g.strokeRect(x + 6 + i * (pw + 6), y + 34 + j * (ph + 6), pw, ph);
  g.fillStyle = '#d9b44a'; g.beginPath(); g.arc(x + w - 9, y + 28 + (h - 28) * 0.52, 3.2, 0, 7); g.fill();
  g.fillRect(x + w / 2 - 9, y + 28 + (h - 28) * 0.55, 18, 4);
  g.fillStyle = '#c3b79c'; g.fillRect(x - 14, y + h - 7, w + 28, 7);
}
// Shop names are generic trades, not real businesses.
const SHOPS = [['#1f3d2b', 'BAKERY', 0], ['#a3231f', 'NEWSAGENT', 1], ['#1d2f4f', 'BARBER', 0], ['#f1eee6', 'CHEMIST', 1], ['#26282c', 'CAFE', 0], ['#1e7f86', 'FLORIST', 1], ['#e0a21c', 'HARDWARE', 1], ['#4a2a55', 'CHIPPY', 0], ['#7a1f2a', 'BUTCHER', 0], ['#2b5ea8', 'CHARITY SHOP', 1]];
function shop(g, R, [col, name, sans]) {
  WALL.stone(g, R);
  const light = col === '#f1eee6' || col === '#e0a21c';
  g.fillStyle = '#d9cdb0'; g.fillRect(0, 4, T, 10);
  g.fillStyle = col; g.fillRect(0, 14, T, 46);
  g.fillStyle = 'rgba(255,255,255,.12)'; g.fillRect(0, 14, T, 4);
  g.fillStyle = 'rgba(0,0,0,.28)'; g.fillRect(0, 56, T, 4);
  g.fillStyle = light ? '#22252a' : '#f4efe2'; g.textAlign = 'center'; g.textBaseline = 'middle';
  let size = 32;
  do { g.font = sans ? `800 ${size}px system-ui, Arial, sans-serif` : `700 ${size}px Georgia, 'Times New Roman', serif`; size -= 2; } while (g.measureText(name).width > 228);
  g.fillText(name, T / 2, 37);
  const frame = light ? '#3a3d42' : col;
  g.fillStyle = frame; g.fillRect(0, 60, 16, T - 60); g.fillRect(T - 16, 60, 16, T - 60); g.fillRect(16, 206, 158, 50);
  g.fillStyle = 'rgba(0,0,0,.22)'; g.fillRect(0, 60, 16, T - 60); g.fillRect(T - 16, 60, 16, T - 60);
  g.strokeStyle = 'rgba(0,0,0,.3)'; g.lineWidth = 2; g.strokeRect(26, 214, 138, 34);
  glass(g, 20, 66, 150, 140);
  g.fillStyle = 'rgba(245,235,200,.18)'; g.fillRect(20, 146, 150, 60);
  for (let i = 0; i < 5; i++) { g.fillStyle = ['#c8553d', '#e9c46a', '#5b8e7d', '#e8e4d8', '#7a8fb8'][i]; g.fillRect(30 + i * 28, 176 - (i % 2) * 10, 20, 30 + (i % 2) * 10); }
  g.strokeStyle = frame; g.lineWidth = 5; g.strokeRect(20, 66, 150, 140);
  g.fillStyle = '#1b2026'; g.fillRect(176, 66, 64, 190);
  glass(g, 184, 74, 48, 130);
  g.strokeStyle = frame; g.lineWidth = 6; g.strokeRect(184, 74, 48, 176);
  g.fillStyle = frame; g.fillRect(184, 204, 48, 46);
  g.fillStyle = '#d9b44a'; g.fillRect(188, 150, 4, 26);
}
function archDoor(g) {
  g.fillStyle = '#ddd2b6'; g.beginPath(); g.moveTo(58, T); g.lineTo(58, 100); g.arc(128, 100, 70, Math.PI, 0); g.lineTo(198, T); g.fill();
  g.save(); g.beginPath(); g.moveTo(70, T); g.lineTo(70, 100); g.arc(128, 100, 58, Math.PI, 0); g.lineTo(186, T); g.clip();
  glass(g, 70, 40, 116, 62); g.fillStyle = '#3d2a1c'; g.fillRect(70, 104, 116, 152); g.restore();
  g.strokeStyle = 'rgba(0,0,0,.4)'; g.lineWidth = 3; g.beginPath(); g.moveTo(128, 104); g.lineTo(128, T); g.stroke();
  g.strokeRect(80, 116, 38, 54); g.strokeRect(138, 116, 38, 54); g.strokeRect(80, 182, 38, 62); g.strokeRect(138, 182, 38, 62);
  g.fillStyle = '#d9b44a'; g.beginPath(); g.arc(120, 180, 4, 0, 7); g.arc(136, 180, 4, 0, 7); g.fill();
}

export function facadeAtlas() {
  const cv = document.createElement('canvas');
  cv.width = T * COLS; cv.height = T * ROWS;
  const g = cv.getContext('2d');
  const R = rng(42);
  const tile = (i, fn) => { g.save(); g.translate((i % COLS) * T, Math.floor(i / COLS) * T); g.beginPath(); g.rect(0, 0, T, T); g.clip(); fn(); g.restore(); };

  SHOPS.forEach((s, i) => tile(TILE.SHOP + i, () => shop(g, R, s)));
  tile(TILE.CIVIC_DOOR, () => { WALL.ashlar(g, R); archDoor(g); g.fillStyle = '#ddd2b6'; g.fillRect(0, 0, T, 12); });
  tile(TILE.CHURCH, () => {
    courses(g, R, [150, 138, 116], 22, 30, 40, 'rgb(96,88,74)', 0.26);
    g.fillStyle = '#cdbf9f'; g.beginPath(); g.moveTo(88, 226); g.lineTo(88, 100); g.quadraticCurveTo(92, 44, 128, 26); g.quadraticCurveTo(164, 44, 168, 100); g.lineTo(168, 226); g.fill();
    g.save(); g.beginPath(); g.moveTo(98, 218); g.lineTo(98, 102); g.quadraticCurveTo(102, 58, 128, 42); g.quadraticCurveTo(154, 58, 158, 102); g.lineTo(158, 218); g.clip();
    const gr = g.createLinearGradient(0, 40, 0, 220); gr.addColorStop(0, '#6a5a8a'); gr.addColorStop(0.5, '#3d5a78'); gr.addColorStop(1, '#27313d'); g.fillStyle = gr; g.fillRect(90, 30, 80, 200); g.restore();
    g.strokeStyle = '#cdbf9f'; g.lineWidth = 4; g.beginPath(); g.moveTo(128, 44); g.lineTo(128, 218); g.stroke();
  });
  const cladding = () => { g.fillStyle = '#9aa3a8'; g.fillRect(0, 0, T, T); for (let x = 0; x < T; x += 12) { g.fillStyle = 'rgba(0,0,0,.10)'; g.fillRect(x, 0, 3, T); g.fillStyle = 'rgba(255,255,255,.10)'; g.fillRect(x + 6, 0, 2, T); } g.fillStyle = '#5f676c'; g.fillRect(0, 0, T, 10); };
  tile(TILE.SHED, () => { cladding(); g.save(); g.translate(0, 196); g.beginPath(); g.rect(0, 0, T, 60); g.clip(); WALL.brick_red(g, R); g.restore(); });
  tile(TILE.SHED_DOOR, () => { cladding(); g.fillStyle = '#3e5d7a'; g.fillRect(40, 60, 176, 196); for (let y = 64; y < T; y += 10) { g.fillStyle = 'rgba(0,0,0,.18)'; g.fillRect(40, y, 176, 3); } });
  tile(TILE.FIRE_DOOR, () => {
    WALL.brick_brown(g, R);
    g.fillStyle = '#e8e4da'; g.fillRect(0, 0, T, 26);
    g.fillStyle = '#c9423e'; g.fillRect(10, 30, 236, 226);
    for (let x = 10; x < 246; x += 29.5) { g.fillStyle = 'rgba(0,0,0,.22)'; g.fillRect(x, 30, 3, 226); g.fillStyle = '#2a2f36'; g.fillRect(x + 11, 150, 7, 22); g.fillRect(x + 11, 186, 7, 22); }
  });
  tile(TILE.GLASS, () => { g.fillStyle = '#5b5e62'; g.fillRect(0, 0, T, T); for (let i = 0; i < 3; i++) glass(g, 6 + i * 84, 30, 76, 220); g.fillStyle = '#8a6a4a'; g.fillRect(0, 0, T, 26); });

  MATERIALS.forEach((m, mi) => {
    const wall = () => WALL[m](g, R), base = 16 + mi * 8;
    const st = m === 'stone' || m === 'ashlar' ? 'stone' : m === 'render' ? 'plain' : 'brick';
    const doors = m === 'render' ? ['#1b1b1d', '#234d32'] : ['#7a1f1f', '#1f3557'];
    tile(base + PART.BLANK, wall);
    tile(base + PART.UPPER, () => { wall(); win(g, 36, 78, 52, 104, { curtain: true, style: st }); win(g, 134, 78, 84, 104, { curtain: true, style: st }); });
    tile(base + PART.UPPER_B, () => { wall(); win(g, 92, 78, 76, 104, { style: st }); });
    tile(base + PART.BACK, () => { wall(); win(g, 96, 96, 66, 84, { style: st }); g.fillStyle = '#3a3d40'; g.fillRect(216, 0, 7, T); });
    tile(base + PART.HOUSE, () => { wall(); door(g, 30, 70, 56, 186, doors[0], st); win(g, 132, 84, 88, 110, { curtain: true, style: st }); });
    tile(base + PART.HOUSE_B, () => { wall(); door(g, 170, 70, 56, 186, doors[1], st); win(g, 36, 84, 88, 110, { curtain: true, style: st }); });
    tile(base + PART.TALL, () => { wall(); win(g, 86, 50, 84, 164, { style: st, cols: 3, rows: 4 }); if (m !== 'render') { g.fillStyle = STONE_TRIM; g.fillRect(0, 236, T, 20); } });
    tile(base + PART.TALL_DOOR, () => { wall(); archDoor(g); });
  });

  // Pub ground floor, after photos of the Starkie Arms and Flying Dutchman: white door surround and white framed windows.
  tile(TILE.PUB_DOOR, () => {
    WALL.stone(g, R);
    g.fillStyle = '#f1eee6'; g.fillRect(74, 52, 108, 204); g.fillRect(66, 44, 124, 16);
    g.fillStyle = 'rgba(0,0,0,.12)'; for (const x of [80, 92, 164, 176]) g.fillRect(x, 64, 3, 186);
    glass(g, 100, 66, 56, 26);
    g.fillStyle = '#2a2126'; g.fillRect(100, 96, 56, 160);
    g.strokeStyle = 'rgba(255,255,255,.18)'; g.lineWidth = 2; g.strokeRect(107, 104, 42, 60); g.strokeRect(107, 174, 42, 70);
    g.fillStyle = '#d9b44a'; g.beginPath(); g.arc(148, 176, 3.5, 0, 7); g.fill();
    g.fillStyle = '#c3b79c'; g.fillRect(60, 248, 136, 8);
  });
  tile(TILE.PUB_WIN, () => {
    WALL.stone(g, R);
    g.fillStyle = '#f1eee6'; g.fillRect(58, 84, 140, 124);
    for (const x of [66, 132]) { glass(g, x, 92, 58, 108); g.strokeStyle = '#f7f4ec'; g.lineWidth = 4; g.strokeRect(x + 2, 94, 54, 104); g.beginPath(); g.moveTo(x, 146); g.lineTo(x + 58, 146); g.stroke(); }
    g.fillStyle = '#ddd1b4'; g.fillRect(52, 208, 152, 8);
  });

  const tex = new THREE.CanvasTexture(cv);
  tex.colorSpace = THREE.SRGBColorSpace;
  tex.anisotropy = 8;
  return tex;
}

export function unionTexture() {
  const cv = document.createElement('canvas'); cv.width = 120; cv.height = 72;
  const g = cv.getContext('2d');
  g.fillStyle = '#1f3a8a'; g.fillRect(0, 0, 120, 72);
  g.lineCap = 'butt';
  for (const [w, c] of [[15, '#fff'], [6, '#c8102e']]) { g.strokeStyle = c; g.lineWidth = w; g.beginPath(); g.moveTo(0, 0); g.lineTo(120, 72); g.moveTo(120, 0); g.lineTo(0, 72); g.stroke(); }
  g.fillStyle = '#fff'; g.fillRect(48, 0, 24, 72); g.fillRect(0, 24, 120, 24);
  g.fillStyle = '#c8102e'; g.fillRect(53, 0, 14, 72); g.fillRect(0, 29, 120, 14);
  const tex = new THREE.CanvasTexture(cv);
  tex.colorSpace = THREE.SRGBColorSpace; tex.anisotropy = 4;
  return tex;
}

function repeating(cv, rep) {
  const tex = new THREE.CanvasTexture(cv);
  tex.wrapS = tex.wrapT = THREE.RepeatWrapping;
  tex.colorSpace = THREE.SRGBColorSpace;
  tex.anisotropy = 4;
  if (rep) tex.repeat.set(rep, rep);
  return tex;
}
export function slateTexture() {
  const cv = document.createElement('canvas'); cv.width = cv.height = 128;
  const g = cv.getContext('2d'), R = rng(9);
  g.fillStyle = '#3c4046'; g.fillRect(0, 0, 128, 128);
  for (let y = 0; y < 128; y += 16) for (let x = (y / 16) % 2 ? -8 : 0; x < 128; x += 16) {
    const k = 0.8 + R() * 0.4;
    g.fillStyle = `rgb(${92 * k | 0},${98 * k | 0},${108 * k | 0})`;
    g.fillRect(x + 1, y + 1, 14.5, 14);
  }
  return repeating(cv);
}
export function groundTexture() {
  const cv = document.createElement('canvas'); cv.width = cv.height = 256;
  const g = cv.getContext('2d'), R = rng(5);
  g.fillStyle = '#84936c'; g.fillRect(0, 0, 256, 256);
  for (let i = 0; i < 2600; i++) {
    const k = R();
    g.fillStyle = k < 0.5 ? `rgba(60,84,44,${0.10 + R() * 0.18})` : `rgba(170,176,120,${0.08 + R() * 0.14})`;
    g.fillRect(R() * 256, R() * 256, 2 + R() * 5, 2 + R() * 5);
  }
  return repeating(cv, 1600);
}
export function signTexture(text, bg = '#1f3d2b', fg = '#ecc866') {
  const cv = document.createElement('canvas'); cv.width = 512; cv.height = 72;
  const g = cv.getContext('2d');
  g.fillStyle = bg; g.fillRect(0, 0, 512, 72);
  g.strokeStyle = fg; g.lineWidth = 3; g.strokeRect(6, 6, 500, 60);
  let size = 44;
  do { g.font = `700 ${size}px Georgia, 'Times New Roman', serif`; size -= 2; } while (g.measureText(text).width > 470 && size > 14);
  g.fillStyle = fg; g.textAlign = 'center'; g.textBaseline = 'middle';
  g.fillText(text, 256, 38);
  const tex = new THREE.CanvasTexture(cv);
  tex.colorSpace = THREE.SRGBColorSpace; tex.anisotropy = 8;
  return tex;
}
export function closedTexture() {
  const cv = document.createElement('canvas'); cv.width = 512; cv.height = 256;
  const g = cv.getContext('2d');
  g.fillStyle = '#c8201c'; g.fillRect(0, 0, 512, 256);
  g.strokeStyle = '#fff'; g.lineWidth = 10; g.strokeRect(12, 12, 488, 232);
  g.fillStyle = '#fff'; g.textAlign = 'center';
  g.font = '800 76px system-ui, sans-serif'; g.fillText('ROAD CLOSED', 256, 116);
  g.font = '700 34px system-ui, sans-serif'; g.fillText('More of Padiham', 256, 172); g.fillText('coming soon', 256, 212);
  const tex = new THREE.CanvasTexture(cv);
  tex.colorSpace = THREE.SRGBColorSpace; tex.anisotropy = 8;
  return tex;
}
export function cloudTexture() {
  const cv = document.createElement('canvas'); cv.width = 256; cv.height = 128;
  const g = cv.getContext('2d'), R = rng(3);
  for (let i = 0; i < 14; i++) {
    const x = 50 + R() * 156, y = 50 + R() * 34, r = 22 + R() * 30;
    const gr = g.createRadialGradient(x, y, 0, x, y, r);
    gr.addColorStop(0, 'rgba(255,255,255,.75)'); gr.addColorStop(1, 'rgba(255,255,255,0)');
    g.fillStyle = gr; g.fillRect(x - r, y - r, r * 2, r * 2);
  }
  return new THREE.CanvasTexture(cv);
}
