// The Padiham Job: a third-person walking game on the real street layout of Padiham.
// Map data (c) OpenStreetMap contributors, ODbL. Building outlines contain OS data (c) Crown copyright
// and database right 2026. The world file is built by scripts/padiham-game/build_world.py.
import * as THREE from 'three';
import { chunks, buildWorld, walls, roadGrid, riverGrid, distToSeg, nearWall } from './city.js';
import { cloudTexture } from './textures.js';
import { setTerrain, groundH } from './terrain.js';
import { makeFigure, animateFigure, Pedestrians, Pigeons, SKIN, HAIR } from './life.js';
import { Sound } from './audio.js';
import { buildPub } from './interior.js';
import { PLAQUES } from './history.js';

const $ = (id) => document.getElementById('sop-' + id);
const stage = $('stage');
const isTouch = matchMedia('(pointer: coarse)').matches || 'ontouchstart' in window;
if (isTouch) stage.classList.add('touch');

// ---------- Jobs (fiction, for fun) ----------
const SCRIPT = [
  { title: 'First steps', steps: [['Padiham Fire Station', 'Walk to the glowing beam by the Fire Station']] },
  { title: 'Paint job', steps: [['PaintWell', 'Pick up two tins of paint from PaintWell'], ['Hand & Shuttle', 'Carry the paint up to the Hand & Shuttle']] },
  { title: 'Quiz night', steps: [['Padiham Town Hall', 'Collect the quiz sheets from the Town Hall'], ['Starkie Arms', 'Get them up to the Starkie Arms before quiz night starts']] },
  { title: 'Pie and peas', steps: [["Bertwistle's Bakery", "Collect a tray of pies from Bertwistle's Bakery"], ['Kings Arms', 'Take the pies round to the Kings Arms']] },
  { title: 'Lost phone', steps: [['Padiham Mobile Phone & Computers iFix4U', 'Collect the mended phone from iFix4U'], ['Flying Dutchman', 'Its owner is waiting at the Flying Dutchman']] },
  { title: 'Spare part', steps: [['KD Motorcycles', 'Pick up a spare part from KD Motorcycles'], ['Padiham Bridge', 'Meet your mate with the broken bike on Padiham Bridge']] },
  { title: 'Bunting', steps: [['Boyes', 'Buy another roll of bunting from Boyes'], ['Padiham Town Hall', 'Get the bunting to the Town Hall']] },
  { title: 'Match day', steps: [['Padiham Town Hall', 'Pick up the raffle prizes from the Town Hall'], ['Padiham Football Club', 'Get them to the Arbories before kick off']] },
];
const CARGO = ['a parcel', 'a toolbox', 'a bunch of flowers', 'a box of teacakes', 'a birthday cake', 'a set of keys', 'a bag of shopping'];
// Good turns: something small carried to a neighbour's street. They pay nothing but thanks.
const KINDNESS = ['a hot dinner', 'a bag of shopping', 'a get well card', 'a bunch of flowers', 'the morning paper', 'a flask of soup', 'a borrowed ladder'];
const THANKS = ['That made their day', 'The kettle is on if you want a brew', 'They were made up with it', 'You are a star, they said', 'They waved you off from the step'];
const HELLOS = ['Ey up!', 'Morning!', 'Hiya', 'Alright?', 'Nice day for it', 'How do', 'Now then'];

// ---------- State ----------
const SAVE_KEY = 'padiham-errands-2';
const save = (() => { try { return JSON.parse(localStorage.getItem(SAVE_KEY) || '{}'); } catch { return {}; } })();
const list = (v) => new Set(Array.isArray(v) ? v : []);
const state = { money: save.money || 0, done: save.done || 0, jobIndex: save.jobIndex || 0, pigeons: save.pigeons || 0, plaques: list(save.plaques),
  litter: save.litter || 0, hellos: save.hellos || 0, turns: save.turns || 0, metres: save.metres || 0, dailies: save.dailies || 0, pubs: list(save.pubs), stamps: list(save.stamps),
  job: null, playing: false, inside: null };
function persist() {
  try { localStorage.setItem(SAVE_KEY, JSON.stringify({ money: state.money, done: state.done, jobIndex: state.jobIndex, pigeons: state.pigeons, plaques: [...state.plaques], litter: state.litter, hellos: state.hellos, turns: state.turns, metres: Math.round(state.metres), dailies: state.dailies, pubs: [...state.pubs], stamps: [...state.stamps] })); } catch { /* private mode */ }
}
// Link options for supporters and public bodies, read once from the address. Nothing is stored or sent.
// ?calm=1 gives a quieter town: fewer passers-by and softer sound. ?edition=clean leaves out the selfie
// taker and keeps pub doors shut. Both can be combined: ?calm=1&edition=clean
const opts = new URLSearchParams(location.search);
// ?route=12_-45.130_20 is a practice route set by a supporter: a start and up to eleven markers, each a pair of
// whole metres on the game's own grid. The route is all there is in the link: no name, no words, nothing kept.
const ROUTE = (() => {
  const rawRoute = opts.get('route');
  if (!rawRoute || !/^-?\d+_-?\d+(\.-?\d+_-?\d+){1,11}$/.test(rawRoute)) return null;
  return { key: rawRoute, pts: rawRoute.split('.').map((q) => q.split('_').map(Number)) };
})();
// A practice route is always the plain version: no selfie taker and no pubs.
const CLEAN = opts.get('edition') === 'clean' || !!ROUTE;
if (ROUTE) stage.classList.add('practice');

// ---------- Comfort and access settings ----------
// Kept in the browser, and only the ones the player has changed: the rest follow the device (reduced motion,
// more contrast) or the calm link. S.text is 0, 1 or 2 for standard, large and largest panel text.
const SET_KEY = 'padiham-settings-1';
const mq = (q) => { try { return matchMedia(q).matches; } catch { return false; } };
const S = { calm: opts.get('calm') === '1' || !!ROUTE, text: 0, reduce: mq('(prefers-reduced-motion: reduce)'), contrast: mq('(prefers-contrast: more)'), hold: false, follow: true, lefty: false };
const chosen = (() => { try { return JSON.parse(localStorage.getItem(SET_KEY) || '{}') || {}; } catch { return {}; } })();
for (const k in S) if (typeof chosen[k] === typeof S[k] && !(k === 'calm' && S.calm)) S[k] = chosen[k];
if (![0, 1, 2].includes(S.text)) S.text = 0;
const sound = new Sound(S.calm ? 0.4 : 1);
function applySettings() {
  const c = stage.classList;
  c.toggle('t1', S.text === 1); c.toggle('t2', S.text === 2);
  c.toggle('hc', S.contrast); c.toggle('still', S.reduce); c.toggle('hold', S.hold); c.toggle('lefty', S.lefty);
  sound.setLevel(S.calm ? 0.4 : 1);
  if (peds) peds.setCount(S.calm ? 6 : Infinity);
  if (S.calm) $('timer').textContent = '';
  for (const i of stage.querySelectorAll('input[data-set]')) i.checked = !!S[i.dataset.set];
  for (const b of stage.querySelectorAll('button[data-text]')) b.setAttribute('aria-pressed', String(Number(b.dataset.text) === S.text));
}
function setSetting(k, v) {
  S[k] = v; chosen[k] = v;
  try { localStorage.setItem(SET_KEY, JSON.stringify(chosen)); } catch { /* private mode */ }
  applySettings();
}
// On a practice route calm is fixed, so its switch is put away.
if (ROUTE) for (const i of stage.querySelectorAll('input[data-set="calm"]')) i.closest('label').hidden = true;
for (const i of stage.querySelectorAll('input[data-set]')) i.addEventListener('change', () => setSetting(i.dataset.set, i.checked));
for (const b of stage.querySelectorAll('button[data-text]')) b.addEventListener('click', () => setSetting('text', Number(b.dataset.text)));

// ---------- Renderer, sky and light ----------
let renderer;
try { renderer = new THREE.WebGLRenderer({ canvas: $('view'), antialias: true, powerPreference: 'high-performance' }); } catch (err) {
  // No WebGL (old device, or 3D graphics switched off): say so instead of loading for ever.
  $('jobTitle').textContent = 'Could not start the game';
  $('jobText').textContent = 'This browser or device cannot show 3D graphics';
  $('play').textContent = 'Not available here';
  throw err;
}
renderer.setPixelRatio(Math.min(devicePixelRatio || 1, isTouch ? 1.75 : 2));
renderer.shadowMap.enabled = true;
renderer.shadowMap.type = THREE.PCFShadowMap; // the soft kind costs far more and the sun here is hard daylight
const scene = new THREE.Scene();
const HORIZON = 0xd3e2ec;
scene.fog = new THREE.Fog(HORIZON, 140, 900);
const camera = new THREE.PerspectiveCamera(62, 1, 0.4, 2400);
scene.add(new THREE.HemisphereLight(0xdfeeff, 0x9a8f7a, 1.7));
const sunDir = new THREE.Vector3(-0.5, 0.78, 0.37).normalize();
const sun = new THREE.DirectionalLight(0xfff0d6, 2.3);
sun.castShadow = true;
sun.shadow.mapSize.set(isTouch ? 1024 : 2048, isTouch ? 1024 : 2048);
Object.assign(sun.shadow.camera, { left: -75, right: 75, top: 75, bottom: -75, near: 1, far: 420 });
sun.shadow.bias = -0.0004; sun.shadow.normalBias = 0.06;
scene.add(sun, sun.target);

const raw = (hex) => new THREE.Color().setHex(hex, THREE.LinearSRGBColorSpace);
const sky = new THREE.Mesh(new THREE.SphereGeometry(1900, 24, 12), new THREE.ShaderMaterial({
  side: THREE.BackSide, depthWrite: false, fog: false,
  uniforms: { zenith: { value: raw(0x4f8fd0) }, horizon: { value: raw(HORIZON) }, sunDir: { value: sunDir } },
  vertexShader: 'varying vec3 vDir; void main(){ vDir = normalize(position); gl_Position = projectionMatrix * modelViewMatrix * vec4(position, 1.0); }',
  fragmentShader: 'uniform vec3 zenith; uniform vec3 horizon; uniform vec3 sunDir; varying vec3 vDir; void main(){ vec3 d = normalize(vDir); float h = max(d.y, 0.0); vec3 c = mix(horizon, zenith, pow(h, 0.5)); float s = max(dot(d, sunDir), 0.0); c += vec3(1.0, 0.92, 0.75) * (pow(s, 600.0) * 1.5 + pow(s, 12.0) * 0.14); gl_FragColor = vec4(c, 1.0); }',
}));
sky.renderOrder = -2;
scene.add(sky);
const clouds = new THREE.Group();
{
  const tex = cloudTexture();
  for (let i = 0; i < 16; i++) {
    const sp = new THREE.Sprite(new THREE.SpriteMaterial({ map: tex, fog: false, depthWrite: false, opacity: 0.85, transparent: true }));
    const a = (i / 16) * Math.PI * 2 + Math.random(), r = 700 + Math.random() * 800;
    sp.position.set(Math.cos(a) * r, 230 + Math.random() * 260, Math.sin(a) * r);
    const s = 320 + Math.random() * 320; sp.scale.set(s, s * 0.45, 1);
    sp.renderOrder = -1; clouds.add(sp);
  }
  scene.add(clouds);
}
let baseFov = 62;
function resize() {
  const w = stage.clientWidth || 800, h = stage.clientHeight || 600;
  renderer.setSize(w, h, false);
  camera.aspect = w / h;
  baseFov = camera.fov = w < h ? 74 : 62;
  camera.updateProjectionMatrix();
}
new ResizeObserver(resize).observe(stage);
resize();
// The site header is fixed over the page. Size and park the game below it, so the whole panel fits on a phone.
const siteHeader = document.querySelector('.site-header');
function clearHeader() { stage.style.setProperty('--sop-top', `${siteHeader ? Math.ceil(siteHeader.getBoundingClientRect().height) : 0}px`); }
clearHeader();
addEventListener('resize', clearHeader);
// Draw nothing while the game is scrolled out of view.
let onScreen = true;
new IntersectionObserver(([e]) => { onScreen = e.isIntersecting; }).observe(stage);
// Full screen. An iPhone cannot put part of a page full screen, so there the game fills the window instead.
function setFill(on) { stage.classList.toggle('fill', on); document.documentElement.classList.toggle('sop-filled', on); clearHeader(); resize(); }
$('full').addEventListener('click', () => {
  if (stage.classList.contains('fill')) setFill(false);
  else if (document.fullscreenElement) document.exitFullscreen();
  else if (stage.requestFullscreen) stage.requestFullscreen().catch(() => setFill(true));
  else setFill(true);
});

// ---------- World ----------
let world, built, slice, peds, pigeons;
applySettings();
const pois = new Map();
function inPoly(x, z, p) {
  let inside = false;
  for (let i = 0, j = p.length - 1; i < p.length; j = i++) if ((p[i][1] > z) !== (p[j][1] > z) && x < ((p[j][0] - p[i][0]) * (z - p[i][1])) / (p[j][1] - p[i][1]) + p[i][0]) inside = !inside;
  return inside;
}
// Make sure a job marker is on open ground, not inside a building outline.
function safeDoor(p) {
  const blocked = (x, z) => world.buildings.some((b) => inPoly(x, z, b.p));
  if (!blocked(p.dx, p.dz)) return;
  let best = null, bd = 1e9;
  for (const r of world.roads) {
    if (r.c === 'foot' || r.c === 'service') continue;
    for (let i = 0; i < r.p.length - 1; i++) {
      const [d, qx, qz] = distToSeg(p.x, p.z, [r.p[i][0], r.p[i][1], r.p[i + 1][0], r.p[i + 1][1]]);
      if (d < bd && !blocked(qx, qz)) { bd = d; best = [qx, qz]; }
    }
  }
  if (best) { p.dx = best[0]; p.dz = best[1]; }
}
const labels = [];
function label(text, x, y, z) {
  const cv = document.createElement('canvas');
  cv.width = 512; cv.height = 96;
  const g = cv.getContext('2d');
  g.font = '700 44px system-ui, sans-serif';
  const w = Math.min(500, g.measureText(text).width + 40);
  g.fillStyle = 'rgba(20,22,26,.82)';
  g.beginPath(); g.roundRect((512 - w) / 2, 12, w, 72, 20); g.fill();
  g.fillStyle = '#f4f1e8'; g.textAlign = 'center'; g.textBaseline = 'middle';
  g.fillText(text, 256, 50, 470);
  const tex = new THREE.CanvasTexture(cv);
  tex.colorSpace = THREE.SRGBColorSpace;
  const sp = new THREE.Sprite(new THREE.SpriteMaterial({ map: tex, transparent: true, depthWrite: false, fog: false }));
  sp.scale.set(20, 3.75, 1);
  sp.position.set(x, y + groundH(x, z), z);
  scene.add(sp);
  labels.push(sp);
}

// ---------- Minimap ----------
const MS = 0.5;
let mapBase, mapOrigin;
function drawMapBase() {
  const b = world.bounds, pad = 150;
  mapOrigin = [b[0] - pad, b[1] - pad];
  mapBase = document.createElement('canvas');
  mapBase.width = (b[2] - b[0] + pad * 2) * MS;
  mapBase.height = (b[3] - b[1] + pad * 2) * MS;
  const g = mapBase.getContext('2d');
  g.fillStyle = '#8f987c'; g.fillRect(0, 0, mapBase.width, mapBase.height);
  g.setTransform(MS, 0, 0, MS, -mapOrigin[0] * MS, -mapOrigin[1] * MS);
  const poly = (p) => { g.beginPath(); p.forEach(([x, z], i) => (i ? g.lineTo(x, z) : g.moveTo(x, z))); };
  g.fillStyle = '#6fae5c'; for (const gr of world.green) { poly(gr.p); g.fill(); }
  g.lineCap = g.lineJoin = 'round';
  for (const wt of world.water) {
    if (wt.poly) { poly(wt.poly); g.fillStyle = '#4f86a6'; g.fill(); } else { poly(wt.p); g.strokeStyle = '#4f86a6'; g.lineWidth = wt.w; g.stroke(); }
  }
  for (const pass of ['foot', 'road']) for (const r of world.roads) {
    if ((r.c === 'foot') !== (pass === 'foot')) continue;
    poly(r.p); g.strokeStyle = pass === 'foot' ? '#d8ccb0' : '#f4f1e8'; g.lineWidth = Math.max(r.w, 4); g.stroke();
  }
  g.fillStyle = '#5a5148'; for (const bd of world.buildings) { poly(bd.p); g.closePath(); g.fill(); }
  g.strokeStyle = '#c8201c'; g.lineWidth = 3; g.setLineDash([8, 6]);
  g.strokeRect(slice[0], slice[1], slice[2] - slice[0], slice[3] - slice[1]);
}
const mini = $('mini').getContext('2d');
function drawMini() {
  const S = 272, zoom = 1.9;
  mini.setTransform(1, 0, 0, 1, 0, 0);
  mini.fillStyle = '#8f987c'; mini.fillRect(0, 0, S, S);
  const sx = (player.x - mapOrigin[0]) * MS, sz = (player.z - mapOrigin[1]) * MS;
  mini.setTransform(zoom, 0, 0, zoom, S / 2 - sx * zoom, S / 2 - sz * zoom);
  mini.drawImage(mapBase, 0, 0);
  mini.setTransform(1, 0, 0, 1, 0, 0);
  mini.fillStyle = '#b8f26a';
  for (const l of litter) {
    const lx2 = (l.x - player.x) * MS * zoom, lz2 = (l.z - player.z) * MS * zoom;
    if (l.mesh.visible && Math.hypot(lx2, lz2) < S / 2) { mini.beginPath(); mini.arc(S / 2 + lx2, S / 2 + lz2, 4, 0, 7); mini.fill(); }
  }
  if (state.job && state.job.route) {
    mini.strokeStyle = '#ffcf33'; mini.lineWidth = 5; mini.setLineDash([10, 8]); mini.beginPath(); mini.moveTo(S / 2, S / 2);
    for (const st of state.job.steps.slice(state.job.i)) mini.lineTo(S / 2 + (st.poi.dx - player.x) * MS * zoom, S / 2 + (st.poi.dz - player.z) * MS * zoom);
    mini.stroke(); mini.setLineDash([]);
  }
  const t = target();
  if (t) {
    let dx = (t.dx - player.x) * MS * zoom, dz = (t.dz - player.z) * MS * zoom;
    const d = Math.hypot(dx, dz), max = S / 2 - 14;
    if (d > max) { dx *= max / d; dz *= max / d; }
    mini.fillStyle = '#ffcf33'; mini.strokeStyle = '#14161a'; mini.lineWidth = 4;
    mini.beginPath(); mini.arc(S / 2 + dx, S / 2 + dz, 11, 0, 7); mini.fill(); mini.stroke();
  }
  mini.translate(S / 2, S / 2);
  mini.rotate(-player.heading + Math.PI);
  mini.fillStyle = '#ff5a3c'; mini.strokeStyle = '#fff'; mini.lineWidth = 3;
  mini.beginPath(); mini.moveTo(0, -15); mini.lineTo(10, 11); mini.lineTo(0, 5); mini.lineTo(-10, 11); mini.closePath(); mini.fill(); mini.stroke();
}

// ---------- Player ----------
// The player picks a look on the start screen: hair, hair colour, skin and jacket. It is kept in the browser.
// The figure is a courier in a plain jacket with a backpack, with no name, number or badge.
const LOOK_KEY = 'padiham-look-1';
const LOOKS = {
  style: [['short', 'Short hair'], ['pony', 'Ponytail'], ['long', 'Long hair'], ['cap', 'Cap']],
  hair: HAIR.map((c, i) => [i, ['Dark brown', 'Black', 'Fair', 'Grey', 'Auburn'][i]]),
  skin: SKIN.map((c, i) => [i, `Skin tone ${i + 1}`]),
  jacket: [[0xf08a24, 'Orange'], [0x2f6fb5, 'Blue'], [0x2f8a5a, 'Green'], [0x8a3fa0, 'Purple'], [0x3a3d42, 'Charcoal']].map(([c, n], i) => [i, n, c]),
};
const JACKET = LOOKS.jacket.map((j) => j[2]);
const look = (() => {
  const d = { style: 'short', hair: 0, skin: 0, jacket: 0 };
  try { const v = JSON.parse(localStorage.getItem(LOOK_KEY) || '{}'); for (const k in d) if (LOOKS[k].some((o) => o[0] === v[k])) d[k] = v[k]; } catch { /* private mode */ }
  return d;
})();
const lookFigure = () => makeFigure({ jacket: JACKET[look.jacket], trousers: 0x2a2f3a, skin: SKIN[look.skin], hair: HAIR[look.hair], face: true, pack: true, style: look.style, cap: look.style === 'cap' ? 0x2b3a67 : undefined });
const player = { x: 0, z: 0, heading: 0, speed: 0, phase: 0, y: 0, vy: 0, wave: 0, fig: lookFigure() };
scene.add(player.fig.group);
const hex = (c) => '#' + c.toString(16).padStart(6, '0');
function drawLook() {
  const cv = document.getElementById('sop-lookPic');
  if (!cv) return;
  const g = cv.getContext('2d'), hair = hex(HAIR[look.hair]), skin = hex(SKIN[look.skin]);
  g.clearRect(0, 0, 120, 150);
  if (look.style === 'long') { g.fillStyle = hair; g.fillRect(31, 40, 58, 52); }
  if (look.style === 'pony') { g.fillStyle = hair; g.beginPath(); g.ellipse(93, 62, 9, 22, -0.3, 0, 7); g.fill(); }
  g.fillStyle = '#3a3f4a'; g.fillRect(28, 88, 64, 50);                       // backpack straps behind the jacket
  g.fillStyle = hex(JACKET[look.jacket]); g.beginPath(); g.roundRect(24, 84, 72, 66, 18); g.fill();
  g.fillStyle = '#3a3f4a'; g.fillRect(36, 88, 7, 62); g.fillRect(77, 88, 7, 62);
  g.fillStyle = skin; g.beginPath(); g.arc(60, 52, 30, 0, 7); g.fill();
  g.fillStyle = look.style === 'cap' ? '#2b3a67' : hair;
  g.beginPath(); g.arc(60, 48, 31, Math.PI * 1.02, Math.PI * 1.98); g.fill();
  if (look.style === 'cap') g.fillRect(52, 40, 46, 8);
  g.fillStyle = '#2b2f36'; g.beginPath(); g.arc(49, 56, 3.4, 0, 7); g.arc(71, 56, 3.4, 0, 7); g.fill();
  g.fillStyle = '#8a4a42'; g.fillRect(52, 68, 16, 3);
}
function setLook(k, v) {
  look[k] = v;
  try { localStorage.setItem(LOOK_KEY, JSON.stringify(look)); } catch { /* private mode */ }
  scene.remove(player.fig.group);
  player.fig = lookFigure();
  scene.add(player.fig.group);
  for (const b of document.querySelectorAll(`#sop-look button[data-k="${k}"]`)) b.setAttribute('aria-pressed', String(b.dataset.v === String(v)));
  drawLook();
}
{
  const host = document.getElementById('sop-lookOpts');
  if (host) {
    const NAMES = { style: 'Hair', hair: 'Hair colour', skin: 'Skin', jacket: 'Jacket' }, SWATCH = { hair: HAIR, skin: SKIN, jacket: JACKET };
    for (const k of Object.keys(LOOKS)) {
      const row = document.createElement('div'); row.className = 'row'; row.setAttribute('role', 'group'); row.setAttribute('aria-label', NAMES[k]);
      const lab = document.createElement('span'); lab.textContent = NAMES[k]; row.append(lab);
      for (const [v, name] of LOOKS[k]) {
        const b = document.createElement('button'); b.type = 'button'; b.dataset.k = k; b.dataset.v = String(v);
        b.setAttribute('aria-pressed', String(look[k] === v));
        if (SWATCH[k]) { b.className = 'dot'; b.style.background = hex(SWATCH[k][v]); b.title = name; b.setAttribute('aria-label', name); } else b.textContent = name;
        b.addEventListener('click', () => setLook(k, v));
        row.append(b);
      }
      host.append(row);
    }
    drawLook();
  }
}
const RADIUS = 0.5;
function inRiver(x, z) {
  for (const s of riverGrid.at(x, z)) {
    if (distToSeg(x, z, s)[0] < s[4] - 1) {
      for (const r of roadGrid.at(x, z)) if (distToSeg(x, z, r)[0] < r[4] + 2.8) return false; // on a bridge
      return true;
    }
  }
  return false;
}
function pushOut(o) {
  for (let it = 0; it < 3; it++) {
    let hit = false;
    for (const s of walls.at(o.x, o.z)) {
      const [d, qx, qz] = distToSeg(o.x, o.z, s);
      if (d < RADIUS && d > 1e-4) { o.x = qx + ((o.x - qx) / d) * RADIUS; o.z = qz + ((o.z - qz) / d) * RADIUS; hit = true; }
    }
    if (!hit) break;
  }
}
// Fraction along the segment P to Q at which the first tall wall is hit (1 if clear).
function wallHit(px, pz, qx, qz) {
  let best = 1;
  const seen = new Set();
  for (const f of [0, 0.5, 1]) for (const s of walls.at(px + (qx - px) * f, pz + (qz - pz) * f)) {
    if (s[4] || seen.has(s)) continue; seen.add(s);
    const rx = qx - px, rz = qz - pz, sx = s[2] - s[0], sz = s[3] - s[1];
    const den = rx * sz - rz * sx;
    if (Math.abs(den) < 1e-9) continue;
    const t = ((s[0] - px) * sz - (s[1] - pz) * sx) / den;
    const u = ((s[0] - px) * rz - (s[1] - pz) * rx) / den;
    if (t > 0 && t < best && u >= 0 && u <= 1) best = t;
  }
  return best;
}

// ---------- Input ----------
const keys = new Set();
const input = { x: 0, y: 0, gear: 0 };
addEventListener('keydown', (e) => {
  keys.add(e.code);
  // Keep the page still while playing, but leave the keys alone once the game is out of view or a control has focus.
  const onControl = e.target instanceof Element && e.target.closest('button, a, input, select, textarea, summary');
  if (!state.playing || !onScreen || onControl) return;
  if (e.code.startsWith('Arrow') || e.code === 'Space' || e.code.startsWith('Page')) e.preventDefault();
  if (e.repeat || e.ctrlKey || e.metaKey || e.altKey) return;
  if (e.code === 'KeyR') toggleRun();
  else if (e.code === 'Space') jump();
  else if (e.code === 'KeyH') hello();
  else if (e.code === 'KeyG') whereNext();
  else if (e.code === 'Enter') closeToast();
});
addEventListener('keyup', (e) => keys.delete(e.code));
addEventListener('blur', () => keys.clear());

const clock = new THREE.Clock();
const cam = { yaw: 0, pitch: 0.3, dist: 7.5, cur: 7.5, lastLook: -10 };
const clampPitch = (p) => Math.min(state.inside ? 0.3 : 1.15, Math.max(0.06, p));
const view = $('view');
let lookId = null, lx = 0, ly = 0;
const stick = $('stick'), knob = $('knob');
let stickId = null, stickAt = null;
const stickVec = { x: 0, y: 0 };
function stickMove(e) {
  let dx = (e.clientX - stickAt[0]) / 48, dy = (e.clientY - stickAt[1]) / 48;
  const l = Math.hypot(dx, dy);
  if (l > 1) { dx /= l; dy /= l; }
  stickVec.x = dx; stickVec.y = -dy;
  knob.style.transform = `translate(${dx * 38}px, ${dy * 38}px)`;
}
function endStick(e) {
  if (e.pointerId !== stickId) return;
  stickId = null; stickVec.x = stickVec.y = 0; knob.style.transform = '';
  stick.style.left = stick.style.top = stick.style.right = stick.style.bottom = '';
}
// On a touch screen a thumb put down anywhere in the lower half on the stick's side becomes the thumbstick,
// so the thumb never has to find a fixed spot. Everywhere else, a drag turns the view.
function stickZone(e) {
  if (!isTouch || e.pointerType === 'mouse' || stickId !== null || !state.playing) return false;
  const r = stage.getBoundingClientRect(), fx = (e.clientX - r.left) / r.width, fy = (e.clientY - r.top) / r.height;
  return fy > 0.42 && (S.lefty ? fx > 0.55 : fx < 0.45);
}
view.addEventListener('pointerdown', (e) => {
  try { view.setPointerCapture(e.pointerId); } catch { /* the pointer has already gone */ }
  if (stickZone(e)) {
    const r = stage.getBoundingClientRect();
    stickId = e.pointerId; stickAt = [e.clientX, e.clientY];
    const half = (stick.offsetWidth || 130) / 2;
    stick.style.left = `${e.clientX - r.left - half}px`; stick.style.top = `${e.clientY - r.top - half}px`; stick.style.right = stick.style.bottom = 'auto';
    return;
  }
  lookId = e.pointerId; lx = e.clientX; ly = e.clientY;
});
view.addEventListener('pointermove', (e) => {
  if (e.pointerId === stickId) { stickMove(e); return; }
  if (e.pointerId !== lookId) return;
  cam.yaw -= (e.clientX - lx) * 0.006;
  cam.pitch = clampPitch(cam.pitch + (e.clientY - ly) * 0.004);
  lx = e.clientX; ly = e.clientY; cam.lastLook = clock.elapsedTime;
});
const endLook = (e) => { endStick(e); if (e.pointerId === lookId) lookId = null; };
view.addEventListener('pointerup', endLook);
view.addEventListener('pointercancel', endLook);
view.addEventListener('contextmenu', (e) => e.preventDefault());

stick.addEventListener('pointerdown', (e) => {
  const r = stick.getBoundingClientRect();
  stickId = e.pointerId; stickAt = [r.left + r.width / 2, r.top + r.height / 2];
  try { stick.setPointerCapture(e.pointerId); } catch { /* gone */ }
  stickMove(e);
});
stick.addEventListener('pointermove', (e) => { if (e.pointerId === stickId) stickMove(e); });
stick.addEventListener('pointerup', endStick);
stick.addEventListener('pointercancel', endStick);

// Three paces: walk, run and sprint. R, the pace button or the controller's A button steps through them,
// and holding Shift goes one pace faster for as long as it is held.
const PACE = [4.6, 9.5, 14], PACE_NAME = ['Walk', 'Run', 'Sprint'];
let gear = 0;
function toggleRun() {
  gear = (gear + 1) % (ROUTE ? 2 : 3);
  const b = $('run');
  b.textContent = PACE_NAME[gear].toUpperCase(); b.classList.toggle('on', gear > 0); b.classList.toggle('fast', gear === 2);
  b.setAttribute('aria-label', `Pace: ${PACE_NAME[gear].toLowerCase()}. Press to change`);
  say(['Walking', 'Running', 'Sprinting'][gear]);
}
$('run').addEventListener('click', toggleRun);
const buzz = (ms) => { if (isTouch && !S.reduce && navigator.vibrate) { try { navigator.vibrate(ms); } catch { /* not allowed */ } } };
// A hop, for the fun of it. It changes nothing about where the player can go.
function jump() {
  if (!state.playing || player.y > 0) return;
  player.vy = state.inside ? 4.2 : 6.6; player.y = 0.001; sound.step(true);
}
$('jump').addEventListener('click', jump);
// Say hello: the player waves, and the nearest passer-by within a few metres waves back.
let helloCool = 0;
function hello() {
  if (!state.playing || helloCool > 0) return;
  helloCool = 1.1; player.wave = 1;
  if (state.inside) { toast('Hiya', 'Somebody at the bar nods back', 1600); sound.pickup(); return; }
  if (peds && peds.greet(player, 7)) {
    state.hellos++; persist(); sound.pickup(); buzz(15);
    toast(HELLOS[Math.floor(Math.random() * HELLOS.length)], state.hellos === 1 ? 'They waved back' : `${state.hellos} hellos so far`, 1500);
    checkStamps();
  } else say('Nobody is near enough to wave back');
}
$('hello').addEventListener('click', hello);
// Words for the hidden status line, which a screen reader reads out. The same words twice are still read twice.
let sayFlip = false;
function say(t) { sayFlip = !sayFlip; $('say').textContent = t + (sayFlip ? '' : '\u00a0'); }
// A game controller: left stick to move, right stick to look, A to change pace, B to jump, X to say hello,
// Y for the way, Start for the menu.
const pad = { x: 0, y: 0, was: [] };
function readPad(dt) {
  pad.x = pad.y = 0;
  let gp = null;
  try { gp = navigator.getGamepads ? [...navigator.getGamepads()].find((g) => g && g.connected) : null; } catch { /* not allowed here */ }
  if (!gp) return;
  const dead = (v) => (Math.abs(v || 0) < 0.2 ? 0 : v);
  pad.x = dead(gp.axes[0]); pad.y = -dead(gp.axes[1]);
  const rx = dead(gp.axes[2]), ry = dead(gp.axes[3]);
  if (state.playing && (rx || ry)) { cam.yaw -= rx * dt * 2.2; cam.pitch = clampPitch(cam.pitch + ry * dt * 1.2); cam.lastLook = clock.elapsedTime; }
  const hit = (i) => { const now = !!(gp.buttons[i] && gp.buttons[i].pressed), first = now && !pad.was[i]; pad.was[i] = now; return first; };
  const a = hit(0), bB = hit(1), xB = hit(2), y = hit(3), start = hit(9);
  if (start && started) { if (menu.classList.contains('on')) closeMenu(); else openMenu(); }
  if (!state.playing) return;
  if (a) { if (S.hold && $('toast').classList.contains('on')) closeToast(); else toggleRun(); }
  if (bB) jump();
  if (xB) hello();
  if (y) whereNext();
}
$('mute').addEventListener('click', () => { sound.setMuted(!sound.muted); $('mute').textContent = sound.muted ? 'Sound is off' : 'Sound is on'; });

function readInput(dt) {
  readPad(dt);
  let x = stickVec.x + pad.x, y = stickVec.y + pad.y;
  // Q and E turn the view, Page Up and Page Down tilt it, for play without a mouse.
  if (state.playing) {
    const turn = (keys.has('KeyQ') ? 1 : 0) - (keys.has('KeyE') ? 1 : 0), tilt = (keys.has('PageUp') ? 1 : 0) - (keys.has('PageDown') ? 1 : 0);
    if (turn || tilt) { cam.yaw += turn * dt * 1.9; cam.pitch = clampPitch(cam.pitch + tilt * dt * 0.9); cam.lastLook = clock.elapsedTime; }
  }
  if (keys.has('KeyW') || keys.has('ArrowUp')) y += 1;
  if (keys.has('KeyS') || keys.has('ArrowDown')) y -= 1;
  if (keys.has('KeyD') || keys.has('ArrowRight')) x += 1;
  if (keys.has('KeyA') || keys.has('ArrowLeft')) x -= 1;
  const l = Math.hypot(x, y);
  if (l > 1) { x /= l; y /= l; }
  input.x = x; input.y = y;
  input.gear = Math.min(2, gear + (keys.has('ShiftLeft') || keys.has('ShiftRight') ? 1 : 0));
}

// ---------- Jobs ----------
const beam = new THREE.Group();
{
  const col = new THREE.Mesh(new THREE.CylinderGeometry(2.2, 2.2, 60, 20, 1, true), new THREE.MeshBasicMaterial({ color: 0xffcf33, transparent: true, opacity: 0.34, side: THREE.DoubleSide, depthWrite: false, fog: false }));
  col.position.y = 30;
  const ring = new THREE.Mesh(new THREE.RingGeometry(2.0, 2.8, 28), new THREE.MeshBasicMaterial({ color: 0xffcf33, side: THREE.DoubleSide, fog: false }));
  ring.rotation.x = -Math.PI / 2; ring.position.y = 0.3;
  beam.add(col, ring); beam.visible = false; scene.add(beam);
}
// ---------- Pubs you can walk into ----------
const PUBS = {
  'Starkie Arms': { x: 2000, z: 2000, layout: 'starkie', carpet: 'tartan', paper: 'floral', panel: 'cream', barColor: 0x6b2a3a },
  'Flying Dutchman': { x: 2100, z: 2000, layout: 'dutchman', carpet: 'damask', paper: 'morris', panel: 'wood', barColor: 0x4a2c1a, poolLamps: true },
};
const doors = [];
const POOL_LINES = ['Potted the black. Get in!', 'In off the white. Unlucky', 'Fluked a double', 'Cleared up in one visit'];
const DART_LINES = ['One hundred and eighty!', 'Treble twenty', 'Bounced out. Mind your toes', 'Bullseye'];
let playCool = 0;
function fade() { if (S.reduce) return; const f = $('fade'); f.classList.add('on'); setTimeout(() => f.classList.remove('on'), 260); }
function enterPub(door) {
  const pub = door.pub;
  state.inside = { door, pub, camDist: cam.dist, camPitch: cam.pitch };
  player.x = pub.spawn.x; player.z = pub.spawn.z; player.heading = cam.yaw = Math.PI;
  cam.dist = cam.cur = 3.1; cam.pitch = 0.2; beam.visible = false;
  fade(); sound.pickup(); toast(pub.name, 'Walk back to the door to leave', 2200);
  if (!state.pubs.has(pub.name)) { state.pubs.add(pub.name); persist(); checkStamps(); }
  $('street').textContent = pub.name;
}
function leavePub() {
  const { door, camDist, camPitch } = state.inside;
  state.inside = null;
  player.x = door.x + door.nx * 1.5; player.z = door.z + door.nz * 1.5;
  player.heading = cam.yaw = Math.atan2(door.nx, door.nz);
  cam.dist = cam.cur = camDist; cam.pitch = camPitch; beam.visible = !!state.job;
  fade();
}

const target = () => (state.job ? state.job.steps[state.job.i].poi : null);
const dist2 = (a, b) => Math.hypot(a.dx - b.dx, a.dz - b.dz);
let slicePois = [], jobPois = [];
// Random deliveries keep to shops, pubs and public places: not schools or the surgery.
const NO_RANDOM = new Set(['school', 'doctors']);

// ---------- Practice routes ----------
// For a supporter and a learner: the same markers in the same order every time, one instruction at a time, no
// clock and no score. It is for getting to know a route before walking it for real with a trainer. It does not
// teach road safety, and the start screen says so.
const spotName = (x, z) => {
  let best = null, bd = 35;
  for (const q of slicePois) { const d = Math.min(Math.hypot(q.x - x, q.z - z), Math.hypot(q.dx - x, q.dz - z) + 4); if (d < bd) { bd = d; best = q.n; } }
  if (best) return `by ${best}`;
  let name = '', near = 20;
  for (const r of roadGrid.at(x, z)) if (r[5]) { const d = distToSeg(x, z, r)[0]; if (d < near) { near = d; name = r[5]; } }
  return name ? `on ${name}` : 'in Padiham';
};
const routeKey = () => `padiham-route-${ROUTE.key}`;
const routeCount = () => { try { return Number(localStorage.getItem(routeKey())) || 0; } catch { return 0; } };
function startRoute() {
  const clampX = (x) => Math.min(slice[2] - 3, Math.max(slice[0] + 3, x)), clampZ = (z) => Math.min(slice[3] - 3, Math.max(slice[1] + 3, z));
  const p = ROUTE.pts.map(([x, z]) => [clampX(x), clampZ(z)]);
  player.x = p[0][0]; player.z = p[0][1]; player.speed = 0;
  player.heading = cam.yaw = Math.atan2(p[1][0] - p[0][0], p[1][1] - p[0][1]);
  const steps = p.slice(1).map(([x, z], i) => {
    const nm = spotName(x, z), last = i === p.length - 2;
    return { poi: { n: last ? `the end of the route, ${nm}` : `the marker ${nm}`, dx: x, dz: z }, text: last ? `Walk to the end of the route, ${nm}` : `Walk to the marker ${nm}` };
  });
  state.job = { title: 'Practice route', route: true, steps, i: 0, t: 0, par: 0, pay: 0 };
  showStep();
}
function routeArrive(j) {
  buzz(30);
  if (j.i < j.steps.length - 1) { j.i++; showStep(); sound.pickup(); toast('Well done', j.steps[j.i].text, 3200); return; }
  const n = routeCount() + 1;
  try { localStorage.setItem(routeKey(), String(n)); } catch { /* private mode */ }
  state.job = null; beam.visible = false; sound.done();
  $('jobTitle').textContent = 'Route finished'; $('jobText').textContent = `You have walked it ${plural(n, 'time')}. Press Walk it again to start over`; $('dist').textContent = '';
  toast('Route finished', `Well done. That is ${plural(n, 'time')}`, 4000);
}
$('again').addEventListener('click', () => { if (ROUTE && started) { closeToast(); startRoute(); say('Back at the start of the route'); } });

// Making a route: the supporter walks it in the game and marks each turning point from the menu. The link holds
// the spots and nothing else. Free text is left out on purpose, so a link cannot put words on the screen.
const draft = [];
function draftNote(msg) {
  $('routeNote').textContent = msg || (draft.length ? `${plural(draft.length, 'spot')}: start ${draft.map(([x, z]) => spotName(x, z)).join(', then ')}` : 'No spots yet. Stand where the route starts and add it');
  $('routeCopy').disabled = draft.length < 2;
}
$('routeAdd').addEventListener('click', () => {
  if (state.inside) { draftNote('Step outside first'); return; }
  const x = Math.round(player.x), z = Math.round(player.z), last = draft[draft.length - 1];
  if (draft.length >= 12) { draftNote('Twelve spots is the most a route can hold'); return; }
  if (last && Math.hypot(last[0] - x, last[1] - z) < 15) { draftNote('Walk a little further before adding the next spot'); return; }
  draft.push([x, z]); draftNote();
});
$('routeClear').addEventListener('click', () => { draft.length = 0; $('routeLink').hidden = true; draftNote(); });
$('routeCopy').addEventListener('click', async () => {
  const url = `${location.origin}${location.pathname}?route=${draft.map((q) => q.join('_')).join('.')}`;
  const box = $('routeLink'); box.value = url; box.hidden = false;
  try { await navigator.clipboard.writeText(url); draftNote('Link copied. Anyone who opens it gets this route'); } catch { box.select(); draftNote('Copy the link from the box below'); }
});

// The bonus time allows for running most of the way, with a little to spare at each stop.
const parFor = (d, stops) => Math.round((d * 1.4) / 8 + 8 * stops);
// A spot on a named street, a fair walk from the shop, for a good turn. Only the street is named, never a house.
function doorstep(from) {
  const roads = world.roads.filter((r) => r.n && r.c !== 'foot' && r.c !== 'service' && r.p.length > 1);
  for (let k = 0; k < 200 && roads.length; k++) {
    const r = roads[Math.floor(Math.random() * roads.length)], i = Math.floor(Math.random() * (r.p.length - 1));
    const x = (r.p[i][0] + r.p[i + 1][0]) / 2, z = (r.p[i][1] + r.p[i + 1][1]) / 2, d = Math.hypot(x - from.dx, z - from.dz);
    if (d < 90 || d > 380 || x < slice[0] + 8 || x > slice[2] - 8 || z < slice[1] + 8 || z > slice[3] - 8 || inRiver(x, z) || nearWall(x, z, 1)) continue;
    return { n: `a neighbour on ${r.n}`, dx: x, dz: z };
  }
  return null;
}
function makeJob() {
  let def = SCRIPT[state.jobIndex];
  if (def && !def.steps.every((s) => slicePois.includes(pois.get(s[0])))) def = null;
  let job;
  if (def) job = { title: `Job ${state.jobIndex + 1}: ${def.title}`, steps: def.steps.map(([n, text]) => ({ poi: pois.get(n), text })) };
  else {
    const here = { dx: player.x, dz: player.z };
    let a, b, tries = 0;
    do { a = jobPois[Math.floor(Math.random() * jobPois.length)]; b = jobPois[Math.floor(Math.random() * jobPois.length)]; tries++; }
    while (tries < 400 && (a === b || dist2(a, b) < 70 || dist2(a, b) > 380 || dist2(here, a) < 30 || dist2(here, a) > 380));
    const thing = CARGO[Math.floor(Math.random() * CARGO.length)];
    // Every third delivery is a good turn for a neighbour, if a street can be found for it.
    const home = state.done % 3 === 2 ? doorstep(a) : null;
    if (home) { const gift = KINDNESS[Math.floor(Math.random() * KINDNESS.length)]; job = { title: 'Good turn', turn: true, steps: [{ poi: a, text: `Collect ${gift} at ${a.n}` }, { poi: home, text: `Take it round to ${home.n}` }] }; }
    else job = { title: 'Delivery', steps: [{ poi: a, text: `Collect ${thing} at ${a.n}` }, { poi: b, text: `Deliver it to ${b.n}` }] };
  }
  let d = 0, prev = { dx: player.x, dz: player.z };
  for (const s of job.steps) { d += dist2(prev, s.poi); prev = s.poi; }
  job.i = 0; job.t = 0;
  job.par = parFor(d, job.steps.length);
  job.pay = job.turn ? 0 : Math.max(10, Math.round(d / 25) * 5);
  state.job = job;
  showStep();
}
function showStep() {
  const j = state.job, s = j.steps[j.i];
  $('jobTitle').textContent = j.title + (j.steps.length > 1 ? `, part ${j.i + 1} of ${j.steps.length}` : '');
  $('jobText').textContent = s.text;
  beam.position.set(s.poi.dx, groundH(s.poi.dx, s.poi.dz), s.poi.dz);
  beam.visible = true;
  lastWords = ''; saidAt = -99;
}
// The way to the next place in words, measured from where the view is facing: the arrow says the same thing.
function bearingWords(a) {
  const d = Math.abs(a) * 57.2958, side = a > 0 ? 'left' : 'right';
  if (d < 20) return 'straight ahead';
  if (d < 70) return `ahead, to your ${side}`;
  if (d < 110) return `to your ${side}`;
  if (d < 160) return `behind you, to your ${side}`;
  return 'behind you';
}
let lastWords = '', saidAt = -99;
function wayNow() {
  const tg = target();
  if (!tg) return null;
  const dx = tg.dx - player.x, dz = tg.dz - player.z;
  return { m: Math.round(Math.hypot(dx, dz) / 5) * 5, words: bearingWords(wrap(Math.atan2(dx, dz) - cam.yaw)) };
}
function whereNext() {
  if (state.inside) { toast(state.inside.pub.name, 'Walk back to the door to leave'); return; }
  const w = wayNow();
  if (!w) return;
  toast(state.job.steps[state.job.i].text, `${w.m} metres, ${w.words}. You are on ${$('street').textContent}`, 5000);
}
$('whereBtn').addEventListener('click', () => { if (state.playing) whereNext(); });
// Messages show for at least two and a half seconds, or until closed if the player has asked for that.
// The menu keeps the last one.
let toastTimer, lastMsg = '';
function toast(title, sub = '', ms = 2600) {
  const t = $('toast');
  t.textContent = title;
  if (sub) { const s = document.createElement('small'); s.textContent = sub; t.appendChild(s); }
  lastMsg = sub ? `${title}. ${sub}` : title;
  if (S.hold) { const s = document.createElement('small'); s.textContent = isTouch ? 'Tap to close' : 'Press Enter or click to close'; t.appendChild(s); }
  t.classList.add('on');
  clearTimeout(toastTimer);
  if (!S.hold) toastTimer = setTimeout(() => t.classList.remove('on'), Math.max(ms, 2500));
}
function closeToast() { clearTimeout(toastTimer); $('toast').classList.remove('on'); }
$('toast').addEventListener('click', closeToast);
function arrive() {
  const j = state.job;
  if (j.route) { routeArrive(j); return; }
  if (j.i < j.steps.length - 1) { j.i++; showStep(); sound.pickup(); buzz(20); toast('Got it!', 'Now follow the arrow to the drop off', 1900); return; }
  const bonus = !S.calm && j.t <= j.par ? Math.round(j.pay / 2) : 0;
  state.money += j.pay + bonus; state.done++; buzz(60);
  if (j.turn) state.turns++;
  if (j.daily) state.dailies++;
  if (!j.daily && state.jobIndex < SCRIPT.length) state.jobIndex++;
  persist(); hud(); sound.done();
  if (j.daily) {
    const old = dailyResult(), r = { date: j.daily, t: Math.round(j.t), par: j.par, calm: S.calm };
    if (!old || old.date !== r.date || old.calm || (!r.calm && r.t < old.t)) { try { localStorage.setItem(DAILY_KEY, JSON.stringify(r)); } catch { /* private mode */ } }
    toast("Today's job done!", S.calm ? 'Open the menu to share it' : `${mmss(j.t)}. Open the menu to share it`, 4000);
    makeJob(); checkStamps();
    return;
  }
  if (j.turn) toast('Good turn done', THANKS[Math.floor(Math.random() * THANKS.length)], 3200);
  else toast(`Job done! +£${j.pay + bonus}`, bonus ? `Includes a £${bonus} speed bonus` : S.calm ? '' : 'Beat the clock next time for a bonus');
  makeJob(); checkStamps();
}
function hud() { $('money').textContent = state.money; $('done').textContent = state.done; $('pigeons').textContent = state.pigeons; $('litterN').textContent = state.litter; $('plaques').textContent = `${foundCount()} of ${plaques.length}`; }

// ---------- Stamps ----------
// Small rewards for doing things around town. Each is worked out from the saved counts, so nothing extra is kept
// but the list of stamps already shown.
const STAMPS = [
  ['first', 'First job', 'Finish a job', () => state.done >= 1],
  ['ten', 'Safe pair of hands', 'Finish ten jobs', () => state.done >= 10],
  ['turn', 'Good neighbour', 'Do three good turns', () => state.turns >= 3],
  ['litter', 'Litter picker', 'Pick up ten pieces of litter', () => state.litter >= 10],
  ['tidy', 'Tidy town', 'Pick up fifty pieces of litter', () => state.litter >= 50],
  ['hello', 'Friendly face', 'Say hello to ten people', () => state.hellos >= 10],
  ['history', 'Local historian', 'Find five plaques', () => foundCount() >= 5],
  ['all', 'Knows every stone', 'Find every plaque', () => plaques.length > 0 && foundCount() >= plaques.length],
  ['daily', 'Regular', "Finish today's job", () => state.dailies >= 1],
  ['mile', 'Stretched your legs', 'Cover a mile on foot', () => state.metres >= 1609],
  ['five', 'Legs of steel', 'Cover five miles on foot', () => state.metres >= 8047],
  ['pigeon', 'Pigeon bother', 'Startle fifty pigeons', () => state.pigeons >= 50],
  ['pubs', 'Both bars', 'Call in at both pubs', () => state.pubs.size >= 2],
].filter((st) => !(CLEAN && st[0] === 'pubs'));
function checkStamps() {
  if (ROUTE) return;
  const fresh = STAMPS.filter((st) => !state.stamps.has(st[0]) && st[3]());
  if (!fresh.length) return;
  for (const st of fresh) state.stamps.add(st[0]);
  persist();
  const first = fresh[0];
  // Wait for the message already on screen to be read before showing the stamp.
  setTimeout(() => { toast(`Stamp: ${first[1]}`, fresh.length > 1 ? `And ${fresh.length - 1} more. See them in the menu` : 'See your stamps in the menu', 3200); sound.done(); buzz(40); }, $('toast').classList.contains('on') ? 2700 : 0);
}
const miles = (m) => (m < 1609 ? `${Math.round(m / 10) * 10} metres` : `${(m / 1609.344).toFixed(1)} miles`);

// ---------- Litter ----------
// Pieces of litter lie on the pavements. Walking over one picks it up, and it turns up somewhere else later,
// so there is always some to find. They show as pale green dots on the map.
const litter = [];
function placeLitter() {
  const paths = built.pedPaths;
  if (!paths.length) return;
  const geos = [new THREE.BoxGeometry(0.34, 0.06, 0.26), new THREE.CylinderGeometry(0.09, 0.09, 0.3, 8).rotateZ(Math.PI / 2), new THREE.BoxGeometry(0.3, 0.2, 0.22)];
  const mats = [0xe2473d, 0x3d7fe2, 0xf2c230, 0xf4f1e8].map((c) => new THREE.MeshLambertMaterial({ color: c }));
  const n = isTouch ? 22 : 34;
  for (let i = 0; i < n; i++) {
    const mesh = new THREE.Mesh(geos[i % geos.length], mats[i % mats.length]);
    const l = { mesh, x: 0, z: 0, back: 0 };
    scene.add(mesh); litter.push(l); moveLitter(l);
  }
}
function moveLitter(l) {
  const paths = built.pedPaths;
  for (let k = 0; k < 60; k++) {
    const path = paths[Math.floor(Math.random() * paths.length)], i = Math.floor(Math.random() * (path.length - 1)), t = Math.random();
    const x = path[i][0] + (path[i + 1][0] - path[i][0]) * t + (Math.random() - 0.5) * 1.6, z = path[i][1] + (path[i + 1][1] - path[i][1]) * t + (Math.random() - 0.5) * 1.6;
    if (x < slice[0] + 6 || x > slice[2] - 6 || z < slice[1] + 6 || z > slice[3] - 6 || nearWall(x, z, 0.7) || inRiver(x, z) || Math.hypot(x - player.x, z - player.z) < 25) continue;
    if (litter.some((o) => o !== l && Math.hypot(o.x - x, o.z - z) < 18) || plaques.some((q) => Math.hypot(q.x - x, q.z - z) < 3)) continue;
    l.x = x; l.z = z; l.mesh.position.set(x, 0.2 + groundH(x, z), z); l.mesh.rotation.y = Math.random() * 6.28; l.mesh.visible = true;
    return;
  }
  l.mesh.visible = false; l.back = 20;
}
function updateLitter(dt) {
  for (const l of litter) {
    if (!l.mesh.visible) { l.back -= dt; if (l.back <= 0) moveLitter(l); continue; }
    if (state.inside || player.y > 0.5 || Math.hypot(player.x - l.x, player.z - l.z) > 1.25) continue;
    l.mesh.visible = false; l.back = 45 + Math.random() * 45;
    state.litter++; persist(); hud(); sound.pickup(); buzz(15);
    if (state.litter === 1) toast('Litter picked up', 'Walk over any you see. The pale green dots on the map show where', 3600);
    else if (state.litter % 5 === 0) toast(`${state.litter} pieces picked up`, 'Padiham thanks you', 1700);
    else say(`Litter picked up. ${state.litter} so far`);
    checkStamps();
  }
}
const foundCount = () => plaques.filter((p) => state.plaques.has(p.id)).length;
const mmss = (s) => `${Math.floor(s / 60)}:${String(Math.floor(s % 60)).padStart(2, '0')}`;

// ---------- Today's job (a first cut of ideas I148 and I149) ----------
// One job a day, made from the date alone, so everyone in town gets the same three stops and can compare.
// Nothing is sent anywhere: the result is kept in the browser and shared only if the player chooses to.
const DAILY_KEY = 'padiham-daily-1';
const today = () => { const d = new Date(); return `${d.getFullYear()}-${String(d.getMonth() + 1).padStart(2, '0')}-${String(d.getDate()).padStart(2, '0')}`; };
function seeded(str) {
  let h = 2166136261;
  for (const c of str) { h ^= c.charCodeAt(0); h = Math.imul(h, 16777619); }
  return () => { h = (h + 0x6d2b79f5) | 0; let t = h; t = Math.imul(t ^ (t >>> 15), t | 1); t ^= t + Math.imul(t ^ (t >>> 7), t | 61); return ((t ^ (t >>> 14)) >>> 0) / 4294967296; };
}
const dailyResult = () => { try { return JSON.parse(localStorage.getItem(DAILY_KEY) || 'null'); } catch { return null; } };
function dailyJob() {
  const rnd = seeded('padiham ' + today()), list = [...jobPois].sort((a, b) => (a.n < b.n ? -1 : 1));
  const start = pois.get('Padiham Town Hall') || list[0], stops = [];
  let prev = start, tries = 0;
  while (stops.length < 3 && tries++ < 800) {
    const c = list[Math.floor(rnd() * list.length)], d = dist2(prev, c);
    if (c !== start && !stops.includes(c) && d > 80 && d < 420) { stops.push(c); prev = c; }
  }
  if (stops.length < 3) return null;
  const thing = CARGO[Math.floor(rnd() * CARGO.length)];
  return { daily: today(), start, title: "Today's job", steps: [{ poi: stops[0], text: `Collect ${thing} at ${stops[0].n}` }, { poi: stops[1], text: `Call in at ${stops[1].n}` }, { poi: stops[2], text: `Deliver it to ${stops[2].n}` }] };
}
function startDaily() {
  const job = dailyJob();
  if (!job) { note('No job could be made today'); return; }
  if (state.inside) leavePub();
  // Everyone sets off from the same spot, outside the Town Hall.
  player.x = job.start.dx; player.z = job.start.dz;
  for (let k = 0; k < 12 && nearWall(player.x, player.z, 1.2); k++) { player.x += 1; player.z += 0.6; }
  let d = 0, prev = { dx: player.x, dz: player.z };
  for (const st of job.steps) { d += dist2(prev, st.poi); prev = st.poi; }
  job.i = 0; job.t = 0; job.par = parFor(d, job.steps.length); job.pay = 30;
  state.job = job; showStep(); closeMenu();
  toast("Today's job", 'The same three stops for everyone today', 3500);
}
function dailyLine() {
  const r = dailyResult();
  if (!r || r.date !== today()) return null;
  const day = new Date().toLocaleDateString('en-GB', { day: 'numeric', month: 'long' });
  const marks = r.calm || r.t <= r.par ? '🟩🟩🟩' : r.t <= r.par * 1.5 ? '🟩🟩🟨' : '🟩🟨🟨';
  return `The Padiham Job, ${day}\n${marks} ${r.calm ? 'done at a stroll' : mmss(r.t)}\nSame three stops for everyone today\ntompickup.co.uk/the-padiham-job`;
}

// ---------- Local history plaques ----------
// Each plaque stands on the pavement by its place. The words and their sources are in history.js,
// and the page lists every source under the game.
const plaques = [];
let plaqueShown = null;
const LAT0 = 53.802, LON0 = -2.315, MXM = Math.cos((LAT0 * Math.PI) / 180) * 111320, MZM = 110540;
function placePlaques() {
  const blocked = (x, z) => nearWall(x, z, 1.1) || world.buildings.some((b) => inPoly(x, z, b.p));
  const blue = new THREE.MeshLambertMaterial({ color: 0x1f4fa3 }), rim = new THREE.MeshLambertMaterial({ color: 0xf4f1e8 }), post = new THREE.MeshLambertMaterial({ color: 0x2e3a36 });
  const inSlice = (x, z) => x > slice[0] + 6 && x < slice[2] - 6 && z > slice[1] + 6 && z < slice[3] - 6;
  const taken = (x, z) => Math.min(...plaques.map((p) => Math.hypot(p.x - x, p.z - z)), ...doors.map((d) => Math.hypot(d.x - x, d.z - z)), ...slicePois.map((q) => Math.hypot(q.dx - x, q.dz - z)), 60);
  for (const def of PLAQUES) {
    let poi = def.at ? pois.get(def.at) : null;
    if (def.ll) {
      // A building given by latitude and longitude, in the world file's local metres (see build_world.py).
      const bx = (def.ll[1] - LON0) * MXM, bz = -(def.ll[0] - LAT0) * MZM;
      let bd = 1e9;
      for (const r of world.roads) {
        if (r.c === 'foot' || r.c === 'service') continue;
        for (let i = 0; i < r.p.length - 1; i++) { const [d, qx, qz] = distToSeg(bx, bz, [r.p[i][0], r.p[i][1], r.p[i + 1][0], r.p[i + 1][1]]); if (d < bd && inSlice(qx, qz)) { bd = d; poi = { x: bx, z: bz, dx: qx, dz: qz }; } }
      }
    }
    if (def.road) {
      // A fact about a street: stand on its pavement, on the stretch furthest from the other plaques and job markers.
      let bs = -1;
      for (const r of world.roads) {
        if (r.n !== def.road || r.c === 'foot') continue;
        for (let i = 0; i < r.p.length - 1; i++) {
          const mx = (r.p[i][0] + r.p[i + 1][0]) / 2, mz = (r.p[i][1] + r.p[i + 1][1]) / 2, l = Math.hypot(r.p[i + 1][0] - r.p[i][0], r.p[i + 1][1] - r.p[i][1]) || 1;
          const nx = -(r.p[i + 1][1] - r.p[i][1]) / l, nz = (r.p[i + 1][0] - r.p[i][0]) / l;
          for (const side of [1, -1]) {
            const px = mx + nx * side * (r.w / 2 + 1.5), pz = mz + nz * side * (r.w / 2 + 1.5);
            if (!inSlice(px, pz) || blocked(px, pz)) continue;
            const sc = taken(px, pz);
            if (sc > bs) { bs = sc; poi = { x: px + nx * side * 0.4, z: pz + nz * side * 0.4, dx: px, dz: pz, k: 'street' }; }
          }
        }
      }
    }
    if (!poi) continue;
    // Walk from the road point back towards the building and stop at the last clear spot: the pavement by the frontage.
    let x = poi.dx, z = poi.dz;
    const L = Math.hypot(poi.x - poi.dx, poi.z - poi.dz) || 1, ux = (poi.x - poi.dx) / L, uz = (poi.z - poi.dz) / L;
    const cap = poi.k === 'park' || poi.k === 'pitch' || poi.k === 'bridge' ? 5.5 : 16;
    for (let t = 0.5; t <= Math.min(L, cap); t += 0.5) { const qx = poi.dx + ux * t, qz = poi.dz + uz * t; if (blocked(qx, qz)) break; x = qx; z = qz; }
    // Then settle on the nearest spot that is off the carriageway and clear of pub doors, the selfie taker,
    // job markers and other plaques.
    const onRoad = (qx, qz) => { for (const sg of roadGrid.at(qx, qz)) if (sg[6] !== 'foot' && distToSeg(qx, qz, sg)[0] < sg[4] + 0.5) return true; return false; };
    const good = (qx, qz) => inSlice(qx, qz) && !blocked(qx, qz) && !onRoad(qx, qz) && !doors.some((d) => Math.hypot(d.x - qx, d.z - qz) < 3.5) && !plaques.some((p) => Math.hypot(p.x - qx, p.z - qz) < 4) && !slicePois.some((q) => Math.hypot(q.dx - qx, q.dz - qz) < 4.4) && !(selfie && Math.hypot(selfie.x - qx, selfie.z - qz) < 3);
    if (!good(x, z)) search: for (let r = 1; r <= 16; r += 1) for (let k = 0; k < 16; k++) { const qx = x + Math.cos(k * 0.3927) * r, qz = z + Math.sin(k * 0.3927) * r; if (good(qx, qz)) { x = qx; z = qz; break search; } }
    const g = new THREE.Group(); g.position.set(x, 0.14 + groundH(x, z), z);
    const pole = new THREE.Mesh(new THREE.CylinderGeometry(0.045, 0.045, 1.5, 8), post); pole.position.y = 0.75; pole.castShadow = true;
    const disc = new THREE.Mesh(new THREE.CylinderGeometry(0.34, 0.34, 0.05, 24), blue); disc.rotation.x = Math.PI / 2; disc.position.y = 1.62; disc.castShadow = true;
    const ring = new THREE.Mesh(new THREE.TorusGeometry(0.34, 0.025, 6, 28), rim); ring.position.y = 1.62;
    g.add(pole, disc, ring); g.rotation.y = Math.atan2(poi.dx - poi.x, poi.dz - poi.z); scene.add(g);
    plaques.push({ ...def, x, z, g });
  }
}
function updatePlaques(time) {
  let near = null;
  if (!state.inside) for (const p of plaques) { if (!S.reduce) p.g.children[1].rotation.z = time * 0.6; if (Math.hypot(player.x - p.x, player.z - p.z) < 2.6) near = p; }
  if (near === plaqueShown) return;
  plaqueShown = near;
  const el = $('plaque');
  if (!near) { el.classList.remove('on'); return; }
  $('plaqueTitle').textContent = near.title; $('plaqueText').textContent = near.text; $('plaqueSource').textContent = `Source: ${near.short}`;
  el.classList.add('on');
  if (!state.plaques.has(near.id)) { state.plaques.add(near.id); persist(); hud(); sound.done(); buzz(40); checkStamps(); }
}

// ---------- Menu: pause, the plaque journal, sharing and postcards ----------
let started = false;
const menu = $('menu');
const note = (t) => { $('menuNote').textContent = t; };
const plural = (n, w) => `${n} ${w}${n === 1 ? '' : 's'}`;
function renderJournal() {
  const ul = $('journal'); ul.textContent = '';
  for (const p of plaques) {
    const found = state.plaques.has(p.id), li = document.createElement('li');
    const b = document.createElement('b'); b.textContent = p.title;
    const d = document.createElement('span'); d.textContent = found ? p.text : 'Not found yet';
    li.append(b, d);
    if (found) { const sm = document.createElement('small'); sm.textContent = `Source: ${p.short}`; li.appendChild(sm); } else li.className = 'todo';
    ul.appendChild(li);
  }
  $('jcount').textContent = `${foundCount()} of ${plaques.length}`;
  $('tally').textContent = `${plural(state.done, 'job')} done, ${plural(state.turns, 'good turn')}, ${plural(state.litter, 'piece')} of litter picked up, ${plural(state.hellos, 'hello')} said and ${miles(state.metres)} covered on foot.`;
  const sl = $('stamps'); sl.textContent = '';
  for (const [id, name, how] of STAMPS) {
    const li = document.createElement('li'), got = state.stamps.has(id);
    const b = document.createElement('b'); b.textContent = name;
    const d = document.createElement('span'); d.textContent = got ? 'Earned' : how;
    li.append(b, d); if (!got) li.className = 'todo';
    sl.appendChild(li);
  }
  $('scount').textContent = `${state.stamps.size} of ${STAMPS.length}`;
}
function openMenu() {
  if (!started || menu.classList.contains('on')) return;
  state.playing = false; keys.clear(); stickVec.x = stickVec.y = 0; persist();
  $('full').textContent = stage.classList.contains('fill') || document.fullscreenElement ? 'Leave full screen' : 'Full screen';
  $('dailyShare').hidden = !dailyLine();
  renderJournal(); note(''); $('last').textContent = lastMsg ? `Last message: ${lastMsg}` : '';
  menu.classList.add('on'); $('resume').focus();
}
function closeMenu() { if (!menu.classList.contains('on')) return; menu.classList.remove('on'); state.playing = true; $('menuBtn').focus(); }
// Keep the Tab key inside the menu while it is open.
menu.addEventListener('keydown', (e) => {
  if (e.key !== 'Tab') return;
  const f = [...menu.querySelectorAll('button, a[href], input, summary')].filter((el) => el.offsetParent !== null);
  if (!f.length) return;
  const first = f[0], last = f[f.length - 1];
  if (e.shiftKey && document.activeElement === first) { e.preventDefault(); last.focus(); }
  else if (!e.shiftKey && document.activeElement === last) { e.preventDefault(); first.focus(); }
});
$('menuBtn').addEventListener('click', openMenu);
$('daily').addEventListener('click', startDaily);
$('dailyShare').addEventListener('click', async () => {
  const text = dailyLine();
  if (!text) return;
  try {
    if (navigator.share && isTouch) await navigator.share({ text });
    else { await navigator.clipboard.writeText(text); note('Copied. Paste it wherever you like'); }
  } catch { /* the share sheet was closed, or copying is blocked */ }
});
$('resume').addEventListener('click', closeMenu);
addEventListener('keydown', (e) => { if (e.code === 'Escape' && started) { if (menu.classList.contains('on')) closeMenu(); else openMenu(); } });
// Pause when the tab is left. The dev server can switch this off for testing in a background pane.
addEventListener('pagehide', persist);
document.addEventListener('visibilitychange', () => { if (document.hidden && !(import.meta.env.DEV && window.__noAutoPause)) openMenu(); });
const PAGE = 'https://tompickup.co.uk/the-padiham-job/';
$('share').addEventListener('click', async () => {
  const text = `I found ${foundCount()} of ${plaques.length} history plaques, did ${plural(state.done, 'job')} and picked up ${plural(state.litter, 'piece')} of litter in The Padiham Job, a game on the real streets of Padiham.`;
  try {
    if (navigator.share) await navigator.share({ title: 'The Padiham Job', text, url: PAGE });
    else { await navigator.clipboard.writeText(`${text} ${PAGE}`); note('Copied. Paste it wherever you like'); }
  } catch { /* the share sheet was closed, or copying is blocked */ }
});
// A postcard is the view as it stands, without the panels, on a card with the place name under it.
function placeName() {
  if (state.inside) return state.inside.pub.name;
  let best = null, bd = 45;
  for (const q of slicePois) { const d = Math.hypot(q.x - player.x, q.z - player.z); if (d < bd) { bd = d; best = q.n; } }
  return best || $('street').textContent;
}
$('card').addEventListener('click', () => {
  const was = beam.visible; beam.visible = false;
  renderer.render(scene, camera);
  const src = renderer.domElement, W = 1600, H = Math.round((W * src.height) / src.width);
  const cv = document.createElement('canvas'); cv.width = W; cv.height = H + 170;
  const g = cv.getContext('2d');
  g.fillStyle = '#f4f1e8'; g.fillRect(0, 0, cv.width, cv.height);
  g.drawImage(src, 0, 0, W, H);
  beam.visible = was;
  g.textBaseline = 'middle';
  g.fillStyle = '#14161a'; g.font = '700 52px Georgia, serif'; g.fillText('Greetings from Padiham', 40, H + 48, W - 80);
  g.fillStyle = '#3c3f44'; g.font = '500 28px system-ui, sans-serif'; g.fillText(`${placeName()}. The Padiham Job, tompickup.co.uk/the-padiham-job`, 40, H + 100, W - 80);
  g.fillStyle = '#6a6d72'; g.font = '400 18px system-ui, sans-serif'; g.fillText('Map data \u00a9 OpenStreetMap contributors. Contains OS data \u00a9 Crown copyright and database right 2026. \u00a9 Environment Agency copyright and/or database right 2022. All rights reserved.', 40, H + 142, W - 80);
  cv.toBlob(async (blob) => {
    if (!blob) { note('Could not make the postcard here'); return; }
    const file = new File([blob], 'padiham-postcard.png', { type: 'image/png' });
    if (isTouch && navigator.canShare && navigator.canShare({ files: [file] })) { try { await navigator.share({ files: [file], title: 'A postcard from Padiham' }); } catch { /* closed */ } return; }
    const a = document.createElement('a'); a.href = URL.createObjectURL(blob); a.download = 'padiham-postcard.png'; a.click();
    setTimeout(() => URL.revokeObjectURL(a.href), 5000);
    note('Postcard saved to your downloads');
  }, 'image/png');
});

// ---------- The selfie taker by the Town Hall ----------
// A caricature of the town's MP, Oliver Ryan (Labour and Co-operative), drawn from his official portrait:
// short light brown hair, clean shaven, navy suit, red tie. Light satire; it says nothing about him as fact.
let selfie = null;
const SELFIE_LINES = ['Say cheese!', 'Just one more for the socials', 'Is the Town Hall in shot?', 'Hang on, that is my good side', 'Can you move? You are in my selfie'];
function makeSelfie(x, z, heading) {
  const fig = makeFigure({ jacket: 0x1c2740, trousers: 0x1c2740, hair: 0x9a6b42, skin: 0xf0c8ad, suit: true, tie: 0xb3252a, rosette: 0xd4202a, phone: true, face: true, eyes: 0x3f6f9f });
  fig.group.position.set(x, 0.14 + groundH(x, z), z); fig.group.rotation.y = heading;
  const flash = new THREE.PointLight(0xffffff, 0, 9, 1.6); flash.position.set(0.3, 1.9, 0.75); fig.group.add(flash);
  scene.add(fig.group);
  // He stands still, so he is a small obstacle rather than something to walk through.
  for (let k = 0; k < 6; k++) { const a0 = (k / 6) * 6.2832, a1 = ((k + 1) / 6) * 6.2832, s = [x + Math.cos(a0) * 0.35, z + Math.sin(a0) * 0.35, x + Math.cos(a1) * 0.35, z + Math.sin(a1) * 0.35, 1]; walls.add(s[0], s[1], s[2], s[3], 1.5, s); }
  return { fig, x, z, flash, next: 2, cool: 0, line: 0 };
}
function updateSelfie(dt, time) {
  const s = selfie, f = s.fig;
  // Right arm up and out with the phone, a little sway, left hand giving a thumbs up at his side.
  f.arms[1].rotation.x = -2.05 + Math.sin(time * 1.3) * 0.06; f.arms[1].rotation.z = 0.28;
  f.arms[0].rotation.x = -0.25 + Math.sin(time * 2.1) * 0.05;
  f.group.rotation.z = Math.sin(time * 0.9) * 0.03;
  s.next -= dt;
  if (s.next <= 0) { if (!S.calm && !S.reduce) s.flash.intensity = 30; s.next = 3 + Math.random() * 4; }
  s.flash.intensity *= Math.max(0, 1 - dt * 9);
  s.cool -= dt;
  if (s.cool <= 0 && !S.calm && state.playing && !state.inside && Math.hypot(player.x - s.x, player.z - s.z) < 3.2) {
    toast(SELFIE_LINES[s.line++ % SELFIE_LINES.length], 'Selfie in progress', 2000); sound.pickup(); s.cool = 7;
  }
}

// ---------- Main loop ----------
const wrap = (a) => Math.atan2(Math.sin(a), Math.cos(a));
let uiTick = 0, edgeWarn = 0, lastStep = 0;

function step(dt) {
  readInput(dt);
  const time = clock.elapsedTime;
  const fx = Math.sin(cam.yaw), fz = Math.cos(cam.yaw);
  const mx = fx * input.y - fz * input.x, mz = fz * input.y + fx * input.x;
  const mag = Math.min(1, Math.hypot(mx, mz));
  // Indoors the top pace is a run: there is no room to sprint round a pool table.
  const top = PACE[state.inside ? Math.min(1, input.gear) : input.gear];
  const want = state.playing ? mag * top : 0;
  player.speed += (want - player.speed) * Math.min(1, dt * (want > player.speed ? 6 : 10));
  if (mag > 0.05 && state.playing) {
    const h = Math.atan2(mx, mz);
    player.heading += wrap(h - player.heading) * Math.min(1, dt * 12);
    const ox = player.x, oz = player.z;
    // Move in short steps, so that at a sprint the player cannot pass through a wall between two frames.
    const dist = player.speed * dt, n = Math.max(1, Math.ceil(dist / 0.3));
    for (let k = 0; k < n; k++) { player.x += (mx / mag) * (dist / n); player.z += (mz / mag) * (dist / n); pushOut(player); }
    if (!state.inside) state.metres += Math.hypot(player.x - ox, player.z - oz);
    if (!state.inside && inRiver(player.x, player.z)) { player.x = ox; player.z = oz; if (time > edgeWarn) { toast('Too deep to wade', 'Find a bridge', 1500); edgeWarn = time + 4; } }
    const cx = Math.min(slice[2] - 1, Math.max(slice[0] + 1, player.x)), cz = Math.min(slice[3] - 1, Math.max(slice[1] + 1, player.z));
    if (!state.inside && (cx !== player.x || cz !== player.z)) { player.x = cx; player.z = cz; if (time > edgeWarn) { toast('Edge of the map', 'Turn back towards town', 1800); edgeWarn = time + 5; } }
    if (S.follow && lookId === null && time - cam.lastLook > 1.2) cam.yaw += Math.sin(wrap(h - cam.yaw)) * dt * 1.7;
  }
  // Character
  // Legs turn over faster at a run, but not in step with the speed, or a sprint would be a blur.
  player.phase += (Math.min(player.speed, 4.6) + Math.max(0, player.speed - 4.6) * 0.42) * dt * 2.3;
  const amt = Math.min(1, player.speed / 4) * (player.speed > 11 ? 1.25 : 1);
  animateFigure(player.fig, player.phase, amt);
  // Hop
  if (player.y > 0) {
    player.vy -= 20 * dt; player.y += player.vy * dt;
    if (player.y <= 0) { player.y = 0; player.vy = 0; sound.step(false); }
    else { player.fig.legs[0].rotation.x = -0.5; player.fig.legs[1].rotation.x = 0.35; player.fig.arms[0].rotation.x = player.fig.arms[1].rotation.x = -1.1; }
  }
  // Wave
  helloCool -= dt;
  if (player.wave > 0) { player.wave -= dt; player.fig.arms[1].rotation.x = -2.7 + Math.sin(time * 16) * 0.3; player.fig.arms[1].rotation.z = 0.25; if (player.wave <= 0) player.fig.arms[1].rotation.z = 0; }
  const stepN = Math.floor(player.phase / Math.PI);
  if (stepN !== lastStep) { lastStep = stepN; if (player.speed > 1) sound.step(player.speed > 6); }
  const baseY = state.inside ? state.inside.pub.floorY : 0.14 + groundH(player.x, player.z);
  if (state.playing) {
    if (!state.inside) { for (const d of doors) if (Math.hypot(player.x - d.x, player.z - d.z) < 1.0) { enterPub(d); break; } }
    else {
      const pub = state.inside.pub;
      playCool -= dt;
      if (Math.hypot(player.x - pub.exit.x, player.z - pub.exit.z) < 0.62) leavePub();
      else if (playCool <= 0) {
        // The dartboard is checked first: its spot is inside the pool table's wider reach.
        if (pub.darts && Math.hypot(player.x - pub.darts.x, player.z - pub.darts.z) < 1.1) { toast(DART_LINES[Math.floor(Math.random() * DART_LINES.length)], 'Dartboard', 1700); sound.pickup(); playCool = 6; }
        else for (const pl of pub.pools) if (Math.hypot(player.x - pl.x, player.z - pl.z) < 2.0) { toast(POOL_LINES[Math.floor(Math.random() * POOL_LINES.length)], 'Pool table', 1700); sound.pickup(); playCool = 6; break; }
      }
    }
  }
  const sway = S.reduce ? 0 : amt;
  player.fig.group.position.set(player.x, baseY + player.y + Math.abs(Math.sin(player.phase)) * 0.05 * sway, player.z);
  player.fig.group.rotation.y = player.heading;
  player.fig.group.rotation.x = sway * (player.speed > 11 ? 0.24 : player.speed > 6 ? 0.16 : 0.05);

  for (const sp of labels) { const k = Math.min(1, Math.max(0.2, camera.position.distanceTo(sp.position) / 70)); sp.scale.set(20 * k, 3.75 * k, 1); }
  if (selfie) updateSelfie(dt, time);
  if (state.playing) { updatePlaques(time); updateLitter(dt); }
  if (peds) peds.update(dt, player);
  if (pigeons) pigeons.update(dt, player, time);

  // Camera, pulled in if a wall is in the way
  const cp = Math.cos(cam.pitch), flat = cam.dist * cp;
  const t = wallHit(player.x, player.z, player.x - fx * (flat + 0.6), player.z - fz * (flat + 0.6));
  const wantDist = Math.max(1.5, cam.dist * t - (t < 1 ? 0.5 : 0));
  cam.cur += (wantDist - cam.cur) * Math.min(1, dt * (wantDist < cam.cur ? 14 : 3));
  camera.position.set(player.x - fx * cam.cur * cp, baseY + 1.6 + Math.sin(cam.pitch) * cam.cur, player.z - fz * cam.cur * cp);
  // On a slope the ground behind the player can rise above the camera: keep the camera clear of it.
  if (!state.inside) camera.position.y = Math.max(camera.position.y, groundH(camera.position.x, camera.position.z) + 0.6);
  camera.lookAt(player.x, baseY + 1.45, player.z);
  // The view opens out a little at a sprint, unless less movement has been asked for.
  const fov = baseFov + (S.reduce ? 0 : Math.max(0, Math.min(1, (player.speed - 9.5) / 4.5)) * 7);
  if (Math.abs(fov - camera.fov) > 0.05) { camera.fov += (fov - camera.fov) * Math.min(1, dt * 5); camera.updateProjectionMatrix(); }
  sky.position.copy(camera.position); clouds.position.set(camera.position.x, 0, camera.position.z);
  const sx = Math.round(player.x / 4) * 4, sz = Math.round(player.z / 4) * 4;
  sun.target.position.set(sx, baseY, sz);
  sun.position.set(sx, baseY, sz).addScaledVector(sunDir, 220);

  // Job progress
  const tg = target();
  if (tg && state.playing && !state.inside) {
    state.job.t += dt;
    const dx = tg.dx - player.x, dz = tg.dz - player.z, d = Math.hypot(dx, dz);
    if (d < 3.6) arrive();
    if (S.reduce) beam.scale.setScalar(1);
    else { beam.rotation.y += dt * 0.8; beam.scale.setScalar(1 + Math.sin(time * 3) * 0.06); }
    uiTick -= dt;
    if (uiTick <= 0) {
      uiTick = 0.1;
      $('arrow').style.transform = `rotate(${-wrap(Math.atan2(dx, dz) - cam.yaw)}rad)`;
      const words = bearingWords(wrap(Math.atan2(dx, dz) - cam.yaw)), m = Math.round(d / 5) * 5;
      $('dist').textContent = `${m} m, ${words}`;
      // Read the way out when it changes, but not more than once every eight seconds.
      if (words !== lastWords && time - saidAt > 8) { lastWords = words; saidAt = time; say(`${state.job.steps[state.job.i].poi.n}: ${m} metres, ${words}`); }
      const j = state.job;
      $('timer').textContent = S.calm ? '' : j.t <= j.par ? `${mmss(j.t)}, bonus up to ${mmss(j.par)}` : mmss(j.t);
      let name = '', best = 14;
      for (const r of roadGrid.at(player.x, player.z)) if (r[5]) { const dd = distToSeg(player.x, player.z, r)[0]; if (dd < best) { best = dd; name = r[5]; } }
      $('street').textContent = name || 'Padiham';
      drawMini();
    }
  }
}
// ---------- Quality ----------
// The town is built in squares, and squares beyond the haze are not drawn. If the game still runs slowly it steps
// down by itself: first a lower resolution, then no shadows, then a nearer horizon. It never steps back up, so it
// cannot flicker between settings. A phone starts with a nearer horizon than a desktop.
const quality = { level: 0, view: isTouch ? 600 : 900, frames: 0, since: 0, last: 0, hold: 2 };
scene.fog.far = quality.view;
if (isTouch) scene.fog.near = 110;
function cullChunks() {
  const lim = quality.view + 110;
  for (const c of chunks) c.mesh.visible = Math.hypot(c.x - camera.position.x, c.z - camera.position.z) < lim;
}
function stepDown() {
  if (quality.level >= 3) return;
  quality.level++;
  if (quality.level === 1) renderer.setPixelRatio(Math.max(1, renderer.getPixelRatio() * 0.75));
  else if (quality.level === 2) {
    sun.castShadow = false;
    scene.traverse((o) => { if (o.material) for (const m of [].concat(o.material)) m.needsUpdate = true; });
  } else {
    quality.view = 380; scene.fog.far = 380; scene.fog.near = 80;
    renderer.setPixelRatio(Math.max(0.75, renderer.getPixelRatio() * 0.8));
  }
}
function watchSpeed(now) {
  // Judge three seconds of play at a time. A long gap (the tab was hidden, or a pause) throws the sample away.
  if (!state.playing || now - quality.last > 400) { quality.frames = 0; quality.since = now; quality.last = now; return; }
  quality.last = now; quality.frames++;
  if (now - quality.since < 3000) return;
  const fps = (quality.frames * 1000) / (now - quality.since);
  quality.frames = 0; quality.since = now;
  if (quality.hold > 0) { quality.hold--; return; }
  if (fps < 27) { stepDown(); quality.hold = 1; }
}
function frame() {
  requestAnimationFrame(frame);
  const dt = Math.min(0.05, clock.getDelta());
  if (!onScreen) return;
  step(dt);
  cullChunks();
  renderer.render(scene, camera);
  watchSpeed(performance.now());
}

// ---------- Boot ----------
// The terrain file is optional: without it the town is built flat, as it was before the hills were added.
const terrainUrl = stage.dataset.world.replace('world.json', 'terrain.json');
Promise.all([fetch(stage.dataset.world).then((r) => r.json()), fetch(terrainUrl).then((r) => (r.ok ? r.json() : null)).catch(() => null)]).then(([w, tr]) => {
  setTerrain(tr);
  if (tr && Array.isArray(tr.bh) && tr.bh.length === w.buildings.length) w.buildings.forEach((b, i) => { if (tr.bh[i]) b.lh = tr.bh[i]; });
  world = w; slice = w.slice;
  built = buildWorld(scene, w, isTouch);
  for (const p of w.pois) {
    pois.set(p.n, p);
    const inside = p.dx > slice[0] + 4 && p.dx < slice[2] - 4 && p.dz > slice[1] + 4 && p.dz < slice[3] - 4;
    if (inside) { safeDoor(p); slicePois.push(p); if (!NO_RANDOM.has(p.k)) jobPois.push(p); }
    if (!built.signs.has(p.n)) label(p.n.replace('Church of England', 'CE'), p.x, p.k === 'park' || p.k === 'pitch' || p.k === 'bridge' ? 8 : 15, p.z);
  }
  for (const d of built.doors) {
    if (CLEAN || !PUBS[d.name]) continue;
    d.pub = buildPub(scene, { ...PUBS[d.name], name: d.name });
    const ring = new THREE.Mesh(new THREE.RingGeometry(0.55, 0.8, 24), new THREE.MeshBasicMaterial({ color: 0x4fb3ff, side: THREE.DoubleSide, fog: false }));
    ring.rotation.x = -Math.PI / 2; ring.position.set(d.x, 0.32 + groundH(d.x, d.z), d.z); scene.add(ring);
    doors.push(d);
  }
  drawMapBase();
  const home = pois.get('Padiham Town Hall') || slicePois[0];
  const hall = built.fronts.get('Padiham Town Hall');
  if (hall) {
    // Start facing the Town Hall from across its forecourt, with the selfie taker in shot beside the portico.
    const tx = -hall.nz, tz = hall.nx;
    const clear = (x, z, r) => { let k = 0; while (nearWall(x, z, r) && k++ < 10) { x += hall.nx * 1.2; z += hall.nz * 1.2; } return [x, z]; };
    [player.x, player.z] = clear(hall.x + hall.nx * 14 + tx * 2, hall.z + hall.nz * 14 + tz * 2, 1.5);
    player.heading = cam.yaw = Math.atan2(-hall.nx, -hall.nz);
    const [sx, sz] = clear(hall.x + hall.nx * 7.5 - tx * 4.5, hall.z + hall.nz * 7.5 - tz * 4.5, 1.2);
    if (!CLEAN) selfie = makeSelfie(sx, sz, Math.atan2(hall.nx, hall.nz));
  } else {
    const first0 = pois.get('Padiham Fire Station') || home;
    const dl = Math.hypot(first0.dx - home.dx, first0.dz - home.dz) || 1;
    player.x = home.dx; player.z = home.dz;
    for (let k = 0; k < 12 && (nearWall(player.x, player.z, 3) || k < 3); k++) { player.x += ((first0.dx - home.dx) / dl) * 1.5; player.z += ((first0.dz - home.dz) / dl) * 1.5; }
    player.heading = cam.yaw = Math.atan2(first0.dx - home.dx, first0.dz - home.dz);
  }
  if (!ROUTE) { placePlaques(); placeLitter(); }
  peds = new Pedestrians(scene, built.pedPaths, isTouch ? 16 : 34);
  peds.setCount(S.calm ? 6 : Infinity);
  if (!ROUTE) pigeons = new Pigeons(scene, built.spots, 7, (n) => {
    state.pigeons += n; persist(); hud();
    if (S.calm) return;
    sound.flap(n);
    toast(`${n} pigeons startled`, `That is ${state.pigeons} so far`, 1600);
  });
  // Stamps already earned by an older save are marked quietly, without a run of messages.
  for (const st of STAMPS) if (st[3]()) state.stamps.add(st[0]);
  hud(); if (ROUTE) startRoute(); else makeJob(); drawMini(); draftNote();
  const b = $('play');
  b.disabled = false; b.textContent = ROUTE ? 'Start the route' : state.done ? 'Carry on' : 'Play';
  b.addEventListener('click', () => { $('setSlot').append($('settings')); $('start').style.display = 'none'; state.playing = true; started = true; sound.start(); clearHeader(); stage.scrollIntoView({ block: 'start' }); });
  frame();
}).catch((err) => { console.error(err); $('jobTitle').textContent = 'Could not load the town'; $('jobText').textContent = String(err); });

// A handle for testing in the dev server only; it is not in the built site.
if (import.meta.env.DEV) window.__padiham = { quality, stepDown, chunks, player, state, cam, pois, doors, input: stickVec, scene, renderer, plaques, get world() { return world; }, get built() { return built; }, get pigeons() { return pigeons; }, litter, jump, hello, toggleRun, get peds() { return peds; }, draft, ROUTE };
