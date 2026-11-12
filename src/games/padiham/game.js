// The Padiham Job: a third-person walking game on the real street layout of Padiham.
// Map data (c) OpenStreetMap contributors, ODbL. Building outlines contain OS data (c) Crown copyright
// and database right 2026. The world file is built by scripts/padiham-game/build_world.py.
import * as THREE from 'three';
import { chunks, buildWorld, walls, roadGrid, riverGrid, distToSeg, nearWall } from './city.js';
import { cloudTexture } from './textures.js';
import { setTerrain, groundH } from './terrain.js';
import { makeFigure, animateFigure, animateSeated, Pedestrians, Pigeons, SKIN, HAIR } from './life.js';
import { Sound } from './audio.js';
import { buildPub } from './interior.js';
import { PLAQUES } from './history.js';
import { Buses } from './bus.js';
import { Streets, along, pointAt, nextTurn } from './route.js';

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
// How the game is being played: 'explore' (no job until one is asked for) or 'jobs'. A save from before the
// modes were added was a game of jobs. A new player has no mode until they pick one on the start screen.
let mode = save.mode === 'explore' || save.mode === 'jobs' ? save.mode : save.done ? 'jobs' : null;
function persist() {
  try { localStorage.setItem(SAVE_KEY, JSON.stringify({ mode, money: state.money, done: state.done, jobIndex: state.jobIndex, pigeons: state.pigeons, plaques: [...state.plaques], litter: state.litter, hellos: state.hellos, turns: state.turns, metres: Math.round(state.metres), dailies: state.dailies, pubs: [...state.pubs], stamps: [...state.stamps] })); } catch { /* private mode */ }
}
// Link options for supporters and public bodies, read once from the address. Nothing is stored or sent.
// ?calm=1 gives a quieter town: fewer passers-by and softer sound. ?edition=clean leaves out the selfie
// taker and keeps pub doors shut. Both can be combined: ?calm=1&edition=clean
const opts = new URLSearchParams(location.search);
// ?route=12_-45.130_20 is a practice route set by a supporter: a start and up to eleven markers, each a pair of
// whole metres on the game's own grid. The route is all there is in the link: no name, no words, nothing kept.
// The part of an address after the # is never sent to the website, so routes and trips are carried there.
// Older links that put the route in the query still work.
const frag = new URLSearchParams(location.hash.slice(1));
const ROUTE = (() => {
  const rawRoute = frag.get('route') || opts.get('route');
  if (!rawRoute || !/^-?\d+_-?\d+(\.-?\d+_-?\d+){1,11}$/.test(rawRoute)) return null;
  return { key: rawRoute, pts: rawRoute.split('.').map((q) => q.split('_').map(Number)) };
})();
// A trip is two named places and, if wanted, one bus ride between two stops: #trip=1.from.to or
// #trip=1.from.to.board.alight.service. Each part is a short code made from a public name (see indexEnds),
// so the link holds no words, no coordinates of anyone's home and nothing about the person it is for.
const TRIP_RAW = (() => { const t = frag.get('trip'); return !ROUTE && t && /^1\.[a-z0-9]{2,12}\.[a-z0-9]{2,12}(\.[a-z0-9]{2,12}){0,3}$/.test(t) ? t : null; })();
const PRACTICE = !!ROUTE || !!TRIP_RAW;
// A practice route or trip is always the plain version: no selfie taker and no pubs.
const CLEAN = opts.get('edition') === 'clean' || PRACTICE;
if (PRACTICE) stage.classList.add('practice');

// ---------- Comfort and access settings ----------
// Kept in the browser, and only the ones the player has changed: the rest follow the device (reduced motion,
// more contrast) or the calm link. S.text is 0, 1 or 2 for standard, large and largest panel text.
const SET_KEY = 'padiham-settings-1';
const mq = (q) => { try { return matchMedia(q).matches; } catch { return false; } };
const S = { calm: opts.get('calm') === '1' || PRACTICE, text: 0, clock: false, reduce: mq('(prefers-reduced-motion: reduce)'), contrast: mq('(prefers-contrast: more)'), hold: false, follow: true, lefty: false };
// Jobs are against the clock only if the player has asked for it, and never in the calm town.
const timed = () => S.clock && !S.calm;
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
  if (!timed()) $('timer').textContent = '';
  for (const i of stage.querySelectorAll('input[data-set]')) i.checked = !!S[i.dataset.set];
  for (const b of stage.querySelectorAll('button[data-text]')) b.setAttribute('aria-pressed', String(Number(b.dataset.text) === S.text));
}
function setSetting(k, v) {
  S[k] = v; chosen[k] = v;
  try { localStorage.setItem(SET_KEY, JSON.stringify(chosen)); } catch { /* private mode */ }
  applySettings();
}
// On a practice route calm is fixed, so its switch is put away.
if (PRACTICE) for (const i of stage.querySelectorAll('input[data-set="calm"]')) i.closest('label').hidden = true;
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
let world, built, slice, peds, pigeons, buses, homeStart = null;
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
function label(text, x, y, z, bg = 'rgba(20,22,26,.82)') {
  const cv = document.createElement('canvas');
  cv.width = 512; cv.height = 96;
  const g = cv.getContext('2d');
  g.font = '700 44px system-ui, sans-serif';
  const w = Math.min(500, g.measureText(text).width + 40);
  g.fillStyle = bg;
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
  paintTown(g);
}
// The town drawn flat, in the game's own metres: the caller sets the scale and the offset.
function paintTown(g) {
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
  g.setLineDash([]);
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
  if (buses) {
    mini.fillStyle = '#2f6fb5'; mini.strokeStyle = '#fff'; mini.lineWidth = 2;
    for (const st of buses.stops) { if (st.dead) continue; const bx = (st.x - player.x) * MS * zoom, bz = (st.z - player.z) * MS * zoom; if (Math.hypot(bx, bz) < S / 2) { mini.fillRect(S / 2 + bx - 5, S / 2 + bz - 5, 10, 10); mini.strokeRect(S / 2 + bx - 5, S / 2 + bz - 5, 10, 10); } }
  }
  mini.fillStyle = '#b8f26a';
  for (const l of litter) {
    const lx2 = (l.x - player.x) * MS * zoom, lz2 = (l.z - player.z) * MS * zoom;
    if (l.mesh.visible && Math.hypot(lx2, lz2) < S / 2) { mini.beginPath(); mini.arc(S / 2 + lx2, S / 2 + lz2, 4, 0, 7); mini.fill(); }
  }
  if (way && wayPos) {
    // The way along the streets, from where the player is now.
    mini.lineCap = mini.lineJoin = 'round'; mini.beginPath(); mini.moveTo(S / 2, S / 2);
    for (let i = wayPos.i + 1; i < way.pts.length; i++) mini.lineTo(S / 2 + (way.pts[i][0] - player.x) * MS * zoom, S / 2 + (way.pts[i][1] - player.z) * MS * zoom);
    mini.strokeStyle = '#14161a'; mini.lineWidth = 9; mini.stroke();
    mini.strokeStyle = '#ffcf33'; mini.lineWidth = 5; mini.stroke();
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
  // How the player gets about. It is a look and nothing more: every pace, place and job is the same for all three.
  move: [['foot', 'On foot'], ['chair', 'Wheelchair'], ['power', 'Powerchair']],
  style: [['short', 'Short hair'], ['pony', 'Ponytail'], ['long', 'Long hair'], ['cap', 'Cap']],
  hair: HAIR.map((c, i) => [i, ['Dark brown', 'Black', 'Fair', 'Grey', 'Auburn'][i]]),
  skin: SKIN.map((c, i) => [i, `Skin tone ${i + 1}`]),
  jacket: [[0xf08a24, 'Orange'], [0x2f6fb5, 'Blue'], [0x2f8a5a, 'Green'], [0x8a3fa0, 'Purple'], [0x3a3d42, 'Charcoal']].map(([c, n], i) => [i, n, c]),
};
const JACKET = LOOKS.jacket.map((j) => j[2]);
const look = (() => {
  const d = { move: 'foot', style: 'short', hair: 0, skin: 0, jacket: 0 };
  try { const v = JSON.parse(localStorage.getItem(LOOK_KEY) || '{}'); for (const k in d) if (LOOKS[k].some((o) => o[0] === v[k])) d[k] = v[k]; } catch { /* private mode */ }
  return d;
})();
const lookFigure = () => makeFigure({ seat: look.move === 'foot' ? undefined : look.move, jacket: JACKET[look.jacket], trousers: 0x2a2f3a, skin: SKIN[look.skin], hair: HAIR[look.hair], face: true, pack: true, style: look.style, cap: look.style === 'cap' ? 0x2b3a67 : undefined });
const player = { x: 0, z: 0, heading: 0, speed: 0, phase: 0, y: 0, vy: 0, wave: 0, trick: 0, rolled: 0, fig: lookFigure() };
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
  if (look.move === 'chair') {
    // A push wheel either side of the figure.
    g.strokeStyle = '#1b1d20'; g.lineWidth = 5;
    for (const x of [16, 104]) { g.beginPath(); g.ellipse(x, 128, 10, 24, 0, 0, 7); g.stroke(); }
    g.strokeStyle = '#b9bec6'; g.lineWidth = 2;
    for (const x of [16, 104]) { g.beginPath(); g.ellipse(x, 128, 6, 18, 0, 0, 7); g.stroke(); }
  }
  if (look.move === 'power') {
    // Armrests, with the joystick on one of them.
    g.fillStyle = '#24272c'; g.fillRect(6, 112, 20, 9); g.fillRect(94, 112, 20, 9); g.fillRect(8, 121, 6, 29); g.fillRect(106, 121, 6, 29);
    g.fillRect(101, 100, 4, 12); g.beginPath(); g.arc(103, 99, 4, 0, 7); g.fill();
  }
}
function setLook(k, v) {
  look[k] = v;
  try { localStorage.setItem(LOOK_KEY, JSON.stringify(look)); } catch { /* private mode */ }
  scene.remove(player.fig.group);
  player.fig = lookFigure();
  scene.add(player.fig.group);
  for (const b of document.querySelectorAll(`#sop-look button[data-k="${k}"]`)) b.setAttribute('aria-pressed', String(b.dataset.v === String(v)));
  drawLook();
  if (k === 'move') paceLabel();
}
{
  const host = document.getElementById('sop-lookOpts');
  if (host) {
    const NAMES = { move: 'Getting about', style: 'Hair', hair: 'Hair colour', skin: 'Skin', jacket: 'Jacket' }, SWATCH = { hair: HAIR, skin: SKIN, jacket: JACKET };
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
  else if (e.code === 'KeyB') busKey();
  else if (e.code === 'KeyG') whereNext();
  else if (e.code === 'KeyM') openBig();
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
// A button pressed with the mouse or a finger must not keep the keyboard: left there, Space would press it again
// and the action keys would be ignored. A button reached and pressed from the keyboard keeps its focus.
for (const b of stage.querySelectorAll('.sop-buttons button, #sop-bell, #sop-run, #sop-jump, #sop-hello')) b.addEventListener('click', (e) => { if (e.detail > 0 && state.playing) view.focus({ preventScroll: true }); });

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
const PACE = [4.6, 9.5, 14], PACE_NAME = { foot: ['Walk', 'Run', 'Sprint'], wheels: ['Steady', 'Quick', 'Fast'] };
const onWheels = () => look.move !== 'foot';
let gear = 0;
// The words on the pace and jump buttons follow how the player gets about.
function paceLabel() {
  const b = $('run'), name = PACE_NAME[onWheels() ? 'wheels' : 'foot'][gear];
  b.textContent = name.toUpperCase(); b.classList.toggle('on', gear > 0); b.classList.toggle('fast', gear === 2);
  b.setAttribute('aria-label', `Pace: ${name.toLowerCase()}. Press to change`);
  const j = $('jump'), trick = look.move === 'chair' ? 'Wheelie' : look.move === 'power' ? 'Spin' : 'Jump';
  j.textContent = trick.toUpperCase(); j.classList.toggle('long', trick.length > 4);
  return name;
}
function toggleRun() {
  gear = (gear + 1) % (PRACTICE ? 2 : 3);
  const name = paceLabel();
  say(onWheels() ? `${name} pace` : ['Walking', 'Running', 'Sprinting'][gear]);
}
$('run').addEventListener('click', toggleRun);
const buzz = (ms) => { if (isTouch && !S.reduce && navigator.vibrate) { try { navigator.vibrate(ms); } catch { /* not allowed */ } } };
// A hop, for the fun of it. It changes nothing about where the player can go.
function jump() {
  if (!state.playing || player.y > 0 || player.trick > 0) return;
  // In a wheelchair the same button is a wheelie, and in a powerchair a turn on the spot.
  if (onWheels()) { player.trick = look.move === 'chair' ? 0.8 : 0.9; sound.step(true); return; }
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

// ---------- The way along the streets ----------
// The arrow, the distance and the line on the map follow the streets and footpaths, not a straight line, so the
// way never leads into a wall or the river. It is worked out again when the player strays from it.
let streets = null, way = null, wayFor = null, wayAt = -9, wayPos = null;
// Whether two spots can be joined on foot in a straight line: no tall wall and no river between them.
function joined(ax, az, bx, bz) {
  if (wallHit(ax, az, bx, bz) < 1) return false;
  const n = Math.ceil(Math.hypot(bx - ax, bz - az) / 3);
  for (let i = 1; i < n; i++) if (inRiver(ax + ((bx - ax) * i) / n, az + ((bz - az) * i) / n)) return false;
  return true;
}
// Whether any wall, tall or low, crosses the line from A to B.
function crossesWall(ax, az, bx, bz) {
  const rx = bx - ax, rz = bz - az, n = Math.max(1, Math.ceil(Math.hypot(rx, rz) / 6));
  for (let i = 0; i <= n; i++) for (const s of walls.at(ax + (rx * i) / n, az + (rz * i) / n)) {
    const sx = s[2] - s[0], sz = s[3] - s[1], den = rx * sz - rz * sx;
    if (Math.abs(den) < 1e-9) continue;
    const t = ((s[0] - ax) * sz - (s[1] - az) * sx) / den, u = ((s[0] - ax) * rz - (s[1] - az) * rx) / den;
    if (t > 0 && t < 1 && u >= 0 && u <= 1) return true;
  }
  return false;
}
// A stretch of road is shut if a walker sent along it, sliding round whatever is in the way as the player does,
// cannot reach the far end. Some building outlines in the town file clip a carriageway: those can be walked
// round and stay open. Only stretches a wall crosses are tried, so this costs little when the town is loaded.
function shutRoad(ax, az, bx, bz) {
  if (!crossesWall(ax, az, bx, bz)) return false;
  const o = { x: ax, z: az }, max = Math.ceil((Math.hypot(bx - ax, bz - az) * 3) / 0.3) + 20;
  pushOut(o);
  for (let k = 0; k < max; k++) {
    const dx = bx - o.x, dz = bz - o.z, d = Math.hypot(dx, dz);
    if (d < 1.5) return false;
    o.x += (dx / d) * 0.3; o.z += (dz / d) * 0.3; pushOut(o);
  }
  return true;
}
// Where the way leads now. Before a bus ride it is the stop to board at; after it, the place itself.
function guideTarget() {
  const j = state.job; if (!j) return null;
  const s = j.steps[j.i];
  return s.ride && !s.rode ? { dx: s.ride.stop.x, dz: s.ride.stop.z } : s.poi;
}
// { m: metres left by the streets, x, z: the spot to walk towards now, turn: the next change of street }
function guide(time) {
  const tg = guideTarget(), riding = !!buses && buses.riding;
  if (riding && state.job) state.job.steps[state.job.i].rode = true;
  if (!tg || state.inside || riding) { way = null; wayFor = null; wayPos = null; return null; }
  const straight = Math.hypot(tg.dx - player.x, tg.dz - player.z);
  if (!streets) return { m: straight, x: tg.dx, z: tg.dz, turn: null };
  wayPos = way && wayFor === tg ? along(way, player.x, player.z) : null;
  if ((!wayPos || wayPos.off > 14) && (wayFor !== tg || time - wayAt > 1)) {
    way = streets.route(player.x, player.z, tg.dx, tg.dz, joined); wayFor = tg; wayAt = time;
    wayPos = way ? along(way, player.x, player.z) : null;
  }
  if (!way || !wayPos) return { m: straight, x: tg.dx, z: tg.dz, turn: null };
  // Close by and in plain sight, walk straight to it. Otherwise walk towards a spot a little further along the way.
  if (straight < 24 && joined(player.x, player.z, tg.dx, tg.dz)) return { m: straight, x: tg.dx, z: tg.dz, turn: null };
  const [x, z] = pointAt(way, wayPos.s + 14);
  return { m: Math.max(straight, way.len - wayPos.s + wayPos.off), x, z, turn: nextTurn(way, wayPos.s) };
}
// The distance on foot between two spots by the streets, or the straight line if no way is found.
function walkLen(ax, az, bx, bz) {
  const r = streets && streets.route(ax, az, bx, bz, joined);
  return r ? r.len : Math.hypot(bx - ax, bz - az);
}
const turnWords = (t) => (t && t.m < 400 ? `In ${Math.max(5, Math.round(t.m / 5) * 5)} metres, ${t.side ? `turn ${t.side} into` : 'carry on into'} ${t.name}` : '');
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
const routeKey = () => `padiham-route-${ROUTE ? ROUTE.key : 'trip-' + TRIP_RAW}`;
const routeCount = () => { try { return Number(localStorage.getItem(routeKey())) || 0; } catch { return 0; } };
// ---------- Trips: named ends, and one bus ride if wanted ----------
// Every place and stop gets a short code made from its public name, so a link survives the town being rebuilt
// as long as the name stays. Two stops with one name (either side of a road) are told apart by their order
// west to east. Proper stop codes (NaPTAN) would be better and are not in the town file yet.
const hash36 = (str) => { let h = 2166136261; for (const c of str) { h ^= c.charCodeAt(0); h = Math.imul(h, 16777619); } return (h >>> 0).toString(36); };
let ends = [], services = [], trip = null;
function indexEnds() {
  const seen = new Map(), code = (pre, key) => { const n = seen.get(pre + key) || 0; seen.set(pre + key, n + 1); return pre + hash36(key) + (n || ''); };
  const order = (a, b) => (a.n < b.n ? -1 : a.n > b.n ? 1 : a.x - b.x || a.z - b.z);
  ends = [];
  for (const p of [...slicePois].sort(order)) ends.push({ id: code('p', p.n), n: p.n, label: p.n, x: p.dx, z: p.dz });
  for (const st of [...buses.stops].sort(order)) {
    const o = buses.options(st)[0];
    ends.push({ id: code('s', st.n), n: st.n, stop: st, x: st.x, z: st.z, label: `${st.n} bus stop (${o ? `${o.route.ref} to ${o.route.to}` : 'no buses in the game'})` });
  }
  services = buses.routes.map((r) => ({ id: code('r', `${r.ref}>${r.to}`), route: r }));
}
const endOf = (stop) => ends.find((q) => q.stop === stop);
const apart = (a, b) => Math.hypot(a.x - b.x, a.z - b.z);
function resolveTrip(raw) {
  const [, a, b, c, d, e] = raw.split('.'), from = ends.find((q) => q.id === a), to = ends.find((q) => q.id === b);
  if (!from || !to || from === to) return null;
  const t = { from, to };
  if (c) {
    const board = ends.find((q) => q.id === c && q.stop), off = ends.find((q) => q.id === d && q.stop), sv = services.find((q) => q.id === e);
    if (!board || !off || !sv || !buses.options(board.stop).some((o) => o.route === sv.route && o.later.includes(off.stop))) return null;
    t.bus = { board, off, route: sv.route };
  }
  return t;
}
const endName = (e) => (e.stop ? `the ${e.n} bus stop` : e.n);
function tripSteps(t) {
  const at = (e) => ({ n: endName(e), dx: e.x, dz: e.z }), steps = [];
  if (t.bus) {
    const { board, off, route } = t.bus;
    if (apart(t.from, board) > 8) steps.push({ poi: at(board), text: `Walk to ${endName(board)}` });
    // The ride: the marker stands at the stop to get off at. It is reached on foot from the bus, never while riding.
    steps.push({ poi: at(off), text: `Catch the ${route.ref} to ${route.to} from ${endName(board)}. Ring the bell for ${off.n}`, ride: { stop: board.stop, route }, r: 16 });
    if (apart(off, t.to) > 8) steps.push({ poi: at(t.to), text: `Walk to ${endName(t.to)}` });
  } else steps.push({ poi: at(t.to), text: `Walk to ${endName(t.to)}` });
  return steps;
}
function startRoute() {
  const clampX = (x) => Math.min(slice[2] - 3, Math.max(slice[0] + 3, x)), clampZ = (z) => Math.min(slice[3] - 3, Math.max(slice[1] + 3, z));
  stopBus();
  let p0, steps;
  if (trip) { p0 = [clampX(trip.from.x), clampZ(trip.from.z)]; steps = tripSteps(trip); }
  else {
    const p = ROUTE.pts.map(([x, z]) => [clampX(x), clampZ(z)]);
    p0 = p[0];
    steps = p.slice(1).map(([x, z], i) => {
      const nm = spotName(x, z), last = i === p.length - 2;
      return { poi: { n: last ? `the end of the route, ${nm}` : `the marker ${nm}`, dx: x, dz: z }, text: last ? `Walk to the end of the route, ${nm}` : `Walk to the marker ${nm}` };
    });
  }
  player.x = p0[0]; player.z = p0[1]; player.speed = 0;
  for (let k = 0; k < 12 && nearWall(player.x, player.z, 0.8); k++) { player.x += 0.8; player.z += 0.5; }
  player.heading = cam.yaw = Math.atan2(steps[0].poi.dx - player.x, steps[0].poi.dz - player.z);
  state.job = { title: trip ? 'Your trip' : 'Practice route', route: true, steps, i: 0, t: 0, par: 0, pay: 0 };
  showStep();
}
function routeArrive(j) {
  buzz(30);
  if (j.i < j.steps.length - 1) { j.i++; showStep(); sound.pickup(); toast('Well done', j.steps[j.i].text, 3200); return; }
  const n = routeCount() + 1;
  try { localStorage.setItem(routeKey(), String(n)); } catch { /* private mode */ }
  state.job = null; beam.visible = false; sound.done(); stage.classList.add('nojob');
  const done = trip ? 'You have arrived' : 'Route finished';
  $('jobTitle').textContent = done; $('jobText').textContent = `${trip ? `You are at ${endName(trip.to)}. You have done this trip` : 'You have walked it'} ${plural(n, 'time')}. Press Walk it again to start over`; $('dist').textContent = '';
  toast(done, `Well done. That is ${plural(n, 'time')}`, 4000);
}
$('again').addEventListener('click', () => { if ((ROUTE || trip) && started) { closeToast(); startRoute(); say('Back at the start of the route'); } });

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
  const url = `${location.origin}${location.pathname}#route=${draft.map((q) => q.join('_')).join('.')}`;
  const box = $('routeLink'); box.value = url; box.hidden = false;
  try { await navigator.clipboard.writeText(url); draftNote('Link copied. Anyone who opens it gets this route'); } catch { box.select(); draftNote('Copy the link from the box below'); }
});

// The bonus time allows for running most of the way by the streets, with a little to spare at each stop.
const parFor = (d, stops) => Math.round((d * 1.15) / 8 + 8 * stops);
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
  if (def) job = { title: `Job ${state.jobIndex + 1}: ${def.title}`, noClock: state.jobIndex === 0, steps: def.steps.map(([n, text]) => ({ poi: pois.get(n), text })) };
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
  for (const s of job.steps) { d += walkLen(prev.dx, prev.dz, s.poi.dx, s.poi.dz); prev = s.poi; }
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
  beam.visible = true; stage.classList.remove('nojob');
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
  const g = guide(clock.elapsedTime);
  if (!g) return null;
  return { m: Math.round(g.m / 5) * 5, words: bearingWords(wrap(Math.atan2(g.x - player.x, g.z - player.z) - cam.yaw)), turn: turnWords(g.turn) };
}
function whereNext() {
  if (state.inside) { toast(state.inside.pub.name, 'Walk back to the door to leave'); return; }
  const w = wayNow();
  if (!w) return;
  toast(state.job.steps[state.job.i].text, `${w.m} metres by the streets, ${w.words}. ${w.turn ? w.turn + '. ' : ''}You are on ${$('street').textContent}`, 5000);
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
  const bonus = timed() && !j.noClock && j.t <= j.par ? Math.round(j.pay / 2) : 0;
  state.money += j.pay + bonus; state.done++; buzz(60);
  if (j.turn) state.turns++;
  if (j.daily) state.dailies++;
  if (!j.daily && state.jobIndex < SCRIPT.length) state.jobIndex++;
  persist(); hud(); sound.done();
  if (j.daily) {
    const old = dailyResult(), r = { date: j.daily, t: Math.round(j.t), par: j.par, calm: !timed() };
    if (!old || old.date !== r.date || old.calm || (!r.calm && r.t < old.t)) { try { localStorage.setItem(DAILY_KEY, JSON.stringify(r)); } catch { /* private mode */ } }
    toast("Today's job done!", !timed() ? 'Open the menu to share it' : `${mmss(j.t)}. Open the menu to share it`, 4000);
    nextJob(); checkStamps();
    return;
  }
  if (j.turn) toast('Good turn done', THANKS[Math.floor(Math.random() * THANKS.length)], 3200);
  else toast(`Job done! +£${j.pay + bonus}`, bonus ? `Includes a £${bonus} speed bonus` : !timed() ? '' : j.noClock ? `The next job pays a bonus for being quick. ${isTouch ? 'Tap WALK' : 'Press R'} to run` : 'Beat the clock next time for a bonus', j.noClock ? 4200 : 2600);
  nextJob(); checkStamps();
}
// With no job in hand the card says so and the arrow is put away.
function idleCard() {
  state.job = null; beam.visible = false; way = null; wayFor = null; wayPos = null;
  $('jobTitle').textContent = 'Exploring'; $('jobText').textContent = `Wander where you like. ${isTouch ? 'Open the menu' : 'Press Esc'} if you want a job`;
  $('dist').textContent = ''; $('timer').textContent = ''; stage.classList.add('nojob');
}
const nextJob = () => { if (mode === 'explore') idleCard(); else makeJob(); };
const MODE_NAME = { explore: 'Explore', jobs: 'Jobs' };
function setMode(m) {
  mode = m; persist();
  $('modeBtn').textContent = m === 'explore' ? 'Take jobs' : 'Just explore';
  if (!world || PRACTICE) return;
  if (m === 'jobs' && !state.job) makeJob();
  else if (m === 'explore' && state.job && !state.job.daily) idleCard();
}
function hud() { $('money').textContent = state.money; $('done').textContent = `${plural(state.done, 'job')} done`; }

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
  ['mile', 'Out and about', 'Cover a mile', () => state.metres >= 1609],
  ['five', 'Five mile club', 'Cover five miles', () => state.metres >= 8047],
  ['pigeon', 'Pigeon bother', 'Startle fifty pigeons', () => state.pigeons >= 50],
  ['pubs', 'Both bars', 'Call in at both pubs', () => state.pubs.size >= 2],
].filter((st) => !(CLEAN && st[0] === 'pubs'));
function checkStamps() {
  if (PRACTICE) return;
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
let dailyAsked = false;
function startDaily() {
  const job = dailyJob();
  if (!job) { note('No job could be made today'); return; }
  // Part way through a job, the first press only asks. A job not yet begun is simply offered again afterwards.
  if (state.job && !state.job.daily && state.job.i > 0 && !dailyAsked) { dailyAsked = true; note("You are part way through a job. Press Today's job again to leave it"); return; }
  if (state.inside) leavePub();
  stopBus();
  // Everyone sets off from the same spot, outside the Town Hall: where a new game starts, facing the same way.
  if (homeStart) { player.x = homeStart.x; player.z = homeStart.z; player.heading = cam.yaw = homeStart.h; player.speed = 0; }
  else { player.x = job.start.dx; player.z = job.start.dz; for (let k = 0; k < 12 && nearWall(player.x, player.z, 1.2); k++) { player.x += 1; player.z += 0.6; } }
  let d = 0, prev = { dx: player.x, dz: player.z };
  for (const st of job.steps) { d += walkLen(prev.dx, prev.dz, st.poi.dx, st.poi.dz); prev = st.poi; }
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
  $('tally').textContent = `${plural(state.done, 'job')} done, ${plural(state.turns, 'good turn')}, ${plural(state.litter, 'piece')} of litter picked up, ${plural(state.hellos, 'hello')} said, ${plural(state.pigeons, 'pigeon')} startled and ${miles(state.metres)} covered.`;
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
  closeBusMenu();
  state.playing = false; keys.clear(); stickVec.x = stickVec.y = 0; persist();
  $('full').textContent = stage.classList.contains('fill') || document.fullscreenElement ? 'Leave full screen' : 'Full screen';
  $('dailyShare').hidden = !dailyLine();
  dailyAsked = false; newAsked = false; renderJournal(); note(''); $('last').textContent = lastMsg ? `Last message: ${lastMsg}` : '';
  menu.classList.add('on'); $('resume').focus();
}
function closeMenu() { if (!menu.classList.contains('on')) return; menu.classList.remove('on'); state.playing = true; view.focus({ preventScroll: true }); }
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
$('modeBtn').addEventListener('click', () => {
  const m = mode === 'explore' ? 'jobs' : 'explore';
  setMode(m); closeMenu();
  toast(m === 'jobs' ? 'Jobs' : 'Exploring', m === 'jobs' ? 'Follow the arrow to the first place' : 'No jobs for now. Wander where you like', 3000);
});
// Starting again wipes the jobs, money, stamps and plaques. The look and the settings stay. The first press asks.
let newAsked = false;
$('newGame').addEventListener('click', () => {
  if (!newAsked) { newAsked = true; note('This wipes your jobs, money, stamps and plaques. Your look and settings stay. Press Start a new game again to do it'); return; }
  Object.assign(state, { money: 0, done: 0, jobIndex: 0, pigeons: 0, litter: 0, hellos: 0, turns: 0, metres: 0, dailies: 0 });
  state.plaques.clear(); state.pubs.clear(); state.stamps.clear(); mode = null;
  try { localStorage.removeItem(HINT_KEY); } catch { /* private mode */ }
  persist(); location.reload();
});
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
addEventListener('keydown', (e) => { if (e.code === 'Escape' && started) { if (busMenu.classList.contains('on')) closeBusMenu(); else if (menu.classList.contains('on')) closeMenu(); else openMenu(); } });
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

// ---------- Buses ----------
// At a stop a button offers the buses that call there. The player picks one by its number and where it is
// going, waits for it, rides, and rings the bell before the stop they want. See bus.js.
let atStop = null, camSaved = null, snapCam = false, boardView = false;
const busUi = {
  toast, say, fade,
  ding() { sound.tone(1320, 0.28, 0.11); sound.tone(1100, 0.4, 0.11, 0.2); buzz(30); },
  panel(text, canRing) {
    const el = $('ride');
    $('askBtns').textContent = '';
    if (!text) { el.classList.remove('on'); return; }
    $('rideText').textContent = text; $('bell').hidden = !canRing;
    el.classList.add('on');
  },
  // A question from someone, answered with one of a few large buttons. There is no wrong answer.
  ask(text, choices, done) {
    const host = $('askBtns'); host.textContent = '';
    $('rideText').textContent = text; $('bell').hidden = true;
    choices.forEach((c, i) => {
      const b = document.createElement('button'); b.type = 'button'; b.textContent = c;
      b.addEventListener('click', () => { host.textContent = ''; view.focus({ preventScroll: true }); done(i); });
      host.append(b);
    });
    $('ride').classList.add('on'); say(`${text}. ${choices.join(', or ')}`);
    host.querySelector('button').focus({ preventScroll: true });
  },
  // Getting on: the view comes down to the pavement by the front door, facing the bus, and stays there while
  // the player speaks to the driver. The door is on the left of the bus, the pavement side.
  board(h) {
    camSaved = { dist: cam.dist, pitch: cam.pitch };
    const dx = Math.sin(h) * 0.8 + Math.cos(h) * 0.6, dz = Math.cos(h) * 0.8 - Math.sin(h) * 0.6;
    cam.yaw = Math.atan2(-dx, -dz); cam.dist = cam.cur = 9.5; cam.pitch = 0.2; boardView = true;
    player.fig.group.visible = false; player.speed = 0; sound.pickup();
  },
  // Setting off: the view lifts above the roofs and goes behind the bus.
  depart() { boardView = false; cam.dist = 17; cam.pitch = 0.78; snapCam = true; },
  alight(x, z, heading) {
    player.x = x; player.z = z;
    for (let k = 0; k < 12 && nearWall(player.x, player.z, 0.8); k++) { player.x += Math.sin(heading) * 0.8; player.z += Math.cos(heading) * 0.8; }
    boardView = false; player.fig.group.visible = true; player.speed = 0;
    if (camSaved) { cam.dist = camSaved.dist; cam.pitch = camSaved.pitch; camSaved = null; }
    sound.done(); buzz(50);
  },
};
function stopBus() { boardView = false; if (buses && buses.busy) { const was = buses.riding; buses.cancel(); if (was) { player.fig.group.visible = true; if (camSaved) { cam.dist = camSaved.dist; cam.pitch = camSaved.pitch; camSaved = null; } } } }
const busMenu = $('busMenu');
function openBusMenu() {
  if (!atStop || !buses || buses.busy || !state.playing) return;
  const opts = buses.options(atStop), list = $('busList'), want = state.job && state.job.steps[state.job.i].ride;
  list.textContent = '';
  $('busStopName').textContent = `Bus stop: ${atStop.n}`;
  $('busNone').hidden = opts.length > 0;
  for (const o of opts) {
    const b = document.createElement('button'); b.type = 'button';
    const big = document.createElement('b'); big.textContent = `${o.route.ref} to ${o.route.to}`;
    const sm = document.createElement('span'); sm.textContent = `Stops at ${o.later.map((q) => q.n).join(', ')}`;
    b.append(big, sm);
    const stop = atStop;
    b.addEventListener('click', () => { closeBusMenu(); buses.call(stop, o); });
    if (want && want.stop === stop && want.route === o.route) { const em = document.createElement('em'); em.textContent = 'This is your bus'; b.prepend(em); b.classList.add('mine'); list.prepend(b); }
    else list.appendChild(b);
  }
  state.playing = false; keys.clear(); stickVec.x = stickVec.y = 0;
  busMenu.classList.add('on');
  (list.querySelector('button') || $('busClose')).focus();
}
function closeBusMenu() { if (!busMenu.classList.contains('on')) return; busMenu.classList.remove('on'); state.playing = true; view.focus({ preventScroll: true }); }
$('busClose').addEventListener('click', closeBusMenu);
$('busBtn').addEventListener('click', openBusMenu);
$('bell').addEventListener('click', () => buses && buses.ring());
function busKey() { const first = $('askBtns').querySelector('button'); if (first) first.click(); else if (buses && buses.phase === 'ride') buses.ring(); else openBusMenu(); }

// ---------- Planning a trip ----------
// A helper, or the player, picks where a trip starts and ends and whether part of it is by bus. The choice
// becomes a link. Starting it reloads the page on that link, so the game is always in one clear state.
const tripBox = $('trip');
const pick = (id) => ends.find((q) => q.id === $(id).value);
function fillTripPicker() {
  for (const [id, first] of [['tripFrom', 'Padiham Town Hall'], ['tripTo', 'Tesco']]) {
    const sel = $(id); sel.textContent = '';
    for (const [title, test] of [['Places', (q) => !q.stop], ['Bus stops', (q) => !!q.stop]]) {
      const grp = document.createElement('optgroup'); grp.label = title;
      for (const q of ends.filter(test)) { const o = document.createElement('option'); o.value = q.id; o.textContent = q.label; if (q.n === first && !q.stop) o.selected = true; grp.append(o); }
      sel.append(grp);
    }
    sel.addEventListener('change', tripWays);
  }
  tripWays();
}
// The ways to make the trip: on foot, and up to three buses that save a fair part of the walk.
function tripWays() {
  const a = pick('tripFrom'), b = pick('tripTo'), host = $('tripWays'); host.textContent = '';
  $('tripGo').disabled = $('tripCopy').disabled = !a || !b || a === b; $('tripLink').hidden = true;
  if (!a || !b || a === b) { $('tripNote').textContent = 'Pick two different places'; return; }
  $('tripNote').textContent = '';
  const direct = apart(a, b), ways = [{ code: `1.${a.id}.${b.id}`, text: `Walk all the way. About ${Math.round(walkLen(a.x, a.z, b.x, b.z) / 10) * 10} metres by the streets` }], rides = [];
  for (const B of ends) {
    if (!B.stop || apart(a, B) > 300) continue;
    for (const o of buses.options(B.stop)) for (const L of o.later) {
      const A = endOf(L), walk = apart(a, B) + apart(A, b);
      if (apart(A, b) <= 300 && walk < direct - 60) rides.push({ walk, B, A, o });
    }
  }
  rides.sort((p, q) => p.walk - q.walk);
  const used = new Set();
  for (const r of rides) {
    if (used.has(r.o.route) || used.size >= 3) continue; used.add(r.o.route);
    const sv = services.find((q) => q.route === r.o.route);
    ways.push({ code: `1.${a.id}.${b.id}.${r.B.id}.${r.A.id}.${sv.id}`, text: `By bus. ${r.B === a ? '' : `Walk to the ${r.B.n} stop. `}Catch the ${r.o.route.ref} to ${r.o.route.to} and get off at ${r.A.n}${r.A === b ? '' : `. Then walk to ${endName(b)}`}` });
  }
  ways.forEach((w, i) => {
    const lab = document.createElement('label'), inp = document.createElement('input'), sp = document.createElement('span');
    inp.type = 'radio'; inp.name = 'sop-way'; inp.value = w.code; inp.checked = i === 0; sp.textContent = w.text;
    inp.addEventListener('change', () => { $('tripLink').hidden = true; });
    lab.append(inp, sp); host.append(lab);
  });
}
const tripUrl = () => { const w = tripBox.querySelector('input[name="sop-way"]:checked'); return w ? `${location.origin}${location.pathname}#trip=${w.value}` : null; };
function openTrip() { tripBox.classList.add('on'); $('tripFrom').focus(); }
function closeTrip() { if (!tripBox.classList.contains('on')) return; tripBox.classList.remove('on'); if (menu.classList.contains('on')) $('tripBtn').focus(); else if (!started) $('tripOpen').focus(); }
$('tripOpen').addEventListener('click', openTrip);
$('tripBtn').addEventListener('click', openTrip);
$('tripClose').addEventListener('click', closeTrip);
addEventListener('keydown', (e) => { if (e.code === 'Escape' && tripBox.classList.contains('on')) { e.stopImmediatePropagation(); closeTrip(); } }, true);
// Going to a link that differs only after the # does not reload the page, so the address is set and the page reloaded.
function goTo(url) { persist(); history.replaceState(null, '', url); location.reload(); }
$('tripGo').addEventListener('click', () => { const u = tripUrl(); if (u) goTo(u); });
$('tripCopy').addEventListener('click', async () => {
  const u = tripUrl(), box = $('tripLink'); if (!u) return;
  box.value = u; box.hidden = false;
  try { await navigator.clipboard.writeText(u); $('tripNote').textContent = 'Link copied. Anyone who opens it gets this trip'; } catch { box.select(); $('tripNote').textContent = 'Copy the link from the box below'; }
});
$('leave').addEventListener('click', () => goTo(location.pathname));
// The plan on the start screen: every step of the trip in order, before anything happens.
function showPlan() {
  const ol = $('plan'); ol.textContent = '';
  if (!trip) return;
  for (const t of [`Start at ${endName(trip.from)}`, ...state.job.steps.map((q) => q.text), `Arrive at ${endName(trip.to)}`]) { const li = document.createElement('li'); li.textContent = t; ol.append(li); }
}

// ---------- The full map ----------
// Opened with M, from the menu, or by pressing the small map. The game waits while it is open. It shows the whole
// of the town that can be walked, with place names, the bus stops that work, plaques already found and the way
// to the next place. It can be dragged, and zoomed with the buttons, the wheel or the plus and minus keys.
const bigBox = $('big'), bigCv = $('bigMap'), bigG = bigCv.getContext('2d');
const big = { x: 0, z: 0, k: 1, min: 0.3, max: 4, queued: false };
function bigFit() {
  const r = bigCv.getBoundingClientRect();
  big.min = Math.min(r.width / (slice[2] - slice[0] + 40), r.height / (slice[3] - slice[1] + 40));
  big.k = big.min; big.x = (slice[0] + slice[2]) / 2; big.z = (slice[1] + slice[3]) / 2;
}
function bigClamp() {
  big.k = Math.min(big.max, Math.max(big.min, big.k));
  big.x = Math.min(slice[2], Math.max(slice[0], big.x)); big.z = Math.min(slice[3], Math.max(slice[1], big.z));
}
function drawBig() {
  big.queued = false;
  if (!bigBox.classList.contains('on')) return;
  const r = bigCv.getBoundingClientRect(), dpr = Math.min(2, devicePixelRatio || 1), W = Math.round(r.width * dpr), H = Math.round(r.height * dpr);
  if (bigCv.width !== W || bigCv.height !== H) { bigCv.width = W; bigCv.height = H; }
  const g = bigG, k = big.k * dpr, X = (x) => W / 2 + (x - big.x) * k, Z = (z) => H / 2 + (z - big.z) * k;
  g.setTransform(1, 0, 0, 1, 0, 0);
  g.fillStyle = '#8f987c'; g.fillRect(0, 0, W, H);
  g.setTransform(k, 0, 0, k, W / 2 - big.x * k, H / 2 - big.z * k);
  paintTown(g);
  g.setTransform(1, 0, 0, 1, 0, 0);
  g.lineCap = g.lineJoin = 'round';
  // The way to the next place, then the places after it.
  if (way) {
    g.beginPath(); g.moveTo(X(player.x), Z(player.z));
    for (let i = (wayPos ? wayPos.i : 0) + 1; i < way.pts.length; i++) g.lineTo(X(way.pts[i][0]), Z(way.pts[i][1]));
    g.strokeStyle = '#14161a'; g.lineWidth = 8 * dpr; g.stroke();
    g.strokeStyle = '#ffcf33'; g.lineWidth = 4.5 * dpr; g.stroke();
  }
  // Names are placed in order of how much they matter, and one that would cover another is left out.
  const taken = [], fs = 12.5 * dpr;
  const name = (text, x, y, colour = '#fff', must = false, italic = false) => {
    g.font = `${italic ? 'italic 600' : '700'} ${fs}px system-ui, sans-serif`;
    const w = g.measureText(text).width, box = [x - w / 2 - 4 * dpr, y - fs * 0.75, x + w / 2 + 4 * dpr, y + fs * 0.75];
    if (box[2] < 0 || box[0] > W || box[3] < 0 || box[1] > H) return false;
    if (!must && taken.some((t) => box[0] < t[2] && box[2] > t[0] && box[1] < t[3] && box[3] > t[1])) return false;
    taken.push(box);
    g.textAlign = 'center'; g.textBaseline = 'middle'; g.lineWidth = 3.5 * dpr; g.strokeStyle = 'rgba(20,22,26,.9)';
    g.strokeText(text, x, y); g.fillStyle = colour; g.fillText(text, x, y);
    return true;
  };
  const dot = (x, y, rad, fill, line = '#14161a') => { g.beginPath(); g.arc(x, y, rad * dpr, 0, 7); g.fillStyle = fill; g.fill(); g.lineWidth = 2 * dpr; g.strokeStyle = line; g.stroke(); };
  const j = state.job, next = j ? j.steps[j.i].poi : null;
  if (j) for (const st of j.steps.slice(j.i + 1)) dot(X(st.poi.dx), Z(st.poi.dz), 6, 'rgba(255,207,51,.45)');
  if (next) { dot(X(next.dx), Z(next.dz), 9, '#ffcf33'); name(next.n.replace(/^the /, ''), X(next.dx), Z(next.dz) - 19 * dpr, '#ffcf33', true); }
  // The player: an arrow pointing the way they face.
  g.save(); g.translate(X(player.x), Z(player.z)); g.rotate(-player.heading + Math.PI); g.scale(dpr, dpr);
  g.fillStyle = '#ff5a3c'; g.strokeStyle = '#fff'; g.lineWidth = 2.5;
  g.beginPath(); g.moveTo(0, -13); g.lineTo(9, 10); g.lineTo(0, 5); g.lineTo(-9, 10); g.closePath(); g.fill(); g.stroke(); g.restore();
  taken.push([X(player.x) - 12 * dpr, Z(player.z) - 12 * dpr, X(player.x) + 12 * dpr, Z(player.z) + 12 * dpr]);
  name('You', X(player.x), Z(player.z) + 21 * dpr, '#ffd9cf', true);
  if (buses) for (const st of buses.stops) {
    if (st.dead) continue;
    const x = X(st.x), y = Z(st.z), q = 5 * dpr;
    g.fillStyle = '#2f6fb5'; g.strokeStyle = '#fff'; g.lineWidth = 1.5 * dpr; g.fillRect(x - q, y - q, q * 2, q * 2); g.strokeRect(x - q, y - q, q * 2, q * 2);
  }
  for (const p of plaques) if (state.plaques.has(p.id)) dot(X(p.x), Z(p.z), 4.5, '#fff', '#2f6fb5');
  for (const p of slicePois) { if (p === next) continue; if (name(p.n.replace('Church of England', 'CE'), X(p.dx), Z(p.dz) - 11 * dpr)) dot(X(p.dx), Z(p.dz), 3, '#14161a', '#fff'); }
  if (buses && big.k > 0.9) for (const st of buses.stops) if (!st.dead) name(`${st.n} stop`, X(st.x), Z(st.z) + 14 * dpr, '#cfe4ff');
  // Street names, once each, at the middle of the longest stretch that carries the name.
  if (big.k > 0.7) {
    const longest = new Map();
    for (const rd of world.roads) {
      if (!rd.n || rd.c === 'foot' || rd.c === 'service') continue;
      let len = 0; for (let i = 1; i < rd.p.length; i++) len += Math.hypot(rd.p[i][0] - rd.p[i - 1][0], rd.p[i][1] - rd.p[i - 1][1]);
      const m = rd.p[Math.floor((rd.p.length - 1) / 2)], m2 = rd.p[Math.ceil((rd.p.length - 1) / 2)], old = longest.get(rd.n);
      if (!old || len > old.len) longest.set(rd.n, { len, x: (m[0] + m2[0]) / 2, z: (m[1] + m2[1]) / 2 });
    }
    for (const [n, q] of [...longest].sort((a, b) => b[1].len - a[1].len)) if (q.x > slice[0] && q.x < slice[2] && q.z > slice[1] && q.z < slice[3]) name(n, X(q.x), Z(q.z), '#efe9d8', false, true);
  }
}
const bigRedraw = () => { if (!big.queued) { big.queued = true; requestAnimationFrame(drawBig); } };
function bigWords() {
  const st = $('street').textContent, here = st && st !== 'Padiham' ? `You are on ${st}.` : 'You are in Padiham.';
  if (state.inside) return `You are inside ${state.inside.pub.name}.`;
  const w = wayNow();
  if (!w || !state.job) return here;
  return `${here} Next: ${state.job.steps[state.job.i].text}. ${w.m} metres by the streets. ${w.turn ? w.turn + '.' : ''}`;
}
function openBig() {
  if (!started || bigBox.classList.contains('on') || (buses && buses.riding)) return;
  closeBusMenu(); if (menu.classList.contains('on')) menu.classList.remove('on');
  state.playing = false; keys.clear(); stickVec.x = stickVec.y = 0;
  $('bigText').textContent = bigWords(); hintState.map = true;
  bigBox.classList.add('on');
  bigFit(); drawBig();
  $('bigClose').focus();
}
function closeBig() { if (!bigBox.classList.contains('on')) return; bigBox.classList.remove('on'); state.playing = true; view.focus({ preventScroll: true }); }
function bigZoom(f, px, pz) {
  // Zoom about a point on the map (the middle if none is given), so the spot under the pointer stays put.
  const r = bigCv.getBoundingClientRect(), ox = px === undefined ? 0 : px - r.width / 2, oz = pz === undefined ? 0 : pz - r.height / 2;
  const wx = big.x + ox / big.k, wz = big.z + oz / big.k;
  big.k = Math.min(big.max, Math.max(big.min, big.k * f));
  big.x = wx - ox / big.k; big.z = wz - oz / big.k;
  bigClamp(); bigRedraw();
}
$('bigIn').addEventListener('click', () => bigZoom(1.6));
$('bigOut').addEventListener('click', () => bigZoom(1 / 1.6));
$('bigMe').addEventListener('click', () => { big.x = player.x; big.z = player.z; big.k = Math.max(big.k, 1.4); bigClamp(); bigRedraw(); });
$('bigClose').addEventListener('click', closeBig);
$('map').addEventListener('click', openBig);
$('mapBtn').addEventListener('click', openBig);
bigCv.addEventListener('wheel', (e) => { e.preventDefault(); const r = bigCv.getBoundingClientRect(); bigZoom(e.deltaY < 0 ? 1.25 : 0.8, e.clientX - r.left, e.clientY - r.top); }, { passive: false });
{
  // One finger or the mouse drags the map; two fingers pinch it.
  const touches = new Map();
  let gap = 0;
  bigCv.addEventListener('pointerdown', (e) => { bigCv.setPointerCapture(e.pointerId); touches.set(e.pointerId, [e.clientX, e.clientY]); gap = 0; });
  bigCv.addEventListener('pointermove', (e) => {
    const was = touches.get(e.pointerId); if (!was) return;
    touches.set(e.pointerId, [e.clientX, e.clientY]);
    if (touches.size === 1) { big.x -= (e.clientX - was[0]) / big.k; big.z -= (e.clientY - was[1]) / big.k; bigClamp(); bigRedraw(); }
    else if (touches.size === 2) {
      const [a, b] = [...touches.values()], d = Math.hypot(a[0] - b[0], a[1] - b[1]), r = bigCv.getBoundingClientRect();
      if (gap > 0 && d > 0) bigZoom(d / gap, (a[0] + b[0]) / 2 - r.left, (a[1] + b[1]) / 2 - r.top);
      gap = d;
    }
  });
  const up = (e) => { touches.delete(e.pointerId); gap = 0; };
  bigCv.addEventListener('pointerup', up); bigCv.addEventListener('pointercancel', up);
}
bigBox.addEventListener('keydown', (e) => {
  const step = 60 / big.k;
  if (e.key === 'Tab') {
    const f = [...bigBox.querySelectorAll('button')], first = f[0], last = f[f.length - 1];
    if (e.shiftKey && document.activeElement === first) { e.preventDefault(); last.focus(); }
    else if (!e.shiftKey && document.activeElement === last) { e.preventDefault(); first.focus(); }
  } else if (e.key === '+' || e.key === '=') bigZoom(1.6);
  else if (e.key === '-' || e.key === '_') bigZoom(1 / 1.6);
  else if (e.key === 'ArrowLeft') { big.x -= step; bigClamp(); bigRedraw(); e.preventDefault(); }
  else if (e.key === 'ArrowRight') { big.x += step; bigClamp(); bigRedraw(); e.preventDefault(); }
  else if (e.key === 'ArrowUp') { big.z -= step; bigClamp(); bigRedraw(); e.preventDefault(); }
  else if (e.key === 'ArrowDown') { big.z += step; bigClamp(); bigRedraw(); e.preventDefault(); }
  else if (e.code === 'KeyM' && !e.repeat) closeBig();
});
addEventListener('keydown', (e) => { if (e.code === 'Escape' && bigBox.classList.contains('on')) { e.stopImmediatePropagation(); closeBig(); } }, true);
addEventListener('resize', () => { if (bigBox.classList.contains('on')) { bigFit(); bigRedraw(); } });

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
  const riding = !!buses && buses.riding;
  const want = state.playing && !riding ? mag * top : 0;
  player.speed += (want - player.speed) * Math.min(1, dt * (want > player.speed ? 6 : 10));
  if (buses && state.playing) {
    buses.update(dt, player);
    // The button for the stop the player is standing at.
    const near = !state.inside && !buses.busy ? buses.near(player.x, player.z) : null, st = near && !near.dead ? near : null;
    if (st !== atStop) { atStop = st; const b = $('busBtn'); b.hidden = !st; stage.classList.toggle('atstop', !!st); if (st) { b.textContent = `Bus stop: ${st.n}`; say(`Bus stop: ${st.n}. Press B to catch a bus`); } }
  }
  // On the bus the view starts from behind it, then follows it round the corners.
  if (riding && snapCam && buses.phase === 'ride') { cam.yaw = player.heading; snapCam = false; }
  if (riding && !boardView && S.follow && lookId === null && time - cam.lastLook > 1.2) cam.yaw += Math.sin(wrap(player.heading - cam.yaw)) * dt * 1.7;
  if (mag > 0.05 && state.playing && !riding) {
    const h = Math.atan2(mx, mz);
    player.heading += wrap(h - player.heading) * Math.min(1, dt * 12);
    const ox = player.x, oz = player.z;
    // Move in short steps, so that at a sprint the player cannot pass through a wall between two frames.
    const dist = player.speed * dt, n = Math.max(1, Math.ceil(dist / 0.3));
    for (let k = 0; k < n; k++) { player.x += (mx / mag) * (dist / n); player.z += (mz / mag) * (dist / n); pushOut(player); }
    if (!state.inside) state.metres += Math.hypot(player.x - ox, player.z - oz);
    if (!state.inside && inRiver(player.x, player.z)) { player.x = ox; player.z = oz; if (time > edgeWarn) { toast('Too deep to wade', 'The yellow line on the map leads to a bridge', 2600); edgeWarn = time + 4; } }
    const cx = Math.min(slice[2] - 1, Math.max(slice[0] + 1, player.x)), cz = Math.min(slice[3] - 1, Math.max(slice[1] + 1, player.z));
    if (!state.inside && (cx !== player.x || cz !== player.z)) { player.x = cx; player.z = cz; if (time > edgeWarn) { toast('Edge of the map', 'Turn back towards town', 1800); edgeWarn = time + 5; } }
    if (S.follow && lookId === null && time - cam.lastLook > 1.2) cam.yaw += Math.sin(wrap(h - cam.yaw)) * dt * 1.7;
  }
  // Character
  // Legs turn over faster at a run, but not in step with the speed, or a sprint would be a blur.
  player.phase += (Math.min(player.speed, 4.6) + Math.max(0, player.speed - 4.6) * 0.42) * dt * 2.3;
  const amt = Math.min(1, player.speed / 4) * (player.speed > 11 ? 1.25 : 1);
  const wheels = !!player.fig.wheels;
  if (wheels) { player.rolled += player.speed * dt; animateSeated(player.fig, player.rolled, player.speed > 0.4); }
  else animateFigure(player.fig, player.phase, amt);
  if (player.trick > 0) player.trick -= dt;
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
  if (stepN !== lastStep) { lastStep = stepN; if (player.speed > 1) { if (wheels) sound.roll(); else sound.step(player.speed > 6); } }
  const baseY = state.inside ? state.inside.pub.floorY : 0.14 + groundH(player.x, player.z);
  if (state.playing) {
    if (!state.inside && !riding) { for (const d of doors) if (Math.hypot(player.x - d.x, player.z - d.z) < 1.0) { enterPub(d); break; } }
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
  const sway = S.reduce || wheels ? 0 : amt;
  player.fig.group.position.set(player.x, baseY + player.y + Math.abs(Math.sin(player.phase)) * 0.05 * sway, player.z);
  // A wheelie tips the chair back on its big wheels; a powerchair turns once round on the spot.
  const trick = Math.max(0, player.trick), spin = look.move === 'power' ? (1 - trick / 0.9) * Math.PI * 2 * (trick > 0 ? 1 : 0) : 0;
  player.fig.group.rotation.y = player.heading + spin;
  player.fig.group.rotation.x = wheels ? (look.move === 'chair' ? -0.34 * Math.sin(Math.PI * Math.min(1, trick / 0.8)) : 0) : sway * (player.speed > 11 ? 0.24 : player.speed > 6 ? 0.16 : 0.05);

  for (const sp of labels) { const k = Math.min(1, Math.max(0.2, camera.position.distanceTo(sp.position) / 70)); sp.scale.set(20 * k, 3.75 * k, 1); }
  if (selfie) updateSelfie(dt, time);
  if (state.playing && !riding) { updatePlaques(time); updateLitter(dt); }
  if (peds) peds.update(dt, player);
  if (pigeons) pigeons.update(dt, player, time);

  // Camera, pulled in if a wall is in the way
  const cp = Math.cos(cam.pitch), flat = cam.dist * cp;
  // On the bus the view rides high above the roofs, so walls behind it are not allowed to push it in.
  const t = riding ? 1 : wallHit(player.x, player.z, player.x - fx * (flat + 0.6), player.z - fz * (flat + 0.6));
  const wantDist = Math.max(1.5, cam.dist * t - (t < 1 ? 0.5 : 0));
  cam.cur += (wantDist - cam.cur) * Math.min(1, dt * (wantDist < cam.cur ? 14 : 3));
  camera.position.set(player.x - fx * cam.cur * cp, baseY + 1.6 + Math.sin(cam.pitch) * cam.cur, player.z - fz * cam.cur * cp);
  // On a slope the ground behind the player can rise above the camera: keep the camera clear of it.
  if (!state.inside) camera.position.y = Math.max(camera.position.y, groundH(camera.position.x, camera.position.z) + 0.6);
  camera.lookAt(player.x, baseY + (riding ? 2.2 : 1.45), player.z);
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
    if (!plaqueShown) state.job.t += dt; // the clock waits while a plaque is being read
    const dx = tg.dx - player.x, dz = tg.dz - player.z, d = Math.hypot(dx, dz);
    if (d < (state.job.steps[state.job.i].r || 3.6) && !riding) arrive();
    if (S.reduce) beam.scale.setScalar(1);
    else { beam.rotation.y += dt * 0.8; beam.scale.setScalar(1 + Math.sin(time * 3) * 0.06); }
    uiTick -= dt;
    if (uiTick <= 0) {
      uiTick = 0.1;
      // The arrow points along the streets to the next turn, not straight at the place.
      const g = guide(time) || { m: d, x: tg.dx, z: tg.dz, turn: null }, bearing = wrap(Math.atan2(g.x - player.x, g.z - player.z) - cam.yaw);
      $('arrow').style.transform = `rotate(${-bearing}rad)`;
      const words = bearingWords(bearing), m = Math.round(g.m / 5) * 5;
      const ride = state.job.steps[state.job.i].ride;
      $('dist').textContent = ride && riding ? 'On the bus' : ride && Math.hypot(player.x - ride.stop.x, player.z - ride.stop.z) < 14 ? (buses.busy ? 'Wait here for your bus' : 'Choose Bus stop to catch your bus') : `${m} m, ${words}`;
      // Read the way out when it changes, but not more than once every eight seconds.
      if (words !== lastWords && time - saidAt > 8) { lastWords = words; saidAt = time; const tw = turnWords(g.turn); say(`${state.job.steps[state.job.i].poi.n}: ${m} metres, ${words}${tw ? '. ' + tw : ''}`); }
      const j = state.job;
      $('timer').textContent = !timed() || j.noClock ? '' : j.t <= j.par ? `${mmss(j.t)}, bonus up to ${mmss(j.par)}` : mmss(j.t);
      streetNow(); drawMini(); hintTick(time);
    }
  } else if (state.playing && !state.inside) {
    // No job in hand: the street name, the small map and the prompts still keep up.
    uiTick -= dt;
    if (uiTick <= 0) { uiTick = 0.1; streetNow(); drawMini(); hintTick(time); }
  }
}
function streetNow() {
  let name = '', best = 14;
  for (const r of roadGrid.at(player.x, player.z)) if (r[5]) { const dd = distToSeg(player.x, player.z, r)[0]; if (dd < best) { best = dd; name = r[5]; } }
  $('street').textContent = name || 'Padiham';
}
// ---------- First prompts ----------
// The controls are taught in play, one line at a time. Each line goes once the thing has been done, and is not
// shown again on this device. The full list stays under Settings and controls on the start screen.
const HINT_KEY = 'padiham-hints-1';
const hintsDone = (() => { try { return new Set(JSON.parse(localStorage.getItem(HINT_KEY) || '[]')); } catch { return new Set(); } })();
const hintState = { at: null, m0: 0, look0: 0, wait: 0, map: false };
const HINTS = [
  ['move', () => (isTouch ? 'Put a thumb on the lower left and push to move' : 'Move with W A S D or the arrow keys'), () => state.metres - hintState.m0 > 6],
  ['look', () => (isTouch ? 'Drag anywhere else to look around' : 'Drag to look around, or press Q and E'), () => cam.lastLook > hintState.look0],
  ['pace', () => (isTouch ? `Tap ${$('run').textContent} to go faster` : 'Press R to go faster'), () => gear > 0],
  ['map', () => (isTouch ? 'Tap the small map to open the full map' : 'Press M for the map'), () => hintState.map],
];
function hintTick(time) {
  const el = $('hint'), cur = HINTS.find((h) => !hintsDone.has(h[0]));
  if (!cur || time < hintState.wait) { el.classList.remove('on'); return; }
  if (hintState.at !== cur[0]) { hintState.at = cur[0]; hintState.m0 = state.metres; hintState.look0 = cam.lastLook; hintState.map = false; el.textContent = cur[1](); el.classList.add('on'); return; }
  if (cur[0] === 'pace') el.textContent = cur[1]();
  if (cur[2]()) {
    hintsDone.add(cur[0]); hintState.at = null; hintState.wait = time + 1.2; el.classList.remove('on');
    try { localStorage.setItem(HINT_KEY, JSON.stringify([...hintsDone])); } catch { /* private mode */ }
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
  // The network of streets and paths for finding the way.
  streets = new Streets(w.roads, slice, shutRoad);
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
  homeStart = { x: player.x, z: player.z, h: player.heading };
  if (!PRACTICE) { placePlaques(); placeLitter(); }
  buses = new Buses(scene, w, busUi);
  // A stop with no bus going on to another stop in the town centre is left as street furniture: no button, no map square.
  for (const st of buses.stops) st.dead = buses.options(st).length === 0;
  // Every stop that works gets a tall blue sign and a name that can be read from along the street.
  {
    const pole = new THREE.MeshLambertMaterial({ color: 0x2e3a36 }), blue = new THREE.MeshLambertMaterial({ color: 0x2f6fb5 }), white = new THREE.MeshLambertMaterial({ color: 0xffffff });
    for (const st of buses.stops) {
      if (st.dead) continue;
      const y = groundH(st.x, st.z) + 0.14, grp = new THREE.Group(); grp.position.set(st.x, y, st.z);
      const p = new THREE.Mesh(new THREE.CylinderGeometry(0.07, 0.07, 3.6, 8), pole); p.position.y = 1.8; p.castShadow = true;
      const disc = new THREE.Mesh(new THREE.CylinderGeometry(0.62, 0.62, 0.08, 24), blue); disc.rotation.x = Math.PI / 2; disc.position.y = 3.5; disc.castShadow = true;
      const bar = new THREE.Mesh(new THREE.BoxGeometry(0.8, 0.2, 0.1), white); bar.position.y = 3.5;
      const pad = new THREE.Mesh(new THREE.CylinderGeometry(1.5, 1.5, 0.04, 28), blue); pad.position.y = 0.03;
      grp.add(p, disc, bar, pad); scene.add(grp);
      label(`Bus stop: ${st.n}`, st.x, 5.2, st.z, 'rgba(47,111,181,.95)');
    }
  }
  peds = new Pedestrians(scene, built.pedPaths, isTouch ? 16 : 34);
  peds.setCount(S.calm ? 6 : Infinity);
  if (!PRACTICE) pigeons = new Pigeons(scene, built.spots, 7, (n) => {
    state.pigeons += n; persist(); hud();
    if (S.calm) return;
    sound.flap(n);
    toast(`${n} pigeons startled`, `That is ${state.pigeons} so far`, 1600);
  });
  // Stamps already earned by an older save are marked quietly, without a run of messages.
  for (const st of STAMPS) if (st[3]()) state.stamps.add(st[0]);
  indexEnds(); fillTripPicker();
  if (TRIP_RAW) trip = resolveTrip(TRIP_RAW);
  hud();
  if (ROUTE || trip) { startRoute(); showPlan(); }
  else if (TRIP_RAW) { $('jobTitle').textContent = 'This trip link did not work'; $('jobText').textContent = 'Ask for a new link, or open the menu and choose Practise a trip'; $('dist').textContent = ''; }
  else if (mode === 'jobs') makeJob();
  else idleCard();
  drawMini(); draftNote(); paceLabel();
  // The start screen: a returning player gets Carry on first, in the way they last played. A new player picks.
  const b = $('play');
  const begin = () => {
    $('setSlot').append($('settings')); $('lookSlot').append($('look'));
    $('start').style.display = 'none'; stage.classList.add('started');
    state.playing = true; started = true; sound.start(); clearHeader(); stage.scrollIntoView({ block: 'start' }); view.focus({ preventScroll: true });
  };
  b.disabled = false;
  if (PRACTICE) b.textContent = trip ? 'Start the trip' : ROUTE ? 'Start the route' : 'Play';
  else if (mode) { b.textContent = `Carry on: ${MODE_NAME[mode]}`; setMode(mode); }
  else b.hidden = true;
  b.addEventListener('click', begin);
  $(mode === 'jobs' ? 'modeJobs' : 'modeExplore').classList.add('pick');
  for (const [id, m] of [['modeExplore', 'explore'], ['modeJobs', 'jobs']]) { $(id).disabled = false; $(id).addEventListener('click', () => { setMode(m); begin(); }); }
  $('modeDaily').disabled = false;
  $('modeDaily').addEventListener('click', () => { setMode('jobs'); begin(); startDaily(); });
  $('tripOpen').hidden = false;
  frame();
}).catch((err) => { console.error(err); $('jobTitle').textContent = 'Could not load the town'; $('jobText').textContent = String(err); });

// A handle for testing in the dev server only; it is not in the built site.
if (import.meta.env.DEV) window.__padiham = { quality, stepDown, chunks, player, state, cam, pois, doors, input: stickVec, scene, renderer, plaques, get world() { return world; }, get built() { return built; }, get pigeons() { return pigeons; }, litter, jump, hello, toggleRun, get peds() { return peds; }, draft, ROUTE, get buses() { return buses; }, get ends() { return ends; }, get services() { return services; }, get trip() { return trip; }, get mode() { return mode; }, setMode, hintsDone, step, clock, S, get streets() { return streets; }, get way() { return way; }, guide, big };
