// Straight Outta Padiham: a third-person walking game on the real street layout of Padiham.
// Map data (c) OpenStreetMap contributors, ODbL. Building outlines contain OS data (c) Crown copyright
// and database right 2026. The world file is built by scripts/padiham-game/build_world.py.
import * as THREE from 'three';
import { buildWorld, walls, roadGrid, riverGrid, distToSeg, nearWall } from './city.js';
import { cloudTexture } from './textures.js';
import { makeFigure, animateFigure, Pedestrians, Pigeons } from './life.js';
import { Sound } from './audio.js';
import { buildPub } from './interior.js';

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
];
const CARGO = ['a parcel', 'a toolbox', 'a bunch of flowers', 'a box of leaflets', 'a birthday cake', 'a set of keys', 'a bag of shopping'];

// ---------- State ----------
const SAVE_KEY = 'padiham-errands-2';
const save = (() => { try { return JSON.parse(localStorage.getItem(SAVE_KEY) || '{}'); } catch { return {}; } })();
const state = { money: save.money || 0, done: save.done || 0, jobIndex: save.jobIndex || 0, pigeons: save.pigeons || 0, job: null, playing: false, inside: null };
function persist() {
  try { localStorage.setItem(SAVE_KEY, JSON.stringify({ money: state.money, done: state.done, jobIndex: state.jobIndex, pigeons: state.pigeons })); } catch { /* private mode */ }
}
const sound = new Sound();

// ---------- Renderer, sky and light ----------
const renderer = new THREE.WebGLRenderer({ canvas: $('view'), antialias: true, powerPreference: 'high-performance' });
renderer.setPixelRatio(Math.min(devicePixelRatio || 1, isTouch ? 1.75 : 2));
renderer.shadowMap.enabled = true;
renderer.shadowMap.type = THREE.PCFSoftShadowMap;
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
function resize() {
  const w = stage.clientWidth || 800, h = stage.clientHeight || 600;
  renderer.setSize(w, h, false);
  camera.aspect = w / h;
  camera.fov = w < h ? 74 : 62;
  camera.updateProjectionMatrix();
}
new ResizeObserver(resize).observe(stage);
resize();
$('full').addEventListener('click', () => {
  if (document.fullscreenElement) document.exitFullscreen();
  else if (stage.requestFullscreen) stage.requestFullscreen();
});

// ---------- World ----------
let world, built, slice, peds, pigeons;
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
  sp.position.set(x, y, z);
  scene.add(sp);
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
const player = { x: 0, z: 0, heading: 0, speed: 0, phase: 0, fig: makeFigure({ jacket: 0xff7a1a, hiVis: true, pack: true, cap: 0x2b3a67 }) };
scene.add(player.fig.group);
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
const input = { x: 0, y: 0, run: false };
addEventListener('keydown', (e) => { keys.add(e.code); if (state.playing && (e.code.startsWith('Arrow') || e.code === 'Space')) e.preventDefault(); });
addEventListener('keyup', (e) => keys.delete(e.code));
addEventListener('blur', () => keys.clear());

const clock = new THREE.Clock();
const cam = { yaw: 0, pitch: 0.3, dist: 7.5, cur: 7.5, lastLook: -10 };
const view = $('view');
let lookId = null, lx = 0, ly = 0;
view.addEventListener('pointerdown', (e) => { lookId = e.pointerId; lx = e.clientX; ly = e.clientY; view.setPointerCapture(e.pointerId); });
view.addEventListener('pointermove', (e) => {
  if (e.pointerId !== lookId) return;
  cam.yaw -= (e.clientX - lx) * 0.006;
  cam.pitch = Math.min(state.inside ? 0.3 : 1.15, Math.max(0.06, cam.pitch + (e.clientY - ly) * 0.004));
  lx = e.clientX; ly = e.clientY; cam.lastLook = clock.elapsedTime;
});
const endLook = (e) => { if (e.pointerId === lookId) lookId = null; };
view.addEventListener('pointerup', endLook);
view.addEventListener('pointercancel', endLook);

const stick = $('stick'), knob = $('knob');
let stickId = null;
const stickVec = { x: 0, y: 0 };
function stickMove(e) {
  const r = stick.getBoundingClientRect();
  let dx = (e.clientX - (r.left + r.width / 2)) / 48, dy = (e.clientY - (r.top + r.height / 2)) / 48;
  const l = Math.hypot(dx, dy);
  if (l > 1) { dx /= l; dy /= l; }
  stickVec.x = dx; stickVec.y = -dy;
  knob.style.transform = `translate(${dx * 38}px, ${dy * 38}px)`;
}
stick.addEventListener('pointerdown', (e) => { stickId = e.pointerId; stick.setPointerCapture(e.pointerId); stickMove(e); });
stick.addEventListener('pointermove', (e) => { if (e.pointerId === stickId) stickMove(e); });
const endStick = (e) => { if (e.pointerId === stickId) { stickId = null; stickVec.x = stickVec.y = 0; knob.style.transform = ''; } };
stick.addEventListener('pointerup', endStick);
stick.addEventListener('pointercancel', endStick);
let runToggle = false;
$('run').addEventListener('click', () => { runToggle = !runToggle; $('run').classList.toggle('on', runToggle); });
$('mute').addEventListener('click', () => { sound.setMuted(!sound.muted); $('mute').textContent = sound.muted ? 'Sound off' : 'Sound on'; });

function readInput() {
  let x = stickVec.x, y = stickVec.y;
  if (keys.has('KeyW') || keys.has('ArrowUp')) y += 1;
  if (keys.has('KeyS') || keys.has('ArrowDown')) y -= 1;
  if (keys.has('KeyD') || keys.has('ArrowRight')) x += 1;
  if (keys.has('KeyA') || keys.has('ArrowLeft')) x -= 1;
  const l = Math.hypot(x, y);
  if (l > 1) { x /= l; y /= l; }
  input.x = x; input.y = y;
  input.run = runToggle || keys.has('ShiftLeft') || keys.has('ShiftRight');
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
function fade() { const f = $('fade'); f.classList.add('on'); setTimeout(() => f.classList.remove('on'), 260); }
function enterPub(door) {
  const pub = door.pub;
  state.inside = { door, pub, camDist: cam.dist, camPitch: cam.pitch };
  player.x = pub.spawn.x; player.z = pub.spawn.z; player.heading = cam.yaw = Math.PI;
  cam.dist = cam.cur = 3.1; cam.pitch = 0.2; beam.visible = false;
  fade(); sound.pickup(); toast(pub.name, 'Walk back to the door to leave', 2200);
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
let slicePois = [];

function makeJob() {
  let def = SCRIPT[state.jobIndex];
  if (def && !def.steps.every((s) => slicePois.includes(pois.get(s[0])))) def = null;
  let job;
  if (def) job = { title: `Job ${state.jobIndex + 1}: ${def.title}`, steps: def.steps.map(([n, text]) => ({ poi: pois.get(n), text })) };
  else {
    const here = { dx: player.x, dz: player.z };
    let a, b, tries = 0;
    do { a = slicePois[Math.floor(Math.random() * slicePois.length)]; b = slicePois[Math.floor(Math.random() * slicePois.length)]; tries++; }
    while (tries < 200 && (a === b || dist2(a, b) < 70 || dist2(here, a) < 30));
    const thing = CARGO[Math.floor(Math.random() * CARGO.length)];
    job = { title: 'Delivery', steps: [{ poi: a, text: `Collect ${thing} at ${a.n}` }, { poi: b, text: `Deliver it to ${b.n}` }] };
  }
  let d = 0, prev = { dx: player.x, dz: player.z };
  for (const s of job.steps) { d += dist2(prev, s.poi); prev = s.poi; }
  job.i = 0; job.t = 0;
  job.par = Math.round((d * 1.4) / 5 + 12 * job.steps.length);
  job.pay = Math.max(10, Math.round(d / 25) * 5);
  state.job = job;
  showStep();
}
function showStep() {
  const j = state.job, s = j.steps[j.i];
  $('jobTitle').textContent = j.title + (j.steps.length > 1 ? `, part ${j.i + 1} of ${j.steps.length}` : '');
  $('jobText').textContent = s.text;
  beam.position.set(s.poi.dx, 0, s.poi.dz);
  beam.visible = true;
}
let toastTimer;
function toast(title, sub = '', ms = 2600) {
  const t = $('toast');
  t.textContent = title;
  if (sub) { const s = document.createElement('small'); s.textContent = sub; t.appendChild(s); }
  t.classList.add('on');
  clearTimeout(toastTimer); toastTimer = setTimeout(() => t.classList.remove('on'), ms);
}
function arrive() {
  const j = state.job;
  if (j.i < j.steps.length - 1) { j.i++; showStep(); sound.pickup(); toast('Got it!', 'Now follow the arrow to the drop off', 1900); return; }
  const bonus = j.t <= j.par ? Math.round(j.pay / 2) : 0;
  state.money += j.pay + bonus; state.done++;
  if (state.jobIndex < SCRIPT.length) state.jobIndex++;
  persist(); hud(); sound.done();
  toast(`Job done! +£${j.pay + bonus}`, bonus ? `Includes a £${bonus} speed bonus` : 'Beat the clock next time for a bonus');
  makeJob();
}
function hud() { $('money').textContent = state.money; $('done').textContent = state.done; $('pigeons').textContent = state.pigeons; }
const mmss = (s) => `${Math.floor(s / 60)}:${String(Math.floor(s % 60)).padStart(2, '0')}`;

// ---------- Main loop ----------
const wrap = (a) => Math.atan2(Math.sin(a), Math.cos(a));
let uiTick = 0, edgeWarn = 0, lastStep = 0;

function step(dt) {
  readInput();
  const time = clock.elapsedTime;
  const fx = Math.sin(cam.yaw), fz = Math.cos(cam.yaw);
  const mx = fx * input.y - fz * input.x, mz = fz * input.y + fx * input.x;
  const mag = Math.min(1, Math.hypot(mx, mz));
  const top = input.run ? 8.5 : 4.6;
  const want = state.playing ? mag * top : 0;
  player.speed += (want - player.speed) * Math.min(1, dt * 10);
  if (mag > 0.05 && state.playing) {
    const h = Math.atan2(mx, mz);
    player.heading += wrap(h - player.heading) * Math.min(1, dt * 12);
    const ox = player.x, oz = player.z;
    player.x += (mx / mag) * player.speed * dt;
    player.z += (mz / mag) * player.speed * dt;
    pushOut(player);
    if (!state.inside && inRiver(player.x, player.z)) { player.x = ox; player.z = oz; if (time > edgeWarn) { toast('Too deep to wade', 'Find a bridge', 1500); edgeWarn = time + 4; } }
    const cx = Math.min(slice[2] - 1, Math.max(slice[0] + 1, player.x)), cz = Math.min(slice[3] - 1, Math.max(slice[1] + 1, player.z));
    if (!state.inside && (cx !== player.x || cz !== player.z)) { player.x = cx; player.z = cz; if (time > edgeWarn) { toast('Closed off for now', 'More of Padiham is coming soon', 1800); edgeWarn = time + 5; } }
    if (lookId === null && time - cam.lastLook > 1.2) cam.yaw += Math.sin(wrap(h - cam.yaw)) * dt * 1.7;
  }
  // Character
  player.phase += player.speed * dt * 2.3;
  const amt = Math.min(1, player.speed / 4);
  animateFigure(player.fig, player.phase, amt);
  const stepN = Math.floor(player.phase / Math.PI);
  if (stepN !== lastStep) { lastStep = stepN; if (player.speed > 1) sound.step(player.speed > 6); }
  const baseY = state.inside ? state.inside.pub.floorY : 0.14;
  if (state.playing) {
    if (!state.inside) { for (const d of doors) if (Math.hypot(player.x - d.x, player.z - d.z) < 1.0) { enterPub(d); break; } }
    else {
      const pub = state.inside.pub;
      playCool -= dt;
      if (Math.hypot(player.x - pub.exit.x, player.z - pub.exit.z) < 0.62) leavePub();
      else if (playCool <= 0) {
        for (const pl of pub.pools) if (Math.hypot(player.x - pl.x, player.z - pl.z) < 2.0) { toast(POOL_LINES[Math.floor(Math.random() * POOL_LINES.length)], 'Pool table', 1700); sound.pickup(); playCool = 6; }
        if (pub.darts && playCool <= 0 && Math.hypot(player.x - pub.darts.x, player.z - pub.darts.z) < 1.1) { toast(DART_LINES[Math.floor(Math.random() * DART_LINES.length)], 'Dartboard', 1700); sound.pickup(); playCool = 6; }
      }
    }
  }
  player.fig.group.position.set(player.x, baseY + Math.abs(Math.sin(player.phase)) * 0.05 * amt, player.z);
  player.fig.group.rotation.y = player.heading;
  player.fig.group.rotation.x = amt * (player.speed > 6 ? 0.16 : 0.05);

  if (peds) peds.update(dt, player);
  if (pigeons) pigeons.update(dt, player, time);

  // Camera, pulled in if a wall is in the way
  const cp = Math.cos(cam.pitch), flat = cam.dist * cp;
  const t = wallHit(player.x, player.z, player.x - fx * (flat + 0.6), player.z - fz * (flat + 0.6));
  const wantDist = Math.max(1.5, cam.dist * t - (t < 1 ? 0.5 : 0));
  cam.cur += (wantDist - cam.cur) * Math.min(1, dt * (wantDist < cam.cur ? 14 : 3));
  camera.position.set(player.x - fx * cam.cur * cp, baseY + 1.6 + Math.sin(cam.pitch) * cam.cur, player.z - fz * cam.cur * cp);
  camera.lookAt(player.x, baseY + 1.45, player.z);
  sky.position.copy(camera.position); clouds.position.set(camera.position.x, 0, camera.position.z);
  const sx = Math.round(player.x / 4) * 4, sz = Math.round(player.z / 4) * 4;
  sun.target.position.set(sx, 0, sz);
  sun.position.set(sx, 0, sz).addScaledVector(sunDir, 220);

  // Job progress
  const tg = target();
  if (tg && state.playing && !state.inside) {
    state.job.t += dt;
    const dx = tg.dx - player.x, dz = tg.dz - player.z, d = Math.hypot(dx, dz);
    if (d < 3.6) arrive();
    beam.rotation.y += dt * 0.8;
    beam.scale.setScalar(1 + Math.sin(time * 3) * 0.06);
    uiTick -= dt;
    if (uiTick <= 0) {
      uiTick = 0.1;
      $('arrow').style.transform = `rotate(${-wrap(Math.atan2(dx, dz) - cam.yaw)}rad)`;
      $('dist').textContent = `${Math.round(d / 5) * 5} m`;
      const j = state.job;
      $('timer').textContent = j.t <= j.par ? `Bonus if done by ${mmss(j.par)} (${mmss(j.t)})` : mmss(j.t);
      let name = '', best = 14;
      for (const r of roadGrid.at(player.x, player.z)) if (r[5]) { const dd = distToSeg(player.x, player.z, r)[0]; if (dd < best) { best = dd; name = r[5]; } }
      if (name) $('street').textContent = name;
      drawMini();
    }
  }
}
function frame() {
  step(Math.min(0.05, clock.getDelta()));
  renderer.render(scene, camera);
  requestAnimationFrame(frame);
}

// ---------- Boot ----------
fetch(stage.dataset.world).then((r) => r.json()).then((w) => {
  world = w; slice = w.slice;
  built = buildWorld(scene, w, isTouch);
  for (const p of w.pois) {
    pois.set(p.n, p);
    const inside = p.dx > slice[0] + 4 && p.dx < slice[2] - 4 && p.dz > slice[1] + 4 && p.dz < slice[3] - 4;
    if (inside) { safeDoor(p); slicePois.push(p); }
    if (!built.signs.has(p.n)) label(p.n.replace('Church of England', 'CE'), p.x, p.k === 'park' || p.k === 'pitch' || p.k === 'bridge' ? 8 : 15, p.z);
  }
  for (const d of built.doors) {
    if (!PUBS[d.name]) continue;
    d.pub = buildPub(scene, { ...PUBS[d.name], name: d.name });
    const ring = new THREE.Mesh(new THREE.RingGeometry(0.55, 0.8, 24), new THREE.MeshBasicMaterial({ color: 0x4fb3ff, side: THREE.DoubleSide, fog: false }));
    ring.rotation.x = -Math.PI / 2; ring.position.set(d.x, 0.32, d.z); scene.add(ring);
    doors.push(d);
  }
  drawMapBase();
  const home = pois.get('Padiham Town Hall') || slicePois[0];
  // Start on the pavement in front of the Town Hall, clear of the portico.
  const first0 = pois.get('Padiham Fire Station') || home;
  const dl = Math.hypot(first0.dx - home.dx, first0.dz - home.dz) || 1;
  player.x = home.dx; player.z = home.dz;
  for (let k = 0; k < 12 && (nearWall(player.x, player.z, 3) || k < 3); k++) { player.x += ((first0.dx - home.dx) / dl) * 1.5; player.z += ((first0.dz - home.dz) / dl) * 1.5; }
  const first = pois.get('Padiham Fire Station');
  if (first) player.heading = cam.yaw = Math.atan2(first.dx - home.dx, first.dz - home.dz);
  peds = new Pedestrians(scene, built.pedPaths, isTouch ? 10 : 18);
  pigeons = new Pigeons(scene, built.spots, 7, (n) => {
    state.pigeons += n; persist(); hud(); sound.flap(n);
    toast(`${n} pigeons startled`, `That is ${state.pigeons} so far`, 1600);
  });
  hud(); makeJob(); drawMini();
  const b = $('play');
  b.disabled = false; b.textContent = state.done ? 'Carry on' : 'Play';
  b.addEventListener('click', () => { $('start').style.display = 'none'; state.playing = true; sound.start(); });
  frame();
}).catch((err) => { console.error(err); $('jobTitle').textContent = 'Could not load the town'; $('jobText').textContent = String(err); });

window.__padiham = { player, state, cam, pois, doors, input: stickVec, scene, renderer, get built() { return built; }, get pigeons() { return pigeons; } };
