// People and pigeons.
import * as THREE from 'three';
import { groundH } from './terrain.js';

const mats = new Map();
const mat = (c) => { let m = mats.get(c); if (!m) mats.set(c, m = new THREE.MeshLambertMaterial({ color: c })); return m; };
const G = {
  torso: new THREE.CapsuleGeometry(0.2, 0.36, 4, 10),
  head: new THREE.SphereGeometry(0.175, 14, 12),
  hair: new THREE.SphereGeometry(0.185, 14, 8, 0, Math.PI * 2, 0, Math.PI * 0.55),
  leg: new THREE.CapsuleGeometry(0.085, 0.6, 3, 8),
  arm: new THREE.CapsuleGeometry(0.065, 0.46, 3, 8),
  shoe: new THREE.BoxGeometry(0.15, 0.09, 0.3),
  hand: new THREE.SphereGeometry(0.07, 8, 6),
  band: new THREE.CylinderGeometry(0.255, 0.255, 0.07, 12),
  pack: new THREE.BoxGeometry(0.36, 0.42, 0.18),
  peak: new THREE.BoxGeometry(0.2, 0.035, 0.16),
  shorts: new THREE.CylinderGeometry(0.115, 0.125, 0.3, 10),
  sock: new THREE.CylinderGeometry(0.09, 0.085, 0.3, 10),
  sleeve: new THREE.CylinderGeometry(0.08, 0.085, 0.2, 10),
  back: new THREE.PlaneGeometry(0.36, 0.4),
  tie: new THREE.BoxGeometry(0.06, 0.3, 0.02),
  shirt: new THREE.BoxGeometry(0.16, 0.34, 0.02),
  rosette: new THREE.CylinderGeometry(0.075, 0.075, 0.02, 14),
  ribbon: new THREE.BoxGeometry(0.035, 0.14, 0.012),
  phone: new THREE.BoxGeometry(0.085, 0.16, 0.012),
  stripe: new THREE.BoxGeometry(0.006, 0.085, 0.022),
  eye: new THREE.SphereGeometry(0.02, 8, 6),
  smile: new THREE.BoxGeometry(0.07, 0.014, 0.01),
  fringe: new THREE.BoxGeometry(0.3, 0.07, 0.12),
  tongue: new THREE.BoxGeometry(0.1, 0.03, 0.09),
  bun: new THREE.SphereGeometry(0.075, 10, 8),
  tail: new THREE.CapsuleGeometry(0.05, 0.2, 3, 8),
  long: new THREE.BoxGeometry(0.35, 0.36, 0.13),
  lock: new THREE.BoxGeometry(0.06, 0.3, 0.16),
};

// Name and number for the back of a football shirt, drawn on the shirt colour so it needs no transparency.
const prints = new Map();
function backPrint(name, number, bg, fg) {
  const key = [name, number, bg, fg].join('|');
  if (prints.has(key)) return prints.get(key);
  const cv = document.createElement('canvas'); cv.width = 256; cv.height = 288;
  const g = cv.getContext('2d');
  g.fillStyle = bg; g.fillRect(0, 0, 256, 288);
  g.fillStyle = fg; g.textAlign = 'center'; g.textBaseline = 'middle';
  g.font = '800 62px Arial, sans-serif'; g.fillText(name, 128, 56, 240);
  g.font = '900 176px Arial, sans-serif'; g.fillText(number, 128, 186, 236);
  const t = new THREE.CanvasTexture(cv); t.colorSpace = THREE.SRGBColorSpace; t.anisotropy = 4;
  const m = new THREE.MeshLambertMaterial({ map: t });
  prints.set(key, m);
  return m;
}

export function makeFigure(o = {}) {
  const g = new THREE.Group();
  const part = (geo, c, x, y, z, parent = g) => { const m = new THREE.Mesh(geo, mat(c)); m.position.set(x, y, z); m.castShadow = true; parent.add(m); return m; };
  const jacket = o.jacket ?? 0xff7a1a, trousers = o.trousers ?? 0x2a2f3a, skin = o.skin ?? 0xe6b596, hair = o.hair ?? 0x3a2a1e;
  const torso = part(G.torso, jacket, 0, 1.17, 0); torso.scale.set(1.2, 1, 0.82);
  const css = (c) => '#' + c.toString(16).padStart(6, '0');
  if (o.back) { const b = new THREE.Mesh(G.back, backPrint(o.back.name, o.back.number, css(jacket), css(o.back.color ?? 0xffffff))); b.position.set(0, 1.2, -0.172); b.rotation.y = Math.PI; g.add(b); }
  if (o.suit) { part(G.shirt, 0xf4f1e8, 0, 1.3, 0.158); part(G.tie, o.tie ?? 0xb3252a, 0, 1.29, 0.17); }
  if (o.rosette !== undefined) {
    const r = part(G.rosette, o.rosette, -0.15, 1.36, 0.17); r.rotation.x = Math.PI / 2;
    const c = part(G.rosette, 0xf4d35e, -0.15, 1.36, 0.182); c.rotation.x = Math.PI / 2; c.scale.set(0.42, 1, 0.42);
    for (const s of [-1, 1]) { const rb = part(G.ribbon, o.rosette, -0.15 + s * 0.03, 1.25, 0.166); rb.rotation.z = s * 0.25; }
  }
  if (o.hiVis) { const b = part(G.band, 0xf4f1e8, 0, 1.1, 0); b.scale.set(1, 1, 0.72); }
  part(G.head, skin, 0, 1.69, 0);
  const h = part(G.hair, o.cap ?? hair, 0, 1.72, -0.01); h.rotation.x = -0.15;
  if (o.cap !== undefined) part(G.peak, o.cap, 0, 1.76, 0.2);
  if (o.face) {
    for (const s of [-1, 1]) part(G.eye, o.eyes ?? 0x2b2f36, s * 0.062, 1.71, 0.16);
    part(G.smile, 0x8a4a42, 0, 1.63, 0.168);
    // a short fringe swept to one side
    if (o.cap === undefined) { const f = part(G.fringe, hair, 0.02, 1.83, 0.09); f.rotation.z = -0.14; f.rotation.x = 0.25; }
  }
  // Hair styles for the player's chosen look: 'pony' is a ponytail, 'long' falls to the shoulders.
  if (o.style === 'pony') { part(G.bun, hair, 0, 1.76, -0.19); const t = part(G.tail, hair, 0, 1.6, -0.25); t.rotation.x = 0.35; }
  if (o.style === 'long') { part(G.long, hair, 0, 1.6, -0.12); for (const sd of [-1, 1]) part(G.lock, hair, sd * 0.165, 1.6, -0.02); }
  if (o.pack) part(G.pack, 0x3a3f4a, 0, 1.2, -0.25);
  const limb = (x, y, geo, len, c, endGeo, endC, ez) => {
    const pivot = new THREE.Group(); pivot.position.set(x, y, 0); g.add(pivot);
    part(geo, c, 0, -len / 2, 0, pivot);
    part(endGeo, endC, 0, -len - 0.02, ez, pivot);
    return pivot;
  };
  // A football kit: bare legs and arms with shorts, socks and short sleeves over them.
  const kit = o.kit;
  const legs = [-0.11, 0.11].map((x) => {
    const p = limb(x, 0.84, G.leg, 0.74, kit ? skin : trousers, G.shoe, kit ? kit.boots ?? 0x1b1d20 : 0x1b1d20, 0.05);
    if (kit) { part(G.shorts, kit.shorts, 0, -0.1, 0, p); part(G.sock, kit.socks, 0, -0.6, 0, p); }
    // Classic black leather boots: three white stripes down each side and a white fold-over tongue.
    if (kit && kit.stripes !== undefined) {
      for (const s of [-1, 1]) for (let i = 0; i < 3; i++) { const st = part(G.stripe, kit.stripes, s * 0.077, -0.76, 0.03 + i * 0.042, p); st.rotation.x = -0.45; }
      part(G.tongue, kit.stripes, 0, -0.71, 0.1, p);
    }
    return p;
  });
  const arms = [-0.31, 0.31].map((x) => {
    // Short sleeves, for a football kit or a replica shirt worn with ordinary trousers.
    const sleeves = kit ? kit.sleeves : o.sleeves;
    const p = limb(x, 1.42, G.arm, 0.56, sleeves !== undefined ? skin : jacket, G.hand, skin, 0);
    if (sleeves !== undefined) part(G.sleeve, sleeves, 0, -0.1, 0, p);
    return p;
  });
  let phone = null;
  if (o.phone) { phone = part(G.phone, 0x16171a, 0, -0.66, 0.03, arms[1]); phone.rotation.x = -0.5; }
  g.scale.setScalar(o.scale ?? 1);
  return { group: g, legs, arms, phone };
}
export function animateFigure(f, phase, amt) {
  const sw = Math.sin(phase) * amt * 0.8;
  f.legs[0].rotation.x = sw; f.legs[1].rotation.x = -sw;
  f.arms[0].rotation.x = -sw * 0.75; f.arms[1].rotation.x = sw * 0.75;
}

const pick = (a) => a[Math.floor(Math.random() * a.length)];
const JACKETS = [0x3b5b8c, 0x7a2f2f, 0x2f6b4f, 0x5a5560, 0xb8a03a, 0x8c8f96, 0x3a3d42, 0xa84a6a, 0x4a7d8c];
const TROUSERS = [0x2a2f3a, 0x3d4a66, 0x4a4238, 0x5a5d62];
export const SKIN = [0xe6b596, 0xd49a78, 0xf0c8ad, 0xa87453, 0x7a5238];
export const HAIR = [0x3a2a1e, 0x1b1b1d, 0x8a6a3a, 0xb9b9b4, 0x6a3a22];

export class Pedestrians {
  constructor(scene, paths, count) {
    this.peds = [];
    if (!paths.length) return;
    for (let i = 0; i < count; i++) {
      const path = pick(paths), cum = [0];
      for (let j = 1; j < path.length; j++) cum.push(cum[j - 1] + Math.hypot(path[j][0] - path[j - 1][0], path[j][1] - path[j - 1][1]));
      // About one passer-by in six wears a claret and blue shirt with the number ten of Andy Payton. No club badge is drawn.
      const fan = i % 6 === 2 ? { jacket: 0x6c1d45, sleeves: 0x99d6ea, back: { name: 'PAYTON', number: '10', color: 0xf4f1e8 } } : null;
      const fig = makeFigure({ jacket: pick(JACKETS), trousers: pick(TROUSERS), skin: pick(SKIN), hair: pick(HAIR), cap: Math.random() < 0.2 ? pick([0x5a5248, 0x2b3a67, 0x3a3d42]) : undefined, style: pick(['short', 'short', 'pony', 'long']), scale: 0.92 + Math.random() * 0.14, ...fan });
      scene.add(fig.group);
      this.peds.push({ fig, path, cum, total: cum[cum.length - 1], s: Math.random() * cum[cum.length - 1], dir: Math.random() < 0.5 ? 1 : -1, speed: 1.1 + Math.random() * 0.6, phase: Math.random() * 6, side: (Math.random() - 0.5) * 1.2, wait: 0 });
    }
  }
  update(dt, player) {
    for (const p of this.peds) {
      let j = 1;
      while (j < p.cum.length - 1 && p.cum[j] < p.s) j++;
      const a = p.path[j - 1], b = p.path[j], l = p.cum[j] - p.cum[j - 1] || 1, t = (p.s - p.cum[j - 1]) / l;
      const dx = (b[0] - a[0]) / l, dz = (b[1] - a[1]) / l;
      const x = a[0] + (b[0] - a[0]) * t - dz * p.side, z = a[1] + (b[1] - a[1]) * t + dx * p.side;
      const near = Math.hypot(player.x - x, player.z - z) < 1.3;
      let moving = !near && p.wait <= 0;
      if (p.wait > 0) p.wait -= dt;
      if (moving) {
        p.s += p.dir * p.speed * dt; p.phase += p.speed * dt * 4.2;
        if (p.s >= p.total) { p.s = p.total; p.dir = -1; p.wait = 1 + Math.random() * 4; }
        if (p.s <= 0) { p.s = 0; p.dir = 1; p.wait = 1 + Math.random() * 4; }
      }
      const g = p.fig.group;
      g.position.set(x, 0.14 + groundH(x, z), z);
      const want = near ? Math.atan2(player.x - x, player.z - z) : Math.atan2(dx * p.dir, dz * p.dir);
      g.rotation.y += Math.atan2(Math.sin(want - g.rotation.y), Math.cos(want - g.rotation.y)) * Math.min(1, dt * 6);
      animateFigure(p.fig, p.phase, moving ? 0.7 : 0);
    }
  }
}

// ---------- Pigeons ----------
const PG = {
  body: new THREE.SphereGeometry(1, 10, 8), head: new THREE.SphereGeometry(0.075, 8, 6),
  wing: new THREE.BoxGeometry(0.3, 0.015, 0.2).translate(0.15, 0, 0), tail: new THREE.BoxGeometry(0.11, 0.02, 0.2), beak: new THREE.ConeGeometry(0.02, 0.06, 5),
};
function makePigeon() {
  const g = new THREE.Group(), grey = pick([0x7d8794, 0x8b93a0, 0x6c7480, 0x9aa0a8]);
  const body = new THREE.Mesh(PG.body, mat(grey)); body.scale.set(0.11, 0.105, 0.19); body.position.y = 0.16; body.castShadow = true; g.add(body);
  const neck = new THREE.Mesh(PG.head, mat(0x4f6f66)); neck.position.set(0, 0.24, 0.13); neck.scale.set(1, 1.3, 1); g.add(neck);
  const head = new THREE.Mesh(PG.head, mat(0x5f6772)); head.position.set(0, 0.31, 0.16); g.add(head);
  const beak = new THREE.Mesh(PG.beak, mat(0xd9a05a)); beak.position.set(0, 0.3, 0.25); beak.rotation.x = Math.PI / 2; g.add(beak);
  const tail = new THREE.Mesh(PG.tail, mat(0x4a5058)); tail.position.set(0, 0.17, -0.24); g.add(tail);
  const wings = [1, -1].map((s) => { const w = new THREE.Mesh(PG.wing, mat(0x5a626e)); w.position.set(0.06 * s, 0.2, 0); w.scale.x = s; w.visible = false; g.add(w); return w; });
  g.scale.setScalar(1.5);
  return { g, wings };
}
export class Pigeons {
  constructor(scene, spots, perFlock, onScare) {
    this.onScare = onScare; this.flocks = []; this.spots = [];
    const pool = [...spots].sort(() => Math.random() - 0.5);
    for (const s of pool) if (this.spots.every((q) => Math.hypot(q[0] - s[0], q[1] - s[1]) > 32)) this.spots.push(s);
    const nFlocks = Math.min(7, Math.max(0, this.spots.length - 1));
    for (let f = 0; f < nFlocks; f++) {
      const flock = { spot: this.spots[f], flying: 0, birds: [] };
      for (let i = 0; i < perFlock; i++) {
        const b = makePigeon(), a = Math.random() * 6.28, r = Math.random() * 2;
        Object.assign(b, { x: flock.spot[0] + Math.cos(a) * r, z: flock.spot[1] + Math.sin(a) * r, y: 0.14, state: 'ground', tx: 0, tz: 0, t: Math.random() * 3, walk: false, phase: Math.random() * 6, h: Math.random() * 6 });
        scene.add(b.g); flock.birds.push(b);
      }
      this.flocks.push(flock);
    }
  }
  update(dt, player, time) {
    for (const f of this.flocks) {
      if (!f.flying && player.speed > 0.6 && Math.hypot(player.x - f.spot[0], player.z - f.spot[1]) < (player.speed > 6 ? 8.5 : 4.8)) {
        const taken = new Set(this.flocks.map((q) => q.spot));
        const free = this.spots.filter((s) => !taken.has(s));
        if (free.length) {
          const dest = free.sort((a, b) => Math.hypot(b[0] - player.x, b[1] - player.z) - Math.hypot(a[0] - player.x, a[1] - player.z))[Math.floor(Math.random() * Math.min(2, free.length))];
          f.spot = dest; f.flying = f.birds.length;
          for (const b of f.birds) {
            const a = Math.random() * 6.28, r = Math.random() * 2.2;
            Object.assign(b, { state: 'fly', fx: b.x, fz: b.z, tx: dest[0] + Math.cos(a) * r, tz: dest[1] + Math.sin(a) * r, t: -Math.random() * 0.35, H: 6 + Math.random() * 5 });
            b.dur = Math.hypot(b.tx - b.fx, b.tz - b.fz) / (9 + Math.random() * 3) + 1.2;
          }
          this.onScare(f.birds.length);
        }
      }
      for (const b of f.birds) {
        if (b.state === 'fly') {
          b.t += dt;
          const k = Math.min(1, Math.max(0, b.t / b.dur)), e = k * k * (3 - 2 * k);
          b.x = b.fx + (b.tx - b.fx) * e; b.z = b.fz + (b.tz - b.fz) * e;
          b.y = 0.14 + Math.sin(Math.PI * k) * b.H;
          b.h = Math.atan2(b.tx - b.fx, b.tz - b.fz);
          const flap = Math.sin(time * 30 + b.phase) * 0.95;
          b.wings[0].visible = b.wings[1].visible = true;
          b.wings[0].rotation.z = flap; b.wings[1].rotation.z = -flap;
          b.g.rotation.x = -0.25 * Math.cos(Math.PI * k);
          if (k >= 1) { b.state = 'ground'; b.t = Math.random() * 2; b.walk = false; b.wings[0].visible = b.wings[1].visible = false; f.flying--; }
        } else {
          b.t -= dt;
          if (b.t <= 0) {
            b.walk = !b.walk; b.t = b.walk ? 0.8 + Math.random() * 1.5 : 1 + Math.random() * 3;
            if (b.walk) { const a = Math.random() * 6.28, r = Math.random() * 2.2; b.tx = f.spot[0] + Math.cos(a) * r; b.tz = f.spot[1] + Math.sin(a) * r; b.h = Math.atan2(b.tx - b.x, b.tz - b.z); }
          }
          if (b.walk) { b.x += Math.sin(b.h) * 0.5 * dt; b.z += Math.cos(b.h) * 0.5 * dt; b.g.rotation.x = Math.sin(time * 14 + b.phase) * 0.08; }
          else b.g.rotation.x = Math.max(0, Math.sin(time * 5 + b.phase)) * 0.7;
        }
        b.g.position.set(b.x, b.y + groundH(b.x, b.z), b.z);
        b.g.rotation.y = b.h;
      }
    }
  }
}
