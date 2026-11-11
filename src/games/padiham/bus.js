// Bus rides between the real stops in the town centre.
// The stops, the service numbers and the order each service calls in come from OpenStreetMap route relations
// by way of build_world.py. They can be out of date, and the game shows no times: it is for getting to know the
// steps of a bus journey (find the stop, pick the right number, get on, ring the bell, get off), not for
// planning one. The bus is a plain white single decker with no operator's colours or name.
import * as THREE from 'three';
import { groundH } from './terrain.js';

const SPEED = 10, BRAKE = 2.4, LEN = 10.4;

function blind(text) {
  const cv = document.createElement('canvas'); cv.width = 512; cv.height = 96;
  const g = cv.getContext('2d');
  g.fillStyle = '#14161a'; g.fillRect(0, 0, 512, 96);
  g.fillStyle = '#ffb81f'; g.font = '800 58px system-ui, sans-serif'; g.textAlign = 'center'; g.textBaseline = 'middle';
  g.fillText(text, 256, 52, 490);
  const t = new THREE.CanvasTexture(cv); t.colorSpace = THREE.SRGBColorSpace;
  return t;
}

function makeBus() {
  const g = new THREE.Group();
  const m = (c) => new THREE.MeshLambertMaterial({ color: c });
  const box = (c, w, h, l, x, y, z) => { const b = new THREE.Mesh(new THREE.BoxGeometry(w, h, l), typeof c === 'number' ? m(c) : c); b.position.set(x, y, z); b.castShadow = true; g.add(b); return b; };
  box(0xf4f1e8, 2.5, 2.6, LEN, 0, 1.75, 0);             // body
  box(0x55606b, 2.52, 0.55, LEN + 0.02, 0, 0.72, 0);    // skirt
  box(0x22303a, 2.54, 0.95, LEN - 1.4, 0, 2.25, -0.2);  // side windows
  box(0x22303a, 2.3, 1.25, 0.06, 0, 2.05, LEN / 2);     // windscreen
  box(0x22303a, 2.2, 0.8, 0.06, 0, 2.3, -LEN / 2);      // back window
  box(0x2b3138, 0.06, 2.0, 1.1, 1.26, 1.45, LEN / 2 - 1.2); // the door, on the pavement side
  const wheel = new THREE.CylinderGeometry(0.48, 0.48, 0.3, 14).rotateZ(Math.PI / 2), tyre = m(0x1b1d20);
  for (const x of [-1.15, 1.15]) for (const z of [LEN / 2 - 2.2, -LEN / 2 + 2.4]) { const w = new THREE.Mesh(wheel, tyre); w.position.set(x, 0.48, z); g.add(w); }
  const front = new THREE.MeshBasicMaterial({ color: 0xffffff }), side = new THREE.MeshBasicMaterial({ color: 0xffffff });
  const fb = new THREE.Mesh(new THREE.PlaneGeometry(2.2, 0.42), front); fb.position.set(0, 2.86, LEN / 2 + 0.04); g.add(fb);
  const sb = new THREE.Mesh(new THREE.PlaneGeometry(2.4, 0.45), side); sb.position.set(1.275, 2.86, LEN / 2 - 3.2); sb.rotation.y = Math.PI / 2; g.add(sb);
  g.visible = false;
  return { g, mats: [front, side] };
}

export class Buses {
  // ui: { toast, say, ding, fade, panel(text, canRing), board(), alight(x, z, heading) }
  constructor(scene, world, ui) {
    this.ui = ui;
    const b = world.bus || { stops: [], routes: [] };
    this.stops = b.stops; this.routes = b.routes;
    // The roads a bus can use, as a graph of their points.
    this.nodes = new Map(); this.segs = [];
    const key = (p) => p[0] + ',' + p[1];
    for (const r of world.roads) {
      if (r.c === 'foot' || r.c === 'service') continue;
      for (let i = 0; i < r.p.length - 1; i++) {
        const a = r.p[i], c = r.p[i + 1], l = Math.hypot(c[0] - a[0], c[1] - a[1]);
        if (l < 0.01) continue;
        for (const [p, q] of [[a, c], [c, a]]) { let n = this.nodes.get(key(p)); if (!n) this.nodes.set(key(p), n = { p, e: [] }); n.e.push([key(q), l]); }
        this.segs.push([a, c]);
      }
    }
    for (const s of this.stops) s.on = this.project(s.x, s.z);
    this.bus = makeBus(); scene.add(this.bus.g);
    this.phase = 'idle'; this.t = 0;
  }
  get riding() { return this.phase === 'ride' || this.phase === 'boarding'; }
  get busy() { return this.phase !== 'idle'; }
  project(x, z) {
    let best = null, bd = 1e9;
    for (const [a, c] of this.segs) {
      const dx = c[0] - a[0], dz = c[1] - a[1], t = Math.max(0, Math.min(1, ((x - a[0]) * dx + (z - a[1]) * dz) / (dx * dx + dz * dz)));
      const qx = a[0] + dx * t, qz = a[1] + dz * t, d = Math.hypot(x - qx, z - qz);
      if (d < bd) { bd = d; best = { q: [qx, qz], a, c }; }
    }
    return best;
  }
  // The shortest way along the roads between two stops, as a line of points.
  path(from, to) {
    const A = from.on, B = to.on, key = (p) => p[0] + ',' + p[1], d2 = (p, q) => Math.hypot(p[0] - q[0], p[1] - q[1]);
    if (!A || !B) return null;
    if (A.a === B.a && A.c === B.c) return [A.q, B.q];
    const dist = new Map([[key(A.a), d2(A.q, A.a)], [key(A.c), d2(A.q, A.c)]]), prev = new Map(), done = new Set();
    const ends = [key(B.a), key(B.c)];
    while (!(done.has(ends[0]) && done.has(ends[1]))) {
      let k = null, kd = Infinity;
      for (const [n, d] of dist) if (!done.has(n) && d < kd) { kd = d; k = n; }
      if (k === null) break;
      done.add(k);
      for (const [n, l] of this.nodes.get(k).e) if (!done.has(n) && kd + l < (dist.get(n) ?? Infinity)) { dist.set(n, kd + l); prev.set(n, k); }
    }
    const cost = (e, p) => (dist.has(e) ? dist.get(e) + d2(p, B.q) : Infinity);
    let k = cost(ends[0], B.a) <= cost(ends[1], B.c) ? ends[0] : ends[1];
    if (!dist.has(k)) return null;
    const out = [B.q];
    for (; k; k = prev.get(k)) out.push(this.nodes.get(k).p);
    out.push(A.q);
    return out.reverse().filter((p, i, arr) => i === 0 || d2(p, arr[i - 1]) > 0.05);
  }
  // Keep to the left of the road, and measure the running length.
  lane(p) {
    const out = [];
    for (let i = 0; i < p.length; i++) {
      let nx = 0, nz = 0;
      for (const j of [i - 1, i]) { if (j < 0 || j >= p.length - 1) continue; const dx = p[j + 1][0] - p[j][0], dz = p[j + 1][1] - p[j][1], l = Math.hypot(dx, dz) || 1; nx += dz / l; nz += -dx / l; }
      const l = Math.hypot(nx, nz) || 1;
      out.push([p[i][0] + (nx / l) * 2.1, p[i][1] + (nz / l) * 2.1]);
    }
    const cum = [0];
    for (let i = 1; i < out.length; i++) cum.push(cum[i - 1] + Math.hypot(out[i][0] - out[i - 1][0], out[i][1] - out[i - 1][1]));
    return { p: out, cum, len: cum[cum.length - 1] };
  }
  at(line, s) {
    const c = line.cum; let i = 1;
    while (i < c.length - 1 && c[i] < s) i++;
    const a = line.p[i - 1], b = line.p[i], l = c[i] - c[i - 1] || 1, t = Math.max(0, Math.min(1, (s - c[i - 1]) / l));
    return { x: a[0] + (b[0] - a[0]) * t, z: a[1] + (b[1] - a[1]) * t, h: Math.atan2(b[0] - a[0], b[1] - a[1]) };
  }
  // The stop the player is standing at, if any.
  near(x, z) { for (const s of this.stops) if (Math.hypot(s.x - x, s.z - z) < 5) return s; return null; }
  // The buses that call at a stop and go on to at least one more stop in the town centre.
  options(stop) {
    const ix = this.stops.indexOf(stop), out = [];
    for (const r of this.routes) {
      const k = r.s.indexOf(ix);
      if (k < 0 || k === r.s.length - 1) continue;
      out.push({ route: r, k, later: r.s.slice(k + 1).map((i) => this.stops[i]) });
    }
    return out;
  }
  // The player has chosen a bus: bring it to the stop.
  call(stop, opt) {
    const legs = [], halts = [];
    let all = [], from = stop;
    for (const to of opt.later) {
      const leg = this.path(from, to);
      if (!leg) break;
      all = all.length ? all.concat(leg.slice(1)) : leg;
      legs.push(to); from = to;
    }
    if (!legs.length) { this.ui.toast('No bus can be drawn from here', 'Try another stop', 2500); return; }
    this.ride = this.lane(all);
    // Where along the ride each later stop is.
    for (const to of legs) { let best = 0, bd = 1e9; for (let s = 0; s <= this.ride.len; s += 2) { const q = this.at(this.ride, s), d = Math.hypot(q.x - to.x, q.z - to.z); if (d < bd) { bd = d; best = s; } } halts.push({ stop: to, s: best }); }
    halts[halts.length - 1].s = this.ride.len;
    // The way in: from the stop before, if the service has one on the map, or else straight along the road.
    const prevIx = opt.k > 0 ? this.stops[opt.route.s[opt.k - 1]] : null;
    let inPath = prevIx ? this.path(prevIx, stop) : null;
    if (!inPath || inPath.length < 2) { const a = all[0], b = all[1], l = Math.hypot(b[0] - a[0], b[1] - a[1]) || 1; inPath = [[a[0] - ((b[0] - a[0]) / l) * 45, a[1] - ((b[1] - a[1]) / l) * 45], a]; }
    this.arrive = this.lane(inPath);
    this.s = Math.max(0, this.arrive.len - 70);
    this.stop = stop; this.opt = opt; this.halts = halts; this.next = 0; this.belled = false; this.v = 0;
    const name = `${opt.route.ref} ${opt.route.to}`;
    for (const mt of this.bus.mats) { if (mt.map) mt.map.dispose(); mt.map = blind(name); mt.needsUpdate = true; }
    this.phase = 'wait'; this.t = 2.5;
    this.ui.toast(`Wait here for the ${opt.route.ref}`, `It says ${opt.route.to} on the front`, 3200);
    this.ui.panel(`Waiting at ${stop.n} for the ${opt.route.ref} to ${opt.route.to}`, false);
  }
  ring() {
    if (this.phase !== 'ride' || this.belled) return;
    this.belled = true; this.ui.ding();
    const h = this.halts[this.next];
    this.ui.panel(`Bell rung. The bus will stop at ${h.stop.n}`, false);
    this.ui.say(`Bell rung. Stopping at ${h.stop.n}`);
  }
  cancel() { this.phase = 'idle'; this.bus.g.visible = false; this.ui.panel(null); }
  place(line, s) {
    const q = this.at(line, s), g = this.bus.g;
    g.position.set(q.x, groundH(q.x, q.z) + 0.14, q.z); g.rotation.y = q.h; g.visible = true;
    return q;
  }
  // Move along a line towards a stopping point, slowing as it nears. Returns true once stopped there.
  drive(dt, target) {
    const left = target - this.s, want = Math.min(SPEED, Math.sqrt(Math.max(0, 2 * BRAKE * left)) + 0.4);
    this.v += Math.max(-BRAKE * 2 * dt, Math.min(BRAKE * dt, want - this.v));
    this.s = Math.min(target, this.s + this.v * dt);
    return left < 0.15;
  }
  update(dt, player) {
    const ui = this.ui, ref = this.opt && this.opt.route.ref;
    if (this.phase === 'idle') return;
    if (this.phase === 'wait' || this.phase === 'in') {
      if (Math.hypot(player.x - this.stop.x, player.z - this.stop.z) > 14) { this.cancel(); ui.toast('You walked away from the stop', 'Go back to it to catch a bus', 2600); return; }
    }
    if (this.phase === 'wait') { this.t -= dt; if (this.t <= 0) { this.phase = 'in'; ui.say(`The ${ref} is coming`); } return; }
    if (this.phase === 'in') {
      const stopped = this.drive(dt, this.arrive.len);
      this.place(this.arrive, this.s);
      if (stopped) {
        this.phase = 'boarding'; this.t = 2.2; this.v = 0;
        ui.toast(`The ${ref} to ${this.opt.route.to} is here`, 'Getting on. Show your pass or pay the driver', 2600);
        ui.fade(); ui.board();
      }
      return;
    }
    if (this.phase === 'boarding') {
      this.t -= dt;
      const q = this.place(this.arrive, this.arrive.len);
      player.x = q.x; player.z = q.z; player.heading = q.h;
      if (this.t <= 0) { this.phase = 'ride'; this.s = 0; this.tellNext(); }
      return;
    }
    if (this.phase === 'ride') {
      const h = this.halts[this.next], last = this.next === this.halts.length - 1;
      const stopping = this.belled || last;
      const stopped = this.drive(dt, stopping ? h.s : this.ride.len);
      const q = this.place(this.ride, this.s);
      player.x = q.x; player.z = q.z; player.heading = q.h;
      if (stopping && stopped) {
        // Step off on to the pavement by the stop.
        this.phase = 'out'; this.t = 0; this.v = 0;
        // The stop is mapped at the kerb: step a little further on to the pavement, away from the road.
        const on = h.stop.on.q, l = Math.hypot(h.stop.x - on[0], h.stop.z - on[1]) || 1;
        ui.fade(); ui.alight(h.stop.x + ((h.stop.x - on[0]) / l) * 1.5, h.stop.z + ((h.stop.z - on[1]) / l) * 1.5, q.h);
        ui.toast(`You got off at ${h.stop.n}`, last && !this.belled ? 'This is as far as the bus goes in the game' : 'Well done', 3000);
        ui.panel(null);
      } else if (!stopping && this.s > h.s + 4) { this.next++; this.tellNext(); }
      return;
    }
    if (this.phase === 'out') {
      // The bus pulls away and goes.
      this.t += dt;
      if (this.t > 1.5) { this.v = Math.min(SPEED, this.v + BRAKE * dt); this.s += this.v * dt; }
      if (this.s >= this.ride.len + 0.01 || this.t > 9) { this.cancel(); return; }
      this.place(this.ride, Math.min(this.s, this.ride.len));
    }
  }
  tellNext() {
    const h = this.halts[this.next], last = this.next === this.halts.length - 1;
    this.belled = false;
    this.ui.panel(last ? `Next stop: ${h.stop.n}. The bus stops here` : `Next stop: ${h.stop.n}`, !last);
    this.ui.say(`Next stop: ${h.stop.n}${last ? '' : '. Ring the bell to get off there'}`);
  }
}
