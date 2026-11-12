// The Padiham Job: walking routes along the streets and footpaths in the town file.
// The roads in the file share points where they meet, so the network is those points joined along each road.
// A route runs from the nearest point on the network to the nearest point to the place wanted. It is advice:
// the player may still cut across a park, and the route is worked out again from wherever they are.

const CELL = 40;
const KEY = (p) => `${Math.round(p[0] * 10)}_${Math.round(p[1] * 10)}`;

function project(px, pz, ax, az, bx, bz) {
  const dx = bx - ax, dz = bz - az, l2 = dx * dx + dz * dz;
  const t = l2 ? Math.max(0, Math.min(1, ((px - ax) * dx + (pz - az) * dz) / l2)) : 0;
  const qx = ax + dx * t, qz = az + dz * t;
  return [Math.hypot(px - qx, pz - qz), qx, qz, t];
}

export class Streets {
  // roads: [{ p: [[x, z], ...], c: class, n: name }], slice: [x0, z0, x1, z1] the part of town that can be walked,
  // blocked(ax, az, bx, bz): true where a stretch of road cannot be walked because a wall stands right across it.
  constructor(roads, slice, blocked) {
    this.nx = []; this.nz = []; this.adj = []; this.edges = []; this.grid = new Map();
    const ids = new Map();
    const node = (p) => {
      const k = KEY(p);
      let i = ids.get(k);
      if (i === undefined) { i = this.nx.length; ids.set(k, i); this.nx.push(p[0]); this.nz.push(p[1]); this.adj.push([]); }
      return i;
    };
    const inside = (p) => p[0] > slice[0] + 1 && p[0] < slice[2] - 1 && p[1] > slice[1] + 1 && p[1] < slice[3] - 1;
    for (const r of roads) {
      // Back yards and car parks are walked through only when there is no street that will do.
      const weight = r.c === 'service' ? 1.25 : 1;
      for (let i = 0; i < r.p.length - 1; i++) {
        const a = r.p[i], b = r.p[i + 1], len = Math.hypot(b[0] - a[0], b[1] - a[1]);
        if (len < 0.05 || !inside(a) || !inside(b)) continue;
        if (blocked(a[0], a[1], b[0], b[1])) continue;
        const u = node(a), v = node(b), e = { u, v, len, cost: len * weight, n: r.n || '', c: r.c };
        this.adj[u].push(e); this.adj[v].push(e); this.edges.push(e);
        const x0 = Math.floor(Math.min(a[0], b[0]) / CELL), x1 = Math.floor(Math.max(a[0], b[0]) / CELL), z0 = Math.floor(Math.min(a[1], b[1]) / CELL), z1 = Math.floor(Math.max(a[1], b[1]) / CELL);
        for (let gx = x0; gx <= x1; gx++) for (let gz = z0; gz <= z1; gz++) { const k = gx + ',' + gz; if (!this.grid.has(k)) this.grid.set(k, []); this.grid.get(k).push(e); }
      }
    }
    // Keep only the largest joined-up part, so a route is never sent to a stretch that leads nowhere.
    const part = new Int32Array(this.nx.length).fill(-1), sizes = [];
    for (let s = 0; s < this.nx.length; s++) {
      if (part[s] >= 0) continue;
      const id = sizes.length, stack = [s]; let n = 0; part[s] = id;
      while (stack.length) { const u = stack.pop(); n++; for (const e of this.adj[u]) { const v = e.u === u ? e.v : e.u; if (part[v] < 0) { part[v] = id; stack.push(v); } } }
      sizes.push(n);
    }
    const main = sizes.indexOf(Math.max(...sizes));
    this.live = (e) => part[e.u] === main;
    this.size = sizes[main] || 0;
  }

  // The nearest point on the network to (x, z). clear(x, z, qx, qz), if given, says whether the two can be joined
  // on foot; the nearest point that can is preferred, and the nearest of all is the fallback.
  snap(x, z, clear) {
    let best = null, open = null;
    for (let ring = 1; ring <= 5 && !open; ring++) {
      const gx = Math.floor(x / CELL), gz = Math.floor(z / CELL), seen = new Set();
      best = null;
      for (let i = gx - ring; i <= gx + ring; i++) for (let j = gz - ring; j <= gz + ring; j++) for (const e of this.grid.get(i + ',' + j) || []) {
        if (seen.has(e) || !this.live(e)) continue; seen.add(e);
        const [d, qx, qz, t] = project(x, z, this.nx[e.u], this.nz[e.u], this.nx[e.v], this.nz[e.v]);
        if (d > ring * CELL) continue;
        const hit = { e, d, x: qx, z: qz, t };
        if (!best || d < best.d) best = hit;
        if ((!open || d < open.d) && (!clear || clear(x, z, qx, qz))) open = hit;
      }
    }
    return open || best;
  }

  // The way from A to B: { pts: [[x, z], ...], names: [street name of each leg], len } or null.
  route(ax, az, bx, bz, clear) {
    const A = this.snap(ax, az, clear), B = this.snap(bx, bz, clear);
    if (!A || !B) return null;
    const pts = [[ax, az], [A.x, A.z]], names = [''];
    if (A.e === B.e) { pts.push([B.x, B.z]); names.push(A.e.n); }
    else {
      // Shortest way over the network, from the two ends of the stretch A is on to the two ends of the one B is on.
      const N = this.nx.length, dist = new Float64Array(N).fill(Infinity), via = new Array(N).fill(null), done = new Uint8Array(N);
      const heap = [];
      const push = (d, u) => { heap.push([d, u]); let i = heap.length - 1; while (i > 0) { const p = (i - 1) >> 1; if (heap[p][0] <= heap[i][0]) break; [heap[p], heap[i]] = [heap[i], heap[p]]; i = p; } };
      const pop = () => {
        const top = heap[0], last = heap.pop();
        if (heap.length) { heap[0] = last; let i = 0; for (;;) { const l = i * 2 + 1, r = l + 1; let m = i; if (l < heap.length && heap[l][0] < heap[m][0]) m = l; if (r < heap.length && heap[r][0] < heap[m][0]) m = r; if (m === i) break; [heap[m], heap[i]] = [heap[i], heap[m]]; i = m; } }
        return top;
      };
      const k = A.e.cost / A.e.len;
      dist[A.e.u] = A.t * A.e.len * k; push(dist[A.e.u], A.e.u);
      dist[A.e.v] = (1 - A.t) * A.e.len * k; push(dist[A.e.v], A.e.v);
      const kb = B.e.cost / B.e.len, endU = B.t * B.e.len * kb, endV = (1 - B.t) * B.e.len * kb;
      while (heap.length) {
        const [d, u] = pop();
        if (done[u]) continue; done[u] = 1;
        if (done[B.e.u] && done[B.e.v]) break;
        for (const e of this.adj[u]) { const v = e.u === u ? e.v : e.u, nd = d + e.cost; if (nd < dist[v]) { dist[v] = nd; via[v] = e; push(nd, v); } }
      }
      const last = dist[B.e.u] + endU <= dist[B.e.v] + endV ? B.e.u : B.e.v;
      if (!isFinite(dist[last])) return null;
      const chain = [];
      for (let u = last; via[u]; u = via[u].u === u ? via[u].v : via[u].u) chain.push([u, via[u]]);
      chain.reverse();
      const first = chain.length ? (chain[0][1].u === chain[0][0] ? chain[0][1].v : chain[0][1].u) : last;
      pts.push([this.nx[first], this.nz[first]]); names.push(A.e.n);
      for (const [u, e] of chain) { pts.push([this.nx[u], this.nz[u]]); names.push(e.n); }
      pts.push([B.x, B.z]); names.push(B.e.n);
    }
    pts.push([bx, bz]); names.push('');
    // Drop legs of no length, which appear where a snapped point sits on a corner.
    const P = [pts[0]], Nm = [];
    for (let i = 1; i < pts.length; i++) { if (Math.hypot(pts[i][0] - P[P.length - 1][0], pts[i][1] - P[P.length - 1][1]) < 0.05) continue; P.push(pts[i]); Nm.push(names[i - 1]); }
    const cum = [0];
    for (let i = 1; i < P.length; i++) cum.push(cum[i - 1] + Math.hypot(P[i][0] - P[i - 1][0], P[i][1] - P[i - 1][1]));
    return { pts: P, names: Nm, cum, len: cum[cum.length - 1] };
  }
}

// Where (x, z) is along a route: the leg, how far along the whole route, and how far off the line.
export function along(way, x, z) {
  let best = { i: 0, s: 0, off: Infinity };
  for (let i = 0; i < way.pts.length - 1; i++) {
    const a = way.pts[i], b = way.pts[i + 1], [d, , , t] = project(x, z, a[0], a[1], b[0], b[1]);
    if (d < best.off) best = { i, s: way.cum[i] + t * (way.cum[i + 1] - way.cum[i]), off: d };
  }
  return best;
}
// The point a given distance along a route.
export function pointAt(way, s) {
  const P = way.pts, c = way.cum;
  if (s >= way.len) return P[P.length - 1];
  let i = 0; while (i < c.length - 2 && c[i + 1] < s) i++;
  const t = c[i + 1] > c[i] ? (s - c[i]) / (c[i + 1] - c[i]) : 0;
  return [P[i][0] + (P[i + 1][0] - P[i][0]) * t, P[i][1] + (P[i + 1][1] - P[i][1]) * t];
}
// The next change of street after distance s: { m: metres to it, side: 'left' | 'right' | '', name } or null.
export function nextTurn(way, s) {
  const P = way.pts, c = way.cum;
  let i = 0; while (i < c.length - 2 && c[i + 1] <= s) i++;
  let cur = way.names[i];
  for (let j = i + 1; j < way.names.length; j++) {
    const nm = way.names[j];
    if (!cur) { cur = nm; continue; }
    if (!nm || nm === cur) continue;
    const a = P[j - 1], b = P[j], d = P[Math.min(P.length - 1, j + 1)];
    const cross = (b[0] - a[0]) * (d[1] - b[1]) - (b[1] - a[1]) * (d[0] - b[0]), dot = (b[0] - a[0]) * (d[0] - b[0]) + (b[1] - a[1]) * (d[1] - b[1]);
    const ang = Math.atan2(cross, dot);
    // In the game x runs east and z runs south, so a positive turn on the map is a turn to the right on the ground.
    return { m: c[j] - s, side: Math.abs(ang) < 0.45 ? '' : ang > 0 ? 'right' : 'left', name: nm };
  }
  return null;
}
