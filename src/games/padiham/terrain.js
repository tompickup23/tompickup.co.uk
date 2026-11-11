// Ground heights. The grid comes from Environment Agency LIDAR by way of scripts/padiham-game/build_terrain.py.
// Each grid square is two flat triangles, split along the same diagonal everywhere, so the ground mesh and
// groundH() describe exactly the same surface. With no terrain loaded the ground is flat at zero.
let T = null;

export function setTerrain(t) {
  if (!t || !Array.isArray(t.h)) { T = null; return; }
  // Each bridge deck: its line, with the running length at each point, and a box around it for a quick first test.
  const decks = (t.decks || []).map((d) => {
    const cum = [0];
    for (let i = 1; i < d.p.length; i++) cum.push(cum[i - 1] + Math.hypot(d.p[i][0] - d.p[i - 1][0], d.p[i][1] - d.p[i - 1][1]));
    const xs = d.p.map((q) => q[0]), zs = d.p.map((q) => q[1]);
    return { ...d, cum, box: [Math.min(...xs) - d.hw, Math.min(...zs) - d.hw, Math.max(...xs) + d.hw, Math.max(...zs) + d.hw] };
  });
  const h = Float32Array.from(t.h, (v) => v / 100);
  // The water has its own copy of the grid: a few nodes by bridge ends were raised to build the bank, and the
  // river must stay level beneath them.
  const hw = Float32Array.from(h);
  for (const [k, v] of t.wk || []) hw[k] = v / 100;
  T = { x0: t.x0, z0: t.z0, s: t.s, nx: t.nx, nz: t.nz, h, hw, decks };
}
export const terrain = () => T;

// Height of a bridge deck over a point, or -Infinity where there is none.
function deckH(x, z) {
  let best = -Infinity;
  for (const d of T.decks) {
    if (x < d.box[0] || z < d.box[1] || x > d.box[2] || z > d.box[3]) continue;
    for (let i = 0; i < d.p.length - 1; i++) {
      const a = d.p[i], b = d.p[i + 1], dx = b[0] - a[0], dz = b[1] - a[1], l2 = dx * dx + dz * dz || 1;
      const t = Math.max(0, Math.min(1, ((x - a[0]) * dx + (z - a[1]) * dz) / l2));
      if (Math.hypot(x - a[0] - dx * t, z - a[1] - dz * t) > d.hw) continue;
      const along = (d.cum[i] + t * (d.cum[i + 1] - d.cum[i])) / (d.cum[d.cum.length - 1] || 1);
      best = Math.max(best, d.a + (d.b - d.a) * along);
    }
  }
  return best;
}

// Height of the surface at a point, in metres: the ground, or a bridge deck where one passes over it.
// With 'under' true it is always the ground itself, which is what the ground mesh follows; with 'under' set to
// 'water' it is the level the water lies at.
// Beyond the grid the ground carries on level from the nearest edge.
export function groundH(x, z, under = false) {
  if (!T) return 0;
  const g = under === 'water' ? T.hw : T.h;
  const u = Math.min(Math.max((x - T.x0) / T.s, 0), T.nx - 1.0001), v = Math.min(Math.max((z - T.z0) / T.s, 0), T.nz - 1.0001);
  const i = Math.floor(u), j = Math.floor(v), fu = u - i, fv = v - j, k = j * T.nx + i;
  const h00 = g[k], h10 = g[k + 1], h01 = g[k + T.nx], h11 = g[k + T.nx + 1];
  const h = fu >= fv ? h00 + fu * (h10 - h00) + fv * (h11 - h10) : h00 + fu * (h11 - h01) + fv * (h01 - h00);
  return under || !T.decks.length ? h : Math.max(h, deckH(x, z));
}
