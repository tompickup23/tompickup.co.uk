// Budget Council: the rules engine. No DOM. The page and the test both drive it through
// createEngine(data) -> { start(opts), view(state), act(state, choiceId, payload) }.
// State is plain JSON, so a run can be replayed exactly from its seed and its choices.
// Every change to a variable is logged with the reason, and the debrief reads that log.
import { getBeat, FIRST } from './content.js';

/* Seeded randomness: Mulberry32. The state carries the generator's position, so the same seed
   and the same choices always give the same meeting. */
export function rand(s) {
  let t = (s.rng = (s.rng + 0x6d2b79f5) >>> 0);
  t = Math.imul(t ^ (t >>> 15), t | 1);
  t ^= t + Math.imul(t ^ (t >>> 7), t | 61);
  return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
}
export const pick = (s, list) => list[Math.floor(rand(s) * list.length)];
const clamp = (v, lo, hi) => Math.max(lo, Math.min(hi, v));
const r3 = (v) => Math.round(v * 1000) / 1000;
export const money = (v) => `£${Math.abs(v) < 1 ? Math.round(Math.abs(v) * 1000).toLocaleString('en-GB') + ',000' : Math.abs(v).toFixed(3) + 'm'}`;

export const ORDER_NOTE = 'Opposition groups are called in order of group size (Standing Order 4.9(c)).';

export function createEngine(data) {
  const { chamber, minutes, amendments } = data;
  const G = Object.fromEntries(chamber.groups.map((g) => [g.id, g]));
  const OPP = chamber.groups.filter((g) => g.id !== chamber.administration).sort((a, b) => b.seats - a.seats).map((g) => g.id);
  const PRIOR = {};
  for (const v of minutes.votes) {
    PRIOR[v.id] = {};
    for (const g of chamber.groups) {
      const n = (k) => v.byGroup[k][g.id] || 0;
      const tot = n('for') + n('against') + n('abstain');
      PRIOR[v.id][g.id] = tot ? { for: n('for') / tot, against: n('against') / tot, abstain: n('abstain') / tot } : { for: 0.1, against: 0.2, abstain: 0.7 };
    }
  }

  /* Seats: an invented plan. Opposition groups fill the Chairman's left from the front, the
     Administration the rest. Index order is the order the plan in the page draws them. */
  function seatGroups() {
    const out = [];
    const opp = OPP.flatMap((id) => Array(G[id].seats).fill(id));
    const rows = [11, 14, 17, 20, 22];
    let k = 0;
    const left = [];
    rows.forEach((n) => { const h = Math.ceil(n / 2); for (let i = 0; i < n; i++) { out.push(null); if (i < h) left.push(k); k++; } });
    const leftByRow = left; // front row first
    leftByRow.forEach((idx, j) => { if (j < opp.length) out[idx] = opp[j]; });
    for (let i = 0; i < out.length; i++) if (!out[i]) out[i] = chamber.administration;
    return out;
  }
  const SEATS = seatGroups();

  /* -------- state helpers, used by content.js -------- */
  function log(s, text, tag) { s.log.push({ at: s.beat, text, tag: tag || '' }); }
  const api = {
    G, OPP, amendments, data, money, rand, pick,
    add(s, key, delta, because) {
      // key: 'cred', 'mood', 'chair', 'score.proc' etc, or 'lean.<question>.<group>' / 'disc.<group>'
      const path = key.split('.');
      let o = s.v;
      for (let i = 0; i < path.length - 1; i++) o = o[path[i]] ??= {};
      const last = path[path.length - 1];
      const lim = key === 'mood' ? [-3, 3] : key === 'chair' ? [0, 5] : key.startsWith('score.') ? [0, 10] : key.startsWith('lean.') ? [-60, 60] : [0, 100];
      const before = o[last] ?? 0;
      o[last] = clamp(before + delta, lim[0], lim[1]);
      if (because) log(s, because, key);
    },
    flag(s, f, because) { if (!s.flags.includes(f)) s.flags.push(f); if (because) log(s, because, 'flag:' + f); },
    has: (s, f) => s.flags.includes(f),
    note(s, text) { s.record.push(text); log(s, text, 'record'); },
    score(s, axis, delta, because) { api.add(s, 'score.' + axis, delta, because); },
    lean(s, q, g, delta, because) { api.add(s, `lean.${q}.${g}`, delta, because); },
    vote: (s, q) => runVote(s, q),
    build: (s) => buildCheck(s),
    mine: (s) => s.group,
    opp: () => OPP,
  };

  /* -------- the amendment builder -------- */
  function buildCheck(s) {
    const A = amendments;
    const parts = (s.amend.parts || []).map((id) => ({ id, ...A.parts[id] })).filter((p) => p.label && !p.realOnly);
    const years = [0, 1, 2].map((y) => {
      const ph = parts.filter((p) => p.ph).reduce((n, p) => n + p.cost[y], 0);
      const other = parts.filter((p) => !p.ph).reduce((n, p) => n + p.cost[y], 0);
      let need = other, phUse = Math.min(ph, A.funds.ph.max[y]);
      need += ph - phUse; // public health spending above the reserve falls on general funds
      const rsg = Math.min(Math.max(need, 0), A.funds.rsg.max[y]);
      need -= rsg;
      const sc = Math.min(Math.max(need, 0), A.funds.sc.max[y]);
      need -= sc;
      return { spend: r3(ph + other), ph: r3(phUse), rsg: r3(rsg), sc: r3(sc), gap: r3(Math.max(need, 0)) };
    });
    const busgates = parts.filter((p) => p.busgate).length > 1;
    const balanced = years.every((y) => y.gap <= 0) && !busgates;
    const usesSc = years.some((y) => y.sc > 0);
    const groups = [...new Set(parts.map((p) => p.group))];
    const y1 = years[0].spend;
    return { parts, years, balanced, usesSc, busgates, groups, y1, empty: parts.filter((p) => !p.saving).length === 0 };
  }

  /* -------- votes -------- */
  // Each councillor present and able to vote votes for, against or abstains. Their group's
  // starting tendency comes from the real recorded votes; leans and whips move it.
  function runVote(s, q) {
    const prior = PRIOR[q.prior] || PRIOR.budget;
    const tally = [];
    const n = { for: 0, against: 0, abstain: 0, absent: 0, remote: 0, why: [] };
    const byGroup = {};
    SEATS.forEach((g, i) => {
      const at = s.att[g];
      byGroup[g] ??= { for: 0, against: 0, abstain: 0, out: 0 };
      const idx = SEATS.slice(0, i + 1).filter((x) => x === g).length; // 1-based within group
      if (idx > at.present) { tally.push('absent'); n.absent++; byGroup[g].out++; return; }
      const me = g === s.group && idx === 1;
      if (!me && s.remote.group === g && idx === at.present) { tally.push('absent'); n.remote++; byGroup[g].out++; return; }
      let choice;
      if (me) {
        // You. A member who has declared council tax arrears may not vote (Standing Order 4.9(j)).
        choice = api.has(s, 'arrears_declared') ? 'none' : s.myVote[q.id] || q.whip?.[g] || 'abstain';
      } else {
        const whip = q.whip?.[g];
        let p = { ...prior[g] };
        if (whip) {
          const d = (s.v.disc[g] ?? 80) / 100;
          p = { for: (1 - d) * p.for, against: (1 - d) * p.against, abstain: (1 - d) * p.abstain };
          p[whip] += d;
        }
        const lean = (s.v.lean[q.id]?.[g] || 0) + (s.v.lean.all?.[g] || 0);
        const k = Math.exp(lean / 25);
        const w = { for: (p.for + 0.01) * k, against: (p.against + 0.01) / k, abstain: p.abstain + 0.01 };
        const tot = w.for + w.against + w.abstain;
        const r = rand(s) * tot;
        choice = r < w.for ? 'for' : r < w.for + w.against ? 'against' : 'abstain';
      }
      if (choice === 'none') { tally.push('abstain'); n.abstain++; byGroup[g].out++; return; }
      tally.push(choice);
      n[choice]++;
      byGroup[g][choice]++;
    });
    let carried = n.for > n.against;
    let cast = null;
    if (n.for === n.against) { cast = q.chairCasts; carried = cast === 'for'; }
    const res = { id: q.id, label: q.label, ...n, carried, cast, tally, byGroup };
    s.votes[q.id] = res;
    return res;
  }

  /* -------- the run -------- */
  function start({ seed = 1, role = 'opp', group = 'pl', facilitator = false } = {}) {
    const s = {
      seed: seed >>> 0, rng: seed >>> 0, role, group: role === 'admin' ? chamber.administration : group, facilitator,
      beat: FIRST, flags: [], log: [], record: [], votes: {}, myVote: {}, hist: [],
      amend: { kind: null, parts: [], notice: null, circulate: null, s151: 'none' },
      v: { cred: 50, mood: 0, chair: 5, disc: {}, lean: {}, score: { proc: 5, faith: 5, infl: role === 'admin' ? 4 : 3 } },
      att: {}, remote: { group: null },
      clock: 0, ended: null,
    };
    for (const g of chamber.groups) s.v.disc[g.id] = g.id === chamber.administration ? 92 : 80;
    // Attendance varies run to run: a few apologies, and one member joins remotely.
    for (const g of chamber.groups) {
      const away = g.id === chamber.administration ? Math.floor(rand(s) * 4) + 1 : rand(s) < 0.25 ? 1 : 0;
      s.att[g.id] = { seats: g.seats, present: g.seats - (g.id === s.group ? Math.min(away, g.seats - 1) : away) };
    }
    s.remote.group = pick(s, OPP);
    // Which personal declaration this councillor faces: an interest, council tax arrears, or none.
    s.personal = pick(s, ['interest', 'arrears', 'none', 'interest']);
    return s;
  }

  function clone(s) { return JSON.parse(JSON.stringify(s)); }

  function view(s) {
    const b = getBeat(s.beat, s, api);
    return { ...b, choices: (b.choices || []).filter((c) => !c.when || c.when(s, api)) };
  }

  function act(state, choiceId, payload) {
    const s = clone(state);
    const b = getBeat(s.beat, s, api);
    if (s.ended) return s;
    let next = b.next;
    if (b.kind === 'build') {
      if (choiceId === 'parts') { s.amend.parts = (payload || []).filter((id) => amendments.parts[id] && !amendments.parts[id].realOnly); return s; }
    }
    const c = (b.choices || []).find((x) => x.id === choiceId && (!x.when || x.when(s, api)));
    if (!c) throw new Error(`No choice ${choiceId} at ${s.beat}`);
    s.hist.push(`${s.beat}:${choiceId}`);
    if (c.fx) c.fx(s, api, payload);
    if (c.go) next = c.go;
    s.clock += b.mins || 2;
    if (typeof next === 'function') next = next(s, api);
    if (!next) throw new Error(`Dead end at ${s.beat}`);
    s.beat = next;
    // A beat may run something as you arrive (a vote, an opponent's move).
    let guard = 0;
    for (;;) {
      const nb = getBeat(s.beat, s, api);
      if (nb.enter && !s.flags.includes('entered:' + s.beat)) { s.flags.push('entered:' + s.beat); nb.enter(s, api); }
      if (nb.kind === 'end') { s.ended = nb.ending; break; }
      if (nb.skip && nb.skip(s, api)) { s.beat = typeof nb.next === 'function' ? nb.next(s, api) : nb.next; if (++guard > 50) throw new Error('skip loop'); continue; }
      break;
    }
    return s;
  }

  // For the facilitator: replace the simulated count with hands counted in the room.
  function roomVote(state, qid, counts) {
    const s = clone(state);
    const v = s.votes[qid];
    if (!v) return s;
    Object.assign(v, { for: counts.for, against: counts.against, abstain: counts.abstain, room: true });
    v.carried = v.for > v.against || (v.for === v.against && v.cast === 'for');
    return s;
  }

  return { start, view, act, roomVote, SEATS, OPP, G, PRIOR, api, buildCheck };
}
