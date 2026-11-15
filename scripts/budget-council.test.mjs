// Budget Council: walks the game headless. Fails on dead ends, endings that cannot be reached,
// choices that change nothing, runs that do not replay exactly from their seed, data that does
// not add up, figures without a source, and dashes the house style does not allow.
import { test } from 'node:test';
import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import { createEngine } from '../src/games/budget-council/engine.js';
import { ENDINGS } from '../src/games/budget-council/content.js';

const J = (p) => JSON.parse(readFileSync(new URL('../src/data/budget-council/' + p, import.meta.url)));
const data = { chamber: J('chamber.json'), minutes: J('minutes.json'), amendments: J('amendments.json'), papers: J('papers.json'), sources: J('sources.json') };
const E = createEngine(data);
const GROUPS = E.OPP;

// A separate generator for the walker's own choices, so it never disturbs the game's.
function walkerRng(seed) { let a = seed >>> 0; return () => { a = (a + 0x9e3779b9) >>> 0; let t = a ^ (a >>> 16); t = Math.imul(t, 0x21f0aaad); t ^= t >>> 15; t = Math.imul(t, 0x735a2d97); return ((t ^ (t >>> 15)) >>> 0) / 4294967296; }; }

const sig = (s) => JSON.stringify([s.v, s.flags.filter((f) => !f.startsWith('entered:') && !f.startsWith('used:')), s.record, s.amend, s.myVote, s.adminLine || {}, s.att, s.clock, s.beat, s.asked || []]);

function play(opts, chooser) {
  let s = E.start(opts);
  const actions = [];
  for (let i = 0; i < 300; i++) {
    if (s.ended) return { s, actions };
    const v = E.view(s);
    if (v.kind === 'build') {
      const parts = chooser.parts(s);
      s = E.act(s, 'parts', parts);
      actions.push(['parts', parts]);
    }
    const vv = E.view(s);
    assert.ok(vv.choices.length > 0, `dead end at ${s.beat}`);
    const c = chooser.pick(s, vv);
    s = E.act(s, c, undefined);
    actions.push([c]);
  }
  throw new Error('run did not end within 300 steps: ' + s.beat);
}
function replay(opts, actions) {
  let s = E.start(opts);
  for (const [c, p] of actions) s = E.act(s, c, p);
  return s;
}
const partIds = Object.keys(data.amendments.parts).filter((id) => !data.amendments.parts[id].realOnly);
function randomChooser(r) {
  return {
    parts: () => partIds.filter(() => r() < 0.18),
    pick: (s, v) => v.choices[Math.floor(r() * v.choices.length)].id,
  };
}

test('every run ends, replays exactly, and every ending is reachable for each side', () => {
  const seen = { admin: new Set(), opp: new Set() };
  const beats = new Set();
  let runs = 0;
  for (let seed = 1; seed <= 500; seed++) {
    for (const role of ['admin', 'opp']) {
      const group = role === 'opp' ? GROUPS[seed % GROUPS.length] : undefined;
      const opts = { seed: seed * 7919, role, group };
      const { s, actions } = play(opts, randomChooser(walkerRng(seed * 31 + (role === 'admin' ? 1 : 2))));
      runs++;
      assert.ok(ENDINGS[s.ended], `unknown ending ${s.ended}`);
      seen[role].add(s.ended);
      s.hist.forEach((h) => beats.add(h.split(':').slice(0, -1).join(':')));
      if (seed % 25 === 0) assert.equal(JSON.stringify(replay(opts, actions)), JSON.stringify(s), 'replay differs');
    }
  }
  // Scripted best play: a small, approved, early, briefed amendment can be accepted.
  const best = bestPlay();
  seen.opp.add(best);
  const want = { admin: ['misstep', 'reconvene', 'amended', 'concession', 'clean'], opp: ['misstep', 'ruled_out', 'carried', 'record', 'defeated', 'statement'] };
  for (const role of ['admin', 'opp']) for (const e of want[role]) assert.ok(seen[role].has(e), `${role} never reached ending ${e} in ${runs} runs (saw ${[...seen[role]]})`);
  assert.ok(beats.size > 40, 'too few beats visited: ' + beats.size);
});

function bestPlay() {
  const prefer = ['own', 'done', 'consult', 'early', 'cm', 'g_lab', 'line', 'declare', 'none', 'listen', 'money', 'reserve', 'figure', 's25', 'sorry', 'pe', 'thank', 'declare', 'thank', 'vote', 'for', 'next', 'abstain', 'end'];
  for (let seed = 1; seed < 400; seed++) {
    const { s } = play({ seed, role: 'opp', group: 'owl' }, {
      parts: () => ['owl_flood', 'owl_pavements'],
      pick: (s, v) => (prefer.find((p) => v.choices.some((c) => c.id === p)) || v.choices[0].id),
    });
    if (s.ended === 'carried') return 'carried';
  }
  return 'none';
}

test('every choice at a decision changes something', () => {
  for (let seed = 1; seed <= 120; seed++) {
    for (const role of ['admin', 'opp']) {
      const r = walkerRng(seed * 101 + (role === 'admin' ? 3 : 4));
      let s = E.start({ seed, role, group: GROUPS[seed % GROUPS.length] });
      const ch = randomChooser(r);
      for (let i = 0; i < 300 && !s.ended; i++) {
        if (E.view(s).kind === 'build') s = E.act(s, 'parts', ch.parts(s));
        const v = E.view(s);
        if (v.choices.length > 1) {
          const outs = v.choices.map((c) => sig(E.act(s, c.id)));
          const uniq = new Set(outs);
          assert.equal(uniq.size, outs.length, `two choices at ${s.beat} lead to the same state: ${v.choices.map((c) => c.id)}`);
        }
        s = E.act(s, ch.pick(s, v));
      }
    }
  }
});

test('the real amendments balance in every year, as printed', () => {
  const A = data.amendments;
  for (const [g, am] of Object.entries(A.amendments)) {
    for (let y = 0; y < 3; y++) {
      const spend = am.parts.reduce((n, id) => n + A.parts[id].cost[y], 0);
      const fund = Object.values(am.funding).reduce((n, f) => n + f[y], 0);
      assert.ok(Math.abs(spend - fund) < 0.0006, `${g} ${A.years[y]}: parts ${spend.toFixed(3)} against funding ${fund.toFixed(3)}`);
    }
    for (const id of am.parts) assert.ok(A.parts[id] && A.parts[id].at, `${g}: part ${id} missing or unsourced`);
  }
});

test('the minutes add up, group by group', () => {
  for (const v of data.minutes.votes) for (const k of ['for', 'against', 'abstain']) {
    const sum = Object.values(v.byGroup[k]).reduce((a, b) => a + b, 0);
    assert.equal(sum, v.totals[k], `${v.id} ${k}`);
  }
  const seats = data.chamber.groups.reduce((n, g) => n + g.seats, 0);
  assert.equal(seats, data.chamber.seats);
  assert.equal(data.chamber.groups.find((g) => g.id === 'reform').seats, 51);
});

test('every figure and rule names a source that exists', () => {
  const P = data.papers;
  for (const set of [P.figures, P.quotes, P.rules]) for (const [k, f] of Object.entries(set)) {
    assert.ok(data.sources[f.src], `${k} cites unknown source ${f.src}`);
    assert.ok(f.at, `${k} has no page, paragraph or table`);
  }
  for (const f of Object.values(data.amendments.funds)) if (f.max) assert.ok(data.sources[f.src] && f.at);
  const content = readFileSync(new URL('../src/games/budget-council/content.js', import.meta.url), 'utf8');
  assert.ok(!/£\d/.test(content), 'content.js has a £ figure typed in; take it from papers.json or amendments.json');
});

test('no em dashes or en dashes in the game\'s words', () => {
  const files = ['src/games/budget-council/content.js', 'src/games/budget-council/engine.js', 'src/pages/budget-council/index.astro',
    'src/data/budget-council/papers.json', 'src/data/budget-council/amendments.json', 'src/data/budget-council/minutes.json', 'src/data/budget-council/chamber.json', 'src/data/budget-council/sources.json'];
  for (const f of files) {
    let t;
    try { t = readFileSync(new URL('../' + f, import.meta.url), 'utf8'); } catch { continue; }
    assert.ok(!/[–—]|&[mn]dash;|&#821[12];|&#x201[34];/i.test(t), `${f} has an em or en dash`);
    assert.ok(!/\s-\s/.test(t.replace(/\/\/.*$/gm, '').replace(/\/\*[\s\S]*?\*\//g, '').replace(/[0-9a-z)\]] - [0-9a-z(]/gi, '')), `${f} has a spaced hyphen used as a dash`);
  }
});
