// Burnley Budget Council: walks the game headless, as scripts/budget-council.test.mjs does for
// Lancashire. Fails on dead ends, endings that cannot be reached, choices that change nothing,
// runs that do not replay exactly, seats that do not add up, figures without a source, and dashes.
import { test } from 'node:test';
import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import { createEngine } from '../src/games/budget-council/engine.js';
import * as content from '../src/games/budget-council/burnley/content.js';
import { ROOMS } from '../src/games/rooms/plans.js';

const { ENDINGS } = content;
const J = (p) => JSON.parse(readFileSync(new URL('../src/data/budget-council/burnley/' + p, import.meta.url)));
const chamber = J('chamber.json');
const data = { chamber, minutes: J('minutes.json'), amendments: J('amendments.json'), papers: J('papers.json'), sources: J('sources.json'), priors: chamber.priors };
const E = createEngine(data, content);
const SIDES = ['ref', 'lab', 'con'];

function walkerRng(seed) { let a = seed >>> 0; return () => { a = (a + 0x9e3779b9) >>> 0; let t = a ^ (a >>> 16); t = Math.imul(t, 0x21f0aaad); t ^= t >>> 15; t = Math.imul(t, 0x735a2d97); return ((t ^ (t >>> 15)) >>> 0) / 4294967296; }; }
const sig = (s) => JSON.stringify([s.v, s.flags.filter((f) => !f.startsWith('entered:') && !f.startsWith('used:')), s.record, s.amend, s.myVote, s.adminLine || {}, s.att, s.clock, s.beat, s.asked || []]);
const partIds = Object.keys(data.amendments.parts);
const randomChooser = (r) => ({ parts: () => partIds.filter(() => r() < 0.25), pick: (s, v) => v.choices[Math.floor(r() * v.choices.length)].id });

function play(opts, chooser) {
  let s = E.start(opts);
  const actions = [];
  for (let i = 0; i < 300; i++) {
    if (s.ended) return { s, actions };
    if (E.view(s).kind === 'build') { const p = chooser.parts(s); s = E.act(s, 'parts', p); actions.push(['parts', p]); }
    const v = E.view(s);
    assert.ok(v.choices.length > 0, `dead end at ${s.beat}`);
    const c = chooser.pick(s, v);
    s = E.act(s, c);
    actions.push([c]);
  }
  throw new Error('run did not end: ' + s.beat);
}
function replay(opts, actions) { let s = E.start(opts); for (const [c, p] of actions) s = E.act(s, c, p); return s; }

test('every run ends, replays exactly, and every ending is reachable', () => {
  const seen = { admin: new Set(), opp: new Set() };
  for (let seed = 1; seed <= 600; seed++) {
    for (const role of ['admin', 'opp']) {
      const opts = { seed: seed * 7919, role, group: role === 'opp' ? SIDES[seed % 3] : undefined };
      const { s, actions } = play(opts, randomChooser(walkerRng(seed * 31 + (role === 'admin' ? 1 : 2))));
      assert.ok(ENDINGS[s.ended], `unknown ending ${s.ended}`);
      seen[role].add(s.ended);
      if (seed % 25 === 0) assert.equal(JSON.stringify(replay(opts, actions)), JSON.stringify(s), 'replay differs');
    }
  }
  const want = { admin: ['misstep', 'nobudget', 'objected', 'amended', 'widened', 'concession', 'clean'], opp: ['misstep', 'folded', 'carried', 'record', 'defeated', 'statement', 'amended'] };
  for (const role of ['admin', 'opp']) for (const e of want[role]) assert.ok(seen[role].has(e), `${role} never reached ${e} (saw ${[...seen[role]]})`);
});

test('every choice at a decision changes something', () => {
  for (let seed = 1; seed <= 120; seed++) {
    for (const role of ['admin', 'opp']) {
      const r = walkerRng(seed * 101 + (role === 'admin' ? 3 : 4));
      let s = E.start({ seed, role, group: SIDES[seed % 3] });
      const ch = randomChooser(r);
      for (let i = 0; i < 300 && !s.ended; i++) {
        if (E.view(s).kind === 'build') s = E.act(s, 'parts', ch.parts(s));
        const v = E.view(s);
        if (v.choices.length > 1) {
          const outs = v.choices.map((c) => sig(E.act(s, c.id)));
          assert.equal(new Set(outs).size, outs.length, `two choices at ${s.beat} lead to the same state: ${v.choices.map((c) => c.id)}`);
        }
        s = E.act(s, ch.pick(s, v));
      }
    }
  }
});

test('the seats add up, and the room has a bench for each councillor but the Mayor', () => {
  const seats = chamber.groups.reduce((n, g) => n + g.seats, 0);
  assert.equal(seats, chamber.seats);
  assert.equal([].concat(chamber.administration).reduce((n, id) => n + chamber.groups.find((g) => g.id === id).seats, 0), 19);
  assert.equal(ROOMS.burnley.rows.reduce((a, b) => a + b, 0), chamber.seats - 1);
  assert.equal(chamber.rows.join(), ROOMS.burnley.rows.join());
  assert.equal(E.SEATS.length, chamber.seats - 1);
  const s = E.start({ seed: 3, role: 'admin' });
  const v = E.api.vote(s, { id: 'x', prior: 'budget', label: 'x', whip: {} });
  assert.equal(v.for + v.against + v.abstain + v.absent + v.remote, chamber.seats, 'every councillor, the Mayor included, is counted');
  for (const y of data.minutes.lastYear) assert.ok(y.totals.for + y.totals.against + y.totals.abstain <= chamber.seats);
});

test('every figure, rule and option names a source that exists', () => {
  const P = data.papers;
  for (const set of [P.figures, P.quotes, P.rules]) for (const [k, f] of Object.entries(set)) {
    assert.ok(data.sources[f.src], `${k} cites unknown source ${f.src}`);
    assert.ok(f.at, `${k} has no page, paragraph or table`);
  }
  for (const [k, p] of Object.entries(data.amendments.parts)) assert.ok(data.sources[p.src] && p.at && p.cost.length === 1, `part ${k}`);
  const words = readFileSync(new URL('../src/games/budget-council/burnley/content.js', import.meta.url), 'utf8');
  assert.ok(!/£\d/.test(words), 'content.js has a £ figure typed in; take it from papers.json');
});

test('no em dashes or en dashes in the game\'s words', () => {
  const files = ['src/games/budget-council/burnley/content.js', 'src/games/budget-council/burnley/ui.js', 'src/games/rooms/burnley.js', 'src/games/rooms/plans.js',
    'src/pages/budget-council/burnley/index.astro', 'src/components/budget-council/Shell.astro',
    ...['papers', 'amendments', 'minutes', 'chamber', 'sources'].map((f) => `src/data/budget-council/burnley/${f}.json`)];
  for (const f of files) {
    const t = readFileSync(new URL('../' + f, import.meta.url), 'utf8');
    assert.ok(!/[–—]|&[mn]dash;|&#821[12];|&#x201[34];/i.test(t), `${f} has an em or en dash`);
    assert.ok(!/\s-\s/.test(t.replace(/\/\/.*$/gm, '').replace(/\/\*[\s\S]*?\*\//g, '').replace(/[0-9a-z)\]] - [0-9a-z(]/gi, '')), `${f} has a spaced hyphen used as a dash`);
  }
});
