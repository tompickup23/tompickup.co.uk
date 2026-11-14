import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { spawnSync } from 'node:child_process';
import { fileURLToPath } from 'node:url';
import test from 'node:test';
import { cardFigures, DATA, loadData, loadEdition, PAGE_SRC, SIDECAR } from './generate_burnley_budget_game_card.mjs';

const root = fileURLToPath(new URL('../', import.meta.url));
const pngPath = 'dist/images/share/burnley-budget-game.png';
const pagePath = 'dist/burnley-budget-game/index.html';
const D_RE = /const D = (\{[\s\S]*?\});\nconst PY/;

function fixture(t) {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'burnleycard'));
  t.after(() => fs.rmSync(dir, { recursive: true, force: true }));
  for (const file of [SIDECAR, DATA, PAGE_SRC, pngPath, pagePath]) {
    fs.mkdirSync(path.dirname(path.join(dir, file)), { recursive: true });
    fs.copyFileSync(path.join(root, file), path.join(dir, file));
  }
  return dir;
}

function guard(dir) {
  return spawnSync(process.execPath, [path.join(root, 'scripts/guard-burnley-budget-game-card.mjs')], { cwd: dir, encoding: 'utf8' });
}

function editData(dir, change) {
  const file = path.join(dir, DATA);
  const html = fs.readFileSync(file, 'utf8');
  const data = JSON.parse(html.match(D_RE)[1]);
  change(data);
  fs.writeFileSync(file, html.replace(D_RE, () => `const D = ${JSON.stringify(data)};\nconst PY`));
}

test('the restored fixture passes', t => {
  const passed = guard(fixture(t));
  assert.equal(passed.status, 0, passed.stderr);
});

test('every recorded figure fails when changed', t => {
  const dir = fixture(t);
  const original = fs.readFileSync(path.join(dir, SIDECAR), 'utf8');
  const baseline = JSON.parse(original);
  const mutations = [
    ['play year', side => { side.play_year = '2000/01'; }],
    ['gap', side => { side.gap_m += 0.001; }],
    ['rate', side => { side.rate_pct = 2.99; }],
    ['table', side => { side.table = 'Table 5'; }],
    ['page', side => { side.page += 1; }],
    ['edition', side => { side.edition = '1 January 2026'; }],
    ['source', side => { side.source = 'Fixture citation'; }],
    ['card figure text', side => { side.text.figure = '£0.507m'; }],
  ];
  for (const [name, change] of mutations) {
    const side = structuredClone(baseline);
    change(side);
    fs.writeFileSync(path.join(dir, SIDECAR), JSON.stringify(side));
    const failed = guard(dir);
    assert.equal(failed.status, 1, `${name}: ${failed.stderr}`);
    assert.match(failed.stderr, /Burnley budget game card guard failed:/);
  }
});

test('a resync with a different gap fails until the card is redrawn', t => {
  const dir = fixture(t);
  editData(dir, data => { data.gap['2027/28'].assumed.value_m = 0.6; });
  const failed = guard(dir);
  assert.equal(failed.status, 1);
  assert.match(failed.stderr, /gap_m 0.594 differs from data 0.6/);
});

test('a changed citation shape or edition line stops the build', t => {
  const dir = fixture(t);
  editData(dir, data => { data.gap['2027/28'].assumed.src = 'Fixture source, p5'; });
  assert.match(guard(dir).stderr, /citation has changed shape/);
  const page = path.join(dir, PAGE_SRC);
  fs.writeFileSync(page, fs.readFileSync(page, 'utf8').replace(/Edition of \d{1,2} [A-Z][a-z]+ \d{4}\./, 'Edition of 1 December 2026.'));
  fs.copyFileSync(path.join(root, DATA), path.join(dir, DATA));
  assert.match(guard(dir).stderr, /edition "14 November 2026" differs/);
});

test('a changed image is refused', t => {
  const dir = fixture(t);
  fs.appendFileSync(path.join(dir, pngPath), 'fixture');
  assert.match(guard(dir).stderr, /Card PNG differs/);
});

test('the card carries the reviewed figures and no dash', () => {
  const figures = cardFigures(loadData(), loadEdition());
  assert.equal(figures.text.figure, '£0.594m');
  assert.equal(figures.text.panel_note, 'at a 1.99% council tax rise');
  assert.equal(figures.table, 'Table 4');
  assert.equal(figures.page, 5);
  for (const text of Object.values(figures.text)) assert.doesNotMatch(text, /[\p{Dash_Punctuation}−]/u);
});
