import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { spawnSync } from 'node:child_process';
import { fileURLToPath } from 'node:url';
import test from 'node:test';
import { cardFigures, DIVIDER_Y, GRID, loadData, sourceText } from './generate_budget_game_card.mjs';

const root = fileURLToPath(new URL('../', import.meta.url));
const sidePath = 'src/data/lcc-budget-game/card-figures.json';
const dataPath = 'src/data/lcc-budget-game/game.html';
const pngPath = 'dist/images/share/lcc-budget-game.png';
const pagePath = 'dist/lcc-budget-game/index.html';

function fixture(t) {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'budgetcard'));
  t.after(() => fs.rmSync(dir, { recursive: true, force: true }));
  for (const file of [sidePath, dataPath, pngPath, pagePath]) {
    fs.mkdirSync(path.dirname(path.join(dir, file)), { recursive: true });
    fs.copyFileSync(path.join(root, file), path.join(dir, file));
  }
  return dir;
}

function guard(dir) {
  return spawnSync(process.execPath, [path.join(root, 'scripts/guard-budget-game-card.mjs')], { cwd: dir, encoding: 'utf8' });
}

test('every recorded figure fails when changed and passes when restored', t => {
  const dir = fixture(t);
  const original = fs.readFileSync(path.join(dir, sidePath), 'utf8');
  const baseline = JSON.parse(original);
  const mutations = [
    ['budget year', side => { side.base_year = '2000/01'; }],
    ['requirement', side => { side.requirement_m += 0.0001; }],
    ['square value', side => { side.square_m += 0.001; }],
    ...baseline.blocks.flatMap((block, i) => [
      [`${block.id} percentage`, side => { side.blocks[i].pct += 1; }],
      [`${block.id} value`, side => { side.blocks[i].value_m += 0.001; }],
    ]),
  ];
  for (const [name, change] of mutations) {
    const side = structuredClone(baseline);
    change(side);
    fs.writeFileSync(path.join(dir, sidePath), JSON.stringify(side));
    const failed = guard(dir);
    assert.equal(failed.status, 1, `${name}: ${failed.stderr}`);
    assert.match(failed.stderr, /Budget game card guard failed:/);
    console.log(`Changed fixture ${name}: exit 1`);
  }
  fs.writeFileSync(path.join(dir, sidePath), original);
  const passed = guard(dir);
  assert.equal(passed.status, 0, passed.stderr);
  console.log(`Restored fixture: exit 0. ${passed.stdout.trim()}`);
});

test('changed source figures fail even if their rounded percentage stays the same', t => {
  const dir = fixture(t);
  const html = fs.readFileSync(path.join(dir, dataPath), 'utf8');
  const data = loadData();
  data.lines[0].values[data.meta.base_year] += 0.001;
  fs.writeFileSync(path.join(dir, dataPath), html.replace(/const D = (\{[\s\S]*?\});\nconst PY/, () => `const D = ${JSON.stringify(data)};\nconst PY`));
  const failed = guard(dir);
  assert.equal(failed.status, 1);
  assert.match(failed.stderr, /value differs from data/);
});

test('citation follows the source document and its locator', () => {
  const data = loadData();
  const id = data.lines[0].src.split(' ')[0];
  data.meta.docs[id].title = 'Fixture source title, 4 November 2026';
  data.lines.forEach(line => { line.src = `${id} Annex 9 Fixture table, p123`; });
  const source = sourceText(data);
  assert.ok(source.includes(data.meta.docs[id].title));
  assert.match(source, /Annex 9 Fixture table, p123/);
  assert.doesNotMatch(source, /Annex 5|p92|Medium Term Financial Strategy/);
  delete data.meta.docs[id];
  assert.throws(() => sourceText(data), /source title or locator is missing/);
});

test('changed citation and image are refused', t => {
  const dir = fixture(t);
  const original = fs.readFileSync(path.join(dir, sidePath), 'utf8');
  const side = JSON.parse(original);
  side.source = 'Fixture citation';
  fs.writeFileSync(path.join(dir, sidePath), JSON.stringify(side));
  assert.match(guard(dir).stderr, /Source citation differs/);
  fs.writeFileSync(path.join(dir, sidePath), original);
  fs.appendFileSync(path.join(dir, pngPath), 'fixture');
  const failed = guard(dir);
  assert.equal(failed.status, 1);
  assert.match(failed.stderr, /Card PNG differs/);
});

test('grid and divider have clear space and shares match the reviewed budget', () => {
  assert.equal(DIVIDER_Y - (GRID.y + GRID.size), 24);
  assert.deepEqual(cardFigures(loadData()).blocks.map(block => block.pct), [42, 20, 20, 7, 11]);
});
