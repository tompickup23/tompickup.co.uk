/* Fails the build if the budget game share card is missing, the wrong size,
 * or was made from figures that no longer match src/data/lcc-budget-game/game.html.
 *
 * The generator writes public/images/share/lcc-budget-game.json beside the PNG
 * with the five block percentages. This guard recomputes those percentages from
 * the data file (largest remainder, same as the page waffle) and compares.
 */
import fs from 'node:fs';
import path from 'node:path';

const ROOT = process.cwd();
const DATA = path.join(ROOT, 'src', 'data', 'lcc-budget-game', 'game.html');
const PAGE = path.join(ROOT, 'dist', 'lcc-budget-game', 'index.html');
const PNG = path.join(ROOT, 'dist', 'images', 'share', 'lcc-budget-game.png');
const SIDE = path.join(ROOT, 'src', 'data', 'lcc-budget-game', 'card-figures.json');
const W = 1200;
const H = 630;

function meta(html, attr, name) {
  const re = new RegExp(`<meta[^>]*${attr}="${name}"[^>]*>`, 'i');
  const tag = html.match(re)?.[0];
  return tag?.match(/content="([^"]*)"/i)?.[1];
}

function imageSize(file) {
  const b = fs.readFileSync(file);
  if (b.slice(1, 4).toString() === 'PNG') return [b.readUInt32BE(16), b.readUInt32BE(20)];
  return null;
}

function loadD() {
  const html = fs.readFileSync(DATA, 'utf8');
  const m = html.match(/const D = (\{[\s\S]*?\});\nconst PY/);
  if (!m) throw new Error(`const D not found in ${DATA}`);
  return JSON.parse(m[1]);
}

function waffleCells(values, total) {
  const raw = values.map((v) => (v / total) * 100);
  const cells = raw.map((r) => Math.trunc(r));
  const remainders = raw
    .map((r, i) => [r - cells[i], i])
    .sort((a, b) => b[0] - a[0]);
  let left = 100 - cells.reduce((s, n) => s + n, 0);
  for (const [, i] of remainders) {
    if (left <= 0) break;
    cells[i] += 1;
    left -= 1;
  }
  return cells;
}

const problems = [];
const fail = (msg) => problems.push(msg);

if (!fs.existsSync(PAGE)) fail('dist/lcc-budget-game/index.html missing');
if (!fs.existsSync(PNG)) fail('dist/images/share/lcc-budget-game.png missing');
if (!fs.existsSync(SIDE)) fail('src/data/lcc-budget-game/card-figures.json missing (run the card generator)');

if (fs.existsSync(PNG)) {
  const size = imageSize(PNG);
  if (!size || size[0] !== W || size[1] !== H) {
    fail(`card PNG is ${size ? size.join('x') : 'unknown'}, not ${W}x${H}`);
  }
}

if (fs.existsSync(PAGE)) {
  const html = fs.readFileSync(PAGE, 'utf8');
  const image = meta(html, 'property', 'og:image');
  if (!image || !image.endsWith('/images/share/lcc-budget-game.png')) {
    fail(`og:image is ${image || '(missing)'}, expected .../images/share/lcc-budget-game.png`);
  }
  if (meta(html, 'property', 'og:image:width') !== String(W) || meta(html, 'property', 'og:image:height') !== String(H)) {
    fail(`og:image:width/height must be ${W}x${H}`);
  }
  if (meta(html, 'name', 'twitter:card') !== 'summary_large_image') fail('twitter:card is not summary_large_image');
}

if (fs.existsSync(SIDE) && fs.existsSync(DATA)) {
  const side = JSON.parse(fs.readFileSync(SIDE, 'utf8'));
  const D = loadD();
  const by = D.meta.base_year;
  const req = D.requirement[by];
  const blockVals = Object.fromEntries(D.blocks.map((b) => [b.id, 0]));
  for (const line of D.lines) blockVals[line.block] += line.values[by];
  const values = D.blocks.map((b) => blockVals[b.id]);
  const cells = waffleCells(values, req);

  if (side.base_year !== by) fail(`sidecar base_year ${side.base_year} != data ${by}`);
  if (Math.abs(side.requirement_m - req) > 0.001) {
    fail(`sidecar requirement ${side.requirement_m} != data ${req}`);
  }
  if (!Array.isArray(side.blocks) || side.blocks.length !== D.blocks.length) {
    fail(`sidecar has ${side.blocks?.length} blocks, data has ${D.blocks.length}`);
  } else {
    D.blocks.forEach((b, i) => {
      const s = side.blocks[i];
      const expect = cells[i];
      if (s.id !== b.id) fail(`block ${i}: sidecar id ${s.id} != ${b.id}`);
      if (s.label !== b.label) fail(`block ${b.id}: sidecar label differs from data`);
      if (s.pct !== expect) fail(`block ${b.id}: card ${s.pct}% != data ${expect}%`);
    });
  }
}

if (problems.length) {
  console.error('Budget game card guard failed:');
  for (const p of problems) console.error('  -', p);
  process.exit(1);
}
console.log('Budget game card guard: ok');
