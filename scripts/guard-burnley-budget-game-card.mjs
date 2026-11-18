/* Compare the rendered Burnley card and every recorded figure with the game data and the page. */
import fs from 'node:fs';
import path from 'node:path';
import { createHash } from 'node:crypto';
import { cardFigures, DATA, loadData, loadEdition, PAGE_SRC, SIDECAR, W, H } from './generate_burnley_budget_game_card.mjs';

const ROOT = process.cwd();
const PAGE = path.join(ROOT, 'dist', 'burnley-budget-game', 'index.html');
const PNG = path.join(ROOT, 'dist', 'images', 'share', 'burnley-budget-game.png');
const SIDE = path.join(ROOT, SIDECAR);

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

const problems = [];
const fail = (msg) => problems.push(msg);

if (!fs.existsSync(PAGE)) fail('dist/burnley-budget-game/index.html missing');
if (!fs.existsSync(PNG)) fail('dist/images/share/burnley-budget-game.png missing');
if (!fs.existsSync(SIDE)) fail(`${SIDECAR} missing (run the card generator)`);
for (const file of [DATA, PAGE_SRC]) if (!fs.existsSync(path.join(ROOT, file))) fail(`${file} missing`);

if (fs.existsSync(PNG)) {
  const size = imageSize(PNG);
  if (!size || size[0] !== W || size[1] !== H) fail(`card PNG is ${size ? size.join('x') : 'unknown'}, not ${W}x${H}`);
}

if (fs.existsSync(PAGE)) {
  const html = fs.readFileSync(PAGE, 'utf8');
  for (const [attr, name] of [['property', 'og:image'], ['name', 'twitter:image']]) {
    const image = meta(html, attr, name);
    if (!image || !image.endsWith('/images/share/burnley-budget-game.png')) {
      fail(`${name} is ${image || '(missing)'}, expected .../images/share/burnley-budget-game.png`);
    }
  }
  if (meta(html, 'property', 'og:image:width') !== String(W) || meta(html, 'property', 'og:image:height') !== String(H)) {
    fail(`og:image:width/height must be ${W}x${H}`);
  }
  if (meta(html, 'name', 'twitter:card') !== 'summary_large_image') fail('twitter:card is not summary_large_image');
}

if (!problems.length) {
  const side = JSON.parse(fs.readFileSync(SIDE, 'utf8'));
  let expect;
  try {
    expect = cardFigures(loadData(ROOT), loadEdition(ROOT));
  } catch (err) {
    fail(err.message);
  }
  if (expect) {
    for (const key of ['play_year', 'gap_m', 'rate_pct', 'table', 'page', 'edition', 'source']) {
      if (side[key] !== expect[key]) fail(`sidecar ${key} ${JSON.stringify(side[key])} differs from data ${JSON.stringify(expect[key])}`);
    }
    for (const [key, text] of Object.entries(expect.text)) {
      if (side.text?.[key] !== text) fail(`card text ${key} differs from data`);
    }
  }
  if (side.png_sha256 !== createHash('sha256').update(fs.readFileSync(PNG)).digest('hex')) {
    fail('Card PNG differs from the recorded render');
  }
}

if (problems.length) {
  console.error('Burnley budget game card guard failed:');
  for (const p of problems) console.error('  ', p);
  process.exit(1);
}
console.log('Burnley budget game card guard: ok');
