/* Draws the 1200x630 share card for /burnley-budget-game/ from the game's own data.
 *
 * Runs in prebuild, beside the Lancashire card (generate_budget_game_card.mjs), and writes
 * public/images/share/burnley-budget-game.png and src/data/burnley-budget-game/card-figures.json.
 * scripts/guard-burnley-budget-game-card.mjs (postbuild) compares both with the game data.
 *
 * Every figure on the card is read from the game (src/data/burnley-budget-game/game.html):
 * the play year, the council's printed gap at its assumed council tax rise, that rise, and the
 * table and page they come from. The edition date is read from the page's edition line. A resync
 * that changes the figures changes the card; a resync that changes the citation's shape stops the build.
 * The card is neutral: no party wording, no portrait and no named individual.
 */
import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { createHash } from 'node:crypto';
import satori from 'satori';
import { Resvg } from '@resvg/resvg-js';

const ROOT = fileURLToPath(new URL('../', import.meta.url));
export const W = 1200;
export const H = 630;
export const DATA = 'src/data/burnley-budget-game/game.html';
export const PAGE_SRC = 'src/pages/burnley-budget-game/index.astro';
export const PNG_OUT = 'public/images/share/burnley-budget-game.png';
export const SIDECAR = 'src/data/burnley-budget-game/card-figures.json';
/* The hex values are the game's and the Lancashire card's: --hero, --hero-ink, --mute, --line. */
const C = { bg: '#0f1b26', ink: '#f3f6f9', mute: '#b9c1ca', line: '#2e3b48', panel: '#16242f', accent: '#ff7a8a' };
export const PANEL = { x: 738, y: 90, w: 414, h: 414 };
export const DIVIDER_Y = PANEL.y + PANEL.h + 24;

export function loadData(root = ROOT) {
  const html = fs.readFileSync(path.join(root, DATA), 'utf8');
  const match = html.match(/const D = (\{[\s\S]*?\});\nconst PY/);
  if (!match) throw new Error('Burnley budget data is missing');
  return JSON.parse(match[1]);
}

/** The edition date from the page's edition line, which is the one place it is written. */
export function loadEdition(root = ROOT) {
  const page = fs.readFileSync(path.join(root, PAGE_SRC), 'utf8');
  const found = [...page.matchAll(/Edition of (\d{1,2} [A-Z][a-z]+ \d{4})\./g)];
  if (found.length !== 1) throw new Error('Burnley budget card: expected one edition line in the page');
  return found[0][1];
}

/* The game cites Table 4 as "Burnley Borough Council, Revenue Budget 2027-30, Latest Position and
 * Savings Proposals (MTFS mid-year update), Executive 16 September 2026 (s51890), Table 4, p5".
 * The card text may carry no dash of any kind, so the year range and the bracketed subtitle are left
 * out of the short title; everything else is kept as the game cites it. */
const CITATION = /^(Burnley Borough Council), Revenue Budget \d{4}-\d{2}, (Latest Position and Savings Proposals) \(MTFS mid-year update\), (Executive) (\d{1,2} [A-Z][a-z]+ \d{4}) \(s\d+\), (Table \d+), p(\d+)$/;

export function cardFigures(data, edition) {
  const year = data.meta.play_year;
  const gap = data.gap?.[year]?.assumed;
  if (!gap || gap.fy !== year) throw new Error(`Burnley budget card: no assumed gap for ${year}`);
  if (typeof gap.value_m !== 'number' || typeof gap.rate !== 'number') throw new Error('Burnley budget card: gap or rate is not a number');
  const cite = gap.src.match(CITATION);
  if (!cite || cite[5] !== gap.table) throw new Error('Burnley budget card: the gap citation has changed shape; update CITATION');
  const [, publisher, title, meeting, date, table, page] = cite;
  const source = `Source: ${publisher}, Revenue Budget, ${title}, ${meeting}, ${date}, ${table}, page ${page}. Edition of ${edition}.`;
  return {
    play_year: year,
    gap_m: gap.value_m,
    rate_pct: gap.rate,
    table,
    page: Number(page),
    edition,
    source,
    text: {
      kicker: publisher,
      title: 'Budget scenario game',
      year,
      lede: "Where the council's money goes, its own budget gap, and the choices that could close it.",
      panel_label: `The council's printed budget gap for ${year}`,
      figure: `£${gap.value_m.toFixed(3)}m`,
      panel_note: `at a ${gap.rate.toFixed(2)}% council tax rise`,
      source,
    },
  };
}

const box = (style, children) => ({ type: 'div', props: { style: { display: 'flex', ...style }, children } });
const label = (text, style = {}) => box(style, text);

export async function renderCard(data, edition) {
  const figures = cardFigures(data, edition);
  const t = figures.text;
  if (Object.values(t).some(text => /[\p{Dash_Punctuation}−]/u.test(text))) throw new Error('Card text contains a dash');
  const tree = box({ width: W, height: H, backgroundColor: C.bg, color: C.ink, fontFamily: 'Public Sans' }, [
    box({ position: 'absolute', top: 0, left: 0, width: W, height: 8, backgroundColor: C.accent }),
    label(t.kicker, { position: 'absolute', left: 48, top: 36, fontSize: 24, fontWeight: 600, color: C.mute }),
    box({ position: 'absolute', left: 48, top: 85, flexDirection: 'column', fontFamily: 'Big Shoulders Display', fontWeight: 900, fontSize: 72, lineHeight: 1.04 }, [
      label(t.title), label(t.year, { color: C.accent }),
    ]),
    label(t.lede, { position: 'absolute', left: 48, top: 282, width: 620, fontSize: 30, lineHeight: 1.3 }),
    box({
      position: 'absolute', left: PANEL.x, top: PANEL.y, width: PANEL.w, height: PANEL.h,
      flexDirection: 'column', justifyContent: 'center', padding: '0 36px', boxSizing: 'border-box',
      backgroundColor: C.panel, border: `2px solid ${C.line}`, borderRadius: 8,
    }, [
      label(t.panel_label, { fontSize: 26, fontWeight: 600, lineHeight: 1.25, color: C.mute }),
      label(t.figure, { fontFamily: 'Big Shoulders Display', fontWeight: 900, fontSize: 92, lineHeight: 1.05, marginTop: 14 }),
      label(t.panel_note, { fontSize: 26, lineHeight: 1.25, marginTop: 8 }),
    ]),
    box({ position: 'absolute', left: 48, top: DIVIDER_Y, width: W - 96, height: 2, backgroundColor: C.line }),
    label(t.source, { position: 'absolute', left: 48, top: DIVIDER_Y + 14, width: W - 96, fontSize: 22, lineHeight: 1.12, color: C.mute }),
  ]);
  const fonts = [
    ['Big Shoulders Display', 'big-shoulders-display', 900],
    ['Public Sans', 'public-sans', 400],
    ['Public Sans', 'public-sans', 600],
  ].map(([name, pkg, weight]) => ({
    name, weight, style: 'normal',
    data: fs.readFileSync(path.join(ROOT, `node_modules/@fontsource/${pkg}/files/${pkg}-latin-${weight}-normal.woff`)),
  }));
  const svg = await satori(tree, { width: W, height: H, fonts });
  const png = new Resvg(svg, { font: { loadSystemFonts: false } }).render().asPng();
  const { text, ...recorded } = figures;
  return { svg, png, sidecar: { ...recorded, text, png_sha256: createHash('sha256').update(png).digest('hex') } };
}

if (process.argv[1] && path.resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  const { png, sidecar } = await renderCard(loadData(), loadEdition());
  const output = path.join(ROOT, PNG_OUT);
  fs.mkdirSync(path.dirname(output), { recursive: true });
  fs.writeFileSync(output, png);
  fs.writeFileSync(path.join(ROOT, SIDECAR), `${JSON.stringify(sidecar, null, 2)}\n`);
  console.log(`Burnley budget card rendered: ${W}x${H}`);
}
