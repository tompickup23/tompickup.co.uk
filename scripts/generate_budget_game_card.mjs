import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { createHash } from 'node:crypto';
import satori from 'satori';
import { Resvg } from '@resvg/resvg-js';

const ROOT = fileURLToPath(new URL('../', import.meta.url));
export const W = 1200;
export const H = 630;
/** Hex values match the game's --c1 to --c5; keys are the service blocks they colour. */
const COLOURS = {
  adults: '#2a78d6',
  children: '#eb6834',
  place: '#1baf7a',
  backoffice: '#eda100',
  corporate: '#e87ba4',
};
export const GRID = { x: 738, y: 90, cell: 36, gap: 6, size: 414 };
export const DIVIDER_Y = GRID.y + GRID.size + 24;

export function loadData() {
  const html = fs.readFileSync(path.join(ROOT, 'src/data/lcc-budget-game/game.html'), 'utf8');
  const match = html.match(/const D = (\{[\s\S]*?\});\nconst PY/);
  if (!match) throw new Error('Budget data is missing');
  return JSON.parse(match[1]);
}

export function cardFigures(data) {
  const year = data.meta.base_year;
  const requirement = data.requirement[year];
  const values = data.blocks.map(block => data.lines
    .filter(line => line.block === block.id)
    .reduce((sum, line) => sum + line.values[year], 0));
  const raw = values.map(value => value / requirement * 100);
  const cells = raw.map(Math.floor);
  const order = raw.map((value, i) => ({ i, remainder: value - cells[i] }))
    .sort((a, b) => b.remainder - a.remainder);
  const remaining = 100 - cells.reduce((sum, value) => sum + value, 0);
  if (remaining < 0 || remaining > order.length) throw new Error('Budget blocks do not reconcile');
  for (const { i } of order.slice(0, remaining)) cells[i] += 1;
  return {
    base_year: year,
    requirement_m: requirement,
    square_m: Number((requirement / 100).toFixed(3)),
    blocks: data.blocks.map((block, i) => ({
      id: block.id, label: block.label, pct: cells[i], value_m: Number(values[i].toFixed(3)),
    })),
  };
}

export function sourceText(data) {
  const sources = [...new Set(data.lines.map(line => line.src))];
  const citations = sources.map(source => {
    const match = source.match(/^(\S+)\s+(.+)$/);
    const title = match && data.meta.docs[match[1]]?.title;
    if (!title) throw new Error('Budget source title or locator is missing');
    return `${title}, ${match[2]}`;
  });
  return `Source: Lancashire County Council. ${citations.join('; ')}. Net budget ${data.meta.base_year}. Each square 1%.`;
}

const box = (style, children) => ({ type: 'div', props: { style: { display: 'flex', ...style }, children } });
const label = (text, style = {}) => box(style, text);

export async function renderCard(data) {
  const figures = cardFigures(data);
  const source = sourceText(data);
  const visible = [data.meta.base_year, 'Can you balance Lancashire?', source, ...figures.blocks.map(b => b.label)];
  if (visible.some(text => /[\p{Dash_Punctuation}\u2212]/u.test(text))) throw new Error('Card text contains a dash');
  const palette = figures.blocks.flatMap((block) => Array(block.pct).fill(COLOURS[block.id]));
  const tree = box({ width: W, height: H, backgroundColor: '#0f1b26', color: '#f3f6f9', fontFamily: 'Public Sans' }, [
    box({ position: 'absolute', top: 0, left: 0, width: W, height: 8, backgroundColor: '#ff7a8a' }),
    label(`Lancashire County Council budget ${figures.base_year}`, {
      position: 'absolute', left: 48, top: 36, fontSize: 24, fontWeight: 600, color: '#b9c1ca',
    }),
    box({ position: 'absolute', left: 48, top: 85, flexDirection: 'column', fontFamily: 'Big Shoulders Display', fontWeight: 900, fontSize: 72, lineHeight: 1.04 }, [
      label('Can you balance'), label('Lancashire?', { color: '#ff7a8a' }),
    ]),
    box({ position: 'absolute', left: 48, top: 258, width: 640, flexDirection: 'column', gap: 12 }, figures.blocks.map((block) =>
      box({ alignItems: 'flex-start', gap: 14 }, [
        box({ width: 18, height: 18, marginTop: 6, borderRadius: 3, flexShrink: 0, backgroundColor: COLOURS[block.id] }),
        label(block.label, { width: 540, fontSize: 24, lineHeight: 1.15 }),
        label(`${block.pct}%`, { fontSize: 26, fontWeight: 600, width: 64, justifyContent: 'flex-end' }),
      ]))),
    ...palette.map((colour, i) => box({
      position: 'absolute', left: GRID.x + (i % 10) * (GRID.cell + GRID.gap),
      top: GRID.y + Math.floor(i / 10) * (GRID.cell + GRID.gap),
      width: GRID.cell, height: GRID.cell, backgroundColor: colour, borderRadius: 5,
    })),
    box({ position: 'absolute', left: 48, top: DIVIDER_Y, width: W - 96, height: 2, backgroundColor: '#2e3b48' }),
    label(source, { position: 'absolute', left: 48, top: DIVIDER_Y + 14, width: W - 96, fontSize: 22, lineHeight: 1.12, color: '#b9c1ca' }),
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
  return { svg, png, sidecar: { ...figures, source, png_sha256: createHash('sha256').update(png).digest('hex') } };
}

if (process.argv[1] && path.resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  const { png, sidecar } = await renderCard(loadData());
  const output = path.join(ROOT, 'public/images/share/lcc-budget-game.png');
  fs.mkdirSync(path.dirname(output), { recursive: true });
  fs.writeFileSync(output, png);
  fs.writeFileSync(path.join(ROOT, 'src/data/lcc-budget-game/card-figures.json'), `${JSON.stringify(sidecar, null, 2)}\n`);
  console.log(`Budget card rendered: ${W}x${H}`);
}
